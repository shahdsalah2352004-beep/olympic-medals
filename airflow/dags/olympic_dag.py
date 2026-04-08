from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import snowflake.connector
import os
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'olympic_etl',
    default_args=default_args,
    description='ETL pipeline for Olympic medals: scrape → stage → transform → load to star schema',
    schedule_interval='@once',
    catchup=False,
    tags=['olympics', 'etl'],
)


def get_snowflake_connection():
    """Create and return a Snowflake connection using Airflow Variables."""
    try:
        return snowflake.connector.connect(
            account=Variable.get('SNOWFLAKE_ACCOUNT'),
            user=Variable.get('SNOWFLAKE_USER'),
            password=Variable.get('SNOWFLAKE_PASSWORD'),
            database=Variable.get('SNOWFLAKE_DATABASE'),
            schema=Variable.get('SNOWFLAKE_SCHEMA'),
            warehouse=Variable.get('SNOWFLAKE_WAREHOUSE'),
        )
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def validate_source_data():
    logger.info("Task 1: Validating source data...")

    csv_path = '/home/airflow/dags/../../../scraper/output/all_medals.csv'
    if not os.path.exists(csv_path):
        csv_path = './scraper/output/all_medals.csv'

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"all_medals.csv not found at {csv_path}")

    file_size = os.path.getsize(csv_path)
    if file_size == 0:
        raise ValueError("all_medals.csv is empty")

    with open(csv_path, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f) - 1

    logger.info(f"Validation passed: {file_size} bytes, {line_count} data rows")


def load_to_staging():
    logger.info("Task 2: Loading data to staging table...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE raw_medals")

        csv_path = '/home/airflow/dags/../../../scraper/output/all_medals.csv'
        if not os.path.exists(csv_path):
            csv_path = './scraper/output/all_medals.csv'
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        import pandas as pd
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from CSV")

        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            values = []
            for _, row in batch.iterrows():
                values.append(f"""(
                    '{str(row['athlete_name']).replace("'", "''")}',
                    '{str(row['country_code']).replace("'", "''")}',
                    {int(row['year']) if pd.notna(row['year']) else 'NULL'},
                    '{str(row['season']) if pd.notna(row['season']) else ''}',
                    '{str(row['sport']).replace("'", "''")}',
                    '{str(row['event_name']).replace("'", "''") if pd.notna(row['event_name']) else ''}',
                    '{str(row['gender']).replace("'", "''")}',
                    '{str(row['medal_type']).replace("'", "''")}'
                )""")

            insert_sql = f"""
                INSERT INTO raw_medals
                (athlete_name, country_code, year, season, sport, event_name, gender, medal_type)
                VALUES {','.join(values)}
            """
            cursor.execute(insert_sql)

        conn.commit()
        logger.info(f"Loaded {len(df)} rows to raw_medals")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def transform_dimensions():
    logger.info("Task 3: Transforming and loading dimensions...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO dim_country (country_code, country_name)
            SELECT DISTINCT country_code, country_code
            FROM raw_medals
            WHERE country_code IS NOT NULL AND country_code != ''
            AND NOT EXISTS (
                SELECT 1 FROM dim_country WHERE dim_country.country_code = raw_medals.country_code
            )
        """)

        cursor.execute("""
            INSERT INTO dim_sport (sport_id, sport_name, category)
            SELECT
                SEQ_SPORT_ID.NEXTVAL,
                s.sport,
                s.category
            FROM (
                SELECT
                    sport,
                    CASE
                        WHEN MAX(CASE WHEN season = 'Summer' THEN 1 ELSE 0 END) = 1 THEN 'Summer'
                        WHEN MAX(CASE WHEN season = 'Winter' THEN 1 ELSE 0 END) = 1 THEN 'Winter'
                        ELSE 'Unknown'
                    END AS category
                FROM raw_medals
                WHERE sport IS NOT NULL AND sport != ''
                GROUP BY sport
            ) s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_sport ds WHERE ds.sport_name = s.sport
            )
        """)

        cursor.execute("""
            INSERT INTO dim_event (event_id, sport_id, event_name, gender)
            SELECT
                SEQ_EVENT_ID.NEXTVAL,
                e.sport_id,
                e.event_name,
                e.gender
            FROM (
                SELECT DISTINCT
                    ds.sport_id AS sport_id,
                    r.event_name AS event_name,
                    r.gender AS gender
                FROM raw_medals r
                JOIN dim_sport ds ON ds.sport_name = r.sport
                WHERE r.event_name IS NOT NULL AND r.event_name != ''
                  AND r.gender IS NOT NULL AND r.gender != ''
            ) e
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_event de
                WHERE de.sport_id = e.sport_id
                  AND de.event_name = e.event_name
                  AND de.gender = e.gender
            )
        """)

        cursor.execute("""
            INSERT INTO dim_games (games_id, year, season, host_city, host_country)
            SELECT
                SEQ_GAMES_ID.NEXTVAL,
                g.year,
                g.season,
                NULL,
                NULL
            FROM (
                SELECT DISTINCT
                    COALESCE(year, -1) AS year,
                    COALESCE(NULLIF(season, ''), 'Unknown') AS season
                FROM raw_medals
            ) g
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_games
                WHERE dim_games.year = g.year
                  AND dim_games.season = g.season
            )
        """)

        cursor.execute("""
            INSERT INTO dim_athlete (athlete_id, full_name, country_code)
            SELECT
                SEQ_ATHLETE_ID.NEXTVAL,
                a.full_name,
                a.country_code
            FROM (
                SELECT DISTINCT
                    athlete_name AS full_name,
                    country_code AS country_code
                FROM raw_medals
                WHERE athlete_name IS NOT NULL AND athlete_name != ''
                  AND country_code IS NOT NULL AND country_code != ''
            ) a
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_athlete
                WHERE full_name = a.full_name AND country_code = a.country_code
            )
        """)

        conn.commit()
        logger.info("Dimension loading complete")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def transform_facts():
    logger.info("Task 4: Transforming and loading facts...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE fact_medals")
        cursor.execute("""
            INSERT INTO fact_medals
            (medal_id, athlete_id, country_code, sport_id, event_id, games_id, medal_type)
            SELECT
                SEQ_MEDAL_ID.NEXTVAL,
                da.athlete_id,
                r.country_code,
                ds.sport_id,
                de.event_id,
                dg.games_id,
                r.medal_type
            FROM raw_medals r
            INNER JOIN dim_athlete da ON r.athlete_name = da.full_name AND r.country_code = da.country_code
            INNER JOIN dim_sport ds ON r.sport = ds.sport_name
            INNER JOIN dim_event de
                ON r.event_name = de.event_name
               AND r.gender = de.gender
               AND ds.sport_id = de.sport_id
            INNER JOIN dim_games dg
                ON COALESCE(r.year, -1) = dg.year
               AND COALESCE(NULLIF(r.season, ''), 'Unknown') = dg.season
        """)
        conn.commit()
        logger.info("Fact loading complete")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def validate_load():
    logger.info("Task 5: Validating data load...")

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM raw_medals")
        raw_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM fact_medals")
        fact_count = cursor.fetchone()[0]

        if raw_count == 0:
            raise ValueError("No rows in raw_medals")
        if fact_count != raw_count:
            raise ValueError(f"Row count mismatch: raw_medals={raw_count}, fact_medals={fact_count}")

        logger.info(f"Validation passed: {fact_count} rows in fact table")
    finally:
        cursor.close()
        conn.close()


task_validate = PythonOperator(
    task_id='validate_source_data',
    python_callable=validate_source_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    dag=dag,
)

task_transform_dim = PythonOperator(
    task_id='transform_dimensions',
    python_callable=transform_dimensions,
    dag=dag,
)

task_transform_fact = PythonOperator(
    task_id='transform_facts',
    python_callable=transform_facts,
    dag=dag,
)

task_validate_load = PythonOperator(
    task_id='validate_load',
    python_callable=validate_load,
    dag=dag,
)

task_validate >> task_load >> task_transform_dim >> task_transform_fact >> task_validate_load
