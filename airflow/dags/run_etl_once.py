import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=r"D:\DEPI\medalists_project\.env", override=True)


def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OLYMPIC_MEDALS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def main():
    csv_path = r"D:\DEPI\medalists_project\scraper\output\all_medals.csv"

    if not os.path.exists(csv_path):
        print(f"ERROR: all_medals.csv not found at {csv_path}")
        return

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from CSV")

    conn = get_conn()
    cur = conn.cursor()
    try:
        print("Loading raw_medals...")
        cur.execute("TRUNCATE TABLE RAW_MEDALS")
        rows = [
            (
                str(row["athlete_name"]),
                str(row["country_code"]),
                int(row["year"]) if str(row["year"]).isdigit() else None,
                str(row["season"]) if str(row["season"]) != "nan" else None,
                str(row["sport"]),
                str(row["event_name"]) if str(row["event_name"]) != "nan" else None,
                str(row["gender"]) if str(row["gender"]) != "nan" else None,
                str(row["medal_type"]),
            )
            for _, row in df.iterrows()
        ]
        cur.executemany(
            """INSERT INTO RAW_MEDALS
            (athlete_name, country_code, year, season, sport, event_name, gender, medal_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            rows,
        )
        print(f"Inserted {len(rows)} rows into RAW_MEDALS")

        cur.execute("TRUNCATE TABLE FACT_MEDALS")
        cur.execute("TRUNCATE TABLE DIM_ATHLETE")
        cur.execute("TRUNCATE TABLE DIM_EVENT")
        cur.execute("TRUNCATE TABLE DIM_GAMES")
        cur.execute("TRUNCATE TABLE DIM_SPORT")
        cur.execute("TRUNCATE TABLE DIM_COUNTRY")

        cur.execute("""
            INSERT INTO DIM_COUNTRY (country_code, country_name)
            SELECT DISTINCT country_code, country_code
            FROM RAW_MEDALS
            WHERE country_code IS NOT NULL AND country_code != ''
        """)
        print("DIM_COUNTRY loaded")

        cur.execute("""
            INSERT INTO DIM_SPORT (sport_id, sport_name, category)
            SELECT SEQ_SPORT_ID.NEXTVAL, s.sport, s.category
            FROM (
                SELECT sport,
                    CASE
                        WHEN MAX(CASE WHEN season='Summer' THEN 1 ELSE 0 END)=1 THEN 'Summer'
                        WHEN MAX(CASE WHEN season='Winter' THEN 1 ELSE 0 END)=1 THEN 'Winter'
                        ELSE 'Unknown'
                    END AS category
                FROM RAW_MEDALS
                WHERE sport IS NOT NULL AND sport != ''
                GROUP BY sport
            ) s
        """)
        print("DIM_SPORT loaded")

        cur.execute("""
            INSERT INTO DIM_EVENT (event_id, sport_id, event_name, gender)
            SELECT SEQ_EVENT_ID.NEXTVAL, e.sport_id, e.event_name, e.gender
            FROM (
                SELECT DISTINCT ds.sport_id, r.event_name, r.gender
                FROM RAW_MEDALS r
                JOIN DIM_SPORT ds ON ds.sport_name = r.sport
                WHERE r.event_name IS NOT NULL AND r.event_name != ''
                  AND r.gender IS NOT NULL AND r.gender != ''
            ) e
        """)
        print("DIM_EVENT loaded")

        cur.execute("""
            INSERT INTO DIM_GAMES (games_id, year, season, host_city, host_country)
            SELECT SEQ_GAMES_ID.NEXTVAL, g.year, g.season, NULL, NULL
            FROM (
                SELECT DISTINCT
                    COALESCE(year, -1) AS year,
                    COALESCE(NULLIF(season, ''), 'Unknown') AS season
                FROM RAW_MEDALS
            ) g
        """)
        print("DIM_GAMES loaded")

        cur.execute("""
            INSERT INTO DIM_ATHLETE (athlete_id, full_name, country_code)
            SELECT SEQ_ATHLETE_ID.NEXTVAL, a.full_name, a.country_code
            FROM (
                SELECT DISTINCT athlete_name AS full_name, country_code
                FROM RAW_MEDALS
                WHERE athlete_name IS NOT NULL AND athlete_name != ''
                  AND country_code IS NOT NULL AND country_code != ''
            ) a
        """)
        print("DIM_ATHLETE loaded")

        cur.execute("""
            INSERT INTO FACT_MEDALS
            (medal_id, athlete_id, country_code, sport_id, event_id, games_id, medal_type)
            SELECT SEQ_MEDAL_ID.NEXTVAL,
                da.athlete_id, r.country_code, ds.sport_id,
                de.event_id, dg.games_id, r.medal_type
            FROM RAW_MEDALS r
            JOIN DIM_ATHLETE da ON r.athlete_name=da.full_name AND r.country_code=da.country_code
            JOIN DIM_SPORT ds ON r.sport=ds.sport_name
            JOIN DIM_EVENT de ON r.event_name=de.event_name AND r.gender=de.gender AND ds.sport_id=de.sport_id
            JOIN DIM_GAMES dg ON COALESCE(r.year,-1)=dg.year
                AND COALESCE(NULLIF(r.season,''),'Unknown')=dg.season
        """)
        print("FACT_MEDALS loaded")

        conn.commit()
        print("\n✅ ETL completed successfully!")
        print("\nRow counts:")
        for table in ["RAW_MEDALS","DIM_ATHLETE","DIM_COUNTRY","DIM_SPORT","DIM_EVENT","DIM_GAMES","FACT_MEDALS"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"  {table}: {cur.fetchone()[0]}")

    except Exception as e:
        conn.rollback()
        print(f"❌ ETL failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()