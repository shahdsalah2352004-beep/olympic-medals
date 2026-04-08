-- ============================================================================
-- SNOWFLAKE STAR SCHEMA FOR OLYMPIC MEDALS
-- ============================================================================

-- Create staging table (raw load target)
CREATE TABLE IF NOT EXISTS raw_medals (
    athlete_name VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    year INTEGER,
    season VARCHAR,
    sport VARCHAR NOT NULL,
    event_name VARCHAR,
    gender VARCHAR,
    medal_type VARCHAR NOT NULL,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_athlete (
    athlete_id INTEGER PRIMARY KEY,
    full_name VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_code VARCHAR PRIMARY KEY,
    country_name VARCHAR NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_sport (
    sport_id INTEGER PRIMARY KEY,
    sport_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_event (
    event_id INTEGER PRIMARY KEY,
    sport_id INTEGER NOT NULL,
    event_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS dim_games (
    games_id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    season VARCHAR NOT NULL,
    host_city VARCHAR,
    host_country VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS fact_medals (
    medal_id INTEGER PRIMARY KEY,
    athlete_id INTEGER NOT NULL,
    country_code VARCHAR NOT NULL,
    sport_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    games_id INTEGER NOT NULL,
    medal_type VARCHAR NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- SEQUENCES FOR SURROGATE KEY GENERATION
-- ============================================================================

CREATE SEQUENCE IF NOT EXISTS seq_athlete_id START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS seq_sport_id START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS seq_event_id START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS seq_games_id START = 1 INCREMENT = 1;
CREATE SEQUENCE IF NOT EXISTS seq_medal_id START = 1 INCREMENT = 1;

-- ============================================================================
-- TRUNCATE TABLES SCRIPT (for reruns)
-- ============================================================================
-- UNCOMMENT TO USE:
-- TRUNCATE TABLE fact_medals;
-- TRUNCATE TABLE dim_athlete;
-- TRUNCATE TABLE dim_event;
-- TRUNCATE TABLE dim_games;
-- TRUNCATE TABLE dim_sport;
-- TRUNCATE TABLE dim_country;
-- TRUNCATE TABLE raw_medals;
-- ALTER SEQUENCE seq_athlete_id RESTART START WITH 1;
-- ALTER SEQUENCE seq_sport_id RESTART START WITH 1;
-- ALTER SEQUENCE seq_event_id RESTART START WITH 1;
-- ALTER SEQUENCE seq_games_id RESTART START WITH 1;
-- ALTER SEQUENCE seq_medal_id RESTART START WITH 1;

# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'information_schema.packages'
    dataframe = session.table(tableName).filter(col("language") == 'python')

    # Print a sample of the dataframe to standard output.
    dataframe.show()


    # Return value will appear in the Results tab.
    return dataframe

