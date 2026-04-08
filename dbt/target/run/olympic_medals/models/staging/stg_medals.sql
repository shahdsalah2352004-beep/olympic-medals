
  create or replace   view OLYMPIC_MEDALS.PUBLIC_PUBLIC.stg_medals
  
   as (
    

SELECT
    TRIM(UPPER(athlete_name)) AS athlete_name,
    TRIM(UPPER(country_code)) AS country_code,
    year AS year,
    TRIM(season) AS season,
    TRIM(sport) AS sport,
    TRIM(event_name) AS event_name,
    TRIM(gender) AS gender,
    TRIM(medal_type) AS medal_type
FROM OLYMPIC_MEDALS.PUBLIC.raw_medals
WHERE athlete_name IS NOT NULL
AND medal_type IN ('Gold', 'Silver', 'Bronze')
  );

