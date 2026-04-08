{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY athlete_name) AS athlete_id,
    athlete_name AS full_name,
    country_code
FROM (SELECT DISTINCT athlete_name, country_code FROM {{ ref('stg_medals') }} WHERE athlete_name IS NOT NULL) a
