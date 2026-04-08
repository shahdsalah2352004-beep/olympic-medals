{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY year, season) AS games_id,
    year,
    season,
    NULL AS host_city,
    NULL AS host_country
FROM (SELECT DISTINCT year, season FROM {{ ref('stg_medals') }} WHERE year IS NOT NULL) g
