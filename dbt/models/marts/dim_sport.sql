{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY sport) AS sport_id,
    sport AS sport_name,
    'General' AS category
FROM (SELECT DISTINCT sport FROM {{ ref('stg_medals') }} WHERE sport IS NOT NULL) s
