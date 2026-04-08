{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY e.sport, e.event_name, e.gender) AS event_id,
    s.sport_id,
    e.event_name,
    e.gender
FROM (SELECT DISTINCT sport, event_name, gender FROM {{ ref('stg_medals') }} WHERE event_name IS NOT NULL) e
JOIN {{ ref('dim_sport') }} s ON e.sport = s.sport_name
