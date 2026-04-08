{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY m.athlete_name, m.year, m.medal_type) AS medal_id,
    a.athlete_id,
    m.country_code,
    s.sport_id,
    e.event_id,
    g.games_id,
    m.medal_type
FROM {{ ref('stg_medals') }} m
JOIN {{ ref('dim_athlete') }} a ON m.athlete_name = a.full_name AND m.country_code = a.country_code
JOIN {{ ref('dim_sport') }} s ON m.sport = s.sport_name
JOIN {{ ref('dim_event') }} e ON m.event_name = e.event_name AND m.gender = e.gender
JOIN {{ ref('dim_games') }} g ON m.year = g.year AND m.season = g.season
