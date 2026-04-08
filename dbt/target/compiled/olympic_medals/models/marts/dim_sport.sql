

SELECT
    ROW_NUMBER() OVER (ORDER BY sport) AS sport_id,
    sport AS sport_name,
    'General' AS category
FROM (SELECT DISTINCT sport FROM OLYMPIC_MEDALS.PUBLIC_PUBLIC.stg_medals WHERE sport IS NOT NULL) s