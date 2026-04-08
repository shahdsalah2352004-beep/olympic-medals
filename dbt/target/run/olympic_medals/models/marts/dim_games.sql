
  
    

        create or replace transient table OLYMPIC_MEDALS.PUBLIC_PUBLIC.dim_games
         as
        (

SELECT
    ROW_NUMBER() OVER (ORDER BY year, season) AS games_id,
    year,
    season,
    NULL AS host_city,
    NULL AS host_country
FROM (SELECT DISTINCT year, season FROM OLYMPIC_MEDALS.PUBLIC_PUBLIC.stg_medals WHERE year IS NOT NULL) g
        );
      
  