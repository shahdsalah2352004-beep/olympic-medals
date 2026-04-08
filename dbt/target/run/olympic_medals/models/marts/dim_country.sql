
  
    

        create or replace transient table OLYMPIC_MEDALS.PUBLIC_PUBLIC.dim_country
         as
        (

SELECT DISTINCT
    country_code,
    country_code AS country_name
FROM OLYMPIC_MEDALS.PUBLIC_PUBLIC.stg_medals
WHERE country_code IS NOT NULL
        );
      
  