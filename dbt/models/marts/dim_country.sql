{{ config(materialized='table') }}

SELECT DISTINCT
    country_code,
    country_code AS country_name
FROM {{ ref('stg_medals') }}
WHERE country_code IS NOT NULL
