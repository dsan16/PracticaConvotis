{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
 WITH ranking_per_day AS (
    SELECT
        published_date,
        keyword,
        conteo,
        ROW_NUMBER() OVER (PARTITION BY published_date ORDER BY conteo DESC) AS rank
    FROM {{ ref('int_keywords') }}
)
SELECT
    published_date,
    keyword,
    conteo,
    rank
FROM ranking_per_day
where rank <= 3
