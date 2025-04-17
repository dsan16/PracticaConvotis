{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH fct_news AS (
    SELECT
        *
    FROM
        {{ ref('src_news') }}
)
SELECT
    raw_data:"author"::STRING            AS author,
    raw_data:"title"::STRING             AS title,
    raw_data:"description"::STRING       AS description,
    raw_data:"publishedAt"::TIMESTAMP    AS published_at,
    raw_data:"url"::STRING               AS url,
    raw_data:"urlToImage"::STRING        AS image_url,
    raw_data:"source":"name"::STRING     AS source_name
FROM fct_news