WITH raw_news AS (
    SELECT
        *
    FROM
       {{ source('newsapi_db', 'news') }}
)
SELECT
    raw_file as raw_data
FROM
    raw_news