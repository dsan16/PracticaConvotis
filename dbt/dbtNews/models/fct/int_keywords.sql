{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH tokenized as (
    select
        published_at::date as published_date,
        lower(f.value::string) as keyword
    from source
    , lateral flatten(
        input => split(
            regexp_replace(
                description,
                '[^A-Za-zÁÉÍÓÚáéíóúñÑ ]',
                ''
            ),
            ''
        )
    ) f
),

WITH filtered as (
    select *
    from tokenized
    where keyword <> ''
      and keyword not in (
        'de','la','que','el','y','a','en','un','ser','se','no','haber',
        'por','con','su','para','como','estar','tener','le','lo','lo','sí'
      )
)

select
  published_date,
  keyword,
  count(*) as conteo
from filtered
group by published_date, keyword