{{ config(materialized="view") }}

with raw_budget as (
        select * from {{ source("google_sheets", "budget") }}
    ),

    filtered_budget as (
        select
            md5(cast(_row as string)) as budget_id,  -- Verificar si _row existe
            product_id,
            quantity,
            to_char(month, 'YYYY-MM') as budget_date,  -- Extrae año-mes de la fecha
            convert_timezone('UTC', _fivetran_synced) as synced_utc  -- Ya es TIMESTAMP_TZ, solo convértelo
        from raw_budget
        where product_id is not null
    )

select * from filtered_budget
