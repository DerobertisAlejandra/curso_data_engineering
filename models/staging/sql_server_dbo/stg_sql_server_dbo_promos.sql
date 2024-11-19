{{ config(materialized="view") }}

with
    promos_raw as (select * from {{ source("sql_server_dbo", "promos") }}),

    filterd_promos as (
        select
            md5(promo_id) as promo_id,
            discount as discount_value,
            status as promo_status,
            convert_timezone('UTC', _fivetran_synced) as sinced_timestamp_utc
        from promos_raw
    )

select *
from promos_raw
