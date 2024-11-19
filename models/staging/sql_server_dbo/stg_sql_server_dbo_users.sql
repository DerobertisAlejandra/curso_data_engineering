{{ config(materialized="view") }}

with
    users_raw as (select * from {{ source("sql_server_dbo", "users") }})

    filtered_users as (
        select
            user_id,
            updated_at,
            address_id,
            last_name,
            created_at,
            phone_number,
            total_orders,
            first_name,
            email,
            fivetran_deleted,
            fivetran_synced as date_load
        from users_raw
    )

select *
from filtered_users
