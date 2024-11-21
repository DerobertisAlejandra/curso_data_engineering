{{ config(materialized="view") }}

with
    events_raw as (select * from {{ source("sql_server_dbo", "events") }}),

    -- Filtrar eventos válidos
    filtered_events as (
        select
            event_id,
            page_url,
            event_type,
            user_id,
            product_id,
            session_id,
            created_at,
            order_id,
            _fivetran_synced
        from events_raw
        where event_id is not null and _fivetran_deleted = false
    ),

    -- Validar formato y rangos de valores, manejando casos específicos de event_type
    validated_events as (
        select
            event_id,
            page_url,
            event_type,
            user_id,
            case
                when event_type in ('checkout', 'packages_shipped')
                then null
                else product_id
            end as product_id,
            session_id,
            convert_timezone('UTC', created_at) as created_at_utc,
            case
                when event_type in ('add_to_cart', 'page_view') then null else order_id
            end as order_id,
            convert_timezone('UTC', _fivetran_synced) as _fivetran_synced_utc
        from filtered_events
        where page_url is not null and event_type is not null
    ),

    -- Identificar duplicados basados en event_id y seleccionar solo la fila más
    -- reciente
    ranked_events as (
        select
            *,
            row_number() over (
                partition by event_id order by _fivetran_synced_utc desc  -- Usar _fivetran_synced para tener la selección del dato más reciente basada en la sincronización
            ) as row_num
        from validated_events
    ),

    -- Seleccionar solo la fila más reciente para cada event_id
    final_events as (
        select
            event_id,
            page_url,
            event_type,
            user_id,
            product_id,
            session_id,
            created_at_utc as created_at,
            order_id
        from ranked_events
        where row_num = 1
    )

-- Unir con tablas de dimensiones
select
    e.event_id,
    e.page_url,
    e.event_type,
    e.user_id,
    u.first_name,
    u.last_name,
    p.name as product_name,
    e.session_id,
    e.created_at,
    e.order_id,
    o.order_total
from final_events e
left join
    alumno30_dev_silver_db.alumno30.stg_sql_server_dbo_users u on e.user_id = u.user_id
left join
    alumno30_dev_silver_db.alumno30.stg_sql_server_dbo_products p
    on e.product_id = p.product_id
left join
    alumno30_dev_silver_db.alumno30.stg_sql_server_dbo_orders o
    on e.order_id = o.order_id
