{{ config(materialized='view') }}

WITH order_items_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'order_items') }}
),

-- Filtrar artículos de pedido válidos
filtered_order_items AS (
    SELECT
        order_id,
        product_id,
        quantity,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
    FROM order_items_raw
    WHERE 
        order_id IS NOT NULL
        AND product_id IS NOT NULL
        AND quantity >= 0
        AND _fivetran_deleted = FALSE
),

-- Identificar duplicados basados en order_id y product_id y seleccionar solo la fila más reciente
ranked_order_items AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, product_id
            ORDER BY 
                _fivetran_synced_utc DESC
        ) AS row_num
    FROM filtered_order_items
)

-- Seleccionar solo la fila más reciente para cada combinación de order_id y product_id
, final_order_items AS (
    SELECT
        order_id,
        product_id,
        quantity,
        _fivetran_synced_utc AS synced_timestamp
    FROM ranked_order_items
    WHERE row_num = 1
)

-- Unir con tablas de dimensiones
SELECT
    oi.order_id,
    oi.product_id,
    p.name AS product_name,
    oi.quantity,
    oi.synced_timestamp
FROM final_order_items oi
LEFT JOIN ALUMNO30_DEV_SILVER_DB.alumno30.stg_sql_server_dbo_products p ON oi.product_id = p.product_id

