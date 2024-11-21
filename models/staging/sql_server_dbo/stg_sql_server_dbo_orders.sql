{{ config(materialized='view') }}

WITH orders_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'orders') }}
),

-- Filtrar pedidos válidos
filtered_orders AS (
    SELECT
        order_id,
        COALESCE(shipping_service, 'Unknown') AS shipping_service,
        shipping_cost,
        address_id,
        created_at,
        NULLIF(promo_id, '') AS promo_id,  -- Manejar vacíos como nulos
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        tracking_id,
        status,
        _fivetran_synced
    FROM orders_raw
    WHERE 
        order_id IS NOT NULL
        AND _fivetran_deleted = FALSE
),

-- Validar formato y rangos de valores, manejando casos específicos
validated_orders AS (
    SELECT
        order_id,
        shipping_service,
        shipping_cost,
        address_id,
        CONVERT_TIMEZONE('UTC', created_at) AS created_at_utc,
        promo_id,
        CASE
            WHEN shipping_service IS NULL THEN NULL
            ELSE CONVERT_TIMEZONE('UTC', estimated_delivery_at)
        END AS estimated_delivery_at_utc,
        order_cost,
        user_id,
        order_total,
        CASE
            WHEN shipping_service IS NULL THEN NULL
            ELSE CONVERT_TIMEZONE('UTC', delivered_at)
        END AS delivered_at_utc,
        CASE
            WHEN shipping_service IS NULL THEN NULL
            ELSE tracking_id
        END AS tracking_id,
        status,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
    FROM filtered_orders
    WHERE 
        shipping_service IS NOT NULL OR status = 'preparing'
),

-- Validar relación de valores calculados
validated_totals AS (
    SELECT
        *,
        (shipping_cost + order_cost - COALESCE(promo_id, 0)) = order_total AS is_total_valid
    FROM validated_orders
),

-- Identificar duplicados basados en order_id y seleccionar solo la fila más reciente
ranked_orders AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY _fivetran_synced_utc DESC
        ) AS row_num
    FROM validated_totals
)

-- Seleccionar solo la fila más reciente para cada order_id
SELECT
    order_id,
    shipping_service,
    shipping_cost,
    address_id,
    created_at_utc AS created_at,
    promo_id,
    estimated_delivery_at_utc AS estimated_delivery_at,
    order_cost,
    user_id,
    order_total,
    delivered_at_utc AS delivered_at,
    tracking_id,
    status
FROM ranked_orders
WHERE row_num = 1
AND is_total_valid
