{{ config(materialized='view') }}

WITH products_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'products') }}
),

-- Filtrar productos válidos
filtered_products AS (
    SELECT
        product_id,
        price,
        name,
        inventory,
        _fivetran_synced
    FROM products_raw
    WHERE 
        product_id IS NOT NULL
        AND _fivetran_deleted = FALSE
),

-- Validar formato y rangos de valores
validated_products AS (
    SELECT
        product_id,
        price,
        name,
        inventory,
        _fivetran_synced,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _synced_utc
    FROM filtered_products
    WHERE 
        price >= 0
        AND inventory >= 0
),

-- Identificar duplicados basados en product_id y seleccionar solo la fila más reciente
ranked_products AS (
    SELECT
        product_id,
        price,
        name,
        inventory,
        _synced_utc,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _synced_utc DESC) AS row_num
    FROM validated_products
)

-- Seleccionar solo la fila más reciente para cada product_id
SELECT
    product_id,
    price,
    name,
    inventory
FROM ranked_products
WHERE row_num = 1
