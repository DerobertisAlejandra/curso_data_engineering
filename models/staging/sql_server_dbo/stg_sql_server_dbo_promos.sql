{{ config(materialized='view') }}

WITH promos_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'promos') }}
),

-- Filtrar promociones válidas
filtered_promos AS (
    SELECT
        promo_id,
        discount,
        status,
        _fivetran_synced
    FROM promos_raw
    WHERE 
        promo_id IS NOT NULL
        AND _fivetran_deleted = FALSE
),

-- Validar formato y rangos de valores
validated_promos AS (
    SELECT
        promo_id,
        discount,
        status,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
    FROM filtered_promos
    WHERE 
        discount >= 0
        AND status IS NOT NULL
),

-- Identificar duplicados basados en promo_id y seleccionar solo la fila más reciente
ranked_promos AS (
    SELECT
        promo_id,
        discount,
        status,
        _fivetran_synced_utc,
        ROW_NUMBER() OVER (PARTITION BY promo_id ORDER BY _fivetran_synced_utc DESC) AS row_num   -- Usar _fivetran_synced para tener la selección del dato más reciente basada en la sincronización.
    FROM validated_promos
)

-- Seleccionar solo la fila más reciente para cada promo_id
SELECT
    promo_id,
    discount,
    status,
    _fivetran_synced_utc AS synced_timestamp
FROM ranked_promos
WHERE row_num = 1


