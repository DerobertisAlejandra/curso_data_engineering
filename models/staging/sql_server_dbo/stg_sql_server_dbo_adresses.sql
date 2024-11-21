{{ config(materialized='view') }}

WITH addresses_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'addresses') }}
),

-- Filtrar direcciones válidas
filtered_addresses AS (
    SELECT
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_synced
    FROM addresses_raw
    WHERE 
        address_id IS NOT NULL
        AND _fivetran_deleted = FALSE
),

-- Validar formato y rangos de valores
validated_addresses AS (
    SELECT
        address_id,
        zipcode,
        country,
        address,
        state,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
    FROM filtered_addresses
    WHERE 
        zipcode IS NOT NULL
        AND country IS NOT NULL
        AND address IS NOT NULL
        AND state IS NOT NULL 
),

-- Identificar duplicados basados en address_id y seleccionar solo la fila más reciente
ranked_addresses AS (
    SELECT
        address_id,
        zipcode,
        country,
        address,
        state,
        _fivetran_synced_utc,
        ROW_NUMBER() OVER (
            PARTITION BY address_id 
            ORDER BY 
                _fivetran_synced_utc DESC  
        ) AS row_num
    FROM validated_addresses
)

-- Seleccionar solo la fila más reciente para cada address_id
SELECT
    address_id,
    zipcode,
    country,
    address,
    state,
    _fivetran_synced_utc AS synced_timestamp
FROM ranked_addresses
WHERE row_num = 1

