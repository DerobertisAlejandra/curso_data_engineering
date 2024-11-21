{{ config(materialized='view') }}

WITH users_raw AS (
    SELECT * FROM {{ source('sql_server_dbo', 'users') }}
),

-- Filtrar usuarios válidos
filtered_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        created_at,
        updated_at,
        _fivetran_synced
    FROM users_raw
    WHERE 
        user_id IS NOT NULL
        AND _FIVETRAN_DELETED = FALSE
),

-- Validar formato y rangos de valores
validated_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        CONVERT_TIMEZONE('UTC', created_at) AS created_at_utc,
        CONVERT_TIMEZONE('UTC', updated_at) AS updated_at_utc,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS _fivetran_synced_utc
    FROM filtered_users
    WHERE 
        email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'  -- Validar formato de email
        AND phone_number IS NOT NULL
),

-- Identificar duplicados basados en user_id y seleccionar solo la fila más reciente
ranked_users AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY 
                updated_at_utc DESC,  -- Prioridad a updated_at para tener una mayor precision de la actualizacion en el origen 
                _fivetran_synced_utc DESC  -- Luego _fivetran_synced. me aseguro de tener la seleccion del dato mas reciente basada en la sincronizacion. 
        ) AS row_num
    FROM validated_users
)

SELECT
    user_id,
    first_name,
    last_name,
    email,
    phone_number,
    created_at_utc AS created_at,
    updated_at_utc AS updated_at
FROM ranked_users
WHERE row_num = 1