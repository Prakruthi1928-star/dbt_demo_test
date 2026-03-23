{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
) }}

SELECT distinct
    store_id,
    store_name,
    city,
    state,
    region,
    created_at,
    updated_at
FROM {{ ref('stg_stores') }}
QUALIFY ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY updated_at DESC NULLS LAST
        ) = 1