WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'sellers') }}
),

renamed AS (
    SELECT
        -- IDs
        seller_id,
        
        --Location
        seller_zip_code_prefix,
        seller_city,
        seller_state
        
    FROM source
)

SELECT * FROM renamed