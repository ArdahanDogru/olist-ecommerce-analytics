WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'customers') }}
),

renamed AS (
    SELECT
        -- IDs
        customer_id,  -- Links to orders table
        customer_unique_id,  -- The actual customer
        
        -- Location
        customer_zip_code_prefix,
        customer_city,
        customer_state
        
    FROM source
)

SELECT * FROM renamed