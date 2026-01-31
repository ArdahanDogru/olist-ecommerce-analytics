WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'geolocation') }}
),

renamed AS (
    SELECT
        -- Zipcode
        geolocation_zip_code_prefix,
        
        --Coordinates
        geolocation_lat,
        geolocation_lng,

        --City-State
        geolocation_city, 
        geolocation_state
        
    FROM source
)

SELECT * FROM renamed