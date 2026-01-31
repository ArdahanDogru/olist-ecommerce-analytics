WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'products') }}
),

renamed AS (
    SELECT
        -- IDs
        product_id,

        --About the product name/description
        product_category_name,
        product_name_lenght,
        product_description_lenght,


        --Product Details
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm



    FROM source
)

SELECT * FROM renamed