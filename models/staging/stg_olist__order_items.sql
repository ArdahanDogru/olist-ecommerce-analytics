WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'order_items') }}
),

renamed AS (
    SELECT
        --IDs
        order_id,
        order_item_id,
        product_id,
        seller_id,

        --Seller warning
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,

        --item price
        price,

        --if an order has more than one item -> freight value split between items
        freight_value
        
    FROM source
)

SELECT * FROM renamed