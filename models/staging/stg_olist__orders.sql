WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'orders') }}
),

renamed AS (
    SELECT
        -- IDs
        order_id,
        customer_id,
        
        -- Timestamps
        CAST(order_purchase_timestamp AS TIMESTAMP) AS ordered_at,
        CAST(order_approved_at AS TIMESTAMP) AS approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_to_carrier_at,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_to_customer_at,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at,
        
        -- Status
        order_status
        
    FROM source
)

SELECT * FROM renamed