WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'order_payments') }}
),

renamed AS (
    SELECT
        -- IDs
        order_id,
        
        --payment sequence for payments with more than one method
        payment_sequential,
        payment_type,

        --Number of installments and value
        payment_installments,
        payment_value

    FROM source
)

SELECT * FROM renamed