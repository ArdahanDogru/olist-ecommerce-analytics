WITH dataset_bounds AS (
    -- Define the valid data period from EDA findings
    SELECT 
        DATE('2017-01-01') AS data_start,
        DATE('2018-08-31') AS data_end
),

order_details AS(
    SELECT o.order_id, c.customer_unique_id, o.ordered_at, o.delivered_to_customer_at, o.estimated_delivery_at, 
        DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) AS delivery_delay_days,
        CAST(CASE WHEN DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) > 3 THEN 0 ELSE 1 END AS STRING) AS delivery_on_time,
        CASE
            WHEN DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) < -7 THEN "Early"
            WHEN DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) <= 3 THEN "On Time"
            WHEN DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) <= 10 THEN "Slightly Late"
            ELSE "Very Late" 
        END AS delivery_status,
        c.customer_city, c.customer_state, 
        DATE_TRUNC(DATE(o.ordered_at), YEAR) AS order_year, 
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS order_month,
        DATE_TRUNC(DATE(o.ordered_at), QUARTER) AS order_quarter,
        EXTRACT(DAYOFWEEK FROM o.ordered_at) AS day_of_week
    FROM {{ ref('stg_olist__orders') }} o 
    JOIN {{ ref('stg_olist__customers') }} c ON o.customer_id = c.customer_id
    CROSS JOIN dataset_bounds db
    WHERE DATE(o.ordered_at) >= db.data_start AND DATE(o.ordered_at) <= db.data_end AND o.order_status = 'delivered' 
        AND o.delivered_to_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
),

seller_details AS(
    SELECT o.order_id, MAX(s.seller_id) AS seller_id, MAX(s.seller_city) AS seller_city, MAX(s.seller_state) AS seller_state,
        SUM(oi.price) AS order_value,
        COUNT(*) AS num_items,
        CASE 
            WHEN COUNT(*) = 1 THEN MAX(t.product_category_name_english)
            ELSE NULL
        END AS product_category
    FROM {{ ref('stg_olist__orders') }} o 
    JOIN {{ ref('stg_olist__order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_olist__sellers') }} s ON oi.seller_id = s.seller_id
    JOIN {{ ref('stg_olist__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_olist__product_category_name_translation') }} t ON p.product_category_name = t.product_category_name
    CROSS JOIN dataset_bounds db
    WHERE DATE(o.ordered_at) >= db.data_start AND DATE(o.ordered_at) <= db.data_end AND o.order_status = 'delivered' 
        AND o.delivered_to_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
    GROUP BY o.order_id
),
final AS(
    SELECT od.*, sd.* EXCEPT (order_id),
    CAST(CASE WHEN od.customer_state = sd.seller_state THEN 1 ELSE 0 END AS STRING) AS same_state
    FROM order_details od
    JOIN seller_details sd ON od.order_id = sd.order_id
)
SELECT * FROM final