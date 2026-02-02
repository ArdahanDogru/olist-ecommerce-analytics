-- Valid data period from EDA findings
WITH dataset_bounds AS(
    SELECT 
        DATE('2017-01-01') AS data_start,
        DATE('2018-08-31') AS data_end
),

-- customer lifetime information
customer_months AS(
    SELECT c.customer_unique_id, MAX(c.customer_state) AS customer_state, MAX(c.customer_city) AS customer_city, 
        COUNT(DISTINCT DATE_TRUNC(DATE(o.ordered_at), MONTH)) as active_months,
        ROUND(SUM(op.payment_value), 2) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders,
        CAST(CASE WHEN COUNT(DISTINCT o.order_id) > 1 THEN 1 ELSE 0 END AS STRING) AS is_repeat_customer,
        ROUND(SUM(op.payment_value) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
    FROM {{ ref('stg_olist__orders') }} o 
    LEFT JOIN {{ ref('stg_olist__order_payments') }} op ON o.order_id = op.order_id
    JOIN {{ ref('stg_olist__customers') }} c ON o.customer_id = c.customer_id
    CROSS JOIN dataset_bounds db
    WHERE DATE(o.ordered_at) >= db.data_start AND DATE(o.ordered_at) <= db.data_end AND o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

-- customer first month order information
customer_first_order AS(
    SELECT c.customer_unique_id,
        MIN(o.ordered_at) AS first_order_at,
        DATE_TRUNC(DATE(MIN(o.ordered_at)), MONTH) AS cohort_month
    FROM {{ ref('stg_olist__orders') }} o 
    JOIN {{ ref('stg_olist__customers') }} c ON o.customer_id = c.customer_id
    CROSS JOIN dataset_bounds db
    WHERE DATE(o.ordered_at) >= db.data_start AND DATE(o.ordered_at) <= db.data_end
    GROUP BY c.customer_unique_id
),

customer_first_order_details_all AS(
    SELECT cfo.customer_unique_id, cfo.first_order_at, cfo.cohort_month, o.order_id AS first_order_id,
        CAST(CASE WHEN DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) > 3 THEN 0 ELSE 1 END AS STRING) AS first_order_delivered_on_time,
        DATE_DIFF(o.delivered_to_customer_at, o.estimated_delivery_at, DAY) AS first_order_delivery_delay_days,
        ROW_NUMBER() OVER (PARTITION BY cfo.customer_unique_id ORDER BY o.order_id) as rn
    FROM customer_first_order cfo
    JOIN {{ ref('stg_olist__customers') }} c ON cfo.customer_unique_id = c.customer_unique_id
    JOIN {{ ref('stg_olist__orders') }} o ON c.customer_id = o.customer_id AND cfo.first_order_at = o.ordered_at
    WHERE o.order_status = 'delivered' AND o.delivered_to_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
),

customer_first_order_details AS(
    SELECT * EXCEPT (rn) FROM customer_first_order_details_all WHERE rn = 1
),

final AS(
    SELECT cfod.*, cm.* EXCEPT (customer_unique_id),
        DATE_DIFF((SELECT data_end FROM dataset_bounds), DATE(cohort_month), MONTH) AS months_since_first_order
    FROM customer_first_order_details cfod
    JOIN customer_months cm ON cfod.customer_unique_id = cm.customer_unique_id
    WHERE cfod.cohort_month < '2018-06-01'
)

SELECT * FROM final