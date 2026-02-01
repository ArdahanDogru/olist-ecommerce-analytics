WITH dataset_bounds AS (
    -- Define the valid data period from EDA findings
    SELECT 
        DATE('2017-01-01') AS data_start,
        DATE('2018-08-31') AS data_end
),

seller_first_sale AS (
    SELECT 
        oi.seller_id,
        MIN(o.ordered_at) AS first_sale_at,
        DATE_TRUNC(DATE(MIN(o.ordered_at)), MONTH) AS cohort_month
    FROM {{ ref('stg_olist__order_items') }} oi
    JOIN {{ ref('stg_olist__orders') }} o
        ON oi.order_id = o.order_id
    CROSS JOIN dataset_bounds db
    WHERE o.ordered_at IS NOT NULL
        AND DATE(o.ordered_at) >= db.data_start
        AND DATE(o.ordered_at) <= db.data_end
    GROUP BY oi.seller_id
),

seller_activity AS (
    SELECT 
        oi.seller_id,
        DATE_TRUNC(DATE(o.ordered_at), MONTH) AS activity_month,
        COUNT(DISTINCT oi.order_id) AS orders_in_month,
        SUM(oi.price) AS revenue_in_month
    FROM {{ ref('stg_olist__order_items') }} oi
    JOIN {{ ref('stg_olist__orders') }} o
        ON oi.order_id = o.order_id
    CROSS JOIN dataset_bounds db
    WHERE o.order_status = 'delivered'
        AND o.ordered_at IS NOT NULL
        AND DATE(o.ordered_at) >= db.data_start
        AND DATE(o.ordered_at) <= db.data_end
    GROUP BY oi.seller_id, activity_month
),

seller_metrics AS (
    SELECT 
        sfs.seller_id,
        sfs.first_sale_at,
        sfs.cohort_month,
        
        -- Lifetime activity
        COUNT(DISTINCT sa.activity_month) AS active_months,
        MAX(sa.activity_month) AS last_activity_month,
        SUM(sa.orders_in_month) AS total_orders,
        ROUND(SUM(sa.revenue_in_month), 2) AS total_revenue,
        
        -- Retention flags
        CASE 
            WHEN MAX(sa.activity_month) >= DATE_ADD(sfs.cohort_month, INTERVAL 3 MONTH) 
            THEN 1 ELSE 0 
        END AS retained_3_months,
        
        CASE 
            WHEN MAX(sa.activity_month) >= DATE_ADD(sfs.cohort_month, INTERVAL 6 MONTH) 
            THEN 1 ELSE 0 
        END AS retained_6_months,
        
        -- FIXED: Calculate recency from END of dataset, not current date!
        DATE_DIFF(
            (SELECT data_end FROM dataset_bounds), 
            DATE(MAX(sa.activity_month)), 
            MONTH
        ) AS months_since_last_sale
        
    FROM seller_first_sale sfs
    LEFT JOIN seller_activity sa
        ON sfs.seller_id = sa.seller_id
    GROUP BY sfs.seller_id, sfs.first_sale_at, sfs.cohort_month
),

final AS (
    SELECT 
        sm.*,
        s.seller_state,
        s.seller_city,
        
        -- FIXED: Health status now uses dataset end date as reference
        CASE 
            WHEN sm.months_since_last_sale <= 1 THEN 'Active'
            WHEN sm.months_since_last_sale <= 3 THEN 'At Risk'
            ELSE 'Churned'
        END AS merchant_health_status,
        
        ROUND(sm.total_revenue / NULLIF(sm.active_months, 0), 2) AS avg_monthly_revenue,
        ROUND(sm.total_orders / NULLIF(sm.active_months, 0), 2) AS avg_monthly_orders
        
    FROM seller_metrics sm
    JOIN {{ ref('stg_olist__sellers') }} s
        ON sm.seller_id = s.seller_id
)

SELECT * FROM final