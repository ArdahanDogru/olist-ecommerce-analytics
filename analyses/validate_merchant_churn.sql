-- Validate that mart matches EDA findings
SELECT 
    COUNT(*) as total_merchants,
    
    -- Overall retention rates
    ROUND(100.0 * SUM(retained_3_months) / COUNT(*), 1) as retention_3mo_pct,
    ROUND(100.0 * SUM(retained_6_months) / COUNT(*), 1) as retention_6mo_pct,
    
    -- Health status breakdown
    SUM(CASE WHEN merchant_health_status = 'Active' THEN 1 ELSE 0 END) as active_count,
    SUM(CASE WHEN merchant_health_status = 'At Risk' THEN 1 ELSE 0 END) as at_risk_count,
    SUM(CASE WHEN merchant_health_status = 'Churned' THEN 1 ELSE 0 END) as churned_count
    
FROM {{ ref('fct_merchant_performance') }}
WHERE cohort_month < '2018-03-01'  -- Only cohorts with 6 months to track