# Olist E-Commerce Analytics Pipeline

End-to-end analytics project analyzing Brazilian e-commerce marketplace data using dbt, BigQuery, and Python.

## 🎯 Project Overview

Portfolio project demonstrating production-grade analytics engineering: dimensional modeling, data quality testing, and business insight generation for a marketplace platform.

**Goal:** Identify retention drivers for merchants and customers through cohort analysis and statistical testing.

## 🛠️ Tech Stack

- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt (data build tool)
- **Analysis:** Python (pandas, plotly, matplotlib, scipy)
- **Version Control:** Git/GitHub
- **Environment:** Conda

## 📊 Current Status

✅ **Staging Layer:** 9 clean, tested source models
✅ **EDA Notebook:** Key findings validated (50% merchant churn, 24% delivery impact)
✅ **Dimensional Marts:** 
  - `fct_merchant_performance` - Cohort retention tracking (COMPLETE)
  - `fct_customer_cohorts` - Coming soon
  - `fct_delivery_impact` - Coming soon
