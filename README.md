# Olist E-Commerce Analytics Pipeline

Analyzing merchant and customer retention in a 100K+ order Brazilian e-commerce marketplace using dbt, BigQuery, and Python.

## Key Finding

A single delayed first delivery reduces repeat purchase probability by 23.1% 
(statistically validated, p < 0.05), making delivery experience the primary driver 
of customer churn over pricing and product selection.

## Project Overview

This project builds a complete analytics pipeline from raw data to product 
recommendations:

1. **Data Modeling**: 3 dimensional mart models in dbt (merchant retention, 
   customer cohorts, delivery impact) transforming 8 raw tables with comprehensive 
   testing
2. **Analysis**: Cohort analysis, statistical testing, and geographic risk 
   assessment across 3K merchants and 75K customers
3. **Recommendations**: 3 data-backed product recommendations: delivery-first 
   merchant onboarding, corridor-specific logistics optimization, and a merchant 
   delivery health score

## Tech Stack

- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt (data build tool)
- **Analysis:** Python (pandas, plotly, matplotlib, scipy)
- **Version Control:** Git/GitHub

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 
— 100K+ orders, Jan 2017 - Aug 2018
