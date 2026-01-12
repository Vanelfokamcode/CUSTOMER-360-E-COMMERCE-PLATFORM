-- models/marts/customer_metrics_advanced.sql
--
-- OBJECTIVE: Advanced per-customer metrics
-- INCLUDES: Purchase patterns, trends, predictions

{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'advanced']
    )
}}

WITH customers AS (
    
    SELECT * FROM {{ ref('dim_customers') }}

),

transactions AS (
    
    SELECT
        customer_key,
        transaction_date,
        amount
    FROM raw.transactions

),

-- Calculate detailed transaction metrics
customer_transaction_metrics AS (

    SELECT
        t.customer_key,
        
        -- Counts
        COUNT(*) as total_transactions,
        
        -- Monetary
        SUM(t.amount) as lifetime_value,
        AVG(t.amount) as avg_transaction_value,
        MIN(t.amount) as min_transaction,
        MAX(t.amount) as max_transaction,
        STDDEV(t.amount) as transaction_value_stddev,
        
        -- Dates
        MIN(t.transaction_date) as first_transaction_date,
        MAX(t.transaction_date) as last_transaction_date,
        
        -- Customer lifetime in days
        MAX(t.transaction_date) - MIN(t.transaction_date) as customer_lifetime_days,
        
        -- Average days between purchases
        CASE 
            WHEN COUNT(*) > 1 
            THEN (MAX(t.transaction_date) - MIN(t.transaction_date)) / (COUNT(*) - 1)
            ELSE NULL
        END as avg_days_between_purchases,
        
        -- Days since last purchase
        CURRENT_DATE - MAX(t.transaction_date) as days_since_last_purchase

    FROM transactions t
    GROUP BY t.customer_key

),

-- Calculate trends (are they buying more or less over time?)
purchase_trends AS (

    SELECT
        customer_key,
        
        -- Compare first half vs second half of purchases
        AVG(CASE WHEN row_num <= total_txns/2 THEN amount END) as first_half_avg,
        AVG(CASE WHEN row_num > total_txns/2 THEN amount END) as second_half_avg
        
    FROM (
        SELECT
            customer_key,
            amount,
            ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY transaction_date) as row_num,
            COUNT(*) OVER (PARTITION BY customer_key) as total_txns
        FROM transactions
    ) ranked
    GROUP BY customer_key

),

-- Join everything
final AS (

    SELECT
        c.customer_key,
        c.full_name,
        c.email,
        
        -- Transaction metrics
        COALESCE(tm.total_transactions, 0) as total_transactions,
        COALESCE(tm.lifetime_value, 0) as lifetime_value,
        COALESCE(tm.avg_transaction_value, 0) as avg_transaction_value,
        tm.min_transaction,
        tm.max_transaction,
        COALESCE(tm.transaction_value_stddev, 0) as spending_consistency,
        
        -- Time metrics
        tm.first_transaction_date,
        tm.last_transaction_date,
        COALESCE(tm.customer_lifetime_days, 0) as customer_lifetime_days,
        COALESCE(tm.avg_days_between_purchases, 0) as avg_days_between_purchases,
        COALESCE(tm.days_since_last_purchase, 999) as days_since_last_purchase,
        
        -- Purchase frequency (purchases per month)
        CASE 
            WHEN tm.customer_lifetime_days > 0 
            THEN ROUND(tm.total_transactions * 30.0 / tm.customer_lifetime_days, 2)
            ELSE 0
        END as purchases_per_month,
        
        -- Revenue per day active
        CASE 
            WHEN tm.customer_lifetime_days > 0 
            THEN ROUND(tm.lifetime_value / tm.customer_lifetime_days, 2)
            ELSE 0
        END as revenue_per_day,
        
        -- Trend indicator
        CASE
            WHEN pt.second_half_avg > pt.first_half_avg * 1.2 THEN 'Growing'
            WHEN pt.second_half_avg < pt.first_half_avg * 0.8 THEN 'Declining'
            ELSE 'Stable'
        END as spending_trend,
        
        -- Next purchase prediction (simple: avg days between + last purchase date)
        CASE 
            WHEN tm.avg_days_between_purchases > 0 
            THEN tm.last_transaction_date + (tm.avg_days_between_purchases || ' days')::INTERVAL
            ELSE NULL
        END as predicted_next_purchase_date,
        
        -- Is customer overdue?
        CASE
            WHEN tm.days_since_last_purchase > tm.avg_days_between_purchases * 1.5 
            THEN TRUE
            ELSE FALSE
        END as is_overdue,
        
        CURRENT_TIMESTAMP as calculated_at

    FROM customers c
    LEFT JOIN customer_transaction_metrics tm ON c.customer_key = tm.customer_key
    LEFT JOIN purchase_trends pt ON c.customer_key = pt.customer_key

)

SELECT * FROM final
