-- models/marts/cohort_retention.sql
--
-- OBJECTIVE: Cohort retention analysis
-- SHOWS: % of customers who return each month after first purchase

{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'cohort']
    )
}}

WITH transactions AS (
    
    SELECT
        customer_key,
        transaction_date,
        amount
    FROM raw.transactions

),

-- Identify first purchase for each customer
customer_cohorts AS (

    SELECT
        customer_key,
        DATE_TRUNC('month', MIN(transaction_date)) as cohort_month,
        MIN(transaction_date) as first_purchase_date
    FROM transactions
    GROUP BY customer_key

),

-- Get all transactions with cohort info
transactions_with_cohort AS (

    SELECT
        t.customer_key,
        t.transaction_date,
        t.amount,
        c.cohort_month,
        c.first_purchase_date,
        
        -- Calculate months since first purchase
        EXTRACT(YEAR FROM AGE(t.transaction_date, c.first_purchase_date)) * 12 +
        EXTRACT(MONTH FROM AGE(t.transaction_date, c.first_purchase_date)) as months_since_first
        
    FROM transactions t
    JOIN customer_cohorts c ON t.customer_key = c.customer_key

),

-- Count active customers per cohort per month
cohort_activity AS (

    SELECT
        cohort_month,
        months_since_first,
        COUNT(DISTINCT customer_key) as active_customers,
        SUM(amount) as cohort_revenue
    FROM transactions_with_cohort
    GROUP BY cohort_month, months_since_first

),

-- Get cohort sizes (month 0 = all customers who joined that month)
cohort_sizes AS (

    SELECT
        cohort_month,
        COUNT(DISTINCT customer_key) as cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month

),

-- Calculate retention rates
final AS (

    SELECT
        ca.cohort_month,
        cs.cohort_size,
        ca.months_since_first,
        ca.active_customers,
        ca.cohort_revenue,
        
        -- Retention rate
        ROUND(100.0 * ca.active_customers / cs.cohort_size, 2) as retention_rate,
        
        -- Revenue per customer in cohort
        ROUND(ca.cohort_revenue / cs.cohort_size, 2) as revenue_per_customer

    FROM cohort_activity ca
    JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month

)

SELECT * FROM final
ORDER BY cohort_month, months_since_first
