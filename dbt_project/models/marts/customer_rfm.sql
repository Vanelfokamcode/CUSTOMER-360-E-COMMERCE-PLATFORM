-- models/marts/customer_rfm.sql
--
-- OBJECTIVE: RFM analysis for customer segmentation
-- OUTPUT: Each customer with R, F, M scores (1-5) and segment

{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'rfm']
    )
}}

WITH customers AS (
    
    SELECT * FROM {{ ref('dim_customers') }}

),

-- Calculate raw RFM metrics from transactions
transactions AS (
    
    SELECT
        customer_key,
        COUNT(*) as total_orders,
        SUM(amount) as total_spent,
        MAX(transaction_date) as last_order_date,
        MIN(transaction_date) as first_order_date
    FROM raw.transactions
    GROUP BY customer_key

),

-- Join with customer data
customer_metrics AS (

    SELECT
        c.customer_key,
        c.full_name,
        c.email,
        c.first_seen_date,
        
        -- RFM raw metrics
        COALESCE(t.total_orders, 0) as frequency_value,
        COALESCE(t.total_spent, 0) as monetary_value,
        COALESCE(
            CURRENT_DATE - t.last_order_date,
            CURRENT_DATE - c.first_seen_date
        ) as recency_days,
        
        t.last_order_date,
        t.first_order_date
        
    FROM customers c
    LEFT JOIN transactions t ON c.customer_key = t.customer_key

),

-- Calculate RFM scores (1-5) using quintiles
rfm_scores AS (

    SELECT
        *,
        
        -- RECENCY SCORE: Lower days = better (5)
        -- Use NTILE to split into 5 groups, then reverse (5-score)
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END as recency_score,
        
        -- FREQUENCY SCORE: More orders = better (5)
        NTILE(5) OVER (ORDER BY frequency_value) as frequency_score,
        
        -- MONETARY SCORE: More spent = better (5)
        NTILE(5) OVER (ORDER BY monetary_value) as monetary_score
        
    FROM customer_metrics

),

-- Create segments based on RFM scores
with_segments AS (

    SELECT
        *,
        
        -- Concatenate RFM scores for easy reference
        recency_score::TEXT || frequency_score::TEXT || monetary_score::TEXT as rfm_score,
        
        -- Segment logic
        CASE
            -- VIP: Recent, frequent, high spenders
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
            THEN 'VIP'
            
            -- Champions: High on all dimensions
            WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 3
            THEN 'Champion'
            
            -- Loyal: Frequent buyers, maybe not recent
            WHEN frequency_score >= 4 AND monetary_score >= 3
            THEN 'Loyal'
            
            -- At Risk: Were good, but haven't bought recently
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3
            THEN 'At Risk'
            
            -- Promising: Recent but low frequency/monetary
            WHEN recency_score >= 4 AND frequency_score <= 2
            THEN 'Promising'
            
            -- Need Attention: Medium on everything
            WHEN recency_score = 3 AND frequency_score = 3 AND monetary_score = 3
            THEN 'Need Attention'
            
            -- Lost: Haven't bought in long time, low value
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2
            THEN 'Lost'
            
            -- New: Recent first purchase
            WHEN frequency_value = 1 AND recency_score >= 4
            THEN 'New'
            
            ELSE 'Other'
        END as customer_segment,
        
        -- Calculate Customer Lifetime Value (simple version)
        monetary_value as ltv_actual,
        
        -- Predicted LTV (simple: extrapolate based on frequency)
        CASE 
            WHEN frequency_value > 0 
            THEN ROUND((monetary_value / frequency_value) * (frequency_value + 12), 2)
            ELSE 0
        END as ltv_predicted

    FROM rfm_scores

)

SELECT
    customer_key,
    full_name,
    email,
    
    -- RFM Metrics
    recency_days,
    frequency_value as total_orders,
    monetary_value as total_spent,
    
    -- RFM Scores
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    
    -- Segmentation
    customer_segment,
    
    -- Value metrics
    ltv_actual,
    ltv_predicted,
    ROUND(monetary_value / NULLIF(frequency_value, 0), 2) as avg_order_value,
    
    -- Dates
    first_seen_date,
    last_order_date,
    first_order_date,
    
    CURRENT_TIMESTAMP as calculated_at

FROM with_segments
