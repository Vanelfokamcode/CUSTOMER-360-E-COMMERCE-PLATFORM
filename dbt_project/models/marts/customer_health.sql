-- models/marts/customer_health.sql
--
-- OBJECTIVE: Customer Health Score (0-100) for churn prediction
-- COMBINES: Recency, frequency, monetary, engagement trends

{{
    config(
        materialized='table',
        tags=['marts', 'analytics', 'health']
    )
}}

WITH rfm_data AS (
    
    SELECT * FROM {{ ref('customer_rfm') }}

),

-- Calculate health score components
health_components AS (

    SELECT
        customer_key,
        full_name,
        email,
        customer_segment,
        
        -- Raw metrics
        recency_days,
        total_orders,
        total_spent,
        avg_order_value,
        
        -- COMPONENT 1: Recency Health (0-30 points)
        -- Recent activity = healthy
        CASE
            WHEN recency_days <= 30 THEN 30
            WHEN recency_days <= 60 THEN 25
            WHEN recency_days <= 90 THEN 20
            WHEN recency_days <= 180 THEN 10
            ELSE 0
        END as recency_health,
        
        -- COMPONENT 2: Frequency Health (0-25 points)
        -- More orders = more engaged
        CASE
            WHEN total_orders >= 10 THEN 25
            WHEN total_orders >= 7 THEN 20
            WHEN total_orders >= 5 THEN 15
            WHEN total_orders >= 3 THEN 10
            WHEN total_orders >= 1 THEN 5
            ELSE 0
        END as frequency_health,
        
        -- COMPONENT 3: Monetary Health (0-25 points)
        -- High spenders = valuable
        CASE
            WHEN total_spent >= 1000 THEN 25
            WHEN total_spent >= 500 THEN 20
            WHEN total_spent >= 250 THEN 15
            WHEN total_spent >= 100 THEN 10
            WHEN total_spent >= 50 THEN 5
            ELSE 0
        END as monetary_health,
        
        -- COMPONENT 4: AOV Health (0-10 points)
        -- High AOV = quality customer
        CASE
            WHEN avg_order_value >= 150 THEN 10
            WHEN avg_order_value >= 100 THEN 7
            WHEN avg_order_value >= 50 THEN 5
            ELSE 2
        END as aov_health,
        
        -- COMPONENT 5: Segment Bonus (0-10 points)
        CASE customer_segment
            WHEN 'VIP' THEN 10
            WHEN 'Champion' THEN 8
            WHEN 'Loyal' THEN 6
            WHEN 'Promising' THEN 4
            WHEN 'At Risk' THEN -5  -- Penalty
            WHEN 'Lost' THEN -10    -- Big penalty
            ELSE 0
        END as segment_bonus

    FROM rfm_data

),

-- Calculate final health score
with_health_score AS (

    SELECT
        *,
        
        -- Total health score (capped at 100)
        LEAST(
            recency_health + 
            frequency_health + 
            monetary_health + 
            aov_health + 
            segment_bonus,
            100
        ) as health_score,
        
        -- Health status
        CASE
            WHEN (recency_health + frequency_health + monetary_health + aov_health + segment_bonus) >= 75 THEN 'Excellent'
            WHEN (recency_health + frequency_health + monetary_health + aov_health + segment_bonus) >= 60 THEN 'Good'
            WHEN (recency_health + frequency_health + monetary_health + aov_health + segment_bonus) >= 40 THEN 'Fair'
            WHEN (recency_health + frequency_health + monetary_health + aov_health + segment_bonus) >= 20 THEN 'Poor'
            ELSE 'Critical'
        END as health_status,
        
        -- Churn risk flag
        CASE
            WHEN recency_days > 180 AND total_orders <= 2 THEN TRUE
            WHEN (recency_health + frequency_health + monetary_health) < 30 THEN TRUE
            ELSE FALSE
        END as high_churn_risk

    FROM health_components

)

SELECT
    customer_key,
    full_name,
    email,
    customer_segment,
    
    -- Health metrics
    health_score,
    health_status,
    high_churn_risk,
    
    -- Components breakdown
    recency_health,
    frequency_health,
    monetary_health,
    aov_health,
    segment_bonus,
    
    -- Raw data
    recency_days,
    total_orders,
    total_spent,
    avg_order_value,
    
    CURRENT_TIMESTAMP as calculated_at

FROM with_health_score

