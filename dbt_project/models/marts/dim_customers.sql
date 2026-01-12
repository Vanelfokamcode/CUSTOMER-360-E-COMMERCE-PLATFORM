-- models/marts/dim_customers.sql
-- models/marts/dim_customers.sql
--
-- OBJECTIVE: Final customer dimension - ONE row per unique person
-- Uses exact match only (Phase 1 identity resolution)

{{
    config(
        materialized='table',
        tags=['marts', 'dimensions']
    )
}}

WITH deduped AS (
    
    SELECT * FROM {{ ref('int_customer_deduped') }}

),

with_metrics AS (

    SELECT
        customer_key,
        original_customer_id,
        
        -- Demographics
        email_clean AS email,
        first_name_clean AS first_name,
        last_name_clean AS last_name,
        CONCAT(first_name_clean, ' ', last_name_clean) AS full_name,
        phone_clean AS phone,
        address,
        city,
        country,
        
        -- Dates
        created_at_parsed AS first_seen_date,
        created_at_parsed AS account_created_date,
        
        -- Metadata
        was_duplicate AS had_duplicates,
        duplicate_count,
        
        -- Record keeping
        loaded_at,
        source_file,
        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM deduped

)

SELECT * FROM with_metrics
