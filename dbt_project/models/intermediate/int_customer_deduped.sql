-- models/intermediate/int_customer_deduped.sql
--
-- OBJECTIVE: One row per unique customer (golden records only)
-- USES: Only records where is_golden_record = TRUE

{{
    config(
        materialized='view',
        tags=['intermediate', 'dedup']
    )
}}

WITH identity AS (
    
    SELECT * FROM {{ ref('int_customer_identity') }}
    
    -- Filter: Only golden records
    WHERE is_golden_record = TRUE

),

-- Add metadata about deduplication
final AS (

    SELECT
        customer_key,
        customer_id AS original_customer_id,
        
        -- Clean data
        email_clean,
        first_name_clean,
        last_name_clean,
        phone_clean,
        address,
        city,
        country,
        created_at_parsed,
        
        -- Metadata
        duplicate_count,
        loaded_at,
        source_file,
        
        -- Flag if this customer was a duplicate
        CASE 
            WHEN duplicate_count > 1 THEN TRUE 
            ELSE FALSE 
        END AS was_duplicate

    FROM identity

)

SELECT * FROM final
