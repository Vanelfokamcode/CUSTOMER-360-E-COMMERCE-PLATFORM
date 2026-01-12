-- models/intermediate/int_customer_identity.sql
--
-- OBJECTIVE: Assign a unique customer_key to each real person
-- STRATEGY: Phase 1 - Exact match on normalized email
--
-- BUSINESS LOGIC:
--   - Same email = same person (even if names differ slightly)
--   - For NULL emails: each row is a unique person (conservative)
--   - Use surrogate_key for deterministic hashing

{{
    config(
        materialized='view',
        tags=['intermediate', 'identity']
    )
}}

WITH staging AS (
    
    SELECT * FROM {{ ref('stg_csv_customers') }}

),

-- Step 1: Create a matching key for deduplication
identity_keys AS (

    SELECT
        *,
        
        -- MATCHING STRATEGY:
        -- If email exists: use it as identity
        -- If email NULL: use customer_id (treat as unique)
        COALESCE(
            email_clean,
            'NULL_EMAIL_' || customer_id
        ) AS identity_match_key
        
    FROM staging

),

-- Step 2: Assign a unique customer_key using surrogate_key
-- This creates a deterministic hash from the match key
with_customer_key AS (

    SELECT
        *,
        
        -- Generate surrogate key (deterministic hash)
        {{ dbt_utils.generate_surrogate_key(['identity_match_key']) }} AS customer_key
        
    FROM identity_keys

),

-- Step 3: For each customer_key, determine which record is the "golden" one
-- Strategy: Take the EARLIEST record (by created_at, then customer_id)
ranked AS (

    SELECT
        *,
        
        -- Rank records within each customer_key
        ROW_NUMBER() OVER (
            PARTITION BY customer_key
            ORDER BY 
                created_at_parsed ASC,  -- Earliest first
                customer_id ASC         -- Tie-breaker (deterministic)
        ) AS record_rank,
        
        -- Count how many records share this customer_key
        COUNT(*) OVER (
            PARTITION BY customer_key
        ) AS duplicate_count
        
    FROM with_customer_key

)

SELECT
    -- Original fields
    customer_id,
    email_clean,
    email_raw,
    first_name_clean,
    first_name_raw,
    last_name_clean,
    last_name_raw,
    phone_clean,
    phone_raw,
    address,
    city,
    country,
    created_at_parsed,
    created_at_raw,
    loaded_at,
    source_file,
    
    -- Identity resolution fields
    customer_key,
    identity_match_key,
    record_rank,
    duplicate_count,
    
    -- Flag: is this the golden record?
    CASE 
        WHEN record_rank = 1 THEN TRUE 
        ELSE FALSE 
    END AS is_golden_record
    
FROM ranked
