-- models/intermediate/int_customer_fuzzy.sql
--
-- OBJECTIVE: Phase 2 identity resolution using fuzzy name matching
-- STRATEGY: Match customers with similar names but different emails
--
-- BUSINESS RULES:
--   1. Names must be phonetically identical (SOUNDEX)
--   2. Name length must be similar (within ±3 chars)
--   3. Conservative: prefer false negatives over false positives

{{
    config(
        materialized='view',
        tags=['intermediate', 'fuzzy']
    )
}}

WITH deduped AS (
    
    -- Start with Phase 1 results (exact email matches resolved)
    SELECT * FROM {{ ref('int_customer_deduped') }}

),

-- Step 1: Generate fuzzy matching keys
with_fuzzy_keys AS (

    SELECT
        *,
        
        -- Phonetic key for name matching
        {{ generate_name_key('first_name_clean', 'last_name_clean') }} AS name_soundex,
        
        -- Name length (for similarity check)
        LENGTH(first_name_clean || last_name_clean) AS name_length,
        
        -- Create a fuzzy matching key
        -- Combine soundex + approximate length bucket
        {{ generate_name_key('first_name_clean', 'last_name_clean') }} || '_' ||
        FLOOR(LENGTH(first_name_clean || last_name_clean) / 3) AS fuzzy_match_key
        
    FROM deduped

),

-- Step 2: Find potential duplicate groups
-- Group by fuzzy_match_key to find candidates
fuzzy_groups AS (

    SELECT
        *,
        
        -- Count how many customers share this fuzzy key
        COUNT(*) OVER (
            PARTITION BY fuzzy_match_key
        ) AS fuzzy_group_size,
        
        -- Assign a master customer_key within each fuzzy group
        -- Take the customer_key of the earliest record
        FIRST_VALUE(customer_key) OVER (
            PARTITION BY fuzzy_match_key
            ORDER BY created_at_parsed ASC, customer_key ASC
        ) AS master_customer_key
        
    FROM with_fuzzy_keys

),

-- Step 3: Decide which records to merge
-- Conservative approach: only merge if highly confident
final AS (

    SELECT
        *,
        
        -- DECISION LOGIC:
        -- Merge if:
        --   1. fuzzy_group_size > 1 (potential duplicate)
        --   2. Names are phonetically identical (same soundex)
        --   3. Name lengths are similar (within 3 chars)
        CASE
            WHEN fuzzy_group_size > 1 THEN master_customer_key
            ELSE customer_key
        END AS final_customer_key,
        
        -- Flag if this was a fuzzy match
        CASE
            WHEN fuzzy_group_size > 1 AND master_customer_key != customer_key 
            THEN TRUE
            ELSE FALSE
        END AS was_fuzzy_matched

    FROM fuzzy_groups

)

SELECT
    -- Use final_customer_key instead of customer_key
    final_customer_key AS customer_key,
    original_customer_id,
    
    -- Keep all other fields
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
    was_duplicate,
    was_fuzzy_matched,
    name_soundex,
    fuzzy_match_key,
    loaded_at,
    source_file

FROM final
