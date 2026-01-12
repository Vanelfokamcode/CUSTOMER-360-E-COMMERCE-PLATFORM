-- Clean the messy raw data

{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

WITH source AS (
    
    SELECT * FROM {{ source('raw', 'csv_customers') }}

),

cleaned AS (

    SELECT
        customer_id,
        
        -- EMAIL CLEANING
        CASE
            WHEN email IS NULL THEN NULL
            WHEN TRIM(email) NOT LIKE '%@%' THEN NULL
            ELSE LOWER(TRIM(email))
        END AS email_clean,
        
        email AS email_raw,
        
        -- NAME CLEANING
        INITCAP(
            TRIM(
                REGEXP_REPLACE(first_name, '[™©]', '', 'g')
            )
        ) AS first_name_clean,
        
        INITCAP(
            TRIM(
                REGEXP_REPLACE(last_name, '[™©]', '', 'g')
            )
        ) AS last_name_clean,
        
        first_name AS first_name_raw,
        last_name AS last_name_raw,
        
        -- PHONE CLEANING
        CASE
            WHEN phone IS NULL THEN NULL
            ELSE REGEXP_REPLACE(phone, '[^0-9+]', '', 'g')
        END AS phone_clean,
        
        phone AS phone_raw,
        
        address,
        city,
        country,
        created_at AS created_at_raw,
        {{ parse_mixed_dates('created_at') }} AS created_at_parsed,
        loaded_at,
        source_file
        
    FROM source

)

SELECT * FROM cleaned
