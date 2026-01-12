-- tests/test_date_parsing.sql
--
-- TEST: Date parsing should not produce NULL for non-null inputs
-- FAILS if dates failed to parse

SELECT
    created_at_raw,
    created_at_parsed,
    'Date failed to parse' as failure_reason
FROM {{ ref('stg_csv_customers') }}
WHERE created_at_raw IS NOT NULL
  AND created_at_parsed IS NULL
