-- tests/test_email_format.sql
--
-- TEST: All non-null emails must contain @ and .
-- FAILS if any email is malformed

SELECT
    email_clean,
    'Email missing @ or .' as failure_reason
FROM {{ ref('stg_csv_customers') }}
WHERE email_clean IS NOT NULL
  AND (
      email_clean NOT LIKE '%@%'
      OR email_clean NOT LIKE '%.%'
  )
