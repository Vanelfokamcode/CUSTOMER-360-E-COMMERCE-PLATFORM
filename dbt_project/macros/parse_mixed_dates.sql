-- macros/parse_mixed_dates.sql
--
-- OBJECTIVE: Parse mixed date formats into standard DATE
-- HANDLES:
--   - ISO: 2024-01-15
--   - European: 15/01/2024
--   - American: 01-15-2024
--   - Timestamp: 2024/01/15 14:30:00

{% macro parse_mixed_dates(column_name) %}

CASE
    -- Format 1: ISO (YYYY-MM-DD) - already standard
    WHEN {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}$' 
    THEN TO_DATE({{ column_name }}, 'YYYY-MM-DD')
    
    -- Format 2: European (DD/MM/YYYY)
    WHEN {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$' 
    THEN TO_DATE({{ column_name }}, 'DD/MM/YYYY')
    
    -- Format 3: American (MM-DD-YYYY)
    WHEN {{ column_name }} ~ '^\d{2}-\d{2}-\d{4}$' 
    THEN TO_DATE({{ column_name }}, 'MM-DD-YYYY')
    
    -- Format 4: Timestamp (YYYY/MM/DD HH:MI:SS)
    WHEN {{ column_name }} ~ '^\d{4}/\d{2}/\d{2}' 
    THEN TO_DATE(SPLIT_PART({{ column_name }}, ' ', 1), 'YYYY/MM/DD')
    
    -- Fallback: NULL if unparseable
    ELSE NULL
END

{% endmacro %}

