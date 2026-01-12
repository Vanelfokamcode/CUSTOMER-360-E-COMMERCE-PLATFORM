-- macros/generate_name_key.sql
--
-- OBJECTIVE: Generate a fuzzy matching key for names
-- USES: SOUNDEX for phonetic matching
-- STRATEGY: Concatenate first + last name, then SOUNDEX

{% macro generate_name_key(first_name, last_name) %}

SOUNDEX(
    REGEXP_REPLACE(
        UPPER(TRIM(COALESCE({{ first_name }}, ''))) || 
        UPPER(TRIM(COALESCE({{ last_name }}, ''))),
        '[^A-Z]',  -- Remove non-letters
        '',
        'g'
    )
)

{% endmacro %}
