{{ config(materialized = 'table') }}

WITH rate_code_staging AS (
    SELECT DISTINCT(rate_code_id)
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT  {{ dbt_utils.generate_surrogate_key(['rate_code_id']) }} AS rate_code_key,
        rate_code_id,
        {{ get_rate_code_description('rate_code_id') }} AS rate_code_description
FROM rate_code_staging
WHERE rate_code_id IS NOT NULL 
    AND CAST(rate_code_id AS INTEGER) < 7
ORDER BY rate_code_id