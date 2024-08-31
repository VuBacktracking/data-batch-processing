{{ config(materialized = 'table') }}

WITH vendor_staging AS (
    SELECT DISTINCT(vendor_id)
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT  {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} AS vendor_key,
        CAST(vendor_id AS INT),
        {{ get_vendor_description('vendor_id') }} AS vendor_name
FROM vendor_staging
WHERE vendor_id IS NOT NULL
    AND vendor_id < 3
ORDER BY vendor_id