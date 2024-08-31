{{ config(materialized = 'table') }}

WITH payment_staging AS (
    SELECT DISTINCT(payment_type_id)
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT  {{ dbt_utils.generate_surrogate_key(['payment_type_id']) }} AS payment_type_key,
        payment_type_id,
        {{ get_payment_description('payment_type_id') }} AS payment_description
FROM payment_staging
WHERE payment_type_id IS NOT NULL
ORDER BY payment_type_id