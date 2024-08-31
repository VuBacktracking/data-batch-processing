{{ config(materialized = 'table') }}

with service_type_staging as (
    SELECT DISTINCT(service_type)
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT service_type AS service_type_id,
       {{ get_service_name('service_type') }} AS service_name
FROM service_type_staging
WHERE service_type IS NOT NULL
ORDER BY service_type