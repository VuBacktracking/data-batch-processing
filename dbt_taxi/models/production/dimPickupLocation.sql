{{ config(materialized = 'table') }}

WITH pickup_location AS (
    SELECT DISTINCT pickup_location_id, pickup_latitude, pickup_longitude
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT *
FROM pickup_location
WHERE pickup_location_id IS NOT NULL
ORDER BY pickup_location_id