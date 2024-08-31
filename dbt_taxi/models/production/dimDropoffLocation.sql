{{ config(materialized = 'table') }}

WITH dropoff_location AS (
    SELECT DISTINCT dropoff_location_id, dropoff_latitude, dropoff_longitude
    FROM staging.nyc_taxi
    WHERE vendor_id IS NOT NULL
)

SELECT *
FROM dropoff_location
WHERE dropoff_location_id IS NOT NULL
ORDER BY dropoff_location_id