{{ config(materialized = 'table') }}

WITH trip_staging AS (
    SELECT 
        -- Identifiers
        {{ dbt_utils.generate_surrogate_key([
            'trip.vendor_id', 'trip.rate_code_id', 'trip.pickup_location_id', 
            'trip.dropoff_location_id', 'trip.payment_type_id', 'trip.service_type', 
            'trip.pickup_datetime', 'trip.dropoff_datetime']) }} AS trip_id,
        dv.vendor_key AS vendor_key,
        dr.rate_code_key AS rate_code_key,
        pl.pickup_location_id AS pickup_location_id,
        dl.dropoff_location_id AS dropoff_location_id,
        dp.payment_type_key AS payment_type_key,
        trip.service_type AS service_type_id,

        -- Timestamps
        trip.pickup_datetime AS pickup_datetime,
        trip.dropoff_datetime AS dropoff_datetime,

        -- Trip info
        trip.passenger_count AS passenger_count,
        trip.trip_distance AS trip_distance,

        -- Payment info
        trip.extra AS extra,
        trip.mta_tax AS mta_tax,
        trip.fare_amount AS fare_amount,
        trip.tip_amount AS tip_amount,
        trip.tolls_amount AS tolls_amount,
        trip.total_amount AS total_amount,
        trip.improvement_surcharge AS improvement_surcharge,
        trip.congestion_surcharge AS congestion_surcharge

    FROM staging.nyc_taxi AS trip  -- Reference the existing table directly
    JOIN {{ ref('dimVendor') }} AS dv 
        ON trip.vendor_id = dv.vendor_id
    JOIN {{ ref('dimRateCode') }} AS dr 
        ON trip.rate_code_id = dr.rate_code_id
    JOIN {{ ref('dimPayment') }} AS dp 
        ON trip.payment_type_id = dp.payment_type_id
    JOIN {{ ref('dimPickupLocation') }} AS pl
        ON trip.pickup_location_id = pl.pickup_location_id
    JOIN {{ ref('dimDropoffLocation') }} AS dl
        ON trip.dropoff_location_id = dl.dropoff_location_id
)

SELECT *
FROM trip_staging
