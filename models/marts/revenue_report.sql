{{ config(materialized='table', schema='marts') }}

SELECT 
            -- Reveneue grouping 
            PULocationID AS revenue_zone,
            EXTRACT(MONTH FROM lpep_pickup_datetime) as revenue_month,

            -- Revenue calculation 
            SUM(fare_amount) AS revenue_monthly_fare,
            SUM(extra) AS revenue_monthly_extra,
            SUM(mta_tax) AS revenue_monthly_mta_tax,
            SUM(tip_amount) AS revenue_monthly_tip_amount,
            SUM(tolls_amount) AS revenue_monthly_tolls_amount,
            SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
            SUM(total_amount) AS revenue_monthly_total_amount,
            SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

            -- Additional calculations
            AVG(passenger_count) AS avg_montly_passenger_count,
            AVG(trip_distance) AS avg_montly_trip_distance
from {{ source('bigquery_prj','green_taxi') }}

GROUP BY
1, 2