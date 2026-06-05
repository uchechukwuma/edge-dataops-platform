{{ config(materialized='table') }}

SELECT 
    sensor_id,
    value,
    unit,
    checksum,
    validated,
    source_timestamp,
    batch_id,
    CASE 
        WHEN sensor_id LIKE 'temp%' THEN 'Temperature'
        WHEN sensor_id LIKE 'pressure%' THEN 'Pressure'
        WHEN sensor_id LIKE 'flow%' THEN 'Flow'
        ELSE 'Other'
    END as sensor_category
FROM {{ source('supabase_source', 'silver_sensor_data') }}