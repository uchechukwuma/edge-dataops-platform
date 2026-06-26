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
        WHEN sensor_id LIKE 'vibration%' THEN 'Vibration' -- Added explicit map
        WHEN sensor_id LIKE 'humidity%' THEN 'Humidity'   -- Added explicit map
        ELSE 'Other'
    END as sensor_category
FROM {{ source('supabase_source', 'silver_sensor_data') }}