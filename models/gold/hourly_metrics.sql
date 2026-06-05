{{ config(materialized='table') }}

SELECT 
    date_trunc('hour', source_timestamp) as hourly_window,
    sensor_category,
    AVG(value) as avg_value,
    COUNT(*) as reading_count
FROM {{ ref('clean_sensor_data') }}
GROUP BY 1, 2