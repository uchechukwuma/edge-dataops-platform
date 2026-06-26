{{ config(materialized='view') }}

SELECT 
    source_timestamp as hourly_window, -- High-resolution timestamp directly from the edge
    sensor_category,
    value as avg_value,                 -- Individual raw value reading
    1 as reading_count
FROM {{ ref('clean_sensor_data') }}