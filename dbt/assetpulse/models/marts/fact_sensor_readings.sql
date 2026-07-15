WITH sensor_data AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

equipment AS (
    SELECT * FROM {{ ref('dim_equipment') }}
)

SELECT
    e.equipment_key,
    s.unit_id,
    s.dataset_id,
    s.cycle,
    s.operational_setting_1,
    s.operational_setting_2,
    s.operational_setting_3,
    s.sensor_01,
    s.sensor_02,
    s.sensor_03,
    s.sensor_04,
    s.sensor_05,
    s.sensor_06,
    s.sensor_07,
    s.sensor_08,
    s.sensor_09,
    s.sensor_10,
    s.sensor_11,
    s.sensor_12,
    s.sensor_13,
    s.sensor_14,
    s.sensor_15,
    s.sensor_16,
    s.sensor_17,
    s.sensor_18,
    s.sensor_19,
    s.sensor_20,
    s.sensor_21,
    s.record_quality_status,
    s.ingested_at,
    s.silver_processed_at
FROM sensor_data s
INNER JOIN equipment e
    ON s.unit_id = e.unit_id
    AND s.dataset_id = e.dataset_id
