WITH source_data AS (
    SELECT *
    FROM read_parquet('../../data/silver/sensor_readings/**/*.parquet')
)

SELECT
    unit_id,
    cycle,
    operational_setting_1,
    operational_setting_2,
    operational_setting_3,
    sensor_01,
    sensor_02,
    sensor_03,
    sensor_04,
    sensor_05,
    sensor_06,
    sensor_07,
    sensor_08,
    sensor_09,
    sensor_10,
    sensor_11,
    sensor_12,
    sensor_13,
    sensor_14,
    sensor_15,
    sensor_16,
    sensor_17,
    sensor_18,
    sensor_19,
    sensor_20,
    sensor_21,
    dataset_id,
    record_quality_status,
    has_cycle_gap,
    has_sensor_range_violation,
    has_sensor_spike,
    ingested_at,
    silver_processed_at
FROM source_data
WHERE record_quality_status != 'QUARANTINED'
