WITH equipment_data AS (
    SELECT
        unit_id,
        dataset_id,
        MIN(cycle) AS first_observed_cycle,
        MAX(cycle) AS last_observed_cycle,
        COUNT(*) AS total_operational_cycles
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY unit_id, dataset_id
)

SELECT
    CONCAT(CAST(unit_id AS VARCHAR), '_', dataset_id) AS equipment_key,
    unit_id,
    dataset_id,
    'turbofan_engine' AS equipment_type,
    first_observed_cycle,
    last_observed_cycle,
    total_operational_cycles
FROM equipment_data
