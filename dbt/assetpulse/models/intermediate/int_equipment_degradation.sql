WITH sensor_data AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

sensor_changes AS (
    SELECT
        unit_id,
        cycle,
        dataset_id,
        sensor_02,
        sensor_03,
        sensor_04,
        sensor_07,
        sensor_11,
        sensor_12,

        sensor_02 - LAG(sensor_02, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_02_rate_of_change,

        sensor_03 - LAG(sensor_03, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_03_rate_of_change,

        sensor_04 - LAG(sensor_04, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_04_rate_of_change,

        sensor_07 - LAG(sensor_07, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_07_rate_of_change,

        sensor_11 - LAG(sensor_11, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_11_rate_of_change,

        sensor_12 - LAG(sensor_12, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_12_rate_of_change,

        LEAD(sensor_02, 1) OVER (
            PARTITION BY unit_id ORDER BY cycle
        ) AS sensor_02_next_value

    FROM sensor_data
),

degradation_trend AS (
    SELECT
        unit_id,
        cycle,
        dataset_id,
        sensor_02,
        sensor_02_rate_of_change,
        sensor_03_rate_of_change,
        sensor_04_rate_of_change,
        sensor_07_rate_of_change,
        sensor_11_rate_of_change,
        sensor_12_rate_of_change,
        sensor_02_next_value,

        AVG(sensor_02_rate_of_change) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_02_degradation_trend,

        AVG(sensor_04_rate_of_change) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_04_degradation_trend,

        (CAST(cycle AS DOUBLE) / CAST(MAX(cycle) OVER (PARTITION BY unit_id) AS DOUBLE))
            AS lifecycle_ratio

    FROM sensor_changes
)

SELECT * FROM degradation_trend
