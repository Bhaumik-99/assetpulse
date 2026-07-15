WITH sensor_data AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

rolling_stats AS (
    SELECT
        unit_id,
        cycle,
        dataset_id,

        AVG(sensor_02) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_02_rolling_mean_10,

        STDDEV(sensor_02) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_02_rolling_std_10,

        AVG(sensor_03) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_03_rolling_mean_10,

        STDDEV(sensor_03) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_03_rolling_std_10,

        AVG(sensor_04) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_04_rolling_mean_10,

        AVG(sensor_07) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_07_rolling_mean_10,

        AVG(sensor_11) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_11_rolling_mean_10,

        AVG(sensor_12) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS sensor_12_rolling_mean_10,

        MIN(sensor_02) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sensor_02_rolling_min_20,

        MAX(sensor_02) OVER (
            PARTITION BY unit_id
            ORDER BY cycle
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sensor_02_rolling_max_20

    FROM sensor_data
)

SELECT * FROM rolling_stats
