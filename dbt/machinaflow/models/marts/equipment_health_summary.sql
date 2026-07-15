WITH quality_data AS (
    SELECT
        record_quality_status,
        has_cycle_gap,
        has_sensor_range_violation,
        has_sensor_spike,
        dataset_id
    FROM {{ ref('stg_sensor_readings') }}
),

quality_counts AS (
    SELECT
        dataset_id,
        COUNT(*) AS total_records,
        SUM(CASE WHEN record_quality_status = 'VALID' THEN 1 ELSE 0 END) AS valid_records,
        SUM(CASE WHEN record_quality_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_records,
        SUM(CASE WHEN record_quality_status = 'QUARANTINED' THEN 1 ELSE 0 END) AS quarantined_records,
        SUM(CASE WHEN has_cycle_gap THEN 1 ELSE 0 END) AS cycle_gap_count,
        SUM(CASE WHEN has_sensor_range_violation THEN 1 ELSE 0 END) AS range_violation_count,
        SUM(CASE WHEN has_sensor_spike THEN 1 ELSE 0 END) AS spike_count
    FROM quality_data
    GROUP BY dataset_id
),

equipment_summary AS (
    SELECT *
    FROM read_parquet('../../data/gold/analytics/equipment_health_summary.parquet')
),

latest_health AS (
    SELECT
        unit_id,
        health_score,
        estimated_rul,
        risk_level,
        latest_cycle,
        sensors_in_warning_state
    FROM equipment_summary
)

SELECT
    qc.dataset_id,
    qc.total_records,
    qc.valid_records,
    qc.warning_records,
    qc.quarantined_records,
    ROUND(
        CAST(qc.valid_records AS DOUBLE) / NULLIF(CAST(qc.total_records AS DOUBLE), 0) * 100.0,
        2
    ) AS quality_pass_percentage,
    qc.cycle_gap_count,
    qc.range_violation_count,
    qc.spike_count,
    (SELECT COUNT(*) FROM latest_health) AS total_equipment_units,
    (SELECT AVG(health_score) FROM latest_health) AS avg_health_score,
    (SELECT COUNT(*) FROM latest_health WHERE risk_level = 'HIGH' OR risk_level = 'CRITICAL') AS high_risk_equipment_count,
    (SELECT COUNT(*) FROM latest_health WHERE risk_level = 'CRITICAL') AS critical_equipment_count
FROM quality_counts qc
