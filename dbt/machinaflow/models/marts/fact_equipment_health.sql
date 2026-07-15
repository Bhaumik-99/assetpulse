WITH health_source AS (
    SELECT *
    FROM read_parquet('../../data/gold/analytics/fact_equipment_health.parquet')
),

ranked_equipment AS (
    SELECT
        equipment_key,
        cycle,
        health_score,
        degradation_score,
        remaining_useful_life AS estimated_rul,
        risk_level,
        calculated_at,

        ROW_NUMBER() OVER (
            ORDER BY health_score ASC
        ) AS maintenance_priority_rank,

        ROW_NUMBER() OVER (
            PARTITION BY equipment_key
            ORDER BY cycle DESC
        ) AS cycle_rank

    FROM health_source
)

SELECT
    equipment_key,
    cycle,
    health_score,
    degradation_score,
    estimated_rul,
    risk_level,
    maintenance_priority_rank,
    calculated_at
FROM ranked_equipment
