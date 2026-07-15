from __future__ import annotations

import random
from pathlib import Path

from src.utils.config import get_project_root


def generate_synthetic_cmapss(
    output_dir: Path,
    n_units: int = 5,
    min_cycles: int = 50,
    max_cycles: int = 150,
    seed: int = 42,
) -> dict[str, Path]:
    random.seed(seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    sensor_baselines = [
        (518.67, 1.0), (642.15, 2.0), (1589.70, 5.0), (1400.60, 4.0),
        (14.62, 0.5), (21.61, 1.0), (554.36, 3.0), (2388.06, 5.0),
        (9046.19, 20.0), (1.3, 0.01), (47.47, 0.5), (521.66, 2.0),
        (2388.02, 5.0), (8138.62, 20.0), (8.4195, 0.1), (0.03, 0.001),
        (392.0, 5.0), (2388.0, 5.0), (100.0, 0.0), (39.06, 0.5),
        (23.42, 0.3),
    ]

    train_lines: list[str] = []
    rul_values: list[int] = []

    for unit_id in range(1, n_units + 1):
        total_cycles = random.randint(min_cycles, max_cycles)
        rul_values.append(0)

        for cycle in range(1, total_cycles + 1):
            degradation = (cycle / total_cycles) * 0.1

            op1 = round(random.uniform(-0.0087, 0.0087), 4)
            op2 = round(random.uniform(-0.0004, 0.0004), 4)
            op3 = round(random.uniform(99.0, 100.0), 1)

            sensors = []
            for base, noise in sensor_baselines:
                drift = base * degradation * random.uniform(0.5, 1.5)
                value = base + random.gauss(0, noise) + drift
                sensors.append(round(value, 4))

            values = [str(unit_id), str(cycle), str(op1), str(op2), str(op3)]
            values.extend(str(s) for s in sensors)
            train_lines.append("  ".join(values))

    test_lines: list[str] = []
    test_rul_values: list[int] = []

    for unit_id in range(1, n_units + 1):
        total_cycles = random.randint(min_cycles, max_cycles)
        visible_cycles = random.randint(int(total_cycles * 0.3), int(total_cycles * 0.8))
        test_rul_values.append(total_cycles - visible_cycles)

        for cycle in range(1, visible_cycles + 1):
            degradation = (cycle / total_cycles) * 0.1

            op1 = round(random.uniform(-0.0087, 0.0087), 4)
            op2 = round(random.uniform(-0.0004, 0.0004), 4)
            op3 = round(random.uniform(99.0, 100.0), 1)

            sensors = []
            for base, noise in sensor_baselines:
                drift = base * degradation * random.uniform(0.5, 1.5)
                value = base + random.gauss(0, noise) + drift
                sensors.append(round(value, 4))

            values = [str(unit_id), str(cycle), str(op1), str(op2), str(op3)]
            values.extend(str(s) for s in sensors)
            test_lines.append("  ".join(values))

    files = {}

    train_path = output_dir / "train_FD001.txt"
    train_path.write_text("\n".join(train_lines) + "\n")
    files["train"] = train_path

    test_path = output_dir / "test_FD001.txt"
    test_path.write_text("\n".join(test_lines) + "\n")
    files["test"] = test_path

    rul_path = output_dir / "RUL_FD001.txt"
    rul_path.write_text("\n".join(str(v) for v in test_rul_values) + "\n")
    files["rul"] = rul_path

    return files


if __name__ == "__main__":
    project_root = get_project_root()
    source_dir = project_root / "data" / "source"
    result = generate_synthetic_cmapss(source_dir, n_units=10, min_cycles=80, max_cycles=200)
    for name, path in result.items():
        print(f"Generated {name}: {path}")
