"""
Failure simulation scripts for DataIQ testing.
Creates various failure scenarios to test the multi-agent pipeline.
"""

import json
import os
import shutil
import sys
from datetime import datetime, timedelta

import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
SOURCE_FILE = os.path.join(RAW_DATA_DIR, "yellow_tripdata_2024_01.parquet")
BACKUP_FILE = os.path.join(RAW_DATA_DIR, "yellow_tripdata_2024_01.parquet.bak")
TARGET_DIR = os.path.join(PROJECT_ROOT, "dataiq", "target")


def write_active_scenario(name: str, description: str):
    """Write active_scenario.json so run_pipeline.py knows which scenario is active."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    marker_path = os.path.join(PROCESSED_DIR, "active_scenario.json")
    data = {
        "scenario": name,
        "applied_at": datetime.now().isoformat(),
        "description": description,
    }
    with open(marker_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote active scenario marker: {name}")


def backup_source():
    """Backup original parquet if not already backed up."""
    if not os.path.exists(BACKUP_FILE):
        shutil.copy2(SOURCE_FILE, BACKUP_FILE)
        print(f"Backed up source to {BACKUP_FILE}")


def restore_source():
    """Restore original parquet from backup."""
    if os.path.exists(BACKUP_FILE):
        shutil.copy2(BACKUP_FILE, SOURCE_FILE)
        os.remove(BACKUP_FILE)
        print("Restored original source file")


def scenario_reset():
    """Scenario 0: Reset everything to normal state."""
    restore_source()

    marker = os.path.join(PROCESSED_DIR, "last_run.json")
    if os.path.exists(marker):
        os.remove(marker)
        print("Removed freshness marker")

    override = os.path.join(TARGET_DIR, "run_results_override.json")
    if os.path.exists(override):
        os.remove(override)
        print("Removed performance override")

    active = os.path.join(PROCESSED_DIR, "active_scenario.json")
    if os.path.exists(active):
        os.remove(active)
        print("Removed active scenario marker")

    print("Reset complete — all scenarios cleared.")


def scenario_schema_change():
    """Scenario 1: Rename fare_amount to fare in the parquet."""
    backup_source()

    conn = duckdb.connect()
    df = conn.execute(f"""
        SELECT * EXCLUDE(fare_amount), fare_amount AS fare
        FROM read_parquet('{SOURCE_FILE.replace(os.sep, '/')}')
    """).fetchdf()
    conn.close()

    df.to_parquet(SOURCE_FILE, index=False)
    print(f"Schema change applied: renamed fare_amount -> fare")
    print(f"This will cause stg_taxi_trips to fail on missing fare_amount column.")
    write_active_scenario("schema_change", "fare_amount renamed to fare")


def scenario_data_freshness():
    """Scenario 2: Create a stale last_run marker."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    marker = os.path.join(PROCESSED_DIR, "last_run.json")

    stale_date = (datetime.now() - timedelta(days=2)).isoformat()
    data = {
        "last_run": stale_date,
        "status": "success",
        "models_run": 6
    }

    with open(marker, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Freshness issue created: last_run set to {stale_date}")
    print("DataIQ agents will detect the pipeline hasn't run recently.")
    write_active_scenario("data_freshness", f"last_run set to {stale_date}")


def scenario_quality_drift():
    """Scenario 3: Set 34% of fare_amount values to null."""
    backup_source()

    conn = duckdb.connect()
    df = conn.execute(f"""
        SELECT * FROM read_parquet('{SOURCE_FILE.replace(os.sep, '/')}')
    """).fetchdf()
    conn.close()

    n_nulls = int(len(df) * 0.34)
    null_indices = df.sample(n=n_nulls, random_state=42).index
    df.loc[null_indices, "fare_amount"] = None

    df.to_parquet(SOURCE_FILE, index=False)
    null_pct = df["fare_amount"].isna().mean() * 100
    print(f"Quality drift applied: {null_pct:.1f}% of fare_amount set to null ({n_nulls:,} rows)")
    print("This will cause data quality test failures.")
    write_active_scenario("quality_drift", f"{null_pct:.1f}% of fare_amount set to null")


def scenario_performance_degradation():
    """Scenario 4: Create run_results override with 10x execution times."""
    run_results_path = os.path.join(TARGET_DIR, "run_results.json")

    if not os.path.exists(run_results_path):
        print(f"No run_results.json found at {run_results_path}")
        print("Run 'dbt run' first, then apply this scenario.")
        return

    with open(run_results_path) as f:
        results = json.load(f)

    for result in results.get("results", []):
        if "execution_time" in result:
            result["execution_time"] = result["execution_time"] * 10

    override_path = os.path.join(TARGET_DIR, "run_results_override.json")
    with open(override_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Performance degradation applied: execution times 10x'd")
    print(f"Override saved to {override_path}")
    write_active_scenario("performance_degradation", "execution times multiplied by 10x")


SCENARIOS = {
    "0": ("reset", scenario_reset),
    "reset": ("reset", scenario_reset),
    "1": ("schema_change", scenario_schema_change),
    "schema_change": ("schema_change", scenario_schema_change),
    "2": ("data_freshness", scenario_data_freshness),
    "data_freshness": ("data_freshness", scenario_data_freshness),
    "3": ("quality_drift", scenario_quality_drift),
    "quality_drift": ("quality_drift", scenario_quality_drift),
    "4": ("performance_degradation", scenario_performance_degradation),
    "performance_degradation": ("performance_degradation", scenario_performance_degradation),
}


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/simulate_failure.py <scenario>")
        print("\nScenarios:")
        print("  0 / reset                  - Restore everything to normal")
        print("  1 / schema_change          - Rename fare_amount to fare")
        print("  2 / data_freshness         - Create stale run marker")
        print("  3 / quality_drift          - Set 34% of fares to null")
        print("  4 / performance_degradation - 10x execution times")
        sys.exit(1)

    scenario_key = sys.argv[1].lower()
    if scenario_key not in SCENARIOS:
        print(f"Unknown scenario: {scenario_key}")
        sys.exit(1)

    name, func = SCENARIOS[scenario_key]
    print(f"\n{'='*50}")
    print(f"Running scenario: {name}")
    print(f"{'='*50}\n")
    func()


if __name__ == "__main__":
    main()
