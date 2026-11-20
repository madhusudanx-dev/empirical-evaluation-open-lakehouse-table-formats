#!/usr/bin/env bash
set -euo pipefail

mkdir -p results/raw results/logs site

echo "=== Environment Info ==="
echo "CPUs: $(nproc) | RAM: $(free -g | awk '/^Mem:/{print $2}') GB"
java -version
python -c "import pyspark; print('PySpark:', pyspark.__version__)"

echo "=== Step 0: Generating TPC-DS Source Data ==="
python scripts/00_generate_data.py

STEPS=("01_load" "02_queries" "03_upserts" "04_compaction" "06_time_travel_schema" "07_audit_repro" "05_storage")
FORMATS=("iceberg" "delta" "hudi")

for fmt in "${FORMATS[@]}"; do
  echo "=========================================="
  echo "Running Benchmark Suite for: ${fmt^^}"
  echo "=========================================="
  for step in "${STEPS[@]}"; do
    echo "[${fmt}] Executing ${step}..."
    if ! python "scripts/${step}.py" --format "${fmt}" > "results/logs/${fmt}_${step}.log" 2>&1; then
      echo "[WARN] ${fmt}:${step} encountered an error. Check results/logs/${fmt}_${step}.log"
      if [ "${step}" == "01_load" ]; then
        echo "[ERROR] Load failed for ${fmt}, skipping remaining steps."
        break
      fi
    fi
  done
done

echo "=== Aggregating Results ==="
python scripts/aggregate.py
echo "Benchmark complete. Artifacts saved to results/"
