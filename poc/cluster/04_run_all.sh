#!/usr/bin/env bash
# Run full Situated Learning PoC on lab cluster: HDFS + Spark + Hive
# Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
export LOCAL_DATA="${LOCAL_DATA:-${ROOT_DIR}/sample_data}"
export HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/situated_learning/contracts}"

echo "===================================================="
echo " Situated Learning PoC — 3 Hadoop tools"
echo " 1) HDFS  2) Spark  3) Hive"
echo " HDFS_BASE=${HDFS_BASE}"
echo " LOCAL_DATA=${LOCAL_DATA}"
echo "===================================================="

chmod +x "${SCRIPT_DIR}/01_hdfs_ingest.sh" || true

echo ""
echo ">>>> TOOL 1/3: HDFS ingest"
bash "${SCRIPT_DIR}/01_hdfs_ingest.sh"

echo ""
echo ">>>> TOOL 2/3: Spark validate + map"
spark-submit "${SCRIPT_DIR}/02_spark_validate_map.py"

echo ""
echo ">>>> TOOL 3/3: Hive analytics"
TMP_HQL="$(mktemp /tmp/situated_hive_XXXX.hql)"
sed "s|__HDFS_BASE__|${HDFS_BASE}|g" "${SCRIPT_DIR}/03_hive_analytics.hql" > "${TMP_HQL}"

if command -v hive >/dev/null 2>&1; then
  hive -f "${TMP_HQL}"
elif command -v beeline >/dev/null 2>&1; then
  # Adjust JDBC URL if your lab uses a different HiveServer2 endpoint
  beeline -u "${HIVE_JDBC:-jdbc:hive2://localhost:10000}" -f "${TMP_HQL}"
else
  echo "ERROR: neither hive nor beeline found in PATH" >&2
  exit 1
fi

rm -f "${TMP_HQL}"

echo ""
echo "===================================================="
echo " PoC complete. Capture screenshots now:"
echo "  - HDFS ls / du"
echo "  - Spark show() + metrics"
echo "  - Hive Q1–Q8 results"
echo "===================================================="
