#!/usr/bin/env bash
# Tool 1: HDFS — create zones and ingest synthetic contract data
# Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
set -euo pipefail

# ---- edit if your lab user/home path differs ----
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/situated_learning/contracts}"
LOCAL_DATA="${LOCAL_DATA:-$(cd "$(dirname "$0")/../sample_data" && pwd)}"
HDFS_NAMENODE="${HDFS_NAMENODE:-hdfs://hadoop-master:9000}"

echo "==> HDFS namenode: ${HDFS_NAMENODE}"
echo "==> HDFS base: ${HDFS_BASE}"
echo "==> Local data: ${LOCAL_DATA}"

# Fail fast if HDFS is down
if ! hdfs dfs -ls / >/dev/null 2>&1; then
  echo "ERROR: cannot talk to HDFS. Run start-dfs.sh and check: jps && hdfs dfs -ls /" >&2
  exit 1
fi

echo "==> Creating HDFS directories"
hdfs dfs -mkdir -p \
  "${HDFS_BASE}/raw/pdf" \
  "${HDFS_BASE}/raw/docx" \
  "${HDFS_BASE}/extracted/json" \
  "${HDFS_BASE}/extracted/flat" \
  "${HDFS_BASE}/curated/parquet" \
  "${HDFS_BASE}/curated/csv" \
  "${HDFS_BASE}/quarantine" \
  "${HDFS_BASE}/exports" \
  "${HDFS_BASE}/logs"

echo "==> Putting raw PDF contracts"
hdfs dfs -put -f "${LOCAL_DATA}/contracts/pdf/"*.pdf "${HDFS_BASE}/raw/pdf/"

echo "==> Putting raw DOCX contracts"
hdfs dfs -put -f "${LOCAL_DATA}/contracts/docx/"*.docx "${HDFS_BASE}/raw/docx/"

echo "==> Putting AI extraction JSON"
hdfs dfs -put -f "${LOCAL_DATA}/extracted/json/"*.json "${HDFS_BASE}/extracted/json/"

echo "==> Putting flat CSV (for Hive external table fallback)"
hdfs dfs -put -f "${LOCAL_DATA}/extracted/flat/contract_extractions.csv" "${HDFS_BASE}/extracted/flat/"

echo "==> HDFS listing (screenshot this)"
hdfs dfs -ls -R "${HDFS_BASE}" | sed -n '1,80p'

echo "==> HDFS disk usage (screenshot this)"
hdfs dfs -du -h -s "${HDFS_BASE}"/* || true

echo "==> Done: HDFS ingest complete"
echo "HDFS_BASE=${HDFS_BASE}"
