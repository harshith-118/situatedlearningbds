# Situated Learning PoC — Synthetic Contract Extraction Pipeline

**Student:** DEVATA SAI HARSHITH  
**BITS ID:** 2024DC04035  
**Course:** Big Data Systems (CC ZG522)

## Important
All contracts and extraction outputs in this folder are **100% synthetic**.
No real LifeSource / client documents or production data are included.

## Layout
```
sample_data/
  contracts/pdf/          # synthetic PDF agreements
  contracts/docx/         # synthetic DOCX agreements
  extracted/json/         # simulated AI extraction results
  templates/              # empty Excel business template
  exports/                # filled Excel + quality summary
scripts/
  map_json_to_excel.py    # code-execution step: JSON -> Excel
```

## Hadoop mapping (conceptual PoC)
| Zone | Local folder | Hadoop tool |
|------|--------------|-------------|
| Raw | contracts/ | HDFS |
| Processed | extracted/json/ | Spark (validate/map) |
| Curated / analytics | exports/poc_quality_summary.json | Hive |
| Business delivery | exports/*.xlsx | Python code execution |

## Run Excel mapping (local business step)
```bash
# from Simulated Learning/
.venv/bin/python poc/scripts/map_json_to_excel.py
```

## Real PoC on lab cluster (required — 3 Hadoop tools)
See **`cluster/RUNBOOK.md`**.

```bash
# on lab machine, after copying poc/
./cluster/04_run_all.sh
```

| Tool | Script |
|------|--------|
| HDFS | `cluster/01_hdfs_ingest.sh` |
| Spark | `cluster/02_spark_validate_map.py` |
| Hive | `cluster/03_hive_analytics.hql` |
