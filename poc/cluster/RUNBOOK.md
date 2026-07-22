# Lab Cluster PoC Runbook — 3 Hadoop Tools

**Student:** DEVATA SAI HARSHITH  
**BITS ID:** 2024DC04035  
**Tools used:** **HDFS + Spark + Hive** (meets “at least 3 Hadoop tools”)

This is the real Proof of Concept. Sample files alone are not enough — run these on your lab cluster and save screenshots.

---

## What each tool does

| # | Tool | Job in this PoC |
|---|------|-----------------|
| 1 | **HDFS** | Land raw PDF/DOCX + AI JSON; store curated/quarantine outputs |
| 2 | **Spark** | Read JSON, validate required fields, write curated parquet/CSV + quarantine |
| 3 | **Hive** | SQL analytics: counts, formats, regions, averages, quarantine detail |

Excel fill (`scripts/map_json_to_excel.py`) is the business delivery step **after** the Big Data pipeline.

---

## Copy project to the lab

From your laptop (adjust host/user):

```bash
scp -r "Simulated Learning/poc" <lab-user>@<lab-host>:~/situated_learning_poc
```

SSH into the lab:

```bash
ssh <lab-user>@<lab-host>
cd ~/situated_learning_poc
```

---

## One-command run

```bash
chmod +x cluster/*.sh
# optional: export HDFS_BASE=/user/YOUR_ID/situated_learning/contracts
./cluster/04_run_all.sh
```

Or step-by-step:

```bash
export HDFS_BASE=/user/$(whoami)/situated_learning/contracts
export LOCAL_DATA=$PWD/sample_data

# 1) HDFS
./cluster/01_hdfs_ingest.sh

# 2) Spark
spark-submit cluster/02_spark_validate_map.py

# 3) Hive
sed "s|__HDFS_BASE__|$HDFS_BASE|g" cluster/03_hive_analytics.hql > /tmp/hive_poc.hql
hive -f /tmp/hive_poc.hql
# or: beeline -u jdbc:hive2://localhost:10000 -f /tmp/hive_poc.hql
```

---

## Screenshots checklist (for submission)

Save under `screenshots/` (create on lab or copy back):

1. **HDFS** — `hdfs dfs -ls -R $HDFS_BASE`
2. **HDFS** — `hdfs dfs -du -h -s $HDFS_BASE/*`
3. **Spark** — curated `show()` output
4. **Spark** — quarantine row (`SYN-2024-008` missing broker)
5. **Spark** — metrics table (total/valid/invalid/success %)
6. **Hive** — Q1/Q2 counts
7. **Hive** — Q3 format split + Q8 quarantine detail
8. **Excel** — filled template from `sample_data/exports/` (optional but nice)

---

## Expected PoC result

- 10 contracts ingested (5 PDF + 5 DOCX)
- Spark: **9 VALID**, **1 INVALID** (SYN-2024-008 missing `broker`)
- Hive: queries return same split + region/plan stats

---

## If something fails on your lab

| Issue | Fix |
|-------|-----|
| `hdfs: command not found` | Load module / use cluster’s Hadoop env script (`source /etc/hadoop/...` or lab notes) |
| Spark JSON read empty | Confirm `hdfs dfs -ls $HDFS_BASE/extracted/json` has files |
| Hive table empty | Spark must finish first; check `hdfs dfs -ls $HDFS_BASE/curated/csv` |
| `OpenCSVSerde` error | Ask lab admin, or switch Hive table to `STORED AS PARQUET` pointing at `curated/parquet` |
| Permission denied on HDFS | Use your own `/user/$(whoami)/...` path |

### Hive on Parquet (alternate DDL)

```sql
CREATE EXTERNAL TABLE contract_extractions_parquet (
  contract_id STRING, client_name STRING, effective_date STRING,
  termination_date STRING, plan_type STRING, network_name STRING,
  coverage_limit BIGINT, deductible BIGINT, copay BIGINT,
  region STRING, broker STRING, status STRING, source_format STRING,
  extraction_status STRING, missing_fields STRING, run_id STRING,
  source_file STRING, extracted_at STRING, validation_status STRING
)
STORED AS PARQUET
LOCATION '/user/YOUR_ID/situated_learning/contracts/curated/parquet';
```

---

## After cluster run

1. Copy screenshots into the assignment folder.  
2. Re-export proposal/PPT to PDF if you add a “PoC screenshots” slide.  
3. Submit on Taxila by **6 Aug 2026**.
