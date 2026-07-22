# Situated Learning — Big Data Systems (CC ZG522)

**Student:** DEVATA SAI HARSHITH  
**BITS ID:** 2024DC04035  

AI-based benefits contract extraction pipeline on the Hadoop ecosystem (**HDFS + Spark + Hive**), with synthetic PoC data, Excel template population, and lab-cluster screenshots.

## Deliverables

| File | Description |
|------|-------------|
| `DEVATA_SAI_HARSHITH_2024DC04035_Proposal.docx` | Project proposal |
| `DEVATA_SAI_HARSHITH_2024DC04035_Presentation.pptx` | 10-slide presentation (includes PoC evidence) |
| `poc/` | Sample data, Excel script, cluster scripts |
| `screenshots/` | Lab PoC evidence (HDFS / Spark / Hive) |

## PoC results (lab cluster)

- **10** synthetic contracts (5 PDF + 5 DOCX)
- Spark: **9 VALID**, **1 INVALID** (SYN-2024-008 missing `broker`) ? **90%** success
- Hive analytics on curated + quarantine tables

See `screenshots/INDEX.md` for the labeled screenshot list.

## Run on lab

```bash
cd poc
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HDFS_BASE=/user/$(whoami)/situated_learning/contracts
export HDFS_NAMENODE=hdfs://hadoop-master:9000
export LOCAL_DATA=$PWD/sample_data
./cluster/01_hdfs_ingest.sh
spark-submit --master yarn --deploy-mode client cluster/02_spark_validate_map.py
# then simple Hive queries (see cluster/RUNBOOK.md)
```

## Note

All sample contracts are **synthetic**. No real client / LifeSource documents are included.
