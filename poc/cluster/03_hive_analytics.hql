-- Tool 3: Apache Hive analytics (simplified for lab MR stability)
-- Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
--
-- Replace __HDFS_BASE__ before running, e.g.:
--   sed "s|__HDFS_BASE__|$HDFS_BASE|g" 03_hive_analytics.hql > /tmp/hive_poc.hql
--   hive -f /tmp/hive_poc.hql

CREATE DATABASE IF NOT EXISTS situated_learning;
USE situated_learning;

DROP TABLE IF EXISTS contract_extractions;
CREATE EXTERNAL TABLE contract_extractions (
  contract_id        STRING,
  client_name        STRING,
  effective_date     STRING,
  termination_date   STRING,
  plan_type          STRING,
  network_name       STRING,
  coverage_limit     BIGINT,
  deductible         BIGINT,
  copay              BIGINT,
  region             STRING,
  broker             STRING,
  status             STRING,
  source_format      STRING,
  extraction_status  STRING,
  missing_fields     STRING,
  run_id             STRING,
  source_file        STRING,
  extracted_at       STRING,
  validation_status  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '__HDFS_BASE__/curated/csv'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS contract_quarantine;
CREATE EXTERNAL TABLE contract_quarantine (
  contract_id        STRING,
  client_name        STRING,
  effective_date     STRING,
  termination_date   STRING,
  plan_type          STRING,
  network_name       STRING,
  coverage_limit     STRING,
  deductible         STRING,
  copay              STRING,
  region             STRING,
  broker             STRING,
  status             STRING,
  source_format      STRING,
  extraction_status  STRING,
  missing_fields     STRING,
  run_id             STRING,
  source_file        STRING,
  extracted_at       STRING,
  validation_status  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '__HDFS_BASE__/quarantine'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Prefer fetch / simple plans on small PoC data
SET hive.fetch.task.conversion=more;
SET hive.exec.mode.local.auto=true;

-- Q1: overall volume
SELECT 'Q1_total_valid' AS query_id, COUNT(*) AS total_valid
FROM contract_extractions;

-- Q2a / Q2b: split (avoid UNION ALL MR failure on some labs)
SELECT 'Q2_valid' AS bucket, COUNT(*) AS cnt FROM contract_extractions;
SELECT 'Q2_quarantine' AS bucket, COUNT(*) AS cnt FROM contract_quarantine;

-- Q3: split by source format
SELECT source_format, COUNT(*) AS contracts
FROM contract_extractions
GROUP BY source_format
ORDER BY source_format;

-- Q4: contracts by region
SELECT region, COUNT(*) AS contracts
FROM contract_extractions
GROUP BY region
ORDER BY contracts DESC;

-- Q5: average coverage / deductible
SELECT
  ROUND(AVG(coverage_limit), 2) AS avg_coverage_limit,
  ROUND(AVG(deductible), 2) AS avg_deductible,
  ROUND(AVG(copay), 2) AS avg_copay
FROM contract_extractions;

-- Q6: plan type distribution
SELECT plan_type, COUNT(*) AS contracts
FROM contract_extractions
GROUP BY plan_type
ORDER BY contracts DESC;

-- Q7: sample curated extract
SELECT contract_id, client_name, plan_type, network_name, status, source_format
FROM contract_extractions
ORDER BY contract_id
LIMIT 20;

-- Q8: quarantine detail
SELECT contract_id, client_name, missing_fields, extraction_status, validation_status
FROM contract_quarantine;
