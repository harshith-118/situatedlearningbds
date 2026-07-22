-- Tool 3: Apache Hive — curated analytics over Spark/HDFS outputs
-- Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
--
-- BEFORE RUNNING: replace __HDFS_BASE__ with your path, e.g.
--   /user/2024DC04035/situated_learning/contracts
-- Or export and sed:
--   sed "s|__HDFS_BASE__|$HDFS_BASE|g" 03_hive_analytics.hql | hive -f -

CREATE DATABASE IF NOT EXISTS situated_learning;
USE situated_learning;

-- External table over Spark curated CSV (header handled by skipping / or use parquet)
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

-- Quarantine / invalid extractions
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

-- ========== Analytics queries (screenshot each result) ==========

-- Q1: overall volume
SELECT 'Q1_total_valid' AS query_id, COUNT(*) AS total_valid
FROM contract_extractions;

-- Q2: success vs quarantine
SELECT 'Q2_valid' AS bucket, COUNT(*) AS cnt FROM contract_extractions
UNION ALL
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
  ROUND(AVG(CAST(coverage_limit AS DOUBLE)), 2) AS avg_coverage_limit,
  ROUND(AVG(CAST(deductible AS DOUBLE)), 2) AS avg_deductible,
  ROUND(AVG(CAST(copay AS DOUBLE)), 2) AS avg_copay
FROM contract_extractions;

-- Q6: plan type distribution
SELECT plan_type, COUNT(*) AS contracts
FROM contract_extractions
GROUP BY plan_type
ORDER BY contracts DESC;

-- Q7: sample curated extract (business-facing preview)
SELECT contract_id, client_name, plan_type, network_name, status, source_format
FROM contract_extractions
ORDER BY contract_id
LIMIT 20;

-- Q8: quarantine detail (shows missing broker case)
SELECT contract_id, client_name, missing_fields, extraction_status, validation_status
FROM contract_quarantine;
