#!/usr/bin/env python3
"""
Tool 2: Apache Spark — validate AI extraction JSON, map fields, write curated data.

Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
Course: Big Data Systems (CC ZG522) — Situated Learning PoC

Run on lab cluster:
  spark-submit poc/cluster/02_spark_validate_map.py

Optional env:
  HDFS_BASE=/user/<you>/situated_learning/contracts
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REQUIRED_FIELDS = [
    "contract_id",
    "client_name",
    "effective_date",
    "termination_date",
    "plan_type",
    "network_name",
    "coverage_limit",
    "deductible",
    "copay",
    "region",
    "broker",
    "status",
]


def main() -> int:
    hdfs_base = os.environ.get(
        "HDFS_BASE",
        f"/user/{os.environ.get('USER', 'student')}/situated_learning/contracts",
    )
    json_path = f"{hdfs_base}/extracted/json/*.json"
    curated_parquet = f"{hdfs_base}/curated/parquet"
    curated_csv = f"{hdfs_base}/curated/csv"
    quarantine = f"{hdfs_base}/quarantine"
    metrics_path = f"{hdfs_base}/logs/spark_metrics"

    spark = (
        SparkSession.builder.appName("SituatedLearning_ContractValidateMap")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"==> Reading JSON from {json_path}")
    raw = spark.read.option("multiLine", True).json(json_path)

    # Flatten nested AI extraction payload
    flat = raw.select(
        F.col("fields.contract_id").alias("contract_id"),
        F.col("fields.client_name").alias("client_name"),
        F.col("fields.effective_date").alias("effective_date"),
        F.col("fields.termination_date").alias("termination_date"),
        F.col("fields.plan_type").alias("plan_type"),
        F.col("fields.network_name").alias("network_name"),
        F.col("fields.coverage_limit").cast("long").alias("coverage_limit"),
        F.col("fields.deductible").cast("long").alias("deductible"),
        F.col("fields.copay").cast("long").alias("copay"),
        F.col("fields.region").alias("region"),
        F.col("fields.broker").alias("broker"),
        F.col("fields.status").alias("status"),
        F.col("source_format"),
        F.col("extraction_status"),
        F.concat_ws(",", F.col("missing_fields")).alias("missing_fields"),
        F.col("run_id"),
        F.col("source_file"),
        F.col("extracted_at"),
    )

    # Validation: required fields must be non-null / non-empty
    cond = None
    for field in REQUIRED_FIELDS:
        c = F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == "")
        cond = c if cond is None else (cond | c)

    invalid = flat.filter(cond)
    valid = flat.filter(~cond).withColumn("validation_status", F.lit("VALID"))
    invalid = invalid.withColumn("validation_status", F.lit("INVALID"))

    print("==> Sample curated rows (screenshot this)")
    valid.show(20, truncate=False)

    print("==> Quarantine / invalid rows (screenshot this)")
    invalid.show(20, truncate=False)

    total = flat.count()
    ok = valid.count()
    bad = invalid.count()
    print(f"==> Totals: total={total}, valid={ok}, invalid={bad}")

    print(f"==> Writing curated parquet -> {curated_parquet}")
    (
        valid.write.mode("overwrite")
        .format("parquet")
        .save(curated_parquet)
    )

    print(f"==> Writing curated CSV -> {curated_csv}")
    (
        valid.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(curated_csv)
    )

    print(f"==> Writing quarantine -> {quarantine}")
    (
        invalid.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(quarantine)
    )

    metrics = spark.createDataFrame(
        [
            ("total_contracts", float(total)),
            ("valid_contracts", float(ok)),
            ("invalid_contracts", float(bad)),
            ("success_rate_pct", round(100.0 * ok / total, 2) if total else 0.0),
        ],
        ["metric", "value"],
    )
    print("==> Spark metrics (screenshot this)")
    metrics.show(truncate=False)
    metrics.coalesce(1).write.mode("overwrite").option("header", True).csv(metrics_path)

    # Optional: register temp view for quick SQL demo inside Spark
    valid.createOrReplaceTempView("contract_extractions_valid")
    print("==> Spark SQL by format (screenshot this)")
    spark.sql(
        """
        SELECT source_format, COUNT(*) AS cnt
        FROM contract_extractions_valid
        GROUP BY source_format
        ORDER BY source_format
        """
    ).show()

    spark.stop()
    print("==> Done: Spark validate/map complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
