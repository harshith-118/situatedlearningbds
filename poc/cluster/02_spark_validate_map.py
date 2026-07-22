#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tool 2: Apache Spark - validate AI extraction JSON, map fields, write curated data.

Student: DEVATA SAI HARSHITH | BITS ID: 2024DC04035
Course: Big Data Systems (CC ZG522) - Situated Learning PoC

Run on lab cluster:
  spark-submit poc/cluster/02_spark_validate_map.py

Optional env:
  HDFS_BASE=/user/<you>/situated_learning/contracts
"""

from __future__ import print_function

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


def main():
    hdfs_base = os.environ.get(
        "HDFS_BASE",
        "/user/{0}/situated_learning/contracts".format(
            os.environ.get("USER", "student")
        ),
    )
    json_path = "{0}/extracted/json/*.json".format(hdfs_base)
    curated_parquet = "{0}/curated/parquet".format(hdfs_base)
    curated_csv = "{0}/curated/csv".format(hdfs_base)
    quarantine = "{0}/quarantine".format(hdfs_base)
    metrics_path = "{0}/logs/spark_metrics".format(hdfs_base)

    spark = (
        SparkSession.builder.appName("SituatedLearning_ContractValidateMap")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("==> Reading JSON from {0}".format(json_path))
    raw = spark.read.option("multiLine", True).json(json_path)

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

    cond = None
    for field in REQUIRED_FIELDS:
        c = F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == "")
        cond = c if cond is None else (cond | c)

    invalid = flat.filter(cond).withColumn("validation_status", F.lit("INVALID"))
    valid = flat.filter(~cond).withColumn("validation_status", F.lit("VALID"))

    print("==> Sample curated rows (screenshot this)")
    valid.show(20, truncate=False)

    print("==> Quarantine / invalid rows (screenshot this)")
    invalid.show(20, truncate=False)

    total = flat.count()
    ok = valid.count()
    bad = invalid.count()
    print("==> Totals: total={0}, valid={1}, invalid={2}".format(total, ok, bad))

    print("==> Writing curated parquet -> {0}".format(curated_parquet))
    valid.write.mode("overwrite").format("parquet").save(curated_parquet)

    print("==> Writing curated CSV -> {0}".format(curated_csv))
    valid.coalesce(1).write.mode("overwrite").option("header", True).csv(curated_csv)

    print("==> Writing quarantine -> {0}".format(quarantine))
    invalid.coalesce(1).write.mode("overwrite").option("header", True).csv(quarantine)

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
