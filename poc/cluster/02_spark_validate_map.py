#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Situated Learning PoC - Spark validate/map (HDFS forced)."""

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
    # Force HDFS. Override with env if your namenode port differs.
    namenode = os.environ.get("HDFS_NAMENODE", "hdfs://hadoop-master:9000")
    base = os.environ.get("HDFS_BASE", "/user/cloud/situated_learning/contracts")
    if base.startswith("hdfs://"):
        # keep only the path part
        base = "/" + base.split("/", 3)[-1]

    json_dir = "{0}{1}/extracted/json".format(namenode.rstrip("/"), base)
    curated_parquet = "{0}{1}/curated/parquet".format(namenode.rstrip("/"), base)
    curated_csv = "{0}{1}/curated/csv".format(namenode.rstrip("/"), base)
    quarantine = "{0}{1}/quarantine".format(namenode.rstrip("/"), base)
    metrics_path = "{0}{1}/logs/spark_metrics".format(namenode.rstrip("/"), base)

    spark = (
        SparkSession.builder.appName("SituatedLearning_ContractValidateMap")
        .config("spark.hadoop.fs.defaultFS", namenode)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("==> Reading JSON from {0}".format(json_dir))
    raw = spark.read.option("multiLine", "true").json(json_dir)

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
    valid.coalesce(1).write.mode("overwrite").option("header", "true").csv(curated_csv)

    print("==> Writing quarantine -> {0}".format(quarantine))
    invalid.coalesce(1).write.mode("overwrite").option("header", "true").csv(quarantine)

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
    metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(metrics_path)

    valid.createOrReplaceTempView("contract_extractions_valid")
    print("==> Spark SQL by format (screenshot this)")
    spark.sql(
        "SELECT source_format, COUNT(*) AS cnt "
        "FROM contract_extractions_valid GROUP BY source_format ORDER BY source_format"
    ).show()

    spark.stop()
    print("==> Done: Spark validate/map complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
