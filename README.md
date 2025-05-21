# Glue S3 Parquet Compaction Utility

## Overview

This AWS Glue job utility is designed to perform **S3 compaction** of plain Parquet files stored in Amazon S3 and managed via the **AWS Glue Data Catalog**. It merges small files from partitions of a Glue Catalog table into larger, optimized Parquet files to improve query performance and reduce costs.

---

## Features

- Supports compaction of Parquet data for Glue Catalog tables.
- Handles only partitioned datasets.
- Preserves schema and table metadata.
- Optionally deletes original small files after successful compaction.
- Customizable output file size (128/256 MB sized file).
- Can be scheduled or run on-demand.

---

## Prerequisites

- AWS Glue (Job & IAM Role)
- AWS Glue Data Catalog table with S3 as the source.
- Parquet data format.
- AWS SDK permissions for Glue and S3.
- Glue 4.0 environment.

---

## Configuration

The Glue job accepts the following job parameters:

| Parameter | Description | Required | Example |
|----------|-------------|----------|---------|
| `--database_name` | Glue catalog database name | ✅ | `my_database` |
| `--table_name` | Glue catalog table name | ✅ | `my_table` |
| `--target_s3_path` | (Optional) Destination S3 path for compacted files | ❌ | `s3://bucket/optimized/` |
| `--partition_predicate` | (Optional) SQL-like filter for partitions | ❌ | `year='2023' AND month='05'` |
| `--delete_original_files` | Whether to delete original files after compaction | ❌ | `true` |
| `--target_file_size_mb` | Approximate size of output files in MB | ❌ | `256` |

---

## How It Works

1. Reads metadata from the Glue Catalog.
2. Loads data from the underlying S3 Parquet files using Spark.
3. (Optional) Filters data by partition predicate.
4. Coalesces or repartitions data based on the target file size.
5. Writes compacted Parquet files to the target path.
6. (Optional) Deletes original S3 objects after successful write.

---

## Running the Job

You can run the Glue job via the AWS Console, CLI, or as part of an automated pipeline.

### Example CLI Command

```bash
aws glue start-job-run \
  --job-name s3-compaction \
  --arguments '{
    "--database_name":"my_database",
    "--table_name":"my_table",
    "--partition_predicate":"year=2023",
    "--delete_original_files":"true",
    "--target_file_size_mb":"256"
  }'
