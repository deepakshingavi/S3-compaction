# Glue S3 Parquet Compaction Utility

## Overview

This AWS Glue job utility is designed to perform **S3 compaction** of plain Parquet files stored in Amazon S3 and managed via the **AWS Glue Data Catalog**. It merges small files from partitions of a Glue Catalog table into larger, optimized Parquet files to improve query performance and reduce costs.

---

## Features

- Supports compaction of Parquet data for Glue Catalog tables.
- Handles partitioned and non-partitioned datasets.
- Preserves schema and table metadata.
- Optionally deletes original small files after successful compaction.
- Customizable output file size.
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

## Estimation Guide for File Sizing

To help choose an appropriate target file size (e.g., 128MB, 256MB), you can estimate how many records will fit into a compacted file:

### 1. Download a Sample Data File

Download a sample Parquet file from your existing S3 storage. Preferably select a larger file (in MBs) to ensure a more accurate record count.

### 2. Extract Total Records from the Parquet Footer

Use the following command to extract the total record count from the Parquet file's footer:

```bash
parquet footer temp/data-file.c000.snappy.parquet | jq '[.blocks[].rowCount] | add'
