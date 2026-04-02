#!/bin/bash
set -euo pipefail

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

# Local parquet path (inside /app in the container). Override if your file lives elsewhere.
PREPARE_PARQUET_LOCAL="${PREPARE_PARQUET_LOCAL:-data/a.parquet}"
# HDFS path Spark will read via spark.read.parquet(...)
PREPARE_PARQUET_PATH="${PREPARE_PARQUET_PATH:-/a.parquet}"

if [ ! -f "$PREPARE_PARQUET_LOCAL" ]; then
  echo "Parquet not found at $PREPARE_PARQUET_LOCAL" >&2
  echo "Place a.parquet there or set PREPARE_PARQUET_LOCAL to your file path." >&2
  exit 1
fi

hdfs dfs -put -f "$PREPARE_PARQUET_LOCAL" "$PREPARE_PARQUET_PATH"
export PREPARE_PARQUET_PATH

SPARK_MASTER="${SPARK_MASTER:-yarn}"
export SPARK_MASTER

spark-submit --master "$SPARK_MASTER" --deploy-mode client prepare_data.py

echo "HDFS ${PREPARE_HDFS_DATA_DIR:-/data}:"
hdfs dfs -ls /data
echo "HDFS ${PREPARE_HDFS_INPUT_DIR:-/input/data}:"
hdfs dfs -ls /input/data
echo "Data preparation finished."
