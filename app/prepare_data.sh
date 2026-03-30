#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

hdfs dfs -put -f a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /indexer/data && \
    echo "done data preparation!"
