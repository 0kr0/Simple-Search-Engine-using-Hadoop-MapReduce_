#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
export PYSPARK_PYTHON=./.venv/bin/python

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <search query words>" >&2
  exit 1
fi

SPARK_MASTER="${SPARK_MASTER:-yarn}"
export SPARK_MASTER

spark-submit --master "$SPARK_MASTER" --archives /app/.venv.tar.gz#.venv query.py "$@"