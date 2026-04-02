#!/bin/bash
set -euo pipefail

cd /app
source .venv/bin/activate

echo "Waiting for Cassandra, then creating schema and loading index from HDFS."
python3 app.py
echo "Cassandra load finished."
