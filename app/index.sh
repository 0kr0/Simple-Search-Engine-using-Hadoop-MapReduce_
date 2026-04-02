#!/bin/bash
set -euo pipefail

cd /app

echo "Running MapReduce indexing, then loading into Cassandra."
bash create_index.sh
bash store_index.sh
echo "Indexing pipeline finished."
