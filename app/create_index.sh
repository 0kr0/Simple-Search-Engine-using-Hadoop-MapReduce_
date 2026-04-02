#!/bin/bash
set -euo pipefail

cd /app

STREAM_JAR="$(ls -1 "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)"
if [[ -z "${STREAM_JAR}" ]]; then
  echo "Could not find hadoop-streaming jar under HADOOP_HOME=${HADOOP_HOME}" >&2
  exit 1
fi

INPUT_PATH="${INDEX_INPUT:-/input/data}"
OUT_INV="${INDEX_INVERTED:-/index/inverted}"
OUT_DOC="${INDEX_DOCS:-/index/doc_stats}"

echo "Building inverted index: input=${INPUT_PATH} output=${OUT_INV}"
hdfs dfs -rm -r -f "$OUT_INV"
hadoop jar "$STREAM_JAR" \
  -D mapreduce.job.name="build_inverted_index" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$OUT_INV"

echo "Building doc stats: output=${OUT_DOC}"
hdfs dfs -rm -r -f "$OUT_DOC"
hadoop jar "$STREAM_JAR" \
  -D mapreduce.job.name="build_doc_stats" \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper2.py,mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input "$INPUT_PATH" \
  -output "$OUT_DOC"

echo "HDFS ${OUT_INV}:"
hdfs dfs -ls "$OUT_INV"
echo "HDFS ${OUT_DOC}:"
hdfs dfs -ls "$OUT_DOC"
echo "MapReduce index jobs finished."
