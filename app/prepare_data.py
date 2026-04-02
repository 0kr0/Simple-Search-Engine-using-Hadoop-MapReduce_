"""
Phase 1 — Data preparation (PySpark only, no Pandas).

1) Read parquet (HDFS or local URI via PREPARE_PARQUET_PATH).
2) Keep id, title, text; drop empty text; sample PREPARE_NUM_DOCS rows (default 1000).
3) Write local UTF-8 plain text files: data/<doc_id>_<title_with_spaces_as_underscores>.txt
4) hdfs dfs -put those files to PREPARE_HDFS_DATA_DIR (default /data).
5) Read /data on HDFS via RDD, emit doc_id\tdoc_title\tdoc_text (single line text),
   coalesce(1), save to PREPARE_HDFS_INPUT_DIR (default /input/data).
"""
from __future__ import annotations

import glob
import os
import subprocess
import sys

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def hdfs_dfs(args: list[str], check: bool = True) -> None:
    subprocess.run(["hdfs", "dfs", *args], check=check)


def doc_filename(doc_id: str, title: str) -> str:
    title_underscored = title.replace(" ", "_")
    base = sanitize_filename(f"{doc_id}_{title_underscored}")
    return f"{base}.txt"


def basename_from_path(path: str) -> str:
    # wholeTextFiles paths may be hdfs://host:port/data/foo.txt or file:/...
    if "://" in path:
        path = path.rsplit("/", 1)[-1]
    else:
        path = os.path.basename(path)
    return path


def path_to_id_title(filename: str) -> tuple[str, str] | None:
    if not filename.endswith(".txt"):
        return None
    stem = filename[:-4]
    i = stem.find("_")
    if i <= 0:
        return None
    return stem[:i], stem[i + 1 :]


def write_local_corpus(rows, local_dir: str) -> None:
    os.makedirs(local_dir, exist_ok=True)
    for name in os.listdir(local_dir):
        if name.endswith(".txt"):
            os.remove(os.path.join(local_dir, name))
    for row in rows:
        doc_id = str(row["id"])
        title = row["title"] if row["title"] is not None else ""
        title = title if isinstance(title, str) else str(title)
        text = row["text"] if row["text"] is not None else ""
        text = text if isinstance(text, str) else str(text)
        fname = doc_filename(doc_id, title)
        path = os.path.join(local_dir, fname)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)


def put_txt_files_to_hdfs(local_dir: str, hdfs_dir: str) -> None:
    files = glob.glob(os.path.join(local_dir, "*.txt"))
    if not files:
        print("No .txt files written under local data directory.", file=sys.stderr)
        sys.exit(1)
    hdfs_dfs(["-rm", "-r", "-f", hdfs_dir], check=False)
    hdfs_dfs(["-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", *files, hdfs_dir], check=True)


def to_tsv_line(record) -> str | None:
    path, content = record
    name = basename_from_path(path)
    parsed = path_to_id_title(name)
    if parsed is None:
        return None
    doc_id, doc_title = parsed
    text = content
    if not isinstance(text, str):
        text = str(text)
    # One line per document for MapReduce input: normalize tabs / newlines
    text = text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    doc_title = doc_title.replace("\t", " ")
    return f"{doc_id}\t{doc_title}\t{text}"


def main() -> None:
    parquet_path = os.environ.get("PREPARE_PARQUET_PATH", "/a.parquet")
    local_data_dir = os.environ.get("PREPARE_LOCAL_DATA_DIR", "data")
    hdfs_data_dir = os.environ.get("PREPARE_HDFS_DATA_DIR", "/data")
    hdfs_input_dir = os.environ.get("PREPARE_HDFS_INPUT_DIR", "/input/data")
    n_docs = _env_int("PREPARE_NUM_DOCS", 1000)
    spark_master = os.environ.get("SPARK_MASTER", "yarn")

    spark = (
        SparkSession.builder.appName("data_preparation")
        .master(spark_master)
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext

    df = spark.read.parquet(parquet_path)
    for col in ("id", "title", "text"):
        if col not in df.columns:
            print(f"Parquet missing required column: {col}", file=sys.stderr)
            sys.exit(1)

    df = df.select("id", "title", "text")
    df = df.filter(F.col("text").isNotNull())
    df = df.filter(F.length(F.trim(F.col("text"))) > 0)
    df = df.filter(F.col("id").isNotNull())
    df = df.filter(F.col("title").isNotNull())
    df = df.filter(F.length(F.trim(F.col("title"))) > 0)

    total = df.count()
    if total == 0:
        print("No rows left after filtering.", file=sys.stderr)
        sys.exit(1)

    take_n = min(n_docs, total)
    sample_seed = _env_int("PREPARE_SAMPLE_SEED", 42)
    df_sample = df.orderBy(F.rand(sample_seed)).limit(take_n)

    rows = df_sample.collect()
    if len(rows) < take_n:
        print(f"Expected {take_n} rows, got {len(rows)}.", file=sys.stderr)
        sys.exit(1)

    write_local_corpus(rows, local_data_dir)
    put_txt_files_to_hdfs(local_data_dir, hdfs_data_dir)

    read_glob = f"{hdfs_data_dir}/*.txt"
    lines = sc.wholeTextFiles(read_glob).map(to_tsv_line).filter(lambda x: x is not None)

    hdfs_dfs(["-rm", "-r", "-f", hdfs_input_dir], check=False)
    lines.coalesce(1).saveAsTextFile(hdfs_input_dir)

    spark.stop()


if __name__ == "__main__":
    main()
