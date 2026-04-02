"""
Create Cassandra schema and load inverted index + doc stats from HDFS MapReduce output.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


KEYSPACE = "search"
HDFS_INVERTED = os.environ.get("INDEX_INVERTED", "/index/inverted")
HDFS_DOC_STATS = os.environ.get("INDEX_DOCS", "/index/doc_stats")
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra-server").split(",")
MAX_CASSANDRA_WAIT_S = int(os.environ.get("CASSANDRA_WAIT_S", "120"))


def _connect_with_retry(hosts: list[str]):
    deadline = time.monotonic() + MAX_CASSANDRA_WAIT_S
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            cluster = Cluster(hosts, connect_timeout=10)
            session = cluster.connect()
            return cluster, session
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(3)
    print(f"Could not connect to Cassandra at {hosts}: {last_err}", file=sys.stderr)
    sys.exit(1)


def _hdfs_list_part_files(hdfs_dir: str) -> list[str]:
    proc = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_dir],
        check=True,
        capture_output=True,
        text=True,
    )
    paths: list[str] = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 8:
            continue
        p = parts[-1]
        base = os.path.basename(p)
        if base == "_SUCCESS" or base.startswith("."):
            continue
        if base.startswith("part"):
            paths.append(p)
    return sorted(paths)


def _read_hdfs_lines(hdfs_dir: str) -> list[str]:
    files = _hdfs_list_part_files(hdfs_dir)
    if not files:
        print(f"No part files under {hdfs_dir}", file=sys.stderr)
        sys.exit(1)
    lines: list[str] = []
    for path in files:
        out = subprocess.check_output(["hdfs", "dfs", "-cat", path], text=True)
        for line in out.splitlines():
            line = line.strip("\n\r")
            if line:
                lines.append(line)
    return lines


def _ensure_schema(session) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
        """
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text PRIMARY KEY,
            postings text
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS collection_stats (
            name text PRIMARY KEY,
            num_docs bigint,
            avg_doc_length double
        )
        """
    )


def _truncate_tables(session) -> None:
    for table in ("inverted_index", "doc_stats", "collection_stats"):
        session.execute(f"TRUNCATE {table}")


def _load_inverted(session, lines: list[str]) -> None:
    rows: list[tuple[str, str]] = []
    for line in lines:
        if "\t" not in line:
            continue
        term, postings = line.split("\t", 1)
        term = term.strip()
        if term:
            rows.append((term, postings))
    stmt = session.prepare("INSERT INTO inverted_index (term, postings) VALUES (?, ?)")
    execute_concurrent_with_args(session, stmt, rows, concurrency=48)


def _load_doc_stats(session, lines: list[str]) -> tuple[int, float]:
    rows: list[tuple[str, str, int]] = []
    total_len = 0
    for line in lines:
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, dl_s, title = parts[0], parts[1], parts[2]
        try:
            dl = int(dl_s)
        except ValueError:
            continue
        rows.append((doc_id, title, dl))
        total_len += dl

    stmt = session.prepare("INSERT INTO doc_stats (doc_id, title, doc_length) VALUES (?, ?, ?)")
    execute_concurrent_with_args(session, stmt, rows, concurrency=48)

    n = len(rows)
    avg = float(total_len) / n if n else 0.0
    return n, avg


def main() -> None:
    inv_lines = _read_hdfs_lines(HDFS_INVERTED)
    doc_lines = _read_hdfs_lines(HDFS_DOC_STATS)

    cluster, session = _connect_with_retry(CASSANDRA_HOSTS)
    try:
        _ensure_schema(session)
        _truncate_tables(session)

        _load_inverted(session, inv_lines)
        n_docs, avgdl = _load_doc_stats(session, doc_lines)

        session.execute(
            """
            INSERT INTO collection_stats (name, num_docs, avg_doc_length)
            VALUES (%s, %s, %s)
            """,
            ("global", n_docs, avgdl),
        )
        print(f"Loaded {len(inv_lines)} terms, {n_docs} documents (avgdl={avgdl:.4f}).")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
