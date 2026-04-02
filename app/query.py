"""
BM25 search: reads index from Cassandra, scores candidates with PySpark RDD.
"""
from __future__ import annotations

import math
import os
import re
import sys
from collections import Counter

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
K1 = 1.5
B = 0.75


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def parse_postings(blob: str) -> dict[str, int]:
    out: dict[str, int] = {}
    if not blob:
        return out
    for piece in blob.split(","):
        piece = piece.strip()
        if ":" not in piece:
            continue
        doc_id, tf_s = piece.split(":", 1)
        doc_id = doc_id.strip()
        try:
            tf = int(tf_s)
        except ValueError:
            continue
        if doc_id:
            out[doc_id] = tf
    return out


def bm25_idf(n_docs: int, df: int) -> float:
    return math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))


def main() -> None:
    query_text = " ".join(sys.argv[1:]).strip()
    if not query_text:
        print("Usage: query.py words ...", file=sys.stderr)
        sys.exit(1)

    terms_all = tokenize(query_text)
    if not terms_all:
        print("No terms after tokenization.", file=sys.stderr)
        sys.exit(1)
    term_weights = Counter(terms_all)
    unique_terms = list(term_weights.keys())

    spark = (
        SparkSession.builder.appName("bm25_search")
        .master(os.environ.get("SPARK_MASTER", "yarn"))
        .getOrCreate()
    )
    sc = spark.sparkContext

    cluster = Cluster(["cassandra-server"], connect_timeout=15)
    session = cluster.connect()
    session.set_keyspace("search")

    row = session.execute(
        "SELECT num_docs, avg_doc_length FROM collection_stats WHERE name = %s",
        ("global",),
    ).one()
    if row is None:
        print("Collection stats missing. Run indexing (store_index / app.py) first.", file=sys.stderr)
        sys.exit(2)
    n_docs, avgdl = int(row.num_docs), float(row.avg_doc_length)
    if n_docs <= 0 or avgdl <= 0:
        print("Invalid collection stats (empty corpus?).", file=sys.stderr)
        sys.exit(2)

    term_data: dict[str, dict[str, int]] = {}
    idfs: dict[str, float] = {}

    for t in unique_terms:
        r = session.execute(
            "SELECT postings FROM inverted_index WHERE term = %s",
            (t,),
        ).one()
        if r is None:
            term_data[t] = {}
            idfs[t] = 0.0
            continue
        posting = parse_postings(r.postings or "")
        term_data[t] = posting
        df = len(posting)
        idfs[t] = bm25_idf(n_docs, df) if df > 0 else 0.0

    candidates: set[str] = set()
    for t in unique_terms:
        candidates.update(term_data[t].keys())

    if not candidates:
        print("No matching documents.")
        cluster.shutdown()
        spark.stop()
        return

    doc_lengths: dict[str, int] = {}
    doc_titles: dict[str, str] = {}
    for doc_id in candidates:
        dr = session.execute(
            "SELECT title, doc_length FROM doc_stats WHERE doc_id = %s",
            (doc_id,),
        ).one()
        if dr:
            doc_titles[doc_id] = dr.title or ""
            doc_lengths[doc_id] = int(dr.doc_length or 0)

    bc = sc.broadcast(
        {
            "term_weights": dict(term_weights),
            "unique_terms": unique_terms,
            "term_data": term_data,
            "idfs": idfs,
            "doc_lengths": doc_lengths,
            "avgdl": avgdl,
            "k1": K1,
            "b": B,
        }
    )

    def score_doc(doc_id: str) -> tuple[str, float]:
        d = bc.value
        dl = d["doc_lengths"].get(doc_id, 0)
        if dl <= 0:
            return doc_id, 0.0
        total = 0.0
        for t in d["unique_terms"]:
            qtf = d["term_weights"].get(t, 0)
            if qtf == 0:
                continue
            tf = d["term_data"][t].get(doc_id, 0)
            if tf == 0:
                continue
            idf = d["idfs"][t]
            if idf == 0.0:
                continue
            denom = tf + d["k1"] * (1.0 - d["b"] + d["b"] * dl / d["avgdl"])
            total += qtf * idf * (tf * (d["k1"] + 1.0)) / denom
        return doc_id, total

    ranked = (
        sc.parallelize(sorted(candidates))
        .map(score_doc)
        .takeOrdered(10, key=lambda x: -x[1])
    )

    for doc_id, score in ranked:
        print(f"{score:.6f}\t{doc_id}\t{doc_titles.get(doc_id, '')}")

    bc.destroy()
    cluster.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()
