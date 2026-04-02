#!/usr/bin/env python3
"""MapReduce reducer: term + occurrences -> term\\tdoc_id:tf,doc_id:tf,..."""
from __future__ import annotations

import sys

_CURRENT: str | None = None
_COUNTS: dict[str, int] = {}


def _flush() -> None:
    global _CURRENT, _COUNTS
    if _CURRENT is not None and _COUNTS:
        postings = ",".join(f"{d}:{c}" for d, c in sorted(_COUNTS.items(), key=lambda x: x[0]))
        sys.stdout.write(f"{_CURRENT}\t{postings}\n")
    _COUNTS = {}


def main() -> None:
    global _CURRENT
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        term, doc_id, cnt_s = parts[0], parts[1], parts[2]
        try:
            cnt = int(cnt_s)
        except ValueError:
            continue
        if term != _CURRENT:
            _flush()
            _CURRENT = term
        _COUNTS[doc_id] = _COUNTS.get(doc_id, 0) + cnt
    _flush()


if __name__ == "__main__":
    main()
