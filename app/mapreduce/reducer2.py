#!/usr/bin/env python3
"""MapReduce reducer: one doc_id\\tdoc_length\\ttitle per document."""
from __future__ import annotations

import sys


def main() -> None:
    current: str | None = None
    out_dl: str | None = None
    out_title: str | None = None

    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        key, _, rest = line.partition("\t")
        if not key or not rest:
            continue
        dl, _, title = rest.partition("\t")
        if key != current:
            if current is not None and out_dl is not None and out_title is not None:
                sys.stdout.write(f"{current}\t{out_dl}\t{out_title}\n")
            current = key
            out_dl, out_title = dl, title
        # duplicate key: keep first
    if current is not None and out_dl is not None and out_title is not None:
        sys.stdout.write(f"{current}\t{out_dl}\t{out_title}\n")


if __name__ == "__main__":
    main()
