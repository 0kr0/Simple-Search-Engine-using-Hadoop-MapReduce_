#!/usr/bin/env python3
"""MapReduce mapper: doc line -> doc_id\\t(doc_length)\\t(title) for doc stats."""
from __future__ import annotations

import re
import sys

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def main() -> None:
    for line in sys.stdin:
        line = line.rstrip("\n\r")
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, title, text = parts[0], parts[1], parts[2]
        dl = len(tokenize(title) + tokenize(text))
        title_clean = title.replace("\t", " ").replace("\r", " ").replace("\n", " ")
        sys.stdout.write(f"{doc_id}\t{dl}\t{title_clean}\n")


if __name__ == "__main__":
    main()
