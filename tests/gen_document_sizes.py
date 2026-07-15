#!/usr/bin/env python3
"""
Generate a Singer `documents` stream that emulates a real pipeline: a large volume
of small rows with a handful of big documents scattered THROUGH them. Stresses the
storage_write path on both volume and size:

  - ~1000 small rows       -> throughput / batching (the 10k+ rows/min goal)
  - big docs interleaved   -> batches that cross 20 MB          -> request SPLITTING
  - an ~18 MB row          -> fits one AppendRows chunk (near the limit)
  - a >20 MB row           -> cannot fit any AppendRows         -> LOAD JOB fallback

All fields are scalars, so this isolates SIZE/VOLUME behaviour (no JSON-column dependency).

Usage:
    python gen_document_sizes.py /path/to/out.jsonl [num_small_rows=1000]
"""
import json, sys, os

# big documents (MB, label) — spread evenly across the small rows
BIG = [
    (1.0,  "1 MB document"),
    (5.0,  "5 MB document"),
    (5.0,  "5 MB document"),
    (8.0,  "8 MB document"),
    (18.0, "18 MB — near per-request limit"),
    (25.0, "25 MB — OVERSIZED, Load Job fallback"),
    (12.0, "12 MB document"),
]

# weighted mix used when a doc count is requested (3rd CLI arg): per 100 docs —
# mostly routine sizes, a few near-limit, a few oversized (Load Job fallback)
BIG_DISTRIBUTION = [
    (1.0, 40), (3.0, 20), (5.0, 15), (8.0, 10),
    (12.0, 5), (15.0, 4),
    (18.0, 3),   # near per-request limit, must pack as one chunk
    (25.0, 3),   # OVERSIZED -> Load Job fallback
]


def build_big_list(num_big):
    """Expand BIG_DISTRIBUTION proportionally to num_big docs, interleaved by size."""
    docs = []
    for size, weight in BIG_DISTRIBUTION:
        count = max(1, round(num_big * weight / 100))
        label = ("%.0f MB — OVERSIZED, Load Job fallback" % size if size > 19.5
                 else "%.0f MB — near per-request limit" % size if size >= 18
                 else "%.0f MB document" % size)
        docs.extend([(size, label)] * count)
    # deterministic small/large alternation so sizes are spread, not clustered
    docs.sort(key=lambda d: d[0])
    interleaved, lo, hi = [], 0, len(docs) - 1
    while lo <= hi:
        interleaved.append(docs[lo]); lo += 1
        if lo <= hi:
            interleaved.append(docs[hi]); hi -= 1
    return interleaved

# small rows cycle through these sizes in MB (~200 B .. ~2 KB)
SMALL_SIZES = [0.0002, 0.0004, 0.0008, 0.0015, 0.002, 0.0006]

SCHEMA = {
    "type": "SCHEMA", "stream": "documents",
    "schema": {"type": "object", "properties": {
        "id":          {"type": ["integer", "null"]},
        "filename":    {"type": ["string", "null"]},
        "mime_type":   {"type": ["string", "null"]},
        "size_bytes":  {"type": ["integer", "null"]},
        "uploaded_at": {"type": ["string", "null"], "format": "date-time"},
        "client_id":   {"type": ["string", "null"]},
        "content":     {"type": ["string", "null"]},
    }},
    "key_properties": ["id"],
}

def content_of(mb):
    n = int(mb * 1024 * 1024)
    return "D" * max(1, n - 300)   # ~300 B reserved for the other fields + framing

def build_sequence(num_small, big_docs):
    """Interleave the big docs evenly among `num_small` small rows."""
    seq = []                                   # list of (mb, label)
    gap = max(1, num_small // (len(big_docs) + 1))
    bi = 0
    for i in range(num_small):
        seq.append((SMALL_SIZES[i % len(SMALL_SIZES)], "small"))
        if bi < len(big_docs) and (i + 1) % gap == 0:
            seq.append(big_docs[bi]); bi += 1
    while bi < len(big_docs):                   # any leftover bigs
        seq.append(big_docs[bi]); bi += 1
    return seq

def main(out, num_small, num_big=None):
    big_docs = build_big_list(num_big) if num_big else BIG
    seq = build_sequence(num_small, big_docs)
    big_positions = []
    with open(out, "w") as f:
        f.write(json.dumps(SCHEMA) + "\n")
        for idx, (mb, label) in enumerate(seq, start=1):
            f.write(json.dumps({"type": "RECORD", "stream": "documents", "record": {
                "id": idx,
                "filename": f"doc_{idx}.pdf",
                "mime_type": "application/pdf",
                "size_bytes": int(mb * 1024 * 1024),
                "uploaded_at": "2026-07-01T10:00:00+00:00",
                "client_id": f"client-{idx % 50:03d}",
                "content": content_of(mb),
            }}) + "\n")
            if label != "small":
                big_positions.append((idx, mb, label))
        f.write(json.dumps({"type": "STATE", "value": {"bookmarks":
                 {"documents": {"replication_key_value": len(seq)}}}}) + "\n")
    total = os.path.getsize(out)
    n_small = len(seq) - len(big_positions)
    print(f"wrote {out}")
    print(f"  {len(seq)} records ({n_small} small + {len(big_positions)} big) · {total/1024/1024:.1f} MB\n")
    print(f"  big documents (interleaved):")
    print(f"  {'id':>5} {'size':>10}   purpose")
    if len(big_positions) > 20:
        from collections import Counter
        by_size = Counter(f"{mb:.0f} MB" for _, mb, _ in big_positions)
        for size, count in sorted(by_size.items(), key=lambda kv: float(kv[0].split()[0])):
            print(f"  {count:>3}x {size:>9}")
        n_over = sum(1 for _, mb, _ in big_positions if mb > 19.5)
        print(f"  ({n_over} oversized -> Load Job fallback)")
    else:
        for idx, mb, label in big_positions:
            print(f"  {idx:>5} {mb:>7.1f} MB   {label}")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "document_stream.jsonl"
    num_small = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    num_big = int(sys.argv[3]) if len(sys.argv) > 3 else None
    main(out, num_small, num_big)
