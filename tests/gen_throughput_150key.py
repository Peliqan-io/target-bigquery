#!/usr/bin/env python3
"""
Reproduce the upstream storage_write.py benchmark shape:

    "Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB"

Emits N rows of 150 scalar columns (~1.5 KB JSON each -> ~1.5 GB per 1M rows),
streamed row by row so any count is memory-flat. Field mix cycles int / short
string / float. With batch_size=20000 the serialized batches land around the
19.5 MB request budget, so the splitter is exercised continuously — the
realistic wide-table production shape.

Usage:
    python tests/gen_throughput_150key.py 1000000 | target-bigquery --config config.json
    python tests/gen_throughput_150key.py 1000000 out.jsonl
"""
import json
import sys

N_KEYS = 150
STREAM = "throughput_150key"

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _b36(n):
    s = ""
    while True:
        n, r = divmod(n, 36)
        s = _B36[r] + s
        if n == 0:
            return s


def field_type(k: int) -> dict:
    return [
        {"type": ["integer", "null"]},
        {"type": ["string", "null"]},
        {"type": ["number", "null"]},
    ][k % 3]


SCHEMA = {
    "type": "SCHEMA",
    "stream": STREAM,
    "schema": {"type": "object", "properties": {
        "id": {"type": ["integer", "null"]},
        **{f"f{k}": field_type(k) for k in range(N_KEYS)},
    }},
    "key_properties": ["id"],
}


def gen(count: int, fh) -> None:
    fh.write(json.dumps(SCHEMA) + "\n")
    for i in range(1, count + 1):
        record = {"id": i}
        for k in range(N_KEYS):
            t = k % 3
            if t == 0:
                record[f"f{k}"] = (i * 31 + k * 7) % 1_000
            elif t == 1:
                record[f"f{k}"] = _b36((i * 131 + k * 17) % 46_656)  # <=3 chars
            else:
                record[f"f{k}"] = round(((i * 13 + k) % 1_000) / 10, 1)
        fh.write(json.dumps({"type": "RECORD", "stream": STREAM, "record": record}, separators=(",", ":")) + "\n")
        if i % 100_000 == 0:
            fh.write(json.dumps({"type": "STATE", "value": {"bookmarks": {
                STREAM: {"replication_key_value": i}}}}) + "\n")
            print(f"  ... {i:,} rows emitted", file=sys.stderr, flush=True)
    fh.write(json.dumps({"type": "STATE", "value": {"bookmarks": {
        STREAM: {"replication_key_value": count}}}}) + "\n")


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    if len(sys.argv) > 2:
        with open(sys.argv[2], "w") as fh:
            gen(count, fh)
        print(f"wrote {sys.argv[2]} ({count:,} rows x {N_KEYS} keys)", file=sys.stderr)
    else:
        gen(count, sys.stdout)
