"""Generate a ~75 MB singer JSONL that demonstrates the AppendRows 20 MB splitter.

Layout (designed for batch_size=500):
  - batch 1 (rows 1-500):    docs of 15, 15, 15, 12 MB  ->  ~57 MB  -> splits into 3 requests
  - batch 2 (rows 501-1000): docs of 10, 8 MB           ->  ~18 MB  -> single request (no split)

All docs stay under MAX_REQUEST_BYTES (19.5 MB) — for the oversized-row failure
path use a >19.5 MB doc instead (see RecordTooLargeError).

Usage:
    python tests/gen_split_demo.py /tmp/split_demo.jsonl
    cat /tmp/split_demo.jsonl | target-bigquery --config config.json   # batch_size: 500

Watch the target logs for:
    Batch of 500 rows (…bytes) … splitting into 3 AppendRows requests.
    Sent request 1/3: … rows (…bytes) … with offset …
"""
import json
import sys

MB = 1024 * 1024
# (row_index, doc_size_mb) — batch 1 carries ~57 MB, batch 2 ~18 MB
DOCS = {100: 15, 200: 15, 300: 15, 400: 12, 600: 10, 800: 8}
TOTAL_ROWS = 1000

SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": ["integer", "null"]},
        "name": {"type": ["string", "null"]},
        "doc": {"type": ["string", "null"]},
        "meta": {"type": ["object", "null"]},
        "tags": {"type": ["array", "null"], "items": {"type": "string"}},
    },
}


def main(out_path: str) -> None:
    with open(out_path, "w") as f:
        f.write(json.dumps({
            "type": "SCHEMA", "stream": "split_demo",
            "schema": SCHEMA, "key_properties": ["id"],
        }) + "\n")
        for i in range(1, TOTAL_ROWS + 1):
            record = {
                "id": i,
                "name": f"row-{i}",
                "meta": {"i": i, "src": "split-demo"},
                "tags": [f"t{i % 7}"],
            }
            if i in DOCS:
                record["doc"] = ("D%07d" % i) * (DOCS[i] * MB // 8)  # 8 chars * N = exact MB
            f.write(json.dumps({
                "type": "RECORD", "stream": "split_demo", "record": record,
            }) + "\n")
        f.write(json.dumps({
            "type": "STATE",
            "value": {"bookmarks": {"split_demo": {"v": TOTAL_ROWS}}},
        }) + "\n")
    import os
    print(f"wrote {out_path}: {os.path.getsize(out_path) / MB:.1f} MB, "
          f"{TOTAL_ROWS} rows, docs at {sorted(DOCS)} (MB sizes {list(DOCS.values())})")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "split_demo.jsonl")
