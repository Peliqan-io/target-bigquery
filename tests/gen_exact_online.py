#!/usr/bin/env python3
"""
Generate a realistic Exact Online-style Singer stream (GL transaction lines) at
any row count — built for the 10M-row throughput/volume test on PQ-3616.

Modeled on the high-volume Exact Online entity Peliqan syncs (TransactionLines):
GUID ids, division codes, GL account codes, invoice/entry numbers, amounts in
DC/FC, VAT codes, unicode customer names, periodic nulls, and a STATE message
every 100k rows (exercises pre_state_hook / commit on a long run).

Rows are flat scalars (like the real tap output), ~500-700 B each:
    1M rows  ≈ 0.6 GB   ·   10M rows ≈ 6 GB

STREAMS row by row — memory-flat at any count. Two ways to run:

  # pipe straight into the target, no file on disk (recommended for 10M):
  python tests/gen_exact_online.py 10000000 | target-bigquery --config config.json

  # or materialize a file:
  python tests/gen_exact_online.py 1000000 tests/local_fixtures/exact_1m.jsonl

Deterministic (seeded) — same count always produces identical data.
"""
import json
import random
import sys

STREAM = "exact_transactionlines"

SCHEMA = {
    "type": "SCHEMA",
    "stream": STREAM,
    "schema": {"type": "object", "properties": {
        "ID":              {"type": ["string", "null"]},
        "Division":        {"type": ["integer", "null"]},
        "EntryNumber":     {"type": ["integer", "null"]},
        "FinancialYear":   {"type": ["integer", "null"]},
        "FinancialPeriod": {"type": ["integer", "null"]},
        "GLAccountCode":   {"type": ["string", "null"]},
        "GLAccountDescription": {"type": ["string", "null"]},
        "Description":     {"type": ["string", "null"]},
        "AmountDC":        {"type": ["number", "null"]},
        "AmountFC":        {"type": ["number", "null"]},
        "VATCode":         {"type": ["string", "null"]},
        "VATPercentage":   {"type": ["number", "null"]},
        "Currency":        {"type": ["string", "null"]},
        "Date":            {"type": ["string", "null"], "format": "date-time"},
        "DueDate":         {"type": ["string", "null"], "format": "date-time"},
        "InvoiceNumber":   {"type": ["integer", "null"]},
        "AccountName":     {"type": ["string", "null"]},
        "AccountCode":     {"type": ["string", "null"]},
        "CostCenter":      {"type": ["string", "null"]},
        "CostUnit":        {"type": ["string", "null"]},
        "ItemCode":        {"type": ["string", "null"]},
        "Quantity":        {"type": ["number", "null"]},
        "Status":          {"type": ["integer", "null"]},
        "Notes":           {"type": ["string", "null"]},
        "Created":         {"type": ["string", "null"], "format": "date-time"},
        "Modified":        {"type": ["string", "null"], "format": "date-time"},
    }},
    "key_properties": ["ID"],
}

GL_ACCOUNTS = [
    ("1300", "Debiteuren"), ("1600", "Crediteuren"), ("8000", "Omzet binnenland"),
    ("8100", "Omzet EU"), ("8200", "Omzet buiten EU"), ("4000", "Inkoopwaarde omzet"),
    ("4500", "Huisvestingskosten"), ("4600", "Kantoorkosten"), ("4700", "Autokosten"),
    ("2100", "BTW af te dragen hoog"), ("2110", "BTW af te dragen laag"),
    ("1000", "Kas"), ("1100", "Bank"), ("4300", "Personeelskosten"),
]
VAT = [("VH", 21.0), ("VL", 9.0), ("V0", 0.0), ("ICP", 0.0), ("VN", None)]
CUSTOMERS = [
    "Van den Berg Logistics B.V.", "Müller & Söhne GmbH", "Café 't Zonnetje",
    "Société Générale de Construction", "Jansen Installatietechniek",
    "Nordic Solutions ApS", "Peliqan Demo Klant", "De Groene Kruidenier",
    "Firma López & García S.L.", "Østergaard Consulting", "Wally NV",
    "Bakkerij De Vries", "TechnoServ Sp. z o.o.", "Ελληνική Εμπορική Α.Ε.",
]
DESCRIPTIONS = [
    "Factuur {inv}", "Verkoop week {n}", "Inkoop materialen order {n}",
    "Maandelijkse huur", "Creditnota op factuur {inv}", "Bankkosten",
    "Salarisrun periode {p}", "Afschrijving inventaris", "BTW aangifte Q{q}",
    "Doorbelasting project P-{n:05d}",
]
COST_CENTERS = ["CC-100", "CC-200", "CC-300", None, None]  # often empty in real data


def emit(fh, obj):
    fh.write(json.dumps(obj, ensure_ascii=False) + "\n")


def stream_names(num_streams: int):
    if num_streams <= 1:
        return [STREAM]
    return [f"{STREAM}_{s:02d}" for s in range(1, num_streams + 1)]


def gen(count: int, fh, num_streams: int = 1) -> None:
    """Emit `count` records per stream. With num_streams > 1, records are
    interleaved round-robin across streams — the harshest pattern for the
    target (constant table switching, all sinks filling in parallel, one
    worker holding an open AppendRows stream per table)."""
    if num_streams > 1:
        names = stream_names(num_streams)
        for name in names:
            schema = dict(SCHEMA); schema["stream"] = name
            emit(fh, schema)
        rng = random.Random(3616)
        for i in range(1, count + 1):
            for name in names:
                emit(fh, _record(name, i, rng))
            if i % 100_000 == 0:
                emit(fh, {"type": "STATE", "value": {"bookmarks": {
                    n: {"replication_key_value": i} for n in names}}})
                print(f"  ... {i:,} rows/stream emitted", file=sys.stderr, flush=True)
        emit(fh, {"type": "STATE", "value": {"bookmarks": {
            n: {"replication_key_value": count} for n in names}}})
        return
    rng = random.Random(3616)  # deterministic — PQ-3616
    emit(fh, SCHEMA)
    for i in range(1, count + 1):
        emit(fh, _record(STREAM, i, rng))
        if i % 100_000 == 0:
            emit(fh, {"type": "STATE", "value": {"bookmarks": {
                STREAM: {"replication_key_value": i}}}})
            print(f"  ... {i:,} rows emitted", file=sys.stderr, flush=True)
    emit(fh, {"type": "STATE", "value": {"bookmarks": {
        STREAM: {"replication_key_value": f"rows-{count}"}}}})


def _record(stream: str, i: int, rng) -> dict:
    year = 2020 + (i % 6)
    period = 1 + (i % 12)
    day = 1 + (i % 28)
    vat_code, vat_pct = VAT[i % len(VAT)]
    amount = round(rng.uniform(-25000, 25000), 2)
    gl_code, gl_desc = GL_ACCOUNTS[i % len(GL_ACCOUNTS)]
    desc = DESCRIPTIONS[i % len(DESCRIPTIONS)].format(
        inv=20000000 + i, n=i % 1000, p=period, q=1 + period // 4)
    record = {
        "ID": f"{rng.getrandbits(32):08x}-{i % 0xffff:04x}-4{i % 0xfff:03x}"
              f"-9{i % 0xfff:03x}-{rng.getrandbits(48):012x}",
        "Division": 1000000 + (i % 7),
        "EntryNumber": 100000 + i,
        "FinancialYear": year,
        "FinancialPeriod": period,
        "GLAccountCode": gl_code,
        "GLAccountDescription": gl_desc,
        "Description": desc,
        "AmountDC": amount,
        "AmountFC": amount if i % 9 else round(amount * 1.08, 2),
        "VATCode": vat_code,
        "VATPercentage": vat_pct,
        "Currency": "EUR" if i % 9 else "USD",
        "Date": f"{year}-{period:02d}-{day:02d}T00:00:00+00:00",
        "DueDate": f"{year}-{period:02d}-{day:02d}T00:00:00+00:00" if i % 3 else None,
        "InvoiceNumber": 20000000 + i if i % 4 else None,
        "AccountName": CUSTOMERS[i % len(CUSTOMERS)],
        "AccountCode": f"{2000 + (i % len(CUSTOMERS)):>18}",
        "CostCenter": COST_CENTERS[i % len(COST_CENTERS)],
        "CostUnit": None,
        "ItemCode": f"ITEM-{i % 500:04d}" if i % 5 else None,
        "Quantity": round(rng.uniform(1, 250), 2) if i % 5 else None,
        "Status": 50 if i % 20 else 20,  # mostly processed, some open
        "Notes": ("Automatisch geboekt via Peliqan sync. " * (i % 4)) or None,
        "Created": f"{year}-{period:02d}-{day:02d}T08:15:00+00:00",
        "Modified": f"{year}-{period:02d}-{day:02d}T09:30:00+00:00",
    }
    return {"type": "RECORD", "stream": stream, "record": record}


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
    out = sys.argv[2] if len(sys.argv) > 2 else None
    streams = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    if out and out != "-":
        with open(out, "w") as fh:
            gen(count, fh, streams)
        print(f"wrote {out} ({count:,} rows x {streams} stream(s))", file=sys.stderr)
    else:
        gen(count, sys.stdout, streams)
