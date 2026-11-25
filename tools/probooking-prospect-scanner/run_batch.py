#!/usr/bin/env python3
"""Process the next 100 queued domains and persist idempotent cumulative results."""
import csv, json, os, subprocess, sys, tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
QUEUE = ROOT / "domain_queue.csv"
STATE = ROOT / "scan_state.json"
RESULTS = ROOT / "all_scanner_results.csv"
EVIDENCE = ROOT / "all_scanner_evidence.jsonl"
BATCH_SIZE = 100

if not QUEUE.exists():
    raise SystemExit("Missing domain_queue.csv")
with QUEUE.open(encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)
    fields = reader.fieldnames or []
    all_rows = list(reader)
if not fields or not all_rows:
    raise SystemExit("domain_queue.csv is empty or has no header")

if STATE.exists():
    state = json.loads(STATE.read_text(encoding="utf-8"))
else:
    state = {"next_index": 0}
start = max(0, int(state.get("next_index", 0)))
if start >= len(all_rows):
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("has_more=false\n")
    print(f"Queue already complete: {len(all_rows)}/{len(all_rows)}")
    raise SystemExit(0)

batch = all_rows[start:start + BATCH_SIZE]
batch_csv = ROOT / "_current_batch.csv"
tmp_results = ROOT / "_batch_results.csv"
tmp_evidence = ROOT / "_batch_evidence.jsonl"
with batch_csv.open("w", encoding="utf-8-sig", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields)
    w.writeheader()
    w.writerows(batch)

cmd = [
    sys.executable, str(ROOT / "scanner.py"),
    "--input", str(batch_csv),
    "--output", str(tmp_results),
    "--evidence", str(tmp_evidence),
    "--limit", str(BATCH_SIZE),
    "--delay", os.getenv("SCAN_DELAY", "2.5"),
    "--max-pages", "8",
]
subprocess.run(cmd, check=True, cwd=ROOT)

# Append only domains not already present, so a retry after a partial commit is safe.
with tmp_results.open(encoding="utf-8-sig", newline="") as f:
    new_reader = csv.DictReader(f)
    new_rows = list(new_reader)
    result_fields = new_reader.fieldnames or []
existing_domains = set()
if RESULTS.exists():
    with RESULTS.open(encoding="utf-8-sig", newline="") as f:
        existing_domains = {r.get("domain", "") for r in csv.DictReader(f)}
fresh_rows = [r for r in new_rows if r.get("domain", "") not in existing_domains]
if result_fields:
    write_header = not RESULTS.exists()
    with RESULTS.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=result_fields)
        if write_header:
            w.writeheader()
        w.writerows(fresh_rows)

# Evidence is append-only but deduplicated by its complete JSON representation.
known_evidence = set()
if EVIDENCE.exists():
    with EVIDENCE.open(encoding="utf-8") as f:
        known_evidence = {line.strip() for line in f if line.strip()}
if tmp_evidence.exists():
    with tmp_evidence.open(encoding="utf-8") as src, EVIDENCE.open("a", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if line and line not in known_evidence:
                dst.write(line + "\n")
                known_evidence.add(line)

next_index = start + len(batch)
new_state = {
    "next_index": next_index,
    "total": len(all_rows),
    "completed": next_index,
    "remaining": max(0, len(all_rows) - next_index),
}
tmp_state = STATE.with_suffix(".json.tmp")
tmp_state.write_text(json.dumps(new_state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
tmp_state.replace(STATE)
has_more = next_index < len(all_rows)
with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
    out.write(f"has_more={'true' if has_more else 'false'}\n")
print(f"Completed {next_index}/{len(all_rows)} domains; {new_state['remaining']} remaining; next batch={has_more}")
