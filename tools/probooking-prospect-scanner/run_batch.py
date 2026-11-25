#!/usr/bin/env python3
"""Process the next 100 domains, persist progress, and signal whether another run is needed."""
import csv,json,os,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
QUEUE=ROOT/"domain_queue.csv"
STATE=ROOT/"scan_state.json"
RESULTS=ROOT/"all_scanner_results.csv"
EVIDENCE=ROOT/"all_scanner_evidence.jsonl"
BATCH=100
if not QUEUE.exists():
    raise SystemExit("Missing domain_queue.csv. Add the prepared queue file to this folder first.")
with QUEUE.open(encoding="utf-8-sig",newline="") as f:
    all_rows=list(csv.DictReader(f))
state=json.loads(STATE.read_text()) if STATE.exists() else {"next_index":0,"total":len(all_rows),"completed":0}
start=int(state.get("next_index",0))
batch=all_rows[start:start+BATCH]
if not batch:
    print("has_more=false")
    with open(os.environ["GITHUB_OUTPUT"],"a") as f:f.write("has_more=false\n")
    raise SystemExit(0)
batch_csv=ROOT/"_current_batch.csv"
with batch_csv.open("w",encoding="utf-8-sig",newline="") as f:
    w=csv.DictWriter(f,fieldnames=list(all_rows[0].keys()));w.writeheader();w.writerows(batch)
tmp_results=ROOT/"_batch_results.csv";tmp_evidence=ROOT/"_batch_evidence.jsonl"
cmd=[sys.executable,str(ROOT/"scanner.py"),"--input",str(batch_csv),"--output",str(tmp_results),"--evidence",str(tmp_evidence),"--limit",str(BATCH),"--delay",os.getenv("SCAN_DELAY","2.5"),"--max-pages","8"]
subprocess.run(cmd,check=True,cwd=ROOT)
with tmp_results.open(encoding="utf-8-sig",newline="") as f:
    content=f.read()
if RESULTS.exists():
    with RESULTS.open("a",encoding="utf-8",newline="") as out:
        with open(tmp_results,encoding="utf-8-sig") as src:
            next(src,None)
            for line in src:out.write(line)
else:
    RESULTS.write_text(content,encoding="utf-8")
if tmp_evidence.exists():
    with EVIDENCE.open("a",encoding="utf-8") as out:
        out.write(tmp_evidence.read_text(encoding="utf-8"))
next_index=start+len(batch)
state={"next_index":next_index,"total":len(all_rows),"completed":next_index,"remaining":max(0,len(all_rows)-next_index)}
STATE.write_text(json.dumps(state,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
more=next_index<len(all_rows)
with open(os.environ["GITHUB_OUTPUT"],"a") as f:f.write(f"has_more={'true' if more else 'false'}\n")
print(f"Completed {next_index}/{len(all_rows)} domains; {state['remaining']} remaining; next batch={more}")
