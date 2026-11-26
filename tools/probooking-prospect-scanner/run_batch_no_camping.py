#!/usr/bin/env python3
"""Scan the next 100 domains from the separate non-camping queue."""
import csv, json, os, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
QUEUE=ROOT/"domain_queue_no_camping.csv"
STATE=ROOT/"scan_state_no_camping.json"
RESULTS=ROOT/"all_scanner_results_no_camping.csv"
EVIDENCE=ROOT/"all_scanner_evidence_no_camping.jsonl"
BATCH=100
with QUEUE.open(encoding="utf-8-sig",newline="") as f:
    reader=csv.DictReader(f); fields=reader.fieldnames or []; rows=list(reader)
if not fields or not rows: raise SystemExit("Non-camping queue is empty or invalid")
state=json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {"next_index":0}
start=max(0,int(state.get("next_index",0)))
if start>=len(rows):
    with open(os.environ["GITHUB_OUTPUT"],"a") as o:o.write("has_more=false\n")
    raise SystemExit(0)
batch=rows[start:start+BATCH]
batchfile=ROOT/"_current_batch_no_camping.csv"
tmpout=ROOT/"_batch_results_no_camping.csv"
tmpevidence=ROOT/"_batch_evidence_no_camping.jsonl"
with batchfile.open("w",encoding="utf-8-sig",newline="") as f:
    w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(batch)
subprocess.run([sys.executable,str(ROOT/"scanner.py"),"--input",str(batchfile),"--output",str(tmpout),"--evidence",str(tmpevidence),"--limit",str(BATCH),"--delay",os.getenv("SCAN_DELAY","2.5"),"--max-pages","8"],check=True,cwd=ROOT)
with tmpout.open(encoding="utf-8-sig",newline="") as f:
    rd=csv.DictReader(f); result_fields=rd.fieldnames or []; new=list(rd)
known=set()
if RESULTS.exists():
    with RESULTS.open(encoding="utf-8-sig",newline="") as f:known={x.get("domain","").lower() for x in csv.DictReader(f)}
if result_fields:
    exists=RESULTS.exists() and RESULTS.stat().st_size>0
    with RESULTS.open("a",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=result_fields)
        if not exists:w.writeheader()
        w.writerows(x for x in new if x.get("domain","").lower() not in known)
known_ev=set()
if EVIDENCE.exists():
    with EVIDENCE.open(encoding="utf-8") as f:known_ev={x.strip() for x in f if x.strip()}
if tmpevidence.exists():
    with tmpevidence.open(encoding="utf-8") as src,EVIDENCE.open("a",encoding="utf-8") as dst:
        for line in src:
            line=line.strip()
            if line and line not in known_ev:dst.write(line+"\n");known_ev.add(line)
next_i=start+len(batch)
newstate={"next_index":next_i,"total":len(rows),"completed":next_i,"remaining":len(rows)-next_i}
tmp=STATE.with_suffix(".tmp");tmp.write_text(json.dumps(newstate,indent=2)+"\n",encoding="utf-8");tmp.replace(STATE)
more=next_i<len(rows)
with open(os.environ["GITHUB_OUTPUT"],"a") as o:o.write(f"has_more={'true' if more else 'false'}\n")
print(f"Non-camping scan: {next_i}/{len(rows)}; {newstate['remaining']} remaining")
