#!/usr/bin/env python3
"""Merge current and non-camping scanner results into a separate deduplicated master file."""
import csv
from pathlib import Path

ROOT=Path(__file__).resolve().parent
SOURCES=[ROOT/"all_scanner_results.csv",ROOT/"all_scanner_results_no_camping.csv"]
OUT=ROOT/"all_scanner_results_merged.csv"
rows={}
fields=[]
multi={"emails_generic","emails_named_or_personal","emails_departmental","emails_third_party_or_other","phones_public","siret_candidates","accommodations_sample","communes_sample","departments_sample","departments_sample","regions_sample","categories_sample"}
def tokens(v):
    return [x.strip() for x in (v or "").split(" | ") if x.strip()]
for path in SOURCES:
    if not path.exists(): continue
    with path.open(encoding="utf-8-sig",newline="") as f:
        rd=csv.DictReader(f)
        for col in rd.fieldnames or []:
            if col not in fields: fields.append(col)
        for row in rd:
            key=(row.get("domain") or "").strip().lower()
            if not key: continue
            if key not in rows:
                rows[key]=row.copy()
                continue
            old=rows[key]
            for col in set(old)|set(row):
                if col in multi:
                    vals=list(dict.fromkeys(tokens(old.get(col,""))+tokens(row.get(col,""))))
                    old[col]=" | ".join(vals)
                elif not (old.get(col) or "").strip() and (row.get(col) or "").strip():
                    old[col]=row[col]
                elif col in {"pages_scanned"}:
                    try: old[col]=str(max(int(old.get(col) or 0),int(row.get(col) or 0)))
                    except ValueError: pass
                elif col=="scan_status" and row.get(col)=="scanned":
                    old[col]="scanned"
with OUT.open("w",encoding="utf-8-sig",newline="") as f:
    w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader()
    for key in sorted(rows):w.writerow(rows[key])
print(f"Merged {sum(p.exists() for p in SOURCES)} source files into {OUT.name}: {len(rows)} unique domains")
