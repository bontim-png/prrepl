#!/usr/bin/env python3
"""Conservative public-contact scanner for French accommodation websites."""
import argparse,csv,json,re,time,random
from datetime import datetime,timezone
from urllib.parse import urljoin,urlparse,urldefrag
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup

UA="ProBookingProspectResearch/1.0 (+contact: replace-with-your-business-email)"
HINTS=("contact","nous-contacter","contactez","mentions-legales","qui-sommes-nous","a-propos","about","equipe","team","cgv","legal","impressum")
SKIP=("facebook.com","instagram.com","youtube.com","tiktok.com","pinterest.","linkedin.com","booking.com","airbnb.","abritel.","tripadvisor.","gites.fr","gites-de-france","google.","wikipedia.org")
EMAIL=re.compile(r"(?i)(?<![\w.+-])([A-Z0-9._%+-]{1,64}@[A-Z0-9.-]+\.[A-Z]{2,24})(?![\w.-])")
PHONE=re.compile(r"(?<!\w)(?:\+33|0033|0)[ .()\-]?[1-9](?:[ .()\-]?\d{2}){4}(?!\w)")
SIRET=re.compile(r"(?<!\d)(\d{14})(?!\d)")
GENERIC=("contact","info","bonjour","accueil","reservation","reservations","booking","office","secretariat","commercial","location","hello","admin","direction","reception","camping","gite","domain","domaine","chateau","hotel")

def norm(u):
 u=(u or "").strip()
 if not u:return ""
 if not re.match(r"^https?://",u,re.I):u="https://"+u
 p=urlparse(u);return p._replace(fragment="").geturl() if p.hostname else ""
def host(u):
 h=(urlparse(u).hostname or "").lower()
 return h[4:] if h.startswith("www.") else h
def allowed(h,b):return h==b or h.endswith("."+b)
def generic(e):
 l=e.split("@",1)[0].lower()
 return any(l==p or l.startswith(p+".") or l.startswith(p+"-") for p in GENERIC)
def robots(s,u,t):
 p=RobotFileParser()
 try:
  r=s.get(urljoin(u,"/robots.txt"),timeout=t)
  if r.status_code in (401,403):p.parse(["User-agent: *","Disallow: /"])
  elif r.status_code<400:p.parse(r.text.splitlines())
  else:p.parse(["User-agent: *","Disallow:"])
 except requests.RequestException:p.parse(["User-agent: *","Disallow: /"])
 return p
def fetch(s,u,t,m):
 try:
  with s.get(u,timeout=t,allow_redirects=True,stream=True) as r:
   if r.status_code>=400 or "text/html" not in (r.headers.get("Content-Type") or "").lower():return None,r.url
   chunks=[];n=0
   for c in r.iter_content(65536):
    if not c:continue
    n+=len(c)
    if n>m:break
    chunks.append(c)
   return b"".join(chunks).decode(r.encoding or "utf-8",errors="replace"),r.url
 except requests.RequestException:return None,u
def main():
 a=argparse.ArgumentParser()
 a.add_argument("--input",default="pilot_domains.csv");a.add_argument("--output",default="scanner_results.csv");a.add_argument("--evidence",default="scanner_evidence.jsonl")
 a.add_argument("--limit",type=int,default=20);a.add_argument("--delay",type=float,default=2.5);a.add_argument("--timeout",type=int,default=18);a.add_argument("--max-pages",type=int,default=8);a.add_argument("--max-bytes",type=int,default=1500000)
 x=a.parse_args()
 with open(x.input,encoding="utf-8-sig",newline="") as f:seeds=list(csv.DictReader(f))[:x.limit]
 fields=["domain","website","accommodation_count","accommodations","communes","departments","regions","categories","emails_generic","emails_named_or_personal","phones_public","siret_candidates","operator_name_candidate","pages_scanned","scan_status","scan_timestamp_utc","notes"]
 s=requests.Session();s.headers.update({"User-Agent":UA,"Accept":"text/html,application/xhtml+xml"})
 with open(x.output,"w",encoding="utf-8-sig",newline="") as out,open(x.evidence,"w",encoding="utf-8") as ev:
  w=csv.DictWriter(out,fieldnames=fields);w.writeheader()
  for i,seed in enumerate(seeds,1):
   start=norm(seed.get("website") or seed.get("domain"));base=host(start)
   row={k:seed.get(k,"") for k in fields};row.update({"domain":seed.get("domain") or base,"website":start,"emails_generic":"","emails_named_or_personal":"","phones_public":"","siret_candidates":"","operator_name_candidate":"","pages_scanned":0,"scan_status":"pending","scan_timestamp_utc":datetime.now(timezone.utc).isoformat(),"notes":""})
   if not start or not base or any(z in base for z in SKIP):row["scan_status"]="invalid_or_excluded_domain";w.writerow(row);continue
   rp=robots(s,start,x.timeout);q=[start];seen=set();emails={};phones={};sirets={};titles=[];blocked=False
   while q and len(seen)<x.max_pages:
    u,_=urldefrag(q.pop(0))
    if u in seen or not allowed(host(u),base) or any(z in host(u) for z in SKIP):continue
    if not rp.can_fetch(UA,u):
     if u==start:blocked=True
     continue
    html,final=fetch(s,u,x.timeout,x.max_bytes)
    if not allowed(host(final),base):continue
    seen.add(u)
    if not html:continue
    soup=BeautifulSoup(html,"html.parser")
    if soup.title and len(titles)<3:titles.append(soup.title.get_text(" ",strip=True))
    for link in soup.select('a[href^="mailto:"]'):
     v=link.get("href","")[7:].split("?",1)[0].strip().lower()
     if EMAIL.fullmatch(v):emails.setdefault(v,set()).add(final)
    for link in soup.select('a[href^="tel:"]'):
     v=link.get("href","")[4:].strip()
     if v:phones.setdefault(v,set()).add(final)
    txt=soup.get_text(" ",strip=True)
    for v in EMAIL.findall(txt):
     v=v.strip(".,;:()[]{}<>").lower()
     if not v.endswith((".png",".jpg",".jpeg",".svg",".gif")):emails.setdefault(v,set()).add(final)
    for v in PHONE.findall(txt):phones.setdefault(re.sub(r"\s+"," ",v).strip(),set()).add(final)
    for v in SIRET.findall(txt):\n     if valid_siret(v):sirets.setdefault(v,set()).add(final)
    if len(seen)<x.max_pages:
     for link in soup.find_all("a",href=True):
      label=(link.get_text(" ",strip=True)+" "+link["href"]).lower()
      if any(h in label for h in HINTS):
       nxt=urljoin(final,link["href"])
       if urlparse(nxt).scheme in ("http","https") and allowed(host(nxt),base):
        nxt=urlparse(nxt)._replace(fragment="").geturl()
        if nxt not in seen and nxt not in q:q.append(nxt)
    time.sleep(x.delay+random.uniform(0,.4))
   gen=sorted(e for e in emails if generic(e));named=sorted(e for e in emails if e not in gen)
   row.update({"emails_generic":" | ".join(gen),"emails_named_or_personal":" | ".join(named),"phones_public":" | ".join(sorted(phones)),"siret_candidates":" | ".join(sorted(sirets)),"operator_name_candidate":titles[0] if titles else "","pages_scanned":len(seen),"scan_status":"robots_blocked" if blocked and not seen else ("scanned" if seen else "no_pages_scanned"),"notes":"Public-page evidence only; not mailbox verification. SIRET values are candidates. Named addresses require legal review."})
   w.writerow(row);out.flush()
   for typ,vals in (("email",emails),("phone",phones),("siret_candidate",sirets)):
    for v,urls in vals.items():ev.write(json.dumps({"domain":base,"type":typ,"value":v,"evidence_urls":sorted(urls)},ensure_ascii=False)+"\n")
   print(f"[{i}/{len(seeds)}] {base}: {row['scan_status']}, {len(emails)} emails, {len(seen)} pages",flush=True);time.sleep(x.delay)
 print("Scan complete")
if __name__=="__main__":main()
