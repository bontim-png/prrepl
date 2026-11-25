# Pro-Booking France Prospect Scanner

A cautious public-contact research scanner for accommodation operator websites.

## Pilot
Run 10–20 domains first. It respects robots.txt (fails closed if robots cannot be retrieved), uses sequential requests and delays, stays on the supplied domain, and does not bypass CAPTCHA, login or access restrictions.

## Local run
Python 3.10+:
```bash
pip install -r requirements.txt
# Replace the User-Agent contact placeholder in scanner.py with a monitored business email.
python scanner.py --input pilot_domains.csv --limit 20 --delay 2.5
```

## Outputs
- scanner_results.csv: one row per domain, with generic and named-looking emails separated.
- scanner_evidence.jsonl: evidence URLs for each extracted value.

Extracted emails are not deliverability-verified and public availability does not itself establish permission to market. Named/personal-looking addresses require separate legal review. SIRET strings are candidates, not validated identifiers. Confirm the operator behind each website before importing.

GitHub Actions workflow is manual only and uploads results as a 30-day artifact; it does not write to the Pro-Booking database or send messages.
