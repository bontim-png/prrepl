# Pro-Booking France Prospect Scanner

This folder contains a respectful public-page scanner for accommodation business contact details in France.

## Queue and automatic batches

1. Upload the prepared `domain_queue.csv` into this folder in the repository.
2. Confirm `scan_state.json` has `next_index: 0`.
3. Merge this branch/PR into `main` (or run the workflow on this feature branch).
4. In GitHub Actions, run **Pro-Booking France Prospect Scan** once.
5. The workflow processes the next 100 domains, saves cumulative results and progress, then dispatches itself for the next batch. It repeats until the queue is exhausted.

The current queue contains 3,184 unique website domains derived from the 4,631 A-priority accommodation records. Obvious social networks, OTAs and common booking-engine hosts were excluded. Multiple accommodation records sharing one domain are grouped together. This is a research queue, not 3,184 verified independent operators.

## Outputs

- `all_scanner_results.csv`: cumulative one-row-per-domain output.
- `all_scanner_evidence.jsonl`: evidence URLs for every extracted email, phone and SIRET candidate.
- `scan_state.json`: durable cursor and remaining count.
- Each run also uploads a batch backup artifact.

## Safety and quality

- Only publicly accessible pages are scanned.
- Robots.txt restrictions are respected; no login, CAPTCHA, access-control or rate-limit bypass.
- Mailboxes and phone numbers are not verified.
- Email classifications are heuristic. Departmental, named/personal and third-party addresses must not automatically be treated as prospect contacts.
- SIRET candidates pass a Luhn checksum only; legal entity matching remains a manual verification step.
- The workflow uses a 2.5-second delay between requests and scans up to 8 pages per domain.

## Restart / resume

Progress is stored in `scan_state.json`. A new manual run resumes from `next_index`. To restart from the beginning, set `next_index` to 0 and clear the cumulative result files.
