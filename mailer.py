#!/usr/bin/env python3
"""
Mail versturen via Gmail of iCloud, zonder wachtwoord in een bestand.
Het wachtwoord komt van GitHub Actions Secrets (omgevingsvariabelen).

GitHub stappen (zie chat-uitleg):
  1. Repo aanmaken met dit bestand als mailer.py
  2. Repo > Settings > Secrets and variables > Actions > New repository secret:
       MAIL_USER = bon.tim@gmail.com
       MAIL_PASS = jouw 16-tekens app-wachtwoord (GEEN accountwachtwoord)
  3. .github/workflows/mail.yml toevoegen (staat in dezelfde map als dit script)
  4. Actions-tab > workflow kiezen > Run workflow > onderwerp/tekst/ontvanger invullen
"""

import os
import smtplib
import sys
from email.message import EmailMessage

SMTP_SERVERS = {
    "gmail": ("smtp.gmail.com", 587),
    "icloud": ("smtp.mail.me.com", 587),
}


def main() -> None:
    user = os.environ.get("MAIL_USER")
    password = os.environ.get("MAIL_PASS")
    provider = os.environ.get("MAIL_PROVIDER", "gmail").lower()
    to_addr = os.environ.get("MAIL_TO")
    subject = os.environ.get("MAIL_SUBJECT", "(geen onderwerp)")
    body = os.environ.get("MAIL_BODY", "")

    if not user or not password:
        sys.exit("✖ MAIL_USER en/of MAIL_PASS ontbreken; stel ze in als repository secrets.")
    if not to_addr:
        sys.exit("✖ MAIL_TO ontbreekt.")

    host, port = SMTP_SERVERS.get(provider, SMTP_SERVERS["gmail"])
    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)
    print(f"✔ Verzonden naar {to_addr} via {host}:{port} ({provider})")


if __name__ == "__main__":
    main()
