#!/usr/bin/env python3
"""
Mail versturen via Gmail of iCloud met een versleuteld wachtwoord.

Gebruik:
  1. Maak credentials.json (zie structuur hieronder) EN versleutel deze eerst:
       python mailer.py encrypt credentials.json
     Dat maakt credentials.enc aan en verwijdert het plaintext-bestand.
  2. Stuur een mail:
       python mailer.py send credentials.enc --to ontvanger@voorbeeld.com \
              --subject "Test" --body "Hallo!" --key LEY-van-16-tekens
     Of laat --key weg en zet de sleutel in de omgevingsvariabele MAIL_KEY.

Structuur van credentials.json ( Plaintext, alleen vóór versleuteling ):
{
  "email": "jouwaccount@gmail.com",          // of jouw@icloud.com
  "password": "xxxx xxxx xxxx xxxx",          // app-specifiek wachtwoord
  "provider": "gmail"                         // "gmail" of "icloud"
}

Belangrijk: gebruik ALTIJD een app-specifiek wachtwoord
 - Gmail:      myaccount.google.com > Beveiliging > 2-stapsverificatie > App-wachtwoorden
 - iCloud:     appleid.apple.com > Aanmelden en beveiliging > App-specifieke wachtwoorden
"""

import argparse
import base64
import getpass
import json
import os
import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path

# Optioneel: pip install cryptography  (voor versleutelen/ontsleutelen)
try:
    from cryptography.fernet import Fernet
    from cryptography.fernet import InvalidToken
except ImportError:
    Fernet = None
    InvalidToken = None

SMTP_SERVERS = {
    "gmail": ("smtp.gmail.com", 587),
    "icloud": ("smtp.mail.me.com", 587),
}


def _fernet(key: str) -> "Fernet":
    """Accepteer een korte menselijke sleutel en maak er een Fernet-sleutel van."""
    if Fernet is None:
        sys.exit("Installeer eerst: pip install cryptography")
    return Fernet(base64.urlsafe_b64encode(key.encode().ljust(32, b"0")[:32]))


def cmd_encrypt(args) -> None:
    """Versleutel credentials.json naar credentials.enc en verwijder het origineel."""
    path = Path(args.file)
    data = path.read_bytes()
    key = args.key or getpass.getpass("Sleutel (min. 16 tekens): ")
    if len(key) < 16:
        sys.exit("Gebruik een sleutel van minstens 16 tekens.")
    token = _fernet(key).encrypt(data)
    path.with_suffix(".enc").write_bytes(token)
    path.unlink()
    print(f"✔ Versleuteld: {path.with_suffix('.enc')} aangemaakt; {path} verwijderd.")


def load_credentials(encrypted_file: str, key: str) -> dict:
    token = Path(encrypted_file).read_bytes()
    try:
        plain = _fernet(key).decrypt(token)
    except InvalidToken:
        sys.exit("✖ Ontsleutelen mislukt: verkeerde sleutel of beschadigd bestand.")
    creds = json.loads(plain)
    for field in ("email", "password", "provider"):
        if field not in creds:
            sys.exit(f"✖ credentials-bestand mist het veld '{field}'.")
    return creds


def send_mail(creds: dict, to_addr: str, subject: str, body: str,
              reply_to: str | None = None, dry_run: bool = False) -> None:
    host, port = SMTP_SERVERS.get(creds["provider"].lower(), SMTP_SERVERS["gmail"])
    msg = EmailMessage()
    msg["From"] = creds["email"]
    msg["To"] = to_addr
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    if dry_run:
        print(f"[dry-run] {creds['email']} -> {to_addr} | {subject}")
        return

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(creds["email"], creds["password"])
        server.send_message(msg)
    print(f"✔ Verzonden naar {to_addr} via {host}:{port} ({creds['provider']})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Mail via Gmail/iCloud met versleuteld wachtwoord")
    sub = parser.add_subparsers(dest="command", required=True)

    p_enc = sub.add_parser("encrypt", help="credentials.json versleutelen")
    p_enc.add_argument("file", help="pad naar credentials.json")
    p_enc.add_argument("--key", help="sleutel (min. 16 tekens); anders vragen")
    p_enc.set_defaults(func=cmd_encrypt)

    p_send = sub.add_parser("send", help="e-mail versturen")
    p_send.add_argument("file", help="pad naar credentials.enc")
    p_send.add_argument("--to", required=True, help="ontvanger")
    p_send.add_argument("--subject", required=True, help="onderwerp")
    p_send.add_argument("--body", required=True, help="berichttekst")
    p_send.add_argument("--reply-to", help="optioneel Reply-To adres")
    p_send.add_argument("--key", help="sleutel; anders omgevingsvariabele MAIL_KEY of vragen")
    p_send.add_argument("--dry-run", action="store_true", help="alleen tonen, niet versturen")
    p_send.set_defaults(func=lambda a: send_mail(
        load_credentials(a.file, a.key or os.environ.get("MAIL_KEY") or getpass.getpass("Sleutel: ")),
        a.to, a.subject, a.body, a.reply_to, a.dry_run,
    ))

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
