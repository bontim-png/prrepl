#!/usr/bin/env python3
"""GitHub Actions SMTP mailer. Receives an encrypted request via repository_dispatch."""
import base64
import json
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage


def fail(message: str, code: int = 1) -> None:
    print(f"ERROR: {message}")
    raise SystemExit(code)


def main() -> None:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    private_key_b64 = os.environ.get("MAILER_PRIVATE_KEY_B64")
    if not event_path or not private_key_b64:
        fail("Missing event payload or MAILER_PRIVATE_KEY_B64 secret.")

    try:
        from nacl.public import PrivateKey, SealedBox
    except ImportError:
        fail("PyNaCl is not installed.")

    try:
        with open(event_path, "r", encoding="utf-8") as f:
            event = json.load(f)
        encrypted_b64 = event["client_payload"]["encrypted"]
        encrypted = base64.b64decode(encrypted_b64, validate=True)
        private_key = PrivateKey(base64.b64decode(private_key_b64, validate=True))
        clear = SealedBox(private_key).decrypt(encrypted)
        data = json.loads(clear.decode("utf-8"))
    except Exception:
        fail("Could not decode/decrypt the mail request. Check the public/private key pair and payload.")

    required = ["mail_user", "mail_password", "smtp_server", "smtp_port", "mail_to", "subject", "body"]
    missing = [key for key in required if not str(data.get(key, "")).strip()]
    if missing:
        fail("Missing required field(s): " + ", ".join(missing))

    user = str(data["mail_user"]).strip()
    password = str(data["mail_password"])
    host = str(data["smtp_server"]).strip()
    to_addr = str(data["mail_to"]).strip()
    subject = str(data["subject"])
    body = str(data["body"])
    try:
        port = int(data["smtp_port"])
    except (TypeError, ValueError):
        fail("SMTP port must be a number.")
    security = str(data.get("smtp_security", "starttls")).lower()
    timeout = min(max(int(data.get("timeout", 30)), 5), 60)

    if security not in ("starttls", "ssl", "none"):
        fail("smtp_security must be starttls, ssl, or none.")

    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        if security == "ssl":
            with smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context()) as server:
                server.login(user, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=timeout) as server:
                server.ehlo()
                if security == "starttls":
                    server.starttls(context=ssl.create_default_context())
                    server.ehlo()
                server.login(user, password)
                server.send_message(msg)
    except Exception as exc:
        # Avoid printing credentials or the full request payload.
        fail(f"SMTP delivery failed ({type(exc).__name__}): {str(exc)[:500]}")

    print(f"SUCCESS: Email sent to {to_addr} via {host}:{port} ({security}).")


if __name__ == "__main__":
    main()
