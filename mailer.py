#!/usr/bin/env python3
"""GitHub Actions SMTP mailer. Decrypts an AES-GCM request using an RSA private key."""
import base64, json, os, smtplib, ssl
from email.message import EmailMessage


def fail(message: str, code: int = 1) -> None:
    print(f"ERROR: {message}")
    raise SystemExit(code)


def main() -> None:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    private_key_b64 = os.environ.get("MAILER_PRIVATE_KEY_PEM_B64")
    if not event_path or not private_key_b64:
        fail("Missing event payload or MAILER_PRIVATE_KEY_PEM_B64 secret.")
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        fail("cryptography package is not installed.")
    try:
        with open(event_path, "r", encoding="utf-8") as f:
            event = json.load(f)
        payload = event["client_payload"]
        private_pem = base64.b64decode(private_key_b64, validate=True)
        private_key = serialization.load_pem_private_key(private_pem, password=None)
        aes_key = private_key.decrypt(
            base64.b64decode(payload["encrypted_key"], validate=True),
            padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA1()), algorithm=hashes.SHA1(), label=None),
        )
        clear = AESGCM(aes_key).decrypt(
            base64.b64decode(payload["nonce"], validate=True),
            base64.b64decode(payload["ciphertext"], validate=True) + base64.b64decode(payload["tag"], validate=True),
            None,
        )
        data = json.loads(clear.decode("utf-8"))
    except Exception:
        fail("Could not decrypt request. Check that the public/private key pair matches.")

    required = ["mail_user", "mail_password", "smtp_server", "smtp_port", "mail_to", "subject", "body"]
    missing = [key for key in required if not str(data.get(key, "")).strip()]
    if missing:
        fail("Missing required field(s): " + ", ".join(missing))
    user, password = str(data["mail_user"]).strip(), str(data["mail_password"])
    host, to_addr = str(data["smtp_server"]).strip(), str(data["mail_to"]).strip()
    subject, body = str(data["subject"]), str(data["body"])
    try:
        port = int(data["smtp_port"])
        timeout = min(max(int(data.get("timeout", 30)), 5), 60)
    except (TypeError, ValueError):
        fail("SMTP port or timeout is invalid.")
    security = str(data.get("smtp_security", "starttls")).lower()
    if security not in ("starttls", "ssl", "none"):
        fail("smtp_security must be starttls, ssl, or none.")
    msg = EmailMessage(); msg["From"] = user; msg["To"] = to_addr; msg["Subject"] = subject; msg.set_content(body)
    try:
        if security == "ssl":
            with smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context()) as server:
                server.login(user, password); server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=timeout) as server:
                server.ehlo()
                if security == "starttls":
                    server.starttls(context=ssl.create_default_context()); server.ehlo()
                server.login(user, password); server.send_message(msg)
    except Exception as exc:
        fail(f"SMTP delivery failed ({type(exc).__name__}): {str(exc)[:400]}")
    print(f"SUCCESS: Email sent to {to_addr} via {host}:{port} ({security}).")

if __name__ == "__main__":
    main()
