#!/usr/bin/env python3
"""Temporary test mailer: reads SMTP settings from repository_dispatch payload.
Do not use this unencrypted variant in production.
"""
import json, os, smtplib, ssl
from email.message import EmailMessage


def main():
    event_path = os.environ.get('GITHUB_EVENT_PATH')
    if not event_path:
        raise SystemExit('ERROR: GITHUB_EVENT_PATH ontbreekt')
    with open(event_path, encoding='utf-8') as f:
        event = json.load(f)
    data = event.get('client_payload') or {}
    required = ('mail_user', 'mail_password', 'smtp_server', 'smtp_port', 'smtp_security', 'mail_to', 'subject', 'body')
    missing = [k for k in required if not str(data.get(k, '')).strip()]
    if missing:
        raise SystemExit('ERROR: verplichte velden ontbreken: ' + ', '.join(missing))
    user = str(data['mail_user']).strip()
    password = str(data['mail_password'])
    host = str(data['smtp_server']).strip()
    recipient = str(data['mail_to']).strip()
    subject = str(data['subject'])
    body = str(data['body'])
    try:
        port = int(data['smtp_port'])
    except (TypeError, ValueError):
        raise SystemExit('ERROR: ongeldige SMTP-poort')
    security = str(data['smtp_security']).lower()
    if security not in ('starttls', 'ssl'):
        raise SystemExit('ERROR: kies STARTTLS of SSL; onbeveiligde SMTP is uitgeschakeld')
    msg = EmailMessage()
    msg['From'] = user
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.set_content(body)
    try:
        if security == 'ssl':
            with smtplib.SMTP_SSL(host, port, timeout=30, context=ssl.create_default_context()) as server:
                server.login(user, password)
                server.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=30) as server:
                server.ehlo()
                server.starttls(context=ssl.create_default_context())
                server.ehlo()
                server.login(user, password)
                server.send_message(msg)
    except Exception as exc:
        # Do not print credentials or the event payload.
        raise SystemExit(f'ERROR: SMTP-verzending mislukt ({type(exc).__name__}): {str(exc)[:300]}')
    print(f"SUCCESS: e-mail verzonden naar {recipient} via {host}:{port}.")

if __name__ == '__main__':
    main()
