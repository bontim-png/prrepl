#!/usr/bin/env python3
"""Generate an RSA key pair for PHP OpenSSL + GitHub Actions decryption."""
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
private_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
public_pem = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
print('PUBLIC_KEY_PEM_B64=' + base64.b64encode(public_pem).decode())
print('PRIVATE_KEY_PEM_B64=' + base64.b64encode(private_pem).decode())
print('Put PRIVATE_KEY_PEM_B64 in GitHub Actions secret MAILER_PRIVATE_KEY_PEM_B64.')
print('Put PUBLIC_KEY_PEM_B64 in config.php as mailer_public_key_pem_b64.')
