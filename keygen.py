#!/usr/bin/env python3
"""Generate a sealed-box keypair for the GitHub Actions mailer."""
import base64
from nacl.public import PrivateKey

key = PrivateKey.generate()
print('PUBLIC_KEY_B64=' + base64.b64encode(bytes(key.public_key)).decode())
print('PRIVATE_KEY_B64=' + base64.b64encode(bytes(key)).decode())
print('\nAdd PRIVATE_KEY_B64 as GitHub Actions secret MAILER_PRIVATE_KEY_B64.')
print('Put PUBLIC_KEY_B64 in config.php as mailer_public_key_b64.')
