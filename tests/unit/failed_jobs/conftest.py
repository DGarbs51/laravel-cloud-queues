"""Provide Laravel-format encryption and a fake Cloud endpoint."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def encrypt(plaintext: str, key: bytes = b"b" * 32, cipher: str = "AES-256-CBC") -> bytes:
    """Encrypt a string in PHP Encrypter::encryptString's exact wire format."""
    gcm = cipher.endswith("GCM")
    iv = bytes(range(12 if gcm else 16))
    if gcm:
        encrypted = AESGCM(key).encrypt(iv, plaintext.encode(), None)
        encrypted, tag = encrypted[:-16], encrypted[-16:]
    else:
        padder = padding.PKCS7(128).padder()
        padded = padder.update(plaintext.encode()) + padder.finalize()
        encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
        encrypted, tag = encryptor.update(padded) + encryptor.finalize(), b""
    iv_b64 = base64.b64encode(iv).decode()
    value_b64 = base64.b64encode(encrypted).decode()
    mac = "" if gcm else hmac.new(key, (iv_b64 + value_b64).encode(), hashlib.sha256).hexdigest()
    return base64.b64encode(
        json.dumps(
            {
                "iv": iv_b64,
                "value": value_b64,
                "mac": mac,
                "tag": base64.b64encode(tag).decode(),
            },
            separators=(",", ":"),
        ).encode()
    )


@pytest.fixture(autouse=True)
def encryption_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use an explicit dummy retry key in all failed-job tests."""
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_KEY", "b" * 32)
    monkeypatch.delenv("APP_KEY", raising=False)
