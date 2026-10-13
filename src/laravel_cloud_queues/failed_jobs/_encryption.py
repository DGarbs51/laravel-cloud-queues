"""Decrypt Laravel encryptString payloads."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .._narrowing import is_mapping
from ..errors import ConfigurationError


class Encrypter:
    """Authenticate and decrypt a payload with the configured Laravel key."""

    def __init__(self, cipher: str) -> None:
        """Load the key and validate its length for the selected cipher."""
        self.cipher: str = cipher.upper()
        sizes = {"AES-128-CBC": 16, "AES-256-CBC": 32, "AES-128-GCM": 16, "AES-256-GCM": 32}
        if self.cipher not in sizes:
            raise ConfigurationError("Retry supports AES-128/256-CBC and AES-128/256-GCM.")
        value = os.environ.get("LARAVEL_CLOUD_QUEUES_KEY") or os.environ.get("APP_KEY")
        if not value:
            raise ConfigurationError("Retry requires LARAVEL_CLOUD_QUEUES_KEY or APP_KEY.")
        try:
            self._key: bytes = (
                base64.b64decode(value[7:], validate=True)
                if value.startswith("base64:")
                else value.encode("utf-8")
            )
        except ValueError:
            raise ConfigurationError("The retry encryption key is not valid base64.") from None
        if len(self._key) != sizes[self.cipher]:
            raise ConfigurationError(
                f"The retry encryption key must be {sizes[self.cipher]} bytes."
            )

    def decrypt(self, payload: bytes) -> str:
        """Authenticate the encrypted payload before returning its UTF-8 plaintext."""
        try:
            data: object = json.loads(base64.b64decode(payload, validate=True))
            if not is_mapping(data):
                raise ValueError
            iv, value, mac, tag = (data.get(key, "") for key in ("iv", "value", "mac", "tag"))
            if not isinstance(iv, str) or not isinstance(value, str):
                raise ValueError
            if not isinstance(mac, str) or not isinstance(tag, str):
                raise ValueError
            nonce = base64.b64decode(iv, validate=True)
            encrypted = base64.b64decode(value, validate=True)
            auth_tag = base64.b64decode(tag, validate=True)
            if self.cipher.endswith("GCM"):
                if len(nonce) != 12 or len(auth_tag) != 16:
                    raise ValueError
                plaintext = AESGCM(self._key).decrypt(nonce, encrypted + auth_tag, None)
            else:
                if len(nonce) != 16 or auth_tag:
                    raise ValueError
                expected = hmac.new(self._key, (iv + value).encode("ascii"), hashlib.sha256)
                if not hmac.compare_digest(expected.hexdigest(), mac):
                    raise ValueError
                decryptor = Cipher(algorithms.AES(self._key), modes.CBC(nonce)).decryptor()
                padded = decryptor.update(encrypted) + decryptor.finalize()
                unpadder = padding.PKCS7(128).unpadder()
                plaintext = unpadder.update(padded) + unpadder.finalize()
            return plaintext.decode("utf-8")
        except (ValueError, TypeError, InvalidTag, RecursionError):
            raise ValueError(
                "The failed-job payload could not be authenticated or decrypted."
            ) from None
