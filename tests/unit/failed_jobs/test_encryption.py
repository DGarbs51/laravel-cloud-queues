"""Check authenticated Laravel CBC/GCM decryption and configuration."""

from __future__ import annotations

import base64
import json

import pytest

from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.failed_jobs._encryption import Encrypter

from .conftest import encrypt

_PHP_VECTOR = (
    "eyJpdiI6Ilg0dFM5TVRibEFqZW54c3lQdWJoVVE9PSIsInZhbHVlIjoiRGJpa2p2ZHI3eUs0dUtRakJneUhU"
    "UT09IiwibWFjIjoiMjBjZWYxODdhNThhOTk4MTk1NTc0YTE1MDgzODU1OWE0ZmQ4MDc5ZjMxYThkOGM1ZmM1Mzlm"
    "YzBkYTBjMWI1ZiIsInRhZyI6IiJ9"
)


def test_php_framework_known_vector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Match framework/tests/Encryption/EncrypterTest::testItValidatesMacOnPerKeyBasis."""
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_KEY", "b" * 16)
    assert Encrypter("AES-128-CBC").decrypt(_PHP_VECTOR.encode()) == "foo"
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_KEY", "a" * 16)
    with pytest.raises(ValueError, match="authenticated"):
        Encrypter("AES-128-CBC").decrypt(_PHP_VECTOR.encode())


@pytest.mark.parametrize("cipher", ["AES-128-CBC", "AES-256-CBC", "AES-128-GCM", "AES-256-GCM"])
@pytest.mark.parametrize("plaintext", ["", "foo", "queue 🚀\n" * 100])
def test_laravel_cipher_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
    cipher: str,
    plaintext: str,
) -> None:
    key = b"b" * (16 if "128" in cipher else 32)
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_KEY", "base64:" + base64.b64encode(key).decode())
    assert Encrypter(cipher.lower()).decrypt(encrypt(plaintext, key, cipher)) == plaintext


@pytest.mark.parametrize("key", [None, "", "short", "base64:%%%"])
def test_bad_key_configuration(monkeypatch: pytest.MonkeyPatch, key: str | None) -> None:
    if key is None:
        monkeypatch.delenv("LARAVEL_CLOUD_QUEUES_KEY")
    else:
        monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_KEY", key)
    with pytest.raises(ConfigurationError, match=r"key|KEY"):
        Encrypter("AES-256-CBC")


def test_app_key_fallback_and_package_key_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_KEY", "a" * 32)
    assert Encrypter("AES-256-CBC").decrypt(encrypt("foo")) == "foo"
    monkeypatch.delenv("LARAVEL_CLOUD_QUEUES_KEY")
    assert Encrypter("AES-256-CBC").decrypt(encrypt("foo", b"a" * 32)) == "foo"


def test_unsupported_cipher() -> None:
    with pytest.raises(ConfigurationError, match="supports"):
        Encrypter("DES")


@pytest.mark.parametrize("payload", [b"%%%", b"e30=", b"W10=", base64.b64encode(b"garbage")])
def test_malformed_encrypted_payload(payload: bytes) -> None:
    with pytest.raises(ValueError, match="authenticated"):
        Encrypter("AES-256-CBC").decrypt(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("iv", 42),
        ("value", 42),
        ("mac", 42),
        ("tag", 42),
        ("iv", "%%%"),
        ("iv", ""),
        ("value", "%%%"),
        ("mac", "wrong"),
        ("tag", base64.b64encode(b"tag").decode()),
    ],
)
def test_cbc_rejects_tampering(field: str, value: object) -> None:
    payload = json.loads(base64.b64decode(encrypt("foo")))
    payload[field] = value
    with pytest.raises(ValueError, match="authenticated"):
        Encrypter("AES-256-CBC").decrypt(base64.b64encode(json.dumps(payload).encode()))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tag", ""),
        ("tag", base64.b64encode(b"a" * 16).decode()),
        ("iv", base64.b64encode(b"a" * 16).decode()),
        ("value", base64.b64encode(b"wrong ciphertext").decode()),
    ],
)
def test_gcm_rejects_tampering(field: str, value: str) -> None:
    payload = json.loads(base64.b64decode(encrypt("foo", cipher="AES-256-GCM")))
    payload[field] = value
    with pytest.raises(ValueError, match="authenticated"):
        Encrypter("AES-256-GCM").decrypt(base64.b64encode(json.dumps(payload).encode()))
