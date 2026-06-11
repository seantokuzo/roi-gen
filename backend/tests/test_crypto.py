"""Credential crypto: round-trip, tamper detection, per-call IV uniqueness."""

import base64

import pytest
from cryptography.fernet import InvalidToken

from app.services.crypto import decrypt_str, encrypt_str


def test_round_trip() -> None:
    plaintext = "PKTEST123-super-secret"
    assert decrypt_str(encrypt_str(plaintext)) == plaintext


def test_round_trip_empty_and_unicode() -> None:
    assert decrypt_str(encrypt_str("")) == ""
    assert decrypt_str(encrypt_str("clé-鍵-ключ")) == "clé-鍵-ключ"


def test_tampered_ciphertext_raises_invalid_token() -> None:
    token = encrypt_str("super-secret")
    raw = base64.urlsafe_b64decode(token.encode())
    # Flip one bit of the trailing HMAC byte — must fail authentication.
    tampered = base64.urlsafe_b64encode(raw[:-1] + bytes([raw[-1] ^ 0x01])).decode()
    with pytest.raises(InvalidToken):
        decrypt_str(tampered)


def test_garbage_ciphertext_raises_invalid_token() -> None:
    with pytest.raises(InvalidToken):
        decrypt_str("not-a-fernet-token")


def test_distinct_ciphertexts_per_call() -> None:
    # Fernet uses a random IV per token — identical plaintexts must differ.
    assert encrypt_str("same-plaintext") != encrypt_str("same-plaintext")
