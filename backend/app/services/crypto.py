"""Fernet encryption for broker credentials at rest (iron law #9).

The Fernet key is derived from ``settings.secret_key`` via HKDF-SHA256 with a
domain-separation ``info`` tag, so the credential key is independent of any
other key material ever derived from the same secret (e.g. JWT signing).
"""

import base64
from functools import lru_cache

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from app.core.config import get_settings

_HKDF_INFO = b"roi-gen-credentials"
_KEY_LENGTH = 32


@lru_cache(maxsize=1)
def _fernet() -> Fernet:
    """Fernet instance keyed by HKDF-SHA256(settings.secret_key)."""
    hkdf = HKDF(algorithm=hashes.SHA256(), length=_KEY_LENGTH, salt=None, info=_HKDF_INFO)
    key = base64.urlsafe_b64encode(hkdf.derive(get_settings().secret_key.encode()))
    return Fernet(key)


def encrypt_str(plaintext: str) -> str:
    """Encrypt ``plaintext`` → urlsafe Fernet token (unique IV per call)."""
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt_str(ciphertext: str) -> str:
    """Decrypt a Fernet token; raises ``cryptography.fernet.InvalidToken`` on tamper."""
    return _fernet().decrypt(ciphertext.encode()).decode()
