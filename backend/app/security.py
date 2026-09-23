import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from app.config import settings


def _fernet() -> Fernet | None:
    key = settings.secret_key
    if not key:
        return None
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_secret(value: str) -> str:
    if value is None or value == "":
        return value
    f = _fernet()
    if f is None:
        return value
    return f.encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret(value: str) -> str:
    if value is None or value == "":
        return value
    f = _fernet()
    if f is None:
        return value
    try:
        return f.decrypt(value.encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError):
        return value
