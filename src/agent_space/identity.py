"""Identity: Ed25519 keys, proofs of identity in `join`, and passes through doors."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from . import ed25519
from .util import b64url, b64url_decode

try:  # the `crypto` extra: the same signatures, computed faster
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as _CPrivate, Ed25519PublicKey as _CPublic
    from cryptography.exceptions import InvalidSignature as _CInvalid
except ImportError:  # pragma: no cover - the base without extras
    _CPrivate = _CPublic = None  # type: ignore[assignment,misc]

# PKCS#8 for an Ed25519 key is a fixed DER prefix followed by the 32-byte seed (RFC 8410).
_PKCS8_PREFIX = bytes.fromhex("302e020100300506032b657004220420")


def backend() -> str:
    """Which Ed25519 implementation signs and verifies: "cryptography" or "python"."""
    return "cryptography" if _CPrivate is not None else "python"


@dataclass(frozen=True)
class KeyPair:
    """An Ed25519 key pair, kept as its 32-byte private seed. `identity` is the public key
    written as `ed25519:<base64url>`."""

    seed: bytes
    public: bytes = b""

    def __post_init__(self) -> None:
        if len(self.seed) != 32:
            raise ValueError("an Ed25519 seed is 32 bytes")
        if not self.public:
            object.__setattr__(self, "public", _public_key(self.seed))

    @property
    def identity(self) -> str:
        return "ed25519:" + b64url(self.public)

    def sign_bytes(self, data: bytes | str) -> bytes:
        if isinstance(data, str):
            data = data.encode("utf-8")
        if _CPrivate is not None:
            return _CPrivate.from_private_bytes(self.seed).sign(data)
        return ed25519.sign(self.seed, data, self.public)

    def sign(self, data: bytes | str) -> str:
        """Signs bytes, returning the unpadded base64url signature."""
        return b64url(self.sign_bytes(data))

    def pem(self) -> bytes:
        """The private key as a PKCS#8 PEM file."""
        import base64

        body = base64.b64encode(_PKCS8_PREFIX + self.seed).decode("ascii")
        lines = ["-----BEGIN PRIVATE KEY-----", body, "-----END PRIVATE KEY-----", ""]
        return "\n".join(lines).encode("ascii")

    @staticmethod
    def from_pem(data: bytes | str) -> KeyPair:
        import base64

        text = data.decode("ascii") if isinstance(data, bytes) else data
        lines = [line.strip() for line in text.strip().splitlines()]
        if not lines or lines[0] != "-----BEGIN PRIVATE KEY-----" or lines[-1] != "-----END PRIVATE KEY-----":
            raise ValueError("not a PKCS#8 PEM private key")
        der = base64.b64decode("".join(lines[1:-1]))
        if len(der) != 48 or not der.startswith(_PKCS8_PREFIX):
            raise ValueError("not an Ed25519 key")
        return KeyPair(der[16:])


def _public_key(seed: bytes) -> bytes:
    if _CPrivate is not None:
        from cryptography.hazmat.primitives import serialization

        return _CPrivate.from_private_bytes(seed).public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return ed25519.public_key(seed)


def generate_key() -> KeyPair:
    """Makes a new key pair."""
    return KeyPair(os.urandom(32))


def load_or_create_key(path: str | os.PathLike[str]) -> KeyPair:
    """Loads a key pair from a PKCS#8 PEM file, making one first when the file does not exist."""
    p = Path(path)
    if not p.exists():
        key = generate_key()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(key.pem())
        try:
            p.chmod(0o600)
        except OSError:
            pass
        return key
    try:
        return KeyPair.from_pem(p.read_bytes())
    except ValueError as e:
        raise ValueError(f"{path} does not hold an Ed25519 key: {e}") from None


def public_key_of(identity: str) -> bytes | None:
    """The 32 raw bytes of an `ed25519:` identity, or None when it is not one."""
    if not identity.startswith("ed25519:") or len(identity) != 51:
        return None
    try:
        raw = b64url_decode(identity[8:])
    except ValueError:
        return None
    return raw if len(raw) == 32 else None


def verify(identity: str, data: bytes | str, signature: str) -> bool:
    """Checks a base64url signature by an identity."""
    pk = public_key_of(identity)
    if pk is None:
        return False
    if isinstance(data, str):
        data = data.encode("utf-8")
    try:
        sig = b64url_decode(signature)
    except ValueError:
        return False
    if _CPublic is not None:
        try:
            _CPublic.from_public_bytes(pk).verify(sig, data)
            return True
        except (_CInvalid, ValueError):
            return False
    return ed25519.verify(pk, data, sig)


def proof_text(space: str, nonce: str) -> str:
    """The text a participant signs in `join`."""
    return f"asp-v0 join {space} {nonce}"


def make_proof(key: KeyPair, space: str, nonce: str) -> str:
    return key.sign(proof_text(space, nonce))


def check_proof(identity: str, space: str, nonce: str, proof: str) -> bool:
    return verify(identity, proof_text(space, nonce), proof)


def make_pass(space_key: KeyPair, payload: dict[str, Any]) -> str:
    """Makes a pass, signed by the space's key: `<payload>.<signature>`, both base64url."""
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return b64url(body) + "." + space_key.sign(body)


def read_pass(token: str, issuer: str) -> dict[str, Any] | None:
    """Reads and checks a pass. Returns its payload when signed by `issuer` and not expired."""
    try:
        body, signature = token.split(".")
        data = b64url_decode(body)
    except ValueError:
        return None
    if not verify(issuer, data, signature):
        return None
    try:
        payload = json.loads(data)
        expires = datetime.fromisoformat(payload["expires"].replace("Z", "+00:00"))
    except Exception:
        return None
    if expires.timestamp() < datetime.now().timestamp():
        return None
    return payload


def peek_pass(token: str) -> dict[str, Any] | None:
    """Reads a pass without checking it, to find out who issued it."""
    try:
        return json.loads(b64url_decode(token.split(".")[0]))
    except Exception:
        return None
