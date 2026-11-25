"""Identity: Ed25519 keys, proofs of identity in `join`, and passes through doors."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from .util import b64url, b64url_decode


@dataclass(frozen=True)
class KeyPair:
    """An Ed25519 key pair. `identity` is the public key written as `ed25519:<base64url>`."""

    private_key: Ed25519PrivateKey

    @property
    def identity(self) -> str:
        raw = self.private_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        return "ed25519:" + b64url(raw)

    def sign(self, data: bytes | str) -> str:
        """Signs bytes, returning the unpadded base64url signature."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        return b64url(self.private_key.sign(data))


def generate_key() -> KeyPair:
    """Makes a new key pair."""
    return KeyPair(Ed25519PrivateKey.generate())


def load_or_create_key(path: str | os.PathLike[str]) -> KeyPair:
    """Loads a key pair from a PKCS#8 PEM file, making one first when the file does not exist."""
    p = Path(path)
    if not p.exists():
        key = generate_key()
        p.parent.mkdir(parents=True, exist_ok=True)
        pem = key.private_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        p.write_bytes(pem)
        try:
            p.chmod(0o600)
        except OSError:
            pass
        return key
    loaded = serialization.load_pem_private_key(p.read_bytes(), password=None)
    if not isinstance(loaded, Ed25519PrivateKey):
        raise ValueError(f"{path} does not hold an Ed25519 key")
    return KeyPair(loaded)


def public_key_of(identity: str) -> Ed25519PublicKey | None:
    if not identity.startswith("ed25519:") or len(identity) != 51:
        return None
    try:
        return Ed25519PublicKey.from_public_bytes(b64url_decode(identity[8:]))
    except Exception:
        return None


def verify(identity: str, data: bytes | str, signature: str) -> bool:
    """Checks a base64url signature by an identity."""
    pk = public_key_of(identity)
    if pk is None:
        return False
    if isinstance(data, str):
        data = data.encode("utf-8")
    try:
        pk.verify(b64url_decode(signature), data)
        return True
    except (InvalidSignature, ValueError):
        return False


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
