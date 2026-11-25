"""Canonical JSON, content hashes, times and tokens, the same as the TypeScript SDK's."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import secrets
from datetime import datetime, timezone
from typing import Any


def _number(value: float | int) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if not math.isfinite(value):
        raise ValueError("non-finite number")
    if value == int(value) and abs(value) < 1e21:
        return str(int(value))
    # ECMAScript number formatting, as RFC 8785 requires.
    text = repr(value)
    if "e" in text:
        mantissa, exp = text.split("e")
        exponent = int(exp)
        mantissa = mantissa.rstrip("0").rstrip(".") if "." in mantissa else mantissa
        if -7 < exponent < 21:
            return format(value, "f").rstrip("0").rstrip(".")
        return f"{mantissa}e{'+' if exponent > 0 else '-'}{abs(exponent)}"
    return text


def canonical_json(value: Any) -> str:
    """RFC 8785 canonical JSON: keys sorted by UTF-16 code units, no whitespace, ECMAScript numbers."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return _number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical_json(v) for v in value) + "]"
    if isinstance(value, dict):
        keys = sorted((k for k, v in value.items()), key=lambda k: k.encode("utf-16-be"))
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + canonical_json(value[k]) for k in keys) + "}"
    raise TypeError(f"not JSON: {type(value).__name__}")


def content_hash(content: bytes | str | Any) -> str:
    """`sha256:<hex>` of bytes, of text, or of the canonical JSON of a value."""
    if isinstance(content, bytes):
        data = content
    elif isinstance(content, str):
        data = content.encode("utf-8")
    else:
        data = canonical_json(content).encode("utf-8")
    return "sha256:" + hashlib.sha256(data).hexdigest()


def now() -> str:
    """The current time as RFC 3339 in UTC, to the millisecond, as JavaScript writes it."""
    t = datetime.now(timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def iso_after(seconds: float) -> str:
    t = datetime.now(timezone.utc).timestamp() + seconds
    d = datetime.fromtimestamp(t, timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{d.microsecond // 1000:03d}Z"


def random_token(nbytes: int = 16) -> str:
    return b64url(secrets.token_bytes(nbytes))


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def strip_none(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}
