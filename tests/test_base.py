"""The base without extras: Ed25519, validation and transports in the standard library, and the
same results as the extras (cryptography, jsonschema, aiohttp) where those are installed."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import textwrap

import pytest

from agent_space import End, Participant, ed25519, generate_key
from agent_space.identity import KeyPair, check_proof, make_proof, verify
from agent_space.protocol import schema, validate
from agent_space.schemalite import Validator

# RFC 8032, section 7.1: TEST 1, TEST 2, TEST 3 and TEST SHA(abc).
RFC8032 = [
    ("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
     "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a", "",
     "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"),
    ("4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
     "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c", "72",
     "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"),
    ("c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
     "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025", "af82",
     "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"),
    ("833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42",
     "ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf", hashlib.sha512(b"abc").hexdigest(),
     "dc2a4459e7369633a52b1bf277839a00201009a3efbf3ecb69bea2186c26b58909351fc9ac90b3ecfdfbc7c66431e0303dca179c138ac17ad9bef1177331a704"),
]


@pytest.mark.parametrize("secret,public,message,signature", RFC8032)
def test_ed25519_rfc8032_vectors(secret, public, message, signature):
    seed, msg = bytes.fromhex(secret), bytes.fromhex(message)
    assert ed25519.public_key(seed).hex() == public
    assert ed25519.sign(seed, msg).hex() == signature
    assert ed25519.verify(bytes.fromhex(public), msg, bytes.fromhex(signature))
    bad = bytearray.fromhex(signature)
    bad[0] ^= 1
    assert not ed25519.verify(bytes.fromhex(public), msg, bytes(bad))
    assert not ed25519.verify(bytes.fromhex(public), msg + b"x", bytes.fromhex(signature))


@pytest.mark.parametrize("secret,public,message,signature", RFC8032)
def test_ed25519_matches_cryptography(secret, public, message, signature):
    crypto = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ed25519")
    from cryptography.hazmat.primitives import serialization

    seed, msg = bytes.fromhex(secret), bytes.fromhex(message)
    key = crypto.Ed25519PrivateKey.from_private_bytes(seed)
    assert key.sign(msg) == ed25519.sign(seed, msg)
    raw = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    assert raw == ed25519.public_key(seed)
    # The PEM the SDK writes is the PEM cryptography writes and reads.
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    assert KeyPair.from_pem(pem).seed == seed
    assert serialization.load_pem_private_key(KeyPair(seed).pem(), password=None).sign(msg) == key.sign(msg)


def test_random_keys_agree_both_ways():
    for _ in range(3):
        key = generate_key()
        sig = key.sign("asp-v0 join space nonce")
        assert verify(key.identity, "asp-v0 join space nonce", sig)
        assert ed25519.verify(key.public, b"asp-v0 join space nonce", key.sign_bytes("asp-v0 join space nonce"))
        assert check_proof(key.identity, "s", "n", make_proof(key, "s", "n"))
        assert not check_proof(key.identity, "s", "other", make_proof(key, "s", "n"))
        assert KeyPair.from_pem(key.pem()) == key


MESSAGES = [
    {"type": "act", "id": "a1", "verb": "test.toggle", "object": "lamp:1", "basis": 3},
    {"type": "act", "id": "a1", "verb": "test.toggle", "basis": -1},
    {"type": "act", "id": "", "verb": "test.toggle"},
    {"type": "act", "verb": "test.toggle"},
    {"type": "act", "id": "a1", "verb": "test.toggle", "deadline": 0},
    {"type": "observe", "id": "o1", "forms": ["structure", "text"]},
    {"type": "observe", "id": "o1", "forms": []},
    {"type": "record", "id": "r1", "since": "2026-11-25T10:00:00.000Z"},
    {"type": "join", "id": "j1", "role": "member", "presentation": {"name": "x"}},
    {"type": "hello", "parts": {"core": [0]}},
    {"type": "hello", "parts": {"core": "0"}},
    {"type": "cancel", "action": "a1"},
    {"type": "edit", "id": "e1", "changes": [{"op": "set", "thing": "lamp:1", "property": "on", "value": True}]},
    {"type": "edit", "id": "e1", "changes": [{"op": "explode"}]},
]

DEFINITIONS = {"act": "Act", "observe": "Observe", "record": "RecordRequest", "join": "Join", "hello": "ParticipantHello", "cancel": "Cancel", "edit": "Edit"}


@pytest.mark.parametrize("message", MESSAGES, ids=lambda m: json.dumps(m)[:60])
def test_schemalite_agrees_with_jsonschema(message):
    jsonschema = pytest.importorskip("jsonschema")
    from referencing import Registry, Resource

    s = schema()
    definition = DEFINITIONS[message["type"]]
    full = jsonschema.Draft202012Validator(
        {"$ref": f"{s['$id']}#/$defs/{definition}"},
        registry=Registry().with_resource(s["$id"], Resource.from_contents(s)),
        format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
    )
    lite = Validator({"$ref": f"#/$defs/{definition}"}, s)
    assert full.is_valid(message) == lite.is_valid(message)


def test_schemalite_on_its_own():
    assert validate("Act", MESSAGES[0]) is None
    assert "basis" in (validate("Act", MESSAGES[1]) or "")
    params = Validator({"type": "object", "properties": {"n": {"type": "integer", "minimum": 1}}, "required": ["n"], "additionalProperties": False})
    assert params.is_valid({"n": 2})
    assert not params.is_valid({"n": 0})
    assert not params.is_valid({"n": 1.5})
    assert not params.is_valid({})
    assert not params.is_valid({"n": 1, "m": 2})
    assert not params.is_valid({"n": True})
    # date-time is checked even where jsonschema has no format checker for it installed.
    assert validate("RecordRequest", {"type": "record", "id": "r1", "since": "2026-11-25T10:00:00.000Z"}) is None
    assert "date-time" in (Validator({"$ref": "#/$defs/RecordRequest"}, schema()).iter_errors({"type": "record", "id": "r1", "since": "yesterday"}).__next__().message)


@pytest.mark.parametrize("scheme", ["ws", "http"])
@pytest.mark.parametrize("space_side,participant_side", [("aiohttp", "stdlib"), ("stdlib", "aiohttp")])
async def test_transports_interoperate(scheme, space_side, participant_side, monkeypatch):
    """The standard-library transports and the aiohttp ones speak the same wire format."""
    pytest.importorskip("aiohttp")
    from agent_space.testing.test_space import create_test_space

    monkeypatch.setenv("AGENT_SPACE_TRANSPORT", space_side)
    space = create_test_space()
    listener = await space.listen(f"{scheme}://127.0.0.1:0")
    monkeypatch.setenv("AGENT_SPACE_TRANSPORT", participant_side)
    p = await Participant.connect(listener.address, key=generate_key())
    await p.join("member")
    await p.subscribe()
    seen = await p.observe(["text"])
    out = await p.act("test.toggle", "lamp:1", basis=seen["seq"])
    assert out.end is End.DONE and out.result == {"on": True}
    big = "x" * 300_000  # larger than one read and one frame header size class
    out = await p.act("test.note", "lamp:1", {"text": big})
    assert out.end in (End.DONE, End.REFUSED)
    await p.close()
    await space.close()


BLOCK = textwrap.dedent(
    """
    import importlib.abc, sys
    class Block(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in {"aiohttp", "cryptography", "jsonschema", "referencing", "typing_extensions", "multidict", "yarl"}:
                raise ImportError(f"{name} is not part of the base")
    sys.meta_path.insert(0, Block())
    """
)


def test_base_works_without_any_extra():
    """With every extra blocked from importing: a space listens over WebSocket, HTTP and standard
    input and output, a participant signs its way in, and messages are validated."""
    script = BLOCK + textwrap.dedent(
        """
        import asyncio
        from agent_space import End, Participant, generate_key
        from agent_space.identity import backend
        from agent_space.protocol import validator_class
        from agent_space.transport import implementation
        from agent_space.testing.test_space import create_test_space

        async def main():
            assert (backend(), validator_class(), implementation()) == ("python", "python", "stdlib")
            for scheme in ("ws", "http"):
                space = create_test_space()
                listener = await space.listen(f"{scheme}://127.0.0.1:0")
                p = await Participant.connect(listener.address, key=generate_key())
                joined = await p.join("member")
                out = await p.act("test.toggle", "lamp:1")
                assert out.end is End.DONE, out
                bad = await p.act("test.add", "counter:1", {"n": "two"})
                assert bad.end is End.REFUSED and bad.reason.code == "malformed", bad
                await p.close(); await space.close()
            print("base ok")

        asyncio.run(main())
        """
    )
    done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr
    assert "base ok" in done.stdout
