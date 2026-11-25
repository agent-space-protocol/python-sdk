"""The Python SDK of the Agent Space Protocol.

- `agent_space.participant`: connect to a space, enter, observe, act (async, and `SyncParticipant`).
- `agent_space.space`: write a space; the skeleton keeps the protocol's guarantees.
- `agent_space.transport`: WebSocket, HTTP with SSE, standard input and output.
- `agent_space.identity`: Ed25519 keys, proofs and passes.
- `agent_space.protocol`: typed outcomes and reasons, and validation against `schema.json`.
- `agent_space.types`: TypedDicts generated from the specification's `schema.json`.
"""

from .identity import KeyPair, generate_key, load_or_create_key
from .participant import Participant, Pending, SyncParticipant
from .protocol import End, Outcome, OutcomeError, Reason, ReasonCode
from .util import canonical_json, content_hash

__all__ = [
    "End", "KeyPair", "Outcome", "OutcomeError", "Participant", "Pending", "Reason", "ReasonCode",
    "SyncParticipant", "canonical_json", "content_hash", "generate_key", "load_or_create_key",
]
