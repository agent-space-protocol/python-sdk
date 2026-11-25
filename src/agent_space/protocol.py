"""The protocol's fixed names, typed outcomes and reasons, and validation against schema.json."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from functools import lru_cache
from importlib import resources
from typing import Any

from . import schemalite


class End(StrEnum):
    """The five final outcomes."""

    DONE = "done"
    REFUSED = "refused"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class ReasonCode(StrEnum):
    """The reasons the protocol defines. Any other reason carries a namespace and stays a `str`."""

    UNSUPPORTED = "unsupported"
    FORBIDDEN = "forbidden"
    MALFORMED = "malformed"
    STALE = "stale"
    CONSTRAINT = "constraint"
    DEADLINE = "deadline"
    CLOSED = "closed"
    RESTARTED = "restarted"
    EXPIRED = "expired"


def reason_code(code: str) -> ReasonCode | str:
    """A protocol reason as a `ReasonCode`; any other code as it is."""
    try:
        return ReasonCode(code)
    except ValueError:
        return code


@dataclass(frozen=True)
class Reason:
    code: ReasonCode | str
    text: str | None = None
    data: Any = None

    @staticmethod
    def of(raw: dict[str, Any] | None) -> Reason | None:
        if not raw:
            return None
        return Reason(reason_code(raw.get("code", "")), raw.get("text"), raw.get("data"))


@dataclass(frozen=True)
class Outcome:
    """The final outcome of a request, with a typed end and reason. `raw` is the message as received."""

    id: str
    end: End
    reason: Reason | None = None
    result: Any = None
    effects: list[dict[str, Any]] = field(default_factory=list)
    events: list[int] = field(default_factory=list)
    seq: int | None = None
    known: dict[str, Any] = field(default_factory=dict)
    unconfirmed: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @staticmethod
    def of(msg: dict[str, Any]) -> Outcome:
        return Outcome(
            id=msg["id"],
            end=End(msg["end"]),
            reason=Reason.of(msg.get("reason")),
            result=msg.get("result"),
            effects=msg.get("effects") or [],
            events=msg.get("events") or [],
            seq=msg.get("seq"),
            known=msg.get("known") or {},
            unconfirmed=msg.get("unconfirmed") or [],
            raw=msg,
        )

    @property
    def done(self) -> bool:
        return self.end is End.DONE


class OutcomeError(Exception):
    """A final answer that is not `done`."""

    def __init__(self, outcome: Outcome):
        self.outcome = outcome
        r = outcome.reason
        super().__init__(f"{outcome.end}" + (f": {r.code}" + (f" ({r.text})" if r.text else "") if r else ""))


PARTICIPANT_MESSAGES: dict[str, str] = {
    "hello": "ParticipantHello",
    "join": "Join",
    "leave": "Leave",
    "observe": "Observe",
    "subscribe": "Subscribe",
    "act": "Act",
    "edit": "Edit",
    "cancel": "Cancel",
    "record": "RecordRequest",
    "vocabulary": "VocabularyRequest",
    "blob": "BlobRequest",
    "stream": "StreamRequest",
    "consent": "ConsentMessage",
    "snapshot": "SnapshotRequest",
    "restore": "RestoreRequest",
    "episode": "EpisodeRequest",
    "score": "ScoreRequest",
    "pause": "PauseRequest",
    "resume": "ResumeRequest",
    "sense": "SenseRequest",
    "views": "ViewsRequest",
    "offer": "Offer",
    "take": "Take",
}

SPACE_MESSAGES: dict[str, str] = {
    "hello": "SpaceHello",
    "observation": "Observation",
    "subscribed": "Subscribed",
    "accepted": "Accepted",
    "progress": "Progress",
    "waiting": "Waiting",
    "outcome": "Outcome",
    "event": "EventMessage",
    "entries": "Entries",
    "vocabulary": "VocabularyMessage",
    "blob": "BlobMessage",
    "frame": "Frame",
    "error": "ErrorMessage",
}

#: The capability each capability message needs. Except for `edit`, a core message, it is also its part.
MESSAGE_CAPABILITY: dict[str, str] = {
    "edit": "edit",
    "stream": "forms",
    "consent": "consent",
    "snapshot": "snapshot",
    "restore": "snapshot",
    "episode": "episode",
    "score": "episode",
    "pause": "pause",
    "resume": "pause",
    "sense": "senses",
    "views": "views",
    "offer": "doors",
    "take": "doors",
}

CAPABILITY_PARTS = ("forms", "snapshot", "episode", "consent", "pause", "senses", "views", "doors")

#: The versions of each part this SDK implements.
SDK_PARTS: dict[str, list[int]] = {"core": [0], **{p: [0] for p in CAPABILITY_PARTS}}

INTERMEDIATE = frozenset({"accepted", "progress", "waiting", "frame"})


@lru_cache(maxsize=1)
def schema() -> dict[str, Any]:
    """The JSON Schema of every message, as published by the specification."""
    return json.loads(resources.files("agent_space").joinpath("schema/schema.json").read_text(encoding="utf-8"))


def validator_class() -> str:
    """Which validator checks messages: "jsonschema" (the `validate` extra) or "python"."""
    try:
        import jsonschema  # noqa: F401
        import referencing  # noqa: F401
    except ImportError:
        return "python"
    return "jsonschema"


@lru_cache(maxsize=None)
def _validator(definition: str) -> Any:
    s = schema()
    uri = s["$id"]
    if validator_class() == "jsonschema":
        from jsonschema import Draft202012Validator
        from referencing import Registry, Resource

        registry = Registry().with_resource(uri, Resource.from_contents(s))
        return Draft202012Validator({"$ref": f"{uri}#/$defs/{definition}"}, registry=registry, format_checker=Draft202012Validator.FORMAT_CHECKER)
    return schemalite.Validator({"$ref": f"#/$defs/{definition}"}, s)


def schema_validator(params: dict[str, Any]) -> Any:
    """A validator of one standalone schema, such as a verb's parameters: `iter_errors(value)`
    yields errors with `absolute_path` and `message`."""
    if validator_class() == "jsonschema":
        from jsonschema import Draft202012Validator

        return Draft202012Validator(params)
    return schemalite.Validator(params)


def validate(definition: str, value: Any) -> str | None:
    """Validates a value against one definition of the schema. Returns a sentence on failure."""
    error = next(iter(_validator(definition).iter_errors(value)), None)
    if error is None:
        return None
    path = "/" + "/".join(str(p) for p in error.absolute_path) if error.absolute_path else "message"
    return f"{path} {error.message}"
