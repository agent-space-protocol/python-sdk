"""What a law raises to end an action other than `done`. A law that returns ends it as `done`."""

from __future__ import annotations

from typing import Any


class Refused(Exception):
    """Ends the action as `refused`: nothing happened."""

    def __init__(self, code: str, text: str | None = None, data: Any = None):
        super().__init__(text or code)
        self.code, self.text, self.data = code, text, data


class Failed(Exception):
    """Ends the action as `failed`: tried and not achieved, with the effects that did happen."""

    def __init__(self, code: str, text: str | None = None, effects: list[dict[str, Any]] | None = None, data: Any = None):
        super().__init__(text or code)
        self.code, self.text, self.effects, self.data = code, text, effects or [], data


class Stopped(Exception):
    """The action stopped when asked to (cancel, deadline, closing), with the effects from before."""

    def __init__(self, effects: list[dict[str, Any]] | None = None):
        super().__init__("stopped")
        self.effects = effects or []


class Uncertain(Exception):
    """Ends the action as `unknown`: what is known and what is not confirmed."""

    def __init__(self, code: str, text: str, known: dict[str, Any] | None = None, unconfirmed: list[str] | None = None, effects: list[dict[str, Any]] | None = None):
        super().__init__(text)
        self.code, self.text = code, text
        self.known, self.unconfirmed, self.effects = known or {}, unconfirmed or [], effects or []
