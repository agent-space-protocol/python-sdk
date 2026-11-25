"""A JSON Schema (2020-12) validator in the standard library, for the SDK's base.

It covers the keywords the protocol's `schema.json` uses and the ones verb parameters
commonly use: type, enum, const, properties, required, additionalProperties,
patternProperties, items, prefixItems, minItems, maxItems, uniqueItems, minLength,
maxLength, pattern, minimum, maximum, exclusiveMinimum, exclusiveMaximum, multipleOf,
anyOf, oneOf, allOf, not, $ref (local, `#/$defs/...`), and the date-time format. Other
keywords are ignored, as a validator does with annotations. With the `validate` extra
installed, `agent_space.protocol` uses `jsonschema` instead.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterator

_TYPES = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, float) and v.is_integer()),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
}

_DATE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")


def _date_time(value: str) -> bool:
    if not _DATE_TIME.match(value):
        return False
    try:
        datetime.fromisoformat(value.replace("z", "Z").replace("Z", "+00:00").replace("t", "T"))
        return True
    except ValueError:
        return False


def _equal(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    return a == b


class Error:
    def __init__(self, path: list[str | int], message: str):
        self.absolute_path = path
        self.message = message


class Validator:
    """Validates values against one schema, resolving `$ref`s against `root`."""

    def __init__(self, schema: Any, root: Any = None):
        self.schema = schema
        self.root = root if root is not None else schema

    def _resolve(self, ref: str) -> Any:
        if not ref.startswith("#"):
            # A reference to the document by its $id, then a fragment.
            ref = "#" + ref.split("#", 1)[1] if "#" in ref else "#"
        node = self.root
        for part in ref[1:].split("/")[1:] if ref != "#" else []:
            part = part.replace("~1", "/").replace("~0", "~")
            node = node[int(part)] if isinstance(node, list) else node[part]
        return node

    def iter_errors(self, value: Any) -> Iterator[Error]:
        yield from self._check(self.schema, value, [])

    def is_valid(self, value: Any) -> bool:
        return next(self.iter_errors(value), None) is None

    def _check(self, schema: Any, value: Any, path: list[str | int]) -> Iterator[Error]:
        if schema is True or schema is None:
            return
        if schema is False:
            yield Error(path, "is not allowed")
            return
        if "$ref" in schema:
            yield from self._check(self._resolve(schema["$ref"]), value, path)
        t = schema.get("type")
        if t is not None:
            types = t if isinstance(t, list) else [t]
            if not any(_TYPES.get(x, lambda v: True)(value) for x in types):
                yield Error(path, f"{_short(value)} is not of type {', '.join(repr(x) for x in types)}")
                return
        if "const" in schema and not _equal(value, schema["const"]):
            yield Error(path, f"{schema['const']!r} was expected")
        if "enum" in schema and not any(_equal(value, e) for e in schema["enum"]):
            yield Error(path, f"{_short(value)} is not one of {schema['enum']!r}")
        if isinstance(value, str):
            if "minLength" in schema and len(value) < schema["minLength"]:
                yield Error(path, f"{_short(value)} is too short")
            if "maxLength" in schema and len(value) > schema["maxLength"]:
                yield Error(path, f"{_short(value)} is too long")
            if "pattern" in schema and not re.search(schema["pattern"], value):
                yield Error(path, f"{_short(value)} does not match {schema['pattern']!r}")
            if schema.get("format") == "date-time" and not _date_time(value):
                yield Error(path, f"{_short(value)} is not a 'date-time'")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if "minimum" in schema and value < schema["minimum"]:
                yield Error(path, f"{value} is less than the minimum of {schema['minimum']}")
            if "maximum" in schema and value > schema["maximum"]:
                yield Error(path, f"{value} is greater than the maximum of {schema['maximum']}")
            if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
                yield Error(path, f"{value} is less than or equal to the minimum of {schema['exclusiveMinimum']}")
            if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
                yield Error(path, f"{value} is greater than or equal to the maximum of {schema['exclusiveMaximum']}")
            if "multipleOf" in schema and (value / schema["multipleOf"]) % 1:
                yield Error(path, f"{value} is not a multiple of {schema['multipleOf']}")
        if isinstance(value, list):
            if "minItems" in schema and len(value) < schema["minItems"]:
                yield Error(path, f"{_short(value)} should be non-empty" if schema["minItems"] == 1 else f"{_short(value)} is too short")
            if "maxItems" in schema and len(value) > schema["maxItems"]:
                yield Error(path, f"{_short(value)} is too long")
            if schema.get("uniqueItems"):
                seen: list[Any] = []
                for item in value:
                    if any(_equal(item, s) for s in seen):
                        yield Error(path, f"{_short(value)} has non-unique elements")
                        break
                    seen.append(item)
            prefix = schema.get("prefixItems") or []
            for i, (item, sub) in enumerate(zip(value, prefix)):
                yield from self._check(sub, item, path + [i])
            if "items" in schema:
                for i, item in enumerate(value[len(prefix):], start=len(prefix)):
                    yield from self._check(schema["items"], item, path + [i])
        if isinstance(value, dict):
            for name in schema.get("required") or []:
                if name not in value:
                    yield Error(path, f"{name!r} is a required property")
            props = schema.get("properties") or {}
            patterns = schema.get("patternProperties") or {}
            extra: list[str] = []
            for name, item in value.items():
                matched = False
                if name in props:
                    matched = True
                    yield from self._check(props[name], item, path + [name])
                for pattern, sub in patterns.items():
                    if re.search(pattern, name):
                        matched = True
                        yield from self._check(sub, item, path + [name])
                if not matched:
                    extra.append(name)
            additional = schema.get("additionalProperties")
            if additional is False and extra:
                yield Error(path, f"Additional properties are not allowed ({', '.join(repr(e) for e in extra)} {'was' if len(extra) == 1 else 'were'} unexpected)")
            elif isinstance(additional, dict):
                for name in extra:
                    yield from self._check(additional, value[name], path + [name])
            if "minProperties" in schema and len(value) < schema["minProperties"]:
                yield Error(path, f"{_short(value)} does not have enough properties")
        for sub in schema.get("allOf") or []:
            yield from self._check(sub, value, path)
        if "anyOf" in schema and not any(Validator(sub, self.root).is_valid(value) for sub in schema["anyOf"]):
            yield Error(path, f"{_short(value)} is not valid under any of the given schemas")
        if "oneOf" in schema:
            n = sum(1 for sub in schema["oneOf"] if Validator(sub, self.root).is_valid(value))
            if n != 1:
                yield Error(path, f"{_short(value)} is {'not valid under any' if n == 0 else 'valid under each of several'} of the given schemas")
        if "not" in schema and Validator(schema["not"], self.root).is_valid(value):
            yield Error(path, f"{_short(value)} should not be valid under {schema['not']!r}")


def _short(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."
