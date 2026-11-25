"""Generates src/agent_space/types.py from src/agent_space/schema/schema.json, the copy of the
specification's schema.json.

    python scripts/generate_types.py [--sync <spec-repo>] [--check]

--sync copies schema/draft/schema.json from a checkout of the specification first; --check exits 1
when the schema copy or the generated types differ.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "src" / "agent_space" / "schema" / "schema.json"
TYPES = ROOT / "src" / "agent_space" / "types.py"
HEADER = '"""Types generated from schema.json by scripts/generate_types.py. Do not edit by hand."""\n\n'


def generate(schema_text: str) -> str:
    schema = json.loads(schema_text)
    # A root that names every definition, so that each is generated under its own name.
    root = {
        "title": "AllDefinitions",
        "type": "object",
        "properties": {name: {"$ref": f"#/$defs/{name}"} for name in schema["$defs"]},
        "$defs": schema["$defs"],
    }
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "schema.json"
        out = Path(tmp) / "types.py"
        src.write_text(json.dumps(root), encoding="utf-8")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "datamodel_code_generator",
                "--input", str(src),
                "--input-file-type", "jsonschema",
                "--output", str(out),
                "--output-model-type", "typing.TypedDict",
                "--target-python-version", "3.11",
                "--use-schema-description",
                "--use-field-description",
                "--disable-timestamp",
                "--use-standard-collections",
                "--use-union-operator",
                "--enum-field-as-literal", "all",
            ],
            check=True,
        )
        body = out.read_text(encoding="utf-8").replace("\r\n", "\n")
    # The SDK's base needs nothing beyond the standard library: TypedDict comes from typing.
    body = body.replace("from typing_extensions import TypedDict", "from typing import TypedDict")
    lines = [line for line in body.split("\n") if not line.startswith("#   filename:")]
    return HEADER + "\n".join(lines).lstrip("\n")


def emit(path: Path, content: str, check: bool) -> bool:
    old = path.read_text(encoding="utf-8") if path.exists() else None
    if old == content:
        return True
    if check:
        print(f"out of date: {path.relative_to(ROOT)}", file=sys.stderr)
        return False
    path.write_text(content, encoding="utf-8", newline="\n")
    print(f"wrote {path.relative_to(ROOT)}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    ok = True
    if args.sync:
        spec = Path(args.sync) / "schema" / "draft" / "schema.json"
        ok = emit(SCHEMA, spec.read_text(encoding="utf-8"), args.check) and ok
    ok = emit(TYPES, generate(SCHEMA.read_text(encoding="utf-8")), args.check) and ok
    if ok and args.check:
        print("schema copy and generated types are up to date")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
