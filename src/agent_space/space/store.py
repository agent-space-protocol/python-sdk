"""Where a space keeps its record and the requests it remembers: in memory, or in SQLite on disk
so that the record and the IDs survive a restart. All methods are synchronous, so appending and
delivering never interleave."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Protocol

from ..util import now


def complete(entry: dict[str, Any], seq: int) -> dict[str, Any]:
    out: dict[str, Any] = {
        "specversion": "1.0",
        "id": str(seq),
        "source": entry["source"],
        "type": entry["type"],
        "time": entry.get("time") or now(),
        "seq": seq,
        "aspcore": 0,
    }
    for k, v in entry.items():
        if v is not None and k not in out:
            out[k] = v
    return out


class Store(Protocol):
    def append(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]: ...
    def read(self, *, start: int | None = None, to: int | None = None, since: str | None = None, until: str | None = None, limit: int | None = None) -> list[dict[str, Any]]: ...
    def head(self) -> int: ...
    def earliest(self) -> int: ...
    def prune(self, before: str) -> None: ...
    def get_request(self, key: str) -> dict[str, Any] | None: ...
    def put_request(self, request: dict[str, Any]) -> None: ...
    def running_requests(self) -> list[dict[str, Any]]: ...
    def get_meta(self, key: str) -> str | None: ...
    def set_meta(self, key: str, value: str) -> None: ...
    def transaction(self) -> Any: ...
    def close(self) -> None: ...


def _in_range(e: dict[str, Any], start: int | None, to: int | None, since: str | None, until: str | None) -> bool:
    if start is not None and e["seq"] < start:
        return False
    if to is not None and e["seq"] > to:
        return False
    if since is not None and e["time"] < since:
        return False
    if until is not None and e["time"] >= until:
        return False
    return True


class MemoryStore:
    """A store in memory: lost when the process ends."""

    def __init__(self) -> None:
        self._entries: list[dict[str, Any]] = []
        self._head = 0
        self._earliest = 1
        self._requests: dict[str, dict[str, Any]] = {}
        self._meta: dict[str, str] = {}

    def append(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for e in entries:
            self._head += 1
            out.append(complete(e, self._head))
        self._entries.extend(out)
        return out

    def read(self, *, start=None, to=None, since=None, until=None, limit=None):
        out = []
        for e in self._entries:
            if to is not None and e["seq"] > to:
                break
            if _in_range(e, start, to, since, until):
                out.append(e)
                if limit and len(out) >= limit:
                    break
        return out

    def head(self) -> int:
        return self._head

    def earliest(self) -> int:
        return self._earliest

    def prune(self, before: str) -> None:
        cut = 0
        while cut < len(self._entries) and self._entries[cut]["time"] < before:
            cut += 1
        if cut:
            del self._entries[:cut]
            self._earliest = self._entries[0]["seq"] if self._entries else self._head + 1

    def get_request(self, key):
        r = self._requests.get(key)
        return json.loads(json.dumps(r)) if r else None

    def put_request(self, request):
        self._requests[request["key"]] = json.loads(json.dumps(request))

    def running_requests(self):
        return [r for r in self._requests.values() if r["state"] == "running"]

    def get_meta(self, key):
        return self._meta.get(key)

    def set_meta(self, key, value):
        self._meta[key] = value

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def close(self) -> None:
        pass


class SqliteStore:
    """A store in one SQLite file, kept across restarts."""

    def __init__(self, path: str | Path):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._lock = threading.RLock()
        self._depth = 0
        self._db.executescript(
            """
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            CREATE TABLE IF NOT EXISTS entries (seq INTEGER PRIMARY KEY, time TEXT NOT NULL, json TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS entries_time ON entries (time);
            CREATE TABLE IF NOT EXISTS requests (key TEXT PRIMARY KEY, state TEXT NOT NULL, json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        row = self._db.execute("SELECT MAX(seq) FROM entries").fetchone()
        self._head = max(int(row[0] or 0), int(self.get_meta("head") or 0))
        self._earliest = int(self.get_meta("earliest") or 1)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            if self._depth:
                self._depth += 1
                try:
                    yield
                finally:
                    self._depth -= 1
                return
            head = self._head
            self._db.execute("BEGIN IMMEDIATE")
            self._depth = 1
            try:
                yield
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                self._head = head
                raise
            finally:
                self._depth = 0

    def append(self, entries):
        with self.transaction():
            out = []
            for e in entries:
                self._head += 1
                entry = complete(e, self._head)
                self._db.execute("INSERT INTO entries (seq, time, json) VALUES (?, ?, ?)", (entry["seq"], entry["time"], json.dumps(entry)))
                out.append(entry)
            self.set_meta("head", str(self._head))
            return out

    def read(self, *, start=None, to=None, since=None, until=None, limit=None):
        where, args = [], []
        for cond, value in (("seq >= ?", start), ("seq <= ?", to), ("time >= ?", since), ("time < ?", until)):
            if value is not None:
                where.append(cond)
                args.append(value)
        sql = "SELECT json FROM entries" + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY seq" + (f" LIMIT {int(limit)}" if limit else "")
        return [json.loads(r[0]) for r in self._db.execute(sql, args).fetchall()]

    def head(self) -> int:
        return self._head

    def earliest(self) -> int:
        return self._earliest

    def prune(self, before: str) -> None:
        with self.transaction():
            self._db.execute("DELETE FROM entries WHERE time < ?", (before,))
            row = self._db.execute("SELECT MIN(seq) FROM entries").fetchone()
            earliest = self._head + 1 if row[0] is None else int(row[0])
            if earliest != self._earliest:
                self._earliest = earliest
                self.set_meta("earliest", str(earliest))

    def get_request(self, key):
        row = self._db.execute("SELECT json FROM requests WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put_request(self, request):
        self._db.execute(
            "INSERT INTO requests (key, state, json) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET state = excluded.state, json = excluded.json",
            (request["key"], request["state"], json.dumps(request)),
        )

    def running_requests(self):
        return [json.loads(r[0]) for r in self._db.execute("SELECT json FROM requests WHERE state = 'running'").fetchall()]

    def get_meta(self, key):
        row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key, value):
        self._db.execute("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))

    def close(self) -> None:
        self._db.close()
