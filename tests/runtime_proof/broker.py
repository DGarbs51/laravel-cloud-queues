"""Single-queue broker for timeout and watchdog proofs.

Stdlib sqlite only. Semantics match the proof contract:

* receive hides a visible message until now + visibility and increments the count
* renew extends visibility for a live receipt and fails when the receipt is gone
* delete completes a message and invalidates its receipt
* a message whose deadline has passed is received again with count + 1
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass(frozen=True)
class Delivery:
    message_id: str
    payload: str
    receipt: str
    receive_count: int
    visibility_until: float


class Broker:
    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self._immediate() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    receive_count INTEGER NOT NULL DEFAULT 0,
                    visibility_until REAL NOT NULL DEFAULT 0,
                    receipt TEXT,
                    deleted INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT,
                    op TEXT NOT NULL,
                    at REAL NOT NULL,
                    receipt TEXT,
                    detail TEXT
                )
                """
            )

    def publish(self, payload: str, message_id: str | None = None) -> str:
        message_id = message_id or uuid.uuid4().hex
        with self._immediate() as conn:
            conn.execute(
                "INSERT INTO messages (id, payload) VALUES (?, ?)",
                (message_id, payload),
            )
            self._op(conn, message_id, "publish", None, None)
        return message_id

    def receive(self, visibility: float, now: float | None = None) -> Delivery | None:
        now = time.time() if now is None else now
        with self._immediate() as conn:
            row = conn.execute(
                """
                SELECT id, payload, receive_count FROM messages
                WHERE deleted = 0 AND visibility_until <= ?
                ORDER BY id LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            receipt = uuid.uuid4().hex
            count = int(row["receive_count"]) + 1
            until = now + visibility
            conn.execute(
                """
                UPDATE messages
                SET receive_count = ?, visibility_until = ?, receipt = ?
                WHERE id = ?
                """,
                (count, until, receipt, row["id"]),
            )
            self._op(conn, row["id"], "receive", receipt, str(count))
            return Delivery(row["id"], row["payload"], receipt, count, until)

    def renew(self, receipt: str, visibility: float, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        with self._immediate() as conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE receipt = ? AND deleted = 0",
                (receipt,),
            ).fetchone()
            if row is None:
                self._op(conn, self._message_for_receipt(conn, receipt), "renew_failed", receipt, "receipt invalid")
                return False
            conn.execute(
                "UPDATE messages SET visibility_until = ? WHERE id = ?",
                (now + visibility, row["id"]),
            )
            self._op(conn, row["id"], "renew", receipt, None)
            return True

    def delete(self, receipt: str) -> bool:
        with self._immediate() as conn:
            row = conn.execute(
                "SELECT id FROM messages WHERE receipt = ? AND deleted = 0",
                (receipt,),
            ).fetchone()
            if row is None:
                self._op(conn, self._message_for_receipt(conn, receipt), "delete_failed", receipt, "receipt invalid")
                return False
            conn.execute(
                "UPDATE messages SET deleted = 1, receipt = NULL WHERE id = ?",
                (row["id"],),
            )
            self._op(conn, row["id"], "delete", receipt, None)
            return True

    def owns(self, receipt: str) -> bool:
        conn = self.connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM messages WHERE receipt = ? AND deleted = 0",
                (receipt,),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def get(self, message_id: str) -> dict[str, object]:
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
            if row is None:
                raise KeyError(message_id)
            return dict(row)
        finally:
            conn.close()

    def operations(self, message_id: str) -> list[dict[str, object]]:
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT op, at, receipt, detail FROM ops WHERE message_id = ? ORDER BY id",
                (message_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _op(
        conn: sqlite3.Connection,
        message_id: str | None,
        op: str,
        receipt: str | None,
        detail: str | None,
    ) -> None:
        conn.execute(
            "INSERT INTO ops (message_id, op, at, receipt, detail) VALUES (?, ?, ?, ?, ?)",
            (message_id, op, time.time(), receipt, detail),
        )

    @staticmethod
    def _message_for_receipt(conn: sqlite3.Connection, receipt: str) -> str | None:
        row = conn.execute(
            """
            SELECT message_id FROM ops
            WHERE receipt = ? AND message_id IS NOT NULL
            ORDER BY id DESC LIMIT 1
            """,
            (receipt,),
        ).fetchone()
        if row is None:
            return None
        return str(row["message_id"])
