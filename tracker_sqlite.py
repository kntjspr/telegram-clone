"""
sqlite-backed tracker — same interface as the json tracker
but uses a real database so it handles concurrent access and
doesn't slow down with thousands of entries.
"""

import sqlite3
import threading
from datetime import datetime, timezone

from config import SQLITE_DB


class SqliteTracker:
    def __init__(self, db_path: str = SQLITE_DB):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    @property
    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _init_db(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS cloned_messages (
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                filename TEXT,
                media_type TEXT,
                cloned_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS failed_messages (
                channel_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                reason TEXT,
                failed_at TEXT NOT NULL,
                PRIMARY KEY (channel_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        self._conn.commit()

    def is_cloned(self, channel_id: int | str, message_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM cloned_messages WHERE channel_id = ? AND message_id = ?",
            (str(channel_id), message_id),
        ).fetchone()
        return row is not None

    def mark_cloned(
        self,
        channel_id: int | str,
        message_id: int,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO cloned_messages
               (channel_id, message_id, filename, media_type, cloned_at)
               VALUES (?, ?, ?, ?, ?)""",
            (str(channel_id), message_id, filename, media_type, now),
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO stats (key, value) VALUES ('last_run', ?)",
            (now,),
        )
        self._conn.execute(
            "DELETE FROM failed_messages WHERE channel_id = ? AND message_id = ?",
            (str(channel_id), message_id),
        )
        self._conn.commit()

    def mark_failed(self, channel_id: int | str, message_id: int, reason: str):
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO failed_messages
               (channel_id, message_id, reason, failed_at)
               VALUES (?, ?, ?, ?)""",
            (str(channel_id), message_id, reason, now),
        )
        self._conn.commit()

    def get_stats(self) -> dict:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM cloned_messages"
        ).fetchone()[0]

        row = self._conn.execute(
            "SELECT value FROM stats WHERE key = 'last_run'"
        ).fetchone()
        last_run = row[0] if row else None

        return {"total_cloned": count, "last_run": last_run}
