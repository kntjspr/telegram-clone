"""
tracker factory — picks the right backend based on TRACKER_BACKEND env var.
all backends share the same interface:
  - is_cloned(channel_id, message_id) -> bool
  - mark_cloned(channel_id, message_id, filename, media_type)
  - mark_skipped(channel_id, message_id, reason, file_size, limit_bytes, filename, media_type)
  - get_stats() -> dict
"""

import json
import os
import tempfile
from datetime import datetime, timezone

from config import TRACKER_BACKEND, TRACKER_FILE


class CloneTracker:
    """json-backed tracker (default). kept here for backwards compat."""

    def __init__(self, path: str = TRACKER_FILE):
        self.path = path
        self.data: dict = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                data = json.load(f)
                if "failed_messages" not in data:
                    data["failed_messages"] = {}
                if "skipped_messages" not in data:
                    data["skipped_messages"] = {}
                return data
        return {
            "cloned_messages": {},
            "failed_messages": {},
            "skipped_messages": {},
            "stats": {"total_cloned": 0, "last_run": None},
        }

    def _save(self):
        """atomic write so a crash mid-save doesn't nuke the tracker"""
        dir_name = os.path.dirname(self.path) or "."
        with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, suffix=".tmp") as tmp:
            json.dump(self.data, tmp, indent=2, default=str)
            tmp_path = tmp.name
        os.replace(tmp_path, self.path)

    def _key(self, channel_id: int | str, message_id: int) -> str:
        return f"{channel_id}:{message_id}"

    def is_cloned(self, channel_id: int | str, message_id: int) -> bool:
        return self._key(channel_id, message_id) in self.data["cloned_messages"]

    def mark_cloned(
        self,
        channel_id: int | str,
        message_id: int,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        self.data["cloned_messages"][self._key(channel_id, message_id)] = {
            "channel_id": str(channel_id),
            "message_id": message_id,
            "filename": filename,
            "media_type": media_type,
            "cloned_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # if it succeeds later, remove it from failed list just in case
        failed_key = self._key(channel_id, message_id)
        if failed_key in self.data.get("failed_messages", {}):
            del self.data["failed_messages"][failed_key]
        if failed_key in self.data.get("skipped_messages", {}):
            del self.data["skipped_messages"][failed_key]
            
        self.data["stats"]["total_cloned"] = len(self.data["cloned_messages"])
        self.data["stats"]["last_run"] = datetime.now(timezone.utc).isoformat()
        self._save()

    def mark_failed(self, channel_id: int | str, message_id: int, reason: str):
        self.data["failed_messages"][self._key(channel_id, message_id)] = {
            "channel_id": str(channel_id),
            "message_id": message_id,
            "reason": reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def mark_skipped(
        self,
        channel_id: int | str,
        message_id: int,
        reason: str,
        file_size: int | None = None,
        limit_bytes: int | None = None,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        self.data["skipped_messages"][self._key(channel_id, message_id)] = {
            "channel_id": str(channel_id),
            "message_id": message_id,
            "reason": reason,
            "file_size": file_size,
            "limit_bytes": limit_bytes,
            "filename": filename,
            "media_type": media_type,
            "skipped_at": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def get_stats(self) -> dict:
        return {
            "total_cloned": len(self.data["cloned_messages"]),
            "last_run": self.data["stats"].get("last_run"),
        }


def create_tracker():
    """factory — returns the right tracker based on TRACKER_BACKEND config."""
    backend = TRACKER_BACKEND.lower()

    if backend == "sqlite":
        from tracker_sqlite import SqliteTracker
        return SqliteTracker()

    if backend == "supabase":
        from tracker_supabase import SupabaseTracker
        return SupabaseTracker()

    return CloneTracker()
