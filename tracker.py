"""
json-backed tracker that remembers which messages have been cloned
so we never re-upload the same thing twice.
keys by channel_id:message_id so different channels don't collide.
"""

import json
import os
import tempfile
from datetime import datetime, timezone

from config import TRACKER_FILE


class CloneTracker:
    def __init__(self, path: str = TRACKER_FILE):
        self.path = path
        self.data: dict = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.path):
            with open(self.path, "r") as f:
                return json.load(f)
        return {"cloned_messages": {}, "stats": {"total_cloned": 0, "last_run": None}}

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
        self.data["stats"]["total_cloned"] = len(self.data["cloned_messages"])
        self.data["stats"]["last_run"] = datetime.now(timezone.utc).isoformat()
        self._save()

    def get_stats(self) -> dict:
        return {
            "total_cloned": len(self.data["cloned_messages"]),
            "last_run": self.data["stats"].get("last_run"),
        }
