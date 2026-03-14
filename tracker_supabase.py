"""
supabase-backed tracker — persists clone state to a remote postgres db.
survives machine restarts, ephemeral envs like colab, and lets you
check progress from anywhere.

requires a 'cloned_messages' table in supabase:

    create table cloned_messages (
        channel_id text not null,
        message_id bigint not null,
        filename text,
        media_type text,
        cloned_at timestamptz not null default now(),
        primary key (channel_id, message_id)
    );

    create table failed_messages (
        channel_id text not null,
        message_id bigint not null,
        reason text,
        failed_at timestamptz not null default now(),
        primary key (channel_id, message_id)
    );
"""

import logging
from datetime import datetime, timezone

from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger("tracker.supabase")

TABLE = "cloned_messages"


class SupabaseTracker:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def is_cloned(self, channel_id: int | str, message_id: int) -> bool:
        res = (
            self.client.table(TABLE)
            .select("message_id")
            .eq("channel_id", str(channel_id))
            .eq("message_id", message_id)
            .limit(1)
            .execute()
        )
        return len(res.data) > 0

    def mark_cloned(
        self,
        channel_id: int | str,
        message_id: int,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        now = datetime.now(timezone.utc).isoformat()
        self.client.table(TABLE).upsert(
            {
                "channel_id": str(channel_id),
                "message_id": message_id,
                "filename": filename,
                "media_type": media_type,
                "cloned_at": now,
            },
            on_conflict="channel_id,message_id",
        ).execute()

        # cleanup from failed_messages just in case it was retried successfully
        self.client.table("failed_messages").delete().eq("channel_id", str(channel_id)).eq("message_id", message_id).execute()

    def mark_failed(self, channel_id: int | str, message_id: int, reason: str):
        now = datetime.now(timezone.utc).isoformat()
        self.client.table("failed_messages").upsert(
            {
                "channel_id": str(channel_id),
                "message_id": message_id,
                "reason": reason,
                "failed_at": now,
            },
            on_conflict="channel_id,message_id",
        ).execute()

    def get_stats(self) -> dict:
        count_res = (
            self.client.table(TABLE)
            .select("message_id", count="exact")
            .execute()
        )
        total = count_res.count if count_res.count is not None else 0

        last_res = (
            self.client.table(TABLE)
            .select("cloned_at")
            .order("cloned_at", desc=True)
            .limit(1)
            .execute()
        )
        last_run = last_res.data[0]["cloned_at"] if last_res.data else None

        return {"total_cloned": total, "last_run": last_run}
