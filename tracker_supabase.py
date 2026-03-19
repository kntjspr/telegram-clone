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

    create table skipped_messages (
        channel_id text not null,
        message_id bigint not null,
        reason text,
        file_size bigint,
        limit_bytes bigint,
        filename text,
        media_type text,
        skipped_at timestamptz not null default now(),
        primary key (channel_id, message_id)
    );
"""

import logging
import os
import random
import time
from datetime import datetime, timezone

import asyncio
import httpx
from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger("tracker.supabase")

TABLE = "cloned_messages"

DEFAULT_RETRIES = int(os.getenv("SUPABASE_RETRIES", "5"))
BASE_RETRY_DELAY = float(os.getenv("SUPABASE_RETRY_BASE", "0.5"))
MAX_RETRY_DELAY = float(os.getenv("SUPABASE_RETRY_MAX", "10.0"))
RETRY_JITTER_PCT = float(os.getenv("SUPABASE_RETRY_JITTER_PCT", "0.2"))

RETRYABLE_EXCS = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.ReadError,
    httpx.WriteError,
    httpx.NetworkError,
    httpx.TimeoutException,
)


class SupabaseTracker:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    def _with_retries(self, op_name: str, fn):
        attempts = 0
        while True:
            attempts += 1
            try:
                return fn()
            except RETRYABLE_EXCS as exc:
                if attempts >= DEFAULT_RETRIES:
                    log.error(
                        "supabase %s failed after %s attempts: %s",
                        op_name,
                        attempts,
                        exc,
                    )
                    raise
                delay = BASE_RETRY_DELAY * (2 ** (attempts - 1))
                delay = min(delay, MAX_RETRY_DELAY)
                jitter = delay * RETRY_JITTER_PCT
                delay = max(0.1, delay + random.uniform(-jitter, jitter))
                log.warning(
                    "supabase %s error (%s/%s): %s — retrying in %.1fs",
                    op_name,
                    attempts,
                    DEFAULT_RETRIES,
                    exc,
                    delay,
                )
                time.sleep(delay)

    async def ais_cloned(self, channel_id: int | str, message_id: int) -> bool:
        return await asyncio.to_thread(self.is_cloned, channel_id, message_id)

    async def amark_cloned(
        self,
        channel_id: int | str,
        message_id: int,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        return await asyncio.to_thread(
            self.mark_cloned, channel_id, message_id, filename, media_type
        )

    async def amark_failed(self, channel_id: int | str, message_id: int, reason: str):
        return await asyncio.to_thread(self.mark_failed, channel_id, message_id, reason)

    async def amark_skipped(
        self,
        channel_id: int | str,
        message_id: int,
        reason: str,
        file_size: int | None = None,
        limit_bytes: int | None = None,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        return await asyncio.to_thread(
            self.mark_skipped,
            channel_id,
            message_id,
            reason,
            file_size,
            limit_bytes,
            filename,
            media_type,
        )

    async def aget_stats(self) -> dict:
        return await asyncio.to_thread(self.get_stats)

    def is_cloned(self, channel_id: int | str, message_id: int) -> bool:
        def _run():
            return (
                self.client.table(TABLE)
                .select("message_id")
                .eq("channel_id", str(channel_id))
                .eq("message_id", message_id)
                .limit(1)
                .execute()
            )

        res = self._with_retries("is_cloned", _run)
        return len(res.data) > 0

    def mark_cloned(
        self,
        channel_id: int | str,
        message_id: int,
        filename: str | None = None,
        media_type: str | None = None,
    ):
        now = datetime.now(timezone.utc).isoformat()
        def _upsert():
            return (
                self.client.table(TABLE)
                .upsert(
                    {
                        "channel_id": str(channel_id),
                        "message_id": message_id,
                        "filename": filename,
                        "media_type": media_type,
                        "cloned_at": now,
                    },
                    on_conflict="channel_id,message_id",
                )
                .execute()
            )

        self._with_retries("mark_cloned", _upsert)

        # cleanup from failed_messages just in case it was retried successfully
        def _cleanup_failed():
            return (
                self.client.table("failed_messages")
                .delete()
                .eq("channel_id", str(channel_id))
                .eq("message_id", message_id)
                .execute()
            )

        self._with_retries("cleanup_failed", _cleanup_failed)

    def mark_failed(self, channel_id: int | str, message_id: int, reason: str):
        now = datetime.now(timezone.utc).isoformat()
        def _run():
            return (
                self.client.table("failed_messages")
                .upsert(
                    {
                        "channel_id": str(channel_id),
                        "message_id": message_id,
                        "reason": reason,
                        "failed_at": now,
                    },
                    on_conflict="channel_id,message_id",
                )
                .execute()
            )

        self._with_retries("mark_failed", _run)

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
        now = datetime.now(timezone.utc).isoformat()
        def _run():
            return (
                self.client.table("skipped_messages")
                .upsert(
                    {
                        "channel_id": str(channel_id),
                        "message_id": message_id,
                        "reason": reason,
                        "file_size": file_size,
                        "limit_bytes": limit_bytes,
                        "filename": filename,
                        "media_type": media_type,
                        "skipped_at": now,
                    },
                    on_conflict="channel_id,message_id",
                )
                .execute()
            )

        self._with_retries("mark_skipped", _run)

    def get_stats(self) -> dict:
        def _count():
            return (
                self.client.table(TABLE)
                .select("message_id", count="exact")
                .execute()
            )

        count_res = self._with_retries("get_stats.count", _count)
        total = count_res.count if count_res.count is not None else 0

        def _last():
            return (
                self.client.table(TABLE)
                .select("cloned_at")
                .order("cloned_at", desc=True)
                .limit(1)
                .execute()
            )

        last_res = self._with_retries("get_stats.last", _last)
        last_run = last_res.data[0]["cloned_at"] if last_res.data else None

        return {"total_cloned": total, "last_run": last_run}
