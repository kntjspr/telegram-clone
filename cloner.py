"""
the actual cloning engine — iterates source channel messages,
downloads media, re-uploads to dest, deletes local files, tracks everything.
uses FastTelethon for parallel downloads/uploads on big files.
"""

import asyncio
import os
import shutil
import logging
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from telethon import TelegramClient, utils
from telethon.errors.rpcerrorlist import FileReferenceExpiredError
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    InputMediaUploadedDocument, InputMediaUploadedPhoto,
)

from config import DOWNLOAD_DIR
from tracker import CloneTracker
from fast_telethon import download_file, upload_file

log = logging.getLogger("cloner")

# files above this size use parallel transfer (5 MB)
FAST_TRANSFER_THRESHOLD = 5 * 1024 * 1024
MAX_RETRY_DELAY = 300.0  # seconds, cap for exponential backoff
RETRY_JITTER_PCT = 0.2   # +/- 20%
FREE_UPLOAD_LIMIT = 2 * 1024 * 1024 * 1024
PREMIUM_UPLOAD_LIMIT = 4 * 1024 * 1024 * 1024


async def _tracker_call(tracker: CloneTracker, method: str, *args, **kwargs):
    async_method = getattr(tracker, f"a{method}", None)
    if async_method is not None:
        return await async_method(*args, **kwargs)
    return getattr(tracker, method)(*args, **kwargs)


def _media_type(message) -> str | None:
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.audio:
        return "audio"
    if message.voice:
        return "voice"
    if message.video_note:
        return "video_note"
    if message.sticker:
        return "sticker"
    if message.gif:
        return "gif"
    if message.document:
        return "document"
    return None


def _file_size_from_message(message) -> int:
    """try to get file size from message media."""
    if message.document:
        return message.document.size or 0
    if message.photo:
        # photos are usually small, pick the largest size
        if hasattr(message.photo, "sizes") and message.photo.sizes:
            for size in reversed(message.photo.sizes):
                if hasattr(size, "size"):
                    return size.size
    return 0


def _human_size(nbytes: float) -> str:
    """turn byte count into something readable."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            if unit == "B":
                return f"{int(nbytes)} {unit}"
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.2f} TB"


async def _get_upload_limit(client: TelegramClient) -> tuple[int, bool]:
    me = await client.get_me()
    is_premium = bool(getattr(me, "premium", False))
    return (PREMIUM_UPLOAD_LIMIT if is_premium else FREE_UPLOAD_LIMIT), is_premium


def _guess_filename(message) -> str:
    pre_name = "media"
    if message.document and hasattr(message.document, "attributes"):
        for attr in message.document.attributes:
            if hasattr(attr, "file_name") and attr.file_name:
                pre_name = attr.file_name
                break
    return pre_name


def _should_skip_over_limit(message, limit_bytes: int) -> tuple[bool, int, str]:
    size_bytes = _file_size_from_message(message)
    if size_bytes and limit_bytes and size_bytes > limit_bytes:
        return True, size_bytes, _guess_filename(message)
    return False, size_bytes, _guess_filename(message)


def _cleanup_download_dir(dir_path: str) -> int:
    """remove leftover files from a previous run. returns number removed."""
    if not os.path.isdir(dir_path):
        return 0
    removed = 0
    for name in os.listdir(dir_path):
        path = os.path.join(dir_path, name)
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            removed += 1
        except Exception as exc:
            log.warning(f"couldn't remove leftover download {path!r}: {exc}")
    return removed


def _cleanup_new_downloads(before: set[str], dir_path: str) -> None:
    """remove any new files created during a failed download."""
    if not os.path.isdir(dir_path):
        return
    for name in os.listdir(dir_path):
        if name in before:
            continue
        path = os.path.join(dir_path, name)
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            log.debug(f"deleted partial file: {name}")
        except Exception as exc:
            log.warning(f"couldn't remove partial download {path!r}: {exc}")


async def clone_channel(
    client: TelegramClient,
    source,
    dest,
    tracker: CloneTracker,
    rate_limit_delay: float = 2.0,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: asyncio.Event | None = None,
    max_retries: int = 0,
    retry_delay: float = 5.0,
    follow: bool = True,
    follow_poll_interval: float = 5.0,
):
    """clone all messages from source channel to dest channel."""

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    removed = _cleanup_download_dir(DOWNLOAD_DIR)
    if removed:
        log.info(f"cleared {removed} leftover downloads before starting")

    source_entity = await client.get_entity(source)
    dest_entity = await client.get_entity(dest)
    source_id = source_entity.id

    upload_limit, is_premium = await _get_upload_limit(client)
    log.info(
        "account tier: %s | upload limit: %s",
        "premium" if is_premium else "free",
        _human_size(upload_limit),
    )

    log.info(f"source: {getattr(source_entity, 'title', source)} (id: {source_id})")
    log.info(f"dest:   {getattr(dest_entity, 'title', dest)}")

    # grab total message count before iterating
    history = await client.get_messages(source_entity, limit=0)
    total_messages = history.total or 0
    log.info(f"total messages in source: {total_messages}")

    stats = {
        "cloned": 0,
        "skipped": 0,
        "skipped_over_limit": 0,
        "failed": 0,
        "processed": 0,
        "total": total_messages,
        "current_msg": None,
        "file_progress": None,
        "status": "running",
        "failed_ids": [],
        "last_error": None,
        "last_error_msg_id": None,
        "last_error_at": None,
        "last_error_attempt": None,
        "last_error_wait": None,
        "last_skip": None,
        "last_skip_at": None,
        "last_skip_msg_id": None,
        "last_skip_reason": None,
        "last_skip_filename": None,
        "last_skip_size": None,
        "last_skip_size_human": None,
        "last_skip_limit": None,
        "last_skip_limit_human": None,
        "upload_limit": upload_limit,
        "upload_limit_human": _human_size(upload_limit),
        "is_premium": is_premium,
    }

    last_processed_id = 0

    async def _process_message(message):
        nonlocal last_processed_id

        if stop_event and stop_event.is_set():
            stats["status"] = "stopped"
            log.info("clone stopped by user")
            return False

        stats["status"] = "running"
        msg_id = message.id
        last_processed_id = max(last_processed_id, msg_id)
        stats["current_msg"] = msg_id
        stats["processed"] += 1
        stats["file_progress"] = None

        if await _tracker_call(tracker, "is_cloned", source_id, msg_id):
            stats["skipped"] += 1
            if progress_callback:
                progress_callback(stats.copy())
            return True

        should_skip, size_bytes, pre_name = _should_skip_over_limit(message, upload_limit)
        if should_skip:
            stats["skipped_over_limit"] += 1
            stats["last_skip"] = "over_limit"
            stats["last_skip_at"] = datetime.now(timezone.utc).isoformat()
            stats["last_skip_msg_id"] = msg_id
            stats["last_skip_reason"] = "file too large"
            stats["last_skip_filename"] = pre_name
            stats["last_skip_size"] = size_bytes
            stats["last_skip_size_human"] = _human_size(size_bytes)
            stats["last_skip_limit"] = upload_limit
            stats["last_skip_limit_human"] = _human_size(upload_limit)
            try:
                await _tracker_call(
                    tracker,
                    "mark_skipped",
                    source_id,
                    msg_id,
                    reason="file too large",
                    file_size=size_bytes,
                    limit_bytes=upload_limit,
                    filename=pre_name,
                    media_type=_media_type(message),
                )
            except Exception as mark_exc:
                log.warning(f"failed to record skipped msg #{msg_id}: {mark_exc}")
            log.warning(
                "msg #%s skipped: %s exceeds limit %s",
                msg_id,
                _human_size(size_bytes),
                _human_size(upload_limit),
            )
            if progress_callback:
                progress_callback(stats.copy())
            return True

        success, error_reason = await _try_clone_with_retry(
            client, message, dest_entity, tracker, source_id,
            stats, progress_callback, max_retries, retry_delay, stop_event,
        )

        if not success:
            if error_reason == "stopped":
                stats["status"] = "stopped"
                return False
            if msg_id not in stats["failed_ids"]:
                stats["failed_ids"].append(msg_id)
            stats["failed"] = len(stats["failed_ids"])

        stats["file_progress"] = None
        if progress_callback:
            progress_callback(stats.copy())

        await asyncio.sleep(rate_limit_delay)
        return True

    async for message in client.iter_messages(source_entity, reverse=False):
        ok = await _process_message(message)
        if not ok:
            break

    if follow and stats["status"] != "stopped":
        stats["status"] = "watching"
        if progress_callback:
            progress_callback(stats.copy())

        while not (stop_event and stop_event.is_set()):
            new_count = 0
            async for message in client.iter_messages(source_entity, min_id=last_processed_id, reverse=False):
                new_count += 1
                ok = await _process_message(message)
                if not ok:
                    break

            if new_count > 0:
                stats["total"] += new_count
                stats["status"] = "watching"
                if progress_callback:
                    progress_callback(stats.copy())

            await asyncio.sleep(follow_poll_interval)

        stats["status"] = "stopped"

    if stats["status"] != "stopped":
        stats["status"] = "completed"

    if progress_callback:
        progress_callback(stats.copy())

    return stats


async def _try_clone_with_retry(
    client, message, dest_entity, tracker, source_id,
    stats, progress_callback, max_retries, retry_delay,
    stop_event: asyncio.Event | None = None,
) -> tuple[bool, str]:
    """attempt to clone a message with exponential backoff retries."""
    msg_id = message.id

    attempt = 0
    file_ref_attempts = 0
    retry_forever = max_retries <= 0
    while True:
        if stop_event and stop_event.is_set():
            return False, "stopped"
        attempt += 1
        try:
            await _clone_message(
                client, message, dest_entity, tracker,
                source_id, stats, progress_callback,
            )
            stats["cloned"] += 1
            if msg_id in stats["failed_ids"]:
                stats["failed_ids"].remove(msg_id)
            stats["failed"] = len(stats["failed_ids"])
            stats["last_error"] = None
            stats["last_error_msg_id"] = None
            stats["last_error_at"] = None
            stats["last_error_attempt"] = None
            stats["last_error_wait"] = None
            log.info(
                f"[{stats['processed']}/{stats['total']}] "
                f"msg #{msg_id} cloned"
            )
            return True, ""
        except Exception as e:
            if isinstance(e, FileReferenceExpiredError):
                file_ref_attempts += 1
                try:
                    refreshed = await client.get_messages(message.chat_id or message.peer_id, ids=msg_id)
                except Exception as refresh_exc:
                    log.warning(f"failed to refresh msg #{msg_id} after FileReferenceExpiredError: {refresh_exc}")
                else:
                    if refreshed:
                        message = refreshed
                if file_ref_attempts >= 3:
                    error_reason = str(e)
                    stats["status"] = "failed"
                    stats["last_error"] = error_reason
                    stats["last_error_msg_id"] = msg_id
                    stats["last_error_at"] = datetime.now(timezone.utc).isoformat()
                    stats["last_error_attempt"] = attempt
                    stats["last_error_wait"] = None
                    if msg_id not in stats["failed_ids"]:
                        stats["failed_ids"].append(msg_id)
                    stats["failed"] = len(stats["failed_ids"])
                    if progress_callback:
                        progress_callback(stats.copy())
                    try:
                        await _tracker_call(tracker, "mark_failed", source_id, msg_id, error_reason)
                    except Exception as mark_exc:
                        log.warning(f"failed to record failed msg #{msg_id}: {mark_exc}")
                    log.error(
                        f"msg #{msg_id} permanently failed after {file_ref_attempts} FileReferenceExpiredError "
                        f"refresh attempts: {error_reason}"
                    )
                    return False, error_reason
            error_reason = str(e)
            wait = retry_delay * (2 ** (attempt - 1))
            wait = min(wait, MAX_RETRY_DELAY)
            jitter = wait * RETRY_JITTER_PCT
            wait = max(1.0, wait + random.uniform(-jitter, jitter))
            stats["status"] = "retrying"
            stats["last_error"] = error_reason
            stats["last_error_msg_id"] = msg_id
            stats["last_error_at"] = datetime.now(timezone.utc).isoformat()
            stats["last_error_attempt"] = attempt
            stats["last_error_wait"] = wait
            if msg_id not in stats["failed_ids"]:
                stats["failed_ids"].append(msg_id)
            stats["failed"] = len(stats["failed_ids"])
            if progress_callback:
                progress_callback(stats.copy())
            try:
                await _tracker_call(tracker, "mark_failed", source_id, msg_id, error_reason)
            except Exception as mark_exc:
                log.warning(f"failed to record failed msg #{msg_id}: {mark_exc}")

            if not retry_forever and attempt >= max_retries:
                stats["failed"] += 1
                log.error(f"msg #{msg_id} permanently failed after {max_retries} attempts: {error_reason}")
                return False, error_reason

            log.warning(
                f"msg #{msg_id} attempt {attempt}{'' if retry_forever else f'/{max_retries}'} failed: {e} "
                f"— retrying in {wait:.0f}s"
            )
            await asyncio.sleep(wait)

    return False, "Max retries exceeded"


async def _clone_message(
    client: TelegramClient,
    message,
    dest_entity,
    tracker: CloneTracker,
    source_id: int,
    stats: dict,
    progress_callback: Callable[[dict], None] | None = None,
):
    """handle a single message — download if media, upload to dest, track it."""

    caption = message.text or ""
    media_type = _media_type(message)
    filename = None

    has_media = isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument))

    if has_media:
        filename = await _download_and_reupload(
            client, message, dest_entity, caption, stats, progress_callback
        )
    elif caption:
        await client.send_message(dest_entity, caption, formatting_entities=message.entities)

    await _tracker_call(
        tracker,
        "mark_cloned",
        source_id,
        message.id,
        filename=filename,
        media_type=media_type,
    )


async def _download_and_reupload(
    client: TelegramClient,
    message,
    dest_entity,
    caption: str,
    stats: dict,
    progress_callback: Callable[[dict], None] | None = None,
) -> str | None:
    """download media to disk, send to dest, nuke the local copy.
    uses FastTelethon parallel transfer for files above the threshold."""

    file_size = _file_size_from_message(message)
    use_fast = file_size > FAST_TRANSFER_THRESHOLD and message.document is not None

    # guess filename before download
    pre_name = "media"
    if message.document and hasattr(message.document, "attributes"):
        for attr in message.document.attributes:
            if hasattr(attr, "file_name") and attr.file_name:
                pre_name = attr.file_name
                break

    def _make_progress_cb(phase: str, fname: str):
        start_time = time.time()
        
        def cb(current, total):
            elapsed = time.time() - start_time
            speed = current / elapsed if elapsed > 0 else 0

            stats["file_progress"] = {
                "phase": phase,
                "filename": fname,
                "current": current,
                "total": total,
                "current_human": _human_size(current),
                "total_human": _human_size(total),
                "speed_human": _human_size(speed) + "/s",
            }
            if progress_callback:
                progress_callback(stats.copy())
        return cb

    if use_fast:
        return await _fast_transfer(
            client, message, dest_entity, caption, pre_name,
            stats, progress_callback, _make_progress_cb,
        )
    else:
        return await _standard_transfer(
            client, message, dest_entity, caption, pre_name,
            stats, _make_progress_cb,
        )


async def _fast_transfer(
    client, message, dest_entity, caption, pre_name,
    stats, progress_callback, make_cb,
):
    """parallel download + upload via FastTelethon for big files."""

    dl_path = os.path.join(DOWNLOAD_DIR, pre_name)
    # avoid collisions
    base, ext = os.path.splitext(dl_path)
    counter = 0
    while os.path.exists(dl_path):
        counter += 1
        dl_path = f"{base}_{counter}{ext}"

    filename = Path(dl_path).name

    try:
        # parallel download
        with open(dl_path, "wb") as f:
            await download_file(client, message.document, f, progress_callback=make_cb("downloading", pre_name))

        # parallel upload
        with open(dl_path, "rb") as f:
            uploaded = await upload_file(client, f, progress_callback=make_cb("uploading", filename))

        # build the proper media with attributes so it shows correctly
        attributes, mime_type = utils.get_attributes(dl_path)
        if message.document and message.document.attributes:
            attributes = list(message.document.attributes)
        mime_type = message.document.mime_type if message.document else mime_type

        media = InputMediaUploadedDocument(
            file=uploaded,
            mime_type=mime_type,
            attributes=attributes,
            force_file=False,
        )
        await client.send_file(
            dest_entity,
            file=media,
            caption=caption,
            formatting_entities=message.entities,
        )
        return filename
    finally:
        if os.path.exists(dl_path):
            os.remove(dl_path)
            log.debug(f"deleted local file: {filename}")


async def _standard_transfer(
    client, message, dest_entity, caption, pre_name,
    stats, make_cb,
):
    """regular telethon download + upload for smaller files and photos."""

    pre_existing = set(os.listdir(DOWNLOAD_DIR)) if os.path.isdir(DOWNLOAD_DIR) else set()
    try:
        file_path = await client.download_media(
            message,
            file=DOWNLOAD_DIR,
            progress_callback=make_cb("downloading", pre_name),
        )
    except Exception:
        _cleanup_new_downloads(pre_existing, DOWNLOAD_DIR)
        raise
    if not file_path:
        _cleanup_new_downloads(pre_existing, DOWNLOAD_DIR)
        if caption:
            await client.send_message(dest_entity, caption, formatting_entities=message.entities)
        return None

    file_path = str(file_path)
    filename = Path(file_path).name

    try:
        await client.send_file(
            dest_entity,
            file_path,
            caption=caption,
            formatting_entities=message.entities,
            force_document=message.document is not None and not any([
                message.video, message.audio, message.voice,
                message.video_note, message.sticker, message.gif,
            ]),
            progress_callback=make_cb("uploading", filename),
        )
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            log.debug(f"deleted local file: {filename}")

    return filename
