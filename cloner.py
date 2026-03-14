"""
the actual cloning engine — iterates source channel messages,
downloads media, re-uploads to dest, deletes local files, tracks everything.
uses FastTelethon for parallel downloads/uploads on big files.
"""

import asyncio
import os
import logging
from pathlib import Path
from typing import Callable

from telethon import TelegramClient, utils
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


def _human_size(nbytes: int) -> str:
    """turn byte count into something readable."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


async def clone_channel(
    client: TelegramClient,
    source,
    dest,
    tracker: CloneTracker,
    rate_limit_delay: float = 2.0,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: asyncio.Event | None = None,
    max_retries: int = 3,
    retry_delay: float = 5.0,
):
    """clone all messages from source channel to dest channel."""

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    source_entity = await client.get_entity(source)
    dest_entity = await client.get_entity(dest)
    source_id = source_entity.id

    log.info(f"source: {getattr(source_entity, 'title', source)} (id: {source_id})")
    log.info(f"dest:   {getattr(dest_entity, 'title', dest)}")

    # grab total message count before iterating
    history = await client.get_messages(source_entity, limit=0)
    total_messages = history.total or 0
    log.info(f"total messages in source: {total_messages}")

    stats = {
        "cloned": 0,
        "skipped": 0,
        "failed": 0,
        "processed": 0,
        "total": total_messages,
        "current_msg": None,
        "file_progress": None,
        "status": "running",
        "failed_ids": [],
    }

    failed_queue = []

    async for message in client.iter_messages(source_entity, reverse=True):
        if stop_event and stop_event.is_set():
            stats["status"] = "stopped"
            log.info("clone stopped by user")
            break

        msg_id = message.id
        stats["current_msg"] = msg_id
        stats["processed"] += 1
        stats["file_progress"] = None

        if tracker.is_cloned(source_id, msg_id):
            stats["skipped"] += 1
            if progress_callback:
                progress_callback(stats.copy())
            continue

        success = await _try_clone_with_retry(
            client, message, dest_entity, tracker, source_id,
            stats, progress_callback, max_retries, retry_delay,
        )

        if not success:
            failed_queue.append(message)
            stats["failed_ids"].append(msg_id)

        stats["file_progress"] = None
        if progress_callback:
            progress_callback(stats.copy())

        await asyncio.sleep(rate_limit_delay)

    # second pass — retry everything that failed during the main loop
    if failed_queue and not (stop_event and stop_event.is_set()):
        log.info(f"retrying {len(failed_queue)} failed messages (second pass)")
        stats["status"] = "retrying"
        if progress_callback:
            progress_callback(stats.copy())

        for message in failed_queue:
            if stop_event and stop_event.is_set():
                break

            msg_id = message.id
            stats["current_msg"] = msg_id

            if tracker.is_cloned(source_id, msg_id):
                continue

            success = await _try_clone_with_retry(
                client, message, dest_entity, tracker, source_id,
                stats, progress_callback, max_retries, retry_delay,
            )

            if success and msg_id in stats["failed_ids"]:
                stats["failed"] -= 1
                stats["failed_ids"].remove(msg_id)

            stats["file_progress"] = None
            if progress_callback:
                progress_callback(stats.copy())

            await asyncio.sleep(rate_limit_delay)

    if stats["status"] != "stopped":
        stats["status"] = "completed"

    if progress_callback:
        progress_callback(stats.copy())

    return stats


async def _try_clone_with_retry(
    client, message, dest_entity, tracker, source_id,
    stats, progress_callback, max_retries, retry_delay,
) -> bool:
    """attempt to clone a message with exponential backoff retries."""
    msg_id = message.id

    for attempt in range(1, max_retries + 1):
        try:
            await _clone_message(
                client, message, dest_entity, tracker,
                source_id, stats, progress_callback,
            )
            stats["cloned"] += 1
            log.info(
                f"[{stats['processed']}/{stats['total']}] "
                f"msg #{msg_id} cloned"
            )
            return True
        except Exception as e:
            if attempt < max_retries:
                wait = retry_delay * (2 ** (attempt - 1))
                log.warning(
                    f"msg #{msg_id} attempt {attempt}/{max_retries} failed: {e} "
                    f"— retrying in {wait:.0f}s"
                )
                await asyncio.sleep(wait)
            else:
                stats["failed"] += 1
                log.error(f"msg #{msg_id} permanently failed after {max_retries} attempts: {e}")
                return False

    return False


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

    tracker.mark_cloned(source_id, message.id, filename=filename, media_type=media_type)


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
        def cb(current, total):
            stats["file_progress"] = {
                "phase": phase,
                "filename": fname,
                "current": current,
                "total": total,
                "current_human": _human_size(current),
                "total_human": _human_size(total),
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

    # parallel download
    with open(dl_path, "wb") as f:
        await download_file(client, message.document, f, progress_callback=make_cb("downloading", pre_name))

    filename = Path(dl_path).name

    try:
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
    finally:
        if os.path.exists(dl_path):
            os.remove(dl_path)
            log.debug(f"deleted local file: {filename}")

    return filename


async def _standard_transfer(
    client, message, dest_entity, caption, pre_name,
    stats, make_cb,
):
    """regular telethon download + upload for smaller files and photos."""

    file_path = await client.download_media(
        message,
        file=DOWNLOAD_DIR,
        progress_callback=make_cb("downloading", pre_name),
    )
    if not file_path:
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
