"""
entrypoint — handles telegram login, channel selection, and kicks off the cloner.
"""

import asyncio
import logging
import signal
import sys

from telethon import TelegramClient
from telethon.tl.types import Channel
from tqdm import tqdm

from config import API_ID, API_HASH, PHONE, SOURCE_CHANNEL, DEST_CHANNEL, SESSION_FILE
from tracker import create_tracker
from cloner import clone_channel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("main")


async def list_channels(client: TelegramClient) -> list:
    """grab all channels/groups the user is in."""
    channels = []
    async for dialog in client.iter_dialogs():
        if isinstance(dialog.entity, Channel):
            channels.append(dialog)
    return channels


async def pick_channel(client: TelegramClient, prompt: str, default: str = "") -> str:
    """interactive channel picker — shows a numbered list, user picks one."""

    if default:
        use_default = input(f"\n{prompt} [default: {default}] — press enter to use default, or 'l' to list: ").strip()
        if not use_default:
            return default
        if use_default.lower() != "l":
            return use_default

    channels = await list_channels(client)
    if not channels:
        log.error("no channels found. are you in any channels?")
        sys.exit(1)

    print(f"\n{'=' * 50}")
    print(f" {prompt}")
    print(f"{'=' * 50}")
    for i, dialog in enumerate(channels, 1):
        entity = dialog.entity
        member_info = f" ({entity.participants_count} members)" if entity.participants_count else ""
        print(f"  [{i:3d}] {dialog.name}{member_info}")
    print(f"{'=' * 50}")

    while True:
        choice = input("\npick a number (or type channel username): ").strip()
        if not choice:
            continue
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(channels):
                selected = channels[idx]
                log.info(f"selected: {selected.name}")
                return selected.entity.id
        else:
            return choice

    return default


def _make_cli_progress():
    state = {
        "bar": None,
        "phase": None,
        "filename": None,
        "total": None,
        "last": 0,
    }

    def _close_bar():
        if state["bar"] is not None:
            state["bar"].close()
            state["bar"] = None

    def cb(stats: dict):
        fp = stats.get("file_progress")
        if not fp:
            _close_bar()
            return

        phase = fp.get("phase")
        filename = fp.get("filename") or "media"
        total = fp.get("total") or 0
        current = fp.get("current") or 0

        if (
            state["bar"] is None
            or phase != state["phase"]
            or filename != state["filename"]
            or total != state["total"]
        ):
            _close_bar()
            label = "DL" if phase == "downloading" else "UL"
            state["bar"] = tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"{label} {filename}",
                leave=False,
                dynamic_ncols=True,
            )
            state["phase"] = phase
            state["filename"] = filename
            state["total"] = total
            state["last"] = 0

        delta = current - state["last"]
        if delta > 0 and state["bar"] is not None:
            state["bar"].update(delta)
            state["last"] = current

        if total and current >= total:
            _close_bar()

    return cb


async def run():
    if not API_ID or not API_HASH:
        log.error("API_ID and API_HASH are required. get them from https://my.telegram.org")
        log.error("copy .env.example to .env and fill in your credentials")
        sys.exit(1)

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    await client.start(phone=PHONE if PHONE else lambda: input("phone number: "))

    me = await client.get_me()
    log.info(f"logged in as {me.first_name} (@{me.username})")

    source = await pick_channel(client, "source channel (clone FROM)", SOURCE_CHANNEL)
    dest = await pick_channel(client, "destination channel (clone TO)", DEST_CHANNEL)

    if str(source) == str(dest):
        log.error("source and dest are the same channel, that's a loop my guy")
        sys.exit(1)

    tracker = create_tracker()
    existing = tracker.get_stats()
    if existing["total_cloned"] > 0:
        log.info(f"resuming — {existing['total_cloned']} messages already cloned (last run: {existing['last_run']})")

    print(f"\nstarting clone...")
    print(f"{'—' * 40}")

    stop_event = asyncio.Event()
    sigint_count = 0

    def _handle_sigint():
        nonlocal sigint_count
        sigint_count += 1
        if sigint_count == 1:
            print(f"\n[!] SIGINT received, stopping current clone gracefully... (press again to force quit)")
            stop_event.set()
            return
        print(f"\n[!] SIGINT received again, exiting now.")
        sys.exit(1)

    try:
        client.loop.add_signal_handler(signal.SIGINT, _handle_sigint)
    except NotImplementedError:
        # Windows event loops don't implement add_signal_handler
        def _sigint_handler(signum, frame):
            client.loop.call_soon_threadsafe(_handle_sigint)

        signal.signal(signal.SIGINT, _sigint_handler)

    stats = {"cloned": 0, "skipped": 0, "failed": 0, "total": 0}
    try:
        stats = await clone_channel(
            client,
            source,
            dest,
            tracker,
            stop_event=stop_event,
            progress_callback=_make_cli_progress(),
        )
    finally:
        print(f"\n{'—' * 40}")
        print(f"done.")
        print(f"  cloned:  {stats['cloned']}")
        print(f"  skipped: {stats['skipped']} (already cloned)")
        print(f"  failed:  {stats['failed']}")
        print(f"  total:   {stats['total']}")

        await client.disconnect()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
