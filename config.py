import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE = os.getenv("PHONE", "")

SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL", "")
DEST_CHANNEL = os.getenv("DEST_CHANNEL", "")

SESSION_FILE = str(BASE_DIR / "rogue_helix")
TRACKER_FILE = str(BASE_DIR / "clone_tracker.json")
DOWNLOAD_DIR = str(BASE_DIR / "downloads")

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "5000"))

NOTIFY_ON_ERROR = os.getenv("NOTIFY_ON_ERROR", "true").lower() in ("true", "1", "yes")
NOTIFY_ON_COMPLETE = os.getenv("NOTIFY_ON_COMPLETE", "true").lower() in ("true", "1", "yes")
