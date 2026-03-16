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

# tracker backend: "json" (default), "sqlite", or "supabase"
TRACKER_BACKEND = os.getenv("TRACKER_BACKEND", "json")
SQLITE_DB = os.getenv("SQLITE_DB", str(BASE_DIR / "clone_tracker.db"))

# supabase (only needed if TRACKER_BACKEND=supabase)
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "5000"))

NOTIFY_ON_ERROR = os.getenv("NOTIFY_ON_ERROR", "true").lower() in ("true", "1", "yes")
NOTIFY_ON_COMPLETE = os.getenv("NOTIFY_ON_COMPLETE", "true").lower() in ("true", "1", "yes")
