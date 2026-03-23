# ╔══════════════════════════════════════╗
# ║  config.py — AsBhai Drop Bot         ║
# ║  Configuration & Environment Variables║
# ╚══════════════════════════════════════╝
# config.py — AsBhai Drop Bot Configuration
import os, logging
import pytz
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════
#  CONFIG — All from .env
# ═══════════════════════════════════════
API_ID            = int(os.getenv("API_ID", "0"))
API_HASH          = os.getenv("API_HASH", "")
BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
STRING_SESSION    = os.getenv("STRING_SESSION", "")
MONGO_URI         = os.getenv("MONGO_URI", "")
OWNER_ID          = int(os.getenv("OWNER_ID", "7315805581"))
FILE_CHANNEL      = int(os.getenv("FILE_CHANNEL", "-1002463804038"))
LOG_CHANNEL       = int(os.getenv("LOG_CHANNEL", "-1002463804038"))
MAIN_CHANNEL      = os.getenv("MAIN_CHANNEL", "@asbhai_bsr")
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "@asbhai_bsr")
FORCE_SUB_ID      = int(os.getenv("FORCE_SUB_ID", "-1002352329534"))
SHORTLINK_API     = os.getenv("SHORTLINK_API", "")
SHORTLINK_URL     = os.getenv("SHORTLINK_URL", "modijiurl.com")
KOYEB_URL         = os.getenv("KOYEB_URL", "").rstrip("/")
ADMINS            = [OWNER_ID]
IST               = pytz.timezone("Asia/Kolkata")
UPI_ID            = os.getenv("UPI_ID", "arsadsaifi8272@ibl")
PORT              = int(os.getenv("PORT", "8080"))

# In-memory caches
_shortlink_cache  = {}
_search_locks     = {}
_search_cooldown  = {}

def now():
    from datetime import datetime
    import pytz
    return datetime.now(pytz.utc)

def now_ist():
    return datetime.now(IST)

def make_aware(dt):
    import pytz
    if dt is None: return None
    if dt.tzinfo is None: return pytz.utc.localize(dt)
    return dt

DEFAULT_SETTINGS = {
    "auto_delete": True,
    "auto_delete_time": 300,
    "force_sub": True,
    "shortlink_enabled": True,
    "daily_limit": 10,
    "premium_results": 10,
    "free_results": 5,
    "welcome_msg": "👋 Welcome {name}! Koi bhi file ka naam type karo 🗂",
    "maintenance": False,
    "request_mode": False,
    "fsub_channels": [],
    "fsub_groups": [],
}

GROUP_DEFAULTS = {
    "free_results": 5,
    "premium_results": 10,
    "force_sub": True,
    "shortlink_enabled": True,
    "auto_delete": True,
    "auto_delete_time": 300,
    "request_mode": False,
}
