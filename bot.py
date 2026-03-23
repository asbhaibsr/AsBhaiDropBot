import os, re, time, random, string, asyncio, logging
from datetime import datetime, timedelta
from threading import Thread

import pytz, aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    WebAppInfo
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatWriteForbidden, PeerIdInvalid, UserNotParticipant
)

# Global FloodWait middleware
async def safe_send(func, *args, **kwargs):
    """FloodWait ke saath safely message bhejo"""
    for attempt in range(3):
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            if attempt < 2:
                logger.warning(f"FloodWait {e.value}s, waiting...")
                await asyncio.sleep(min(e.value, 30))
            else:
                logger.error(f"FloodWait max retries reached")
                return None
        except Exception as e:
            logger.error(f"safe_send error: {e}")
            return None
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from flask import Flask, jsonify, request as flask_request, send_from_directory

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════
#  CONFIG
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
KOYEB_URL         = os.getenv("KOYEB_URL", "").rstrip("/")  # Mini app URL — trailing slash auto remove
ADMINS            = [OWNER_ID]
IST               = pytz.timezone("Asia/Kolkata")
UPI_ID            = os.getenv("UPI_ID", "arsadsaifi8272@ibl")
PORT              = int(os.getenv("PORT", "8080"))

# ── Shortlink cache: {user_id: {group_id: (link, expiry)}}
_shortlink_cache = {}

def now():
    return datetime.now(pytz.utc)

def now_ist():
    return datetime.now(IST)

def make_aware(dt):
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
    "request_mode": False,         # Agar True: sirf request, search band
    "fsub_channels": [],           # [{id: int, username: str, title: str}]
    "fsub_groups": [],             # [{id: int, username: str, title: str}]
}

# ═══════════════════════════════════════
#  FLASK APP
# ═══════════════════════════════════════
flask_app = Flask(__name__, static_folder="static")

@flask_app.route("/")
def home():
    return send_from_directory(".", "miniapp.html")

@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "time": now_ist().strftime("%d %b %H:%M IST")}), 200

@flask_app.route("/api/plans")
def plans():
    return jsonify({
        "plans": [
            {"id": "10days",  "name": "10 Din",  "days": 10,  "price": 50,  "desc": "Best for beginners"},
            {"id": "30days",  "name": "30 Din",  "days": 30,  "price": 150, "desc": "Most Popular"},
            {"id": "60days",  "name": "60 Din",  "days": 60,  "price": 200, "desc": "Great Value"},
            {"id": "150days", "name": "150 Din", "days": 150, "price": 500, "desc": "Super Saver"},
            {"id": "365days", "name": "1 Saal",  "days": 365, "price": 800, "desc": "Best Deal"},
        ],
        "group_plans": [
            {"id": "group_1m",  "name": "1 Mahina",  "days": 30,  "price": 300, "desc": "Group Premium — Apni shortlink lagao"},
            {"id": "group_2m",  "name": "2 Mahine",  "days": 60,  "price": 550, "desc": "Group Premium — Best Value"},
        ],
        "upi": UPI_ID,
        "qr_url": "https://envs.sh/GdE.jpg",
        "contact": "@asbhaibsr"
    })

@flask_app.route("/api/submit_payment", methods=["POST"])
def submit_payment():
    import asyncio as _a, base64, tempfile, os as _os
    data = flask_request.get_json(silent=True) or {}
    user_id   = data.get("user_id")
    name      = data.get("name", "Unknown")
    plan_id   = data.get("plan_id")
    amount    = data.get("amount")
    txn_id    = data.get("txn_id", "").strip()
    screenshot = data.get("screenshot", "")

    group_id   = data.get("group_id")       # Group premium ke liye
    group_link = data.get("group_link", "")  # Group link

    if not all([user_id, plan_id, txn_id]):
        return jsonify({"ok": False, "error": "Saari details bharo!"}), 400

    plan_days_map = {"10days":10,"30days":30,"60days":60,"150days":150,"365days":365,"group_1m":30,"group_2m":60}
    days = plan_days_map.get(plan_id, 30)

    async def _process():
        existing = await payments_col.find_one({"txn_id": txn_id})
        if existing:
            return False, "Ye Transaction ID pehle se submit ho chuka hai!"
        is_group_plan = str(plan_id).startswith("group_")
        pay_doc = {
            "user_id": user_id, "name": name, "plan_id": plan_id,
            "days": days, "amount": amount, "txn_id": txn_id,
            "screenshot": screenshot[:200] if screenshot else "",
            "status": "pending", "submitted_at": now(),
            "is_group": is_group_plan,
            "group_id": str(group_id) if group_id else None,
            "group_link": group_link or ""
        }
        result = await payments_col.insert_one(pay_doc)
        pay_id = str(result.inserted_id)
        group_info = ""
        if is_group_plan and group_id:
            group_info = f"\n🏘 Group ID: `{group_id}`\n🔗 Link: {group_link}"
        pay_type = "🏘 GROUP PREMIUM" if is_group_plan else "💎 USER PREMIUM"
        msg_text = (
            f"💰 **Naya Payment — {pay_type}**\n\n"
            f"👤 {name} (`{user_id}`)\n"
            f"📦 Plan: **{plan_id}** ({days} din)\n"
            f"💵 Amount: ₹{amount}\n"
            f"🔖 TXN ID: `{txn_id}`"
            f"{group_info}\n"
            f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
        )
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve", callback_data=f"pay_approve_{pay_id}_{user_id}_{days}"),
                InlineKeyboardButton("❌ Reject",  callback_data=f"pay_reject_{pay_id}_{user_id}")
            ]
        ])
        if screenshot and screenshot.startswith("data:image"):
            try:
                img_data = base64.b64decode(screenshot.split(",")[1])
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
                    tf.write(img_data); tf_path = tf.name
                await bot.send_photo(LOG_CHANNEL, tf_path, caption=msg_text, reply_markup=kb)
                await bot.send_photo(OWNER_ID, tf_path, caption=msg_text, reply_markup=kb)
                _os.unlink(tf_path)
            except:
                await bot.send_message(LOG_CHANNEL, msg_text, reply_markup=kb)
                await bot.send_message(OWNER_ID, msg_text, reply_markup=kb)
        else:
            await bot.send_message(LOG_CHANNEL, msg_text, reply_markup=kb)
            try: await bot.send_message(OWNER_ID, msg_text, reply_markup=kb)
            except: pass
        return True, "✅ Request submit ho gayi! 1-2 ghante mein activate hoga."

    try:
        ok, msg = run_async(_process())
        return jsonify({"ok": ok, "message": msg}) if ok else (jsonify({"ok": False, "error": msg}), 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@flask_app.route("/api/claim_trial", methods=["POST"])
def claim_trial():
    data = flask_request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"ok": False, "error": "User ID missing"}), 400

    async def _claim():
        doc = await free_trial_col.find_one({"user_id": user_id})
        if doc:
            return False, "Free trial pehle le chuke ho! Premium lo."
        await free_trial_col.insert_one({"user_id": user_id, "claimed_at": now()})
        expiry = now() + timedelta(minutes=5)
        await premium_col.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "expiry": expiry, "trial": True, "added": now()}},
            upsert=True
        )
        try:
            await bot.send_message(
                user_id,
                "🆓 **Free Trial Shuru!**\n\n**5 minute** ke liye Premium active!\n"
                "Group mein movie search karo abhi!\n\n"
                "⏰ 5 min baad band hoga.\n💎 Continue ke liye Premium lo!"
            )
        except: pass
        await send_log(f"🆓 Free Trial Claimed\n👤 `{user_id}`")
        return True, "Trial shuru! 5 min ke liye premium active."

    try:
        ok, msg = run_async(_claim())
        return jsonify({"ok": ok, "message": msg})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@flask_app.route("/api/user_status/<int:user_id>")
def user_status_api(user_id):
    loop = _a.new_event_loop()
    try:
        async def _check():
            prem_doc = await premium_col.find_one({"user_id": user_id})
            trial_doc = await free_trial_col.find_one({"user_id": user_id})
            user_doc = await users_col.find_one({"user_id": user_id})
            pending = await payments_col.count_documents({"user_id": user_id, "status": "pending"})
            is_prem, is_trial, expiry_str = False, False, None
            if prem_doc and prem_doc.get("expiry"):
                exp = make_aware(prem_doc["expiry"])
                if now() < exp:
                    is_prem = True
                    is_trial = prem_doc.get("trial", False)
                    expiry_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M")
            return {
                "is_premium": is_prem, "is_trial": is_trial,
                "trial_used": bool(trial_doc), "expiry": expiry_str,
                "refer_count": user_doc.get("refer_count", 0) if user_doc else 0,
                "pending_payments": pending
            }
        return jsonify(run_async(_check()))
    except Exception as e:
        return jsonify({"is_premium": False, "error": str(e)})

@flask_app.route("/api/refer/<int:user_id>")
def refer_info(user_id):
    try:
        doc = run_async(users_col.find_one({"user_id": user_id}))
        refer_count = doc.get("refer_count", 0) if doc else 0
        refers_needed = max(0, 10 - (refer_count % 10))
        return jsonify({"refer_count": refer_count, "refers_needed": refers_needed})
    except Exception as e:
        return jsonify({"refer_count": 0, "refers_needed": 10})

@flask_app.route("/api/help", methods=["POST"])
def help_api():
    """Mini app se help message owner ko bhejo"""
    data = flask_request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    name = data.get("name", "Unknown")
    message_text = data.get("message", "")
    if not message_text:
        return jsonify({"ok": False, "error": "No message"}), 400

    async def _send():
        await help_msgs_col.insert_one({
            "user_id": user_id, "name": name,
            "message": message_text, "time": now()
        })
        await send_log(
            f"📩 **Mini App Help**\n\n"
            f"👤 {name} (`{user_id}`)\n\n"
            f"💬 {message_text}"
        )

    try:
        run_async(_send())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@flask_app.route("/stream")
def stream_page():
    """Stream URL redirect — Mini app se aata hai"""
    return send_from_directory(".", "miniapp.html")

@flask_app.route("/stream_file/<int:msg_id>")
def stream_file(msg_id):
    """Telegram file URL get karke redirect karo — proper implementation"""
    from flask import redirect as fl_redirect
    try:
        async def _get_file_url():
            try:
                file_msg = await bot.get_messages(FILE_CHANNEL, msg_id)
                if not file_msg or file_msg.empty:
                    return None
                fid = None
                if file_msg.video: fid = file_msg.video.file_id
                elif file_msg.document: fid = file_msg.document.file_id
                elif file_msg.audio: fid = file_msg.audio.file_id
                if not fid:
                    return None
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={fid}",
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as r:
                        d = await r.json()
                        if d.get("ok"):
                            fp = d["result"]["file_path"]
                            return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fp}"
                return None
            except Exception as e:
                logger.error(f"stream_file error: {e}")
                return None

        file_url = run_async(_get_file_url())
        if file_url:
            return fl_redirect(file_url)
        return jsonify({"error": "File not found or size > 20MB"}), 404
    except Exception as e:
        logger.error(f"stream_file route error: {e}")
        return jsonify({"error": str(e)}), 500

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False)

def run_async(coro):
    """Flask routes se async code run karo — bot ke event loop mein"""
    try:
        loop = bot.loop
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=30)
        else:
            # Fallback
            return asyncio.run(coro)
    except Exception as e:
        logger.error(f"run_async error: {e}")
        raise

# ═══════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════
mongo_client  = AsyncIOMotorClient(MONGO_URI)
db            = mongo_client["asbhaidropbot"]
users_col     = db["users"]
groups_col    = db["groups"]
premium_col   = db["premium"]
settings_col  = db["settings"]
tokens_col    = db["tokens"]
requests_col  = db["requests"]
banned_col    = db["banned"]
refers_col    = db["refers"]        # {referrer_id, referred_id, time}
free_trial_col = db["free_trials"]  # {user_id, uses, last_time}
help_msgs_col  = db["help_msgs"]    # Mini app help messages
payments_col   = db["payments"]     # {user_id, plan_id, txn_id, status, ...}
shortlinks_col = db["shortlinks"]   # {api_key, url, hours, label, active, order}
verify_log_col = db["verify_logs"]  # {user_id, shortlink_id, verified_at, count}
group_prem_col = db["group_premium"] # {chat_id, owner_id, txn_id, status, expiry, days}
group_sl_col   = db["group_shortlinks"] # {chat_id, api_key, url, hours, label, order}

# Group-level settings: {chat_id, free_results, premium_results, force_sub, shortlink_enabled, ...}
group_settings_col = db["group_settings"]

# ═══════════════════════════════════════
#  CLIENTS
# ═══════════════════════════════════════
bot = Client(
    "asbhai_drop_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

userbot = Client(
    "asbhai_userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=STRING_SESSION,
    in_memory=True
) if STRING_SESSION else None

scheduler = AsyncIOScheduler(timezone=IST)

# ═══════════════════════════════════════
#  SETTINGS — Global
# ═══════════════════════════════════════
async def get_settings():
    s = await settings_col.find_one({"_id": "global"})
    if not s:
        await settings_col.insert_one({"_id": "global", **DEFAULT_SETTINGS})
        return DEFAULT_SETTINGS.copy()
    # Ensure keys exist
    for k, v in DEFAULT_SETTINGS.items():
        if k not in s:
            s[k] = v
    return s

async def update_setting(key, value):
    await settings_col.update_one(
        {"_id": "global"}, {"$set": {key: value}}, upsert=True
    )

# ═══════════════════════════════════════
#  GROUP SETTINGS
# ═══════════════════════════════════════
GROUP_DEFAULTS = {
    "free_results": 5,
    "premium_results": 10,
    "force_sub": True,
    "shortlink_enabled": True,
    "auto_delete": True,
    "auto_delete_time": 300,
    "request_mode": False,
}

async def get_group_settings(chat_id):
    doc = await group_settings_col.find_one({"chat_id": chat_id})
    if not doc:
        return GROUP_DEFAULTS.copy()
    result = GROUP_DEFAULTS.copy()
    result.update({k: v for k, v in doc.items() if k != "_id"})
    return result

async def update_group_setting(chat_id, key, value):
    await group_settings_col.update_one(
        {"chat_id": chat_id},
        {"$set": {key: value, "chat_id": chat_id}},
        upsert=True
    )

# ═══════════════════════════════════════
#  USER / GROUP
# ═══════════════════════════════════════
async def save_user(user, referred_by=None):
    existing = await users_col.find_one({"user_id": user.id})
    is_new = existing is None
    update = {
        "user_id": user.id,
        "name": user.first_name,
        "username": user.username,
        "last_seen": now()
    }
    if is_new and referred_by:
        update["referred_by"] = referred_by
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": update, "$setOnInsert": {"joined": now(), "refer_count": 0}},
        upsert=True
    )
    # Refer credit karo
    if is_new and referred_by and referred_by != user.id:
        already = await refers_col.find_one({"referrer_id": referred_by, "referred_id": user.id})
        if not already:
            await refers_col.insert_one({
                "referrer_id": referred_by,
                "referred_id": user.id,
                "time": now()
            })
            result = await users_col.find_one_and_update(
                {"user_id": referred_by},
                {"$inc": {"refer_count": 1}},
                return_document=True
            )
            new_count = result.get("refer_count", 0) if result else 0
            # Har 10 refer pe 15 din premium
            if new_count > 0 and new_count % 10 == 0:
                await add_premium(referred_by, 15)
                try:
                    await bot.send_message(
                        referred_by,
                        f"🎉 **{new_count} Refer Complete!**\n\n"
                        f"Reward: **15 din ka FREE Premium!** 💎\n\n"
                        f"Keep referring! Har 10 refer = 15 din premium!"
                    )
                except: pass
            else:
                # Notification
                needed = 10 - (new_count % 10)
                try:
                    await bot.send_message(
                        referred_by,
                        f"✅ **Naya Refer Aaya!**\n\n"
                        f"Total Refers: **{new_count}**\n"
                        f"Premium ke liye: **{needed}** aur chahiye!\n\n"
                        f"Refer link: /referlink"
                    )
                except: pass
    return is_new

async def save_group(chat):
    existing = await groups_col.find_one({"chat_id": chat.id})
    is_new = existing is None
    await groups_col.update_one(
        {"chat_id": chat.id},
        {"$set": {"chat_id": chat.id, "title": chat.title, "last_active": now()}},
        upsert=True
    )
    return is_new

async def is_banned(user_id):
    return bool(await banned_col.find_one({"user_id": user_id}))

async def ban_user(user_id, reason="No reason"):
    await banned_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "reason": reason, "banned_at": now()}},
        upsert=True
    )

async def unban_user(user_id):
    await banned_col.delete_one({"user_id": user_id})

# ═══════════════════════════════════════
#  FREE TRIAL
# ═══════════════════════════════════════
async def get_free_trial_status(user_id):
    """Returns (uses_left, can_use). 2 baar 5 min ke liye use kar sakta hai."""
    doc = await free_trial_col.find_one({"user_id": user_id})
    if not doc:
        return 2, True
    uses = doc.get("uses", 0)
    return max(0, 2 - uses), uses < 2

async def use_free_trial(user_id):
    await free_trial_col.update_one(
        {"user_id": user_id},
        {"$inc": {"uses": 1}, "$set": {"last_time": now(), "user_id": user_id}},
        upsert=True
    )

# ═══════════════════════════════════════
#  PREMIUM
# ═══════════════════════════════════════
async def is_premium(user_id):
    if user_id in ADMINS: return True
    doc = await premium_col.find_one({"user_id": user_id})
    if not doc: return False
    expiry = make_aware(doc.get("expiry"))
    if expiry and now() > expiry:
        await premium_col.delete_one({"user_id": user_id})
        return False
    return True

async def add_premium(user_id, days=30):
    # Existing premium extend karo
    existing = await premium_col.find_one({"user_id": user_id})
    if existing and existing.get("expiry"):
        old_expiry = make_aware(existing["expiry"])
        base = max(old_expiry, now())
    else:
        base = now()
    expiry = base + timedelta(days=days)
    await premium_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "added": now()}},
        upsert=True
    )

async def remove_premium(user_id):
    await premium_col.delete_one({"user_id": user_id})

async def get_premium_expiry(user_id):
    doc = await premium_col.find_one({"user_id": user_id})
    if doc and doc.get("expiry"):
        return make_aware(doc["expiry"])
    return None

# ═══════════════════════════════════════
#  DAILY LIMIT
# ═══════════════════════════════════════
async def get_daily_count(user_id):
    today = now_ist().strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id, "date": today})
    return doc.get("count", 0) if doc else 0

async def increment_daily(user_id):
    today = now_ist().strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id, "date": today},
        {"$inc": {"count": 1}}, upsert=True
    )

# ═══════════════════════════════════════
#  VERIFICATION (Shortlink)
# ═══════════════════════════════════════
async def is_verified_today(user_id):
    """get_user_verify_state se consistent check"""
    if await is_premium(user_id): return True
    all_done, _, _ = await get_user_verify_state(user_id)
    return all_done

async def mark_verified(user_id):
    today = now_ist().strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"verified_date": today}},
        upsert=True
    )

# ═══════════════════════════════════════
#  TOKEN
# ═══════════════════════════════════════
async def make_token(user_id, token_type="verify"):
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    await tokens_col.insert_one({
        "token": token,
        "user_id": user_id,
        "type": token_type,
        "expiry": now() + timedelta(minutes=5),
        "created": now()
    })
    return token

async def check_token(token, expected_uid=None):
    doc = await tokens_col.find_one({"token": token})
    if not doc: return False, None
    if now() > make_aware(doc["expiry"]): return False, None
    stored_uid = doc.get("user_id")
    if expected_uid and stored_uid != expected_uid: return False, None
    return True, stored_uid

# ═══════════════════════════════════════
#  MULTI-CHANNEL FORCE SUB
# ═══════════════════════════════════════
async def get_fsub_list():
    """Global settings se force sub channels+groups list lo"""
    s = await get_settings()
    channels = s.get("fsub_channels", [])
    groups = s.get("fsub_groups", [])
    return channels + groups

async def check_member_multi(user_id, prem=False):
    """
    Premium users ko force sub nahi.
    Returns: (joined_all: bool, not_joined: list of channel dicts)
    """
    if user_id in ADMINS: return True, []
    if prem: return True, []   # Premium = no force sub
    
    s = await get_settings()
    if not s.get("force_sub"): return True, []
    
    fsub_list = await get_fsub_list()
    if not fsub_list:
        # Fallback to old single channel
        try:
            m = await bot.get_chat_member(FORCE_SUB_ID, user_id)
            if m.status in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                return False, [{"id": FORCE_SUB_ID, "username": FORCE_SUB_CHANNEL, "title": "Channel"}]
            return True, []
        except UserNotParticipant:
            return False, [{"id": FORCE_SUB_ID, "username": FORCE_SUB_CHANNEL, "title": "Channel"}]
        except:
            return True, []
    
    not_joined = []
    for ch in fsub_list:
        ch_id = ch.get("id")
        if not ch_id: continue
        try:
            m = await bot.get_chat_member(ch_id, user_id)
            if m.status in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]:
                not_joined.append(ch)
        except UserNotParticipant:
            not_joined.append(ch)
        except Exception as e:
            logger.warning(f"fsub check error {ch_id}: {e}")
    
    return len(not_joined) == 0, not_joined

async def build_fsub_keyboard(not_joined, uid):
    """Buttons banao — har channel ke liye join button"""
    buttons = []
    for ch in not_joined:
        ch_id = ch.get("id")
        uname = ch.get("username", "")
        title = ch.get("title", "Channel")
        if uname:
            url = f"https://t.me/{uname.replace('@','')}"
        else:
            try:
                url = await bot.export_chat_invite_link(ch_id)
            except:
                url = f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"
        buttons.append([InlineKeyboardButton(f"📢 {title} Join Karo", url=url)])
    buttons.append([InlineKeyboardButton("✅ Join Kar Liya — Verify", callback_data=f"checkjoin_{uid}")])
    return InlineKeyboardMarkup(buttons)

async def force_sub_check(client, message, prem=False):
    uid = message.from_user.id
    if uid in ADMINS: return True
    if prem: return True  # Premium bypass
    
    s = await get_settings()
    if not s.get("force_sub"): return True
    
    joined, not_joined = await check_member_multi(uid, prem)
    if joined: return True
    
    kb = await build_fsub_keyboard(not_joined, uid)
    names = ", ".join(ch.get("title", "Channel") for ch in not_joined)
    msg = await message.reply(
        f"⚠️ **Pehle Join Karo!**\n\n"
        f"📢 **{names}**\n\n"
        f"Join ke baad ✅ **Verify** button dabao.",
        reply_markup=kb
    )
    # Auto delete force sub message
    asyncio.create_task(del_later(msg, 300))
    return False

# ═══════════════════════════════════════
#  MULTI-SHORTLINK SYSTEM
# ═══════════════════════════════════════

async def get_active_shortlinks(chat_id=None):
    """
    Active shortlinks sorted by order.
    Group shortlinks bhi include karo agar chat_id diya.
    Returns list of {api_key, url, hours, label, _id}
    """
    links = []
    async for doc in shortlinks_col.find({"active": True}).sort("order", 1):
        links.append(doc)
    # Group-specific shortlinks
    if chat_id:
        async for doc in group_sl_col.find({"chat_id": chat_id, "active": True}).sort("order", 1):
            doc["_group_sl"] = True
            links.append(doc)
    return links

async def make_shortlink_with(url, api_key, api_url):
    """Ek specific shortlink API se link banao"""
    try:
        api = f"https://{api_url}/api?api={api_key}&url={url}&format=text"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(api, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    result = (await r.text()).strip()
                    if result.startswith("http"):
                        return result
    except Exception as e:
        logger.error(f"shortlink error [{api_url}]: {e}")
    return url

async def make_shortlink(url):
    """Fallback: global settings se shortlink banao (purana system)"""
    s = await get_settings()
    if not s.get("shortlink_enabled"): return url
    links = await get_active_shortlinks()
    if links:
        sl = links[0]
        return await make_shortlink_with(url, sl["api_key"], sl["url"])
    if not SHORTLINK_API: return url
    return await make_shortlink_with(url, SHORTLINK_API, SHORTLINK_URL)

async def get_user_verify_state(user_id):
    """
    User ka current verify state.
    DB shortlinks + env shortlink dono check karta hai.
    Returns: (all_verified: bool, next_sl: dict|None, wait_hours: float)
    next_sl = None means env-based shortlink use karo
    """
    links = await get_active_shortlinks()

    if not links:
        # DB mein koi shortlink nahi — env check karo
        if not SHORTLINK_API:
            return True, None, 0  # Koi shortlink configured nahi — bypass
        # Env shortlink hai — legacy date-based verify check
        today = now_ist().strftime("%Y-%m-%d")
        doc = await users_col.find_one({"user_id": user_id})
        verified = bool(doc and doc.get("verified_date") == today)
        if verified:
            return True, None, 0
        else:
            return False, None, 0  # None = env shortlink use karo

    # DB shortlinks hain — unhe check karo
    for sl in links:
        sl_id = str(sl["_id"])
        hours = sl.get("hours", 24)
        log = await verify_log_col.find_one(
            {"user_id": user_id, "shortlink_id": sl_id},
            sort=[("verified_at", -1)]
        )
        if not log:
            return False, sl, 0
        last_verify = make_aware(log["verified_at"])
        time_passed = (now() - last_verify).total_seconds() / 3600
        if time_passed >= hours:
            return False, sl, 0
    return True, None, 0

async def mark_sl_verified(user_id, shortlink_id, sl_label=""):
    """Shortlink verify ka log save karo"""
    from bson import ObjectId
    # Count kitni baar verify kiya
    count = await verify_log_col.count_documents({"user_id": user_id, "shortlink_id": shortlink_id})
    await verify_log_col.insert_one({
        "user_id": user_id,
        "shortlink_id": shortlink_id,
        "sl_label": sl_label,
        "verified_at": now(),
        "verify_number": count + 1
    })

async def get_cached_shortlink(user_id, group_id, target_url, sl_doc=None):
    """
    5 min cache — same user, same group, same shortlink => same link.
    5 min baad naya bane.
    """
    sl_id = str(sl_doc["_id"]) if sl_doc else "default"
    key = (user_id, group_id, sl_id)
    cached = _shortlink_cache.get(key)
    if cached:
        link, expiry = cached
        if now() < expiry:
            return link
    if sl_doc:
        link = await make_shortlink_with(target_url, sl_doc["api_key"], sl_doc["url"])
    else:
        link = await make_shortlink(target_url)
    _shortlink_cache[key] = (link, now() + timedelta(minutes=5))
    return link

# ═══════════════════════════════════════
#  VERIFY CHECK — Multi-Shortlink
# ═══════════════════════════════════════
async def verify_check(client, message, prem=False):
    uid = message.from_user.id
    if uid in ADMINS: return True
    if prem: return True

    s = await get_settings()
    # Global shortlink enabled check
    if not s.get("shortlink_enabled", True): return True

    # Group-level shortlink check
    if hasattr(message, 'chat') and message.chat:
        try:
            gs = await get_group_settings(message.chat.id)
            if not gs.get("shortlink_enabled", True): return True
        except: pass

    all_done, next_sl, _ = await get_user_verify_state(uid)
    if all_done: return True

    me = await client.get_me()
    group_id = message.chat.id

    # next_sl hai ya env-based shortlink use karo
    if next_sl:
        sl_id = str(next_sl["_id"])
        sl_label = next_sl.get("label", "Shortlink")
        hours = next_sl.get("hours", 24)
        token = await make_token(uid, f"sv_{sl_id}")
        verify_url = f"https://t.me/{me.username}?start=sv_{uid}_{token}_{sl_id}"
        short = await get_cached_shortlink(uid, group_id, verify_url, next_sl)
    else:
        # Fallback: env SHORTLINK_API use karo
        if not SHORTLINK_API:
            # No shortlink configured — mark verified automatically
            await mark_verified(uid)
            return True
        sl_id = "env_default"
        sl_label = "Verify"
        hours = 24
        token = await make_token(uid, "sv_env")
        verify_url = f"https://t.me/{me.username}?start=sv_{uid}_{token}"
        # Direct shortlink banao
        try:
            api_url = f"https://{SHORTLINK_URL}/api?api={SHORTLINK_API}&url={verify_url}&format=text"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        result = (await r.text()).strip()
                        short = result if result.startswith("http") else verify_url
                    else:
                        short = verify_url
        except:
            short = verify_url

    links = await get_active_shortlinks()
    total = max(len(links), 1)
    done_count = 0
    if next_sl:
        for sl in links:
            if str(sl["_id"]) == sl_id: break
            done_count += 1

    step_text = f"Step {done_count+1}/{total}: **{sl_label}**" if total > 1 else f"**{sl_label}**"
    time_text = f"Har **{hours} ghante** baad" if hours < 24 else "**Har din 1 baar**"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔗 {step_text} — Verify Karo", url=short)],
        [InlineKeyboardButton("💎 Premium — Kabhi Verify Nahi", callback_data="buy_premium")]
    ])
    msg = await message.reply(
        f"🔐 **Verification Baaki Hai** ({done_count+1}/{total})\n\n"
        f"👇 Link dabao → shortlink solve karo → verify!\n"
        f"⏰ {time_text} karna hoga.\n\n"
        f"💎 **Premium lo** — kabhi verify nahi!",
        reply_markup=kb
    )
    asyncio.create_task(del_later(msg, 300))
    return False

# ═══════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════
def clean_caption(text):
    if not text: return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r't\.me/\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r'\n+', ' ', text)
    return text.strip()

def get_file_name(msg):
    name = ""
    if msg.document and msg.document.file_name:
        name = msg.document.file_name
    elif msg.video and msg.video.file_name:
        name = msg.video.file_name
    elif msg.audio and msg.audio.file_name:
        name = msg.audio.file_name
    elif msg.caption:
        name = clean_caption(msg.caption)[:80]
    else:
        return "File"
    name = re.sub(r'@\w+', '', name)
    name = re.sub(r'http\S+', '', name)
    name = re.sub(r't\.me/\S+', '', name)
    name = name.strip()
    return name if name else "File"

def get_file_size(msg):
    """File size nicely formatted"""
    size = 0
    if msg.document: size = msg.document.file_size or 0
    elif msg.video: size = msg.video.file_size or 0
    elif msg.audio: size = msg.audio.file_size or 0
    if size == 0: return ""
    for unit in ['B','KB','MB','GB']:
        if size < 1024: return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"

async def del_later(msg, secs):
    await asyncio.sleep(secs)
    try: await msg.delete()
    except FloodWait as e:
        await asyncio.sleep(e.value + 2)
        try: await msg.delete()
        except: pass
    except: pass

async def send_log(text):
    try:
        await bot.send_message(LOG_CHANNEL, text, disable_web_page_preview=True)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try: await bot.send_message(LOG_CHANNEL, text, disable_web_page_preview=True)
        except: pass
    except Exception as e:
        logger.warning(f"log failed: {e}")

# ═══════════════════════════════════════
#  SEARCH
# ═══════════════════════════════════════
async def do_search(query, limit=5):
    if not userbot:
        logger.error("Userbot not available!")
        return []

    query = query.strip()
    words = [w.lower() for w in query.split() if len(w) > 1]
    if not words: return []

    results = []
    seen = set()
    search_queries = []
    if len(words) > 1:
        search_queries.append(query)
    search_queries.extend(words[:4])
    longest = max(words, key=len)
    if longest not in search_queries:
        search_queries.append(longest)

    try:
        for sq in search_queries:
            async for msg in userbot.search_messages(FILE_CHANNEL, sq, limit=50):
                if msg.id in seen: continue
                seen.add(msg.id)
                txt = ""
                if msg.caption: txt += msg.caption.lower() + " "
                if msg.document and msg.document.file_name:
                    txt += msg.document.file_name.lower() + " "
                if msg.text: txt += msg.text.lower() + " "
                if not txt.strip(): continue
                score = sum(2 for w in words if w in txt)
                if query.lower() in txt: score += 10
                if score > 0:
                    results.append((score, msg))

        results.sort(key=lambda x: x[0], reverse=True)
        return [m for _, m in results[:limit]]
    except Exception as e:
        logger.error(f"search error [{query}]: {e}")
        return []

# ═══════════════════════════════════════
#  SEND FILE TO PM
# ═══════════════════════════════════════
async def send_file_to_pm(client, user, msg_id, prem=False):
    try:
        s = await get_settings()
        t = s.get("auto_delete_time", 300)
        mins = t // 60

        file_msg = await bot.get_messages(FILE_CHANNEL, msg_id)
        if not file_msg or file_msg.empty:
            return False, "File nahi mili"

        fname = get_file_name(file_msg)
        fsize = get_file_size(file_msg)
        size_text = f"📦 Size: {fsize}\n" if fsize else ""

        clean_cap = (
            f"🗂 **{fname}**\n\n"
            f"{size_text}"
            f"⏳ **{mins} min** baad delete hogi!\n"
            f"📌 Abhi save ya forward kar lo!"
        )

        # Premium users ke liye stream + download buttons
        kb = None
        if prem and KOYEB_URL:
            stream_url = f"{KOYEB_URL}/stream?uid={user.id}&mid={msg_id}"
            # Download = Telegram file direct URL via bot token
            # Ya stream URL same use karo — mini app mein download button hai
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("▶️ Stream Karo", web_app=WebAppInfo(url=stream_url)),
                    InlineKeyboardButton("⬇️ Download", web_app=WebAppInfo(url=stream_url))
                ]
            ])

        sent = await file_msg.copy(
            chat_id=user.id,
            caption=clean_cap,
            parse_mode=enums.ParseMode.MARKDOWN,
            reply_markup=kb
        )

        if not sent:
            return False, "Send fail hua"

        await increment_daily(user.id)

        if s.get("auto_delete"):
            asyncio.create_task(del_later(sent, t))

        return True, fname

    except Exception as e:
        logger.error(f"send_file_to_pm error: {e}")
        return False, str(e)

# ═══════════════════════════════════════
#  /START
# ═══════════════════════════════════════
@bot.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    uid = message.from_user.id
    args = message.command[1] if len(message.command) > 1 else ""
    referred_by = None

    # Refer link: ref_USERID
    if args.startswith("ref_"):
        try:
            referred_by = int(args[4:])
            if referred_by == uid:
                referred_by = None  # Khud ko refer nahi kar sakte
        except: pass

    is_new = await save_user(message.from_user, referred_by)

    if is_new:
        await send_log(
            f"👤 **Naya User**\n"
            f"Name: {message.from_user.mention}\n"
            f"ID: `{uid}`\n"
            f"Referred by: `{referred_by or 'Direct'}`\n"
            f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
        )

    # sv_ — shortlink verification (format: sv_UID_TOKEN_SLID)
    if args.startswith("sv_"):
        parts = args[3:].split("_", 2)
        if len(parts) < 2:
            await message.reply("❌ Invalid link.")
            return
        try:
            token_uid = int(parts[0])
            token = parts[1]
            sl_id = parts[2] if len(parts) > 2 else "env_default"
        except:
            await message.reply("❌ Invalid link.")
            return
        if uid != token_uid:
            await message.reply("❌ Yeh link aapke liye nahi! Group mein apna link lo.")
            return
        valid, _ = await check_token(token, expected_uid=uid)
        if valid:
            await tokens_col.delete_one({"token": token})
            # Cache clear for this user
            keys_to_del = [k for k in _shortlink_cache if k[0] == uid]
            for k in keys_to_del: del _shortlink_cache[k]

            # Shortlink verify log
            sl_label = "Shortlink"
            if sl_id and sl_id != "env_default":
                try:
                    from bson import ObjectId
                    sl_doc = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
                    if sl_doc:
                        sl_label = sl_doc.get("label", "Shortlink")
                        hours = sl_doc.get("hours", 24)
                        await mark_sl_verified(uid, sl_id, sl_label)
                        verify_count = await verify_log_col.count_documents({"user_id": uid, "shortlink_id": sl_id})
                        await send_log(
                            f"✅ **Shortlink Verified**\n\n"
                            f"👤 {message.from_user.mention} (`{uid}`)\n"
                            f"🔗 {sl_label}\n"
                            f"🔢 #{verify_count}\n"
                            f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
                        )
                except Exception as e:
                    logger.error(f"sl verify log error: {e}")
                    await mark_sl_verified(uid, sl_id or "default", sl_label)
            else:
                # env_default ya legacy — date-based mark karo
                await mark_verified(uid)
                await send_log(
                    f"✅ **Verified**\n\n"
                    f"👤 {message.from_user.mention} (`{uid}`)\n"
                    f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
                )

            # Check baaki shortlinks hain?
            all_done, next_sl, _ = await get_user_verify_state(uid)
            if all_done:
                await message.reply(
                    f"✅ **Sab Verify Ho Gaya!** 🎉\n\n"
                    f"Ab search karo group mein! 🗂\n"
                    f"💎 Roz verify na karna ho: /premium"
                )
            else:
                next_label = next_sl.get("label", "Next Shortlink")
                me2 = await client.get_me()
                token2 = await make_token(uid, f"sv_{str(next_sl['_id'])}")
                sl_id2 = str(next_sl["_id"])
                verify_url2 = f"https://t.me/{me2.username}?start=sv_{uid}_{token2}_{sl_id2}"
                short2 = await get_cached_shortlink(uid, 0, verify_url2, next_sl)
                await message.reply(
                    f"✅ **{sl_label} Verified!**\n\n"
                    f"Ek aur baaki hai: **{next_label}**\n"
                    f"👇 Neeche link dabao:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"🔗 {next_label} Verify Karo", url=short2)],
                        [InlineKeyboardButton("💎 Premium lo — Sab Skip", callback_data="buy_premium")]
                    ])
                )
        else:
            await message.reply("❌ Token expire! Group mein dobara search karo.")
        return

    # getfile_ — file PM mein bhejo
    if args.startswith("getfile_"):
        parts = args[8:].split("_", 1)
        if len(parts) != 2:
            await message.reply("❌ Invalid link.")
            return
        try:
            req_uid = int(parts[0])
            msg_id = int(parts[1])
        except:
            await message.reply("❌ Invalid link.")
            return
        if uid != req_uid:
            await message.reply("❌ Yeh file aapke liye nahi! Apna search karo.")
            return

        s = await get_settings()
        prem = await is_premium(uid)
        if not prem:
            # Free trial check
            _, can_trial = await get_free_trial_status(uid)
            count = await get_daily_count(uid)
            if count >= s.get("daily_limit", 10):
                await message.reply(
                    f"⚠️ **Daily Limit Khatam!**\n"
                    f"💎 /premium lo unlimited ke liye!"
                )
                return

        wait = await message.reply("📥 File aa rahi hai... ⏳")
        success, info = await send_file_to_pm(client, message.from_user, msg_id, prem)
        await wait.delete()

        if not success:
            await message.reply(f"❌ File nahi aa payi.\nError: `{info}`")
        return

    # Normal /start — referred_by se aaya to welcome message
    me = await client.get_me()
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None

    buttons = [
        [
            InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
            InlineKeyboardButton("➕ Group Add", url=f"https://t.me/{me.username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("💎 Premium", callback_data="show_premium"),
            InlineKeyboardButton("ℹ️ Help", callback_data="help")
        ],
        [
            InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
            InlineKeyboardButton("🔗 Refer & Earn", callback_data="refer_info")
        ]
    ]
    if miniapp_url:
        buttons.append([InlineKeyboardButton("🌐 Mini App Kholo", web_app=WebAppInfo(url=miniapp_url))])

    await message.reply(
        f"🗂 **AsBhai Drop Bot**\n\n"
        f"Namaste **{message.from_user.mention}**! 👋\n\n"
        f"Group mein file naam type karo — PM mein file aayegi! 📥\n\n"
        f"**Kaise kaam karta hai:**\n"
        f"1️⃣ Channel join karo\n"
        f"2️⃣ Har roz 1 baar verify karo\n"
        f"3️⃣ Group mein naam type karo\n"
        f"4️⃣ PM mein file aayegi! 🎉\n\n"
        f"💎 **Premium** = No verify + 10 results + stream!\n"
        f"🔗 **10 Refer** = 15 din FREE Premium!\n"
        f"💰 **Earn Money** = Group mein shortlink lagao!\n\n"
        f"/premium | /mystats | /referlink",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ═══════════════════════════════════════
#  REFER COMMANDS
# ═══════════════════════════════════════
@bot.on_message(filters.command("referlink"))
async def refer_link_cmd(client, message: Message):
    uid = message.from_user.id
    me = await client.get_me()
    ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
    doc = await users_col.find_one({"user_id": uid})
    refer_count = doc.get("refer_count", 0) if doc else 0
    needed = 10 - (refer_count % 10) if refer_count % 10 != 0 else 10
    await message.reply(
        f"🔗 **Aapka Refer Link**\n\n"
        f"`{ref_link}`\n\n"
        f"📊 **Aapke Refers:** {refer_count}\n"
        f"🎯 Premium ke liye: **{needed}** aur chahiye\n\n"
        f"**Reward:** Har 10 refer = **15 din FREE Premium** 💎\n\n"
        f"Dosto ko share karo aur free premium pao! 🎉",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Share Karo", url=f"https://t.me/share/url?url={ref_link}&text=Is+bot+se+movies+free+mein+download+karo!")]
        ])
    )

# ═══════════════════════════════════════
#  PM SEARCH — Sirf Premium Users
# ═══════════════════════════════════════
@bot.on_message(
    filters.private & filters.text &
    filters.incoming &
    ~filters.bot &
    ~filters.command([
        "start","help","stats","broadcast","setdelete",
        "addpremium","removepremium","forcesub","settings",
        "premium","ping","shortlink","setlimit","setresults",
        "mystats","ban","unban","maintenance","request","referlink",
        "fsub","groupsettings","gsettings","gset",
        "addshortlink","removeshortlink","shortlinks","setcommands",
        "gshortlink","gshortlinkremove","gshortlinks","requests"
    ])
)
async def pm_search_handler(client, message: Message):
    if not message.from_user: return
    if message.from_user.is_bot: return
    if message.forward_date: return
    uid = message.from_user.id
    await save_user(message.from_user)
    query_text = message.text.strip()
    if len(query_text) < 2: return

    prem = await is_premium(uid)
    if not prem:
        await message.reply(
            f"❌ **PM mein search sirf Premium users ke liye hai!**\n\n"
            f"Group mein search karo:\n"
            f"👉 @asfilter_group\n\n"
            f"💎 Premium lo to yahan bhi search karo:\n"
            f"/premium"
        )
        return

    if await is_banned(uid): return
    s = await get_settings()
    if s.get("maintenance") and uid not in ADMINS:
        await message.reply("🔧 Maintenance chal raha hai. Thodi der baad try karo."); return

    wait_msg = await message.reply(f"🔍 **\'{query_text}\'** dhundh raha hoon... ⏳")
    doc = await users_col.find_one({"user_id": uid})
    limit = doc.get("prem_results_pref", 10) if doc else 10
    found = await do_search(query_text, limit=limit)

    if not found:
        await wait_msg.edit(
            f"😕 **\'{query_text}\' nahi mila**\n\n"
            f"• Sirf naam likhein\n"
            f"• Spelling check karo\n\n"
            f"📩 `/request {query_text}`"
        )
        return

    await wait_msg.delete()
    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r"[@#]\w+", "", fname)
        fname_clean = re.sub(r"_+", " ", fname_clean).strip()
        fname_show = fname_clean[:40] if fname_clean else f"File {idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

    result_msg = await message.reply(
        f"🔍 **{len(found)} Result{'s' if len(found)>1 else ''}**\n\n👇 File ka button dabao:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    s = await get_settings()
    if s.get("auto_delete"):
        asyncio.create_task(del_later(result_msg, s.get("auto_delete_time", 300)))

# ═══════════════════════════════════════
#  GROUP SEARCH
# ═══════════════════════════════════════
# Per-user search cooldown — ek search khatam ho tab tak naya nahi
_search_locks = {}
_search_cooldown = {}  # {uid_query: expire_time}

@bot.on_message(
    filters.group & filters.text &
    ~filters.bot &
    ~filters.via_bot &
    filters.incoming &
    ~filters.command([
        "start","help","stats","broadcast","setdelete",
        "addpremium","removepremium","forcesub","settings",
        "premium","ping","shortlink","setlimit","setresults",
        "mystats","ban","unban","maintenance","request","referlink",
        "fsub","groupsettings","gsettings","gset",
        "addshortlink","removeshortlink","shortlinks",
        "setcommands","gshortlink","gshortlinkremove","gshortlinks",
        "requests","ping","ban","unban","stats","broadcast",
        "addpremium","removepremium","setdelete","setlimit",
        "setresults","maintenance","settings","shortlink"
    ])
)
async def search_handler(client, message: Message):
    # ── Strict guards — infinite loop rokne ke liye ──
    if not message.from_user: return          # Channel posts ignore
    if message.from_user.is_bot: return       # Bot messages ignore
    if message.forward_date: return           # Forwarded ignore
    if message.via_bot: return                # Inline bot results ignore

    # Bot ka apna ID check — most important
    try:
        me = await client.get_me()
        if message.from_user.id == me.id: return
    except: pass

    query = (message.text or "").strip()

    # ─── ABSOLUTE FIRST CHECK: command hai to bilkul ignore ───
    if query.startswith("/"): return

    # Message entities mein bhi check — bot commands
    if message.entities:
        from pyrogram.enums import MessageEntityType
        for ent in message.entities:
            if ent.type == MessageEntityType.BOT_COMMAND: return

    # Minimum length aur maximum length
    if len(query) < 2 or len(query) > 80: return

    # Unicode/emoji-only messages ignore (sirf emoji ya special chars)
    import unicodedata
    plain = ''.join(c for c in query if unicodedata.category(c) not in ('So','Sm','Sk','Sc','Cs')).strip()
    if len(plain) < 2: return

    # Bot ke apne sent messages ignore — har prefix check
    bot_msg_prefixes = (
        "🔍","😕","⚠","📩","🔐","╔","🔧","✅","❌","🗂","🆘",
        "🎉","💎","📢","⏳","🔒","🏓","📊","👤","🚫","🔗","📱",
        "ℹ️","💰","🆓","📥","🏘","⚙️","🔔","📋",
    )
    if any(query.startswith(p) for p in bot_msg_prefixes): return

    # Short common words jo movie naam nahi hain
    skip_words = {"hi","hello","hii","hlo","helo","ok","okay","thanks","ty",
                  "thx","bye","lol","haha","yes","no","kya","koi",
                  "hai","hain","nahi","kaise","kaisa","mera","tera",
                  "acha","accha","theek","thik","bhai","bhi","wala",
                  "movie","film","web","series","episode","part"}
    if query.lower() in skip_words: return

    # Per-user rate limit — ek user ek waqt mein sirf ek search
    uid = message.from_user.id

    # Cooldown — same user ki same query 30 sec mein ek baar
    cache_key = f"{uid}_{query.lower()[:20]}"
    if _search_cooldown.get(cache_key, 0) > asyncio.get_event_loop().time():
        return

    if _search_locks.get(uid):
        return  # Already searching
    _search_locks[uid] = True
    _search_cooldown[cache_key] = asyncio.get_event_loop().time() + 30

    try:
        await save_group(message.chat)
        await save_user(message.from_user)

        chat_id = message.chat.id
        s = await get_settings()
        gs = await get_group_settings(chat_id)

        if await is_banned(uid): return
        if s.get("maintenance") and uid not in ADMINS:
            msg = await message.reply("🔧 Maintenance. Thodi der baad try karo.")
            asyncio.create_task(del_later(msg, 60))
            return

        prem = await is_premium(uid)

        # Request mode check
        if gs.get("request_mode") and not prem and uid not in ADMINS:
            msg = await message.reply(
            f"📩 **Request Mode Active Hai**\n\n"
            f"Search band hai. `/request {query}` karein.\n"
            f"💎 Premium se search bypass karo!"
            )
            asyncio.create_task(del_later(msg, 300))
            return

        # Force sub — premium bypass karta hai
        if not await force_sub_check(client, message, prem): return
        if not await verify_check(client, message, prem): return

        if not prem:
            count = await get_daily_count(uid)
            if count >= s.get("daily_limit", 10):
                msg = await message.reply(
                f"⚠️ {message.from_user.mention}, aaj ki limit "
                f"**{s.get('daily_limit',10)}** ho gayi!\n"
                f"💎 /premium lo unlimited ke liye!"
                )
                asyncio.create_task(del_later(msg, 300))
                return

        try:

            wait_msg = await message.reply(f"🔍 **'{query}'** dhundh raha hoon... ⏳")
        except FloodWait as e:
            logger.warning(f"FloodWait sending wait_msg, skip uid={uid}")
            _search_locks[uid] = False
            return
        except Exception as e:
            logger.error(f"wait_msg error: {e}")
            _search_locks[uid] = False
            return
        # Result count — group settings se
        if prem:
            limit = gs.get("premium_results", 10)
        else:
            limit = gs.get("free_results", 5)

        found = await do_search(query, limit=limit)

        if not found:
            edited = await wait_msg.edit(
            f"😕 **'{query}' nahi mila**\n\n"
            f"• Sirf naam likhein\n"
            f"• Spelling check karo\n\n"
            f"📩 `/request {query}`"
        )
            asyncio.create_task(del_later(edited, 300))
            return

        found = found[:limit]
        await wait_msg.delete()
        me = await client.get_me()

        buttons = []
        for idx, fmsg in enumerate(found):
            fname = get_file_name(fmsg)
            fname_clean = re.sub(r'[@#]\w+', '', fname)
            fname_clean = re.sub(r'_+', ' ', fname_clean).strip()
            fname_show = fname_clean[:40] if fname_clean else f"File {idx+1}"
            fsize = get_file_size(fmsg)
            size_text = f" [{fsize}]" if fsize else ""

            final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
            buttons.append([
                InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)
            ])

        kb = InlineKeyboardMarkup(buttons)
        t = gs.get("auto_delete_time", 300)
        mins = t // 60
        count_text = len(found)

        result_text = (
            f"🔍 {message.from_user.mention}\n\n"
            f"╔══ 🎯 **{count_text} Result{'s' if count_text > 1 else ''}** ══╗\n\n"
            f"👇 File ka button dabao → PM mein file aayegi!\n"
            f"⏳ Ye message {mins} min baad delete hoga."
        )

        result_msg = await message.reply(result_text, reply_markup=kb)

        if gs.get("auto_delete", True):
            asyncio.create_task(del_later(result_msg, t))

    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s uid={uid} — skipping reply")
        # FloodWait mein reply mat karo — bas wait karo
        await asyncio.sleep(min(e.value, 10))
    except Exception as e:
        logger.error(f"search_handler error uid={uid}: {e}")
    finally:
        _search_locks[uid] = False

# ═══════════════════════════════════════
#  /FSUB — Multi-channel management
# ═══════════════════════════════════════
@bot.on_message(filters.command("fsub") & filters.user(ADMINS))
async def fsub_manage(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        channels = s.get("fsub_channels", [])
        groups_list = s.get("fsub_groups", [])
        
        text = f"📢 **Force Sub Management**\n\n"
        text += f"Status: **{'ON' if s.get('force_sub') else 'OFF'}**\n\n"
        
        if channels:
            text += "**Channels:**\n"
            for i, ch in enumerate(channels):
                text += f"  {i+1}. {ch.get('title','?')} `{ch.get('id')}`\n"
        else:
            text += "**Channels:** None\n"
        
        if groups_list:
            text += "\n**Groups:**\n"
            for i, g in enumerate(groups_list):
                text += f"  {i+1}. {g.get('title','?')} `{g.get('id')}`\n"
        else:
            text += "\n**Groups:** None\n"
        
        text += (
            f"\n**Commands:**\n"
            f"`/fsub add channel @username` — channel add karo\n"
            f"`/fsub add group @username` — group add karo\n"
            f"`/fsub remove channel <id>` — hataao\n"
            f"`/fsub remove group <id>` — hataao\n"
            f"`/fsub on` — Force sub ON\n"
            f"`/fsub off` — Force sub OFF\n"
            f"`/fsub request on/off` — Request mode toggle\n"
            f"`/fsub list` — List dekho\n"
            f"`/fsub clear` — Sab hataao"
        )
        await message.reply(text)
        return

    sub_cmd = args[1].lower()

    if sub_cmd in ["on", "off"]:
        await update_setting("force_sub", sub_cmd == "on")
        await message.reply(f"✅ Force Sub **{sub_cmd.upper()}**!")
        return

    if sub_cmd == "request":
        if len(args) < 3:
            await message.reply("Usage: `/fsub request on/off`")
            return
        val = args[2].lower() == "on"
        await update_setting("request_mode", val)
        await message.reply(f"✅ Request Mode **{'ON' if val else 'OFF'}**!")
        return

    if sub_cmd == "list":
        s = await get_settings()
        channels = s.get("fsub_channels", [])
        groups_list = s.get("fsub_groups", [])
        text = "📋 **Force Sub List**\n\n"
        for i, ch in enumerate(channels):
            text += f"🔵 Channel {i+1}: {ch.get('title')} | `{ch.get('id')}` | @{ch.get('username','')}\n"
        for i, g in enumerate(groups_list):
            text += f"🟢 Group {i+1}: {g.get('title')} | `{g.get('id')}` | @{g.get('username','')}\n"
        if not channels and not groups_list:
            text += "Koi channel/group nahi add hua!"
        await message.reply(text)
        return

    if sub_cmd == "clear":
        await update_setting("fsub_channels", [])
        await update_setting("fsub_groups", [])
        await message.reply("✅ Sab force sub channels/groups hata diye!")
        return

    if sub_cmd == "add":
        if len(args) < 4:
            await message.reply(
                "Usage:\n"
                "`/fsub add channel @username_or_id`\n"
                "`/fsub add group @username_or_id`"
            )
            return
        kind = args[2].lower()  # channel ya group
        target = args[3]
        try:
            chat = await client.get_chat(target)
            entry = {
                "id": chat.id,
                "username": chat.username or "",
                "title": chat.title or chat.first_name or target
            }
            s = await get_settings()
            key = "fsub_channels" if kind == "channel" else "fsub_groups"
            existing = s.get(key, [])
            if any(e["id"] == chat.id for e in existing):
                await message.reply(f"⚠️ **{entry['title']}** pehle se add hai!")
                return
            existing.append(entry)
            await update_setting(key, existing)
            await message.reply(
                f"✅ **{entry['title']}** add ho gaya!\n"
                f"ID: `{chat.id}`\n"
                f"Type: {kind}"
            )
        except Exception as e:
            await message.reply(f"❌ Error: {e}\n\nBot ko us channel/group mein admin banao!")
        return

    if sub_cmd == "remove":
        if len(args) < 4:
            await message.reply("Usage: `/fsub remove channel/group <chat_id>`")
            return
        kind = args[2].lower()
        try:
            target_id = int(args[3])
        except:
            await message.reply("❌ Valid ID do!")
            return
        s = await get_settings()
        key = "fsub_channels" if kind == "channel" else "fsub_groups"
        existing = s.get(key, [])
        new_list = [e for e in existing if e["id"] != target_id]
        if len(new_list) == len(existing):
            await message.reply("❌ Ye ID list mein nahi hai!")
            return
        await update_setting(key, new_list)
        await message.reply(f"✅ `{target_id}` hata diya!")
        return

    await message.reply("❌ Unknown sub-command. `/fsub` likho help ke liye.")

# ═══════════════════════════════════════
#  /GSETTINGS — Group-level settings
# ═══════════════════════════════════════
@bot.on_message(filters.command(["gsettings", "gset", "groupsettings"]))
async def group_settings_cmd(client, message: Message):
    uid = message.from_user.id
    
    if message.chat.type == enums.ChatType.PRIVATE:
        # PM mein group select karne ka option
        await message.reply(
            "⚙️ **Group Settings**\n\n"
            "Group mein jaake `/gsettings` type karo\n"
            "ya group ID ke saath:\n"
            "`/gsettings -1001234567890`"
        )
        return

    # Group mein
    chat_id = message.chat.id
    
    # Check admin
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [
            enums.ChatMemberStatus.OWNER,
            enums.ChatMemberStatus.ADMINISTRATOR
        ] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    
    if not is_admin:
        await message.reply("❌ Sirf group admins settings change kar sakte hain!")
        return

    args = message.command
    
    if len(args) == 1:
        # Show current settings
        gs = await get_group_settings(chat_id)
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('force_sub') else '❌'} Force Sub",
                    callback_data=f"gs_toggle_{chat_id}_force_sub"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('shortlink_enabled') else '❌'} Shortlink",
                    callback_data=f"gs_toggle_{chat_id}_shortlink_enabled"
                )
            ],
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('auto_delete') else '❌'} Auto Delete",
                    callback_data=f"gs_toggle_{chat_id}_auto_delete"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('request_mode') else '❌'} Request Mode",
                    callback_data=f"gs_toggle_{chat_id}_request_mode"
                )
            ],
            [
                InlineKeyboardButton(
                    f"📊 Free: {gs.get('free_results',5)} Results",
                    callback_data=f"gs_results_{chat_id}_free"
                ),
                InlineKeyboardButton(
                    f"💎 Premium: {gs.get('premium_results',10)} Results",
                    callback_data=f"gs_results_{chat_id}_prem"
                )
            ],
            [
                InlineKeyboardButton("🌐 PM Settings", callback_data="pm_settings"),
                InlineKeyboardButton("✅ Done", callback_data="gs_done")
            ]
        ])
        await message.reply(
            f"⚙️ **{message.chat.title} — Settings**\n\n"
            f"Force Sub: {'✅ ON' if gs.get('force_sub') else '❌ OFF'}\n"
            f"Shortlink: {'✅ ON' if gs.get('shortlink_enabled') else '❌ OFF'}\n"
            f"Auto Delete: {'✅ ON' if gs.get('auto_delete') else '❌ OFF'}\n"
            f"Request Mode: {'✅ ON' if gs.get('request_mode') else '❌ OFF'}\n"
            f"Free Results: {gs.get('free_results',5)}\n"
            f"Premium Results: {gs.get('premium_results',10)}\n",
            reply_markup=kb
        )
        return

    # Command-based
    sub = args[1].lower()
    if sub == "results" and len(args) >= 4:
        try:
            free = int(args[2])
            prem = int(args[3])
            await update_group_setting(chat_id, "free_results", free)
            await update_group_setting(chat_id, "premium_results", prem)
            await message.reply(f"✅ Results — Free: **{free}** | Premium: **{prem}**")
        except:
            await message.reply("Usage: `/gsettings results <free> <premium>`")
    elif sub in ["forcesub", "shortlink", "autodelete", "requestmode"]:
        if len(args) < 3:
            await message.reply(f"Usage: `/gsettings {sub} on/off`")
            return
        val = args[2].lower() == "on"
        key_map = {
            "forcesub": "force_sub",
            "shortlink": "shortlink_enabled",
            "autodelete": "auto_delete",
            "requestmode": "request_mode"
        }
        await update_group_setting(chat_id, key_map[sub], val)
        await message.reply(f"✅ **{sub}** = **{'ON' if val else 'OFF'}**")

# ═══════════════════════════════════════
#  FILE REQUEST
# ═══════════════════════════════════════
@bot.on_message(filters.command("request"))
async def file_request(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/request Movie Name`")
        return
    req_text = " ".join(args[1:])
    uid = message.from_user.id

    chat_link = ""
    if message.chat.type != enums.ChatType.PRIVATE:
        try:
            invite = await bot.export_chat_invite_link(message.chat.id)
            chat_link = f"\n🔗 Group: {invite}"
        except:
            chat_link = f"\n🏘 Group: `{message.chat.id}`"

    await requests_col.insert_one({
        "user_id": uid,
        "name": message.from_user.first_name,
        "request": req_text,
        "chat_id": message.chat.id,
        "time": now()
    })
    prem = await is_premium(uid)
    eta = "Jaldi" if prem else "1-2 din"
    msg = await message.reply(
        f"📩 **Request Submit Ho Gayi!**\n\n"
        f"📁 `{req_text}`\n\n"
        f"⏳ ETA: **{eta}**\n"
        f"📢 {MAIN_CHANNEL}"
    )
    if message.chat.type != enums.ChatType.PRIVATE:
        asyncio.create_task(del_later(msg, 300))
    
    await send_log(
        f"📩 **New Request** {'⚡ Premium' if prem else ''}\n"
        f"👤 {message.from_user.mention} (`{uid}`)\n"
        f"📁 `{req_text}`"
        f"{chat_link}"
    )

# ═══════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, query: CallbackQuery):
    data = query.data
    uid = query.from_user.id

    # Channel join verify
    if data.startswith("checkjoin_"):
        target = int(data.split("_")[1])
        if uid != target:
            await query.answer("Ye button aapke liye nahi!", show_alert=True)
            return
        prem = await is_premium(uid)
        joined, not_joined = await check_member_multi(uid, prem)
        if joined:
            await query.message.delete()
            await query.answer("✅ Verified! Ab search karo!", show_alert=False)
        else:
            names = ", ".join(ch.get("title", "Channel") for ch in not_joined)
            await query.answer(
                f"❌ Abhi join nahi kiya!\n{names}",
                show_alert=True
            )
        return

    # Group settings toggle
    if data.startswith("gs_toggle_"):
        parts = data.split("_")
        # gs_toggle_CHATID_KEY
        chat_id = int(parts[2])
        key = "_".join(parts[3:])
        gs = await get_group_settings(chat_id)
        new_val = not gs.get(key, True)
        await update_group_setting(chat_id, key, new_val)
        await query.answer(f"{'✅ ON' if new_val else '❌ OFF'}", show_alert=False)
        # Refresh keyboard
        gs = await get_group_settings(chat_id)
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('force_sub') else '❌'} Force Sub",
                    callback_data=f"gs_toggle_{chat_id}_force_sub"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('shortlink_enabled') else '❌'} Shortlink",
                    callback_data=f"gs_toggle_{chat_id}_shortlink_enabled"
                )
            ],
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('auto_delete') else '❌'} Auto Delete",
                    callback_data=f"gs_toggle_{chat_id}_auto_delete"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('request_mode') else '❌'} Request Mode",
                    callback_data=f"gs_toggle_{chat_id}_request_mode"
                )
            ],
            [
                InlineKeyboardButton(
                    f"📊 Free: {gs.get('free_results',5)} Results",
                    callback_data=f"gs_results_{chat_id}_free"
                ),
                InlineKeyboardButton(
                    f"💎 Premium: {gs.get('premium_results',10)} Results",
                    callback_data=f"gs_results_{chat_id}_prem"
                )
            ],
            [InlineKeyboardButton("✅ Done", callback_data="gs_done")]
        ])
        try:
            await query.message.edit_reply_markup(kb)
        except: pass
        return

    if data.startswith("gs_results_"):
        parts = data.split("_")
        chat_id = int(parts[2])
        kind = parts[3]  # free ya prem
        key = "free_results" if kind == "free" else "premium_results"
        gs = await get_group_settings(chat_id)
        cur = gs.get(key, 5)
        # Cycle: 3 → 5 → 7 → 10 → 3
        cycle = [3, 5, 7, 10]
        try:
            idx = cycle.index(cur)
            new_val = cycle[(idx+1) % len(cycle)]
        except:
            new_val = 5
        await update_group_setting(chat_id, key, new_val)
        await query.answer(f"{'Free' if kind=='free' else 'Premium'}: {new_val} results", show_alert=False)
        gs = await get_group_settings(chat_id)
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('force_sub') else '❌'} Force Sub",
                    callback_data=f"gs_toggle_{chat_id}_force_sub"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('shortlink_enabled') else '❌'} Shortlink",
                    callback_data=f"gs_toggle_{chat_id}_shortlink_enabled"
                )
            ],
            [
                InlineKeyboardButton(
                    f"{'✅' if gs.get('auto_delete') else '❌'} Auto Delete",
                    callback_data=f"gs_toggle_{chat_id}_auto_delete"
                ),
                InlineKeyboardButton(
                    f"{'✅' if gs.get('request_mode') else '❌'} Request Mode",
                    callback_data=f"gs_toggle_{chat_id}_request_mode"
                )
            ],
            [
                InlineKeyboardButton(
                    f"📊 Free: {gs.get('free_results',5)} Results",
                    callback_data=f"gs_results_{chat_id}_free"
                ),
                InlineKeyboardButton(
                    f"💎 Premium: {gs.get('premium_results',10)} Results",
                    callback_data=f"gs_results_{chat_id}_prem"
                )
            ],
            [InlineKeyboardButton("✅ Done", callback_data="gs_done")]
        ])
        try:
            await query.message.edit_reply_markup(kb)
        except: pass
        return

    if data == "gs_done":
        await query.message.delete()
        return

    if data == "pm_settings":
        prem = await is_premium(uid)
        doc = await users_col.find_one({"user_id": uid})
        free_res = doc.get("free_results_pref", 5) if doc else 5
        prem_res = doc.get("prem_results_pref", 10) if doc else 10
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"📊 Results: {prem_res if prem else free_res}", callback_data="pmsett_results"),
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"⚙️ **PM Settings**\n\n"
            f"Ye settings sirf aapke liye hain.\n\n"
            f"💎 Premium: {'✅' if prem else '❌'}\n"
            f"Results: **{prem_res if prem else free_res}**\n\n"
            f"{'Premium se results 10 tak set kar sakte ho!' if prem else 'Premium lo to results 10 tak!'}",
            reply_markup=kb
        )
        return

    if data == "pmsett_results":
        prem = await is_premium(uid)
        if not prem:
            await query.answer("💎 Sirf Premium users results change kar sakte hain!", show_alert=True)
            return
        doc = await users_col.find_one({"user_id": uid})
        cur = doc.get("prem_results_pref", 10) if doc else 10
        cycle = [5, 7, 10]
        try:
            idx = cycle.index(cur)
            new_val = cycle[(idx+1) % len(cycle)]
        except:
            new_val = 10
        await users_col.update_one({"user_id": uid}, {"$set": {"prem_results_pref": new_val}}, upsert=True)
        await query.answer(f"✅ Results: {new_val}", show_alert=False)
        return

    if data == "refer_info":
        doc = await users_col.find_one({"user_id": uid})
        refer_count = doc.get("refer_count", 0) if doc else 0
        needed = 10 - (refer_count % 10) if refer_count % 10 != 0 else 10
        me = await client.get_me()
        ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
        await query.message.edit(
            f"🔗 **Refer & Earn**\n\n"
            f"**Aapka Link:**\n`{ref_link}`\n\n"
            f"📊 Total Refers: **{refer_count}**\n"
            f"🎯 Next Premium ke liye: **{needed}** aur\n\n"
            f"**Reward:** Har 10 refer = 15 din FREE Premium 💎\n\n"
            f"Agar koi pehle se join kiya hua ho to\n"
            f"credit **nahi** milega!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={ref_link}&text=Free+movies+download+karo!")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )
        return

    if data == "need_premium":
        await query.answer("💎 Sirf Premium ke liye! /premium type karo.", show_alert=True)

    elif data == "show_premium":
        prem = await is_premium(uid)
        exp = await get_premium_expiry(uid)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        _, can_trial = await get_free_trial_status(uid)
        trial_text = f"\n🆓 **Free Trial:** {'Abhi shuru karo (2 baar)' if can_trial else 'Khatam ho gayi'}" if not prem else ""
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Kharidein", callback_data="buy_premium")],
            [InlineKeyboardButton("🔗 Refer Karo — Free Premium", callback_data="refer_info")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"💎 **Premium**\n\n"
            f"Status: {'✅ Active' if prem else '❌ Nahi'}\n"
            f"Expiry: {exp_str}{trial_text}\n\n"
            f"**Benefits:**\n"
            f"• 🔓 Force join nahi\n"
            f"• 🔗 Shortlink nahi\n"
            f"• 📦 10 results per search\n"
            f"• ∞ Unlimited downloads\n"
            f"• ▶️ Stream karo Mini App mein\n"
            f"• ⚡ Fast + Priority request\n"
            f"• 🎛 Results 5/7/10 customize karo\n\n"
            f"**Pricing:**\n"
            f"• ₹50 — 10 din\n"
            f"• ₹150 — 30 din\n"
            f"• ₹200 — 60 din\n"
            f"• ₹500 — 150 din\n"
            f"• ₹800 — 365 din\n\n"
            f"🆓 **5 min Free Trial:** 2 baar",
            reply_markup=kb
        )

    elif data == "buy_premium":
        miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
        buttons = [
            [InlineKeyboardButton("📞 Contact @asbhaibsr", url="https://t.me/asbhaibsr")],
            [InlineKeyboardButton("🔙 Back", callback_data="show_premium")]
        ]
        if miniapp_url:
            buttons.insert(0, [InlineKeyboardButton("🌐 Mini App mein Buy Karo", web_app=WebAppInfo(url=miniapp_url))])
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n"
            f"**UPI ID:** `{UPI_ID}`\n\n"
            f"1️⃣ UPI se payment karo\n"
            f"2️⃣ Screenshot lo\n"
            f"3️⃣ @asbhaibsr ko bhejo\n"
            f"4️⃣ 1 ghante mein activate! ⚡\n\n"
            f"Ya Mini App mein buy karo 👇",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif data == "help":
        await query.message.edit(
            "📖 **Bot Guide**\n\n"
            "**Kaise use karein:**\n"
            "1️⃣ Channel join karo\n"
            "2️⃣ Daily 1 baar verify karo\n"
            "3️⃣ Group mein naam type karo\n"
            "4️⃣ Button milega → click → PM mein file! 📥\n\n"
            "⚠️ File kuch der baad delete hogi\n"
            "📌 Save ya forward kar lo!\n\n"
            "💎 Premium = No verify + 10 results + stream!\n"
            "🔗 10 Refer = 15 din free premium!\n\n"
            "/premium /mystats /request /referlink",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    elif data == "my_stats":
        prem = await is_premium(uid)
        count = await get_daily_count(uid)
        doc = await users_col.find_one({"user_id": uid})
        joined = make_aware(doc["joined"]).astimezone(IST).strftime("%d %b %Y") if doc and doc.get("joined") else "N/A"
        today = now_ist().strftime("%Y-%m-%d")
        verified = bool(doc and doc.get("verified_date") == today) if doc else False
        refers = doc.get("refer_count", 0) if doc else 0
        s = await get_settings()
        limit = "∞" if prem else str(s.get("daily_limit", 10))
        exp = await get_premium_expiry(uid)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        await query.message.edit(
            f"📊 **Aapki Stats**\n\n"
            f"👤 {query.from_user.mention}\n"
            f"🆔 `{uid}`\n"
            f"📅 Joined: {joined}\n"
            f"💎 Premium: {'✅ — ' + exp_str if prem else '❌'}\n"
            f"✅ Aaj Verified: {'Haan' if verified or prem else 'Nahi'}\n"
            f"📥 Aaj Downloads: {count}/{limit}\n"
            f"🔗 Total Refers: {refers}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    elif data == "back_main":
        me = await client.get_me()
        miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
        buttons = [
            [
                InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
                InlineKeyboardButton("💎 Premium", callback_data="show_premium")
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="help"),
                InlineKeyboardButton("📊 My Stats", callback_data="my_stats")
            ],
            [InlineKeyboardButton("🔗 Refer & Earn", callback_data="refer_info")]
        ]
        if miniapp_url:
            buttons.append([InlineKeyboardButton("🌐 Mini App", web_app=WebAppInfo(url=miniapp_url))])
        await query.message.edit(
            "🗂 **AsBhai Drop Bot**\n\nGroup mein file naam type karo!",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    await query.answer()

# ═══════════════════════════════════════
#  PAYMENT APPROVAL CALLBACKS
# ═══════════════════════════════════════
@bot.on_callback_query(filters.regex(r"^pay_approve_"))
async def pay_approve(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    parts = query.data.split("_")
    # pay_approve_PAYID_USERID_DAYS
    pay_id = parts[2]
    user_id = int(parts[3])
    days = int(parts[4])
    from bson import ObjectId
    pay_doc = await payments_col.find_one({"_id": ObjectId(pay_id)})
    await payments_col.update_one(
        {"_id": ObjectId(pay_id)},
        {"$set": {"status": "approved", "approved_at": now(), "approved_by": query.from_user.id}}
    )
    # Group premium check
    if pay_doc and pay_doc.get("group_id"):
        g_id = int(pay_doc["group_id"])
        expiry = now() + timedelta(days=days)
        await group_prem_col.update_one(
            {"chat_id": g_id},
            {"$set": {"chat_id": g_id, "owner_id": user_id, "status": "approved",
                      "expiry": expiry, "days": days, "approved_at": now()}},
            upsert=True
        )
        me = await client.get_me()
        try:
            await client.send_message(
                user_id,
                f"🎉 **Group Premium Approved!** 🏆\n\n"
                f"🏘 Group ID: `{g_id}`\n"
                f"📅 **{days} din** ke liye active!\n"
                f"⏰ Expiry: {expiry.astimezone(IST).strftime('%d %b %Y')}\n\n"
                f"**Ab kya karein:**\n"
                f"1️⃣ Apne group mein @{me.username} add karo\n"
                f"2️⃣ Group mein apni shortlink lagao:\n"
                f"   `/gshortlink YOUR_API_KEY modijiurl.com 6 ModiJi`\n"
                f"3️⃣ Users search karenge to aapki shortlink aayegi\n"
                f"4️⃣ Shortlink earnings aapke account mein! 💸\n\n"
                f"Help ke liye: @asbhaibsr"
            )
        except: pass
        await query.message.edit_reply_markup(None)
        await query.message.reply(f"✅ Group `{g_id}` ko **{days} din** Group Premium diya!")
        await query.answer("✅ Group Premium Approved!", show_alert=False)
        return
    # Normal user premium
    await add_premium(user_id, days)
    exp = await get_premium_expiry(user_id)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
    try:
        await client.send_message(
            user_id,
            f"🎉 **Premium Approved!**\n\n"
            f"**{days} din** ka Premium activate ho gaya! 💎\n"
            f"Expiry: **{exp_str}**\n\n"
            f"Ab force join nahi, shortlink nahi!\n"
            f"Group mein jaake search karo! 🗂"
        )
    except: pass
    await query.message.edit_reply_markup(None)
    await query.message.reply(f"✅ `{user_id}` ko **{days} din** Premium diya! Expiry: {exp_str}")
    await query.answer("✅ Approved!", show_alert=False)

@bot.on_callback_query(filters.regex(r"^pay_reject_"))
async def pay_reject(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    parts = query.data.split("_")
    pay_id = parts[2]
    user_id = int(parts[3])
    from bson import ObjectId
    await payments_col.update_one(
        {"_id": ObjectId(pay_id)},
        {"$set": {"status": "rejected", "rejected_at": now()}}
    )
    pay_doc_r = await payments_col.find_one({"_id": ObjectId(pay_id)})
    is_grp = pay_doc_r and pay_doc_r.get("is_group")
    g_id_r = pay_doc_r.get("group_id") if pay_doc_r else None
    try:
        rej_msg = (
            f"❌ **Group Premium Rejected**\n\n"
            f"Group ID: `{g_id_r}`\n"
            f"Sahi details ke saath dobara submit karo."
        ) if is_grp else (
            f"❌ **Payment Rejected**\n\n"
            f"Aapki payment reject ho gayi.\n"
            f"Sahi transaction ID ke saath dobara submit karo\n"
            f"ya @asbhaibsr se contact karo."
        )
        await client.send_message(user_id, rej_msg)
    except: pass
    await query.message.edit_reply_markup(None)
    label = f"Group `{g_id_r}`" if is_grp else f"`{user_id}`"
    await query.message.reply(f"❌ {label} ki payment reject ki.")
    await query.answer("❌ Rejected!", show_alert=False)

# ═══════════════════════════════════════
#  OWNER COMMANDS
# ═══════════════════════════════════════
@bot.on_message(filters.command("addpremium") & filters.user(ADMINS))
async def addprem(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/addpremium user_id [days]`")
        return
    try:
        uid = int(args[1])
        days = int(args[2]) if len(args) > 2 else 30
        await add_premium(uid, days)
        await message.reply(f"✅ `{uid}` ko **{days} din** Premium!")
        try:
            await client.send_message(
                uid,
                f"🎉 **Premium Activated!**\n\n"
                f"**{days} din** ka Premium! 💎\n"
                f"Force join nahi, unlimited files, stream karo!"
            )
        except: pass
    except ValueError:
        await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("removepremium") & filters.user(ADMINS))
async def remprem(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/removepremium user_id`")
        return
    try:
        uid = int(args[1])
        await remove_premium(uid)
        await message.reply(f"✅ `{uid}` ka Premium hata diya!")
    except ValueError:
        await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("ban") & filters.user(ADMINS))
async def ban_cmd(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/ban user_id [reason]`")
        return
    try:
        uid = int(args[1])
        reason = " ".join(args[2:]) if len(args) > 2 else "No reason"
        await ban_user(uid, reason)
        await message.reply(f"🚫 `{uid}` banned!\nReason: {reason}")
    except ValueError:
        await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("unban") & filters.user(ADMINS))
async def unban_cmd(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/unban user_id`")
        return
    try:
        uid = int(args[1])
        await unban_user(uid)
        await message.reply(f"✅ `{uid}` unbanned!")
    except ValueError:
        await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("setdelete") & filters.user(ADMINS) & filters.private)
async def setdel(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"⏱ Auto Delete: **{'ON' if s.get('auto_delete') else 'OFF'}** "
            f"| {s.get('auto_delete_time',300)//60} min\n"
            f"Usage: `/setdelete <min>` ya `/setdelete on/off`"
        )
        return
    val = args[1].lower()
    if val in ["on","off"]:
        await update_setting("auto_delete", val == "on")
        await message.reply(f"✅ Auto delete **{val.upper()}**!")
    else:
        try:
            await update_setting("auto_delete_time", int(val) * 60)
            await message.reply(f"✅ Auto delete: **{val} min**")
        except:
            await message.reply("❌ Number likhein.")

@bot.on_message(filters.command("forcesub") & filters.user(ADMINS) & filters.private)
async def fsub_simple(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"📢 Force Sub: **{'ON' if s.get('force_sub') else 'OFF'}**\n"
            f"Usage: `/forcesub on/off`\n\n"
            f"Advanced management: `/fsub`"
        )
        return
    val = args[1].lower()
    await update_setting("force_sub", val == "on")
    await message.reply(f"✅ Force Sub **{val.upper()}**!")

@bot.on_message(filters.command("shortlink") & filters.user(ADMINS) & filters.private)
async def sl_toggle(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"🔗 Shortlink: **{'ON' if s.get('shortlink_enabled') else 'OFF'}**\n"
            f"Usage: `/shortlink on/off`"
        )
        return
    val = args[1].lower()
    await update_setting("shortlink_enabled", val == "on")
    await message.reply(f"✅ Shortlink **{val.upper()}**!")

@bot.on_message(filters.command("addshortlink") & filters.user(ADMINS))
async def add_shortlink_cmd(client, message: Message):
    """
    Usage: /addshortlink <api_key> <website_url> <hours> [label]
    Example: /addshortlink abc123 modijiurl.com 6 ModiJi
    """
    args = message.command
    if len(args) < 4:
        await message.reply(
            "📌 **Shortlink Add Karo**\n\n"
            "Usage:\n"
            "`/addshortlink <api_key> <website_url> <hours> [label]`\n\n"
            "Examples:\n"
            "`/addshortlink abc123 modijiurl.com 6 ModiJi`\n"
            "`/addshortlink xyz789 shrinkme.io 12 ShrinkMe`\n\n"
            "• **hours** = kitne ghante baad dobara verify karna hoga\n"
            "• **label** = optional naam (default: website URL)"
        )
        return
    api_key = args[1]
    api_url  = args[2].strip("/").replace("https://","").replace("http://","")
    try:
        hours = int(args[3])
        if hours < 1 or hours > 720:
            await message.reply("❌ Hours 1 se 720 ke beech hona chahiye!")
            return
    except:
        await message.reply("❌ Hours valid number hona chahiye (jaise 6, 12, 24)")
        return
    label = " ".join(args[4:]) if len(args) > 4 else api_url

    # Order — existing count se zyada
    count = await shortlinks_col.count_documents({})
    doc = {
        "api_key": api_key,
        "url": api_url,
        "hours": hours,
        "label": label,
        "active": True,
        "order": count + 1,
        "added_at": now(),
        "added_by": message.from_user.id
    }
    result = await shortlinks_col.insert_one(doc)
    # Test shortlink
    test_result = await make_shortlink_with("https://t.me/test", api_key, api_url)
    test_status = "✅ Test OK" if test_result != "https://t.me/test" else "⚠️ Test fail (API check karo)"
    await message.reply(
        f"✅ **Shortlink Add Ho Gaya!**\n\n"
        f"🏷 Label: **{label}**\n"
        f"🌐 URL: `{api_url}`\n"
        f"🔑 API Key: `{api_key[:8]}...`\n"
        f"⏰ Interval: **{hours} ghante**\n"
        f"📊 Order: #{count+1}\n"
        f"🧪 {test_status}\n\n"
        f"Shortlinks list: /shortlinks"
    )

@bot.on_message(filters.command("shortlinks") & filters.user(ADMINS))
async def list_shortlinks_cmd(client, message: Message):
    """All shortlinks list with remove buttons"""
    links = []
    async for doc in shortlinks_col.find({}).sort("order", 1):
        links.append(doc)

    if not links:
        await message.reply(
            "📭 **Koi Shortlink Add Nahi Hai**\n\n"
            "Add karo:\n"
            "`/addshortlink <api_key> <url> <hours> [label]`"
        )
        return

    text = f"🔗 **Shortlinks ({len(links)})**\n\n"
    buttons = []
    for i, sl in enumerate(links):
        active_icon = "✅" if sl.get("active") else "❌"
        text += (
            f"{active_icon} **{i+1}. {sl.get('label', sl['url'])}**\n"
            f"   🌐 `{sl['url']}`\n"
            f"   ⏰ Interval: **{sl.get('hours', 24)} ghante**\n"
            f"   📊 Order: #{sl.get('order', i+1)}\n\n"
        )
        sl_id = str(sl["_id"])
        row = [
            InlineKeyboardButton(
                f"{'🔴 Disable' if sl.get('active') else '🟢 Enable'} #{i+1}",
                callback_data=f"sl_toggle_{sl_id}"
            ),
            InlineKeyboardButton(
                f"🗑 Remove #{i+1}",
                callback_data=f"sl_remove_{sl_id}"
            )
        ]
        buttons.append(row)
    buttons.append([InlineKeyboardButton("➕ Add New", callback_data="sl_help_add")])

    await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_message(filters.command("removeshortlink") & filters.user(ADMINS))
async def remove_shortlink_cmd(client, message: Message):
    """
    Usage: /removeshortlink <order_number>
    Example: /removeshortlink 2
    """
    args = message.command
    if len(args) < 2:
        await message.reply(
            "Usage: `/removeshortlink <number>`\n"
            "Example: `/removeshortlink 2`\n\n"
            "List dekhne ke liye: /shortlinks"
        )
        return
    try:
        order_num = int(args[1])
    except:
        await message.reply("❌ Valid number do. /shortlinks se list dekho.")
        return

    links = []
    async for doc in shortlinks_col.find({}).sort("order", 1):
        links.append(doc)

    if order_num < 1 or order_num > len(links):
        await message.reply(f"❌ {order_num} nahi mila. Total {len(links)} shortlinks hain.")
        return

    sl = links[order_num - 1]
    await shortlinks_col.delete_one({"_id": sl["_id"]})
    # Reorder remaining
    remaining = [x for x in links if x["_id"] != sl["_id"]]
    for i, doc in enumerate(remaining):
        await shortlinks_col.update_one({"_id": doc["_id"]}, {"$set": {"order": i+1}})

    await message.reply(
        f"✅ **Shortlink Remove Ho Gaya!**\n\n"
        f"🏷 `{sl.get('label', sl['url'])}`\n\n"
        f"Baaki: {len(remaining)} shortlinks\n"
        f"List: /shortlinks"
    )

@bot.on_callback_query(filters.regex(r"^sl_toggle_"))
async def sl_toggle_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    from bson import ObjectId
    sl_id = query.data.replace("sl_toggle_", "")
    sl = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
    if not sl:
        await query.answer("❌ Shortlink nahi mila!", show_alert=True); return
    new_val = not sl.get("active", True)
    await shortlinks_col.update_one({"_id": ObjectId(sl_id)}, {"$set": {"active": new_val}})
    status = "enabled" if new_val else "disabled"
    await query.answer(f"{'✅ Enabled' if new_val else '🔴 Disabled'}", show_alert=False)
    # Refresh list
    links = []
    async for doc in shortlinks_col.find({}).sort("order", 1):
        links.append(doc)
    text = f"🔗 **Shortlinks ({len(links)})**\n\n"
    buttons = []
    for i, s in enumerate(links):
        ai = "✅" if s.get("active") else "❌"
        text += f"{ai} **{i+1}. {s.get('label', s['url'])}** — ⏰ {s.get('hours',24)}h\n"
        sid = str(s["_id"])
        buttons.append([
            InlineKeyboardButton(f"{'🔴 Disable' if s.get('active') else '🟢 Enable'} #{i+1}", callback_data=f"sl_toggle_{sid}"),
            InlineKeyboardButton(f"🗑 Remove #{i+1}", callback_data=f"sl_remove_{sid}")
        ])
    buttons.append([InlineKeyboardButton("➕ Add New", callback_data="sl_help_add")])
    try:
        await query.message.edit(text, reply_markup=InlineKeyboardMarkup(buttons))
    except: pass

@bot.on_callback_query(filters.regex(r"^sl_remove_"))
async def sl_remove_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    from bson import ObjectId
    sl_id = query.data.replace("sl_remove_", "")
    sl = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
    if not sl:
        await query.answer("❌ Shortlink nahi mila!", show_alert=True); return
    await shortlinks_col.delete_one({"_id": ObjectId(sl_id)})
    # Reorder
    remaining = []
    async for doc in shortlinks_col.find({}).sort("order", 1):
        remaining.append(doc)
    for i, doc in enumerate(remaining):
        await shortlinks_col.update_one({"_id": doc["_id"]}, {"$set": {"order": i+1}})
    await query.answer(f"✅ Removed!", show_alert=False)
    # Refresh
    links = remaining
    if not links:
        await query.message.edit("📭 **Koi Shortlink Nahi**\n\nAdd karo:\n`/addshortlink <api> <url> <hours> [label]`")
        return
    text = f"🔗 **Shortlinks ({len(links)})**\n\n"
    buttons = []
    for i, s in enumerate(links):
        ai = "✅" if s.get("active") else "❌"
        text += f"{ai} **{i+1}. {s.get('label', s['url'])}** — ⏰ {s.get('hours',24)}h\n"
        sid = str(s["_id"])
        buttons.append([
            InlineKeyboardButton(f"{'🔴 Disable' if s.get('active') else '🟢 Enable'} #{i+1}", callback_data=f"sl_toggle_{sid}"),
            InlineKeyboardButton(f"🗑 Remove #{i+1}", callback_data=f"sl_remove_{sid}")
        ])
    buttons.append([InlineKeyboardButton("➕ Add New", callback_data="sl_help_add")])
    await query.message.edit(text, reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^sl_help_add$"))
async def sl_help_add_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    await query.answer()
    await query.message.reply(
        "➕ **Naya Shortlink Add Karo**\n\n"
        "`/addshortlink <api_key> <website_url> <hours> [label]`\n\n"
        "Example:\n"
        "`/addshortlink abc123 modijiurl.com 6 ModiJi`\n"
        "`/addshortlink xyz789 shrinkme.io 24 ShrinkMe`"
    )

@bot.on_message(filters.command("setlimit") & filters.user(ADMINS) & filters.private)
async def setlimit(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"📊 Daily Limit: **{s.get('daily_limit',10)}**\nUsage: `/setlimit <n>`")
        return
    try:
        await update_setting("daily_limit", int(args[1]))
        await message.reply(f"✅ Limit: **{args[1]}**")
    except:
        await message.reply("❌ Number likhein.")

@bot.on_message(filters.command("setresults") & filters.user(ADMINS) & filters.private)
async def setresults(client, message: Message):
    args = message.command
    if len(args) < 3:
        await message.reply("Usage: `/setresults <free> <premium>`")
        return
    try:
        await update_setting("free_results", int(args[1]))
        await update_setting("premium_results", int(args[2]))
        await message.reply(f"✅ Free: **{args[1]}** | Premium: **{args[2]}**")
    except:
        await message.reply("❌ Numbers likhein.")

@bot.on_message(filters.command("maintenance") & filters.user(ADMINS) & filters.private)
async def maint(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"🔧 Maintenance: **{'ON' if s.get('maintenance') else 'OFF'}**\n"
            f"Usage: `/maintenance on/off`"
        )
        return
    val = args[1].lower()
    await update_setting("maintenance", val == "on")
    await message.reply(f"🔧 Maintenance **{val.upper()}**!")

@bot.on_message(filters.command("settings") & filters.user(ADMINS))
async def show_settings(client, message: Message):
    s = await get_settings()
    channels = s.get("fsub_channels", [])
    groups_list = s.get("fsub_groups", [])
    fsub_text = ""
    for ch in channels:
        fsub_text += f"\n  📢 {ch.get('title')} `{ch.get('id')}`"
    for g in groups_list:
        fsub_text += f"\n  🏘 {g.get('title')} `{g.get('id')}`"
    if not fsub_text:
        fsub_text = "\n  None (default channel use hoga)"
    await message.reply(
        f"⚙️ **Global Settings**\n\n"
        f"Auto Delete: {'ON' if s.get('auto_delete') else 'OFF'} ({s.get('auto_delete_time',300)//60} min)\n"
        f"Force Sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"Daily Limit: {s.get('daily_limit',10)}\n"
        f"Free Results: {s.get('free_results',5)}\n"
        f"Premium Results: {s.get('premium_results',10)}\n"
        f"Maintenance: {'ON' if s.get('maintenance') else 'OFF'}\n"
        f"Request Mode: {'ON' if s.get('request_mode') else 'OFF'}\n"
        f"Force Sub Channels/Groups:{fsub_text}"
    )

@bot.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats(client, message: Message):
    u = await users_col.count_documents({})
    g = await groups_col.count_documents({})
    p = await premium_col.count_documents({})
    b = await banned_col.count_documents({})
    r = await requests_col.count_documents({})
    total_refers = await refers_col.count_documents({})
    today = now_ist().strftime("%Y-%m-%d")
    active = await users_col.count_documents({"date": today, "count": {"$gt": 0}})
    verified = await users_col.count_documents({"verified_date": today})
    await message.reply(
        f"📊 **Stats**\n\n"
        f"👥 Users: **{u}**\n"
        f"🏘 Groups: **{g}**\n"
        f"💎 Premium: **{p}**\n"
        f"🚫 Banned: **{b}**\n"
        f"📩 Requests: **{r}**\n"
        f"🔗 Total Refers: **{total_refers}**\n"
        f"📥 Aaj Downloads: **{active}**\n"
        f"✅ Aaj Verified: **{verified}**\n\n"
        f"🕐 {now_ist().strftime('%d %b %Y %H:%M')} IST"
    )

@bot.on_message(filters.command("requests") & filters.user(ADMINS))
async def show_requests(client, message: Message):
    total = await requests_col.count_documents({})
    if total == 0:
        await message.reply("📩 Koi request nahi!")
        return
    text = f"📩 **Requests ({total})**\n\n"
    async for req in requests_col.find({}).sort("time", -1).limit(10):
        text += f"• `{req['request']}` — {req.get('name','?')}\n"
    if total > 10:
        text += f"\n...aur {total-10} hain."
    await message.reply(text)

@bot.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.private)
async def broadcast(client, message: Message):
    args = message.command
    if len(args) < 3:
        await message.reply(
            "Usage:\n"
            "`/broadcast users <msg>`\n"
            "`/broadcast groups <msg>`\n"
            "`/broadcast all <msg>`"
        )
        return
    target = args[1].lower()
    text = " ".join(args[2:])
    sm = await message.reply("📡 Shuru...")
    total = done = failed = blocked = 0
    if target in ["users","all"]:
        async for doc in users_col.find({}):
            uid = doc.get("user_id")
            if not uid: continue
            total += 1
            try:
                await client.send_message(uid, text); done += 1
                await asyncio.sleep(0.05)
            except (UserIsBlocked, InputUserDeactivated):
                blocked += 1; await users_col.delete_one({"user_id": uid})
            except FloodWait as e: await asyncio.sleep(e.value)
            except: failed += 1
    if target in ["groups","all"]:
        async for doc in groups_col.find({}):
            cid = doc.get("chat_id")
            if not cid: continue
            total += 1
            try:
                await client.send_message(cid, text); done += 1
                await asyncio.sleep(0.1)
            except (ChatWriteForbidden, PeerIdInvalid):
                failed += 1; await groups_col.delete_one({"chat_id": cid})
            except FloodWait as e: await asyncio.sleep(e.value)
            except: failed += 1
    await sm.edit(f"📡 **Done!**\nTotal:{total} ✅{done} ❌{failed} 🚫{blocked}")

@bot.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="back_main")]]
    if miniapp_url:
        buttons.insert(0, [InlineKeyboardButton("🌐 Mini App", web_app=WebAppInfo(url=miniapp_url))])
    await message.reply(
        "📖 **Bot Guide**\n\n"
        "**Kaise use karein:**\n"
        "1️⃣ Channel join karo\n"
        "2️⃣ Daily 1 baar verify karo (shortlink)\n"
        "3️⃣ Group mein naam type karo\n"
        "4️⃣ Button milega → click → PM mein file! 📥\n\n"
        "⚠️ File kuch der baad delete hogi — save kar lo!\n\n"
        "💎 **Premium** = No verify + 10 results + stream!\n"
        "🔗 **10 Refer** = 15 din free premium!\n\n"
        "/premium | /mystats | /referlink | /request <naam>",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_message(filters.command("premium"))
async def premium_info(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
    buttons = []
    if not prem:
        if miniapp_url:
            buttons.append([InlineKeyboardButton("🌐 Mini App mein Buy Karo", web_app=WebAppInfo(url=miniapp_url))])
        buttons.append([InlineKeyboardButton("💰 Buy Premium", callback_data="buy_premium")])
    buttons.append([InlineKeyboardButton("🔗 Refer & Earn (Free)", callback_data="refer_info")])
    await message.reply(
        f"💎 **Premium**\n\n"
        f"Status: {'✅ Active — ' + exp_str if prem else '❌ Nahi'}\n\n"
        f"• 🔓 Force join nahi\n"
        f"• 🔗 Shortlink nahi\n"
        f"• 📦 10 results\n"
        f"• ∞ Unlimited\n"
        f"• ▶️ Stream\n\n"
        f"₹50/10din | ₹150/30din | ₹200/60din\n"
        f"₹500/150din | ₹800/1saal\n\n"
        f"🆓 **10 Refer = 15 din FREE!**",
        reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
    )

@bot.on_message(filters.command("mystats"))
async def mystats(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    count = await get_daily_count(uid)
    doc = await users_col.find_one({"user_id": uid})
    joined = make_aware(doc["joined"]).astimezone(IST).strftime("%d %b %Y") if doc and doc.get("joined") else "N/A"
    today = now_ist().strftime("%Y-%m-%d")
    verified = bool(doc and doc.get("verified_date") == today) if doc else False
    refers = doc.get("refer_count", 0) if doc else 0
    s = await get_settings()
    limit = "∞" if prem else str(s.get("daily_limit", 10))
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
    await message.reply(
        f"📊 **Aapki Stats**\n\n"
        f"👤 {message.from_user.mention}\n"
        f"🆔 `{uid}`\n"
        f"📅 Joined: {joined}\n"
        f"💎 Premium: {'✅ — ' + exp_str if prem else '❌'}\n"
        f"✅ Aaj Verified: {'Haan' if verified or prem else 'Nahi'}\n"
        f"📥 Aaj Downloads: {count}/{limit}\n"
        f"🔗 Total Refers: {refers}"
    )

@bot.on_message(filters.command("ping") & filters.user(ADMINS))
async def ping(client, message: Message):
    t = time.time()
    m = await message.reply("🏓")
    ms = round((time.time() - t) * 1000)
    await m.edit(f"🏓 Pong! `{ms}ms`")

# ═══════════════════════════════════════
#  /SETCOMMANDS — Auto set bot commands
# ═══════════════════════════════════════
@bot.on_message(filters.command("setcommands") & filters.user(ADMINS))
async def set_commands(client, message: Message):
    from pyrogram.types import BotCommand
    commands = [
        BotCommand("start",       "Bot shuru karo"),
        BotCommand("help",        "Help aur guide dekho"),
        BotCommand("premium",     "Premium plans aur status"),
        BotCommand("mystats",     "Apni stats dekho"),
        BotCommand("referlink",   "Refer link — 10 refer = 15 din free"),
        BotCommand("request",     "File request karo"),
    ]
    await client.set_bot_commands(commands)
    cmds_text = "\n".join(f"/{c.command} — {c.description}" for c in commands)
    await message.reply(f"✅ **Bot Commands Set Ho Gayi!**\n\n{cmds_text}")

# ═══════════════════════════════════════
#  GROUP PREMIUM — Group owner commands
# ═══════════════════════════════════════
async def is_group_premium(chat_id):
    """Group ka premium active hai?"""
    doc = await group_prem_col.find_one({"chat_id": chat_id, "status": "approved"})
    if not doc: return False
    expiry = make_aware(doc.get("expiry"))
    if expiry and now() > expiry:
        await group_prem_col.update_one({"chat_id": chat_id}, {"$set": {"status": "expired"}})
        return False
    return True

@bot.on_message(filters.command("gshortlink"))
async def group_shortlink_add(client, message: Message):
    """
    Group premium users apni shortlink laga sakte hain.
    Usage: /gshortlink <api_key> <website_url> [hours] [label]
    """
    if not message.chat or message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("❌ Ye command group mein use karo!")
        return
    uid = message.from_user.id
    chat_id = message.chat.id

    # Admin check
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    if not is_admin:
        await message.reply("❌ Sirf group admins ye command use kar sakte hain!")
        return

    # Group premium check
    # Group premium check
    if not await is_group_premium(chat_id) and uid not in ADMINS:
        miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
        kb = []
        if miniapp_url:
            kb.append([InlineKeyboardButton("💰 Group Premium Lo", web_app=WebAppInfo(url=miniapp_url))])
        kb.append([InlineKeyboardButton("💬 @asbhaibsr se lo", url="https://t.me/asbhaibsr")])
        await message.reply(
            "❌ **Group Premium Nahi Hai!**\n\n"
            "Shortlink lagane ke liye Group Premium chahiye.\n"
            "💰 **₹300/mahina | ₹550/2 mahine**\n\n"
            "Mini App mein buy karo ya @asbhaibsr se contact karo.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    args = message.command
    if len(args) < 3:
        await message.reply(
            "📌 **Group Shortlink Add Karo**\n"
            "Usage:\n`/gshortlink <api_key> <website_url> [hours] [label]`\n"
            "Example:\n`/gshortlink abc123 modijiurl.com 6 ModiJi`\n"
            "List dekhne ke liye: /gshortlinks"
        )
        return

    api_key = args[1]
    api_url = args[2].strip("/").replace("https://","").replace("http://","")
    try:
        hours = int(args[3]) if len(args) > 3 else 24
    except:
        hours = 24
    label = " ".join(args[4:]) if len(args) > 4 else api_url

    count = await group_sl_col.count_documents({"chat_id": chat_id})
    await group_sl_col.insert_one({
        "chat_id": chat_id, "api_key": api_key, "url": api_url,
        "hours": hours, "label": label, "active": True,
        "order": count + 1, "added_by": uid, "added_at": now()
    })
    # Test
    test = await make_shortlink_with("https://t.me/test", api_key, api_url)
    test_ok = test != "https://t.me/test"
    await message.reply(
        f"✅ **Shortlink Add Ho Gayi!**\n"
        f"🏷 Label: **{label}**\n"
        f"🌐 URL: `{api_url}`\n"
        f"⏰ Interval: **{hours} ghante**\n"
        f"🧪 Test: {'✅ OK' if test_ok else '⚠️ Fail — API check karo'}"
    )

@bot.on_message(filters.command("gshortlinkremove"))
async def group_shortlink_remove(client, message: Message):
    """Usage: /gshortlinkremove <number>"""
    if not message.chat or message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("❌ Ye command group mein use karo!"); return
    uid = message.from_user.id
    chat_id = message.chat.id
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    if not is_admin:
        await message.reply("❌ Sirf admins use kar sakte hain!"); return

    args = message.command
    if len(args) < 2:
        # List dikho with remove buttons
        links = []
        async for doc in group_sl_col.find({"chat_id": chat_id}).sort("order", 1):
            links.append(doc)
        if not links:
            await message.reply("📭 Koi shortlink nahi hai. `/gshortlink` se add karo."); return
        text = "🔗 **Group Shortlinks:**\n"
        for i, sl in enumerate(links):
            text += f"{i+1}. **{sl.get('label')}** — ⏰ {sl.get('hours',24)}h\n"
        text += "\nHatane ke liye: `/gshortlinkremove <number>`"
        await message.reply(text)
        return

    try:
        num = int(args[1])
    except:
        await message.reply("❌ Valid number do."); return

    links = []
    async for doc in group_sl_col.find({"chat_id": chat_id}).sort("order", 1):
        links.append(doc)
    if num < 1 or num > len(links):
        await message.reply(f"❌ {num} nahi mila. Total: {len(links)}"); return
    sl = links[num-1]
    await group_sl_col.delete_one({"_id": sl["_id"]})
    await message.reply(f"✅ **{sl.get('label')}** remove ho gayi!")

@bot.on_message(filters.command("gshortlinks"))
async def group_shortlinks_list(client, message: Message):
    """Group shortlinks list"""
    if not message.chat or message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("❌ Ye command group mein use karo!"); return
    chat_id = message.chat.id
    links = []
    async for doc in group_sl_col.find({"chat_id": chat_id}).sort("order", 1):
        links.append(doc)
    if not links:
        await message.reply("📭 Koi shortlink nahi.\n`/gshortlink api url hours label` se add karo.")
        return
    text = f"🔗 **Group Shortlinks ({len(links)})**\n"
    for i, sl in enumerate(links):
        active = "✅" if sl.get("active") else "❌"
        text += f"{active} {i+1}. **{sl.get('label')}** | `{sl.get('url')}` | ⏰ {sl.get('hours',24)}h\n"
    await message.reply(text)

# ═══════════════════════════════════════
#  GROUP EVENTS
# ═══════════════════════════════════════
@bot.on_message(filters.new_chat_members)
async def on_new_member(client, message: Message):
    me = (await client.get_me()).id
    for member in message.new_chat_members:
        if member.id == me:
            await save_group(message.chat)
            try:
                if message.chat.type == enums.ChatType.SUPERGROUP:
                    invite = await client.export_chat_invite_link(message.chat.id)
                    link_text = f"\n🔗 Link: {invite}"
                else:
                    link_text = ""
            except:
                link_text = ""
            await send_log(
                f"➕ **Bot Group Mein Add Hua**\n"
                f"🏘 {message.chat.title}\n"
                f"🆔 `{message.chat.id}`"
                f"{link_text}"
            )
            await message.reply(
                f"🗂 **AsBhai Drop Bot Aa Gaya!** 🎉\n\n"
                f"Koi bhi file ka naam type karo!\n"
                f"⚙️ Settings ke liye: `/gsettings`\n\n"
                f"📢 {MAIN_CHANNEL} | 💎 /premium"
            )
        elif not member.is_bot:
            await save_user(member)
            s = await get_settings()
            msg = s.get("welcome_msg", "👋 Welcome {name}! File naam type karo 🗂")
            try:
                await message.reply(msg.replace("{name}", member.mention))
            except: pass

@bot.on_message(filters.left_chat_member)
async def on_left_member(client, message: Message):
    me = (await client.get_me()).id
    if message.left_chat_member and message.left_chat_member.id == me:
        await groups_col.delete_one({"chat_id": message.chat.id})
        await send_log(
            f"➖ **Bot Group Se Nikala Gaya**\n"
            f"🏘 {message.chat.title}\n"
            f"🆔 `{message.chat.id}`"
        )

# ═══════════════════════════════════════
#  INLINE MODE
# ═══════════════════════════════════════
@bot.on_inline_query()
async def inline_search(client, query):
    q = query.query.strip()
    if len(q) < 2: return
    from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent
    found = await do_search(q, limit=5)
    me = await client.get_me()
    items = []
    for idx, msg in enumerate(found):
        fname = get_file_name(msg)
        link = f"https://t.me/{me.username}?start=getfile_{query.from_user.id}_{msg.id}"
        items.append(InlineQueryResultArticle(
            title=fname[:60],
            description="Click karke PM mein file lo",
            input_message_content=InputTextMessageContent(
                f"🗂 **{fname}**\n\n[📥 File Lo]({link})"
            )
        ))
    if not items:
        items = [InlineQueryResultArticle(
            title=f"'{q}' nahi mila",
            description="Kuch aur try karo",
            input_message_content=InputTextMessageContent(f"❌ '{q}' nahi mila.")
        )]
    await query.answer(items, cache_time=10)

# ═══════════════════════════════════════
#  SCHEDULER — Cleanup
# ═══════════════════════════════════════
async def cleanup():
    deleted = await tokens_col.delete_many({"expiry": {"$lt": now()}})
    expired_cache = [k for k, (_, exp) in _shortlink_cache.items() if now() > exp]
    for k in expired_cache:
        del _shortlink_cache[k]
    # Search cooldown cleanup
    cur_time = asyncio.get_event_loop().time()
    expired_cd = [k for k, v in _search_cooldown.items() if cur_time > v]
    for k in expired_cd:
        del _search_cooldown[k]
    # Search locks cleanup — agar koi lock 5 min se zyada purana ho
    stale_locks = [k for k, v in _search_locks.items() if v]
    for k in stale_locks:
        _search_locks[k] = False
    if deleted.deleted_count or expired_cache or expired_cd:
        logger.info(f"Cleanup: {deleted.deleted_count} tokens, {len(expired_cache)} sl_cache, {len(expired_cd)} cooldown")

# ═══════════════════════════════════════
#  START
# ═══════════════════════════════════════
def start_bot():
    # Flask alag thread mein
    Thread(target=run_flask, daemon=True).start()
    logger.info("✅ Flask started")

    # Userbot sync start
    if userbot:
        userbot.start()
        logger.info("✅ Userbot started")
    else:
        logger.warning("⚠️ STRING_SESSION missing!")

    logger.info("🚀 AsBhai Drop Bot starting...")

    # bot.run() — Pyrogram ka sahi tarika
    # Yeh internally event loop banata hai, handlers register karta hai
    # aur hamesha idle rehta hai jab tak Ctrl+C na dabaao
    # Scheduler bot ke on_start se start hoga
    # Scheduler ko bot ke start hone ke baad thread se start karo
    def _start_scheduler_thread():
        import time
        time.sleep(4)
        try:
            loop = bot.loop
            if not loop or not loop.is_running():
                logger.error("Bot loop not running for scheduler!")
                return
            # Scheduler ko bot ke loop mein start karo
            async def _start():
                try:
                    # Cleanup job — asyncio.run_coroutine_threadsafe use karo
                    def _run_cleanup():
                        if bot.loop and bot.loop.is_running():
                            asyncio.run_coroutine_threadsafe(cleanup(), bot.loop)
                    scheduler.add_job(_run_cleanup, 'interval', hours=1)
                    scheduler.start()
                    logger.info("✅ Scheduler started")
                except Exception as e:
                    logger.error(f"Scheduler start error: {e}")
            asyncio.run_coroutine_threadsafe(_start(), loop)
        except Exception as e:
            logger.error(f"Scheduler thread error: {e}")

    Thread(target=_start_scheduler_thread, daemon=True).start()
    bot.run()

if __name__ == "__main__":
    start_bot()
