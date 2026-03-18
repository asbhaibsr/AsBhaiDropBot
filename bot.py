import os, re, time, random, string, asyncio, logging
from datetime import datetime, timedelta
from threading import Thread

import pytz, aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatWriteForbidden, PeerIdInvalid, UserNotParticipant
)
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from flask import Flask

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
ADMINS            = [OWNER_ID]
IST               = pytz.timezone("Asia/Kolkata")
UPI_ID            = "arsadsaifi8272@ibl"
PORT              = int(os.getenv("PORT", "8080"))

# Verify link cache — same user ko 5 min tak same link
_verify_cache = {}   # {user_id: (token, link, expiry)}

def now():
    return datetime.now(pytz.utc)

def now_ist():
    return datetime.now(IST)

def make_aware(dt):
    if dt is None: return None
    if dt.tzinfo is None: return pytz.utc.localize(dt)
    return dt

logger.info(f"FILE_CHANNEL={FILE_CHANNEL} | STRING={'SET' if STRING_SESSION else 'MISSING'}")

DEFAULT_SETTINGS = {
    "auto_delete": True,
    "auto_delete_time": 300,
    "force_sub": True,
    "shortlink_enabled": True,
    "daily_limit": 10,
    "premium_results": 5,
    "free_results": 1,
    "welcome_msg": "👋 Welcome {name}! Koi bhi file ka naam type karo 🗂",
    "maintenance": False,
}

# ═══════════════════════════════════════
#  FLASK
# ═══════════════════════════════════════
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "AsBhai Drop Bot ✅ @asbhaibsr", 200

@flask_app.route("/health")
def health():
    return {"status": "ok"}, 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False)

# ═══════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════
mongo_client = AsyncIOMotorClient(MONGO_URI)
db           = mongo_client["asbhaidropbot"]
users_col    = db["users"]
groups_col   = db["groups"]
premium_col  = db["premium"]
settings_col = db["settings"]
tokens_col   = db["tokens"]
requests_col = db["requests"]
banned_col   = db["banned"]

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

# Userbot sirf search ke liye — file copy nahi karega
userbot = Client(
    "asbhai_userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=STRING_SESSION,
    in_memory=True
) if STRING_SESSION else None

scheduler = AsyncIOScheduler(timezone=IST)

# ═══════════════════════════════════════
#  SETTINGS
# ═══════════════════════════════════════
async def get_settings():
    s = await settings_col.find_one({"_id": "global"})
    if not s:
        await settings_col.insert_one({"_id": "global", **DEFAULT_SETTINGS})
        return DEFAULT_SETTINGS.copy()
    return s

async def update_setting(key, value):
    await settings_col.update_one(
        {"_id": "global"}, {"$set": {key: value}}, upsert=True
    )

# ═══════════════════════════════════════
#  USER / GROUP
# ═══════════════════════════════════════
async def save_user(user):
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "last_seen": now()
        }, "$setOnInsert": {"joined": now()}},
        upsert=True
    )

async def save_group(chat):
    await groups_col.update_one(
        {"chat_id": chat.id},
        {"$set": {"chat_id": chat.id, "title": chat.title, "last_active": now()}},
        upsert=True
    )

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
    expiry = now() + timedelta(days=days)
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
#  VERIFICATION
# ═══════════════════════════════════════
async def is_verified_today(user_id):
    if await is_premium(user_id): return True
    today = now_ist().strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id})
    return bool(doc and doc.get("verified_date") == today)

async def mark_verified(user_id):
    today = now_ist().strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"verified_date": today}},
        upsert=True
    )

# ═══════════════════════════════════════
#  TOKEN — User ID bhi store hoti hai
# ═══════════════════════════════════════
async def make_token(user_id, token_type="verify"):
    # Cache check — 5 min tak same token
    if user_id in _verify_cache:
        cached_token, cached_link, cached_expiry = _verify_cache[user_id]
        if now() < cached_expiry:
            return cached_token, cached_link

    token = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    expiry = now() + timedelta(minutes=5)
    await tokens_col.insert_one({
        "token": token,
        "user_id": user_id,       # IMPORTANT: user_id store karo
        "type": token_type,
        "expiry": expiry,
        "created": now()
    })
    return token, None  # link baad mein banta hai

async def check_token(token, expected_user_id=None):
    """Token valid hai? Aur sahi user ka hai?"""
    doc = await tokens_col.find_one({"token": token})
    if not doc: return False, None
    expiry = make_aware(doc["expiry"])
    if now() > expiry: return False, None
    token_uid = doc.get("user_id")
    # Agar expected_user_id diya hai to match karo
    if expected_user_id and token_uid != expected_user_id:
        return False, None
    return True, token_uid

# ═══════════════════════════════════════
#  CHANNEL MEMBER CHECK
# ═══════════════════════════════════════
async def check_member(user_id):
    s = await get_settings()
    if not s.get("force_sub"): return True
    try:
        m = await bot.get_chat_member(FORCE_SUB_ID, user_id)
        return m.status not in [
            enums.ChatMemberStatus.BANNED,
            enums.ChatMemberStatus.LEFT
        ]
    except UserNotParticipant:
        return False
    except:
        return True

# ═══════════════════════════════════════
#  UTILS
# ═══════════════════════════════════════
def clean_text(text):
    if not text: return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r't\.me/\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    return text.strip()

async def make_shortlink(url):
    s = await get_settings()
    if not s.get("shortlink_enabled") or not SHORTLINK_API:
        return url
    try:
        api = f"https://{SHORTLINK_URL}/api?api={SHORTLINK_API}&url={url}&format=text"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(api, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    result = (await r.text()).strip()
                    if result.startswith("http"):
                        return result
    except Exception as e:
        logger.error(f"shortlink error: {e}")
    return url

async def del_later(msg, secs):
    await asyncio.sleep(secs)
    try: await msg.delete()
    except: pass

async def send_log(text):
    try:
        await bot.send_message(LOG_CHANNEL, text, disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"log failed: {e}")

# ═══════════════════════════════════════
#  SEARCH — Userbot se, sirf search
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
        logger.info(f"Search [{query}] -> {len(results)} results")
        return [m for _, m in results[:limit]]
    except Exception as e:
        logger.error(f"search error [{query}]: {e}")
        return []

# ═══════════════════════════════════════
#  FORCE SUB CHECK
# ═══════════════════════════════════════
async def force_sub_check(client, message):
    uid = message.from_user.id
    if uid in ADMINS: return True
    s = await get_settings()
    if not s.get("force_sub"): return True
    if not await check_member(uid):
        try:
            invite = await client.export_chat_invite_link(FORCE_SUB_ID)
        except:
            invite = f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel Join Karo", url=invite)],
            [InlineKeyboardButton("✅ Join Kar Liya — Check Karo", callback_data=f"checkjoin_{uid}")]
        ])
        await message.reply(
            f"⚠️ **Pehle Hamara Channel Join Karo!**\n\n"
            f"Join ke baad **✅ Join Kar Liya** button dabao.\n\n"
            f"📢 {FORCE_SUB_CHANNEL}",
            reply_markup=kb
        )
        return False
    return True

# ═══════════════════════════════════════
#  DAILY VERIFY CHECK — Cache se same link
# ═══════════════════════════════════════
async def verify_check(client, message):
    uid = message.from_user.id
    if uid in ADMINS: return True
    if await is_verified_today(uid): return True

    # Cache mein dekho — 5 min tak same link
    me = await client.get_me()
    if uid in _verify_cache:
        _, cached_link, cached_expiry = _verify_cache[uid]
        if now() < cached_expiry and cached_link:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔗 Verify Karo — 1 Click", url=cached_link)],
                [InlineKeyboardButton("💎 Premium — Kabhi Verify Nahi", callback_data="buy_premium")]
            ])
            await message.reply(
                f"🔐 **Verify Karo — 1 Baar / 24 Ghante**\n\n"
                f"👇 Link pe click → shortlink solve → verify!\n\n"
                f"💎 Premium lo aur kabhi verify mat karo!",
                reply_markup=kb
            )
            return False

    # Naya token banao
    token, _ = await make_token(uid, "shortverify")
    verify_url = f"https://t.me/{me.username}?start=sv_{uid}_{token}"
    short = await make_shortlink(verify_url)

    # Cache mein 5 min ke liye save karo
    cache_expiry = now() + timedelta(minutes=5)
    _verify_cache[uid] = (token, short, cache_expiry)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Verify Karo — 1 Click", url=short)],
        [InlineKeyboardButton("💎 Premium — Kabhi Verify Nahi", callback_data="buy_premium")]
    ])
    await message.reply(
        f"🔐 **Verify Karo — 1 Baar / 24 Ghante**\n\n"
        f"👇 Link pe click → shortlink solve → verify!\n\n"
        f"✅ Ek baar karo — poore din free!\n"
        f"💎 Premium lo aur kabhi verify mat karo!",
        reply_markup=kb
    )
    return False

# ═══════════════════════════════════════
#  SEND FILE — Bot se forward (PEER_FLOOD fix)
#  File group mein seedha aayegi
# ═══════════════════════════════════════
async def send_file_to_chat(chat_id, msg_id, requester, reply_to=None):
    """
    File FILE_CHANNEL se forward karke chat mein bhejenge.
    Bot forward karega — userbot nahi (PEER_FLOOD avoid)
    """
    try:
        s = await get_settings()
        # Bot se forward karo
        forwarded = await bot.forward_messages(
            chat_id=chat_id,
            from_chat_id=FILE_CHANNEL,
            message_ids=msg_id
        )
        await increment_daily(requester.id)

        # Auto delete warning
        if s.get("auto_delete"):
            t = s.get("auto_delete_time", 300)
            mins = t // 60
            note = await bot.send_message(
                chat_id,
                f"⚠️ {requester.mention} **dhyan do!**\n\n"
                f"📁 File **{mins} minute** baad delete ho jayegi!\n"
                f"📌 **Abhi forward kar lo** — warna chali jayegi!",
                reply_to_message_id=forwarded.id if hasattr(forwarded, 'id') else None
            )
            # Agar list aayi to pehla message lo
            fwd = forwarded[0] if isinstance(forwarded, list) else forwarded
            asyncio.create_task(del_later(fwd, t))
            asyncio.create_task(del_later(note, t))

        await send_log(
            f"📥 **File Sent**\n"
            f"👤 {requester.mention} (`{requester.id}`)\n"
            f"📁 Msg ID: `{msg_id}`\n"
            f"💎 Premium: {'Yes' if await is_premium(requester.id) else 'No'}\n"
            f"🏘 Chat: `{chat_id}`"
        )
        return True

    except Exception as e:
        logger.error(f"send_file_to_chat error: {e}")
        return False

# ═══════════════════════════════════════
#  /START
# ═══════════════════════════════════════
@bot.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    logger.info(f"/start uid={message.from_user.id} args={message.command[1] if len(message.command)>1 else ''}")
    await save_user(message.from_user)
    uid = message.from_user.id

    args = message.command[1] if len(message.command) > 1 else ""

    # sv_USERID_TOKEN — shortlink verification
    if args.startswith("sv_"):
        parts = args[3:].split("_", 1)
        if len(parts) != 2:
            await message.reply("❌ Invalid link.")
            return
        try:
            token_uid = int(parts[0])
            token = parts[1]
        except:
            await message.reply("❌ Invalid link.")
            return

        # Sirf wahi user verify ho sakta hai jiska token hai
        if uid != token_uid:
            await message.reply(
                "❌ **Yeh link aapke liye nahi hai!**\n\n"
                "Group mein jaake apna verify link lo."
            )
            return

        valid, stored_uid = await check_token(token, expected_user_id=uid)
        if valid:
            await tokens_col.delete_one({"token": token})
            # Cache bhi clear karo
            if uid in _verify_cache:
                del _verify_cache[uid]
            await mark_verified(uid)
            await message.reply(
                f"✅ **Verification Ho Gayi!** 🎉\n\n"
                f"Wah {message.from_user.mention}!\n\n"
                f"Aap **24 ghante** ke liye verify ho gaye!\n\n"
                f"📌 Ab group mein jaake file search karo! 🗂\n\n"
                f"💎 Roz verify na karna ho to: /premium"
            )
            await send_log(
                f"✅ **User Verified**\n"
                f"👤 {message.from_user.mention} (`{uid}`)\n"
                f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
            )
        else:
            await message.reply(
                "❌ **Token Expire Ho Gaya!**\n\n"
                "Group mein jaake dobara search karo\n"
                "aur naya verify link lo."
            )
        return

    # Normal /start
    me = await client.get_me()
    kb = InlineKeyboardMarkup([
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
            InlineKeyboardButton("📋 Commands", callback_data="commands")
        ]
    ])
    await message.reply(
        f"🗂 **AsBhai Drop Bot**\n\n"
        f"Namaste **{message.from_user.mention}**! 👋\n\n"
        f"Group mein koi bhi file ka naam type karo —\n"
        f"main dhundh kar **group mein hi** de dunga!\n\n"
        f"**Kaise kaam karta hai:**\n"
        f"1️⃣ Channel join karo\n"
        f"2️⃣ Har roz 1 baar verify karo\n"
        f"3️⃣ Group mein file naam type karo\n"
        f"4️⃣ File seedhi group mein aa jayegi! 🎉\n\n"
        f"💎 **Premium** = No verify + 5 results + unlimited!\n\n"
        f"💎 /premium | 📊 /mystats",
        reply_markup=kb
    )

# ═══════════════════════════════════════
#  GROUP SEARCH
# ═══════════════════════════════════════
@bot.on_message(
    filters.group & filters.text &
    ~filters.command([
        "start","help","stats","broadcast","setdelete",
        "addpremium","removepremium","forcesub","settings",
        "premium","ping","shortlink","setlimit","setresults",
        "mystats","ban","unban","maintenance","request"
    ])
)
async def search_handler(client, message: Message):
    if not message.from_user: return
    await save_user(message.from_user)
    await save_group(message.chat)

    query = message.text.strip()
    if len(query) < 2: return

    uid = message.from_user.id
    s = await get_settings()

    # Checks
    if await is_banned(uid):
        return
    if s.get("maintenance") and uid not in ADMINS:
        await message.reply("🔧 Bot maintenance pe hai. Thodi der baad try karo.")
        return
    if not await force_sub_check(client, message): return
    if not await verify_check(client, message): return

    prem = await is_premium(uid)
    if not prem:
        count = await get_daily_count(uid)
        if count >= s.get("daily_limit", 10):
            await message.reply(
                f"⚠️ {message.from_user.mention}, aaj ki limit "
                f"**{s.get('daily_limit',10)}** ho gayi!\n\n"
                f"💎 /premium lo unlimited ke liye!"
            )
            return

    wait_msg = await message.reply(f"🔍 **'{query}'** dhundh raha hoon... ⏳")

    limit = s.get("premium_results", 5) if prem else 10
    found = await do_search(query, limit=limit)

    if not found:
        await wait_msg.edit(
            f"😕 **'{query}' nahi mila**\n\n"
            f"💡 Try karo:\n"
            f"• Sirf movie/show naam likhein\n"
            f"• English mein likhein\n"
            f"• Spelling check karo\n\n"
            f"📩 Request karo: `/request {query}`\n"
            f"📢 {MAIN_CHANNEL}"
        )
        return

    # Free: 1 result, Premium: 5 result
    if not prem:
        found = found[:s.get("free_results", 1)]
    else:
        found = found[:s.get("premium_results", 5)]

    await wait_msg.delete()

    # Agar multiple results hain to ek saath buttons dikhao pehle
    if len(found) > 1:
        me = await client.get_me()
        lines = []
        buttons = []
        for idx, fmsg in enumerate(found):
            name = ""
            if fmsg.document and fmsg.document.file_name:
                name = fmsg.document.file_name
            elif fmsg.caption:
                name = clean_text(fmsg.caption)[:50]
            else:
                name = f"File #{idx+1}"
            lines.append(f"{idx+1}. 🗂 `{name}`")
            buttons.append([InlineKeyboardButton(
                f"📥 {idx+1}. {name[:30]}",
                callback_data=f"getfile_{uid}_{fmsg.id}"
            )])

        kb = InlineKeyboardMarkup(buttons)
        result_msg = await message.reply(
            f"✅ **{len(found)} results mile {message.from_user.mention}!**\n\n"
            + "\n".join(lines) +
            f"\n\n👇 Jo chahiye us pe click karo:",
            reply_markup=kb
        )
        if s.get("auto_delete"):
            asyncio.create_task(
                del_later(result_msg, s.get("auto_delete_time", 300))
            )
    else:
        # Single result — seedha forward karo
        fmsg = found[0]
        success = await send_file_to_chat(
            message.chat.id, fmsg.id, message.from_user
        )
        if not success:
            await message.reply(
                "❌ File bhejne mein problem aayi. Dobara try karo."
            )

# ═══════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, query: CallbackQuery):
    data = query.data
    uid = query.from_user.id

    # Channel join check
    if data.startswith("checkjoin_"):
        target = int(data.split("_")[1])
        if uid != target:
            await query.answer("Ye button aapke liye nahi!", show_alert=True)
            return
        if await check_member(uid):
            await query.message.delete()
            await query.answer("✅ Verified!", show_alert=False)
        else:
            await query.answer(
                "❌ Abhi channel join nahi kiya! Pehle join karo.",
                show_alert=True
            )
        return

    # File get button — sirf sahi user
    if data.startswith("getfile_"):
        parts = data.split("_")
        # getfile_USERID_MSGID
        if len(parts) != 3:
            await query.answer("Invalid!", show_alert=True)
            return
        req_uid = int(parts[1])
        msg_id = int(parts[2])

        if uid != req_uid:
            await query.answer(
                "❌ Yeh button aapke liye nahi!\nAap group mein apna search karo.",
                show_alert=True
            )
            return

        await query.answer("📥 File aa rahi hai...", show_alert=False)
        success = await send_file_to_chat(
            query.message.chat.id, msg_id, query.from_user
        )
        if not success:
            await query.message.reply("❌ File nahi aa payi. Dobara try karo.")
        # Original result message delete karo
        try:
            await query.message.delete()
        except:
            pass
        return

    if data == "need_premium":
        await query.answer("💎 Sirf Premium ke liye!\n/premium type karo.", show_alert=True)

    elif data == "show_premium":
        prem = await is_premium(uid)
        exp = await get_premium_expiry(uid)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Kharidein", callback_data="buy_premium")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"💎 **Premium Membership**\n\n"
            f"Status: {'✅ Active' if prem else '❌ Nahi'}\n"
            f"Expiry: {exp_str}\n\n"
            f"**Benefits:**\n"
            f"• 🔓 Koi daily verification nahi\n"
            f"• 📦 5 results per search\n"
            f"• ∞ Unlimited downloads\n"
            f"• ▶️ Stream & Download buttons\n\n"
            f"**Price:** ₹250 / 30 din",
            reply_markup=kb
        )

    elif data == "buy_premium":
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n"
            f"**Price:** ₹250 / 30 din\n\n"
            f"**UPI ID:** `{UPI_ID}`\n\n"
            f"1️⃣ UPI se ₹250 bhejo\n"
            f"2️⃣ Screenshot lo\n"
            f"3️⃣ @asbhaibsr ko bhejo\n"
            f"4️⃣ 1 ghante mein activate! ⚡",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📞 Contact @asbhaibsr", url="https://t.me/asbhaibsr")],
                [InlineKeyboardButton("🔙 Back", callback_data="show_premium")]
            ])
        )

    elif data == "help":
        await query.message.edit(
            "📖 **Bot Guide**\n\n"
            "**Kaise use karein:**\n"
            "1️⃣ Channel join karo\n"
            "2️⃣ Daily 1 baar verify karo\n"
            "3️⃣ Group mein naam type karo\n"
            "4️⃣ File seedhi group mein aayegi!\n\n"
            "⏳ File 5 min mein delete hogi\n"
            "📌 Forward kar lo turant!\n\n"
            "💎 Premium = No verify + unlimited!\n\n"
            "/premium /mystats /request <naam>",
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
        s = await get_settings()
        limit = "∞" if prem else str(s.get("daily_limit", 10))
        exp = await get_premium_expiry(uid)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        await query.message.edit(
            f"📊 **Aapki Stats**\n\n"
            f"👤 {query.from_user.mention}\n"
            f"🆔 `{uid}`\n"
            f"📅 Joined: {joined}\n"
            f"💎 Premium: {'✅ — ' + exp_str if prem else '❌ Nahi'}\n"
            f"✅ Aaj Verified: {'Haan' if verified or prem else 'Nahi'}\n"
            f"📥 Aaj Downloads: {count}/{limit}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    elif data == "commands":
        await query.message.edit(
            "📋 **Commands**\n\n"
            "**Users:**\n"
            "/start /premium /mystats\n"
            "/request <naam>\n\n"
            "**Owner (PM):**\n"
            "/addpremium uid [days]\n"
            "/removepremium uid\n"
            "/ban uid [reason]\n"
            "/unban uid\n"
            "/setdelete <min>/on/off\n"
            "/forcesub on/off\n"
            "/shortlink on/off\n"
            "/setlimit <n>\n"
            "/setresults <free> <prem>\n"
            "/maintenance on/off\n"
            "/broadcast users/groups/all\n"
            "/stats /requests /ping",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    elif data == "back_main":
        me = await client.get_me()
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
                InlineKeyboardButton("💎 Premium", callback_data="show_premium")
            ],
            [
                InlineKeyboardButton("ℹ️ Help", callback_data="help"),
                InlineKeyboardButton("📊 My Stats", callback_data="my_stats")
            ]
        ])
        await query.message.edit(
            "🗂 **AsBhai Drop Bot**\n\nGroup mein file naam type karo!",
            reply_markup=kb
        )

    await query.answer()

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
        await message.reply(f"✅ `{uid}` ko **{days} din** Premium diya!")
        try:
            await client.send_message(
                uid,
                f"🎉 **Premium Activated!**\n\n"
                f"**{days} din** ka Premium mila! 💎\n"
                f"Ab koi verification nahi, unlimited files!"
            )
        except: pass
        await send_log(f"💎 Premium: `{uid}` — {days} days")
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
        await message.reply(f"🚫 `{uid}` ban!\nReason: {reason}")
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
        await message.reply(f"✅ `{uid}` unban!")
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
            f"Usage: `/setdelete <minutes>` ya `/setdelete on/off`"
        )
        return
    val = args[1].lower()
    if val in ["on","off"]:
        await update_setting("auto_delete", val == "on")
        await message.reply(f"✅ Auto delete **{val.upper()}**!")
    else:
        try:
            mins = int(val)
            await update_setting("auto_delete_time", mins * 60)
            await message.reply(f"✅ Auto delete: **{mins} min**")
        except:
            await message.reply("❌ Number likhein.")

@bot.on_message(filters.command("forcesub") & filters.user(ADMINS) & filters.private)
async def fsub(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"📢 Force Sub: **{'ON' if s.get('force_sub') else 'OFF'}**\nUsage: `/forcesub on/off`")
        return
    val = args[1].lower()
    await update_setting("force_sub", val == "on")
    await message.reply(f"✅ Force Sub **{val.upper()}**!")

@bot.on_message(filters.command("shortlink") & filters.user(ADMINS) & filters.private)
async def sl_toggle(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"🔗 Shortlink: **{'ON' if s.get('shortlink_enabled') else 'OFF'}**\nUsage: `/shortlink on/off`")
        return
    val = args[1].lower()
    await update_setting("shortlink_enabled", val == "on")
    await message.reply(f"✅ Shortlink **{val.upper()}**!")

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
        await message.reply(f"🔧 Maintenance: **{'ON' if s.get('maintenance') else 'OFF'}**\nUsage: `/maintenance on/off`")
        return
    val = args[1].lower()
    await update_setting("maintenance", val == "on")
    await message.reply(f"🔧 Maintenance **{val.upper()}**!")

@bot.on_message(filters.command("settings") & filters.user(ADMINS))
async def show_settings(client, message: Message):
    s = await get_settings()
    await message.reply(
        f"⚙️ **Bot Settings**\n\n"
        f"🔄 Auto Delete: {'ON' if s.get('auto_delete') else 'OFF'} ({s.get('auto_delete_time',300)//60} min)\n"
        f"📢 Force Sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"🔗 Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"📊 Daily Limit: {s.get('daily_limit',10)}\n"
        f"📦 Free Results: {s.get('free_results',1)}\n"
        f"💎 Premium Results: {s.get('premium_results',5)}\n"
        f"🔧 Maintenance: {'ON' if s.get('maintenance') else 'OFF'}"
    )

@bot.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats(client, message: Message):
    u = await users_col.count_documents({})
    g = await groups_col.count_documents({})
    p = await premium_col.count_documents({})
    b = await banned_col.count_documents({})
    r = await requests_col.count_documents({})
    today = now_ist().strftime("%Y-%m-%d")
    active = await users_col.count_documents({"date": today, "count": {"$gt": 0}})
    verified = await users_col.count_documents({"verified_date": today})
    await message.reply(
        f"📊 **Bot Stats**\n\n"
        f"👥 Users: **{u}**\n"
        f"🏘 Groups: **{g}**\n"
        f"💎 Premium: **{p}**\n"
        f"🚫 Banned: **{b}**\n"
        f"📩 Requests: **{r}**\n"
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
        await message.reply("Usage:\n`/broadcast users <msg>`\n`/broadcast groups <msg>`\n`/broadcast all <msg>`")
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

@bot.on_message(filters.command("premium"))
async def premium_info(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
    kb = None if prem else InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Premium Kharidein (₹250/mo)", callback_data="buy_premium")]
    ])
    await message.reply(
        f"💎 **Premium**\n\n"
        f"Status: {'✅ Active — ' + exp_str if prem else '❌ Nahi'}\n\n"
        f"• 🔓 Koi verify nahi\n• 📦 5 results\n"
        f"• ∞ Unlimited\n• ▶️ Stream\n\n"
        f"₹250/30 din | UPI: `{UPI_ID}`\n"
        f"@asbhaibsr ko screenshot bhejo.",
        reply_markup=kb
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
        f"📥 Aaj Downloads: {count}/{limit}"
    )

@bot.on_message(filters.command("request"))
async def file_request(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/request Movie Name`\nExample: `/request Kalki 2898 AD Hindi`")
        return
    req_text = " ".join(args[1:])
    uid = message.from_user.id
    await requests_col.insert_one({
        "user_id": uid,
        "name": message.from_user.first_name,
        "request": req_text,
        "chat_id": message.chat.id,
        "time": now()
    })
    await message.reply(
        f"📩 **Request Submit Ho Gayi!**\n\n"
        f"📁 File: `{req_text}`\n\n"
        f"Jab file upload hogi notify kiya jayega!\n"
        f"📢 {MAIN_CHANNEL}"
    )
    await send_log(
        f"📩 **New Request**\n"
        f"👤 {message.from_user.mention} (`{uid}`)\n"
        f"📁 `{req_text}`"
    )

@bot.on_message(filters.command("ping") & filters.user(ADMINS))
async def ping(client, message: Message):
    t = time.time()
    m = await message.reply("🏓")
    ms = round((time.time() - t) * 1000)
    await m.edit(f"🏓 Pong! `{ms}ms`")

@bot.on_message(filters.new_chat_members)
async def on_new_member(client, message: Message):
    me = (await client.get_me()).id
    for member in message.new_chat_members:
        if member.id == me:
            await save_group(message.chat)
            # Group ka invite link banao
            try:
                invite = await client.export_chat_invite_link(message.chat.id)
                link_text = f"🔗 Group Link: {invite}"
            except:
                link_text = ""
            await send_log(
                f"➕ **Bot Added to Group**\n"
                f"🏘 {message.chat.title} (`{message.chat.id}`)\n"
                f"{link_text}"
            )
            await message.reply(
                f"🗂 **AsBhai Drop Bot Aa Gaya!** 🎉\n\n"
                f"Koi bhi file ka naam type karo —\n"
                f"main dhundh kar group mein hi de dunga!\n\n"
                f"📢 {MAIN_CHANNEL} | 💎 /premium"
            )
        elif not member.is_bot:
            await save_user(member)
            s = await get_settings()
            msg = s.get("welcome_msg", "👋 Welcome {name}! File ka naam type karo 🗂")
            try:
                await message.reply(msg.replace("{name}", member.mention))
            except: pass

@bot.on_inline_query()
async def inline_search(client, query):
    q = query.query.strip()
    if len(q) < 2: return
    from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent
    found = await do_search(q, limit=5)
    me = await client.get_me()
    items = []
    for idx, msg in enumerate(found):
        name = (msg.document.file_name if msg.document and msg.document.file_name
                else clean_text(msg.caption or f"File #{idx+1}")[:50])
        link = f"https://t.me/{me.username}?start=file_{msg.id}"
        items.append(InlineQueryResultArticle(
            title=name[:60],
            description="Click karke file lo",
            input_message_content=InputTextMessageContent(f"🗂 **{name}**\n\n[📥 Lo]({link})")
        ))
    if not items:
        items = [InlineQueryResultArticle(
            title=f"'{q}' nahi mila",
            description="Kuch aur try karo",
            input_message_content=InputTextMessageContent(f"❌ '{q}' nahi mila.")
        )]
    await query.answer(items, cache_time=10)

# ═══════════════════════════════════════
#  SCHEDULER — cleanup
# ═══════════════════════════════════════
async def cleanup():
    deleted = await tokens_col.delete_many({"expiry": {"$lt": now()}})
    # Cache bhi clean karo
    expired = [uid for uid, (_, _, exp) in _verify_cache.items() if now() > exp]
    for uid in expired:
        del _verify_cache[uid]
    if deleted.deleted_count:
        logger.info(f"Cleaned {deleted.deleted_count} tokens, {len(expired)} cache entries")

# ═══════════════════════════════════════
#  START
# ═══════════════════════════════════════
def start_bot():
    Thread(target=run_flask, daemon=True).start()
    logger.info("✅ Flask started")

    if userbot:
        userbot.start()
        logger.info("✅ Userbot started (search only)")
    else:
        logger.warning("⚠️ STRING_SESSION missing!")

    scheduler.add_job(
        lambda: asyncio.create_task(cleanup()),
        'interval', hours=1
    )

    logger.info("🚀 AsBhai Drop Bot starting...")
    bot.run()

if __name__ == "__main__":
    start_bot()
