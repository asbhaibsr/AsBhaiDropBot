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

def now():
    """Always timezone-aware datetime — MongoDB fix"""
    return datetime.now(pytz.utc)

def now_ist():
    return datetime.now(IST)

def make_aware(dt):
    """MongoDB se aya naive datetime ko aware banao"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return pytz.utc.localize(dt)
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
#  USER / GROUP HELPERS
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
    doc = await banned_col.find_one({"user_id": user_id})
    return bool(doc)

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
    if user_id in ADMINS:
        return True
    doc = await premium_col.find_one({"user_id": user_id})
    if not doc:
        return False
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
#  VERIFICATION (24 ghante)
# ═══════════════════════════════════════
async def is_verified_today(user_id):
    prem = await is_premium(user_id)
    if prem:
        return True
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
#  TOKEN SYSTEM
# ═══════════════════════════════════════
async def make_token(user_id, token_type="verify"):
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    await tokens_col.insert_one({
        "token": token,
        "user_id": user_id,
        "type": token_type,
        "expiry": now() + timedelta(hours=1),
        "created": now()
    })
    return token

async def check_token(token):
    doc = await tokens_col.find_one({"token": token})
    if not doc:
        return False
    expiry = make_aware(doc["expiry"])
    return now() < expiry

async def get_token_user(token):
    doc = await tokens_col.find_one({"token": token})
    return doc.get("user_id") if doc else None

# ═══════════════════════════════════════
#  CHANNEL MEMBER CHECK
# ═══════════════════════════════════════
async def check_member(user_id):
    s = await get_settings()
    if not s.get("force_sub"):
        return True
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
    if not text:
        return ""
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
    try:
        await msg.delete()
    except:
        pass

async def send_log(text):
    try:
        await bot.send_message(
            LOG_CHANNEL, text,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"log failed: {e}")

# ═══════════════════════════════════════
#  SEARCH (Userbot se)
# ═══════════════════════════════════════
async def do_search(query, limit=5):
    if not userbot:
        logger.error("Userbot not available!")
        return []

    query = query.strip()
    words = [w.lower() for w in query.split() if len(w) > 1]
    if not words:
        return []

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
                if msg.id in seen:
                    continue
                seen.add(msg.id)
                txt = ""
                if msg.caption:
                    txt += msg.caption.lower() + " "
                if msg.document and msg.document.file_name:
                    txt += msg.document.file_name.lower() + " "
                if msg.text:
                    txt += msg.text.lower() + " "
                if not txt.strip():
                    continue
                score = sum(2 for w in words if w in txt)
                if query.lower() in txt:
                    score += 10
                if score > 0:
                    results.append((score, msg))

        results.sort(key=lambda x: x[0], reverse=True)
        logger.info(f"Search [{query}] -> {len(results)} results")
        return [m for _, m in results[:limit]]

    except Exception as e:
        logger.error(f"search error [{query}]: {e}")
        return []

# ═══════════════════════════════════════
#  FORCE SUB CHECK (shortlink nahi sirf join)
# ═══════════════════════════════════════
async def force_sub_check(client, message):
    uid = message.from_user.id
    if uid in ADMINS:
        return True
    s = await get_settings()
    if not s.get("force_sub"):
        return True
    if not await check_member(uid):
        try:
            invite = await client.export_chat_invite_link(FORCE_SUB_ID)
        except:
            invite = f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel Join Karo", url=invite)],
            [InlineKeyboardButton("✅ Join Ho Gaya — Check", callback_data=f"checkjoin_{uid}")]
        ])
        await message.reply(
            f"⚠️ **Pehle Channel Join Karo!**\n\n"
            f"Channel join karne ke baad neeche\n"
            f"**✅ Join Ho Gaya** button dabao.\n\n"
            f"📢 {FORCE_SUB_CHANNEL}",
            reply_markup=kb
        )
        return False
    return True

# ═══════════════════════════════════════
#  DAILY SHORTLINK VERIFY CHECK
# ═══════════════════════════════════════
async def verify_check(client, message):
    uid = message.from_user.id
    if uid in ADMINS:
        return True
    if await is_verified_today(uid):
        return True

    # Shortlink token banao
    token = await make_token(uid, "shortverify")
    me = await client.get_me()
    verify_url = f"https://t.me/{me.username}?start=sv_{token}"
    short = await make_shortlink(verify_url)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Verify Karo — 1 Click", url=short)],
        [InlineKeyboardButton("💎 Premium Lo — Kabhi Verify Nahi", callback_data="buy_premium")]
    ])
    await message.reply(
        f"🔐 **Daily Verification Baaki Hai!**\n\n"
        f"Ek baar verify karo — **24 ghante free!** ✅\n\n"
        f"👇 Neeche link pe click karo\n"
        f"→ Shortlink solve karo\n"
        f"→ Bot se verify ho jao\n"
        f"→ File mil jayegi! 🗂\n\n"
        f"💎 **Premium lo** — kabhi verify mat karo!",
        reply_markup=kb
    )
    return False

# ═══════════════════════════════════════
#  SEND FILE — GROUP MEIN HI
# ═══════════════════════════════════════
async def send_file_to_group(client, message, msg_id):
    """File group mein hi bhejo, PM mein nahi"""
    try:
        if userbot:
            file_msg = await userbot.get_messages(FILE_CHANNEL, msg_id)
        else:
            file_msg = await client.get_messages(FILE_CHANNEL, msg_id)

        if not file_msg or not (file_msg.document or file_msg.video or
                                file_msg.audio or file_msg.photo):
            return None

        s = await get_settings()
        prem = await is_premium(message.from_user.id)

        cap = clean_text(file_msg.caption or "")
        cap = (f"🗂 **{cap}**\n\n"
               f"👤 {message.from_user.mention} ke liye\n"
               f"📢 {MAIN_CHANNEL}"
               if cap else
               f"🗂 **AsBhai Drop Bot**\n\n"
               f"👤 {message.from_user.mention} ke liye\n"
               f"📢 {MAIN_CHANNEL}")

        btns = [[
            InlineKeyboardButton(
                "▶️ Stream" if prem else "▶️ Stream (💎 Only)",
                callback_data="need_premium"
            ),
            InlineKeyboardButton(
                "📥 Download" if prem else "📥 Download (💎 Only)",
                callback_data="need_premium"
            )
        ]]

        sent = await file_msg.copy(
            message.chat.id,
            caption=cap,
            reply_markup=InlineKeyboardMarkup(btns),
            reply_to_message_id=message.id
        )

        await increment_daily(message.from_user.id)

        # Auto delete warning
        if s.get("auto_delete"):
            t = s.get("auto_delete_time", 300)
            mins = t // 60
            note = await message.reply(
                f"⚠️ {message.from_user.mention} **dhyan do!**\n\n"
                f"📁 Yeh file **{mins} minute** baad yahan se delete ho jayegi!\n"
                f"📌 **Abhi forward kar lo** kisi aur chat mein, warna file chali jayegi!"
            )
            asyncio.create_task(del_later(sent, t))
            asyncio.create_task(del_later(note, t))

        return sent

    except Exception as e:
        logger.error(f"send_file_to_group error: {e}")
        return None

# ═══════════════════════════════════════
#  /START
# ═══════════════════════════════════════
@bot.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    logger.info(f"/start uid={message.from_user.id}")
    await save_user(message.from_user)

    args = message.command[1] if len(message.command) > 1 else ""

    # Shortlink verification token
    if args.startswith("sv_"):
        token = args[3:]
        if await check_token(token):
            uid = await get_token_user(token)
            if uid:
                await tokens_col.delete_one({"token": token})
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
                await message.reply("❌ Token invalid. Group mein dubara search karo.")
        else:
            await message.reply(
                "❌ **Token Expire Ho Gaya!**\n\n"
                "Group mein jaake dobara file search karo\n"
                "aur naya verify link lo."
            )
        return

    # File request (PM mein agar koi direct link kholta hai)
    if args.startswith("file_"):
        msg_id = int(args[5:])
        uid = message.from_user.id
        prem = await is_premium(uid)
        s = await get_settings()

        if not prem:
            count = await get_daily_count(uid)
            if count >= s.get("daily_limit", 10):
                await message.reply(
                    f"⚠️ **Daily Limit Khatam!**\n"
                    f"Aaj {s.get('daily_limit',10)} files le chuke ho.\n"
                    f"💎 /premium lo unlimited ke liye!"
                )
                return

        if userbot:
            file_msg = await userbot.get_messages(FILE_CHANNEL, msg_id)
        else:
            file_msg = await client.get_messages(FILE_CHANNEL, msg_id)

        if not file_msg:
            await message.reply("❌ File nahi mili.")
            return

        cap = clean_text(file_msg.caption or "")
        cap = (f"🗂 **{cap}**\n\n📢 {MAIN_CHANNEL}"
               if cap else f"🗂 **AsBhai Drop Bot**\n\n📢 {MAIN_CHANNEL}")

        btns = [[
            InlineKeyboardButton("▶️ Stream (💎)" if not prem else "▶️ Stream", callback_data="need_premium"),
            InlineKeyboardButton("📥 Download (💎)" if not prem else "📥 Download", callback_data="need_premium")
        ]]

        sent = await file_msg.copy(
            message.chat.id, caption=cap,
            reply_markup=InlineKeyboardMarkup(btns)
        )
        await increment_daily(uid)

        if s.get("auto_delete") and not prem:
            t = s.get("auto_delete_time", 300)
            note = await message.reply(
                f"⏳ File **{t//60} min** mein delete hogi! Forward kar lo!"
            )
            asyncio.create_task(del_later(sent, t))
            asyncio.create_task(del_later(note, t))
        return

    # Normal start
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
        f"Group mein koi bhi file ka naam type karo\n"
        f"main dhundh kar group mein hi de dunga!\n\n"
        f"✅ Kaise kaam karta hai:\n"
        f"1️⃣ Channel join karo\n"
        f"2️⃣ Daily verify karo (1 baar/din)\n"
        f"3️⃣ Group mein file naam type karo\n"
        f"4️⃣ File seedhi group mein milegi!\n\n"
        f"💎 **Premium** = No verify + 5 results + unlimited!\n\n"
        f"💎 /premium | 📊 /mystats",
        reply_markup=kb
    )

# ═══════════════════════════════════════
#  GROUP SEARCH — FILE GROUP MEIN HI
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
    if not message.from_user:
        return
    await save_user(message.from_user)
    await save_group(message.chat)

    query = message.text.strip()
    if len(query) < 2:
        return

    uid = message.from_user.id

    # Ban check
    if await is_banned(uid):
        await message.reply("🚫 Aap bot use nahi kar sakte. @asbhaibsr se contact karo.")
        return

    # Maintenance check
    s = await get_settings()
    if s.get("maintenance") and uid not in ADMINS:
        await message.reply("🔧 Bot maintenance pe hai. Thodi der baad try karo.")
        return

    # Step 1: Channel join check
    if not await force_sub_check(client, message):
        return

    # Step 2: Daily verify check
    if not await verify_check(client, message):
        return

    # Step 3: Daily limit
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

    # Step 4: Search
    wait_msg = await message.reply(
        f"🔍 **'{query}'** dhundh raha hoon...\n⏳ Thoda wait karo!"
    )

    limit = s.get("premium_results", 5) if prem else 10
    found = await do_search(query, limit=limit)

    if not found:
        await wait_msg.edit(
            f"😕 **'{query}' nahi mila**\n\n"
            f"💡 Try karo:\n"
            f"• Sirf movie/show naam likhein\n"
            f"• English mein likhein\n"
            f"• Spelling check karo\n\n"
            f"📩 Request karo: /request {query}\n"
            f"📢 {MAIN_CHANNEL}"
        )
        return

    if not prem:
        found = found[:s.get("free_results", 1)]

    await wait_msg.delete()

    # File seedhi group mein bhejo (shortlink nahi)
    for fmsg in found:
        sent = await send_file_to_group(client, message, fmsg.id)
        if not sent:
            # Fallback: button de do
            me = await client.get_me()
            name = ""
            if fmsg.document and fmsg.document.file_name:
                name = fmsg.document.file_name
            elif fmsg.caption:
                name = clean_text(fmsg.caption)[:50]
            link = f"https://t.me/{me.username}?start=file_{fmsg.id}"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📥 {name[:40]} — Lo", url=link)]
            ])
            sent2 = await message.reply(
                f"✅ {message.from_user.mention} yeh lo!\n\n"
                f"🗂 **{name}**\n\n👇 Button dabao:",
                reply_markup=kb
            )
            if s.get("auto_delete"):
                asyncio.create_task(
                    del_later(sent2, s.get("auto_delete_time", 300))
                )

# ═══════════════════════════════════════
#  FILE REQUEST SYSTEM
# ═══════════════════════════════════════
@bot.on_message(filters.command("request"))
async def file_request(client, message: Message):
    if not message.from_user:
        return
    args = message.command
    if len(args) < 2:
        await message.reply(
            "Usage: `/request Movie Name`\n"
            "Example: `/request Kalki 2898 AD Hindi`"
        )
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
        f"Jab file upload hogi, aapko notify kiya jayega!\n"
        f"📢 Updates ke liye: {MAIN_CHANNEL}"
    )
    await send_log(
        f"📩 **New File Request**\n"
        f"👤 {message.from_user.mention} (`{uid}`)\n"
        f"📁 Request: `{req_text}`\n"
        f"🏘 Chat: `{message.chat.id}`"
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
        if await check_member(uid):
            await query.message.delete()
            await query.answer("✅ Verified!", show_alert=False)
            try:
                await client.send_message(
                    uid,
                    "✅ **Channel Join Ho Gaya!**\n\n"
                    "Ab group mein file ka naam type karo! 🗂"
                )
            except:
                pass
        else:
            await query.answer(
                "❌ Abhi channel join nahi kiya!\nPehle join karo.",
                show_alert=True
            )
        return

    if data == "need_premium":
        await query.answer(
            "💎 Sirf Premium users ke liye!\n/premium type karo.",
            show_alert=True
        )

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
            f"• ∞ Unlimited daily downloads\n"
            f"• ▶️ Stream & Download buttons\n"
            f"• ⚡ Fast priority access\n\n"
            f"**Price:** ₹250 / 30 din",
            reply_markup=kb
        )

    elif data == "buy_premium":
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n"
            f"**Price:** ₹250 / 30 din\n\n"
            f"**UPI ID:** `{UPI_ID}`\n\n"
            f"**Steps:**\n"
            f"1️⃣ UPI se ₹250 bhejo\n"
            f"2️⃣ Screenshot lo\n"
            f"3️⃣ @asbhaibsr ko bhejo\n"
            f"4️⃣ 1 ghante mein activate! ⚡",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📞 @asbhaibsr se Contact", url="https://t.me/asbhaibsr")],
                [InlineKeyboardButton("🔙 Back", callback_data="show_premium")]
            ])
        )

    elif data == "help":
        await query.message.edit(
            "📖 **Bot Guide**\n\n"
            "**Search kaise karte hain:**\n"
            "1️⃣ Bot ko group mein add karo\n"
            "2️⃣ Channel join karo (1 baar)\n"
            "3️⃣ Daily verify karo (1 baar/din)\n"
            "4️⃣ File ka naam type karo group mein\n"
            "5️⃣ File seedhi group mein aa jayegi!\n\n"
            "**Important:**\n"
            "⏳ File 5 min mein delete hogi\n"
            "📌 Forward kar lo turant!\n\n"
            "**Commands:**\n"
            "/premium — Premium info\n"
            "/mystats — Apni stats\n"
            "/request <naam> — File request karo\n\n"
            "💎 Premium = No verify + unlimited!",
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
            f"💎 Premium: {'✅ Active — ' + exp_str if prem else '❌ Nahi'}\n"
            f"✅ Aaj Verified: {'Haan ✅' if verified or prem else 'Nahi ❌'}\n"
            f"📥 Aaj Downloads: {count}/{limit}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )

    elif data == "commands":
        await query.message.edit(
            "📋 **Saari Commands**\n\n"
            "**User Commands:**\n"
            "/start — Bot shuru karo\n"
            "/premium — Premium info & buy\n"
            "/mystats — Apni stats\n"
            "/request <naam> — File request\n\n"
            "**Owner Commands (PM):**\n"
            "/addpremium uid [days]\n"
            "/removepremium uid\n"
            "/ban uid [reason]\n"
            "/unban uid\n"
            "/setdelete <min>/on/off\n"
            "/forcesub on/off\n"
            "/shortlink on/off\n"
            "/setlimit <n>\n"
            "/setresults <f> <p>\n"
            "/maintenance on/off\n"
            "/broadcast users/groups/all\n"
            "/stats\n"
            "/requests — Pending requests\n"
            "/ping",
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
        await message.reply("Usage: `/addpremium user_id [days]`\nDefault: 30 din")
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
                f"Aapko **{days} din** ka Premium mila! 💎\n\n"
                f"Ab koi verification nahi, unlimited files!\n"
                f"Stream & Download buttons bhi unlock! 🚀"
            )
        except:
            pass
        await send_log(f"💎 Premium added: `{uid}` — {days} days by owner")
    except ValueError:
        await message.reply("❌ Invalid user ID.")

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
        await message.reply("❌ Invalid user ID.")

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
        await message.reply(f"🚫 User `{uid}` ban ho gaya!\nReason: {reason}")
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
        await message.reply(f"✅ User `{uid}` unban ho gaya!")
    except ValueError:
        await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("maintenance") & filters.user(ADMINS) & filters.private)
async def maintenance_cmd(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        status = "ON" if s.get("maintenance") else "OFF"
        await message.reply(f"🔧 Maintenance: **{status}**\nUsage: `/maintenance on/off`")
        return
    val = args[1].lower()
    await update_setting("maintenance", val == "on")
    await message.reply(f"🔧 Maintenance **{val.upper()}**!")

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
            await message.reply(f"✅ Auto delete: **{mins} minute**")
        except:
            await message.reply("❌ Number likhein.")

@bot.on_message(filters.command("forcesub") & filters.user(ADMINS) & filters.private)
async def fsub(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"📢 Force Sub: **{'ON' if s.get('force_sub') else 'OFF'}**\n"
            f"Usage: `/forcesub on/off`"
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

@bot.on_message(filters.command("setlimit") & filters.user(ADMINS) & filters.private)
async def setlimit(client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(
            f"📊 Daily Limit: **{s.get('daily_limit',10)}**\n"
            f"Usage: `/setlimit <number>`"
        )
        return
    try:
        await update_setting("daily_limit", int(args[1]))
        await message.reply(f"✅ Limit: **{args[1]} files/day**")
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
        f"👥 Total Users: **{u}**\n"
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
        await message.reply("📩 Koi pending request nahi!")
        return
    text = f"📩 **Pending Requests ({total})**\n\n"
    async for req in requests_col.find({}).sort("time", -1).limit(10):
        text += f"• `{req['request']}` — {req.get('name','?')}\n"
    if total > 10:
        text += f"\n...aur {total-10} requests hain."
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
    sm = await message.reply("📡 Broadcast shuru...")
    total = done = failed = blocked = 0

    if target in ["users","all"]:
        async for doc in users_col.find({}):
            uid = doc.get("user_id")
            if not uid:
                continue
            total += 1
            try:
                await client.send_message(uid, text)
                done += 1
                await asyncio.sleep(0.05)
            except (UserIsBlocked, InputUserDeactivated):
                blocked += 1
                await users_col.delete_one({"user_id": uid})
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except:
                failed += 1

    if target in ["groups","all"]:
        async for doc in groups_col.find({}):
            cid = doc.get("chat_id")
            if not cid:
                continue
            total += 1
            try:
                await client.send_message(cid, text)
                done += 1
                await asyncio.sleep(0.1)
            except (ChatWriteForbidden, PeerIdInvalid):
                failed += 1
                await groups_col.delete_one({"chat_id": cid})
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except:
                failed += 1

    await sm.edit(
        f"📡 **Broadcast Done!**\n\n"
        f"Total: {total} | ✅ {done} | ❌ {failed} | 🚫 {blocked}"
    )

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
        f"💎 **Premium Membership**\n\n"
        f"Status: {'✅ Active — ' + exp_str if prem else '❌ Active Nahi'}\n\n"
        f"**Benefits:**\n"
        f"• 🔓 Koi daily verification nahi\n"
        f"• 📦 5 results per search\n"
        f"• ∞ Unlimited daily downloads\n"
        f"• ▶️ Stream & Download buttons\n"
        f"• ⚡ Fast access\n\n"
        f"**Price:** ₹250 / 30 din\n"
        f"**UPI:** `{UPI_ID}`\n\n"
        f"Payment ke baad @asbhaibsr ko screenshot bhejo.",
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
        f"💎 Premium: {'✅ — ' + exp_str if prem else '❌ Nahi'}\n"
        f"✅ Aaj Verified: {'Haan' if verified or prem else 'Nahi'}\n"
        f"📥 Aaj Downloads: {count}/{limit}"
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
            await send_log(
                f"➕ **New Group**\n"
                f"🏘 {message.chat.title} (`{message.chat.id}`)"
            )
            await message.reply(
                f"🗂 **AsBhai Drop Bot Aa Gaya!** 🎉\n\n"
                f"Koi bhi file ka naam type karo —\n"
                f"main dhundh kar group mein hi de dunga!\n\n"
                f"📢 {MAIN_CHANNEL} | 💎 /premium | ❓ /help"
            )
        elif not member.is_bot:
            await save_user(member)
            s = await get_settings()
            msg = s.get("welcome_msg", "👋 Welcome {name}! File ka naam type karo 🗂")
            try:
                await message.reply(msg.replace("{name}", member.mention))
            except:
                pass

@bot.on_inline_query()
async def inline_search(client, query):
    q = query.query.strip()
    if len(q) < 2:
        return
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
            input_message_content=InputTextMessageContent(
                f"🗂 **{name}**\n\n[📥 Lo]({link})"
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
#  SCHEDULER — Token cleanup
# ═══════════════════════════════════════
async def cleanup_tokens():
    result = await tokens_col.delete_many({"expiry": {"$lt": now()}})
    if result.deleted_count:
        logger.info(f"Cleaned {result.deleted_count} expired tokens")

# ═══════════════════════════════════════
#  START
# ═══════════════════════════════════════
def start_bot():
    Thread(target=run_flask, daemon=True).start()
    logger.info("✅ Flask started")

    if userbot:
        userbot.start()
        logger.info("✅ Userbot started")
    else:
        logger.warning("⚠️ STRING_SESSION missing! Search nahi chalega.")

    scheduler.add_job(
        lambda: asyncio.create_task(cleanup_tokens()),
        'interval', hours=6
    )

    logger.info("🚀 Starting AsBhai Drop Bot...")
    bot.run()

if __name__ == "__main__":
    start_bot()
