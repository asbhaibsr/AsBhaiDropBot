import os, re, asyncio, logging, time, random, string, threading
from datetime import datetime, timedelta
from flask import Flask
import pytz
from dotenv import load_dotenv
from pyrogram import Client, filters, enums, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatMemberUpdated
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatWriteForbidden, PeerIdInvalid, UserNotParticipant
)
from motor.motor_asyncio import AsyncIOMotorClient
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING - Saari cheezein dikhegi
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API_ID            = int(os.environ.get("API_ID", "0"))
API_HASH          = os.environ.get("API_HASH", "")
BOT_TOKEN         = os.environ.get("BOT_TOKEN", "")
MONGO_URI         = os.environ.get("MONGO_URI", "")
OWNER_ID          = int(os.environ.get("OWNER_ID", "7315805581"))
FILE_CHANNEL      = int(os.environ.get("FILE_CHANNEL", "-1002283182645"))
LOG_CHANNEL       = int(os.environ.get("LOG_CHANNEL", "-1002463804038"))
MAIN_CHANNEL      = os.environ.get("MAIN_CHANNEL", "@asbhai_bsr")
MAIN_CHANNEL_ID   = int(os.environ.get("MAIN_CHANNEL_ID", "-1002352329534"))
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "@asbhai_bsr")
FORCE_SUB_ID      = int(os.environ.get("FORCE_SUB_ID", "-1002352329534"))
SHORTLINK_API     = os.environ.get("SHORTLINK_API", "")
SHORTLINK_URL     = os.environ.get("SHORTLINK_URL", "modijiurl.com")
ADMINS            = [OWNER_ID]
IST               = pytz.timezone("Asia/Kolkata")
UPI_ID            = "arsadsaifi8272@ibl"
PORT              = int(os.environ.get("PORT", "8080"))

# Config check - log mein dikhega
logger.info("=" * 50)
logger.info("🔍 CONFIG CHECK:")
logger.info(f"  API_ID     : {'✅ Set' if API_ID else '❌ MISSING'}")
logger.info(f"  API_HASH   : {'✅ Set' if API_HASH else '❌ MISSING'}")
logger.info(f"  BOT_TOKEN  : {'✅ Set' if BOT_TOKEN else '❌ MISSING'}")
logger.info(f"  MONGO_URI  : {'✅ Set' if MONGO_URI else '❌ MISSING'}")
logger.info(f"  OWNER_ID   : {OWNER_ID}")
logger.info(f"  FILE_CH    : {FILE_CHANNEL}")
logger.info(f"  LOG_CH     : {LOG_CHANNEL}")
logger.info(f"  PORT       : {PORT}")
logger.info("=" * 50)

DEFAULT_SETTINGS = {
    "auto_delete": True,
    "auto_delete_time": 600,
    "force_sub": True,
    "shortlink_enabled": True,
    "daily_limit": 10,
    "premium_results": 5,
    "free_results": 1,
    "welcome_msg": "👋 Welcome {name}! File ka naam type karo 🗂",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FLASK - HEALTH CHECK (PEHLE CHALEGA)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
flask_app = Flask(__name__)
_bot_status = {"started": False, "username": "", "error": ""}

@flask_app.route("/")
def home():
    status = "✅ Running" if _bot_status["started"] else "⏳ Starting..."
    err = f"<p style='color:red'>Error: {_bot_status['error']}</p>" if _bot_status["error"] else ""
    return f"""
    <html><body style='font-family:sans-serif;background:#0a0a0f;color:#e0e0e0;padding:40px'>
    <h2 style='color:#e94560'>🗂 AsBhai Vault Bot</h2>
    <p>Bot Status: <b>{status}</b></p>
    <p>Username: <b>@{_bot_status['username']}</b></p>
    {err}
    <hr style='border-color:#333'>
    <p style='color:#888'>Owner: @asbhaibsr | Channel: @asbhai_bsr</p>
    </body></html>
    """

@flask_app.route("/health")
def health():
    return {"status": "ok", "bot": _bot_status["started"]}, 200

@flask_app.route("/status")
def status():
    return {
        "bot_started": _bot_status["started"],
        "username": _bot_status["username"],
        "error": _bot_status["error"],
        "time": datetime.now(IST).strftime('%d %b %Y %H:%M IST')
    }, 200

def start_flask():
    logger.info(f"🌐 Flask starting on port {PORT}...")
    import werkzeug.serving
    werkzeug.serving.make_server("0.0.0.0", PORT, flask_app).serve_forever()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    db           = mongo_client["asbhaivaultbot"]
    users_col    = db["users"]
    groups_col   = db["groups"]
    premium_col  = db["premium"]
    settings_col = db["settings"]
    logger.info("✅ MongoDB connected")
except Exception as e:
    logger.error(f"❌ MongoDB error: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PYROGRAM CLIENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = Client(
    name="asbhai_vault_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)
scheduler = AsyncIOScheduler(timezone=IST)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def get_settings():
    s = await settings_col.find_one({"_id": "global"})
    if not s:
        await settings_col.insert_one({"_id": "global", **DEFAULT_SETTINGS})
        return DEFAULT_SETTINGS.copy()
    return s

async def update_setting(key, value):
    await settings_col.update_one({"_id": "global"}, {"$set": {key: value}}, upsert=True)

async def is_premium(user_id):
    doc = await premium_col.find_one({"user_id": user_id})
    if not doc: return False
    if doc.get("expiry") and datetime.now(IST) > doc["expiry"]:
        await premium_col.delete_one({"user_id": user_id})
        return False
    return True

async def add_premium(user_id, days=30):
    expiry = datetime.now(IST) + timedelta(days=days)
    await premium_col.update_one({"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "added_on": datetime.now(IST)}}, upsert=True)

async def remove_premium(user_id):
    await premium_col.delete_one({"user_id": user_id})

async def get_daily_count(user_id):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id, "date": today})
    return doc.get("count", 0) if doc else 0

async def increment_daily_count(user_id):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    await users_col.update_one({"user_id": user_id, "date": today}, {"$inc": {"count": 1}}, upsert=True)

async def register_user(user):
    await users_col.update_one({"user_id": user.id},
        {"$set": {"user_id": user.id, "name": user.first_name,
                  "username": user.username, "joined": datetime.now(IST)}}, upsert=True)

async def register_group(chat):
    await groups_col.update_one({"chat_id": chat.id},
        {"$set": {"chat_id": chat.id, "title": chat.title, "joined": datetime.now(IST)}}, upsert=True)

def clean_caption(text):
    if not text: return ""
    text = re.sub(r'http[s]?://\S+', '', text)
    text = re.sub(r't\.me/\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    return re.sub(r'\n\s*\n', '\n', text).strip()

async def generate_shortlink(url):
    s = await get_settings()
    if not s.get("shortlink_enabled") or not SHORTLINK_API:
        return url
    try:
        api_url = f"https://{SHORTLINK_URL}/api?api={SHORTLINK_API}&url={url}&format=text"
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return (await resp.text()).strip()
    except Exception as e:
        logger.error(f"Shortlink error: {e}")
    return url

async def create_token(user_id):
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    await db["tokens"].insert_one({"token": token, "user_id": user_id,
        "expiry": datetime.now(IST) + timedelta(hours=1)})
    return token

async def verify_token(token):
    doc = await db["tokens"].find_one({"token": token})
    return bool(doc and datetime.now(IST) < doc["expiry"])

async def is_member(user_id):
    s = await get_settings()
    if not s.get("force_sub"): return True
    try:
        member = await app.get_chat_member(FORCE_SUB_ID, user_id)
        return member.status not in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]
    except UserNotParticipant:
        return False
    except:
        return True

async def auto_delete_msg(msg, delay):
    await asyncio.sleep(delay)
    try: await msg.delete()
    except: pass

async def log(text):
    try:
        await app.send_message(LOG_CHANNEL, text)
    except Exception as e:
        logger.error(f"Log send failed: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def search_files(query, limit=5):
    words = [w.lower() for w in query.split() if len(w) > 1]
    if not words: return []
    results = []
    seen = set()
    try:
        for word in words[:4]:
            async for msg in app.search_messages(FILE_CHANNEL, word, limit=20):
                if msg.id in seen: continue
                seen.add(msg.id)
                txt = ""
                if msg.caption: txt = msg.caption.lower()
                if msg.document and msg.document.file_name:
                    txt += " " + msg.document.file_name.lower()
                if msg.text: txt += " " + msg.text.lower()
                score = sum(1 for w in words if w in txt)
                if score > 0: results.append((score, msg))
        results.sort(key=lambda x: x[0], reverse=True)
        return [m for _, m in results[:limit]]
    except Exception as e:
        logger.error(f"Search error: {e}")
        return []

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FORCE SUB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def force_sub_check(client, message):
    uid = message.from_user.id
    if uid in ADMINS: return True
    s = await get_settings()
    if not s.get("force_sub"): return True
    if not await is_member(uid):
        token = await create_token(uid)
        me = await client.get_me()
        vlink = await generate_shortlink(f"https://t.me/{me.username}?start=verify_{token}")
        try:
            invite = await client.export_chat_invite_link(FORCE_SUB_ID)
        except:
            invite = f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel Join Karo", url=invite)],
            [InlineKeyboardButton("✅ Verify Karo", url=vlink)]
        ])
        await message.reply(
            f"⚠️ **Pehle Channel Join Karo!**\n\nJoin ke baad Verify dabao.\n📢 {FORCE_SUB_CHANNEL}",
            reply_markup=kb
        )
        return False
    return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SEND FILE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def send_file_to_user(client, message, msg_id_str):
    try:
        msg_id = int(msg_id_str)
        logger.info(f"Fetching file {msg_id} from channel {FILE_CHANNEL}")
        file_msg = await client.get_messages(FILE_CHANNEL, msg_id)
        if not file_msg:
            await message.reply("❌ File nahi mili.")
            return
        s = await get_settings()
        uid = message.from_user.id
        prem = await is_premium(uid)
        if not prem:
            count = await get_daily_count(uid)
            if count >= s.get("daily_limit", 10):
                await message.reply(
                    f"⚠️ **Daily Limit Khatam!**\n"
                    f"Aaj {s.get('daily_limit',10)} files le chuke ho.\n"
                    f"💎 /premium lo unlimited ke liye!"
                )
                return
        caption = clean_caption(file_msg.caption or "")
        caption = f"🗂 **{caption}**\n\n📢 {MAIN_CHANNEL}" if caption else f"🗂 **AsBhai Vault**\n\n📢 {MAIN_CHANNEL}"
        if prem:
            buttons = [[
                InlineKeyboardButton("▶️ Stream (Premium)", callback_data="need_premium"),
                InlineKeyboardButton("📥 Download (Premium)", callback_data="need_premium")
            ]]
        else:
            buttons = [[
                InlineKeyboardButton("▶️ Stream (Premium Only)", callback_data="need_premium"),
                InlineKeyboardButton("📥 Download (Premium Only)", callback_data="need_premium")
            ]]
        sent = await file_msg.copy(
            message.chat.id,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        await increment_daily_count(uid)
        if s.get("auto_delete") and not prem:
            t = s.get("auto_delete_time", 600)
            del_msg = await message.reply(f"⏳ Yeh file **{t//60} min** mein delete hogi! Abhi save karo.")
            asyncio.create_task(auto_delete_msg(sent, t))
            asyncio.create_task(auto_delete_msg(del_msg, t))
        await log(
            f"📥 **File Sent**\n"
            f"👤 {message.from_user.mention} (`{uid}`)\n"
            f"📁 File ID: `{msg_id}`\n"
            f"💎 Premium: {'✅' if prem else '❌'}\n"
            f"🕐 {datetime.now(IST).strftime('%d %b %H:%M')} IST"
        )
    except Exception as e:
        logger.error(f"send_file error: {e}")
        _bot_status["error"] = str(e)
        await message.reply(f"❌ Error: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# /START
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.on_message(filters.command("start") & (filters.group | filters.private))
async def start_cmd(client, message):
    logger.info(f"/start from {message.from_user.id} in {message.chat.id}")
    await register_user(message.from_user)
    args = message.command[1] if len(message.command) > 1 else ""

    if args.startswith("verify_"):
        token = args[7:]
        if await verify_token(token):
            await db["tokens"].delete_one({"token": token})
            await message.reply("✅ **Verification Ho Gayi!**\n\nAb group mein jaake search karo! 🗂")
        else:
            await message.reply("❌ Invalid ya expired token.")
        return

    if args.startswith("file_"):
        await send_file_to_user(client, message, args[5:])
        return

    me = await client.get_me()
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
            InlineKeyboardButton("➕ Group Add", url=f"https://t.me/{me.username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("💎 Premium", callback_data="show_premium"),
            InlineKeyboardButton("ℹ️ Help", callback_data="help")
        ]
    ])
    await message.reply(
        f"🗂 **AsBhai Vault Bot**\n\n"
        f"Namaste **{message.from_user.mention}**! 👋\n\n"
        f"Group mein file ka naam type karo — main dhundh lunga!\n\n"
        f"✨ Smart Search | 🔗 Shortlink | 💎 Premium\n\n"
        f"💎 Premium ke liye /premium",
        reply_markup=kb
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GROUP SEARCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.on_message(filters.group & filters.text & ~filters.command([
    "start","help","stats","broadcast","setdelete","addpremium",
    "removepremium","forcesub","settings","premium","ping",
    "shortlink","setlimit","setresults","mystats"
]))
async def group_search(client, message):
    if not message.from_user: return
    await register_user(message.from_user)
    await register_group(message.chat)
    query = message.text.strip()
    if len(query) < 2: return
    if not await force_sub_check(client, message): return
    uid = message.from_user.id
    prem = await is_premium(uid)
    s = await get_settings()
    if not prem:
        count = await get_daily_count(uid)
        if count >= s.get("daily_limit", 10):
            await message.reply(
                f"⚠️ {message.from_user.mention}, aaj ki limit **{s.get('daily_limit',10)}** ho gayi!\n"
                f"💎 /premium lo unlimited ke liye!"
            )
            return
    searching = await message.reply(f"🔍 Dhundh raha hoon: `{query}`...")
    results = await search_files(query, limit=s.get("premium_results", 5) if prem else 10)
    if not results:
        await searching.edit(
            f"❌ **'{query}' nahi mila!**\n\n"
            f"💡 Spelling check karo ya English mein try karo."
        )
        return
    if not prem:
        results = results[:s.get("free_results", 1)]
    me = await client.get_me()
    await searching.delete()
    for idx, file_msg in enumerate(results):
        name = ""
        if file_msg.document and file_msg.document.file_name:
            name = file_msg.document.file_name
        elif file_msg.caption:
            name = clean_caption(file_msg.caption)[:50]
        else:
            name = f"File #{idx+1}"
        link = await generate_shortlink(f"https://t.me/{me.username}?start=file_{file_msg.id}")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"📥 {name[:35]} — Lo", url=link)]])
        sent = await message.reply(
            f"✅ Mila! {message.from_user.mention}\n\n🗂 **{name}**\n\n👇 Button dabao:",
            reply_markup=kb
        )
        if s.get("auto_delete"):
            asyncio.create_task(auto_delete_msg(sent, s.get("auto_delete_time", 600)))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CALLBACKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.on_callback_query()
async def cb_handler(client, query):
    data = query.data
    uid = query.from_user.id
    if data == "need_premium":
        await query.answer("💎 Sirf Premium users ke liye!\n/premium type karo.", show_alert=True)
    elif data == "show_premium":
        prem = await is_premium(uid)
        doc = await premium_col.find_one({"user_id": uid})
        expiry = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Lo", callback_data="buy_premium")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"💎 **Premium**\n\nStatus: {'✅ Active' if prem else '❌ Nahi'}\nExpiry: {expiry}\n\n"
            f"Benefits:\n• No shortlink\n• 5 results\n• Unlimited\n• Streaming\n\n₹250/mahina",
            reply_markup=kb
        )
    elif data == "buy_premium":
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n₹250 / 30 din\n\nUPI: `{UPI_ID}`\n\n"
            f"Pay karo → Screenshot @asbhaibsr ko bhejo",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="show_premium")]])
        )
    elif data == "help":
        await query.message.edit(
            "📖 **Help**\n\n1. Group mein bot add karo\n2. File naam type karo\n"
            "3. Button dabao\n4. Shortlink solve karo\n5. File milegi!\n\n/premium /mystats",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_main")]])
        )
    elif data == "back_main":
        me = await client.get_me()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
             InlineKeyboardButton("💎 Premium", callback_data="show_premium")],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])
        await query.message.edit("🗂 **AsBhai Vault Bot**\n\nGroup mein file ka naam type karo!", reply_markup=kb)
    await query.answer()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OWNER COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.on_message(filters.command("addpremium") & filters.user(ADMINS))
async def add_prem_cmd(client, message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/addpremium user_id [days]`")
        return
    try:
        uid = int(args[1])
        days = int(args[2]) if len(args) > 2 else 30
        await add_premium(uid, days)
        await message.reply(f"✅ `{uid}` ko **{days} din** Premium diya!")
        try: await client.send_message(uid, f"🎉 Aapko **{days} din** ka Premium mila! 💎")
        except: pass
    except ValueError:
        await message.reply("❌ Invalid ID.")

@app.on_message(filters.command("removepremium") & filters.user(ADMINS))
async def rem_prem_cmd(client, message):
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

@app.on_message(filters.command("setdelete") & filters.user(ADMINS) & filters.private)
async def set_del_cmd(client, message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"⏱ Auto Delete: **{'ON' if s.get('auto_delete') else 'OFF'}** | {s.get('auto_delete_time',600)//60} min\nUsage: `/setdelete <min>` ya `on/off`")
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
        except: await message.reply("❌ Number likhein.")

@app.on_message(filters.command("forcesub") & filters.user(ADMINS) & filters.private)
async def fsub_cmd(client, message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"📢 Force Sub: **{'ON' if s.get('force_sub') else 'OFF'}**\nUsage: `/forcesub on/off`")
        return
    val = args[1].lower()
    await update_setting("force_sub", val == "on")
    await message.reply(f"✅ Force Sub **{val.upper()}**!")

@app.on_message(filters.command("shortlink") & filters.user(ADMINS) & filters.private)
async def sl_cmd(client, message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"🔗 Shortlink: **{'ON' if s.get('shortlink_enabled') else 'OFF'}**\nUsage: `/shortlink on/off`")
        return
    val = args[1].lower()
    await update_setting("shortlink_enabled", val == "on")
    await message.reply(f"✅ Shortlink **{val.upper()}**!")

@app.on_message(filters.command("setlimit") & filters.user(ADMINS) & filters.private)
async def sl2_cmd(client, message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"📊 Limit: **{s.get('daily_limit',10)}**\nUsage: `/setlimit <n>`")
        return
    try:
        await update_setting("daily_limit", int(args[1]))
        await message.reply(f"✅ Limit: **{args[1]}**")
    except: await message.reply("❌ Number likhein.")

@app.on_message(filters.command("setresults") & filters.user(ADMINS) & filters.private)
async def sr_cmd(client, message):
    args = message.command
    if len(args) < 3:
        await message.reply("Usage: `/setresults <free> <premium>`")
        return
    try:
        await update_setting("free_results", int(args[1]))
        await update_setting("premium_results", int(args[2]))
        await message.reply(f"✅ Free: **{args[1]}** | Premium: **{args[2]}**")
    except: await message.reply("❌ Numbers likhein.")

@app.on_message(filters.command("settings") & filters.user(ADMINS))
async def settings_cmd(client, message):
    s = await get_settings()
    await message.reply(
        f"⚙️ **Settings**\n\n"
        f"Auto Delete: {'ON' if s.get('auto_delete') else 'OFF'} ({s.get('auto_delete_time',600)//60} min)\n"
        f"Force Sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"Daily Limit: {s.get('daily_limit',10)}\n"
        f"Free Results: {s.get('free_results',1)}\n"
        f"Premium Results: {s.get('premium_results',5)}"
    )

@app.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats_cmd(client, message):
    u = await users_col.count_documents({})
    g = await groups_col.count_documents({})
    p = await premium_col.count_documents({})
    today = datetime.now(IST).strftime("%Y-%m-%d")
    active = await users_col.count_documents({"date": today, "count": {"$gt": 0}})
    await message.reply(
        f"📊 **Stats**\n\n"
        f"👥 Users: **{u}**\n🏘 Groups: **{g}**\n"
        f"💎 Premium: **{p}**\n📥 Aaj Active: **{active}**\n\n"
        f"🕐 {datetime.now(IST).strftime('%d %b %Y %H:%M')} IST"
    )

@app.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.private)
async def bc_cmd(client, message):
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

@app.on_message(filters.command("premium"))
async def premium_cmd(client, message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    doc = await premium_col.find_one({"user_id": uid})
    expiry = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
    kb = None if prem else InlineKeyboardMarkup([[InlineKeyboardButton("💰 Premium Lo (₹250)", callback_data="buy_premium")]])
    await message.reply(
        f"💎 **Premium**\n\nStatus: {'✅ Active — ' + expiry if prem else '❌ Nahi'}\n\n"
        f"Benefits:\n• 🔓 No shortlink\n• 📦 5 results\n• ∞ Unlimited\n• ▶️ Stream\n\n"
        f"₹250/30 din | UPI: `{UPI_ID}`\n@asbhaibsr ko screenshot bhejo",
        reply_markup=kb
    )

@app.on_message(filters.command("mystats"))
async def mystats_cmd(client, message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    today_count = await get_daily_count(uid)
    doc = await users_col.find_one({"user_id": uid})
    joined = doc["joined"].strftime("%d %b %Y") if doc and doc.get("joined") else "N/A"
    s = await get_settings()
    limit = "∞" if prem else str(s.get("daily_limit", 10))
    await message.reply(
        f"📊 **Aapki Stats**\n\n"
        f"👤 {message.from_user.mention}\n"
        f"🆔 `{uid}`\n"
        f"📅 Joined: {joined}\n"
        f"💎 Premium: {'✅' if prem else '❌'}\n"
        f"📥 Aaj: {today_count}/{limit}"
    )

@app.on_message(filters.command("ping") & filters.user(ADMINS))
async def ping_cmd(client, message):
    s = time.time()
    m = await message.reply("🏓")
    ms = round((time.time() - s) * 1000)
    await m.edit(f"🏓 Pong! `{ms}ms`")

@app.on_chat_member_updated()
async def member_update(client, update):
    if update.new_chat_member and update.new_chat_member.status == enums.ChatMemberStatus.MEMBER:
        user = update.new_chat_member.user
        if user.is_bot: return
        await register_user(user)
        s = await get_settings()
        msg = s.get("welcome_msg", "👋 Welcome {name}! File ka naam type karo 🗂")
        try: await client.send_message(update.chat.id, msg.replace("{name}", user.mention))
        except: pass

@app.on_message(filters.new_chat_members)
async def bot_added(client, message):
    me = (await client.get_me()).id
    for member in message.new_chat_members:
        if member.id == me:
            await register_group(message.chat)
            await message.reply(f"🗂 **AsBhai Vault Bot Aa Gaya!**\n\nFile naam type karo!\n📢 {MAIN_CHANNEL} | 💎 /premium")

@app.on_inline_query()
async def inline_handler(client, query):
    q = query.query.strip()
    if len(q) < 2: return
    from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent
    results = await search_files(q, limit=5)
    me = await client.get_me()
    items = []
    for idx, msg in enumerate(results):
        name = (msg.document.file_name if msg.document and msg.document.file_name
                else clean_caption(msg.caption or f"File #{idx+1}")[:50])
        link = f"https://t.me/{me.username}?start=file_{msg.id}"
        items.append(InlineQueryResultArticle(
            title=name[:60], description="Click karke file lo",
            input_message_content=InputTextMessageContent(f"🗂 **{name}**\n\n[📥 Lo]({link})")
        ))
    if not items:
        items = [InlineQueryResultArticle(
            title=f"'{q}' nahi mila", description="Kuch aur try karo",
            input_message_content=InputTextMessageContent(f"❌ '{q}' nahi mila.")
        )]
    await query.answer(items, cache_time=10)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN - FLASK PEHLE, BOT BAAD MEIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def main():
    # Step 1: Flask pehle start karo alag thread mein
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()
    logger.info("✅ Flask thread started — health check pass hoga ab")

    # Step 2: Thoda wait karo flask start hone do
    await asyncio.sleep(3)

    # Step 3: Bot start karo
    logger.info("🤖 Bot start ho raha hai...")
    try:
        scheduler.start()
        await app.start()
        me = await app.get_me()
        _bot_status["started"] = True
        _bot_status["username"] = me.username
        _bot_status["error"] = ""
        logger.info(f"✅ Bot started: @{me.username}")

        # Log channel pe message
        try:
            await app.send_message(
                LOG_CHANNEL,
                f"✅ **Bot Started!**\n"
                f"@{me.username}\n"
                f"🕐 {datetime.now(IST).strftime('%d %b %Y %H:%M')} IST\n"
                f"🌐 Flask: Running on port {PORT}"
            )
        except Exception as e:
            logger.warning(f"Log channel message failed: {e}")

        await idle()

    except Exception as e:
        _bot_status["error"] = str(e)
        logger.error(f"❌ Bot start failed: {e}")
        # Flask chalta rahega health check ke liye
        while True:
            await asyncio.sleep(60)

    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
