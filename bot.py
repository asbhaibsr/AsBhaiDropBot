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
STRING_SESSION    = os.getenv("STRING_SESSION", "")   # Userbot ke liye
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

logger.info(f"FILE_CHANNEL={FILE_CHANNEL} | STRING_SESSION={'SET' if STRING_SESSION else 'MISSING'}")

DEFAULT_SETTINGS = {
    "auto_delete": True,
    "auto_delete_time": 300,
    "force_sub": True,
    "shortlink_enabled": True,
    "daily_limit": 10,
    "premium_results": 5,
    "free_results": 1,
    "welcome_msg": "👋 Welcome {name}! Koi bhi file ka naam type karo 🗂",
}

# ═══════════════════════════════════════
#  FLASK - HEALTH CHECK
# ═══════════════════════════════════════
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "AsBhai Drop Bot Running! @asbhaibsr", 200

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

# ═══════════════════════════════════════
#  BOT + USERBOT CLIENTS
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
#  HELPERS
# ═══════════════════════════════════════
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
    await premium_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry}},
        upsert=True
    )

async def remove_premium(user_id):
    await premium_col.delete_one({"user_id": user_id})

async def get_daily_count(user_id):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id, "date": today})
    return doc.get("count", 0) if doc else 0

async def increment_daily(user_id):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id, "date": today},
        {"$inc": {"count": 1}}, upsert=True
    )

async def save_user(user):
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "last_seen": datetime.now(IST)
        }, "$setOnInsert": {"joined": datetime.now(IST)}},
        upsert=True
    )

async def save_group(chat):
    await groups_col.update_one(
        {"chat_id": chat.id},
        {"$set": {"chat_id": chat.id, "title": chat.title}},
        upsert=True
    )

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
                    return (await r.text()).strip()
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
#  TOKEN SYSTEM (24 ghante verification)
# ═══════════════════════════════════════
async def make_token(user_id):
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    await tokens_col.insert_one({
        "token": token,
        "user_id": user_id,
        "type": "verify",
        "expiry": datetime.now(IST) + timedelta(hours=1)
    })
    return token

async def check_token(token):
    doc = await tokens_col.find_one({"token": token})
    return bool(doc and datetime.now(IST) < doc["expiry"])

async def is_verified_today(user_id):
    """Check karo user ne aaj shortlink solve ki hai ya nahi"""
    prem = await is_premium(user_id)
    if prem: return True  # Premium ko verify nahi karna
    today = datetime.now(IST).strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id, "verified_date": today})
    return bool(doc)

async def mark_verified(user_id):
    """User ko aaj ke liye verified mark karo"""
    today = datetime.now(IST).strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"verified_date": today}},
        upsert=True
    )

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
#  SEARCH (Userbot se - bots search nahi kar sakte)
# ═══════════════════════════════════════
async def do_search(query, limit=5):
    if not userbot:
        logger.error("Userbot not available! STRING_SESSION set nahi hai.")
        return []

    query = query.strip()
    words = [w.lower() for w in query.split() if len(w) > 1]
    if not words: return []

    results = []
    seen = set()

    # Multiple search strategies
    search_queries = []
    if len(words) > 1:
        search_queries.append(query)        # pehle full query
    search_queries.extend(words[:4])        # phir har word
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
#  SEND FILE TO USER
# ═══════════════════════════════════════
async def send_file(client, message, msg_id_str):
    try:
        msg_id = int(msg_id_str)
        # Userbot se file fetch karo
        if userbot:
            file_msg = await userbot.get_messages(FILE_CHANNEL, msg_id)
        else:
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

        cap = clean_text(file_msg.caption or "")
        cap = (f"🗂 **{cap}**\n\n📢 {MAIN_CHANNEL}"
               if cap else f"🗂 **AsBhai Drop Bot**\n\n📢 {MAIN_CHANNEL}")

        btns = [[
            InlineKeyboardButton(
                "▶️ Stream" if prem else "▶️ Stream (Premium)",
                callback_data="need_premium"
            ),
            InlineKeyboardButton(
                "📥 Download" if prem else "📥 Download (Premium)",
                callback_data="need_premium"
            )
        ]]

        sent = await file_msg.copy(
            message.chat.id,
            caption=cap,
            reply_markup=InlineKeyboardMarkup(btns)
        )
        await increment_daily(uid)

        # Auto delete
        t = s.get("auto_delete_time", 300)
        if s.get("auto_delete"):
            note = await message.reply(
                f"⚠️ {message.from_user.mention} **yeh file {t//60} minute mein delete ho jayegi!**\n"
                f"📌 Kahi aur forward kar lo abhi, warna file chali jayegi!"
            )
            asyncio.create_task(del_later(sent, t))
            asyncio.create_task(del_later(note, t))

        await send_log(
            f"📥 **File Sent**\n"
            f"👤 {message.from_user.mention} (`{uid}`)\n"
            f"📁 ID: `{msg_id}` | 💎 {'Yes' if prem else 'No'}"
        )
    except Exception as e:
        logger.error(f"send_file error: {e}")
        await message.reply(f"❌ Error: {e}")

# ═══════════════════════════════════════
#  FORCE SUB + SHORTLINK FLOW
#
#  Flow:
#  1. User group mein search karta hai
#  2. Bot check karta hai - channel join kiya?
#     - Nahi: Channel join karo button dikhao (shortlink nahi)
#  3. Channel join check hone ke baad - aaj verify kiya?
#     - Nahi: Shortlink dete hain verify ke liye
#     - Haan: Seedha search result dete hain
# ═══════════════════════════════════════
async def force_sub_check(client, message):
    """Sirf channel join check - shortlink nahi"""
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
            [InlineKeyboardButton("✅ Join Ho Gaya — Verify", callback_data=f"check_join_{uid}")]
        ])
        await message.reply(
            f"⚠️ **Pehle Hamara Channel Join Karo!**\n\n"
            f"Channel join karne ke baad **Verify** button dabao.\n\n"
            f"📢 {FORCE_SUB_CHANNEL}",
            reply_markup=kb
        )
        return False
    return True

async def shortlink_verify_check(client, message):
    """
    Search se pehle shortlink verification check.
    Agar aaj verify nahi kiya to shortlink deta hai.
    Premium users ko nahi deta.
    """
    uid = message.from_user.id
    if uid in ADMINS: return True
    if await is_verified_today(uid): return True

    # Shortlink token banao
    token = await make_token(uid)
    me = await client.get_me()
    verify_url = f"https://t.me/{me.username}?start=shortverify_{token}"
    short = await make_shortlink(verify_url)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Verify Karo (1 Click)", url=short)],
        [InlineKeyboardButton("💎 Premium Lo — No Verify", callback_data="buy_premium")]
    ])
    await message.reply(
        f"🔐 **Daily Verification Required**\n\n"
        f"Aaj ki verification baaki hai!\n\n"
        f"👇 Link pe click karo → shortlink solve karo → bot ke PM mein verify ho jao\n\n"
        f"✅ Ek baar verify karo — **24 ghante** ke liye free!\n"
        f"💎 **Premium lo** aur kabhi verify mat karo!",
        reply_markup=kb
    )
    return False

# ═══════════════════════════════════════
#  /START
# ═══════════════════════════════════════
@bot.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    logger.info(f"/start uid={message.from_user.id}")
    await save_user(message.from_user)

    args = message.command[1] if len(message.command) > 1 else ""

    # Shortlink verification token
    if args.startswith("shortverify_"):
        token = args[12:]
        if await check_token(token):
            await tokens_col.delete_one({"token": token})
            await mark_verified(message.from_user.id)
            await message.reply(
                f"✅ **Verification Ho Gayi!**\n\n"
                f"Wah {message.from_user.mention}! 🎉\n\n"
                f"Ab aap **24 ghante** ke liye verify ho gaye!\n"
                f"Group mein jaake koi bhi file search karo! 🗂\n\n"
                f"💎 Har roz verify na karna ho to: /premium"
            )
        else:
            await message.reply(
                "❌ Token invalid ya expire ho gaya.\n"
                "Group mein dubara search karo."
            )
        return

    # Channel join verification
    if args.startswith("verify_"):
        token = args[7:]
        if await check_token(token):
            await tokens_col.delete_one({"token": token})
            await message.reply(
                "✅ **Channel Verify Ho Gaya!**\n\n"
                "Ab group mein jaake file ka naam type karo! 🗂"
            )
        else:
            await message.reply("❌ Invalid token. Dubara try karo.")
        return

    # File request
    if args.startswith("file_"):
        await send_file(client, message, args[5:])
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
        f"🗂 **AsBhai Drop Bot**\n\n"
        f"Namaste **{message.from_user.mention}**! 👋\n\n"
        f"Group mein koi bhi file ka naam type karo — main dhundh lunga!\n\n"
        f"✨ Smart Search | 🔗 Daily Verify | 💎 Premium\n\n"
        f"💎 /premium — Verify se chhutkara pao!\n"
        f"📊 /mystats — Apni stats dekho",
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
        "premium","ping","shortlink","setlimit","setresults","mystats"
    ])
)
async def search_handler(client, message: Message):
    if not message.from_user: return
    await save_user(message.from_user)
    await save_group(message.chat)

    query = message.text.strip()
    if len(query) < 2: return

    uid = message.from_user.id

    # Step 1: Channel join check
    if not await force_sub_check(client, message): return

    # Step 2: Daily shortlink verify check (premium ko nahi)
    if not await shortlink_verify_check(client, message): return

    # Step 3: Daily limit check
    prem = await is_premium(uid)
    s = await get_settings()
    if not prem:
        count = await get_daily_count(uid)
        if count >= s.get("daily_limit", 10):
            await message.reply(
                f"⚠️ {message.from_user.mention}, aaj ki limit "
                f"**{s.get('daily_limit',10)}** ho gayi!\n"
                f"💎 /premium lo unlimited ke liye!"
            )
            return

    # Step 4: Search
    wait_msg = await message.reply(f"🔍 **Dhundh raha hoon:** `{query}`\n\nThoda wait karo... ⏳")

    limit = s.get("premium_results", 5) if prem else 10
    found = await do_search(query, limit=limit)

    if not found:
        await wait_msg.edit(
            f"😕 **'{query}' nahi mila**\n\n"
            f"💡 Try karo:\n"
            f"• Sirf movie/show ka naam likhein\n"
            f"• English mein likhein\n"
            f"• Spelling check karo\n\n"
            f"📢 Request: {MAIN_CHANNEL}"
        )
        return

    if not prem:
        found = found[:s.get("free_results", 1)]

    me = await client.get_me()
    await wait_msg.delete()

    for idx, fmsg in enumerate(found):
        if fmsg.document and fmsg.document.file_name:
            name = fmsg.document.file_name
        elif fmsg.caption:
            name = clean_text(fmsg.caption)[:60]
        else:
            name = f"File #{idx+1}"

        link = await make_shortlink(
            f"https://t.me/{me.username}?start=file_{fmsg.id}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📥 {name[:40]} — Lo", url=link)]
        ])
        sent = await message.reply(
            f"✅ {message.from_user.mention} yeh lo!\n\n"
            f"🗂 **{name}**\n\n"
            f"👇 Button dabao file lene ke liye:",
            reply_markup=kb
        )
        if s.get("auto_delete"):
            t = s.get("auto_delete_time", 300)
            asyncio.create_task(del_later(sent, t))

# ═══════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, query: CallbackQuery):
    data = query.data
    uid = query.from_user.id

    # Channel join verify
    if data.startswith("check_join_"):
        target_uid = int(data.split("_")[-1])
        if uid != target_uid:
            await query.answer("Ye button aapke liye nahi!", show_alert=True)
            return
        if await check_member(uid):
            await query.message.delete()
            await query.answer("✅ Channel join ho gaya!", show_alert=False)
            await client.send_message(
                uid,
                "✅ **Channel Join Ho Gaya!**\n\n"
                "Ab group mein jaake file ka naam type karo! 🗂"
            )
        else:
            await query.answer(
                "❌ Aapne abhi channel join nahi kiya!\nPehle join karo.",
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
        doc = await premium_col.find_one({"user_id": uid})
        exp = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Lo", callback_data="buy_premium")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"💎 **Premium Status**\n\n"
            f"Status: {'✅ Active' if prem else '❌ Nahi'}\n"
            f"Expiry: {exp}\n\n"
            f"**Premium Benefits:**\n"
            f"• 🔓 Koi verification nahi\n"
            f"• 📦 5 results/search\n"
            f"• ∞ Unlimited downloads\n"
            f"• ▶️ Stream & Download\n\n"
            f"**Price:** ₹250/mahina",
            reply_markup=kb
        )

    elif data == "buy_premium":
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n"
            f"**Price:** ₹250 / 30 din\n\n"
            f"**UPI ID:** `{UPI_ID}`\n\n"
            f"**Steps:**\n"
            f"1. UPI se ₹250 bhejo\n"
            f"2. Screenshot lo\n"
            f"3. @asbhaibsr ko bhejo\n"
            f"4. 1 ghante mein activate! ⚡",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📞 Contact Admin", url="https://t.me/asbhaibsr")],
                [InlineKeyboardButton("🔙 Back", callback_data="show_premium")]
            ])
        )

    elif data == "help":
        await query.message.edit(
            "📖 **Bot Kaise Use Karein**\n\n"
            "1️⃣ Bot ko group mein add karo\n"
            "2️⃣ Channel join karo\n"
            "3️⃣ Daily shortlink verify karo (1 baar)\n"
            "4️⃣ File ka naam type karo\n"
            "5️⃣ Button dabao → file milegi PM mein!\n\n"
            "⏳ File **5 minute** baad delete ho jati hai\n"
            "📌 File forward kar lo turant!\n\n"
            "💎 Premium = No verify, 5 results, unlimited!\n\n"
            "/premium /mystats",
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
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
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
                f"Aapko **{days} din** ka Premium mila! 💎\n"
                f"Ab koi verification nahi, unlimited files!"
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
        await message.reply("Usage: `/setresults <free> <premium>`\nExample: `/setresults 1 5`")
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
        f"💎 Premium Results: {s.get('premium_results',5)}\n\n"
        f"**Commands:**\n"
        f"`/setdelete <min>` | `/forcesub on/off`\n"
        f"`/shortlink on/off` | `/setlimit <n>`\n"
        f"`/setresults <f> <p>`"
    )

@bot.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats(client, message: Message):
    u = await users_col.count_documents({})
    g = await groups_col.count_documents({})
    p = await premium_col.count_documents({})
    today = datetime.now(IST).strftime("%Y-%m-%d")
    active = await users_col.count_documents({"date": today, "count": {"$gt": 0}})
    verified = await users_col.count_documents({"verified_date": today})
    await message.reply(
        f"📊 **Bot Stats**\n\n"
        f"👥 Total Users: **{u}**\n"
        f"🏘 Groups: **{g}**\n"
        f"💎 Premium: **{p}**\n"
        f"📥 Aaj Downloads: **{active}**\n"
        f"✅ Aaj Verified: **{verified}**\n\n"
        f"🕐 {datetime.now(IST).strftime('%d %b %Y %H:%M')} IST"
    )

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
            if not uid: continue
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
            if not cid: continue
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
    doc = await premium_col.find_one({"user_id": uid})
    exp = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
    kb = None if prem else InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Premium Lo (₹250/mo)", callback_data="buy_premium")]
    ])
    await message.reply(
        f"💎 **Premium Membership**\n\n"
        f"Status: {'✅ Active — ' + exp if prem else '❌ Active Nahi'}\n\n"
        f"**Benefits:**\n"
        f"• 🔓 Koi daily verification nahi\n"
        f"• 📦 5 results per search\n"
        f"• ∞ Unlimited downloads\n"
        f"• ▶️ Stream & Download buttons\n\n"
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
    joined = doc["joined"].strftime("%d %b %Y") if doc and doc.get("joined") else "N/A"
    today = datetime.now(IST).strftime("%Y-%m-%d")
    verified = bool(doc and doc.get("verified_date") == today) if doc else False
    s = await get_settings()
    limit = "∞" if prem else str(s.get("daily_limit", 10))
    await message.reply(
        f"📊 **Aapki Stats**\n\n"
        f"👤 {message.from_user.mention}\n"
        f"🆔 `{uid}`\n"
        f"📅 Joined: {joined}\n"
        f"💎 Premium: {'✅ Active' if prem else '❌ Nahi'}\n"
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
            await message.reply(
                f"🗂 **AsBhai Drop Bot Aa Gaya!**\n\n"
                f"Koi bhi file ka naam type karo!\n\n"
                f"📢 {MAIN_CHANNEL} | 💎 /premium"
            )
        else:
            if not member.is_bot:
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
#  START BOT
# ═══════════════════════════════════════
def start_bot():
    # Flask thread - health check ke liye
    Thread(target=run_flask, daemon=True).start()
    logger.info("✅ Flask thread started")

    # Userbot start (search ke liye)
    if userbot:
        userbot.start()
        logger.info("✅ Userbot started for search")
    else:
        logger.warning("⚠️ STRING_SESSION not set! Search kaam nahi karega.")

    # Bot start
    logger.info("🚀 Starting AsBhai Drop Bot...")
    bot.run()

if __name__ == "__main__":
    start_bot()
