import os, re, asyncio, logging, time, random, string
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#           CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API_ID = int(os.getenv("API_ID", "29970536"))
API_HASH = os.getenv("API_HASH", "f4bfdcdd4a5c1b7328a7e4f25f024a09")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MONGO_URI = os.getenv("MONGO_URI", "")
OWNER_ID = int(os.getenv("OWNER_ID", "7315805581"))

# ── Jahan se bot files uthata hai (private file storage channel) ──
FILE_CHANNEL = int(os.getenv("FILE_CHANNEL", "-1002463804038"))

# ── Bot ki activity/log channel ──
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", "-1002352329534"))

# ── Main public channel (branding) ──
MAIN_CHANNEL = os.getenv("MAIN_CHANNEL", "@asbhai_bsr")
MAIN_CHANNEL_ID = int(os.getenv("MAIN_CHANNEL_ID", "-1002352329534"))

# ── Force Subscribe channel (user join kare tab hi file mile) ──
FORCE_SUB_CHANNEL = os.getenv("FORCE_SUB_CHANNEL", "@asbhai_bsr")
FORCE_SUB_ID = int(os.getenv("FORCE_SUB_ID", "-1002352329534"))

# ── Aap aur channels add kar sakte ho yahan ──
# EXTRA_FILE_CHANNELS = [-100xxxxxxxxxx, -100xxxxxxxxxx]  # future use

SHORTLINK_API = os.getenv("SHORTLINK_API", "")
SHORTLINK_URL = os.getenv("SHORTLINK_URL", "modijiurl.com")
FLASK_SERVER = os.getenv("FLASK_SERVER", "http://localhost:8080")
ADMINS = [OWNER_ID]
IST = pytz.timezone("Asia/Kolkata")
UPI_ID = "arsadsaifi8272@ibl"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         DEFAULT SETTINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DEFAULT_SETTINGS = {
    "auto_delete": True,
    "auto_delete_time": 600,   # seconds (10 min default)
    "force_sub": True,
    "shortlink_enabled": True,
    "daily_limit": 10,
    "premium_results": 5,
    "free_results": 1,
    "welcome_msg": "🎬 Welcome to As Bhai Drop Bot!\nType any movie name to search.",
    "broadcast_msg": "",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         DATABASE SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client["asbhaimoviebot"]
users_col = db["users"]
groups_col = db["groups"]
premium_col = db["premium"]
settings_col = db["settings"]
stats_col = db["stats"]
files_col = db["files"]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         BOT CLIENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = Client(
    "asbhai_movie_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

scheduler = AsyncIOScheduler(timezone=IST)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         HELPER FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def get_settings():
    s = await settings_col.find_one({"_id": "global"})
    if not s:
        await settings_col.insert_one({"_id": "global", **DEFAULT_SETTINGS})
        return DEFAULT_SETTINGS.copy()
    return s

async def update_setting(key, value):
    await settings_col.update_one({"_id": "global"}, {"$set": {key: value}}, upsert=True)

async def is_premium(user_id: int) -> bool:
    doc = await premium_col.find_one({"user_id": user_id})
    if not doc:
        return False
    if doc.get("expiry") and datetime.now(IST) > doc["expiry"]:
        await premium_col.delete_one({"user_id": user_id})
        return False
    return True

async def add_premium(user_id: int, days: int = 30):
    expiry = datetime.now(IST) + timedelta(days=days)
    await premium_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "added_on": datetime.now(IST)}},
        upsert=True
    )

async def remove_premium(user_id: int):
    await premium_col.delete_one({"user_id": user_id})

async def get_daily_count(user_id: int) -> int:
    today = datetime.now(IST).strftime("%Y-%m-%d")
    doc = await users_col.find_one({"user_id": user_id, "date": today})
    return doc.get("count", 0) if doc else 0

async def increment_daily_count(user_id: int):
    today = datetime.now(IST).strftime("%Y-%m-%d")
    await users_col.update_one(
        {"user_id": user_id, "date": today},
        {"$inc": {"count": 1}},
        upsert=True
    )

async def register_user(user):
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "joined": datetime.now(IST)
        }},
        upsert=True
    )

async def register_group(chat):
    await groups_col.update_one(
        {"chat_id": chat.id},
        {"$set": {
            "chat_id": chat.id,
            "title": chat.title,
            "joined": datetime.now(IST)
        }},
        upsert=True
    )

def remove_links_and_captions(text: str) -> str:
    """Remove all URLs, hashtags, @mentions, and extra whitespace"""
    text = re.sub(r'http[s]?://\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    text = re.sub(r't\.me/\S+', '', text)
    text = re.sub(r'\n\s*\n', '\n', text)
    return text.strip()

def clean_caption(caption: str) -> str:
    if not caption:
        return ""
    return remove_links_and_captions(caption)

async def generate_shortlink(url: str) -> str:
    settings = await get_settings()
    if not settings.get("shortlink_enabled") or not SHORTLINK_API:
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

async def verify_shortlink_token(token: str) -> bool:
    # Store tokens in DB for 1 hour verification
    doc = await db["tokens"].find_one({"token": token})
    if doc and datetime.now(IST) < doc["expiry"]:
        return True
    return False

async def create_verify_token(user_id: int) -> str:
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    await db["tokens"].insert_one({
        "token": token,
        "user_id": user_id,
        "expiry": datetime.now(IST) + timedelta(hours=1)
    })
    return token

async def is_member(user_id: int) -> bool:
    settings = await get_settings()
    if not settings.get("force_sub"):
        return True
    try:
        member = await app.get_chat_member(FORCE_SUB_ID, user_id)
        return member.status not in [
            enums.ChatMemberStatus.BANNED,
            enums.ChatMemberStatus.LEFT
        ]
    except UserNotParticipant:
        return False
    except Exception:
        return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         SEARCH LOGIC
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def search_files(query: str, limit: int = 5):
    """Search files in FILE_CHANNEL and return matching messages"""
    words = [w.lower() for w in query.split() if len(w) > 1]
    if not words:
        return []
    
    results = []
    seen_ids = set()
    
    try:
        # Search each keyword and combine results
        for word in words[:4]:  # Max 4 keywords
            async for msg in app.search_messages(FILE_CHANNEL, word, limit=20):
                if msg.id in seen_ids:
                    continue
                seen_ids.add(msg.id)
                
                # Get text from caption or document name
                text_to_check = ""
                if msg.caption:
                    text_to_check = msg.caption.lower()
                if msg.document and msg.document.file_name:
                    text_to_check += " " + msg.document.file_name.lower()
                if msg.text:
                    text_to_check += " " + msg.text.lower()
                
                # Score: how many keywords match
                score = sum(1 for w in words if w in text_to_check)
                if score > 0:
                    results.append((score, msg))
        
        # Sort by score descending
        results.sort(key=lambda x: x[0], reverse=True)
        return [msg for score, msg in results[:limit]]
    
    except Exception as e:
        logger.error(f"Search error: {e}")
        return []

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         FORCE SUB HANDLER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def force_sub_check(client: Client, message: Message) -> bool:
    user_id = message.from_user.id
    if user_id in ADMINS:
        return True
    
    settings = await get_settings()
    if not settings.get("force_sub"):
        return True
    
    if not await is_member(user_id):
        token = await create_verify_token(user_id)
        bot_username = (await client.get_me()).username
        verify_link = await generate_shortlink(
            f"https://t.me/{bot_username}?start=verify_{token}"
        )
        try:
            invite = await client.export_chat_invite_link(FORCE_SUB_ID)
        except:
            invite = f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel Join Karo", url=invite)],
            [InlineKeyboardButton("✅ Verify Karo", url=verify_link)]
        ])
        
        await message.reply(
            f"⚠️ **Pehle Hamara Channel Join Karo!**\n\n"
            f"Channel join karne ke baad **Verify** button dabaiye.\n\n"
            f"📢 Channel: {FORCE_SUB_CHANNEL}",
            reply_markup=keyboard
        )
        return False
    return True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         /START COMMAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("start") & (filters.group | filters.private))
async def start_cmd(client: Client, message: Message):
    await register_user(message.from_user)
    args = message.command[1] if len(message.command) > 1 else ""
    
    # Handle verify token
    if args.startswith("verify_"):
        token = args[7:]
        if await verify_shortlink_token(token):
            await db["tokens"].delete_one({"token": token})
            await message.reply(
                "✅ **Verification Successful!**\n\n"
                "Ab group mein jaake search karo! 🎬"
            )
        else:
            await message.reply("❌ Invalid ya expired token. Dubara try karo.")
        return
    
    # Handle file request from inline token
    if args.startswith("file_"):
        file_id = args[5:]
        await send_file_to_user(client, message, file_id)
        return
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
            InlineKeyboardButton("➕ Group Add Karo", url=f"https://t.me/{(await client.get_me()).username}?startgroup=true")
        ],
        [
            InlineKeyboardButton("💎 Premium Dekho", callback_data="show_premium"),
            InlineKeyboardButton("ℹ️ Help", callback_data="help")
        ]
    ])
    
    await message.reply(
        f"🎬 **AsBhai Movie Bot**\n\n"
        f"Namaste **{message.from_user.mention}**! 👋\n\n"
        f"🔍 Group mein koi bhi movie ka naam type karo aur main dhundh lunga!\n\n"
        f"✨ **Features:**\n"
        f"• 🔎 Smart Movie Search\n"
        f"• 📥 Auto File Send\n"
        f"• 🔗 Shortlink Protection\n"
        f"• 💎 Premium Members ke liye extra benefits\n\n"
        f"💎 **Premium lo aur shortlink se mukti pao!**",
        reply_markup=keyboard
    )

async def send_file_to_user(client: Client, message: Message, msg_id_str: str):
    """Send a file from channel to user after verification"""
    try:
        msg_id = int(msg_id_str)
        file_msg = await client.get_messages(FILE_CHANNEL, msg_id)
        if not file_msg:
            await message.reply("❌ File nahi mili. Ho sakta hai delete ho gayi ho.")
            return
        
        settings = await get_settings()
        user_id = message.from_user.id
        prem = await is_premium(user_id)
        
        # Daily limit check
        if not prem:
            count = await get_daily_count(user_id)
            if count >= settings.get("daily_limit", 10):
                await message.reply(
                    f"⚠️ **Daily Limit Khatam!**\n\n"
                    f"Aap aaj **{settings.get('daily_limit', 10)} files** le chuke ho.\n"
                    f"Kal dobara aao ya **💎 Premium** lo unlimited ke liye!\n\n"
                    f"/premium type karo jankari ke liye."
                )
                return
        
        # Clean caption
        caption = clean_caption(file_msg.caption or "")
        if caption:
            caption = f"🎬 **{caption}**\n\n📢 {MAIN_CHANNEL}"
        else:
            caption = f"🎬 **File by AsBhai Bot**\n\n📢 {MAIN_CHANNEL}"
        
        auto_del = settings.get("auto_delete", True)
        auto_del_time = settings.get("auto_delete_time", 600)
        
        # Streaming buttons (premium only)
        bot_me = await client.get_me()
        stream_url = f"{FLASK_SERVER}/stream/{FILE_CHANNEL}/{msg_id}"
        download_url = f"{FLASK_SERVER}/download/{FILE_CHANNEL}/{msg_id}"
        
        if prem:
            buttons = [[
                InlineKeyboardButton("▶️ Online Stream", url=stream_url),
                InlineKeyboardButton("📥 Download", url=download_url)
            ]]
        else:
            buttons = [[
                InlineKeyboardButton("▶️ Stream (Premium)", callback_data="need_premium"),
                InlineKeyboardButton("📥 Download (Premium)", callback_data="need_premium")
            ]]
        
        sent = await file_msg.copy(
            message.chat.id,
            caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
        await increment_daily_count(user_id)
        
        if auto_del and not prem:
            mins = auto_del_time // 60
            del_msg = await message.reply(
                f"⏳ Yeh file **{mins} minute** mein delete ho jayegi!\n"
                f"💾 Save karo jaldi se!"
            )
            await asyncio.sleep(auto_del_time)
            try:
                await sent.delete()
                await del_msg.delete()
            except:
                pass
                
    except Exception as e:
        logger.error(f"send_file_to_user error: {e}")
        await message.reply("❌ File bhejne mein problem aayi. Baad mein try karo.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         MAIN SEARCH HANDLER (GROUP)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.group & filters.text & ~filters.command([
    "start","help","stats","broadcast","setdelete","addpremium",
    "removepremium","forcesub","settings","premium","cancel"
]))
async def group_search(client: Client, message: Message):
    if not message.from_user:
        return
    
    await register_user(message.from_user)
    await register_group(message.chat)
    
    query = message.text.strip()
    if len(query) < 2:
        return
    
    # Force sub check
    if not await force_sub_check(client, message):
        return
    
    user_id = message.from_user.id
    prem = await is_premium(user_id)
    settings = await get_settings()
    
    # Daily limit check (non premium)
    if not prem:
        count = await get_daily_count(user_id)
        if count >= settings.get("daily_limit", 10):
            await message.reply(
                f"⚠️ {message.from_user.mention}, aapki daily limit **{settings.get('daily_limit',10)}** ho gayi!\n"
                f"💎 **Premium** lo unlimited results ke liye!\n\n"
                f"/premium type karo."
            )
            return
    
    searching_msg = await message.reply(f"🔍 **Dhundh raha hoon:** `{query}`...")
    
    limit = settings.get("premium_results", 5) if prem else settings.get("free_results", 1)
    results = await search_files(query, limit=limit if prem else 10)
    
    if not results:
        await searching_msg.edit(
            f"❌ **'{query}' nahi mila!**\n\n"
            f"💡 Try karo:\n"
            f"• Spelling check karo\n"
            f"• Sirf movie name likhein\n"
            f"• English mein likhein"
        )
        return
    
    # For non-premium: only 1 result
    if not prem:
        results = results[:1]
    
    bot_me = await client.get_me()
    await searching_msg.delete()
    
    # Send force-sub link first, then verify
    for idx, file_msg in enumerate(results):
        token = await create_verify_token(user_id)
        file_ref = f"https://t.me/{bot_me.username}?start=file_{file_msg.id}"
        short_link = await generate_shortlink(file_ref)
        
        name = ""
        if file_msg.document and file_msg.document.file_name:
            name = file_msg.document.file_name
        elif file_msg.caption:
            name = clean_caption(file_msg.caption)[:50]
        else:
            name = f"File #{idx+1}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📥 {name[:30]}... Lao", url=short_link)],
        ])
        
        sent = await message.reply(
            f"✅ **Mila!** {message.from_user.mention}\n\n"
            f"🎬 **{name}**\n\n"
            f"👇 Button dabao file lene ke liye:",
            reply_markup=keyboard
        )
        
        # Auto-delete the search result message
        if settings.get("auto_delete"):
            asyncio.create_task(
                auto_delete_msg(sent, settings.get("auto_delete_time", 600))
            )

async def auto_delete_msg(msg: Message, delay: int):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         CALLBACK HANDLERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_callback_query()
async def callback_handler(client: Client, query: CallbackQuery):
    data = query.data
    user_id = query.from_user.id
    
    if data == "need_premium":
        await query.answer(
            "💎 Yeh feature sirf Premium users ke liye hai!\n"
            "/premium type karo jankari ke liye.",
            show_alert=True
        )
    
    elif data == "show_premium":
        prem = await is_premium(user_id)
        status = "✅ Active" if prem else "❌ Active Nahi"
        doc = await premium_col.find_one({"user_id": user_id})
        expiry = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Kharidein", callback_data="buy_premium")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        
        await query.message.edit(
            f"💎 **Premium Status**\n\n"
            f"Status: {status}\n"
            f"Expiry: {expiry}\n\n"
            f"**Premium Benefits:**\n"
            f"• 🔓 Shortlink solve nahi karna\n"
            f"• 📦 5 results ek search mein\n"
            f"• ∞ Unlimited daily downloads\n"
            f"• ▶️ Online Streaming\n"
            f"• 📥 Direct Download\n"
            f"• ⚡ Fast Access\n\n"
            f"**Price:** ₹250 / mahina",
            reply_markup=keyboard
        )
    
    elif data == "buy_premium":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💸 UPI se Pay Karo", callback_data="upi_pay")],
            [InlineKeyboardButton("🔙 Back", callback_data="show_premium")]
        ])
        await query.message.edit(
            f"💳 **Premium Kharidein**\n\n"
            f"**Price:** ₹250 / 30 din\n\n"
            f"**Payment karo:**\n"
            f"UPI ID: `{UPI_ID}`\n\n"
            f"**Steps:**\n"
            f"1. UPI se ₹250 bhejo\n"
            f"2. Screenshot owner ko bhejo\n"
            f"3. Owner: @asbhaibsr\n\n"
            f"**Premium Benefits:**\n"
            f"• No Shortlink\n"
            f"• 5 Results/Search\n"
            f"• Unlimited Downloads\n"
            f"• Online Streaming",
            reply_markup=keyboard
        )
    
    elif data == "help":
        await query.message.edit(
            "📖 **Bot Kaise Use Karein:**\n\n"
            "1. Bot ko apne group mein add karo\n"
            "2. Koi bhi movie/show ka naam type karo\n"
            "3. Bot file dhundh kar link dega\n"
            "4. Link par click karo, shortlink solve karo\n"
            "5. File apko PM mein milegi!\n\n"
            "**Commands:**\n"
            "/premium - Premium info\n"
            "/help - Help\n"
            "/stats - Stats (Admin)\n",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="back_main")
            ]])
        )
    
    elif data == "back_main":
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📢 Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL.replace('@','')}"),
                InlineKeyboardButton("💎 Premium", callback_data="show_premium")
            ],
            [InlineKeyboardButton("ℹ️ Help", callback_data="help")]
        ])
        await query.message.edit(
            f"🎬 **AsBhai Movie Bot**\n\n"
            f"Group mein koi bhi movie ka naam type karo!",
            reply_markup=keyboard
        )
    
    await query.answer()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         PREMIUM COMMANDS (OWNER)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("addpremium") & filters.user(ADMINS))
async def add_premium_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/addpremium user_id [days]`\nDefault: 30 days")
        return
    try:
        uid = int(args[1])
        days = int(args[2]) if len(args) > 2 else 30
        await add_premium(uid, days)
        await message.reply(f"✅ User `{uid}` ko **{days} din** ka Premium diya gaya!")
        try:
            await client.send_message(
                uid,
                f"🎉 **Congratulations!**\n\n"
                f"Aapko **{days} din** ka Premium mil gaya!\n"
                f"Ab enjoy karo unlimited features! 💎"
            )
        except:
            pass
    except ValueError:
        await message.reply("❌ Invalid user ID.")

@app.on_message(filters.command("removepremium") & filters.user(ADMINS))
async def remove_premium_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/removepremium user_id`")
        return
    try:
        uid = int(args[1])
        await remove_premium(uid)
        await message.reply(f"✅ User `{uid}` ka Premium hata diya gaya!")
    except ValueError:
        await message.reply("❌ Invalid user ID.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         SETTINGS COMMANDS (OWNER)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("setdelete") & filters.user(ADMINS) & filters.private)
async def set_delete_cmd(client: Client, message: Message):
    """Set auto-delete time in minutes. Owner only via PM."""
    args = message.command
    if len(args) < 2:
        settings = await get_settings()
        cur = settings.get("auto_delete_time", 600) // 60
        await message.reply(
            f"⏱ **Auto Delete Settings**\n\n"
            f"Current: **{cur} minutes**\n\n"
            f"Usage: `/setdelete <minutes>`\n"
            f"Example: `/setdelete 30` (30 minute mein delete)\n\n"
            f"Auto delete: {'ON' if settings.get('auto_delete') else 'OFF'}\n"
            f"Toggle: `/setdelete off` ya `/setdelete on`"
        )
        return
    
    val = args[1].lower()
    if val == "off":
        await update_setting("auto_delete", False)
        await message.reply("✅ Auto delete **OFF** kar diya!")
    elif val == "on":
        await update_setting("auto_delete", True)
        await message.reply("✅ Auto delete **ON** kar diya!")
    else:
        try:
            mins = int(val)
            await update_setting("auto_delete_time", mins * 60)
            await message.reply(f"✅ Auto delete time set: **{mins} minutes**")
        except ValueError:
            await message.reply("❌ Invalid value. Number likhein.")

@app.on_message(filters.command("forcesub") & filters.user(ADMINS) & filters.private)
async def force_sub_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        status = "ON" if s.get("force_sub") else "OFF"
        await message.reply(
            f"📢 **Force Subscribe:** {status}\n\n"
            f"Toggle: `/forcesub on` ya `/forcesub off`"
        )
        return
    val = args[1].lower()
    if val in ["on", "off"]:
        await update_setting("force_sub", val == "on")
        await message.reply(f"✅ Force Sub **{val.upper()}** kar diya!")
    else:
        await message.reply("Usage: `/forcesub on` ya `/forcesub off`")

@app.on_message(filters.command("settings") & filters.user(ADMINS) & filters.private)
async def settings_cmd(client: Client, message: Message):
    s = await get_settings()
    await message.reply(
        f"⚙️ **Bot Settings**\n\n"
        f"🔄 Auto Delete: {'ON' if s.get('auto_delete') else 'OFF'}\n"
        f"⏱ Delete Time: {s.get('auto_delete_time',600)//60} minutes\n"
        f"📢 Force Sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"🔗 Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"📦 Free Results: {s.get('free_results',1)}\n"
        f"💎 Premium Results: {s.get('premium_results',5)}\n"
        f"📊 Daily Limit (Free): {s.get('daily_limit',10)}\n\n"
        f"**Commands:**\n"
        f"`/setdelete <min>` - Delete time\n"
        f"`/forcesub on/off` - Force subscribe\n"
        f"`/shortlink on/off` - Shortlink toggle\n"
        f"`/setlimit <n>` - Daily limit\n"
        f"`/setresults <free> <premium>` - Result count\n"
    )

@app.on_message(filters.command("shortlink") & filters.user(ADMINS) & filters.private)
async def shortlink_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"🔗 Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\nUsage: `/shortlink on/off`")
        return
    val = args[1].lower()
    await update_setting("shortlink_enabled", val == "on")
    await message.reply(f"✅ Shortlink **{val.upper()}** kar diya!")

@app.on_message(filters.command("setlimit") & filters.user(ADMINS) & filters.private)
async def set_limit_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 2:
        s = await get_settings()
        await message.reply(f"📊 Current Daily Limit: {s.get('daily_limit',10)}\nUsage: `/setlimit <number>`")
        return
    try:
        n = int(args[1])
        await update_setting("daily_limit", n)
        await message.reply(f"✅ Daily limit set: **{n} files/day**")
    except:
        await message.reply("❌ Number likhein.")

@app.on_message(filters.command("setresults") & filters.user(ADMINS) & filters.private)
async def set_results_cmd(client: Client, message: Message):
    args = message.command
    if len(args) < 3:
        await message.reply("Usage: `/setresults <free> <premium>`\nExample: `/setresults 1 5`")
        return
    try:
        free = int(args[1])
        prem = int(args[2])
        await update_setting("free_results", free)
        await update_setting("premium_results", prem)
        await message.reply(f"✅ Results set:\nFree: **{free}**\nPremium: **{prem}**")
    except:
        await message.reply("❌ Numbers likhein.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         BROADCAST (OWNER)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    """
    /broadcast users <message>
    /broadcast groups <message>
    /broadcast all <message>
    """
    args = message.command
    if len(args) < 3:
        await message.reply(
            "Usage:\n"
            "`/broadcast users <message>` - Sabhi users ko\n"
            "`/broadcast groups <message>` - Sabhi groups ko\n"
            "`/broadcast all <message>` - Sabko"
        )
        return
    
    target = args[1].lower()
    text = " ".join(args[2:])
    
    status_msg = await message.reply("📡 Broadcast shuru ho raha hai...")
    
    total = done = failed = blocked = 0
    
    if target in ["users", "all"]:
        async for user_doc in users_col.find({}):
            uid = user_doc.get("user_id")
            if not uid:
                continue
            total += 1
            try:
                await client.send_message(uid, text)
                done += 1
                await asyncio.sleep(0.05)
            except UserIsBlocked:
                blocked += 1
                await users_col.delete_one({"user_id": uid})
            except InputUserDeactivated:
                blocked += 1
                await users_col.delete_one({"user_id": uid})
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                failed += 1
    
    if target in ["groups", "all"]:
        async for grp_doc in groups_col.find({}):
            cid = grp_doc.get("chat_id")
            if not cid:
                continue
            total += 1
            try:
                await client.send_message(cid, text)
                done += 1
                await asyncio.sleep(0.1)
            except (ChatWriteForbidden, PeerIdInvalid):
                failed += 1
                await groups_col.delete_one({"chat_id": cid})  # Auto clean
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                failed += 1
    
    await status_msg.edit(
        f"📡 **Broadcast Completed!**\n\n"
        f"Total: {total}\n"
        f"✅ Done: {done}\n"
        f"❌ Failed: {failed}\n"
        f"🚫 Blocked/Deleted: {blocked}"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         STATS (OWNER)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats_cmd(client: Client, message: Message):
    total_users = await users_col.count_documents({})
    total_groups = await groups_col.count_documents({})
    total_premium = await premium_col.count_documents({})
    
    today = datetime.now(IST).strftime("%Y-%m-%d")
    today_downloads = await users_col.count_documents({"date": today, "count": {"$gt": 0}})
    
    await message.reply(
        f"📊 **Bot Statistics**\n\n"
        f"👥 Total Users: **{total_users}**\n"
        f"🏘 Total Groups: **{total_groups}**\n"
        f"💎 Premium Users: **{total_premium}**\n"
        f"📥 Today's Active: **{today_downloads}**\n\n"
        f"📅 Date: {datetime.now(IST).strftime('%d %b %Y %H:%M')} IST"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#         PREMIUM INFO (/premium)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("premium"))
async def premium_info_cmd(client: Client, message: Message):
    user_id = message.from_user.id
    prem = await is_premium(user_id)
    doc = await premium_col.find_one({"user_id": user_id})
    expiry = doc["expiry"].strftime("%d %b %Y") if doc and doc.get("expiry") else "N/A"
    
    status = f"✅ Active (Expires: {expiry})" if prem else "❌ Active Nahi"
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Premium Kharidein (₹250/mo)", callback_data="buy_premium")]
    ])
    
    await message.reply(
        f"💎 **Premium Membership**\n\n"
        f"Status: {status}\n\n"
        f"**Benefits:**\n"
        f"• 🔓 Shortlink solve nahi karna\n"
        f"• 📦 5 results/search (Free: 1)\n"
        f"• ∞ Unlimited downloads/day\n"
        f"• ▶️ Online Streaming\n"
        f"• 📥 Direct Download\n\n"
        f"**Price:** ₹250 / 30 din\n"
        f"**UPI:** `{UPI_ID}`\n\n"
        f"Pay karke @asbhaibsr ko screenshot bhejo.",
        reply_markup=keyboard if not prem else None
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     NEW MEMBER JOIN WELCOME
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_chat_member_updated()
async def member_update(client: Client, update: ChatMemberUpdated):
    if update.new_chat_member and update.new_chat_member.status == enums.ChatMemberStatus.MEMBER:
        user = update.new_chat_member.user
        if user.is_bot:
            return
        await register_user(user)
        settings = await get_settings()
        welcome = settings.get(
            "welcome_msg",
            "👋 Welcome {name}! Movie ka naam type karo aur main dhundhunga! 🎬"
        )
        welcome = welcome.replace("{name}", user.mention)
        try:
            await client.send_message(update.chat.id, welcome)
        except:
            pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     BOT ADDED TO GROUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.new_chat_members)
async def bot_added(client: Client, message: Message):
    bot_id = (await client.get_me()).id
    for member in message.new_chat_members:
        if member.id == bot_id:
            await register_group(message.chat)
            await message.reply(
                f"🎬 **AsBhai Movie Bot Yahan Aa Gaya!**\n\n"
                f"Ab koi bhi movie ka naam type karo, main dhundh lunga!\n\n"
                f"📢 Channel: {MAIN_CHANNEL}\n"
                f"💎 Premium: /premium"
            )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     INLINE MODE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_inline_query()
async def inline_handler(client: Client, query):
    q = query.query.strip()
    if len(q) < 2:
        return
    
    from pyrogram.types import InlineQueryResultArticle, InputTextMessageContent
    
    results = await search_files(q, limit=5)
    inline_results = []
    bot_me = await client.get_me()
    
    for idx, msg in enumerate(results):
        name = ""
        if msg.document and msg.document.file_name:
            name = msg.document.file_name
        elif msg.caption:
            name = clean_caption(msg.caption)[:50]
        else:
            name = f"Result #{idx+1}"
        
        link = f"https://t.me/{bot_me.username}?start=file_{msg.id}"
        
        inline_results.append(
            InlineQueryResultArticle(
                title=name[:60],
                description="Click to get this file",
                input_message_content=InputTextMessageContent(
                    f"🎬 **{name}**\n\n[📥 File Lao]({link})"
                )
            )
        )
    
    if not inline_results:
        inline_results = [
            InlineQueryResultArticle(
                title=f"'{q}' nahi mila",
                description="Kuch aur try karo",
                input_message_content=InputTextMessageContent(
                    f"❌ '{q}' nahi mila."
                )
            )
        ]
    
    await query.answer(inline_results, cache_time=10)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     HEALTH CHECK COMMAND
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.on_message(filters.command("ping") & filters.user(ADMINS))
async def ping_cmd(client: Client, message: Message):
    start = time.time()
    m = await message.reply("Pong!")
    ms = round((time.time() - start) * 1000)
    await m.edit(f"🏓 Pong! `{ms}ms`")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#     MAIN ENTRY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def main():
    logger.info("🎬 AsBhai Movie Bot starting...")
    scheduler.start()
    await app.start()
    me = await app.get_me()
    logger.info(f"✅ Bot started: @{me.username}")
    try:
        await app.send_message(
            OWNER_ID,
            f"✅ **Bot Started!**\n@{me.username}\n{datetime.now(IST).strftime('%d %b %Y %H:%M')} IST"
        )
    except:
        pass
    await app.idle()

if __name__ == "__main__":
    app.run(main())
