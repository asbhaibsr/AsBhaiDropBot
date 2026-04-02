# ╔══════════════════════════════════════╗
# ║  database.py — AsBhai Drop Bot       ║
# ║  MongoDB Collections & Helper Fns    ║
# ╚══════════════════════════════════════╝
# database.py — AsBhai Drop Bot — Database & Helper Functions
import asyncio, re, string, random
from datetime import timedelta
from pyrogram import enums
from pyrogram.errors import UserNotParticipant
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import aiohttp

from config import (
    MONGO_URI, OWNER_ID, ADMINS, IST, FILE_CHANNEL,
    FORCE_SUB_ID, FORCE_SUB_CHANNEL, SHORTLINK_API, SHORTLINK_URL,
    KOYEB_URL, DEFAULT_SETTINGS, GROUP_DEFAULTS,
    _shortlink_cache, _search_locks, _search_cooldown,
    now, now_ist, make_aware, logger
)

# ═══════════════════════════════════════
#  DATABASE INIT
# ═══════════════════════════════════════
mongo_client      = AsyncIOMotorClient(MONGO_URI)
db                = mongo_client["asbhaidropbot"]
users_col         = db["users"]
groups_col        = db["groups"]
premium_col       = db["premium"]
settings_col      = db["settings"]
tokens_col        = db["tokens"]
requests_col      = db["requests"]
banned_col        = db["banned"]
refers_col        = db["refers"]
free_trial_col    = db["free_trials"]
help_msgs_col     = db["help_msgs"]
payments_col      = db["payments"]
shortlinks_col    = db["shortlinks"]
verify_log_col    = db["verify_logs"]
group_prem_col    = db["group_premium"]
group_sl_col      = db["group_shortlinks"]
group_settings_col = db["group_settings"]

# bot/userbot — set from bot.py via set_clients()
bot = None
userbot = None


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
    User ka current verify state — SEQUENTIAL shortlink rotation.
    
    Logic:
    - Shortlinks order mein check karo
    - Agar SL1 verified nahi → SL1 dikhao
    - Agar SL1 verified hai aur time expired nahi → SL2 check karo
    - Agar SL1 time expired → SL1 dobara dikhao (time-based rotation)
    - Sab verified aur valid → bypass (all_done=True)
    
    Returns: (all_verified: bool, next_sl: dict|None, wait_hours: float)
    """
    links = await get_active_shortlinks()

    if not links:
        if not SHORTLINK_API:
            return True, None, 0
        # Env shortlink — date-based
        today = now_ist().strftime("%Y-%m-%d")
        doc = await users_col.find_one({"user_id": user_id})
        if doc and doc.get("verified_date") == today:
            return True, None, 0
        return False, None, 0

    # Sequential check — order mein
    for sl in links:
        sl_id = str(sl["_id"])
        hours = sl.get("hours", 24)
        log = await verify_log_col.find_one(
            {"user_id": user_id, "shortlink_id": sl_id},
            sort=[("verified_at", -1)]
        )
        if not log:
            # Kabhi verify nahi kiya — ye wali dikhao
            return False, sl, 0
        last_verify = make_aware(log["verified_at"])
        time_passed = (now() - last_verify).total_seconds() / 3600
        if time_passed >= hours:
            # Time expire ho gaya — ye wali dobara dikhao
            return False, sl, time_passed - hours
        # Ye SL valid hai — agli check karo

    # Sab shortlinks verified aur valid
    return True, None, 0

async def mark_sl_verified(user_id, shortlink_id, sl_label=""):
    """Shortlink verify ka log save karo"""
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

    step_text = f"Step {done_count+1}/{total}: {sl_label}" if total > 1 else sl_label
    time_text = f"Har {hours} ghante baad" if hours < 24 else "Har din ek baar"

    miniapp_url_v = f"{KOYEB_URL}/" if KOYEB_URL else None
    prem_btn_label = "💎 Premium lo — verify kabhi nahi"
    if miniapp_url_v:
        prem_row = [InlineKeyboardButton(prem_btn_label, url=miniapp_url_v)]
    else:
        prem_row = [InlineKeyboardButton(prem_btn_label, callback_data="buy_premium")]
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔗 {step_text} — Verify Karo", url=short)],
        [prem_row],
    ])
    msgs = [
        f"Bhai ek kaam karna hoga pehle 🙏\n\nNeeche link dabao, shortlink khatam karo, wapas aao!\n⏰ {time_text} karna hoga.\n\nYa seedha premium le lo, phir ye sab nahi!",
        f"Ek minute ruko! 🤚\n\nPehle ye shortlink complete karo:\n⏰ {time_text} — ek baar karna hai.\n\nPremium lo to kabhi nahi karna padega!",
        f"Jaldi karo bhai! 😅\n\nBas ye shortlink complete karo aur film/series aa jaayegi!\n⏰ {time_text}\n\nPremium users ke liye ye step nahi hoti!",
    ]
    import random
    msg = await message.reply(
        random.choice(msgs),
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

async def send_log(text, reply_markup=None):
    """Log silently — koi crash nahi"""
    if not LOG_CHANNEL or not bot: return
    try:
        log_cid = int(str(LOG_CHANNEL).strip())
        await bot.send_message(
            log_cid, text,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 10))
        try:
            await bot.send_message(
                int(LOG_CHANNEL), text,
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
        except Exception: pass
    except Exception as e:
        logger.debug(f"send_log failed: {e}")

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

        # Human style captions — random se choose karo
        import random
        caps = [
            f"🗂 {fname}\n\n{size_text}Bhai save kar lo jaldi — {mins} min mein delete ho jaayegi! 📌",
            f"🎬 {fname}\n\n{size_text}Aa gayi! Seedha save karo ya forward karo — {mins} min ka time hai! ⏰",
            f"📥 {fname}\n\n{size_text}Enjoy karo! Bas {mins} min baad file chali jaayegi, pehle save karo! 🙏",
            f"🎯 {fname}\n\n{size_text}Le lo bhai! {mins} min baad delete. Abhi save ya forward karo! 📲",
        ]
        clean_cap = random.choice(caps)

        # Premium users ke liye stream + download buttons
        kb = None
        if prem and KOYEB_URL:
            # Stream = mini app mein video player
            stream_page_url = f"{KOYEB_URL}/?uid={user.id}&mid={msg_id}"
            # Download = direct download endpoint (attachment mode)
            download_direct_url = f"{KOYEB_URL}/download/{msg_id}?uid={user.id}"
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("▶️ Stream", web_app=WebAppInfo(url=stream_page_url)),
                    InlineKeyboardButton("⬇️ Download", url=download_direct_url)
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

# bot reference — set by bot.py after client creation
bot = None
userbot = None

def set_clients(b, u):
    """Called from bot.py after client creation"""
    global bot, userbot
    bot = b
    userbot = u
