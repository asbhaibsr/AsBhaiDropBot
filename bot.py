# ╔══════════════════════════════════════╗
# ║  bot.py — AsBhai Drop Bot            ║
# ║  Main File — Handlers & Entry Point  ║
# ╚══════════════════════════════════════╝
# bot.py — AsBhai Drop Bot — Main File
# ═══════════════════════════════════════
#  Imports
# ═══════════════════════════════════════
import os, re, time, random, string, asyncio, logging
from datetime import datetime, timedelta
from threading import Thread

import pytz, aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    WebAppInfo
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatWriteForbidden, PeerIdInvalid, UserNotParticipant
)
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Local modules ──
from config import (
    API_ID, API_HASH, BOT_TOKEN, STRING_SESSION,
    OWNER_ID, FILE_CHANNEL, LOG_CHANNEL, MAIN_CHANNEL,
    FORCE_SUB_ID, FORCE_SUB_CHANNEL, SHORTLINK_API, SHORTLINK_URL,
    KOYEB_URL, ADMINS, IST, UPI_ID, PORT,
    _shortlink_cache, _search_locks, _search_cooldown,
    DEFAULT_SETTINGS, GROUP_DEFAULTS,
    now, now_ist, make_aware, logger
)
from database import (
    mongo_client, db,
    users_col, groups_col, premium_col, settings_col, tokens_col,
    requests_col, banned_col, refers_col, free_trial_col, help_msgs_col,
    payments_col, shortlinks_col, verify_log_col, group_prem_col,
    group_sl_col, group_settings_col,
    get_settings, update_setting, get_group_settings, update_group_setting,
    save_user, save_group, is_banned, ban_user, unban_user,
    get_free_trial_status, is_premium, add_premium, remove_premium, get_premium_expiry,
    get_daily_count, increment_daily,
    is_verified_today, mark_verified, make_token, check_token,
    get_fsub_list, check_member_multi, build_fsub_keyboard, force_sub_check,
    get_active_shortlinks, make_shortlink_with, make_shortlink,
    get_user_verify_state, mark_sl_verified, get_cached_shortlink, verify_check,
    clean_caption, get_file_name, get_file_size, del_later, send_log,
    do_search, send_file_to_pm,
    set_clients as db_set_clients,
)
from routes import (
    aio_app, routes, run_aiohttp_server,
    set_clients as routes_set_clients,
)

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
#  LANGUAGES for filter buttons
# ═══════════════════════════════════════
LANGUAGES = [
    ("🇬🇧 English", "english"), ("🇮🇳 Hindi", "hindi"),
    ("🎭 Malayalam", "malayalam"), ("🎵 Tamil", "tamil"),
    ("🎬 Telugu", "telugu"), ("🌸 Bengali", "bengali"),
    ("🎪 Kannada", "kannada"), ("🎠 Punjabi", "punjabi"),
]

# ═══════════════════════════════════════
#  CLEANUP SCHEDULER
# ═══════════════════════════════════════
async def cleanup():
    # Requests 24h se purane delete karo
    from datetime import timedelta
    one_day_ago = now() - timedelta(hours=24)
    req_deleted = await requests_col.delete_many({"time": {"$lt": one_day_ago}})
    if req_deleted.deleted_count:
        logger.info(f"Cleanup: {req_deleted.deleted_count} old requests deleted")

    deleted = await tokens_col.delete_many({"expiry": {"$lt": now()}})
    expired_cache = [k for k, (_, exp) in _shortlink_cache.items() if now() > exp]
    for k in expired_cache:
        del _shortlink_cache[k]

    cur_time = asyncio.get_event_loop().time()
    expired_cd = [k for k, v in _search_cooldown.items() if cur_time > v]
    for k in expired_cd:
        del _search_cooldown[k]

    stale_locks = [k for k, v in _search_locks.items() if v]
    for k in stale_locks:
        _search_locks[k] = False

    if deleted.deleted_count or expired_cache or expired_cd:
        logger.info(f"Cleanup: {deleted.deleted_count} tokens, {len(expired_cache)} sl_cache, {len(expired_cd)} cooldown")

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
                    sl_doc = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
                    if sl_doc:
                        sl_label = sl_doc.get("label", "Shortlink")
                        hours = sl_doc.get("hours", 24)
                        await mark_sl_verified(uid, sl_id, sl_label)
                        verify_count = await verify_log_col.count_documents({"user_id": uid, "shortlink_id": sl_id})
                        uname = message.from_user.username
                        display_name = message.from_user.first_name or "User"
                        # Convert to small caps style: Nᴀᴍᴇ
                        def to_smallcaps(text):
                            sc = str.maketrans(
                                'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
                                'ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ'
                            )
                            return text.translate(sc)
                        sc_name = to_smallcaps(display_name[:20])
                        await send_log(
                            f"#VerifyShortlink\n\n"
                            f"ɪᴅ - {uid}\n\n"
                            f"Nᴀᴍᴇ - {sc_name}\n\n"
                            f"sʜᴏʀᴛʟɪɴᴋ - {sl_label}\n"
                            f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
                        )
                except Exception as e:
                    logger.error(f"sl verify log error: {e}")
                    await mark_sl_verified(uid, sl_id or "default", sl_label)
            else:
                # env_default ya legacy — date-based mark karo
                await mark_verified(uid)
                uname2 = message.from_user.username
                dn2 = message.from_user.first_name or "User"
                def sc2(t):
                    sc = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz','ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ')
                    return t.translate(sc)
                await send_log(
                    f"#VerifyShortlink\n\n"
                    f"ɪᴅ - {uid}\n\n"
                    f"Nᴀᴍᴇ - {sc2(dn2[:20])}\n\n"
                    f"sʜᴏʀᴛʟɪɴᴋ - Env Default\n"
                    f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
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
        google_q = query_text.replace(" ", "+")
        filter_url = "https://t.me/asfilter_bot?start=" + query_text.replace(" ", "_")
        google_url = f"https://www.google.com/search?q={google_q}+telegram+movie"
        await wait_msg.edit(
            f"😕 '{query_text}' yahan nahi mila\n\n"
            f"Kya karein:\n"
            f"1. Spelling check karo\n"
            f"2. Sirf naam likhein (year mat)\n"
            f"3. Niche ke buttons try karo",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 @asfilter_bot mein dhundho", url=filter_url),
            ],[
                InlineKeyboardButton("🌍 Google mein dhundho", url=google_url),
            ]])
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
            google_q = query.replace(" ", "+")
            filter_url = "https://t.me/asfilter_bot?start=" + query.replace(" ", "_")
            google_url = f"https://www.google.com/search?q={google_q}+telegram+movie"
            edited = await wait_msg.edit(
                f"😕 '{query}' yahan nahi mila\n\n"
                f"1. Spelling check karo\n"
                f"2. Niche ke buttons try karo",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 @asfilter_bot mein dhundho", url=filter_url),
                ],[
                    InlineKeyboardButton("🌍 Google mein dhundho", url=google_url),
                ]])
            )
            asyncio.create_task(del_later(edited, 300))
            return

        found = found[:limit]
        await wait_msg.delete()
        me = await client.get_me()

        # ── Pagination setup ──
        # Total results se pages calculate karo
        all_found = found  # already limited to `limit`
        page = 0
        page_size = 5  # ek page pe 5 files
        total_pages = max(1, (len(all_found) + page_size - 1) // page_size)
        page_found = all_found[:page_size]

        # Safe query key for callbacks (spaces → _, max 20 chars)
        qkey = re.sub(r'[^a-zA-Z0-9_]', '_', query)[:18]
        # Cache results for pagination
        _result_cache[f"{uid}_{qkey}"] = all_found

        # ── File buttons for page 0 ──
        file_buttons = []
        for idx, fmsg in enumerate(page_found):
            fname = get_file_name(fmsg)
            fname_clean = re.sub(r'[@#]\w+', '', fname)
            fname_clean = re.sub(r'_+', ' ', fname_clean).strip()
            fname_show = fname_clean[:36] if fname_clean else f"File {idx+1}"
            fsize = get_file_size(fmsg)
            size_text = f" [{fsize}]" if fsize else ""
            final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
            file_buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

        # ── Pagination row ──
        nav_row = []
        if total_pages > 1:
            nav_row.append(InlineKeyboardButton(f"1/{total_pages}", callback_data="noop"))
            nav_row.append(InlineKeyboardButton("▶️ Next", callback_data=f"rpage_{uid}_{qkey}_1"))
            file_buttons.append(nav_row)

        # ── Filter buttons — 2 per row, Send All at bottom ──
        file_buttons.append([
            InlineKeyboardButton("🌐 Language", callback_data=f"flang_{uid}_{qkey}"),
            InlineKeyboardButton("📺 Season",   callback_data=f"fseason_{uid}_{qkey}"),
        ])
        file_buttons.append([
            InlineKeyboardButton("🎬 Episode",  callback_data=f"fepisode_{uid}_{qkey}"),
            InlineKeyboardButton("🔙 Back",     callback_data=f"fback_{uid}_{qkey}"),
        ])
        file_buttons.append([
            InlineKeyboardButton("📤 Send All (Premium)", callback_data=f"fsendall_{uid}_{qkey}"),
        ])

        kb = InlineKeyboardMarkup(file_buttons)
        t = gs.get("auto_delete_time", 300)
        mins = t // 60
        count_text = len(all_found)
        page_info = f" (1/{total_pages})" if total_pages > 1 else ""

        result_text = (
            f"🔍 {message.from_user.mention}\n\n"
            f"╔══ 🎯 **{count_text} Result{'s' if count_text > 1 else ''}{page_info}** ══╗\n\n"
            f"👇 File choose karo → PM mein file milegi!\n"
            f"🌐 Lang 📺 Season 🎬 Ep 📤 All — Premium filters\n"
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
        await query.answer()
        buttons = []
        if miniapp_url:
            # WebAppInfo only works in private — use URL button (works everywhere)
            buttons.append([InlineKeyboardButton("🌐 Mini App mein Plans Dekho", url=miniapp_url)])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="show_premium")])
        try:
            await query.message.edit(
                "💎 Premium ke liye Mini App kholo!\n\n"
                "Silver ₹50 | Gold ₹150 | Diamond ₹200\n"
                "Elite ₹800/saal | Group Premium bhi!\n\n"
                "Choose karo → Pay karo → Active!",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception:
            await query.answer("Mini App: " + (miniapp_url or "unavailable"), show_alert=True)

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

    args = message.command
    # /requests clear — sab delete karo
    if len(args) > 1 and args[1].lower() == "clear":
        await requests_col.delete_many({})
        await message.reply(f"✅ {total} requests clear kar di!")
        return

    # /requests <number> — specific request ka detail
    if len(args) > 1:
        try:
            req_num = int(args[1])
            reqs = []
            async for r in requests_col.find({}).sort("time", -1):
                reqs.append(r)
            if 0 < req_num <= len(reqs):
                req = reqs[req_num-1]
                uid = req.get("user_id")
                txt = req.get("request","")
                name = req.get("name","?")
                t = req.get("time")
                time_str = make_aware(t).astimezone(IST).strftime("%d %b %H:%M") if t else "N/A"
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📥 Upload Karo", callback_data=f"req_upload_{uid}_{txt[:30]}")],
                    [InlineKeyboardButton("🗑 Delete Request", callback_data=f"req_del_{str(req['_id'])}")]
                ])
                await message.reply(
                    f"📩 **Request #{req_num}**\n\n"
                    f"👤 {name} (`{uid}`)\n"
                    f"📁 `{txt}`\n"
                    f"🕐 {time_str}",
                    reply_markup=kb
                )
            else:
                await message.reply(f"❌ #{req_num} nahi mila. Total: {len(reqs)}")
            return
        except ValueError:
            pass

    # Default: list dikho
    text = f"📩 **Requests ({total})**\n\n"
    i = 1
    async for req in requests_col.find({}).sort("time", -1).limit(15):
        t = req.get("time")
        time_str = make_aware(t).astimezone(IST).strftime("%d %b") if t else ""
        text += f"**{i}.** `{req.get('request','?')}` — {req.get('name','?')} ({time_str})\n"
        i += 1
    if total > 15:
        text += f"\n...aur {total-15} hain."
    text += f"\n\n`/requests <number>` — detail dekho\n`/requests clear` — sab delete karo"
    await message.reply(text)

@bot.on_callback_query(filters.regex(r"^req_del_"))
async def req_delete_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    req_id = query.data.replace("req_del_", "")
    try:
        await requests_col.delete_one({"_id": ObjectId(req_id)})
        await query.message.edit_reply_markup(None)
        await query.answer("✅ Request delete ho gayi!", show_alert=False)
    except Exception as e:
        await query.answer(f"❌ {e}", show_alert=True)

@bot.on_callback_query(filters.regex(r"^req_upload_"))
async def req_upload_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True); return
    parts = query.data.split("_", 3)
    user_id = int(parts[2]) if len(parts) > 2 else None
    movie_name = parts[3] if len(parts) > 3 else "file"
    await query.answer()
    await query.message.reply(
        f"📥 **Upload Guide**\n\n"
        f"👤 User: `{user_id}`\n"
        f"📁 File: `{movie_name}`\n\n"
        f"1. File ko FILE_CHANNEL mein upload karo\n"
        f"2. File ka message ID lo\n"
        f"3. `/notify {user_id} <message>` se user ko batao"
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
    sm = await message.reply("📡 Shuru...")
    total = done = failed = blocked = deleted = 0

    if target in ["users","all"]:
        user_ids = []
        async for doc in users_col.find({}, {"user_id": 1}):
            if doc.get("user_id"):
                user_ids.append(doc["user_id"])

        for uid in user_ids:
            total += 1
            try:
                await client.send_message(uid, text)
                done += 1
                await asyncio.sleep(0.05)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    await client.send_message(uid, text)
                    done += 1
                except: failed += 1
            except (UserIsBlocked, InputUserDeactivated):
                blocked += 1
                deleted += 1
                # Blocked/deactivated — user data clean karo
                await users_col.delete_one({"user_id": uid})
                await premium_col.delete_one({"user_id": uid})
                await verify_log_col.delete_many({"user_id": uid})
            except PeerIdInvalid:
                blocked += 1
                deleted += 1
                await users_col.delete_one({"user_id": uid})
            except Exception as e:
                failed += 1
                logger.warning(f"broadcast user {uid}: {e}")

            # Progress update every 50
            if total % 50 == 0:
                try:
                    await sm.edit(f"📡 Broadcasting... {total} done")
                except: pass

    if target in ["groups","all"]:
        group_ids = []
        # Premium groups exclude karo
        prem_group_ids = set()
        async for pg in group_prem_col.find({"status": "approved"}, {"chat_id": 1}):
            if pg.get("chat_id"): prem_group_ids.add(pg["chat_id"])

        async for doc in groups_col.find({}, {"chat_id": 1}):
            cid = doc.get("chat_id")
            if cid and cid not in prem_group_ids:
                group_ids.append(cid)

        for cid in group_ids:
            total += 1
            try:
                await client.send_message(cid, text)
                done += 1
                await asyncio.sleep(0.12)
            except FloodWait as e:
                await asyncio.sleep(min(e.value, 30))
                try:
                    await client.send_message(cid, text)
                    done += 1
                except: failed += 1
            except (ChatWriteForbidden, PeerIdInvalid):
                failed += 1
                deleted += 1
                await groups_col.delete_one({"chat_id": cid})
                await group_settings_col.delete_one({"chat_id": cid})
                await group_prem_col.delete_one({"chat_id": cid})
                await group_sl_col.delete_many({"chat_id": cid})
            except Exception as e:
                failed += 1
                logger.warning(f"broadcast group {cid}: {e}")

            if total % 20 == 0:
                try:
                    await sm.edit(f"📡 Broadcasting... {total} done | ✅{done} ❌{failed}")
                except: pass

    # Stats ko bhi update karo — deleted users/groups show nahi hone chahiye
    await sm.edit(
        f"📡 Broadcast Complete!\n\n"
        f"Total: {total}\n"
        f"✅ Success: {done}\n"
        f"❌ Failed: {failed}\n"
        f"🚫 Blocked/Left: {blocked}\n"
        f"🗑 Data Cleaned: {deleted} (stats se bhi hataye)"
    )

@bot.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
    buttons = [[InlineKeyboardButton("🔙 Back", callback_data="back_main")]]
    if miniapp_url:
        buttons.insert(0, [InlineKeyboardButton("🌐 Mini App", web_app=WebAppInfo(url=miniapp_url))])
    await message.reply(
        "📖 Bot Guide\n"
        "━━━━━━━━━━━━━━━━\n\n"
        "Kaise use karein:\n"
        "1️⃣ Channel join karo\n"
        "2️⃣ Group mein movie naam type karo\n"
        "3️⃣ Button milega — click karo\n"
        "4️⃣ PM mein file aa jaayegi 📥\n\n"
        "File kuch der baad delete hogi — save kar lo!\n\n"
        "━━━━━━━━━━━━━━━━\n"
        "Commands:\n"
        "/premium — Plans & status\n"
        "/mystats — Teri stats\n"
        "/referlink — Refer link\n"
        "/request naam — File request karo\n\n"
        "Admin Commands:\n"
        "/admin — Admin panel\n"
        "/addpremium id days — Premium do\n"
        "/broadcast users/groups msg\n"
        "/stats — Bot stats\n"
        "/settings — Settings\n"
        "/setcommands — Commands set karo",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_message(filters.command("premium"))
async def premium_info(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M") if exp else "—"
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None

    if prem:
        text = (
            f"💎 Premium Active!\n\n"
            f"Expiry: {exp_str}\n\n"
            f"Enjoy karo:\n"
            f"• No verify, no force join\n"
            f"• 10 results per search\n"
            f"• Stream + Download\n"
            f"• PM Search\n\n"
            f"Renew ke liye Mini App kholo!"
        )
    else:
        text = (
            f"💎 Premium nahi hai abhi\n\n"
            f"Plans dekhne ke liye Mini App kholo.\n"
            f"Wahan sab plans hain — choose karo, pay karo,\n"
            f"owner verify karega, auto activate!\n\n"
            f"🆓 10 Refer = 15 din FREE Premium!"
        )

    buttons = []
    if miniapp_url:
        buttons.append([InlineKeyboardButton(
            "💎 Mini App mein Plans Dekho" if not prem else "🌐 Mini App Kholo",
            web_app=WebAppInfo(url=miniapp_url)
        )])
    await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

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

@bot.on_message(filters.command("notify") & filters.user(ADMINS))
async def notify_user(client, message: Message):
    """
    /notify <user_id> <message>
    User ko batao ki uski requested file upload ho gayi
    """
    args = message.command
    if len(args) < 3:
        await message.reply(
            "Usage: `/notify <user_id> <message>`\n\n"
            "Example:\n"
            "`/notify 123456789 Aapki Kalki 2898 AD file upload ho gayi!`"
        )
        return
    try:
        uid = int(args[1])
        msg_text = " ".join(args[2:])
    except:
        await message.reply("❌ Valid user_id do")
        return
    try:
        await client.send_message(
            uid,
            f"📢 **Admin Message**\n\n{msg_text}\n\n"
            f"Group mein search karke download karo! 🗂"
        )
        await message.reply(f"✅ `{uid}` ko notify kar diya!")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

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
    # User commands
    user_cmds = [
        BotCommand("start",       "Bot shuru karo"),
        BotCommand("help",        "Help aur guide dekho"),
        BotCommand("premium",     "Premium plans aur status dekho"),
        BotCommand("mystats",     "Apni stats — downloads, refers, expiry"),
        BotCommand("referlink",   "Refer link lo — 10 refer = 15 din free premium"),
        BotCommand("request",     "Movie/file request karo"),
    ]
    # Admin commands (owner only scope)
    admin_cmds = [
        BotCommand("addpremium",      "User ko premium do"),
        BotCommand("removepremium",   "User ka premium hatao"),
        BotCommand("ban",             "User ko ban karo"),
        BotCommand("unban",           "User ko unban karo"),
        BotCommand("broadcast",       "Sab users/groups ko message bhejo"),
        BotCommand("stats",           "Bot stats dekho"),
        BotCommand("requests",        "File requests list dekho"),
        BotCommand("notify",          "User ko notify karo"),
        BotCommand("ping",            "Bot ping check karo"),
        BotCommand("settings",        "Bot settings dekho/badlo"),
        BotCommand("setdelete",       "Auto delete time set karo"),
        BotCommand("setlimit",        "Daily download limit set karo"),
        BotCommand("setresults",      "Results count set karo"),
        BotCommand("maintenance",     "Maintenance mode on/off"),
        BotCommand("shortlink",       "Shortlink on/off"),
        BotCommand("addshortlink",    "New shortlink add karo"),
        BotCommand("removeshortlink", "Shortlink remove karo"),
        BotCommand("shortlinks",      "Shortlinks list dekho"),
        BotCommand("forcesub",        "Force sub on/off"),
        BotCommand("fsub",            "Force sub channels manage karo"),
        BotCommand("setcommands",     "Bot commands set karo"),
        BotCommand("gsettings",       "Group settings manage karo"),
        BotCommand("gshortlink",      "Group shortlink add karo"),
        BotCommand("gshortlinkremove","Group shortlink remove karo"),
        BotCommand("gshortlinks",     "Group shortlinks list dekho"),
    ]
    await client.set_bot_commands(user_cmds)
    # Admin commands separately (owner scope)
    try:
        from pyrogram.types import BotCommandScopeChat
        for admin_id in ADMINS:
            try:
                await client.set_bot_commands(user_cmds + admin_cmds, scope=BotCommandScopeChat(chat_id=admin_id))
            except: pass
    except: pass
    all_cmds = user_cmds + admin_cmds
    cmds_text = "\n".join(f"/{c.command} — {c.description}" for c in all_cmds)
    await message.reply(f"✅ Bot Commands Set Ho Gayi!\n\n📋 User commands: {len(user_cmds)}\n👑 Admin commands: {len(admin_cmds)}\n\n{cmds_text}", parse_mode=enums.ParseMode.DISABLED)

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
            kb.append([InlineKeyboardButton("💰 Group Premium Lo", url=miniapp_url)])
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
#  FILTER CALLBACKS — Language/Season/Episode/SendAll
# ═══════════════════════════════════════

LANGUAGES = [
    ("🇬🇧 English", "english"), ("🇮🇳 Hindi", "hindi"),
    ("🎭 Malayalam", "malayalam"), ("🎵 Tamil", "tamil"),
    ("🎬 Telugu", "telugu"), ("🌸 Bengali", "bengali"),
    ("🎪 Kannada", "kannada"), ("🎠 Punjabi", "punjabi"),
]


# ═══════════════════════════════════════
#  RESULT PAGINATION CALLBACK
# ═══════════════════════════════════════
# Store search results temporarily in memory
_result_cache = {}  # {uid_qkey: [found_msgs]}

@bot.on_callback_query(filters.regex(r"^noop$"))
async def noop_cb(client, query: CallbackQuery):
    await query.answer()

@bot.on_callback_query(filters.regex(r"^rpage_"))
async def result_page_cb(client, query: CallbackQuery):
    """Result pages — next/prev"""
    parts = query.data.split("_", 3)
    if len(parts) < 4:
        await query.answer("❌ Error", show_alert=True); return
    uid = int(parts[1])
    qkey = parts[2]
    page = int(parts[3])

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye aapka search nahi!", show_alert=True); return

    # Cache se results lo
    cache_key = f"{uid}_{qkey}"
    found = _result_cache.get(cache_key)
    if not found:
        # Re-search
        query_text = qkey.replace("_", " ")
        s = await get_settings()
        prem = await is_premium(uid)
        limit = 10 if prem else s.get("free_results", 5)
        found = await do_search(query_text, limit=limit)
        if not found:
            await query.answer("😕 Results expired, dobara search karo", show_alert=True); return
        _result_cache[cache_key] = found

    page_size = 5
    total_pages = max(1, (len(found) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    page_found = found[page * page_size:(page + 1) * page_size]

    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(page_found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', fname)
        fname_clean = re.sub(r'_+', ' ', fname_clean).strip()
        fname_show = fname_clean[:36] if fname_clean else f"File {page*page_size+idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

    # Navigation row
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rpage_{uid}_{qkey}_{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("▶️ Next", callback_data=f"rpage_{uid}_{qkey}_{page+1}"))
    if nav: buttons.append(nav)

    # Filter buttons — 2 per row
    buttons.append([
        InlineKeyboardButton("🌐 Language", callback_data=f"flang_{uid}_{qkey}"),
        InlineKeyboardButton("📺 Season",   callback_data=f"fseason_{uid}_{qkey}"),
    ])
    buttons.append([
        InlineKeyboardButton("🎬 Episode",  callback_data=f"fepisode_{uid}_{qkey}"),
        InlineKeyboardButton("🔙 Back",     callback_data=f"fback_{uid}_{qkey}"),
    ])
    buttons.append([
        InlineKeyboardButton("📤 Send All (Premium)", callback_data=f"fsendall_{uid}_{qkey}"),
    ])

    try:
        await query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
    except:
        pass
    await query.answer(f"Page {page+1}/{total_pages}")

@bot.on_callback_query(filters.regex(r"^flang_"))
async def lang_filter_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 2)
    uid = int(parts[1]) if len(parts) > 1 else 0
    search_q = parts[2] if len(parts) > 2 else ""

    # Language buttons — sabke liye free
    kb_rows = []
    for i in range(0, len(LANGUAGES), 2):
        row = []
        for lang_name, lang_key in LANGUAGES[i:i+2]:
            row.append(InlineKeyboardButton(
                lang_name,
                callback_data=f"lang_{uid}_{lang_key}_{search_q[:20]}"
            ))
        kb_rows.append(row)
    kb_rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{search_q[:20]}")])

    await query.message.edit_reply_markup(InlineKeyboardMarkup(kb_rows))
    await query.answer("Koi bhi language choose karo:", show_alert=False)

@bot.on_callback_query(filters.regex(r"^lang_"))
async def lang_select_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 3)
    uid = int(parts[1]) if len(parts) > 1 else 0
    lang_key = parts[2] if len(parts) > 2 else ""
    search_q = parts[3] if len(parts) > 3 else ""

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye button aapke liye nahi!", show_alert=True)
        return

    await query.answer(f"🔍 {lang_key.title()} results dhundh raha hoon...", show_alert=False)

    # Re-search with language filter
    real_q = search_q.replace("_", " ").strip()
    combined_q = f"{real_q} {lang_key}"
    found = await do_search(combined_q, limit=10)
    if not found:
            google_q = search_q.replace(" ", "+")
            filter_url = f"https://t.me/asfilter_bot?start={search_q.replace(chr(32),chr(95))}"
            google_url = f"https://www.google.com/search?q={google_q}+telegram"
            await query.message.edit_text(
                f"😕 {lang_key.title()} mein '{search_q}' nahi mila\n\n"
                f"Niche se try karo:",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 @asfilter_bot mein dhundho", url=filter_url)
                ],[
                    InlineKeyboardButton("🌐 Google mein dhundho", url=google_url)
                ]])
            )
            return
    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', fname)
        fname_clean = re.sub(r'_+', ' ', fname_clean).strip()
        fname_show = fname_clean[:38] if fname_clean else f"File {idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

    # Language selector — current language ✅
    lang_row = []
    for lang_name, lk in LANGUAGES[:4]:
        tick = "✅ " if lk == lang_key else ""
        lang_row.append(InlineKeyboardButton(tick + lang_name.split()[-1], callback_data=f"lang_{uid}_{lk}_{search_q[:15]}"))
    buttons.append(lang_row)
    lang_row2 = []
    for lang_name, lk in LANGUAGES[4:]:
        tick = "✅ " if lk == lang_key else ""
        lang_row2.append(InlineKeyboardButton(tick + lang_name.split()[-1], callback_data=f"lang_{uid}_{lk}_{search_q[:15]}"))
    buttons.append(lang_row2)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{search_q[:20]}")])

    await query.message.edit_text(
        f"🌐 **{lang_key.title()}** — {len(found)} Results\n\n"
        f"👇 File ka button dabao:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_callback_query(filters.regex(r"^fseason_"))
async def season_filter_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 2)
    uid = int(parts[1]) if len(parts) > 1 else 0
    qkey = parts[2] if len(parts) > 2 else ""
    search_q = qkey.replace("_", " ").strip()

    # Season — sabke liye

    # Season buttons — S01 to S20 (page 1), more on next page + Full Season
    season_buttons = []
    season_buttons.append([InlineKeyboardButton("📦 Full Season", callback_data=f"season_{uid}_fullseason_{qkey}")])
    # S01-S05
    season_buttons.append([InlineKeyboardButton(f"S{i:02d}", callback_data=f"season_{uid}_s{i:02d}_{qkey}") for i in range(1,6)])
    # S06-S10
    season_buttons.append([InlineKeyboardButton(f"S{i:02d}", callback_data=f"season_{uid}_s{i:02d}_{qkey}") for i in range(6,11)])
    # S11-S15
    season_buttons.append([InlineKeyboardButton(f"S{i:02d}", callback_data=f"season_{uid}_s{i:02d}_{qkey}") for i in range(11,16)])
    # S16-S20
    season_buttons.append([InlineKeyboardButton(f"S{i:02d}", callback_data=f"season_{uid}_s{i:02d}_{qkey}") for i in range(16,21)])
    # More seasons button
    season_buttons.append([
        InlineKeyboardButton("▶️ S21-S40", callback_data=f"spage_{uid}_21_{qkey}"),
        InlineKeyboardButton("▶️ S41-S60", callback_data=f"spage_{uid}_41_{qkey}"),
        InlineKeyboardButton("▶️ S61-S100", callback_data=f"spage_{uid}_61_{qkey}"),
    ])
    season_buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{qkey}")])

    await query.message.edit_reply_markup(InlineKeyboardMarkup(season_buttons))
    await query.answer("Season choose karo:", show_alert=False)

@bot.on_callback_query(filters.regex(r"^season_"))
async def season_select_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 3)
    uid = int(parts[1]) if len(parts) > 1 else 0
    season_key = parts[2] if len(parts) > 2 else ""
    search_q = parts[3] if len(parts) > 3 else ""

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye button aapke liye nahi!", show_alert=True); return

    await query.answer(f"🔍 {season_key.upper()} dhundh raha hoon...", show_alert=False)
    search_q = search_q.replace("_", " ").strip()
    combined_q = f"{search_q} {season_key}"
    found = await do_search(combined_q, limit=10)

    if not found:
        google_q = search_q.replace(" ","+")
        filter_url = f"https://t.me/asfilter_bot?start={search_q.replace(chr(32),chr(95))}"
        google_url = f"https://www.google.com/search?q={google_q}+telegram"
        await query.message.edit_text(
            f"😕 {season_key.upper()} mein '{search_q}' nahi mila\nNiche se try karo:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 @asfilter_bot mein dhundho", url=filter_url)
            ],[
                InlineKeyboardButton("🌐 Google mein dhundho", url=google_url)
            ]])
        )
        return

    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', re.sub(r'_+', ' ', fname)).strip()
        fname_show = fname_clean[:38] if fname_clean else f"File {idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

    # Season selector row
    s_row1 = [InlineKeyboardButton("📦 Full", callback_data=f"season_{uid}_full season_{search_q[:12]}")]
    for i in range(1, 4):
        tick = "✅" if f"s0{i}" == season_key else f"S0{i}"
        s_row1.append(InlineKeyboardButton(tick, callback_data=f"season_{uid}_s0{i}_{search_q[:12]}"))
    buttons.append(s_row1)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{search_q[:20]}")])

    await query.message.edit_text(
        f"📺 **{season_key.upper()}** — {len(found)} Results\n\n👇 File choose karo:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_callback_query(filters.regex(r"^fepisode_"))
async def episode_filter_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 2)
    uid = int(parts[1]) if len(parts) > 1 else 0
    qkey = parts[2] if len(parts) > 2 else ""
    search_q = qkey.replace("_", " ").strip()

    # Episode — sabke liye

    # Episode buttons — E01 to E20 (page 1), next pages for more
    ep_buttons = []
    ep_buttons.append([InlineKeyboardButton(f"E{i:02d}", callback_data=f"ep_{uid}_e{i:02d}_{qkey}") for i in range(1, 6)])
    ep_buttons.append([InlineKeyboardButton(f"E{i:02d}", callback_data=f"ep_{uid}_e{i:02d}_{qkey}") for i in range(6, 11)])
    ep_buttons.append([InlineKeyboardButton(f"E{i:02d}", callback_data=f"ep_{uid}_e{i:02d}_{qkey}") for i in range(11, 16)])
    ep_buttons.append([InlineKeyboardButton(f"E{i:02d}", callback_data=f"ep_{uid}_e{i:02d}_{qkey}") for i in range(16, 21)])
    ep_buttons.append([
        InlineKeyboardButton("▶️ E21-E40",  callback_data=f"epage_{uid}_21_{search_q[:15]}"),
        InlineKeyboardButton("▶️ E41-E60",  callback_data=f"epage_{uid}_41_{search_q[:15]}"),
        InlineKeyboardButton("▶️ E61-E80",  callback_data=f"epage_{uid}_61_{search_q[:15]}"),
        InlineKeyboardButton("▶️ E81-E100", callback_data=f"epage_{uid}_81_{qkey}"),
    ])
    ep_buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{qkey}")])

    await query.message.edit_reply_markup(InlineKeyboardMarkup(ep_buttons))
    await query.answer("Episode number choose karo:", show_alert=False)

@bot.on_callback_query(filters.regex(r"^ep_"))
async def ep_select_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 3)
    uid = int(parts[1]) if len(parts) > 1 else 0
    ep_key = parts[2] if len(parts) > 2 else ""
    search_q = parts[3] if len(parts) > 3 else ""

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye button aapke liye nahi!", show_alert=True); return

    await query.answer(f"🔍 {ep_key.upper()} dhundh raha hoon...", show_alert=False)
    combined_q = f"{search_q} {ep_key}"
    found = await do_search(combined_q, limit=5)

    if not found:
        google_q = search_q.replace(" ","+")
        filter_url = f"https://t.me/asfilter_bot?start={search_q.replace(chr(32),chr(95))}"
        google_url = f"https://www.google.com/search?q={google_q}+telegram"
        await query.message.edit_text(
            f"😕 {ep_key.upper()} mein '{search_q}' nahi mila\nNiche se try karo:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 @asfilter_bot mein dhundho", url=filter_url)
            ],[
                InlineKeyboardButton("🌐 Google mein dhundho", url=google_url)
            ]])
        )
        return

    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', re.sub(r'_+', ' ', fname)).strip()
        fname_show = fname_clean[:38] if fname_clean else f"File {idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{uid}_{search_q[:20]}")])

    await query.message.edit_text(
        f"🎬 **{ep_key.upper()}** — {len(found)} Results\n\n👇 File choose karo:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

@bot.on_callback_query(filters.regex(r"^fsendall_"))
async def sendall_cb(client, query: CallbackQuery):
    parts = query.data.split("_", 2)
    uid = int(parts[1]) if len(parts) > 1 else 0
    qkey = parts[2] if len(parts) > 2 else ""
    search_q = qkey.replace("_", " ").strip()

    # Send All — sabke liye free

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye button aapke liye nahi!", show_alert=True); return

    await query.answer("📤 Sab files bhej raha hoon PM mein...", show_alert=False)

    cache_key = f"{uid}_{qkey}"
    found = _result_cache.get(cache_key) or await do_search(search_q, limit=10)
    if not found:
        await query.answer("😕 Koi file nahi mili!", show_alert=True); return

    sent_count = 0
    for fmsg in found:
        try:
            s = await get_settings()
            t = s.get("auto_delete_time", 300)
            fname = get_file_name(fmsg)
            cap = f"🗂 **{fname}**\n\n⏳ {t//60} min baad delete hogi!"
            sent = await fmsg.copy(
                chat_id=uid,
                caption=cap,
                parse_mode=enums.ParseMode.MARKDOWN
            )
            await increment_daily(uid)
            if s.get("auto_delete"):
                asyncio.create_task(del_later(sent, t))
            sent_count += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"sendall error: {e}")

    await query.message.edit_text(
        f"✅ **{sent_count} files** aapke PM mein bhej di!\n\n"
        f"📌 Save kar lo — {t//60} min baad delete hongi."
    )


@bot.on_callback_query(filters.regex(r"^spage_"))
async def season_page_cb(client, query: CallbackQuery):
    """Season pagination — S21-S40, S41-S60, S61-S100"""
    # Season page — free
    parts = query.data.split("_", 3)
    uid = int(parts[1]) if len(parts) > 1 else 0
    start = int(parts[2]) if len(parts) > 2 else 21
    search_q = parts[3] if len(parts) > 3 else ""
    end = min(start + 19, 100)
    buttons = []
    for i in range(start, end+1, 5):
        row = [InlineKeyboardButton(f"S{j:02d}", callback_data=f"season_{uid}_s{j:02d}_{search_q[:15]}") for j in range(i, min(i+5, end+1))]
        buttons.append(row)
    # Nav row
    nav = []
    if start > 1:
        prev = max(1, start - 20)
        nav.append(InlineKeyboardButton(f"◀️ S{prev:02d}-S{start-1:02d}", callback_data=f"spage_{uid}_{prev}_{search_q[:15]}"))
    if end < 100:
        nav.append(InlineKeyboardButton(f"▶️ S{end+1:02d}-S{min(end+20,100):02d}", callback_data=f"spage_{uid}_{end+1}_{search_q[:15]}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fseason_{uid}_{search_q[:20]}")])
    await query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
    await query.answer(f"Season {start}-{end}", show_alert=False)

@bot.on_callback_query(filters.regex(r"^epage_"))
async def episode_page_cb(client, query: CallbackQuery):
    """Episode pagination — E21-E40, E41-E60, E61-E80, E81-E100"""
    # Episode page — free
    parts = query.data.split("_", 3)
    uid = int(parts[1]) if len(parts) > 1 else 0
    start = int(parts[2]) if len(parts) > 2 else 21
    search_q = parts[3] if len(parts) > 3 else ""
    end = min(start + 19, 100)
    buttons = []
    for i in range(start, end+1, 5):
        row = [InlineKeyboardButton(f"E{j:02d}", callback_data=f"ep_{uid}_e{j:02d}_{search_q[:15]}") for j in range(i, min(i+5, end+1))]
        buttons.append(row)
    nav = []
    if start > 1:
        prev = max(1, start - 20)
        nav.append(InlineKeyboardButton(f"◀️ E{prev:02d}-E{start-1:02d}", callback_data=f"epage_{uid}_{prev}_{search_q[:15]}"))
    if end < 100:
        nav.append(InlineKeyboardButton(f"▶️ E{end+1:02d}-E{min(end+20,100):02d}", callback_data=f"epage_{uid}_{end+1}_{search_q[:15]}"))
    if nav: buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fepisode_{uid}_{search_q[:20]}")])
    await query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
    await query.answer(f"Episode {start}-{end}", show_alert=False)

@bot.on_callback_query(filters.regex(r"^fback_"))
async def filter_back_cb(client, query: CallbackQuery):
    """Filter back — original results dikho"""
    parts = query.data.split("_", 2)
    uid = int(parts[1]) if len(parts) > 1 else 0
    qkey = parts[2] if len(parts) > 2 else ""
    search_q = qkey.replace("_", " ").strip()

    if query.from_user.id != uid and query.from_user.id not in ADMINS:
        await query.answer("❌ Ye button aapke liye nahi!", show_alert=True); return

    await query.answer("🔄 Original results...", show_alert=False)
    found = await do_search(search_q, limit=10)

    if not found:
        await query.message.edit_text(f"😕 '{search_q}' nahi mila")
        return

    me = await client.get_me()
    buttons = []
    for idx, fmsg in enumerate(found):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', re.sub(r'_+', ' ', fname)).strip()
        fname_show = fname_clean[:38] if fname_clean else f"File {idx+1}"
        fsize = get_file_size(fmsg)
        size_text = f" [{fsize}]" if fsize else ""
        final_link = f"https://t.me/{me.username}?start=getfile_{uid}_{fmsg.id}"
        buttons.append([InlineKeyboardButton(f"📥 {fname_show}{size_text}", url=final_link)])

    buttons += [
        [
            InlineKeyboardButton("🌐 Language", callback_data=f"flang_{uid}_{qkey}"),
            InlineKeyboardButton("📺 Season",   callback_data=f"fseason_{uid}_{qkey}"),
        ],
        [
            InlineKeyboardButton("🎬 Episode",  callback_data=f"fepisode_{uid}_{qkey}"),
            InlineKeyboardButton("🔙 Back",     callback_data=f"fback_{uid}_{qkey}"),
        ],
        [
            InlineKeyboardButton("📤 Send All (Premium)", callback_data=f"fsendall_{uid}_{qkey}"),
        ],
    ]

    await query.message.edit_text(
        f"🔍 **{len(found)} Results** — {search_q}\n\n👇 File ka button dabao:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

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



# ═══════════════════════════════════════
#  GROUP PREMIUM STATS (/gstats)
# ═══════════════════════════════════════
@bot.on_message(filters.command("gstats") & filters.user(ADMINS))
async def group_prem_stats(client, message: Message):
    """Group premium status dekho by ID"""
    args = message.command
    if len(args) < 2:
        # Show all premium groups
        cursor = group_prem_col.find({"status": "approved"})
        groups = await cursor.to_list(length=50)
        if not groups:
            await message.reply("📭 Koi group premium nahi abhi.")
            return
        text = "Premium Groups\n" + "="*22 + "\n\n"
        for g in groups[:20]:
            exp = make_aware(g["expiry"]) if g.get("expiry") else None
            is_active = now() < exp if exp else False
            status = "✅ Active" if is_active else "❌ Expired"
            exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
            text += status + ' | ' + str(g.get('chat_id','')) + ' | ' + exp_str + chr(10)
        return

    # Specific group ID
    try:
        g_id = int(args[1])
    except ValueError:
        await message.reply("❌ Group ID number dalo. Example: `/gstats -1001234567890`")
        return

    doc = await group_prem_col.find_one({"chat_id": g_id})
    if not doc:
        await message.reply(f"📭 Group `{g_id}` ko premium nahi mila abhi.")
        return

    exp = make_aware(doc["expiry"]) if doc.get("expiry") else None
    is_active = now() < exp if exp else False
    exp_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M") if exp else "N/A"
    days_left = (exp - now()).days if exp and is_active else 0

    # Try to get group name
    try:
        chat = await client.get_chat(g_id)
        chat_name = chat.title or "Unknown"
    except Exception:
        chat_name = "Unknown"

    status_str = "Active" if is_active else "Expired"
    text = (
        status_str + " — Group Premium\n"
        + "="*22 + "\n\n"
        + "Group: " + chat_name + "\n"
        + "ID: " + str(g_id) + "\n"
        + "Owner: " + str(doc.get("owner_id","N/A")) + "\n"
        + "Days: " + str(doc.get("days","?")) + " din\n"
        + "Expiry: " + exp_str + "\n"
        + "Bacha: " + str(days_left) + " din"
    )
    kb = None
    if is_active:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Remove Group Premium", callback_data=f"grm_prem_{g_id}")
        ]])
    await message.reply(text, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^grm_prem_") & filters.user(ADMINS))
async def remove_group_prem_cb(client, query: CallbackQuery):
    g_id = int(query.data.split("_")[2])
    await group_prem_col.delete_one({"chat_id": g_id})
    await query.message.edit_reply_markup(None)
    await query.answer("✅ Group Premium remove kiya!", show_alert=True)
    await query.message.reply(f"❌ Group `{g_id}` ka premium remove ho gaya.")


# NEW GSTATS HANDLER
@bot.on_message(filters.command("gstats") & filters.user(ADMINS))
async def group_prem_stats(client, message: Message):
    args = message.command
    if len(args) >= 2:
        try:
            g_id = int(args[1])
        except ValueError:
            await message.reply("Example: /gstats -1001234567890")
            return
        doc = await group_prem_col.find_one({"chat_id": g_id})
        if not doc:
            await message.reply(f"Group {g_id} premium nahi hai.")
            return
        exp = make_aware(doc["expiry"]) if doc.get("expiry") else None
        is_active = now() < exp if exp else False
        exp_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M") if exp else "N/A"
        days_left = max(0, (exp - now()).days) if exp and is_active else 0
        try:
            chat = await client.get_chat(g_id)
            chat_name = chat.title or "Unknown"
        except Exception:
            chat_name = str(g_id)
        status = "Active" if is_active else "Expired"
        text = (
            f"Group Premium Status\n"
            f"{'='*22}\n\n"
            f"Status: {status}\n"
            f"Group: {chat_name}\n"
            f"ID: {g_id}\n"
            f"Owner: {doc.get('owner_id','N/A')}\n"
            f"Days: {doc.get('days','?')} din\n"
            f"Expiry: {exp_str}\n"
            f"Bacha: {days_left} din"
        )
        kb = None
        if is_active:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("Remove Premium", callback_data=f"grmprem_{g_id}")
            ]])
        await message.reply(text, reply_markup=kb)
    else:
        cursor = group_prem_col.find({"status": "approved"})
        groups = await cursor.to_list(length=50)
        if not groups:
            await message.reply("Koi group premium nahi abhi.")
            return
        lines = ["Premium Groups\n" + "="*20 + "\n"]
        for g in groups[:20]:
            exp = make_aware(g["expiry"]) if g.get("expiry") else None
            is_active = now() < exp if exp else False
            exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
            st = "Active" if is_active else "Expired"
            lines.append(f"{st} | {g['chat_id']} | {exp_str}")
        await message.reply("\n".join(lines))

@bot.on_callback_query(filters.regex(r"^grmprem_") & filters.user(ADMINS))
async def rm_grp_prem_cb(client, query: CallbackQuery):
    g_id = int(query.data.split("_")[1])
    await group_prem_col.delete_one({"chat_id": g_id})
    await query.message.edit_reply_markup(None)
    await query.answer("Group Premium removed!", show_alert=True)


# ═══════════════════════════════════════
#  ADMIN PANEL
# ═══════════════════════════════════════
@bot.on_message(filters.command("admin") & filters.user(ADMINS) & filters.private)
async def admin_panel(client, message: Message):
    """Full admin control panel"""
    s = await get_settings()
    total_users = await users_col.count_documents({})
    total_groups = await groups_col.count_documents({})
    prem_users = await premium_col.count_documents({"expiry": {"$gt": now()}})
    pending_pay = await payments_col.count_documents({"status": "pending"})
    pending_req = await requests_col.count_documents({})
    banned_count = await banned_col.count_documents({})

    text = (
        f"👑 Admin Panel\n"
        f"{'─'*25}\n"
        f"👤 Users: {total_users}\n"
        f"👥 Groups: {total_groups}\n"
        f"💎 Premium: {prem_users}\n"
        f"🚫 Banned: {banned_count}\n"
        f"{'─'*25}\n"
        f"📩 Pending payments: {pending_pay}\n"
        f"📋 Pending requests: {pending_req}\n"
        f"{'─'*25}\n"
        f"🔧 Maintenance: {'ON' if s.get('maintenance') else 'OFF'}\n"
        f"🔗 Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"📢 Force sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"⏳ Auto delete: {s.get('auto_delete_time', 300)//60} min\n"
        f"📦 Free results: {s.get('free_results', 5)}\n"
        f"💎 Prem results: {s.get('premium_results', 10)}\n"
        f"📥 Daily limit: {s.get('daily_limit', 10)}"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔧 Maintenance ON" if not s.get('maintenance') else "🔧 Maintenance OFF",
                                 callback_data="ap_toggle_maintenance"),
            InlineKeyboardButton("🔗 SL ON" if not s.get('shortlink_enabled') else "🔗 SL OFF",
                                 callback_data="ap_toggle_shortlink"),
        ],
        [
            InlineKeyboardButton("📢 FSub ON" if not s.get('force_sub') else "📢 FSub OFF",
                                 callback_data="ap_toggle_forcesub"),
            InlineKeyboardButton("📩 Payments", callback_data="ap_payments"),
        ],
        [
            InlineKeyboardButton("📊 Full Stats", callback_data="ap_stats"),
            InlineKeyboardButton("📋 Requests", callback_data="ap_requests"),
        ],
        [
            InlineKeyboardButton("📡 Broadcast Users", callback_data="ap_bc_users"),
            InlineKeyboardButton("📡 Broadcast Groups", callback_data="ap_bc_groups"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="ap_refresh"),
        ],
    ])
    await message.reply(text, reply_markup=kb)

@bot.on_callback_query(filters.regex(r"^ap_"))
async def admin_panel_cb(client, query: CallbackQuery):
    if query.from_user.id not in ADMINS:
        await query.answer("❌ Sirf owner!", show_alert=True)
        return
    data = query.data

    if data == "ap_toggle_maintenance":
        s = await get_settings()
        new_val = not s.get("maintenance", False)
        await update_setting("maintenance", new_val)
        await query.answer(f"🔧 Maintenance: {'ON' if new_val else 'OFF'}")
        await admin_panel(client, query.message)
        return

    if data == "ap_toggle_shortlink":
        s = await get_settings()
        new_val = not s.get("shortlink_enabled", True)
        await update_setting("shortlink_enabled", new_val)
        await query.answer(f"🔗 Shortlink: {'ON' if new_val else 'OFF'}")
        await admin_panel(client, query.message)
        return

    if data == "ap_toggle_forcesub":
        s = await get_settings()
        new_val = not s.get("force_sub", True)
        await update_setting("force_sub", new_val)
        await query.answer(f"📢 Force sub: {'ON' if new_val else 'OFF'}")
        await admin_panel(client, query.message)
        return

    if data == "ap_payments":
        total = await payments_col.count_documents({})
        pending = await payments_col.count_documents({"status": "pending"})
        approved = await payments_col.count_documents({"status": "approved"})
        rejected = await payments_col.count_documents({"status": "rejected"})
        await query.answer()
        await query.message.reply(
            f"💳 Payments\n{'─'*20}\n"
            f"Total: {total}\nPending: {pending}\nApproved: {approved}\nRejected: {rejected}"
        )
        return

    if data == "ap_stats":
        await query.answer()
        await stats_cmd(client, query.message)
        return

    if data == "ap_requests":
        await query.answer()
        # Create fake message object for show_requests
        class FakeMsg:
            def __init__(self, m):
                self.chat = m.chat
                self.from_user = m.from_user
                self.command = ["requests"]
                async def reply(self, *a, **k): return await m.reply(*a, **k)
            reply = query.message.reply
        await show_requests(client, type('M', (), {'chat': query.message.chat, 'from_user': query.from_user, 'command': ['requests'], 'reply': query.message.reply})())
        return

    if data == "ap_bc_users":
        await query.answer()
        await query.message.reply(
            "📡 Users broadcast:\n\n`/broadcast users <message>`\n\nYa seedha reply karein:"
        )
        return

    if data == "ap_bc_groups":
        await query.answer()
        await query.message.reply(
            "📡 Groups broadcast:\n\n`/broadcast groups <message>`"
        )
        return

    if data == "ap_refresh":
        await query.answer("🔄 Refreshed!")
        await admin_panel(client, query.message)
        return

    await query.answer()

def start_bot():
    """
    Everything ek hi async event loop mein chalao:
    - Pyrogram bot + userbot
    - aiohttp streaming server (unlimited file sizes)
    - APScheduler
    """
    # database.py aur routes.py ko bot/userbot clients do
    db_set_clients(bot, userbot)
    routes_set_clients(bot, userbot)

    if userbot:
        userbot.start()
        logger.info("✅ Userbot started")
    else:
        logger.warning("⚠️ STRING_SESSION missing! Streaming limited.")

    logger.info("🚀 AsBhai Drop Bot starting...")

    def _start_scheduler_thread():
        import time
        time.sleep(4)
        try:
            loop = bot.loop
            if not loop or not loop.is_running():
                return
            async def _start():
                # aiohttp server start
                await run_aiohttp_server()
                # Scheduler
                def _run_cleanup():
                    if bot.loop and bot.loop.is_running():
                        asyncio.run_coroutine_threadsafe(cleanup(), bot.loop)
                scheduler.add_job(_run_cleanup, 'interval', hours=1)
                scheduler.start()
                logger.info("✅ Scheduler + aiohttp started")
            asyncio.run_coroutine_threadsafe(_start(), loop)
        except Exception as e:
            logger.error(f"Startup thread error: {e}")

    Thread(target=_start_scheduler_thread, daemon=True).start()
    bot.run()

if __name__ == "__main__":
    start_bot()
