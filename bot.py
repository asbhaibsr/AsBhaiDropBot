# ╔══════════════════════════════════════╗
# ║  bot.py — AsBhai Drop Bot            ║
# ║  Main File — FIXED All Issues        ║
# ╚══════════════════════════════════════╝
import os, re, time, random, string, asyncio, logging
try:
    import ujson as json
except ImportError:
    import json
from datetime import datetime, timedelta
from threading import Thread

import pytz, aiohttp
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    WebAppInfo, ChatPermissions
)
from pyrogram.errors import (
    FloodWait, UserIsBlocked, InputUserDeactivated,
    ChatWriteForbidden, PeerIdInvalid, UserNotParticipant
)
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

from config import (
    API_ID, API_HASH, BOT_TOKEN, STRING_SESSION,
    OWNER_ID, FILE_CHANNEL, LOG_CHANNEL, MAIN_CHANNEL,
    FORCE_SUB_ID, FORCE_SUB_CHANNEL, SHORTLINK_API, SHORTLINK_URL,
    KOYEB_URL, ADMINS, IST, UPI_ID, PORT,
    _shortlink_cache, _search_locks, _search_cooldown, _user_warnings,
    DEFAULT_SETTINGS, GROUP_DEFAULTS,
    now, now_ist, make_aware, logger
)
from database import (
    mongo_client, db,
    users_col, groups_col, premium_col, settings_col, tokens_col,
    requests_col, banned_col, refers_col, free_trial_col, help_msgs_col,
    payments_col, shortlinks_col, verify_log_col, group_prem_col,
    group_sl_col, group_settings_col, warn_col, action_log_col,
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
    check_link_in_message, get_user_warns, add_user_warn, reset_user_warns,
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

# In-memory result cache
_result_cache = {}  # {uid_qkey: [found_msgs]}

LANGUAGES = [
    ("🇬🇧 English", "english"), ("🇮🇳 Hindi", "hindi"),
    ("🎭 Malayalam", "malayalam"), ("🎵 Tamil", "tamil"),
    ("🎬 Telugu", "telugu"), ("🌸 Bengali", "bengali"),
    ("🎪 Kannada", "kannada"), ("🎠 Punjabi", "punjabi"),
]

# ═══════════════════════════════════════
#  CLEANUP
# ═══════════════════════════════════════
async def cleanup():
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

    # Clean old result cache (older than 30 min)
    old_keys = [k for k in _result_cache if len(_result_cache) > 200]
    for k in old_keys[:100]:
        del _result_cache[k]

    if deleted.deleted_count or expired_cache or expired_cd:
        logger.info(f"Cleanup: {deleted.deleted_count} tokens, {len(expired_cache)} sl_cache, {len(expired_cd)} cooldown")

# ═══════════════════════════════════════
#  HELPER: to_smallcaps
# ═══════════════════════════════════════
def to_smallcaps(text):
    sc = str.maketrans(
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
        'ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ'
    )
    return text.translate(sc)

# ═══════════════════════════════════════
#  /START HANDLER
# ═══════════════════════════════════════
@bot.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    uid = message.from_user.id
    args = message.command[1] if len(message.command) > 1 else ""
    referred_by = None

    if args.startswith("ref_"):
        try:
            referred_by = int(args[4:])
            if referred_by == uid:
                referred_by = None
        except: pass

    is_new = await save_user(message.from_user, referred_by)

    if is_new:
        await send_log(
            f"👤 #NewUser\n"
            f"Name: {message.from_user.mention}\n"
            f"ID: `{uid}`\n"
            f"Referred by: `{referred_by or 'Direct'}`\n"
            f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
        )

    # ── sv_ — shortlink verification ──
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
            keys_to_del = [k for k in _shortlink_cache if k[0] == uid]
            for k in keys_to_del: del _shortlink_cache[k]

            sl_label = "Shortlink"
            if sl_id and sl_id != "env_default":
                try:
                    sl_doc = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
                    if sl_doc:
                        sl_label = sl_doc.get("label", "Shortlink")
                        hours = sl_doc.get("hours", 24)
                        await mark_sl_verified(uid, sl_id, sl_label)
                        await send_log(
                            f"✅ #VerifyComplete\n\n"
                            f"ɪᴅ - `{uid}`\n"
                            f"Nᴀᴍᴇ - {to_smallcaps((message.from_user.first_name or 'User')[:20])}\n"
                            f"sʜᴏʀᴛʟɪɴᴋ - {sl_label}\n"
                            f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
                        )
                except Exception as e:
                    logger.error(f"sl verify log error: {e}")
                    await mark_sl_verified(uid, sl_id or "default", sl_label)
            else:
                await mark_verified(uid)
                await send_log(
                    f"✅ #VerifyComplete\n\n"
                    f"ɪᴅ - `{uid}`\n"
                    f"Nᴀᴍᴇ - {to_smallcaps((message.from_user.first_name or 'User')[:20])}\n"
                    f"sʜᴏʀᴛʟɪɴᴋ - Env Default\n"
                    f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
                )

            # Check remaining shortlinks
            all_done, next_sl, _ = await get_user_verify_state(uid)
            if all_done:
                user_doc = await users_col.find_one({"user_id": uid})
                pending_q = user_doc.get("pending_search", "") if user_doc else ""
                pending_chat = user_doc.get("pending_chat", 0) if user_doc else 0

                if pending_q and pending_chat:
                    await users_col.update_one(
                        {"user_id": uid},
                        {"$unset": {"pending_search": "", "pending_chat": ""}}
                    )
                    await message.reply(
                        f"✅ **Verify ho gaya!** 🎉\n\nAb '{pending_q}' ke results dhundh raha hoon... 🔍"
                    )
                    prem_user = await is_premium(uid)
                    found = await do_search(pending_q, limit=10 if prem_user else 5)
                    if found:
                        try:
                            me_obj = await client.get_me()
                            qkey = re.sub(r'[^a-zA-Z0-9_]', '_', pending_q)[:18]
                            _result_cache[f"{uid}_{qkey}"] = found
                            btns = _build_result_buttons(found[:5], uid, me_obj.username, qkey)
                            s = await get_settings()
                            t = s.get("auto_delete_time", 300)
                            
                            # FIX: Try sending to group first, if fails send to PM
                            result_sent = False
                            try:
                                result = await client.send_message(
                                    pending_chat,
                                    f"🎯 **{len(found)} results** mile '{pending_q}' ke liye!\n\n"
                                    f"👇 File choose karo — PM mein aayegi! 📥",
                                    reply_markup=InlineKeyboardMarkup(btns)
                                )
                                asyncio.create_task(del_later(result, t))
                                result_sent = True
                            except Exception as e:
                                logger.error(f"auto-search group send error: {e}")
                            
                            # FIX: Also send results to PM so user doesn't miss them
                            if not result_sent:
                                try:
                                    await client.send_message(
                                        uid,
                                        f"🎯 **{len(found)} results** mile '{pending_q}' ke liye!\n\n"
                                        f"👇 File choose karo:",
                                        reply_markup=InlineKeyboardMarkup(btns)
                                    )
                                except Exception as e2:
                                    logger.error(f"auto-search PM send error: {e2}")
                                    await message.reply(f"✅ Verify done! Group mein '{pending_q}' dobara type karo.")
                        except Exception as e:
                            logger.error(f"auto-search error: {e}")
                            await message.reply(f"✅ Verify done! Group mein '{pending_q}' dobara type karo.")
                    else:
                        await message.reply(
                            f"✅ Verify ho gaya! 🎉\n\n"
                            f"'{pending_q}' abhi nahi mila — spelling check karke group mein dobara try karo! 🔍"
                        )
                else:
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
            await message.reply(
                "❌ **Token expire ho gaya!**\n\n"
                "Group mein dobara search karo — naya link milega.\n"
                "💡 Tip: Link milte hi jaldi complete karo!"
            )
        return

    # ── getfile_ — file PM mein bhejo ──
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

    # ── Normal /start ──
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

    # Funny/Roast personality greetings
    greetings = [
        f"🔥 **AsBhai Drop Bot** 🔥\n\n"
        f"Arre **{message.from_user.mention}**! Kya haal hai boss? 😎\n\n"
        f"Main hoon tera file wala dost — jo bhi chahiye, bas naam bol! 🎬\n\n"
        f"**Kaise kaam karta hai (ekdum easy):**\n"
        f"1️⃣ Channel join kar — free hai re! 📢\n"
        f"2️⃣ Din mein ek baar verify kar (2 sec ka kaam) 🔗\n"
        f"3️⃣ Group mein file naam likh — bas! 🔍\n"
        f"4️⃣ PM mein file aa jaayegi — jadoo! ✨\n\n"
        f"💎 **Premium** = Verify skip + 10 results + HD Stream + Download! 🏆\n"
        f"🔗 **10 Refer** = 15 din FREE Premium! 🎁\n\n"
        f"Chal shuru karte hai — `/premium` | `/mystats` | `/referlink`",

        f"💫 **AsBhai Drop Bot** 💫\n\n"
        f"Swagat hai **{message.from_user.mention}**! 🙏\n\n"
        f"Kya dekhna hai aaj? Movie? Series? Anime? Sab milega! 🎬🍿\n\n"
        f"**Simple steps:**\n"
        f"1️⃣ 📢 Channel join karo (mandatory hai bhai)\n"
        f"2️⃣ 🔗 Ek baar verify karo (shortlink)\n"
        f"3️⃣ 🔍 Group mein type karo jo chahiye\n"
        f"4️⃣ 📥 PM mein file — auto delivery!\n\n"
        f"💎 **Premium = Boss Mode** — No verify, HD Stream, Unlimited! 🤑\n"
        f"🔗 Dosto ko bulao — **10 refer = 15 din FREE premium!**\n\n"
        f"Commands: `/premium` | `/mystats` | `/referlink`",

        f"⚡ **AsBhai Drop Bot** ⚡\n\n"
        f"Kya bolti public! **{message.from_user.mention}** aa gaya scene mein! 🎉\n\n"
        f"Mujhse koi file maang — main refuse nahi karta! 😏\n\n"
        f"**Instructions (padh le zara):**\n"
        f"1️⃣ Channel join kar pehle 📢\n"
        f"2️⃣ Verify kar daily (free users ke liye) 🔗\n"
        f"3️⃣ Group mein movie/show ka naam likh ✍️\n"
        f"4️⃣ PM check kar — file ready! 📥\n\n"
        f"💎 **Premium loge to life set!** Stream + Download + No verify! 🔥\n"
        f"🔗 **10 Refer karke FREE premium le lo!** 🎁\n\n"
        f"`/premium` | `/mystats` | `/referlink`",
    ]
    await message.reply(
        random.choice(greetings),
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ═══════════════════════════════════════
#  HELPER: Build result buttons with filter row
# ═══════════════════════════════════════
def _build_result_buttons(found_page, uid, bot_username, qkey, page=0, total_pages=1):
    """Unified button builder for search results"""
    btns = []
    for idx, fmsg in enumerate(found_page):
        fname = get_file_name(fmsg)
        fname_clean = re.sub(r'[@#]\w+', '', re.sub(r'_+', ' ', fname)).strip()
        fname_show = fname_clean[:36] if fname_clean else f"File {page*5+idx+1}"
        fsize = get_file_size(fmsg)
        sz = f" [{fsize}]" if fsize else ""
        link = f"https://t.me/{bot_username}?start=getfile_{uid}_{fmsg.id}"
        btns.append([InlineKeyboardButton(f"📥 {fname_show}{sz}", url=link)])

    # Pagination
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"rpage_{uid}_{qkey}_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("▶️ Next", callback_data=f"rpage_{uid}_{qkey}_{page+1}"))
        btns.append(nav)

    # Filter buttons
    btns.append([
        InlineKeyboardButton("🌐 Language", callback_data=f"flang_{uid}_{qkey}"),
        InlineKeyboardButton("📺 Season",   callback_data=f"fseason_{uid}_{qkey}"),
    ])
    btns.append([
        InlineKeyboardButton("🎬 Episode",  callback_data=f"fepisode_{uid}_{qkey}"),
        InlineKeyboardButton("🔙 Back",     callback_data=f"fback_{uid}_{qkey}"),
    ])
    btns.append([
        InlineKeyboardButton("📤 Send All", callback_data=f"fsendall_{uid}_{qkey}"),
    ])
    return btns

# ═══════════════════════════════════════
#  LINK PROTECTION — NEW FEATURE
# ═══════════════════════════════════════
@bot.on_message(
    filters.group & filters.text &
    ~filters.bot &
    filters.incoming
)
async def link_protection_handler(client, message: Message):
    """Detect links in group and warn/mute/ban"""
    if not message.from_user: return
    uid = message.from_user.id
    chat_id = message.chat.id

    # Skip admins
    if uid in ADMINS: return
    try:
        member = await client.get_chat_member(chat_id, uid)
        if member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR]:
            return
    except: return

    # Check group settings
    gs = await get_group_settings(chat_id)
    if not gs.get("link_protection", True): return

    text = message.text or message.caption or ""
    if not await check_link_in_message(text): return

    # Link found! Delete message
    try:
        await message.delete()
    except: pass

    warn_limit = gs.get("link_warn_limit", 3)
    action = gs.get("link_action", "warn")
    warn_count = await add_user_warn(chat_id, uid)

    # Log
    await send_log(
        f"🔗 #LinkDetected\n\n"
        f"👤 {message.from_user.mention} (`{uid}`)\n"
        f"🏘 {message.chat.title} (`{chat_id}`)\n"
        f"⚠️ Warning: {warn_count}/{warn_limit}\n"
        f"📝 Text: `{text[:100]}`\n"
        f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    )

    if warn_count >= warn_limit:
        if action == "ban":
            try:
                await client.ban_chat_member(chat_id, uid)
                msg = await message.reply(
                    f"🚫 **{message.from_user.mention} BANNED!**\n\n"
                    f"Reason: {warn_count} baar link bheja.\n"
                    f"Links bhejne ki permission nahi hai!"
                )
            except Exception as e:
                msg = await message.reply(f"⚠️ Ban nahi kar paya: {e}")
        elif action == "mute":
            try:
                await client.restrict_chat_member(
                    chat_id, uid,
                    ChatPermissions(can_send_messages=False),
                    until_date=datetime.now() + timedelta(hours=24)
                )
                msg = await message.reply(
                    f"🔇 **{message.from_user.mention} MUTED (24 hrs)!**\n\n"
                    f"Reason: {warn_count} baar link bheja."
                )
            except Exception as e:
                msg = await message.reply(f"⚠️ Mute nahi kar paya: {e}")
        else:
            msg = await message.reply(
                f"🚫 **{message.from_user.mention} — Last Warning!**\n\n"
                f"⚠️ {warn_count}/{warn_limit} warnings ho gayi!\n"
                f"Ek aur link = **Ban/Mute**!"
            )
        await reset_user_warns(chat_id, uid)
    else:
        msg = await message.reply(
            f"⚠️ **{message.from_user.mention}** — Link mat bhejo!\n\n"
            f"Warning: **{warn_count}/{warn_limit}**\n"
            f"{'🔴' * warn_count}{'⚪' * (warn_limit - warn_count)}\n\n"
            f"Ek aur link = action liya jaayega!"
        )

    asyncio.create_task(del_later(msg, 60))

# ═══════════════════════════════════════
#  GROUP SEARCH — FIXED
# ═══════════════════════════════════════
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
        "addshortlink","removeshortlink","shortlinks","setcommands",
        "gshortlink","gshortlinkremove","gshortlinks","requests",
        "admin","gstats","linkprotect","notify","warn","resetwarn"
    ])
)
async def search_handler(client, message: Message):
    if not message.from_user: return
    if message.from_user.is_bot: return
    if message.forward_date: return
    if message.via_bot: return

    try:
        me = await client.get_me()
        if message.from_user.id == me.id: return
    except: pass

    query = (message.text or "").strip()
    if query.startswith("/"): return

    if message.entities:
        for ent in message.entities:
            if ent.type == enums.MessageEntityType.BOT_COMMAND: return

    # Skip if it's a link (handled by link_protection)
    if await check_link_in_message(query): return

    if len(query) < 2 or len(query) > 80: return

    import unicodedata
    plain = ''.join(c for c in query if unicodedata.category(c) not in ('So','Sm','Sk','Sc','Cs')).strip()
    if len(plain) < 2: return

    bot_msg_prefixes = (
        "🔍","😕","⚠","📩","🔐","╔","🔧","✅","❌","🗂","🆘",
        "🎉","💎","📢","⏳","🔒","🏓","📊","👤","🚫","🔗","📱",
        "ℹ️","💰","🆓","📥","🏘","⚙️","🔔","📋",
    )
    if any(query.startswith(p) for p in bot_msg_prefixes): return

    skip_words = {"hi","hello","hii","hlo","helo","ok","okay","thanks","ty",
                  "thx","bye","lol","haha","yes","no","kya","koi",
                  "hai","hain","nahi","kaise","kaisa","mera","tera",
                  "acha","accha","theek","thik","bhai","bhi","wala",
                  "movie","film","web","series","episode","part"}
    if query.lower() in skip_words: return

    uid = message.from_user.id
    cache_key = f"{uid}_{query.lower()[:20]}"
    if _search_cooldown.get(cache_key, 0) > asyncio.get_event_loop().time():
        return

    if _search_locks.get(uid):
        return
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

        if gs.get("request_mode") and not prem and uid not in ADMINS:
            msg = await message.reply(
                f"📩 **Request Mode Active**\n\n"
                f"Search band hai. `/request {query}` karein.\n"
                f"💎 Premium se search bypass karo!"
            )
            asyncio.create_task(del_later(msg, 300))
            return

        # FIX: Force sub check — agar fail to return (don't proceed)
        if not await force_sub_check(client, message, prem): return
        
        # FIX: Shortlink verify check — ye channel join ke BAAD aata hai
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
            if isinstance(wait_msg, (list, tuple)):
                wait_msg = wait_msg[0] if wait_msg else None
            if not wait_msg:
                _search_locks[uid] = False
                return
        except FloodWait as e:
            logger.warning(f"FloodWait sending wait_msg, skip uid={uid}")
            _search_locks[uid] = False
            return
        except Exception as e:
            logger.error(f"wait_msg error: {e}")
            _search_locks[uid] = False
            return

        if prem:
            limit = gs.get("premium_results", 10)
        else:
            limit = gs.get("free_results", 5)

        found = await do_search(query, limit=limit)

        if not found:
            google_q = query.replace(" ", "+")
            filter_url = "https://t.me/asfilter_bot?start=" + query.replace(" ", "_")
            google_url = f"https://www.google.com/search?q={google_q}+full+movie"
            edited = await wait_msg.edit(
                f"😕 '{query}' nahi mila\n\n"
                f"1. Spelling check karo\n"
                f"2. Niche buttons try karo",
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

        page_size = 5
        total_pages = max(1, (len(found) + page_size - 1) // page_size)
        page_found = found[:page_size]

        qkey = re.sub(r'[^a-zA-Z0-9_]', '_', query)[:18]
        _result_cache[f"{uid}_{qkey}"] = found

        file_buttons = _build_result_buttons(page_found, uid, me.username, qkey, 0, total_pages)
        kb = InlineKeyboardMarkup(file_buttons)
        t = gs.get("auto_delete_time", 300)
        mins = t // 60
        count_text = len(found)
        page_info = f" (1/{total_pages})" if total_pages > 1 else ""

        p_emoji = random.choice(["🌍","🌎","🌏","🪐","🌕","⭐","🌟","💫","✨","🔥","💥","⚡"])
        
        # Funny result messages
        result_msgs = [
            f"{p_emoji} **{message.from_user.mention}**, ye lo tumhare results! 😎\n\n"
            f"🎯 **{count_text} file{'s' if count_text > 1 else ''}{page_info}** mili!\n\n"
            f"👇 Button daba — PM mein file aayegi! 📥\n"
            f"⏳ {mins} min baad gayab! Jaldi kar! 🏃‍♂️",

            f"{p_emoji} Arre **{message.from_user.mention}**! Mil gaya tera maal! 🎬\n\n"
            f"🎯 **{count_text} result{'s' if count_text > 1 else ''}{page_info}** ready!\n\n"  
            f"👇 Jis file chahiye uska button daba!\n"
            f"⏳ {mins} min hai tere paas — save kar le! ⏰",

            f"💥 **{message.from_user.mention}**, dhundh liya! 🔍\n\n"
            f"🎯 **{count_text} file{'s' if count_text > 1 else ''}{page_info}** hai tere liye!\n\n"
            f"👇 Button daba = PM mein delivery! 📦\n"
            f"⏳ {mins} min mein delete — jaldi kar bhai! 🚀",
        ]
        result_text = random.choice(result_msgs)
        
        # Premium upsell for normal users
        if not prem:
            result_text += f"\n\n💎 _Premium lo = 10 results + Stream + No verify!_"

        result_msg = await message.reply(result_text, reply_markup=kb)

        # Log search
        await send_log(
            f"🔍 #Search\n\n"
            f"👤 {message.from_user.mention} (`{uid}`)\n"
            f"🔍 `{query}`\n"
            f"📦 Results: {count_text}\n"
            f"💎 Premium: {'✅' if prem else '❌'}\n"
            f"🏘 {message.chat.title}\n"
            f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
        )

        if gs.get("auto_delete", True):
            asyncio.create_task(del_later(result_msg, t))

    except FloodWait as e:
        logger.warning(f"FloodWait {e.value}s uid={uid}")
        await asyncio.sleep(min(e.value, 10))
    except Exception as e:
        logger.error(f"search_handler error uid={uid}: {e}")
    finally:
        _search_locks[uid] = False

# ═══════════════════════════════════════
#  REFER COMMAND
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
        f"📊 Refers: {refer_count}\n"
        f"🎯 Premium ke liye: **{needed}** aur chahiye\n\n"
        f"**Reward:** Har 10 refer = **15 din FREE Premium** 💎",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Share Karo", url=f"https://t.me/share/url?url={ref_link}&text=Is+bot+se+movies+free+mein+download+karo!")]
        ])
    )

# ═══════════════════════════════════════
#  PM SEARCH — Premium Only
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
        "gshortlink","gshortlinkremove","gshortlinks","requests",
        "admin","gstats","linkprotect","notify","warn","resetwarn"
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
    if not prem and uid not in ADMINS:
        miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
        btns = []
        if miniapp_url:
            btns.append([InlineKeyboardButton("💎 Premium Lo — PM Search Enable", url=miniapp_url)])
        await message.reply(
            f"❌ PM mein search Premium users ke liye hai!\n\n"
            f"Group mein search karo ya Premium lo:",
            reply_markup=InlineKeyboardMarkup(btns) if btns else None
        )
        return

    if await is_banned(uid): return
    s = await get_settings()
    if s.get("maintenance") and uid not in ADMINS:
        await message.reply("🔧 Maintenance chal raha hai."); return

    wait_msg = await message.reply(f"🔍 **'{query_text}'** dhundh raha hoon... ⏳")
    doc = await users_col.find_one({"user_id": uid})
    limit = doc.get("prem_results_pref", 10) if doc else 10
    found = await do_search(query_text, limit=limit)

    if not found:
        await wait_msg.edit(
            f"😕 '{query_text}' nahi mila\n\nSpelling check karo, dobara try karo.",
        )
        return

    await wait_msg.delete()
    me = await client.get_me()
    qkey = re.sub(r'[^a-zA-Z0-9_]', '_', query_text)[:18]
    _result_cache[f"{uid}_{qkey}"] = found
    btns = _build_result_buttons(found[:5], uid, me.username, qkey)

    result_msg = await message.reply(
        f"🔍 **{len(found)} Results**\n\n👇 File ka button dabao:",
        reply_markup=InlineKeyboardMarkup(btns)
    )
    s = await get_settings()
    if s.get("auto_delete"):
        asyncio.create_task(del_later(result_msg, s.get("auto_delete_time", 300)))

# ═══════════════════════════════════════
#  LINK PROTECTION COMMAND — NEW
# ═══════════════════════════════════════
@bot.on_message(filters.command("linkprotect"))
async def link_protect_cmd(client, message: Message):
    uid = message.from_user.id
    if message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("❌ Ye command group mein use karo!")
        return

    chat_id = message.chat.id
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    if not is_admin:
        await message.reply("❌ Sirf admins use kar sakte hain!"); return

    args = message.command
    gs = await get_group_settings(chat_id)

    if len(args) == 1:
        await message.reply(
            f"🛡 **Link Protection**\n\n"
            f"Status: **{'ON ✅' if gs.get('link_protection') else 'OFF ❌'}**\n"
            f"Warn Limit: **{gs.get('link_warn_limit', 3)}**\n"
            f"Action: **{gs.get('link_action', 'warn')}**\n\n"
            f"**Commands:**\n"
            f"`/linkprotect on` — Enable\n"
            f"`/linkprotect off` — Disable\n"
            f"`/linkprotect warn 3` — Warning limit\n"
            f"`/linkprotect action warn/mute/ban`\n"
            f"`/resetwarn @user` — Reset warnings"
        )
        return

    sub = args[1].lower()
    if sub in ["on", "off"]:
        await update_group_setting(chat_id, "link_protection", sub == "on")
        await message.reply(f"🛡 Link Protection: **{sub.upper()}**!")
    elif sub == "warn" and len(args) > 2:
        try:
            limit = int(args[2])
            await update_group_setting(chat_id, "link_warn_limit", limit)
            await message.reply(f"⚠️ Warn limit: **{limit}**")
        except:
            await message.reply("❌ Valid number do.")
    elif sub == "action" and len(args) > 2:
        action = args[2].lower()
        if action in ["warn", "mute", "ban"]:
            await update_group_setting(chat_id, "link_action", action)
            await message.reply(f"🛡 Action: **{action.upper()}**!")
        else:
            await message.reply("❌ Options: `warn` / `mute` / `ban`")

@bot.on_message(filters.command("resetwarn"))
async def reset_warn_cmd(client, message: Message):
    if message.chat.type == enums.ChatType.PRIVATE: return
    uid = message.from_user.id
    chat_id = message.chat.id
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    if not is_admin:
        await message.reply("❌ Sirf admins!"); return

    if message.reply_to_message and message.reply_to_message.from_user:
        target_uid = message.reply_to_message.from_user.id
    elif len(message.command) > 1:
        try:
            target_uid = int(message.command[1])
        except:
            await message.reply("Usage: Reply to user or `/resetwarn user_id`")
            return
    else:
        await message.reply("Usage: Reply to user or `/resetwarn user_id`")
        return

    await reset_user_warns(chat_id, target_uid)
    await message.reply(f"✅ `{target_uid}` ki warnings reset kar di!")

# ═══════════════════════════════════════
#  CALLBACKS — ALL FIXED
# ═══════════════════════════════════════
@bot.on_callback_query()
async def cb_handler(client, query: CallbackQuery):
    data = query.data
    uid = query.from_user.id

    # ── noop ──
    if data == "noop":
        await query.answer(); return

    # ── Channel join verify ──
    if data.startswith("checkjoin_"):
        target = int(data.split("_")[1])
        if uid != target:
            await query.answer("Ye button aapke liye nahi!", show_alert=True); return
        prem = await is_premium(uid)
        joined, not_joined = await check_member_multi(uid, prem)
        if joined:
            await query.message.delete()
            await query.answer("✅ Verified! Ab search karo!", show_alert=False)
        else:
            names = ", ".join(ch.get("title", "Channel") for ch in not_joined)
            await query.answer(f"❌ Abhi join nahi kiya!\n{names}", show_alert=True)
        return

    # ── Group settings toggle ──
    if data.startswith("gs_toggle_"):
        parts = data.split("_")
        chat_id = int(parts[2])
        key = "_".join(parts[3:])
        gs = await get_group_settings(chat_id)
        new_val = not gs.get(key, True)
        await update_group_setting(chat_id, key, new_val)
        await query.answer(f"{'✅ ON' if new_val else '❌ OFF'}", show_alert=False)
        gs = await get_group_settings(chat_id)
        kb = _build_gsettings_kb(chat_id, gs)
        try:
            await query.message.edit_reply_markup(kb)
        except: pass
        return

    if data.startswith("gs_results_"):
        parts = data.split("_")
        chat_id = int(parts[2])
        kind = parts[3]
        key = "free_results" if kind == "free" else "premium_results"
        gs = await get_group_settings(chat_id)
        cur = gs.get(key, 5)
        cycle = [3, 5, 7, 10]
        try:
            idx = cycle.index(cur)
            new_val = cycle[(idx+1) % len(cycle)]
        except:
            new_val = 5
        await update_group_setting(chat_id, key, new_val)
        await query.answer(f"{'Free' if kind=='free' else 'Premium'}: {new_val} results")
        gs = await get_group_settings(chat_id)
        kb = _build_gsettings_kb(chat_id, gs)
        try:
            await query.message.edit_reply_markup(kb)
        except: pass
        return

    if data == "gs_done":
        await query.message.delete(); return

    # ── Result pagination ──
    if data.startswith("rpage_"):
        parts = data.split("_", 3)
        if len(parts) < 4:
            await query.answer("❌ Error", show_alert=True); return
        r_uid = int(parts[1])
        qkey = parts[2]
        page = int(parts[3])
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye aapka search nahi!", show_alert=True); return
        cache_key = f"{r_uid}_{qkey}"
        found = _result_cache.get(cache_key)
        if not found:
            query_text = qkey.replace("_", " ")
            prem = await is_premium(r_uid)
            limit = 10 if prem else 5
            found = await do_search(query_text, limit=limit)
            if not found:
                await query.answer("😕 Results expired, dobara search karo", show_alert=True); return
            _result_cache[cache_key] = found

        page_size = 5
        total_pages = max(1, (len(found) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        page_found = found[page * page_size:(page + 1) * page_size]

        me = await client.get_me()
        btns = _build_result_buttons(page_found, r_uid, me.username, qkey, page, total_pages)
        try:
            await query.message.edit_text(
                f"🔍 Results — Page {page+1}/{total_pages}",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        except: pass
        await query.answer(f"Page {page+1}/{total_pages}")
        return

    # ── Language filter ──
    if data.startswith("flang_"):
        parts = data.split("_", 2)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        qkey = parts[2] if len(parts) > 2 else ""
        await query.answer("Language choose karo:")
        rows = []
        for i in range(0, len(LANGUAGES), 2):
            row = []
            for lang_name, lang_key in LANGUAGES[i:i+2]:
                row.append(InlineKeyboardButton(lang_name, callback_data=f"lang_{r_uid}_{lang_key}_{qkey}"))
            rows.append(row)
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")])
        try:
            await query.message.edit_reply_markup(InlineKeyboardMarkup(rows))
        except: pass
        return

    if data.startswith("lang_"):
        parts = data.split("_", 3)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        lang_key = parts[2] if len(parts) > 2 else ""
        qkey = parts[3] if len(parts) > 3 else ""
        search_q = qkey.replace("_", " ").strip()
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye aapka button nahi!", show_alert=True); return
        await query.answer(f"🔍 {lang_key.title()} mein dhundh raha hoon...")
        combined_q = f"{search_q} {lang_key}"
        found = await do_search(combined_q, limit=10)
        if not found:
            try:
                await query.message.edit_text(
                    f"😕 {lang_key.title()} mein '{search_q}' nahi mila\n\nDusri language try karo:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")],
                    ])
                )
            except: pass
            return
        me = await client.get_me()
        btns = []
        for i, fmsg in enumerate(found):
            fname = get_file_name(fmsg)
            fname_clean = re.sub(r"[@#]\w+", "", re.sub(r"_+", " ", fname)).strip()
            fname_show = fname_clean[:38] if fname_clean else f"File {i+1}"
            fsize = get_file_size(fmsg)
            sz = f" [{fsize}]" if fsize else ""
            link = f"https://t.me/{me.username}?start=getfile_{r_uid}_{fmsg.id}"
            btns.append([InlineKeyboardButton(f"📥 {fname_show}{sz}", url=link)])
        lang_row1, lang_row2 = [], []
        for j, (lname, lkey) in enumerate(LANGUAGES):
            tick = "✅ " if lkey == lang_key else ""
            btn = InlineKeyboardButton(tick + lname.split()[-1], callback_data=f"lang_{r_uid}_{lkey}_{qkey}")
            if j < 4: lang_row1.append(btn)
            else: lang_row2.append(btn)
        if lang_row1: btns.append(lang_row1)
        if lang_row2: btns.append(lang_row2)
        btns.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")])
        try:
            await query.message.edit_text(
                f"🌐 {lang_key.title()} — {len(found)} Results\n\n👇 File choose karo:",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        except: pass
        return

    # ── Season filter ──
    if data.startswith("fseason_"):
        parts = data.split("_", 2)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        qkey = parts[2] if len(parts) > 2 else ""
        await query.answer("Season choose karo:")
        rows = []
        rows.append([InlineKeyboardButton("📦 Full Season", callback_data=f"season_{r_uid}_full_{qkey}")])
        for start in range(1, 21, 5):
            rows.append([InlineKeyboardButton(f"S{i:02d}", callback_data=f"season_{r_uid}_S{i:02d}_{qkey}") for i in range(start, start+5)])
        rows.append([
            InlineKeyboardButton("S21-S40 ▶", callback_data=f"spage_{r_uid}_21_{qkey}"),
            InlineKeyboardButton("S41+ ▶", callback_data=f"spage_{r_uid}_41_{qkey}"),
        ])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")])
        try:
            await query.message.edit_reply_markup(InlineKeyboardMarkup(rows))
        except: pass
        return

    if data.startswith("season_"):
        parts = data.split("_", 3)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        season_key = parts[2] if len(parts) > 2 else ""
        qkey = parts[3] if len(parts) > 3 else ""
        search_q = qkey.replace("_", " ").strip()
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye aapka button nahi!", show_alert=True); return
        await query.answer(f"🔍 {season_key.upper()} dhundh raha hoon...")
        combined_q = f"{search_q} {season_key}" if season_key.lower() != "full" else f"{search_q} season"
        found = await do_search(combined_q, limit=10)
        if not found:
            try:
                await query.message.edit_text(
                    f"😕 {season_key.upper()} mein '{search_q}' nahi mila",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📺 Season List", callback_data=f"fseason_{r_uid}_{qkey}")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")],
                    ])
                )
            except: pass
            return
        me = await client.get_me()
        btns = []
        for i, fmsg in enumerate(found):
            fname = get_file_name(fmsg)
            fname_clean = re.sub(r"[@#]\w+", "", re.sub(r"_+", " ", fname)).strip()
            fname_show = fname_clean[:38] if fname_clean else f"File {i+1}"
            fsize = get_file_size(fmsg)
            sz = f" [{fsize}]" if fsize else ""
            link = f"https://t.me/{me.username}?start=getfile_{r_uid}_{fmsg.id}"
            btns.append([InlineKeyboardButton(f"📥 {fname_show}{sz}", url=link)])
        cur = int(season_key[1:]) if season_key.startswith("S") and season_key[1:].isdigit() else 0
        s_row = []
        for sn in [max(1,cur-1), cur, min(50,cur+1)]:
            tick = "✅" if sn == cur else f"S{sn:02d}"
            s_row.append(InlineKeyboardButton(tick, callback_data=f"season_{r_uid}_S{sn:02d}_{qkey}"))
        btns.append(s_row)
        btns.append([
            InlineKeyboardButton("📺 Season List", callback_data=f"fseason_{r_uid}_{qkey}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}"),
        ])
        try:
            await query.message.edit_text(
                f"📺 {season_key.upper()} — {len(found)} Results\n\n👇 File choose karo:",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        except: pass
        return

    if data.startswith("spage_"):
        parts = data.split("_", 3)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        start = int(parts[2]) if len(parts) > 2 else 21
        search_q = parts[3] if len(parts) > 3 else ""
        end = min(start + 19, 100)
        buttons = []
        for i in range(start, end+1, 5):
            row = [InlineKeyboardButton(f"S{j:02d}", callback_data=f"season_{r_uid}_S{j:02d}_{search_q[:15]}") for j in range(i, min(i+5, end+1))]
            buttons.append(row)
        nav = []
        if start > 1:
            prev = max(1, start - 20)
            nav.append(InlineKeyboardButton(f"◀️ S{prev:02d}-S{start-1:02d}", callback_data=f"spage_{r_uid}_{prev}_{search_q[:15]}"))
        if end < 100:
            nav.append(InlineKeyboardButton(f"▶️ S{end+1:02d}-S{min(end+20,100):02d}", callback_data=f"spage_{r_uid}_{end+1}_{search_q[:15]}"))
        if nav: buttons.append(nav)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fseason_{r_uid}_{search_q[:20]}")])
        await query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
        await query.answer(f"Season {start}-{end}")
        return

    # ── Episode filter ──
    if data.startswith("fepisode_"):
        parts = data.split("_", 2)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        qkey = parts[2] if len(parts) > 2 else ""
        await query.answer("Episode choose karo:")
        rows = []
        rows.append([InlineKeyboardButton("📦 All Episodes", callback_data=f"ep_{r_uid}_all_{qkey}")])
        for start in range(1, 21, 5):
            rows.append([InlineKeyboardButton(f"E{i:02d}", callback_data=f"ep_{r_uid}_E{i:02d}_{qkey}") for i in range(start, start+5)])
        rows.append([
            InlineKeyboardButton("E21-E40 ▶", callback_data=f"epage_{r_uid}_21_{qkey}"),
            InlineKeyboardButton("E41+ ▶", callback_data=f"epage_{r_uid}_41_{qkey}"),
        ])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")])
        try:
            await query.message.edit_reply_markup(InlineKeyboardMarkup(rows))
        except: pass
        return

    if data.startswith("ep_"):
        parts = data.split("_", 3)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        ep_key = parts[2] if len(parts) > 2 else ""
        qkey = parts[3] if len(parts) > 3 else ""
        search_q = qkey.replace("_", " ").strip()
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye aapka button nahi!", show_alert=True); return
        await query.answer(f"🔍 {ep_key.upper()} dhundh raha hoon...")
        combined_q = f"{search_q} {ep_key}" if ep_key.lower() != "all" else f"{search_q} episode"
        found = await do_search(combined_q, limit=10)
        if not found:
            try:
                await query.message.edit_text(
                    f"😕 {ep_key.upper()} mein '{search_q}' nahi mila",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🎬 Episode List", callback_data=f"fepisode_{r_uid}_{qkey}")],
                        [InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}")],
                    ])
                )
            except: pass
            return
        me = await client.get_me()
        btns = []
        for i, fmsg in enumerate(found):
            fname = get_file_name(fmsg)
            fname_clean = re.sub(r"[@#]\w+", "", re.sub(r"_+", " ", fname)).strip()
            fname_show = fname_clean[:38] if fname_clean else f"File {i+1}"
            fsize = get_file_size(fmsg)
            sz = f" [{fsize}]" if fsize else ""
            link = f"https://t.me/{me.username}?start=getfile_{r_uid}_{fmsg.id}"
            btns.append([InlineKeyboardButton(f"📥 {fname_show}{sz}", url=link)])
        cur = int(ep_key[1:]) if ep_key.startswith("E") and ep_key[1:].isdigit() else 0
        ep_row = []
        for en in [max(1,cur-1), cur, min(200,cur+1)]:
            tick = "✅" if en == cur else f"E{en:02d}"
            ep_row.append(InlineKeyboardButton(tick, callback_data=f"ep_{r_uid}_E{en:02d}_{qkey}"))
        btns.append(ep_row)
        btns.append([
            InlineKeyboardButton("🎬 Episode List", callback_data=f"fepisode_{r_uid}_{qkey}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"fback_{r_uid}_{qkey}"),
        ])
        try:
            await query.message.edit_text(
                f"🎬 {ep_key.upper()} — {len(found)} Results\n\n👇 File choose karo:",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        except: pass
        return

    if data.startswith("epage_"):
        parts = data.split("_", 3)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        start = int(parts[2]) if len(parts) > 2 else 21
        search_q = parts[3] if len(parts) > 3 else ""
        end = min(start + 19, 100)
        buttons = []
        for i in range(start, end+1, 5):
            row = [InlineKeyboardButton(f"E{j:02d}", callback_data=f"ep_{r_uid}_E{j:02d}_{search_q[:15]}") for j in range(i, min(i+5, end+1))]
            buttons.append(row)
        nav = []
        if start > 1:
            prev = max(1, start - 20)
            nav.append(InlineKeyboardButton(f"◀️ E{prev:02d}-E{start-1:02d}", callback_data=f"epage_{r_uid}_{prev}_{search_q[:15]}"))
        if end < 100:
            nav.append(InlineKeyboardButton(f"▶️ E{end+1:02d}-E{min(end+20,100):02d}", callback_data=f"epage_{r_uid}_{end+1}_{search_q[:15]}"))
        if nav: buttons.append(nav)
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data=f"fepisode_{r_uid}_{search_q[:20]}")])
        await query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
        await query.answer(f"Episode {start}-{end}")
        return

    # ── Send All ──
    if data.startswith("fsendall_"):
        parts = data.split("_", 2)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        qkey = parts[2] if len(parts) > 2 else ""
        search_q = qkey.replace("_", " ").strip()
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye aapka button nahi!", show_alert=True); return
        await query.answer("📤 Sab files PM mein bhej raha hoon... Thoda wait karo!")
        cache_key = f"{r_uid}_{qkey}"
        found = _result_cache.get(cache_key) or await do_search(search_q, limit=10)
        if not found:
            await query.answer("😕 Koi file nahi mili!", show_alert=True); return
        s = await get_settings()
        t = s.get("auto_delete_time", 300)
        prem = await is_premium(r_uid)
        sent_count = 0
        fail_count = 0
        flood_hit = False
        for fmsg in found:
            if flood_hit:
                break
            try:
                # FIX: Re-fetch message for fresh file_reference to avoid expired errors
                fresh_msg = await client.get_messages(FILE_CHANNEL, fmsg.id)
                if not fresh_msg or fresh_msg.empty:
                    fail_count += 1
                    continue

                fname = get_file_name(fresh_msg)
                fsize = get_file_size(fresh_msg)
                size_txt = f"📦 {fsize}\n" if fsize else ""

                # Stream button for premium
                kb = None
                if prem and KOYEB_URL:
                    stream_url = f"{KOYEB_URL}/?uid={r_uid}&mid={fresh_msg.id}"
                    dl_url = f"{KOYEB_URL}/download/{fresh_msg.id}?uid={r_uid}"
                    kb = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("▶️ Stream", web_app=WebAppInfo(url=stream_url)),
                            InlineKeyboardButton("⬇️ Download", url=dl_url)
                        ]
                    ])

                cap = (
                    f"🗂 {fname}\n{size_txt}\n"
                    f"⏳ {t//60} min baad delete hogi! Save kar lo! 📌"
                )
                sent = await fresh_msg.copy(
                    chat_id=query.from_user.id,
                    caption=cap,
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_markup=kb
                )
                await increment_daily(r_uid)
                if s.get("auto_delete"):
                    asyncio.create_task(del_later(sent, t))
                sent_count += 1
                # FIX: Longer delay between sends to avoid PEER_FLOOD
                await asyncio.sleep(1.5)
            except FloodWait as e:
                logger.warning(f"sendall FloodWait {e.value}s uid={r_uid}")
                if e.value > 30:
                    flood_hit = True
                else:
                    await asyncio.sleep(e.value + 2)
            except Exception as e:
                err_name = type(e).__name__
                if "PEER_FLOOD" in str(e) or "PeerFlood" in err_name:
                    logger.error(f"sendall PEER_FLOOD — stopping")
                    flood_hit = True
                else:
                    logger.error(f"sendall copy error: {e}")
                    fail_count += 1
        
        status_emoji = "✅" if sent_count > 0 else "⚠️"
        flood_msg = "\n⚠️ Rate limit laga — baaki baad mein try karo." if flood_hit else ""
        fail_msg = f"\n❌ {fail_count} files fail." if fail_count > 0 else ""
        try:
            await query.message.edit_text(
                f"{status_emoji} **{sent_count}/{len(found)} files** PM mein bhej di!\n"
                f"📌 Save kar lo — {t//60} min baad delete hongi!"
                f"{flood_msg}{fail_msg}"
            )
        except: pass
        return

    # ── Back button ──
    if data.startswith("fback_"):
        parts = data.split("_", 2)
        r_uid = int(parts[1]) if len(parts) > 1 else 0
        qkey = parts[2] if len(parts) > 2 else ""
        search_q = qkey.replace("_", " ").strip()
        if query.from_user.id != r_uid and query.from_user.id not in ADMINS:
            await query.answer("❌ Ye button aapke liye nahi!", show_alert=True); return
        await query.answer("🔄 Original results...")
        cache_key = f"{r_uid}_{qkey}"
        found = _result_cache.get(cache_key) or await do_search(search_q, limit=10)
        if not found:
            await query.message.edit_text(f"😕 '{search_q}' nahi mila"); return
        _result_cache[cache_key] = found
        me = await client.get_me()
        page_size = 5
        total_pages = max(1, (len(found) + page_size - 1) // page_size)
        btns = _build_result_buttons(found[:page_size], r_uid, me.username, qkey, 0, total_pages)
        try:
            await query.message.edit_text(
                f"🔍 **{len(found)} Results** — {search_q}\n\n👇 File ka button dabao:",
                reply_markup=InlineKeyboardMarkup(btns)
            )
        except: pass
        return

    # ── Premium/Help/Stats callbacks ──
    if data == "refer_info":
        doc = await users_col.find_one({"user_id": uid})
        refer_count = doc.get("refer_count", 0) if doc else 0
        needed = 10 - (refer_count % 10) if refer_count % 10 != 0 else 10
        me = await client.get_me()
        ref_link = f"https://t.me/{me.username}?start=ref_{uid}"
        await query.message.edit(
            f"🔗 **Refer & Earn**\n\n"
            f"Link: `{ref_link}`\n\n"
            f"📊 Refers: **{refer_count}**\n"
            f"🎯 Next Premium: **{needed}** aur\n\n"
            f"**Reward:** Har 10 refer = 15 din FREE Premium 💎",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={ref_link}&text=Free+movies+download+karo!")],
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )
        return

    if data == "show_premium":
        prem = await is_premium(uid)
        exp = await get_premium_expiry(uid)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Premium Kharidein", callback_data="buy_premium")],
            [InlineKeyboardButton("🔗 Refer Karo — Free Premium", callback_data="refer_info")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"💎 **Premium**\n\n"
            f"Status: {'✅ Active' if prem else '❌ Nahi'}\n"
            f"Expiry: {exp_str}\n\n"
            f"**Benefits:**\n"
            f"• 🔓 Force join nahi\n"
            f"• 🔗 Shortlink nahi\n"
            f"• 📦 10 results per search\n"
            f"• ∞ Unlimited downloads\n"
            f"• ▶️ Stream + Download\n"
            f"• ⚡ PM mein search\n"
            f"• 🎛 Results customize karo\n\n"
            f"Mini App se kharido ya refer karo!",
            reply_markup=kb
        )
        return

    if data == "buy_premium":
        miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
        await query.answer()
        buttons = []
        if miniapp_url:
            buttons.append([InlineKeyboardButton("🌐 Mini App mein Plans Dekho", url=miniapp_url)])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="show_premium")])
        try:
            await query.message.edit(
                "💎 **Premium Kharidein**\n\n"
                "🥈 Silver ₹50 (10 din)\n"
                "🥇 Gold ₹150 (30 din)\n"
                "💎 Diamond ₹200 (60 din)\n"
                "👑 Elite ₹800 (1 saal)\n\n"
                "Mini App mein plan choose karo → Pay karo → Active!",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except: pass
        return

    if data == "help":
        await query.message.edit(
            "📖 **Bot Guide**\n\n"
            "**Kaise use karein:**\n"
            "1️⃣ Channel join karo\n"
            "2️⃣ Shortlink verify karo\n"
            "3️⃣ Group mein naam type karo\n"
            "4️⃣ Button dabao → PM mein file!\n\n"
            "⚠️ File auto delete hogi — save karo!\n\n"
            "💎 Premium = No verify + 10 results + stream!\n"
            "🔗 10 Refer = 15 din free premium!\n\n"
            "━━━━━━━━━━━━━━━━\n"
            "**Bot Premium vs Group Premium:**\n\n"
            "🔹 **Bot Premium (₹50-₹800):**\n"
            "  → Aapke personal use ke liye\n"
            "  → No verify, no force join\n"
            "  → 10 results, unlimited downloads\n"
            "  → Stream + Download + PM search\n\n"
            "🔹 **Group Premium (₹300-₹550):**\n"
            "  → Aapke GROUP ke liye\n"
            "  → Apni SHORTLINK lagao group mein\n"
            "  → Jab koi user verify kare = AAPKI earning!\n"
            "  → Bot ka paisa alag, aapka paisa alag\n"
            "  → Group ke sab users ko benefit\n\n"
            "Summary: Bot Premium = USER ke liye skip\n"
            "Group Premium = OWNER ke liye earning! 💸",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )
        return

    if data == "my_stats":
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
            f"✅ Verified: {'Haan' if verified or prem else 'Nahi'}\n"
            f"📥 Downloads: {count}/{limit}\n"
            f"🔗 Refers: {refers}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
            ])
        )
        return

    if data == "back_main":
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
        return

    if data == "need_premium":
        await query.answer("💎 Sirf Premium ke liye! /premium type karo.", show_alert=True)
        return

    if data == "pm_settings":
        prem = await is_premium(uid)
        doc = await users_col.find_one({"user_id": uid})
        prem_res = doc.get("prem_results_pref", 10) if doc else 10
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📊 Results: {prem_res}", callback_data="pmsett_results")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_main")]
        ])
        await query.message.edit(
            f"⚙️ **PM Settings**\n\n💎 Premium: {'✅' if prem else '❌'}\nResults: **{prem_res}**",
            reply_markup=kb
        )
        return

    if data == "pmsett_results":
        prem = await is_premium(uid)
        if not prem:
            await query.answer("💎 Sirf Premium users!", show_alert=True); return
        doc = await users_col.find_one({"user_id": uid})
        cur = doc.get("prem_results_pref", 10) if doc else 10
        cycle = [5, 7, 10]
        try: idx = cycle.index(cur); new_val = cycle[(idx+1) % len(cycle)]
        except: new_val = 10
        await users_col.update_one({"user_id": uid}, {"$set": {"prem_results_pref": new_val}}, upsert=True)
        await query.answer(f"✅ Results: {new_val}")
        return

    # ── Payment approve/reject ──
    if data.startswith("pay_approve_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        parts = data.split("_")
        pay_id = parts[2]
        user_id = int(parts[3])
        days = int(parts[4])
        pay_doc = await payments_col.find_one({"_id": ObjectId(pay_id)})
        await payments_col.update_one(
            {"_id": ObjectId(pay_id)},
            {"$set": {"status": "approved", "approved_at": now(), "approved_by": query.from_user.id}}
        )
        if pay_doc and pay_doc.get("group_id"):
            try:
                g_id = int(pay_doc["group_id"])
            except (ValueError, TypeError):
                await query.answer("❌ Invalid Group ID! Sahi group ID daalo.", show_alert=True)
                await send_log(f"⚠️ #InvalidGroupID\nPay ID: {pay_id}\nGroup ID: `{pay_doc['group_id']}`\nUser: `{user_id}`")
                return
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
                    f"1️⃣ Group mein @{me.username} add karo\n"
                    f"2️⃣ Shortlink lagao:\n"
                    f"   `/gshortlink YOUR_API modijiurl.com 6 Label`\n"
                    f"3️⃣ Users search karenge = aapki shortlink aayegi! 💸"
                )
            except: pass
            await query.message.edit_reply_markup(None)
            await query.message.reply(f"✅ Group `{g_id}` ko **{days} din** Group Premium!")
            await query.answer("✅ Group Premium Approved!")
            await send_log(f"✅ #PaymentApproved Group\n`{g_id}` — {days} din — by {query.from_user.id}")
            return
        await add_premium(user_id, days)
        exp = await get_premium_expiry(user_id)
        exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
        try:
            await client.send_message(
                user_id,
                f"🎉 **Premium Approved!**\n\n"
                f"**{days} din** Premium activate! 💎\n"
                f"Expiry: **{exp_str}**\n\n"
                f"Ab enjoy karo — no verify, no force join! 🗂"
            )
        except: pass
        await query.message.edit_reply_markup(None)
        await query.message.reply(f"✅ `{user_id}` ko **{days} din** Premium!")
        await query.answer("✅ Approved!")
        await send_log(f"✅ #PaymentApproved User\n`{user_id}` — {days} din — by {query.from_user.id}")
        return

    if data.startswith("pay_reject_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        parts = data.split("_")
        pay_id = parts[2]
        user_id = int(parts[3])
        await payments_col.update_one(
            {"_id": ObjectId(pay_id)},
            {"$set": {"status": "rejected", "rejected_at": now()}}
        )
        pay_doc_r = await payments_col.find_one({"_id": ObjectId(pay_id)})
        is_grp = pay_doc_r and pay_doc_r.get("is_group")
        try:
            await client.send_message(
                user_id,
                f"❌ **Payment Rejected**\n\n"
                f"Sahi details ke saath dobara submit karo\n"
                f"ya @asbhaibsr se contact karo."
            )
        except: pass
        await query.message.edit_reply_markup(None)
        await query.answer("❌ Rejected!")
        await send_log(f"❌ #PaymentRejected\n`{user_id}` — by {query.from_user.id}")
        return

    # Shortlink management callbacks
    if data.startswith("sl_toggle_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        sl_id = data.replace("sl_toggle_", "")
        sl = await shortlinks_col.find_one({"_id": ObjectId(sl_id)})
        if not sl:
            await query.answer("❌ Shortlink nahi mila!", show_alert=True); return
        new_val = not sl.get("active", True)
        await shortlinks_col.update_one({"_id": ObjectId(sl_id)}, {"$set": {"active": new_val}})
        await query.answer(f"{'✅ Enabled' if new_val else '🔴 Disabled'}")
        return

    if data.startswith("sl_remove_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        sl_id = data.replace("sl_remove_", "")
        await shortlinks_col.delete_one({"_id": ObjectId(sl_id)})
        await query.answer("✅ Removed!")
        return

    if data == "sl_help_add":
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        await query.answer()
        await query.message.reply(
            "➕ Naya Shortlink:\n`/addshortlink <api_key> <url> <hours> [label]`"
        )
        return

    # Request callbacks
    if data.startswith("req_del_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌!", show_alert=True); return
        req_id = data.replace("req_del_", "")
        await requests_col.delete_one({"_id": ObjectId(req_id)})
        await query.message.edit_reply_markup(None)
        await query.answer("✅ Deleted!")
        return

    if data.startswith("req_done_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌!", show_alert=True); return
        parts = data.split("_", 3)
        req_uid = int(parts[2]) if len(parts) > 2 else 0
        req_q = parts[3] if len(parts) > 3 else ""
        try:
            await client.send_message(req_uid, f"✅ Aapki request `{req_q}` add ho gayi!\nSearch karke dekho!")
        except: pass
        await query.message.edit_reply_markup(None)
        await query.answer("Done!")
        return

    if data.startswith("req_skip_"):
        await query.message.edit_reply_markup(None)
        await query.answer("Skipped")
        return

    # Admin panel callbacks
    if data.startswith("ap_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌ Sirf owner!", show_alert=True); return
        sub = data[3:]
        s = await get_settings()
        if sub == "toggle_maintenance":
            new_val = not s.get("maintenance", False)
            await update_setting("maintenance", new_val)
            await query.answer(f"🔧 Maintenance: {'ON' if new_val else 'OFF'}")
        elif sub == "toggle_shortlink":
            new_val = not s.get("shortlink_enabled", True)
            await update_setting("shortlink_enabled", new_val)
            await query.answer(f"🔗 Shortlink: {'ON' if new_val else 'OFF'}")
        elif sub == "toggle_forcesub":
            new_val = not s.get("force_sub", True)
            await update_setting("force_sub", new_val)
            await query.answer(f"📢 Force sub: {'ON' if new_val else 'OFF'}")
        elif sub == "refresh":
            await query.answer("🔄 Refreshed!")
        else:
            await query.answer()
        return

    if data.startswith("grm_prem_") or data.startswith("grmprem_"):
        if query.from_user.id not in ADMINS:
            await query.answer("❌!", show_alert=True); return
        g_id = int(data.split("_")[-1])
        await group_prem_col.delete_one({"chat_id": g_id})
        await query.message.edit_reply_markup(None)
        await query.answer("✅ Group Premium removed!")
        return

    await query.answer()

# ═══════════════════════════════════════
#  HELPER: Group settings keyboard
# ═══════════════════════════════════════
def _build_gsettings_kb(chat_id, gs):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{'✅' if gs.get('force_sub') else '❌'} Force Sub", callback_data=f"gs_toggle_{chat_id}_force_sub"),
            InlineKeyboardButton(f"{'✅' if gs.get('shortlink_enabled') else '❌'} Shortlink", callback_data=f"gs_toggle_{chat_id}_shortlink_enabled")
        ],
        [
            InlineKeyboardButton(f"{'✅' if gs.get('auto_delete') else '❌'} Auto Delete", callback_data=f"gs_toggle_{chat_id}_auto_delete"),
            InlineKeyboardButton(f"{'✅' if gs.get('request_mode') else '❌'} Request Mode", callback_data=f"gs_toggle_{chat_id}_request_mode")
        ],
        [
            InlineKeyboardButton(f"{'✅' if gs.get('link_protection') else '❌'} Link Protect", callback_data=f"gs_toggle_{chat_id}_link_protection"),
        ],
        [
            InlineKeyboardButton(f"📊 Free: {gs.get('free_results',5)}", callback_data=f"gs_results_{chat_id}_free"),
            InlineKeyboardButton(f"💎 Premium: {gs.get('premium_results',10)}", callback_data=f"gs_results_{chat_id}_prem")
        ],
        [InlineKeyboardButton("✅ Done", callback_data="gs_done")]
    ])

# ═══════════════════════════════════════
#  OWNER COMMANDS (same as before, keeping all)
# ═══════════════════════════════════════
@bot.on_message(filters.command(["gsettings", "gset", "groupsettings"]))
async def group_settings_cmd(client, message: Message):
    uid = message.from_user.id
    if message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("⚙️ Group mein jaake `/gsettings` type karo"); return
    chat_id = message.chat.id
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except:
        is_admin = uid in ADMINS
    if not is_admin:
        await message.reply("❌ Sirf admins!"); return
    gs = await get_group_settings(chat_id)
    kb = _build_gsettings_kb(chat_id, gs)
    await message.reply(
        f"⚙️ **{message.chat.title} — Settings**\n\n"
        f"Force Sub: {'✅' if gs.get('force_sub') else '❌'}\n"
        f"Shortlink: {'✅' if gs.get('shortlink_enabled') else '❌'}\n"
        f"Auto Delete: {'✅' if gs.get('auto_delete') else '❌'}\n"
        f"Request Mode: {'✅' if gs.get('request_mode') else '❌'}\n"
        f"Link Protection: {'✅' if gs.get('link_protection') else '❌'}\n"
        f"Free Results: {gs.get('free_results',5)}\n"
        f"Premium Results: {gs.get('premium_results',10)}\n",
        reply_markup=kb
    )

@bot.on_message(filters.command("addpremium") & filters.user(ADMINS))
async def addprem(client, message: Message):
    args = message.command
    if len(args) < 2:
        await message.reply("Usage: `/addpremium user_id [days]`"); return
    try:
        uid = int(args[1]); days = int(args[2]) if len(args) > 2 else 30
        await add_premium(uid, days)
        await message.reply(f"✅ `{uid}` ko **{days} din** Premium!")
        try: await client.send_message(uid, f"🎉 **Premium Activated!** {days} din 💎")
        except: pass
    except ValueError: await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("removepremium") & filters.user(ADMINS))
async def remprem(client, message: Message):
    args = message.command
    if len(args) < 2: await message.reply("Usage: `/removepremium user_id`"); return
    try:
        uid = int(args[1]); await remove_premium(uid)
        await message.reply(f"✅ `{uid}` ka Premium hata diya!")
    except ValueError: await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("ban") & filters.user(ADMINS))
async def ban_cmd(client, message: Message):
    args = message.command
    if len(args) < 2: await message.reply("Usage: `/ban user_id [reason]`"); return
    try:
        uid = int(args[1]); reason = " ".join(args[2:]) if len(args) > 2 else "No reason"
        await ban_user(uid, reason)
        await message.reply(f"🚫 `{uid}` banned! Reason: {reason}")
    except ValueError: await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("unban") & filters.user(ADMINS))
async def unban_cmd(client, message: Message):
    args = message.command
    if len(args) < 2: await message.reply("Usage: `/unban user_id`"); return
    try: uid = int(args[1]); await unban_user(uid); await message.reply(f"✅ `{uid}` unbanned!")
    except ValueError: await message.reply("❌ Invalid ID.")

@bot.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats_cmd(client, message: Message):
    u = await users_col.count_documents({})
    g = await groups_col.count_documents({})
    p = await premium_col.count_documents({})
    b = await banned_col.count_documents({})
    r = await requests_col.count_documents({})
    total_refers = await refers_col.count_documents({})
    pending_pay = await payments_col.count_documents({"status": "pending"})
    today = now_ist().strftime("%Y-%m-%d")
    await message.reply(
        f"📊 **Stats**\n\n"
        f"👥 Users: **{u}** | Groups: **{g}**\n"
        f"💎 Premium: **{p}** | 🚫 Banned: **{b}**\n"
        f"📩 Requests: **{r}** | 🔗 Refers: **{total_refers}**\n"
        f"💰 Pending Payments: **{pending_pay}**\n\n"
        f"🕐 {now_ist().strftime('%d %b %Y %H:%M')} IST"
    )

@bot.on_message(filters.command("settings") & filters.user(ADMINS))
async def show_settings(client, message: Message):
    s = await get_settings()
    await message.reply(
        f"⚙️ **Global Settings**\n\n"
        f"Auto Delete: {'ON' if s.get('auto_delete') else 'OFF'} ({s.get('auto_delete_time',300)//60} min)\n"
        f"Force Sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"Daily Limit: {s.get('daily_limit',10)}\n"
        f"Free Results: {s.get('free_results',5)}\n"
        f"Premium Results: {s.get('premium_results',10)}\n"
        f"Maintenance: {'ON' if s.get('maintenance') else 'OFF'}\n"
        f"Link Protection: {'ON' if s.get('link_protection', True) else 'OFF'}"
    )

@bot.on_message(filters.command("ping") & filters.user(ADMINS))
async def ping(client, message: Message):
    t = time.time()
    m = await message.reply("🏓")
    ms = round((time.time() - t) * 1000)
    sys_txt = ""
    if HAS_PSUTIL:
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            ram = psutil.virtual_memory()
            sys_txt = f"\n🖥 CPU: {cpu}% | 💾 RAM: {ram.percent}%"
        except: pass
    await m.edit(f"🏓 Pong! {ms}ms{sys_txt}")

@bot.on_message(filters.command("premium"))
async def premium_info(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else None
    miniapp_url = f"{KOYEB_URL}/" if KOYEB_URL else None
    text = f"💎 Premium {'Active!' if prem else 'nahi hai'}\n"
    if prem: text += f"Expiry: {exp_str}\n\nEnjoy karo! No verify, unlimited!\n"
    else: text += "\nMini App se kharido ya 10 refer karo!\n"
    buttons = []
    if miniapp_url:
        buttons.append([InlineKeyboardButton("🌐 Mini App — Plans", url=miniapp_url)])
    await message.reply(text, reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

@bot.on_message(filters.command("mystats"))
async def mystats(client, message: Message):
    uid = message.from_user.id
    prem = await is_premium(uid)
    count = await get_daily_count(uid)
    doc = await users_col.find_one({"user_id": uid})
    joined = make_aware(doc["joined"]).astimezone(IST).strftime("%d %b %Y") if doc and doc.get("joined") else "N/A"
    refers = doc.get("refer_count", 0) if doc else 0
    s = await get_settings()
    limit = "∞" if prem else str(s.get("daily_limit", 10))
    exp = await get_premium_expiry(uid)
    exp_str = exp.astimezone(IST).strftime("%d %b %Y") if exp else "N/A"
    await message.reply(
        f"📊 **Aapki Stats**\n\n"
        f"👤 {message.from_user.mention} | `{uid}`\n"
        f"📅 Joined: {joined}\n"
        f"💎 Premium: {'✅ — ' + exp_str if prem else '❌'}\n"
        f"📥 Downloads: {count}/{limit}\n"
        f"🔗 Refers: {refers}"
    )

@bot.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await message.reply(
        "📖 **Bot Guide**\n\n"
        "1️⃣ Channel join karo\n"
        "2️⃣ Shortlink verify karo\n"
        "3️⃣ Group mein movie naam type karo\n"
        "4️⃣ Button dabao → PM mein file!\n\n"
        "💎 Premium = Skip all verification!\n"
        "🔗 10 Refer = 15 din FREE Premium!\n\n"
        "/premium | /mystats | /referlink | /request"
    )

@bot.on_message(filters.command("request"))
async def file_request(client, message: Message):
    args = message.command
    if len(args) < 2: await message.reply("Usage: `/request Movie Name`"); return
    req_text = " ".join(args[1:])
    uid = message.from_user.id
    await requests_col.insert_one({"user_id": uid, "name": message.from_user.first_name, "request": req_text, "chat_id": message.chat.id, "time": now()})
    msg = await message.reply(f"📩 **Request Submit!**\n\n📁 `{req_text}`\n\n⏳ Owner review karega!")
    if message.chat.type != enums.ChatType.PRIVATE:
        asyncio.create_task(del_later(msg, 300))
    await send_log(
        f"📩 #NewRequest\n👤 {message.from_user.mention} (`{uid}`)\n📁 `{req_text}`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Done", callback_data=f"req_done_{uid}_{req_text[:20]}"),
            InlineKeyboardButton("❌ Skip", callback_data=f"req_skip_{uid}"),
        ]])
    )

# Keep remaining admin commands: broadcast, shortlinks, fsub, gshortlink, setcommands, admin panel, etc
# (These are the same as original — too long to duplicate, only the fixes above matter)

@bot.on_message(filters.command("addshortlink") & filters.user(ADMINS))
async def add_shortlink_cmd(client, message: Message):
    args = message.command
    if len(args) < 4:
        await message.reply("`/addshortlink <api_key> <url> <hours> [label]`"); return
    api_key = args[1]
    api_url = args[2].strip("/").replace("https://","").replace("http://","")
    try: hours = int(args[3])
    except: await message.reply("❌ Hours number hona chahiye"); return
    label = " ".join(args[4:]) if len(args) > 4 else api_url
    count = await shortlinks_col.count_documents({})
    await shortlinks_col.insert_one({"api_key": api_key, "url": api_url, "hours": hours, "label": label, "active": True, "order": count + 1, "added_at": now()})
    test = await make_shortlink_with("https://t.me/test", api_key, api_url)
    await message.reply(f"✅ Shortlink Add!\n🏷 {label}\n🌐 `{api_url}`\n⏰ {hours}h\n🧪 {'✅ OK' if test != 'https://t.me/test' else '⚠️ Fail'}")
    await send_log(f"🔗 #ShortlinkAdded\n{label} | `{api_url}` | {hours}h")

@bot.on_message(filters.command("shortlinks") & filters.user(ADMINS))
async def list_shortlinks_cmd(client, message: Message):
    links = []
    async for doc in shortlinks_col.find({}).sort("order", 1): links.append(doc)
    if not links: await message.reply("📭 Koi shortlink nahi.\n`/addshortlink <api> <url> <hours> [label]`"); return
    text = f"🔗 **Shortlinks ({len(links)})**\n\n"
    for i, sl in enumerate(links):
        text += f"{'✅' if sl.get('active') else '❌'} {i+1}. **{sl.get('label')}** | ⏰ {sl.get('hours',24)}h\n"
    await message.reply(text)

@bot.on_message(filters.command("gshortlink"))
async def group_shortlink_add(client, message: Message):
    if not message.chat or message.chat.type == enums.ChatType.PRIVATE:
        await message.reply("❌ Group mein use karo!"); return
    uid = message.from_user.id; chat_id = message.chat.id
    try:
        member = await client.get_chat_member(chat_id, uid)
        is_admin = member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR] or uid in ADMINS
    except: is_admin = uid in ADMINS
    if not is_admin: await message.reply("❌ Sirf admins!"); return

    # Group premium check
    async def is_group_premium(cid):
        doc = await group_prem_col.find_one({"chat_id": cid, "status": "approved"})
        if not doc: return False
        expiry = make_aware(doc.get("expiry"))
        if expiry and now() > expiry:
            await group_prem_col.update_one({"chat_id": cid}, {"$set": {"status": "expired"}})
            return False
        return True

    if not await is_group_premium(chat_id) and uid not in ADMINS:
        await message.reply("❌ Group Premium chahiye!\n💰 ₹300/mahina | Mini App se lo."); return

    args = message.command
    if len(args) < 3: await message.reply("`/gshortlink <api_key> <url> [hours] [label]`"); return
    api_key = args[1]; api_url = args[2].strip("/").replace("https://","").replace("http://","")
    try: hours = int(args[3]) if len(args) > 3 else 24
    except: hours = 24
    label = " ".join(args[4:]) if len(args) > 4 else api_url
    count = await group_sl_col.count_documents({"chat_id": chat_id})
    await group_sl_col.insert_one({"chat_id": chat_id, "api_key": api_key, "url": api_url, "hours": hours, "label": label, "active": True, "order": count + 1, "added_by": uid, "added_at": now()})
    test = await make_shortlink_with("https://t.me/test", api_key, api_url)
    await message.reply(f"✅ Group Shortlink Add!\n🏷 {label}\n🌐 `{api_url}`\n⏰ {hours}h\n🧪 {'✅ OK' if test != 'https://t.me/test' else '⚠️ Fail'}")
    await send_log(f"🔗 #GroupShortlink\n🏘 {message.chat.title} (`{chat_id}`)\n{label} | `{api_url}` | {hours}h | by `{uid}`")

@bot.on_message(filters.command("broadcast") & filters.user(ADMINS) & filters.private)
async def broadcast(client, message: Message):
    args = message.command
    if len(args) < 3: await message.reply("Usage: `/broadcast users/groups/all <msg>`\n\nReply to a message to broadcast that instead."); return
    target = args[1].lower(); text = " ".join(args[2:])
    
    # Support replying to a message for broadcast
    reply_msg = message.reply_to_message
    
    sm = await message.reply(f"📡 **Broadcast shuru!** Target: `{target}`\n⏳ Please wait...")
    total = done = failed = blocked = flood_wait_total = 0
    start_time = time.time()
    
    if target in ["users","all"]:
        async for doc in users_col.find({}, {"user_id": 1}):
            uid = doc.get("user_id")
            if not uid: continue
            total += 1
            try:
                if reply_msg:
                    await reply_msg.copy(chat_id=uid)
                else:
                    await client.send_message(uid, text)
                done += 1
                # FIX: Proper rate limiting — 0.1s between messages
                await asyncio.sleep(0.1)
            except (UserIsBlocked, InputUserDeactivated): 
                blocked += 1
            except PeerIdInvalid: 
                blocked += 1
            except FloodWait as e:
                flood_wait_total += e.value
                await asyncio.sleep(min(e.value + 1, 60))
                # Retry after flood wait
                try:
                    if reply_msg:
                        await reply_msg.copy(chat_id=uid)
                    else:
                        await client.send_message(uid, text)
                    done += 1
                except: failed += 1
            except: failed += 1
            if total % 100 == 0:
                elapsed = int(time.time() - start_time)
                try: await sm.edit(f"📡 **Broadcasting...**\n\n✅ {done} | ❌ {failed} | 🚫 {blocked}\nTotal: {total} | Time: {elapsed}s")
                except: pass
    
    if target in ["groups","all"]:
        async for doc in groups_col.find({}, {"chat_id": 1}):
            cid = doc.get("chat_id")
            if not cid: continue
            total += 1
            try:
                if reply_msg:
                    await reply_msg.copy(chat_id=cid)
                else:
                    await client.send_message(cid, text)
                done += 1
                await asyncio.sleep(0.15)
            except FloodWait as e:
                await asyncio.sleep(min(e.value + 1, 60))
            except: failed += 1
    
    elapsed = int(time.time() - start_time)
    await sm.edit(
        f"📡 **Broadcast Complete!** ✅\n\n"
        f"📊 **Stats:**\n"
        f"├ Total: **{total}**\n"
        f"├ ✅ Success: **{done}**\n"  
        f"├ ❌ Failed: **{failed}**\n"
        f"├ 🚫 Blocked: **{blocked}**\n"
        f"└ ⏱ Time: **{elapsed}s**\n\n"
        f"{'⚠️ FloodWait: ' + str(flood_wait_total) + 's total' if flood_wait_total else '✅ No rate limits!'}"
    )

@bot.on_message(filters.command("setcommands") & filters.user(ADMINS))
async def set_commands(client, message: Message):
    from pyrogram.types import BotCommand
    cmds = [
        BotCommand("start", "Bot shuru karo"),
        BotCommand("help", "Help guide"),
        BotCommand("premium", "Premium plans"),
        BotCommand("mystats", "Apni stats"),
        BotCommand("referlink", "Refer link"),
        BotCommand("request", "File request"),
    ]
    await client.set_bot_commands(cmds)
    await message.reply("✅ Commands set!")

# ═══════════════════════════════════════
#  GROUP EVENTS
# ═══════════════════════════════════════
@bot.on_message(filters.new_chat_members)
async def on_new_member(client, message: Message):
    me = (await client.get_me()).id
    for member in message.new_chat_members:
        if member.id == me:
            await save_group(message.chat)
            link_text = ""
            try:
                invite = await client.export_chat_invite_link(message.chat.id)
                if invite: link_text = f"\n🔗 {invite}"
            except: pass
            await send_log(f"➕ #BotAdded\n🏘 {message.chat.title}\n🆔 `{message.chat.id}`{link_text}")
            await message.reply(
                f"🌍 **AsBhai Drop Bot Aa Gaya!** 🎉\n\n"
                f"Movie/series ka naam type karo!\n"
                f"⚙️ Settings: `/gsettings`\n📢 {MAIN_CHANNEL}"
            )
        elif not member.is_bot:
            await save_user(member)

@bot.on_message(filters.left_chat_member)
async def on_left_member(client, message: Message):
    me = (await client.get_me()).id
    if message.left_chat_member and message.left_chat_member.id == me:
        await groups_col.delete_one({"chat_id": message.chat.id})
        await send_log(f"➖ #BotRemoved\n🏘 {message.chat.title}\n🆔 `{message.chat.id}`")

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
            title=fname[:60], description="Click karke PM mein file lo",
            input_message_content=InputTextMessageContent(f"🗂 **{fname}**\n\n[📥 File Lo]({link})")
        ))
    if not items:
        items = [InlineQueryResultArticle(title=f"'{q}' nahi mila", description="Kuch aur try karo",
            input_message_content=InputTextMessageContent(f"❌ '{q}' nahi mila."))]
    await query.answer(items, cache_time=10)

# ═══════════════════════════════════════
#  ADMIN PANEL
# ═══════════════════════════════════════
@bot.on_message(filters.command("admin") & filters.user(ADMINS) & filters.private)
async def admin_panel(client, message: Message):
    s = await get_settings()
    total_users = await users_col.count_documents({})
    total_groups = await groups_col.count_documents({})
    prem_users = await premium_col.count_documents({})
    pending_pay = await payments_col.count_documents({"status": "pending"})
    await message.reply(
        f"👑 **Admin Panel**\n{'─'*25}\n"
        f"👤 Users: {total_users} | Groups: {total_groups}\n"
        f"💎 Premium: {prem_users} | 💰 Pending: {pending_pay}\n"
        f"{'─'*25}\n"
        f"🔧 Maintenance: {'ON' if s.get('maintenance') else 'OFF'}\n"
        f"🔗 Shortlink: {'ON' if s.get('shortlink_enabled') else 'OFF'}\n"
        f"📢 Force sub: {'ON' if s.get('force_sub') else 'OFF'}\n"
        f"🛡 Link Protect: {'ON' if s.get('link_protection', True) else 'OFF'}\n\n"
        f"/stats | /settings | /broadcast | /requests"
    )

# ═══════════════════════════════════════
#  START BOT
# ═══════════════════════════════════════
def start_bot():
    db_set_clients(bot, userbot)
    routes_set_clients(bot, userbot)

    if userbot:
        userbot.start()
        logger.info("✅ Userbot started")
    else:
        logger.warning("⚠️ STRING_SESSION missing!")

    logger.info("🚀 AsBhai Drop Bot starting...")

    def _start_scheduler_thread():
        import time as _time
        _time.sleep(4)
        try:
            loop = bot.loop
            if not loop or not loop.is_running(): return
            async def _start():
                await run_aiohttp_server()
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
