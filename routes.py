# ╔══════════════════════════════════════════════════════╗
# ║  routes.py — AsBhai Drop Bot v3.1 ULTRA             ║
# ║  Advanced Raw MTProto Streaming — No More Buffering ║
# ║  Auto file_reference refresh on every request       ║
# ╚══════════════════════════════════════════════════════╝
import asyncio
import base64
import logging
import os as _os
import tempfile
from datetime import timedelta

from aiohttp import web as aio_web
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FileReferenceExpired, FileReferenceInvalid,
    AuthBytesInvalid, VolumeLocNotFound
)
from pyrogram import raw
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES
from pyrogram.session import Auth, Session

from config import (
    FILE_CHANNEL, KOYEB_URL, UPI_ID, PORT,
    ADMINS, OWNER_ID, LOG_CHANNEL, IST, now, now_ist, make_aware, logger
)
from database import (
    premium_col, free_trial_col, users_col, payments_col, help_msgs_col,
    is_premium, send_log, add_premium
)

bot    = None
userbot = None

def set_clients(b, u):
    global bot, userbot
    bot    = b
    userbot = u

aio_app = aio_web.Application(client_max_size=50 * 1024 * 1024)
routes  = aio_web.RouteTableDef()

# ═══════════════════════════════════════════════════════
#  ADVANCED RAW STREAMING ENGINE
#  Inspired by VCPlayerBot/subinps — direct MTProto calls
#  Har request pe fresh file_reference — no expired errors
# ═══════════════════════════════════════════════════════

# Chunk size: 1MB — matches Telegram's upload.GetFile limit
TGRAM_CHUNK = 1024 * 1024


async def _get_media_session(client, dc_id: int):
    """
    Get or create a media session for the given DC.
    Exactly the same approach as VCPlayerBot/pyro_dl.py
    """
    async with client.media_sessions_lock:
        session = client.media_sessions.get(dc_id)
        if session is None:
            if dc_id != await client.storage.dc_id():
                session = Session(
                    client, dc_id,
                    await Auth(client, dc_id, await client.storage.test_mode()).create(),
                    await client.storage.test_mode(),
                    is_media=True
                )
                await session.start()
                # Export + Import auth for cross-DC
                for _ in range(3):
                    exported = await client.send(
                        raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                    )
                    try:
                        await session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported.id, bytes=exported.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        continue
                else:
                    await session.stop()
                    raise AuthBytesInvalid
            else:
                session = Session(
                    client, dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True
                )
                await session.start()
            client.media_sessions[dc_id] = session
    return session


async def _fetch_fresh_message(msg_id: int):
    """
    Har baar fresh message fetch karo — file_reference kabhi expire nahi hoga.
    Bot pehle try karta hai, userbot fallback.
    """
    for client_try in [bot, userbot]:
        if not client_try:
            continue
        try:
            msg = await client_try.get_messages(FILE_CHANNEL, msg_id)
            if msg and not msg.empty:
                return msg, client_try
        except Exception as e:
            logger.debug(f"fetch_fresh_message client {client_try} error: {e}")
    return None, None


def _extract_media(msg):
    """Message se media object aur metadata nikalo."""
    if msg.video:
        m = msg.video
        return m, m.file_name or f"video_{msg.id}.mp4", m.mime_type or "video/mp4", m.duration or 0
    elif msg.document:
        m = msg.document
        return m, m.file_name or f"file_{msg.id}", m.mime_type or "application/octet-stream", 0
    elif msg.audio:
        m = msg.audio
        return m, m.file_name or f"audio_{msg.id}.mp3", m.mime_type or "audio/mpeg", m.duration or 0
    return None, None, None, 0


async def get_file_info(msg_id: int):
    """Fresh message se file info nikalo — har baar fresh reference."""
    msg, _ = await _fetch_fresh_message(msg_id)
    if not msg:
        return None
    media, fname, mime, duration = _extract_media(msg)
    if not media:
        return None
    return {
        "file_size": media.file_size or 0,
        "file_name": fname,
        "mime_type": mime,
        "duration":  duration,
        "msg":       msg,
    }


async def _raw_stream_generator(client, msg, from_bytes: int, req_length: int):
    """
    ADVANCED: Direct MTProto upload.GetFile — raw level streaming.
    - Har chunk directly Telegram DC se aata hai
    - file_reference fresh message se liya — kabhi expire nahi hoga
    - Seeking perfectly supported (range requests)
    """
    media, _, _, _ = _extract_media(msg)
    if not media:
        return

    file_id_obj = FileId.decode(media.file_id)
    dc_id       = file_id_obj.dc_id

    # Build location with FRESH file_reference from message
    location = raw.types.InputDocumentFileLocation(
        id=file_id_obj.media_id,
        access_hash=file_id_obj.access_hash,
        file_reference=file_id_obj.file_reference,
        thumb_size=""
    )

    session = await _get_media_session(client, dc_id)

    # Calculate starting offset (aligned to TGRAM_CHUNK)
    offset     = (from_bytes // TGRAM_CHUNK) * TGRAM_CHUNK
    first_cut  = from_bytes - offset
    bytes_sent = 0

    while bytes_sent < req_length:
        remaining = req_length - bytes_sent
        limit     = min(TGRAM_CHUNK, remaining + first_cut if offset == (from_bytes // TGRAM_CHUNK) * TGRAM_CHUNK else remaining)
        limit     = TGRAM_CHUNK  # Always request full chunk — slice ourselves

        try:
            r = await session.send(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=TGRAM_CHUNK
                ),
                sleep_threshold=30
            )
        except Exception as e:
            logger.error(f"raw_stream GetFile error offset={offset}: {e}")
            break

        if not isinstance(r, raw.types.upload.File):
            break

        chunk = r.bytes
        if not chunk:
            break

        # First chunk mein range offset tak skip karo
        if first_cut > 0:
            chunk     = chunk[first_cut:]
            first_cut = 0

        # Zaroorat se zyada mat bhejo
        if len(chunk) > (req_length - bytes_sent):
            chunk = chunk[:(req_length - bytes_sent)]

        if not chunk:
            break

        yield chunk
        bytes_sent += len(chunk)
        offset     += TGRAM_CHUNK

        if len(r.bytes) < TGRAM_CHUNK:
            # End of file
            break


# ═══════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════

@routes.get("/")
async def home_handler(request):
    try:
        with open("miniapp.html", "r", encoding="utf-8") as f:
            html = f.read()
        return aio_web.Response(text=html, content_type="text/html")
    except FileNotFoundError:
        return aio_web.Response(text="AsBhai Drop Bot ✅", content_type="text/plain")


@routes.get("/health")
async def health_handler(request):
    return aio_web.json_response({
        "status": "ok",
        "time":   now_ist().strftime("%d %b %H:%M IST")
    })


@routes.get("/stream")
async def stream_page_handler(request):
    return await home_handler(request)


@routes.get(r"/file_info/{msg_id:\d+}")
async def file_info_handler(request: aio_web.Request):
    msg_id = int(request.match_info["msg_id"])
    info   = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)
    return aio_web.json_response(
        {
            "file_name": info["file_name"],
            "file_size": info["file_size"],
            "mime_type": info["mime_type"],
            "duration":  info.get("duration", 0),
        },
        headers={"Access-Control-Allow-Origin": "*"},
    )


@routes.get(r"/stream_file/{msg_id:\d+}", allow_head=True)
async def stream_file_handler(request: aio_web.Request):
    """
    ADVANCED STREAMING — Direct MTProto raw API.
    Har request pe fresh file_reference — FILE_REFERENCE_EXPIRED kabhi nahi aayega.
    Range requests fully supported — seeking works perfectly.
    """
    msg_id = int(request.match_info["msg_id"])

    # CORS preflight
    if request.method == "OPTIONS":
        return aio_web.Response(headers={
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Max-Age":       "86400",
        })

    # Premium check
    uid_str = request.rel_url.query.get("uid", "")
    if uid_str:
        try:
            uid_int = int(uid_str)
            if uid_int not in ADMINS and not await is_premium(uid_int):
                return aio_web.json_response(
                    {"error": "💎 Premium required for streaming"}, status=403
                )
        except Exception:
            pass

    # Har request pe FRESH message — file_reference never expired
    msg, stream_client = await _fetch_fresh_message(msg_id)
    if not msg:
        return aio_web.json_response({"error": "File not found"}, status=404)

    media, fname, mime, _ = _extract_media(msg)
    if not media:
        return aio_web.json_response({"error": "No media in message"}, status=404)

    file_size = media.file_size or 0
    if file_size == 0:
        return aio_web.json_response({"error": "File size unknown"}, status=404)

    # Fallback: use whichever client is available
    if not stream_client:
        stream_client = userbot if userbot else bot
    if not stream_client:
        return aio_web.json_response({"error": "No client available"}, status=503)

    # Parse Range header
    range_header = request.headers.get("Range", "")
    from_bytes, until_bytes = 0, file_size - 1
    if range_header:
        try:
            parts       = range_header.replace("bytes=", "").split("-")
            from_bytes  = int(parts[0]) if parts[0] else 0
            until_bytes = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
        except Exception:
            pass
    until_bytes = min(until_bytes, file_size - 1)
    req_length  = until_bytes - from_bytes + 1

    headers = {
        "Content-Type":                  mime,
        "Content-Range":                 f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length":                str(req_length),
        "Content-Disposition":           f'inline; filename="{fname}"',
        "Accept-Ranges":                 "bytes",
        "Access-Control-Allow-Origin":   "*",
        "Access-Control-Allow-Methods":  "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers":  "Range, Content-Type",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Cache-Control":                 "no-cache",  # Fresh fetch every time
    }

    status = 206 if range_header else 200

    if request.method == "HEAD":
        return aio_web.Response(status=status, headers=headers)

    response = aio_web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in _raw_stream_generator(stream_client, msg, from_bytes, req_length):
            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError):
                break
            except Exception as e:
                logger.debug(f"stream write error: {e}")
                break
    except Exception as e:
        logger.error(f"stream_file error msg_id={msg_id}: {type(e).__name__}: {e}")

    try:
        await response.write_eof()
    except Exception:
        pass
    return response


@routes.get(r"/download/{msg_id:\d+}", allow_head=True)
async def download_handler(request: aio_web.Request):
    """Download handler — same advanced raw streaming, attachment header."""
    msg_id = int(request.match_info["msg_id"])

    uid_str = request.rel_url.query.get("uid", "")
    if uid_str:
        try:
            uid_int = int(uid_str)
            if uid_int not in ADMINS and not await is_premium(uid_int):
                return aio_web.json_response(
                    {"error": "💎 Premium required"}, status=403
                )
        except Exception:
            pass

    msg, dl_client = await _fetch_fresh_message(msg_id)
    if not msg:
        return aio_web.json_response({"error": "File not found"}, status=404)

    media, fname, mime, _ = _extract_media(msg)
    if not media:
        return aio_web.json_response({"error": "No media"}, status=404)

    file_size = media.file_size or 0
    if not dl_client:
        dl_client = userbot if userbot else bot
    if not dl_client:
        return aio_web.json_response({"error": "No client"}, status=503)

    headers = {
        "Content-Type":                 mime,
        "Content-Length":               str(file_size),
        "Content-Disposition":          f'attachment; filename="{fname}"',
        "Accept-Ranges":                "bytes",
        "Access-Control-Allow-Origin":  "*",
    }

    if request.method == "HEAD":
        return aio_web.Response(status=200, headers=headers)

    response = aio_web.StreamResponse(status=200, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in _raw_stream_generator(dl_client, msg, 0, file_size):
            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError):
                break
            except Exception:
                break
    except Exception as e:
        logger.error(f"download error msg_id={msg_id}: {e}")

    try:
        await response.write_eof()
    except Exception:
        pass
    return response


# ═══════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════

@routes.get("/api/plans")
async def api_plans(request):
    plans = [
        {"id": "10days",  "name": "10 Din",  "days": 10,  "price": 50,  "desc": "Starter"},
        {"id": "30days",  "name": "30 Din",  "days": 30,  "price": 150, "desc": "Popular 🔥"},
        {"id": "60days",  "name": "60 Din",  "days": 60,  "price": 200, "desc": "Great Value"},
        {"id": "150days", "name": "150 Din", "days": 150, "price": 500, "desc": "Super Saver"},
        {"id": "365days", "name": "1 Saal",  "days": 365, "price": 800, "desc": "Best Deal"},
    ]
    return aio_web.json_response({"ok": True, "plans": plans})


@routes.get("/api/user_status/{user_id}")
async def api_user_status(request):
    try:
        user_id  = int(request.match_info["user_id"])
        prem_doc  = await premium_col.find_one({"user_id": user_id})
        trial_doc = await free_trial_col.find_one({"user_id": user_id})
        user_doc  = await users_col.find_one({"user_id": user_id})
        pending   = await payments_col.count_documents({"user_id": user_id, "status": "pending"})

        is_prem, is_trial, expiry_str = False, False, None
        if prem_doc and prem_doc.get("expiry"):
            exp = make_aware(prem_doc["expiry"])
            if now() < exp:
                is_prem    = True
                is_trial   = prem_doc.get("trial", False)
                expiry_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M")

        return aio_web.json_response({
            "is_premium":       is_prem,
            "is_trial":         is_trial,
            "trial_used":       bool(trial_doc),
            "expiry":           expiry_str,
            "refer_count":      user_doc.get("refer_count", 0) if user_doc else 0,
            "pending_payments": pending,
        })
    except Exception as e:
        logger.error(f"api_user_status error: {e}")
        return aio_web.json_response({"is_premium": False, "error": str(e)})


@routes.post("/api/submit_payment")
async def api_submit_payment(request):
    try:
        data = await request.json()
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id    = data.get("user_id")
    name       = data.get("name", "Unknown")
    plan_id    = data.get("plan_id")
    amount     = data.get("amount")
    txn_id     = data.get("txn_id", "").strip()
    screenshot = data.get("screenshot", "")
    group_id   = data.get("group_id")

    if not user_id:
        return aio_web.json_response({"ok": False, "error": "Telegram se kholo — user ID nahi mila!"}, status=400)
    if not plan_id:
        return aio_web.json_response({"ok": False, "error": "Plan select karo!"}, status=400)
    if not txn_id:
        return aio_web.json_response({"ok": False, "error": "Transaction ID bharo!"}, status=400)

    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return aio_web.json_response({"ok": False, "error": "Invalid user ID!"}, status=400)

    existing = await payments_col.find_one({"txn_id": txn_id})
    if existing:
        return aio_web.json_response({"ok": False, "error": "Yeh TXN ID already submit ho chuki!"})

    plan_days = {
        "10days": 10, "30days": 30, "60days": 60,
        "150days": 150, "365days": 365,
        "group_1m": 30, "group_2m": 60
    }
    days     = plan_days.get(str(plan_id), 30)
    is_group = str(plan_id).startswith("group_")

    pay_doc = {
        "user_id": user_id, "name": name, "plan_id": plan_id,
        "days": days, "amount": amount, "txn_id": txn_id,
        "screenshot": screenshot[:200] if screenshot else "",
        "status": "pending", "submitted_at": now(),
        "is_group": is_group,
        "group_id": str(group_id) if group_id else None,
    }
    result = await payments_col.insert_one(pay_doc)
    pay_id  = str(result.inserted_id)

    pay_type   = "🏘 GROUP PREMIUM" if is_group else "💎 USER PREMIUM"
    group_info = f"\n🏘 Group: `{group_id}`" if is_group and group_id else ""
    msg_text   = (
        f"💰 #NewPayment — {pay_type}\n\n"
        f"👤 {name} (`{user_id}`)\n"
        f"📦 Plan: **{plan_id}** ({days} din)\n"
        f"💵 Amount: **₹{amount}**\n"
        f"🔖 TXN: `{txn_id}`"
        f"{group_info}\n"
        f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"pay_approve_{pay_id}_{user_id}_{days}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"pay_reject_{pay_id}_{user_id}"),
    ]])

    sent_to_log = sent_to_owner = False
    if screenshot and screenshot.startswith("data:image"):
        try:
            img_data = base64.b64decode(screenshot.split(",")[1])
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
                tf.write(img_data)
                tf_path = tf.name
            try:
                await bot.send_photo(int(LOG_CHANNEL), tf_path, caption=msg_text, reply_markup=kb)
                sent_to_log = True
            except Exception as e:
                logger.error(f"Log photo error: {e}")
            try:
                await bot.send_photo(int(OWNER_ID), tf_path, caption=msg_text, reply_markup=kb)
                sent_to_owner = True
            except Exception as e:
                logger.error(f"Owner photo error: {e}")
            _os.unlink(tf_path)
        except Exception as e:
            logger.error(f"Screenshot error: {e}")

    if not sent_to_log:
        try:
            await bot.send_message(int(LOG_CHANNEL), msg_text, reply_markup=kb)
        except Exception as e:
            logger.error(f"Log msg error: {e}")
    if not sent_to_owner:
        try:
            await bot.send_message(int(OWNER_ID), msg_text, reply_markup=kb)
        except Exception as e:
            logger.error(f"Owner msg error: {e}")

    try:
        if bot and user_id:
            await bot.send_message(user_id,
                f"✅ **Payment Submit Ho Gayi!**\n\n"
                f"📦 Plan: **{plan_id}** ({days} din)\n"
                f"💵 Amount: **₹{amount}**\n"
                f"🔖 TXN: `{txn_id}`\n\n"
                f"⏳ Owner verify karega — **1-2 ghante** mein activate hoga!\n"
                f"📩 Problem ho to @asbhaibsr se baat karo."
            )
    except Exception as e:
        logger.debug(f"User confirm PM failed: {e}")

    return aio_web.json_response({
        "ok": True,
        "message": "✅ Payment submit ho gayi! Bot PM check karo — 1-2 ghante mein activate hoga."
    })


@routes.post("/api/claim_trial")
async def api_claim_trial(request):
    try:
        data = await request.json()
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id = data.get("user_id")
    if not user_id:
        return aio_web.json_response({"ok": False, "error": "Telegram se kholo — user ID nahi mila"}, status=400)
    try:
        user_id = int(user_id)
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid user ID"}, status=400)

    from database import free_trial_col, premium_col
    doc = await free_trial_col.find_one({"user_id": user_id})
    if doc:
        return aio_web.json_response({"ok": False, "message": "❌ Trial pehle le chuke ho! Premium lo."})

    expiry = now() + timedelta(minutes=5)
    await free_trial_col.insert_one({"user_id": user_id, "claimed_at": now()})
    await premium_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "trial": True, "plan": "trial_5min", "added": now()}},
        upsert=True,
    )
    try:
        if bot:
            await bot.send_message(user_id,
                "🆓 **Free Trial Shuru!**\n\n"
                "5 minute ke liye Premium active hai!\n"
                "Abhi group mein movie naam type karo!\n\n"
                "⏰ 5 min baad khatam hoga.\n"
                "💎 Continue karo — /premium dekho!"
            )
    except Exception as e:
        logger.warning(f"Trial PM failed uid={user_id}: {e}")

    await send_log(
        f"🆓 #FreeTrial\n\nɪᴅ - `{user_id}`\nᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
    )
    return aio_web.json_response({"ok": True, "message": "✅ Trial shuru! 5 min ke liye Premium active. Bot PM check karo!"})


@routes.get("/api/stream_link/{msg_id}")
async def api_stream_link(request):
    """
    Stream + Download link generate karo — Premium ke liye.
    Yahan links ready milte hain — button daba ke seedha player mein!
    """
    msg_id  = int(request.match_info["msg_id"])
    uid_str = request.rel_url.query.get("uid", "")

    if not uid_str:
        return aio_web.json_response({"ok": False, "error": "User ID required"}, status=400)
    try:
        uid = int(uid_str)
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid user ID"}, status=400)

    if uid not in ADMINS and not await is_premium(uid):
        return aio_web.json_response({"ok": False, "error": "💎 Premium required!"}, status=403)

    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"ok": False, "error": "File not found"}, status=404)

    base = KOYEB_URL or ""
    return aio_web.json_response({
        "ok":           True,
        "file_name":    info["file_name"],
        "file_size":    info["file_size"],
        "mime_type":    info["mime_type"],
        "duration":     info.get("duration", 0),
        "stream_url":   f"{base}/stream_file/{msg_id}?uid={uid}",
        "download_url": f"{base}/download/{msg_id}?uid={uid}",
        "player_url":   f"{base}/?uid={uid}&mid={msg_id}",
    }, headers={"Access-Control-Allow-Origin": "*"})


@routes.post("/api/help")
async def api_help(request):
    try:
        data = await request.json()
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id  = data.get("user_id")
    name     = data.get("name", "Unknown")
    msg_text = data.get("message", "").strip()
    if not msg_text:
        return aio_web.json_response({"ok": False, "error": "Message empty"}, status=400)

    await help_msgs_col.insert_one({"user_id": user_id, "name": name, "message": msg_text, "time": now()})
    log_text = (
        f"📩 #HelpMessage\n\n"
        f"👤 {name} (`{user_id}`)\n"
        f"💬 {msg_text}\n"
        f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    )
    await send_log(log_text)
    try:
        if bot and OWNER_ID:
            await bot.send_message(int(OWNER_ID), log_text, disable_web_page_preview=True)
    except Exception as e:
        logger.debug(f"help owner PM failed: {e}")

    return aio_web.json_response({"ok": True, "message": "✅ Message bhej diya! Owner jaldi reply karega."})


# ═══════════════════════════════════════
#  SERVER START
# ═══════════════════════════════════════
async def run_aiohttp_server():
    aio_app.add_routes(routes)
    runner = aio_web.AppRunner(aio_app)
    await runner.setup()
    site = aio_web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"✅ aiohttp server started on port {PORT}")
