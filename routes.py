# ╔══════════════════════════════════════════════════════╗
# ║  routes.py — AsBhai Drop Bot v3.2 FIXED             ║
# ║  Pyrofork Raw MTProto Streaming                     ║
# ║  session.send() — correct pyrofork API              ║
# ║  Inspired by TG-FileStreamBot & VCPlayerBot         ║
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
    FileReferenceExpired, FileReferenceInvalid, AuthBytesInvalid
)
from pyrogram import raw
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES
from pyrogram.session import Auth, Session

from config import (
    FILE_CHANNEL, KOYEB_URL, UPI_ID, PORT,
    ADMINS, OWNER_ID, LOG_CHANNEL, IST, now, now_ist, make_aware, logger,
    AD_SYSTEM_ENABLED
)
from database import (
    premium_col, free_trial_col, users_col, payments_col, help_msgs_col,
    is_premium, send_log, add_premium,
    ads_col, ad_create, ad_list, ad_active_list, ad_get_next, ad_delete, ad_toggle, ad_count
)

bot     = None
userbot = None

def set_clients(b, u):
    global bot, userbot
    bot     = b
    userbot = u

aio_app = aio_web.Application(client_max_size=50 * 1024 * 1024)
routes  = aio_web.RouteTableDef()

# ═══════════════════════════════════════════════════════
#  PYROFORK RAW MTProto STREAMING ENGINE
#  TG-FileStreamBot approach — direct upload.GetFile
#  session.send() — correct pyrofork method (NOT client.send)
#  Har request pe fresh message fetch — file_reference kabhi expire nahi
# ═══════════════════════════════════════════════════════

STREAM_CHUNK = 1024 * 1024  # 1MB — Telegram ka max chunk size


async def _get_media_session(client, dc_id: int) -> Session:
    """
    Pyrofork mein media session banana/lena.
    client.media_sessions dict mein cache hota hai.
    session.send() use karo — client.send() nahi (woh exist nahi karta pyrofork mein).
    """
    async with client.media_sessions_lock:
        session = client.media_sessions.get(dc_id)
        if session is not None:
            return session

        # Kya yeh client ka own DC hai?
        own_dc = await client.storage.dc_id()

        if dc_id != own_dc:
            # Doosre DC ke liye — naya auth key banao aur cross-DC auth karo
            auth_key = await Auth(client, dc_id, await client.storage.test_mode()).create()
            session = Session(
                client, dc_id, auth_key,
                await client.storage.test_mode(),
                is_media=True
            )
            await session.start()

            # Cross-DC authorization
            for attempt in range(3):
                exported = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                try:
                    await session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported.id,
                            bytes=exported.bytes
                        )
                    )
                    break
                except AuthBytesInvalid:
                    if attempt == 2:
                        await session.stop()
                        raise
                    continue
        else:
            # Apne DC ke liye — existing auth key use karo
            auth_key = await client.storage.auth_key()
            session = Session(
                client, dc_id, auth_key,
                await client.storage.test_mode(),
                is_media=True
            )
            await session.start()

        client.media_sessions[dc_id] = session
        return session


async def _fetch_fresh_message(msg_id: int):
    """
    Har baar FRESH message fetch karo from FILE_CHANNEL.
    Fresh message = fresh file_reference = file_reference_expired kabhi nahi.
    """
    for try_client in [userbot, bot]:  # userbot pehle — zyada permissions
        if not try_client:
            continue
        try:
            msg = await try_client.get_messages(FILE_CHANNEL, msg_id)
            if msg and not msg.empty:
                return msg, try_client
        except Exception as e:
            logger.debug(f"fetch_fresh_message {msg_id} via {type(try_client).__name__}: {e}")
    return None, None


def _extract_media(msg):
    """Message se media object, filename, mime, duration nikalo."""
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
    """File info — fresh message se har baar."""
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


async def _stream_chunks(client, msg, from_bytes: int, req_length: int):
    """
    CORE STREAMING — Direct Telegram upload.GetFile via MTProto session.

    Steps (TG-FileStreamBot approach):
    1. Fresh message se file_id decode karo
    2. InputDocumentFileLocation banao (fresh file_reference included)
    3. Correct DC ka media session lo
    4. session.invoke(upload.GetFile) loop se chunks laao
    5. Seekable — from_bytes se start karo

    session.invoke() = correct pyrofork method
    (session.send() bhi kaam karta hai but invoke() preferred hai)
    """
    media, _, _, _ = _extract_media(msg)
    if not media:
        return

    # file_id decode karo — isme dc_id, access_hash, file_reference sab hai
    fid      = FileId.decode(media.file_id)
    dc_id    = fid.dc_id

    # InputDocumentFileLocation — FRESH file_reference from new message fetch
    location = raw.types.InputDocumentFileLocation(
        id=fid.media_id,
        access_hash=fid.access_hash,
        file_reference=fid.file_reference,  # Fresh — kabhi expire nahi hoga
        thumb_size=""
    )

    # Correct DC ka media session
    try:
        session = await _get_media_session(client, dc_id)
    except Exception as e:
        logger.error(f"media session error dc={dc_id}: {e}")
        return

    # Seek: aligned offset (1MB boundary pe)
    offset    = (from_bytes // STREAM_CHUNK) * STREAM_CHUNK
    first_cut = from_bytes - offset
    sent      = 0

    while sent < req_length:
        try:
            # session.invoke() — correct pyrofork raw API call
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=offset,
                    limit=STREAM_CHUNK
                )
            )
        except Exception as e:
            logger.error(f"GetFile error offset={offset}: {type(e).__name__}: {e}")
            break

        if not isinstance(r, raw.types.upload.File):
            logger.error(f"Unexpected GetFile response: {type(r)}")
            break

        chunk = r.bytes
        if not chunk:
            break  # EOF

        # Pehle chunk mein range start tak skip karo
        if first_cut > 0:
            chunk     = chunk[first_cut:]
            first_cut = 0

        # Zaroorat se zyada mat bhejo
        remaining = req_length - sent
        if len(chunk) > remaining:
            chunk = chunk[:remaining]

        if not chunk:
            break

        yield chunk
        sent   += len(chunk)
        offset += STREAM_CHUNK

        # File khatam ho gayi
        if len(r.bytes) < STREAM_CHUNK:
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
    ADVANCED STREAMING — Pyrofork Raw MTProto.
    Har request: fresh message → fresh file_reference → session.invoke(GetFile).
    FILE_REFERENCE_EXPIRED aayega hi nahi.
    Range requests (seeking) perfect kaam karta hai.
    """
    msg_id = int(request.match_info["msg_id"])

    # CORS
    if request.method == "OPTIONS":
        return aio_web.Response(headers={
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
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

    # Fresh message — har request pe
    msg, stream_client = await _fetch_fresh_message(msg_id)
    if not msg:
        return aio_web.json_response({"error": "File not found"}, status=404)

    media, fname, mime, _ = _extract_media(msg)
    if not media:
        return aio_web.json_response({"error": "No streamable media"}, status=404)

    file_size = media.file_size or 0
    if file_size == 0:
        return aio_web.json_response({"error": "File size unknown"}, status=404)

    if not stream_client:
        stream_client = userbot or bot
    if not stream_client:
        return aio_web.json_response({"error": "No client"}, status=503)

    # Range parse
    range_hdr   = request.headers.get("Range", "")
    from_bytes  = 0
    until_bytes = file_size - 1
    if range_hdr:
        try:
            parts       = range_hdr.replace("bytes=", "").split("-")
            from_bytes  = int(parts[0]) if parts[0] else 0
            until_bytes = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
        except Exception:
            pass
    until_bytes = min(until_bytes, file_size - 1)
    req_length  = until_bytes - from_bytes + 1

    status  = 206 if range_hdr else 200
    headers = {
        "Content-Type":                  mime,
        "Content-Range":                 f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length":                str(req_length),
        "Content-Disposition":           f'inline; filename="{fname}"',
        "Accept-Ranges":                 "bytes",
        "Access-Control-Allow-Origin":   "*",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        "Cache-Control":                 "no-cache",
    }

    if request.method == "HEAD":
        return aio_web.Response(status=status, headers=headers)

    response = aio_web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in _stream_chunks(stream_client, msg, from_bytes, req_length):
            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError, asyncio.CancelledError):
                break
            except Exception as e:
                logger.debug(f"write error: {e}")
                break
    except Exception as e:
        logger.error(f"stream_file_handler error msg={msg_id}: {type(e).__name__}: {e}")

    try:
        await response.write_eof()
    except Exception:
        pass
    return response


@routes.get(r"/download/{msg_id:\d+}", allow_head=True)
async def download_handler(request: aio_web.Request):
    """Download — same raw engine, attachment header."""
    msg_id = int(request.match_info["msg_id"])

    uid_str = request.rel_url.query.get("uid", "")
    if uid_str:
        try:
            uid_int = int(uid_str)
            if uid_int not in ADMINS and not await is_premium(uid_int):
                return aio_web.json_response({"error": "💎 Premium required"}, status=403)
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
        dl_client = userbot or bot
    if not dl_client:
        return aio_web.json_response({"error": "No client"}, status=503)

    headers = {
        "Content-Type":                "mime",
        "Content-Length":              str(file_size),
        "Content-Disposition":         f'attachment; filename="{fname}"',
        "Accept-Ranges":               "bytes",
        "Access-Control-Allow-Origin": "*",
    }

    if request.method == "HEAD":
        return aio_web.Response(status=200, headers=headers)

    response = aio_web.StreamResponse(status=200, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in _stream_chunks(dl_client, msg, 0, file_size):
            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError, asyncio.CancelledError):
                break
            except Exception:
                break
    except Exception as e:
        logger.error(f"download_handler error msg={msg_id}: {e}")

    try:
        await response.write_eof()
    except Exception:
        pass
    return response


# ═══════════════════════════════════════
#  API ROUTES — unchanged
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
        user_id   = int(request.match_info["user_id"])
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
        logger.error(f"api_user_status: {e}")
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

    plan_days = {"10days": 10, "30days": 30, "60days": 60, "150days": 150, "365days": 365, "group_1m": 30, "group_2m": 60}
    days     = plan_days.get(str(plan_id), 30)
    is_group = str(plan_id).startswith("group_")
    pay_doc  = {
        "user_id": user_id, "name": name, "plan_id": plan_id,
        "days": days, "amount": amount, "txn_id": txn_id,
        "screenshot": screenshot[:200] if screenshot else "",
        "status": "pending", "submitted_at": now(),
        "is_group": is_group, "group_id": str(group_id) if group_id else None,
    }
    result  = await payments_col.insert_one(pay_doc)
    pay_id  = str(result.inserted_id)
    pay_type = "🏘 GROUP PREMIUM" if is_group else "💎 USER PREMIUM"
    group_info = f"\n🏘 Group: `{group_id}`" if is_group and group_id else ""
    msg_text = (
        f"💰 #NewPayment — {pay_type}\n\n👤 {name} (`{user_id}`)\n"
        f"📦 Plan: **{plan_id}** ({days} din)\n💵 Amount: **₹{amount}**\n"
        f"🔖 TXN: `{txn_id}`{group_info}\n🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"pay_approve_{pay_id}_{user_id}_{days}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"pay_reject_{pay_id}_{user_id}"),
    ]])
    sent_log = sent_own = False
    if screenshot and screenshot.startswith("data:image"):
        try:
            img_data = base64.b64decode(screenshot.split(",")[1])
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
                tf.write(img_data); tf_path = tf.name
            try: await bot.send_photo(int(LOG_CHANNEL), tf_path, caption=msg_text, reply_markup=kb); sent_log = True
            except Exception as e: logger.error(f"log photo: {e}")
            try: await bot.send_photo(int(OWNER_ID), tf_path, caption=msg_text, reply_markup=kb); sent_own = True
            except Exception as e: logger.error(f"owner photo: {e}")
            _os.unlink(tf_path)
        except Exception as e: logger.error(f"screenshot: {e}")
    if not sent_log:
        try: await bot.send_message(int(LOG_CHANNEL), msg_text, reply_markup=kb)
        except Exception as e: logger.error(f"log msg: {e}")
    if not sent_own:
        try: await bot.send_message(int(OWNER_ID), msg_text, reply_markup=kb)
        except Exception as e: logger.error(f"owner msg: {e}")
    try:
        if bot and user_id:
            await bot.send_message(user_id,
                f"✅ **Payment Submit Ho Gayi!**\n\n📦 Plan: **{plan_id}** ({days} din)\n"
                f"💵 Amount: **₹{amount}**\n🔖 TXN: `{txn_id}`\n\n"
                f"⏳ Owner verify karega — **1-2 ghante** mein activate hoga!\n"
                f"📩 Problem ho to @asbhaibsr se baat karo."
            )
    except Exception as e: logger.debug(f"user confirm: {e}")
    return aio_web.json_response({"ok": True, "message": "✅ Payment submit ho gayi! Bot PM check karo."})


@routes.post("/api/claim_trial")
async def api_claim_trial(request):
    try:
        data = await request.json()
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)
    user_id = data.get("user_id")
    if not user_id:
        return aio_web.json_response({"ok": False, "error": "Telegram se kholo"}, status=400)
    try: user_id = int(user_id)
    except: return aio_web.json_response({"ok": False, "error": "Invalid user ID"}, status=400)
    from database import free_trial_col, premium_col
    doc = await free_trial_col.find_one({"user_id": user_id})
    if doc:
        return aio_web.json_response({"ok": False, "message": "❌ Trial pehle le chuke ho!"})
    expiry = now() + timedelta(minutes=5)
    await free_trial_col.insert_one({"user_id": user_id, "claimed_at": now()})
    await premium_col.update_one({"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "trial": True, "plan": "trial_5min", "added": now()}},
        upsert=True)
    try:
        if bot:
            await bot.send_message(user_id,
                "🆓 **Free Trial Shuru!**\n\n5 minute ke liye Premium active!\n"
                "Abhi group mein movie naam type karo!\n\n⏰ 5 min baad khatam.\n💎 /premium dekho!")
    except Exception as e: logger.warning(f"trial PM: {e}")
    await send_log(f"🆓 #FreeTrial\n\nɪᴅ - `{user_id}`\nᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST")
    return aio_web.json_response({"ok": True, "message": "✅ Trial shuru! 5 min Premium. Bot PM check karo!"})


@routes.get("/api/stream_link/{msg_id}")
async def api_stream_link(request):
    msg_id  = int(request.match_info["msg_id"])
    uid_str = request.rel_url.query.get("uid", "")
    if not uid_str:
        return aio_web.json_response({"ok": False, "error": "User ID required"}, status=400)
    try: uid = int(uid_str)
    except: return aio_web.json_response({"ok": False, "error": "Invalid user ID"}, status=400)
    if uid not in ADMINS and not await is_premium(uid):
        return aio_web.json_response({"ok": False, "error": "💎 Premium required!"}, status=403)
    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"ok": False, "error": "File not found"}, status=404)
    base = KOYEB_URL or ""
    return aio_web.json_response({
        "ok": True,
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
    log_text = f"📩 #HelpMessage\n\n👤 {name} (`{user_id}`)\n💬 {msg_text}\n🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    await send_log(log_text)
    try:
        if bot and OWNER_ID:
            await bot.send_message(int(OWNER_ID), log_text, disable_web_page_preview=True)
    except Exception as e: logger.debug(f"help PM: {e}")
    return aio_web.json_response({"ok": True, "message": "✅ Message bhej diya!"})



# ═══════════════════════════════════════
#  AD VERIFY API
# ═══════════════════════════════════════
@routes.post("/api/ad_verify")
async def api_ad_verify(request):
    try:
        data = await request.json()
    except Exception:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    token   = data.get("token", "").strip()
    user_id = data.get("user_id")

    if not token or not user_id:
        return aio_web.json_response({"ok": False, "error": "Token ya user_id missing"}, status=400)

    try:
        from database import (
            check_token, tokens_col, mark_verified, now_ist, send_log,
            users_col as _ucol, make_aware, now as _now
        )
        from bson import ObjectId

        uid = int(user_id)
        valid, stored_uid = await check_token(token, expected_uid=uid)
        if not valid:
            return aio_web.json_response({"ok": False, "error": "Token invalid ya expire ho gaya! Bot pe /start karo."})

        await tokens_col.delete_one({"token": token})
        await mark_verified(uid)

        # Log
        try:
            user_doc = await _ucol.find_one({"user_id": uid})
            fname = (user_doc or {}).get("name", "User")
            _sc = lambda t: t  # simple fallback
            await send_log(
                f"✅ #VerifyComplete\n\n"
                f"ɪᴅ - `{uid}`\n"
                f"Nᴀᴍᴇ - {str(fname)[:20]}\n"
                f"ᴍᴏᴅᴇ - Bot Ad Page\n"
                f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
            )
        except Exception as le:
            logger.warning(f"ad_verify log: {le}")

        # Pending file
        try:
            if bot:
                user_doc = await _ucol.find_one({"user_id": uid})
                pfid = (user_doc or {}).get("pending_file_id", 0)
                pchat = (user_doc or {}).get("pending_file_chat", 0)
                if pfid:
                    await _ucol.update_one(
                        {"user_id": uid},
                        {"$unset": {"pending_file_id": "", "pending_file_chat": ""}}
                    )
                    from database import check_member_multi, build_fsub_keyboard, is_premium as _isp, send_file_to_pm
                    prem_user = await _isp(uid)
                    joined, not_joined = await check_member_multi(uid, prem_user)
                    if not joined:
                        kb = await build_fsub_keyboard(not_joined, uid)
                        await bot.send_message(uid, "📢 Pehle channel join karo — phir file milegi! ✅", reply_markup=kb)
                    else:
                        class _U:
                            id = uid
                            first_name = (user_doc or {}).get("name", "User")
                        await send_file_to_pm(bot, _U(), int(pfid), prem_user)
        except Exception as fe:
            logger.warning(f"ad_verify file: {fe}")

        return aio_web.json_response({"ok": True, "message": "✅ Verified! File aa rahi hai!"})
    except Exception as e:
        logger.error(f"api_ad_verify: {e}")
        return aio_web.json_response({"ok": False, "error": str(e)}, status=500)


# ═══════════════════════════════════════
#  AD MANAGER API  (Admin only)
# ═══════════════════════════════════════

def _is_admin(request):
    """Query param ?admin_id= check karo."""
    try:
        aid = int(request.rel_url.query.get("admin_id", "0"))
        return aid in ADMINS
    except:
        return False

@routes.get("/api/ads")
async def api_ads_list(request):
    """Saare ads list (admin only)."""
    if not _is_admin(request):
        return aio_web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    docs = await ad_list()
    result = []
    for d in docs:
        exp = d.get("expiry_date")
        result.append({
            "id": str(d["_id"]),
            "title": d.get("title", ""),
            "description": d.get("description", ""),
            "button_name": d.get("button_name", "Visit"),
            "ad_url": d.get("ad_url", ""),
            "image_url": d.get("image_url", ""),
            "script": d.get("script", ""),
            "timer1": d.get("timer1", 60),
            "timer2": d.get("timer2", 30),
            "expiry_date": exp.isoformat() if hasattr(exp, "isoformat") else str(exp) if exp else None,
            "active": d.get("active", True),
            "impressions": d.get("impressions", 0),
            "created_at": d.get("created_at", now()).strftime("%d %b %Y %H:%M") if hasattr(d.get("created_at"), "strftime") else "",
        })
    total, active_count = await ad_count()
    return aio_web.json_response({"ok": True, "ads": result, "total": total, "active": active_count})

@routes.post("/api/ads/create")
async def api_ads_create(request):
    """Naya ad create karo (admin only)."""
    if not _is_admin(request):
        return aio_web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    try:
        data = await request.json()
    except:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    title       = data.get("title", "").strip()
    description = data.get("description", "").strip()
    button_name = data.get("button_name", "Visit Ad").strip()
    if not title or not description or not button_name:
        return aio_web.json_response({"ok": False, "error": "title, description, button_name required"})

    ad_url      = data.get("ad_url", "").strip()
    image_url   = data.get("image_url", "").strip()
    script      = data.get("script", "").strip()
    timer1      = max(10, min(300, int(data.get("timer1", 60))))
    timer2      = max(10, min(300, int(data.get("timer2", 30))))

    # Expiry date parse
    expiry_date = None
    exp_str = data.get("expiry_date", "").strip()
    if exp_str:
        try:
            from datetime import datetime as _dt
            expiry_date = _dt.fromisoformat(exp_str)
        except:
            pass

    doc = await ad_create(title, description, button_name, ad_url, image_url, script, timer1, timer2, expiry_date)
    await send_log(f"📢 #NewAd Created\n\n**{title}**\nButton: {button_name}\nTimer: {timer1}s / {timer2}s")
    return aio_web.json_response({"ok": True, "id": str(doc["_id"]), "message": "✅ Ad create ho gaya!"})

@routes.delete("/api/ads/{ad_id}")
async def api_ads_delete(request):
    """Ad delete karo (admin only)."""
    if not _is_admin(request):
        return aio_web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    ad_id = request.match_info.get("ad_id", "")
    deleted = await ad_delete(ad_id)
    if deleted:
        return aio_web.json_response({"ok": True, "message": "✅ Ad delete ho gaya!"})
    return aio_web.json_response({"ok": False, "error": "Ad nahi mila"})

@routes.post("/api/ads/{ad_id}/toggle")
async def api_ads_toggle(request):
    """Ad active/inactive toggle (admin only)."""
    if not _is_admin(request):
        return aio_web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    ad_id = request.match_info.get("ad_id", "")
    new_val = await ad_toggle(ad_id)
    if new_val is None:
        return aio_web.json_response({"ok": False, "error": "Ad nahi mila"})
    return aio_web.json_response({"ok": True, "active": new_val, "message": f"✅ Ad {'active' if new_val else 'inactive'} ho gaya!"})

@routes.get("/api/ads/next")
async def api_ads_next(request):
    """Next active ad fetch karo (user ke liye)."""
    try:
        uid = int(request.rel_url.query.get("uid", "0"))
    except:
        uid = 0
    ad = await ad_get_next(uid)
    if not ad:
        return aio_web.json_response({"ok": False, "error": "Koi active ad nahi"})
    return aio_web.json_response({
        "ok": True,
        "id": str(ad["_id"]),
        "title": ad.get("title", ""),
        "description": ad.get("description", ""),
        "button_name": ad.get("button_name", "Visit"),
        "ad_url": ad.get("ad_url", ""),
        "image_url": ad.get("image_url", ""),
        "script": ad.get("script", ""),
        "timer1": ad.get("timer1", 60),
        "timer2": ad.get("timer2", 30),
    })


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
