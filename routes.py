# ╔══════════════════════════════════════╗
# ║  routes.py — AsBhai Drop Bot         ║
# ║  aiohttp Streaming Server & API      ║
# ╚══════════════════════════════════════╝
import asyncio
import base64
import logging
import os as _os
import tempfile
from datetime import timedelta

from aiohttp import web as aio_web
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from config import (
    FILE_CHANNEL, KOYEB_URL, UPI_ID, PORT,
    ADMINS, OWNER_ID, LOG_CHANNEL, IST, now, now_ist, make_aware, logger
)
from database import (
    premium_col, free_trial_col, users_col, payments_col, help_msgs_col,
    is_premium, send_log, add_premium
)

bot = None
userbot = None

def set_clients(b, u):
    global bot, userbot
    bot = b
    userbot = u

aio_app = aio_web.Application(client_max_size=50*1024*1024)  # 50MB max upload
routes = aio_web.RouteTableDef()

# ═══════════════════════════════════════
#  STREAMING HELPERS
# ═══════════════════════════════════════
async def get_file_info(msg_id: int):
    try:
        file_msg = await bot.get_messages(FILE_CHANNEL, msg_id)
        if not file_msg or file_msg.empty:
            return None
        f = None
        fname = f"file_{msg_id}"
        mime = "application/octet-stream"
        duration = 0
        if file_msg.video:
            f = file_msg.video
            fname = f.file_name or f"video_{msg_id}.mp4"
            mime = f.mime_type or "video/mp4"
            duration = f.duration or 0
        elif file_msg.document:
            f = file_msg.document
            fname = f.file_name or f"file_{msg_id}"
            mime = f.mime_type or "application/octet-stream"
        elif file_msg.audio:
            f = file_msg.audio
            fname = f.file_name or f"audio_{msg_id}.mp3"
            mime = f.mime_type or "audio/mpeg"
            duration = f.duration or 0
        if not f:
            return None
        return {
            "file_id": f.file_id,
            "file_size": f.file_size or 0,
            "file_name": fname,
            "mime_type": mime,
            "duration": duration,
            "msg": file_msg,
        }
    except Exception as e:
        logger.error(f"get_file_info {msg_id}: {e}")
        return None


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
        "time": now_ist().strftime("%d %b %H:%M IST")
    })


@routes.get("/stream")
async def stream_page_handler(request):
    return await home_handler(request)


@routes.get(r"/file_info/{msg_id:\d+}")
async def file_info_handler(request: aio_web.Request):
    msg_id = int(request.match_info["msg_id"])
    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)
    return aio_web.json_response(
        {
            "file_name": info["file_name"],
            "file_size": info["file_size"],
            "mime_type": info["mime_type"],
            "duration": info.get("duration", 0),
        },
        headers={"Access-Control-Allow-Origin": "*"},
    )


@routes.get(r"/stream_file/{msg_id:\d+}", allow_head=True)
async def stream_file_handler(request: aio_web.Request):
    """
    FIX: Improved streaming with proper range requests and CORS.
    Supports seeking, quality control, and all browsers.
    """
    msg_id = int(request.match_info["msg_id"])

    # CORS preflight
    if request.method == "OPTIONS":
        return aio_web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Access-Control-Max-Age": "86400",
        })

    # UID check — premium only for streaming
    uid_str = request.rel_url.query.get("uid", "")
    if uid_str:
        try:
            uid_int = int(uid_str)
            if uid_int not in ADMINS and not await is_premium(uid_int):
                return aio_web.json_response(
                    {"error": "Premium required for streaming"}, status=403
                )
        except Exception:
            pass

    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)

    file_size = info["file_size"]
    mime_type = info["mime_type"]
    file_name = info["file_name"]
    file_msg  = info["msg"]

    if file_size == 0:
        return aio_web.json_response({"error": "File size unknown"}, status=404)

    # Range request parse
    range_header = request.headers.get("Range", "")
    from_bytes, until_bytes = 0, file_size - 1
    if range_header:
        try:
            parts = range_header.replace("bytes=", "").split("-")
            from_bytes  = int(parts[0]) if parts[0] else 0
            until_bytes = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
        except Exception:
            pass
    until_bytes = min(until_bytes, file_size - 1)
    req_length  = until_bytes - from_bytes + 1

    headers = {
        "Content-Type":    mime_type,
        "Content-Range":   f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length":  str(req_length),
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Accept-Ranges":   "bytes",
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Access-Control-Expose-Headers": "Content-Range, Content-Length, Accept-Ranges",
        # Cache for 1 hour — better performance
        "Cache-Control": "public, max-age=3600",
    }

    status   = 206 if range_header else 200
    
    # HEAD request — just return headers
    if request.method == "HEAD":
        return aio_web.Response(status=status, headers=headers)
    
    response = aio_web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    try:
        client = userbot if userbot else bot
        if not client:
            return aio_web.json_response({"error": "No client"}, status=503)

        CHUNK = 1024 * 256  # 256 KB
        offset_kb    = from_bytes // CHUNK
        first_cut    = from_bytes - offset_kb * CHUNK
        bytes_written = 0
        chunk_num    = 0

        async for chunk in client.stream_media(file_msg, offset=offset_kb):
            if not chunk:
                break
            if chunk_num == 0 and first_cut > 0:
                chunk = chunk[first_cut:]
            chunk_num += 1

            remaining = req_length - bytes_written
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            if not chunk:
                break

            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError):
                break
            except Exception:
                break
            bytes_written += len(chunk)
            if bytes_written >= req_length:
                break

    except (ConnectionResetError, ConnectionAbortedError):
        pass
    except Exception as e:
        logger.error(f"stream_file error msg_id={msg_id}: {type(e).__name__}: {e}")

    try:
        await response.write_eof()
    except Exception:
        pass
    return response


@routes.get(r"/download/{msg_id:\d+}", allow_head=True)
async def download_handler(request: aio_web.Request):
    msg_id = int(request.match_info["msg_id"])
    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)

    file_size  = info["file_size"]
    file_name  = info["file_name"]
    mime_type  = info["mime_type"]
    file_msg   = info["msg"]

    headers = {
        "Content-Type":        mime_type,
        "Content-Length":      str(file_size),
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges":       "bytes",
        "Access-Control-Allow-Origin": "*",
    }
    
    if request.method == "HEAD":
        return aio_web.Response(status=200, headers=headers)
    
    response = aio_web.StreamResponse(status=200, headers=headers)
    await response.prepare(request)

    try:
        client = userbot if userbot else bot
        async for chunk in client.stream_media(file_msg):
            if not chunk:
                break
            try:
                await response.write(chunk)
            except (ConnectionResetError, ConnectionAbortedError):
                break
    except (ConnectionResetError, ConnectionAbortedError):
        pass
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
        prem_doc = await premium_col.find_one({"user_id": user_id})
        trial_doc = await free_trial_col.find_one({"user_id": user_id})
        user_doc = await users_col.find_one({"user_id": user_id})
        pending  = await payments_col.count_documents({"user_id": user_id, "status": "pending"})

        is_prem, is_trial, expiry_str = False, False, None
        if prem_doc and prem_doc.get("expiry"):
            exp = make_aware(prem_doc["expiry"])
            if now() < exp:
                is_prem  = True
                is_trial = prem_doc.get("trial", False)
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
    """FIX: Payment submit — PM mein message + approve/reject buttons"""
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

    # Convert user_id to int safely
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        return aio_web.json_response({"ok": False, "error": "Invalid user ID!"}, status=400)

    existing = await payments_col.find_one({"txn_id": txn_id})
    if existing:
        return aio_web.json_response({"ok": False, "error": "Yeh TXN ID already submit ho chuki!"})

    plan_days = {
        "10days": 10, "30days": 30, "60days": 60, "150days": 150,
        "365days": 365, "group_1m": 30, "group_2m": 60
    }
    days = plan_days.get(str(plan_id), 30)
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
    pay_id = str(result.inserted_id)

    pay_type = "🏘 GROUP PREMIUM" if is_group else "💎 USER PREMIUM"
    group_info = f"\n🏘 Group: `{group_id}`" if is_group and group_id else ""
    msg_text = (
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

    # Send to log channel + owner PM
    sent_to_log = False
    sent_to_owner = False
    
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
                logger.error(f"Log channel photo send error: {e}")
            try:
                await bot.send_photo(int(OWNER_ID), tf_path, caption=msg_text, reply_markup=kb)
                sent_to_owner = True
            except Exception as e:
                logger.error(f"Owner PM photo error: {e}")
            _os.unlink(tf_path)
        except Exception as e:
            logger.error(f"Screenshot processing error: {e}")
    
    # Fallback: text message
    if not sent_to_log:
        try:
            await bot.send_message(int(LOG_CHANNEL), msg_text, reply_markup=kb)
            sent_to_log = True
        except Exception as e:
            logger.error(f"Log channel text error: {e}")
    
    if not sent_to_owner:
        try:
            await bot.send_message(int(OWNER_ID), msg_text, reply_markup=kb)
            sent_to_owner = True
        except Exception as e:
            logger.error(f"Owner PM text error: {e}")

    # Send confirmation to user PM
    try:
        if bot and user_id:
            confirm_text = (
                f"✅ **Payment Submit Ho Gayi!**\n\n"
                f"📦 Plan: **{plan_id}** ({days} din)\n"
                f"💵 Amount: **₹{amount}**\n"
                f"🔖 TXN: `{txn_id}`\n\n"
                f"⏳ Owner verify karega — **1-2 ghante** mein activate hoga!\n"
                f"📩 Koi problem ho to @asbhaibsr se baat karo."
            )
            await bot.send_message(user_id, confirm_text)
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
        {"$set": {
            "user_id": user_id,
            "expiry":  expiry,
            "trial":   True,
            "plan":    "trial_5min",
            "added":   now(),
        }},
        upsert=True,
    )

    try:
        if bot:
            await bot.send_message(
                user_id,
                "🆓 **Free Trial Shuru!**\n\n"
                "5 minute ke liye Premium active hai!\n"
                "Abhi group mein movie naam type karo!\n\n"
                "⏰ 5 min baad khatam hoga.\n"
                "💎 Continue karo — /premium dekho!"
            )
    except Exception as e:
        logger.warning(f"Trial PM failed uid={user_id}: {e}")

    await send_log(
        f"🆓 #FreeTrial\n\n"
        f"ɪᴅ - `{user_id}`\n"
        f"ᴛɪᴍᴇ - {now_ist().strftime('%d %b %H:%M')} IST"
    )
    return aio_web.json_response({"ok": True, "message": "✅ Trial shuru! 5 min ke liye Premium active. Bot PM check karo!"})


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

    await help_msgs_col.insert_one({
        "user_id": user_id, "name": name,
        "message": msg_text, "time": now()
    })

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
