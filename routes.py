# ╔══════════════════════════════════════╗
# ║  routes.py — AsBhai Drop Bot         ║
# ║  aiohttp Streaming Server & API       ║
# ╚══════════════════════════════════════╝
# routes.py — AsBhai Drop Bot — aiohttp Streaming Server & API Routes
import asyncio, logging, math, mimetypes, secrets
from aiohttp import web as aio_web

from config import (
    BOT_TOKEN, FILE_CHANNEL, KOYEB_URL, UPI_ID, PORT,
    ADMINS, IST, now, now_ist, make_aware, logger
)

# bot/userbot reference — set from bot.py
bot = None
userbot = None

def set_clients(b, u):
    global bot, userbot
    bot = b
    userbot = u

aio_app = aio_web.Application()
_stream_cache = {}   # {msg_id: ByteStreamer info}

# ── Streaming helper ──
async def get_file_info(msg_id: int):
    """File message se streaming info nikalo"""
    try:
        file_msg = await bot.get_messages(FILE_CHANNEL, msg_id)
        if not file_msg or file_msg.empty:
            return None
        f = None
        fname = f"file_{msg_id}"
        mime = "application/octet-stream"
        if file_msg.video:
            f = file_msg.video
            fname = f.file_name or f"video_{msg_id}.mp4"
            mime = f.mime_type or "video/mp4"
        elif file_msg.document:
            f = file_msg.document
            fname = f.file_name or f"file_{msg_id}"
            mime = f.mime_type or "application/octet-stream"
        elif file_msg.audio:
            f = file_msg.audio
            fname = f.file_name or f"audio_{msg_id}.mp3"
            mime = f.mime_type or "audio/mpeg"
        if not f: return None
        return {
            "file_id": f.file_id,
            "file_size": f.file_size or 0,
            "file_name": fname,
            "mime_type": mime,
            "msg": file_msg
        }
    except Exception as e:
        logger.error(f"get_file_info {msg_id}: {e}")
        return None

async def stream_generator(file_id: str, offset: int = 0, limit: int = None):
    """
    Userbot se file chunks stream karo.
    Telegram ki koi size limit nahi — chunks mein aata hai.
    """
    chunk_size = 512 * 1024  # 512KB chunks
    current_offset = offset
    bytes_sent = 0

    try:
        async for chunk in userbot.stream_media(
            await userbot.get_messages(FILE_CHANNEL, 0) if False else None,  # placeholder
            file_id=file_id,
            offset=current_offset,
        ):
            if limit and bytes_sent + len(chunk) > limit:
                yield chunk[:limit - bytes_sent]
                break
            yield chunk
            bytes_sent += len(chunk)
            current_offset += len(chunk)
    except Exception as e:
        logger.error(f"stream_generator error: {e}")

async def stream_telegram_file(file_id: str, file_size: int, mime_type: str,
                                 file_name: str, request: aio_web.Request):
    """
    Range request support ke saath Telegram file stream karo.
    Seedha Pyrogram userbot se chunks leke browser ko bhejo.
    Works for ALL file sizes.
    """
    range_header = request.headers.get("Range")
    from_bytes = 0
    until_bytes = file_size - 1

    if range_header:
        try:
            rng = range_header.replace("bytes=", "").split("-")
            from_bytes = int(rng[0]) if rng[0] else 0
            until_bytes = int(rng[1]) if rng[1] else file_size - 1
        except:
            pass

    if until_bytes >= file_size:
        until_bytes = file_size - 1
    req_length = until_bytes - from_bytes + 1

    chunk_size = 512 * 1024  # 512KB
    offset = from_bytes - (from_bytes % chunk_size)
    first_cut = from_bytes - offset

    headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }

    status = 206 if range_header else 200
    response = aio_web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    try:
        client_to_use = userbot if userbot else bot
        bytes_written = 0
        async for chunk in client_to_use.stream_media(
            {"chat_id": FILE_CHANNEL, "message_id": 0},
            file_id=file_id,
            offset=offset,
        ):
            if bytes_written == 0 and first_cut > 0:
                chunk = chunk[first_cut:]
            remaining = req_length - bytes_written
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            if not chunk:
                break
            await response.write(chunk)
            bytes_written += len(chunk)
            if bytes_written >= req_length:
                break
    except Exception as e:
        logger.error(f"stream write error: {e}")

    await response.write_eof()
    return response

# ── AIOHTTP Routes ──
routes = aio_web.RouteTableDef()

@routes.get("/")
async def home_handler(request):
    """Mini app HTML serve karo"""
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
    """Stream page — mini app serve karo"""
    return await home_handler(request)

@routes.get(r"/stream_file/{msg_id:\d+}")
async def stream_file_handler(request: aio_web.Request):
    """
    Unlimited size file streaming — TechVJ style.
    Range requests support → browser mein video play hogi.
    """
    msg_id = int(request.match_info["msg_id"])

    # UID check — sirf premium users
    uid = request.rel_url.query.get("uid")
    if uid:
        try:
            uid_int = int(uid)
            if not await is_premium(uid_int) and uid_int not in ADMINS:
                return aio_web.json_response(
                    {"error": "Premium required for streaming"},
                    status=403
                )
        except: pass

    # File info get karo
    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)

    file_id = info["file_id"]
    file_size = info["file_size"]
    mime_type = info["mime_type"]
    file_name = info["file_name"]
    file_msg = info["msg"]

    if file_size == 0:
        return aio_web.json_response({"error": "File size unknown"}, status=404)

    # Range request parse
    range_header = request.headers.get("Range", "")
    from_bytes = 0
    until_bytes = file_size - 1

    if range_header:
        try:
            rng = range_header.replace("bytes=", "").split("-")
            from_bytes = int(rng[0]) if rng[0] else 0
            until_bytes = int(rng[1]) if rng[1] else file_size - 1
        except:
            pass

    if until_bytes >= file_size:
        until_bytes = file_size - 1

    req_length = until_bytes - from_bytes + 1
    chunk_size = 1024 * 1024  # 1MB chunks

    headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }

    status = 206 if range_header else 200
    response = aio_web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)

    # Userbot se stream karo — koi size limit nahi
    try:
        client_to_use = userbot if userbot else bot
        bytes_to_skip = from_bytes
        bytes_written = 0

        async for chunk in client_to_use.stream_media(file_msg):
            if bytes_to_skip > 0:
                if len(chunk) <= bytes_to_skip:
                    bytes_to_skip -= len(chunk)
                    continue
                else:
                    chunk = chunk[bytes_to_skip:]
                    bytes_to_skip = 0

            remaining = req_length - bytes_written
            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            await response.write(chunk)
            bytes_written += len(chunk)

            if bytes_written >= req_length:
                break

    except aio_web.ConnectionResetError:
        pass  # User ne close kiya
    except Exception as e:
        logger.error(f"stream_file_handler error: {e}")

    try:
        await response.write_eof()
    except: pass
    return response

@routes.get(r"/download/{msg_id:\d+}")
async def download_handler(request: aio_web.Request):
    """Download — attachment as file"""
    msg_id = int(request.match_info["msg_id"])
    info = await get_file_info(msg_id)
    if not info:
        return aio_web.json_response({"error": "File not found"}, status=404)

    # Same as stream but disposition = attachment
    request_copy = request
    file_msg = info["msg"]
    file_size = info["file_size"]
    file_name = info["file_name"]
    mime_type = info["mime_type"]
    req_length = file_size

    headers = {
        "Content-Type": mime_type,
        "Content-Length": str(req_length),
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }

    response = aio_web.StreamResponse(status=200, headers=headers)
    await response.prepare(request)

    try:
        client_to_use = userbot if userbot else bot
        async for chunk in client_to_use.stream_media(file_msg):
            await response.write(chunk)
    except aio_web.ConnectionResetError:
        pass
    except Exception as e:
        logger.error(f"download_handler error: {e}")

    try:
        await response.write_eof()
    except: pass
    return response

# JSON API routes
@routes.get("/api/plans")
async def api_plans(request):
    return aio_web.json_response({
        "plans": [
            {"id": "10days",  "name": "10 Din",  "days": 10,  "price": 50,  "desc": "Best Starter"},
            {"id": "30days",  "name": "30 Din",  "days": 30,  "price": 150, "desc": "Most Popular"},
            {"id": "60days",  "name": "60 Din",  "days": 60,  "price": 200, "desc": "Great Value"},
            {"id": "150days", "name": "150 Din", "days": 150, "price": 500, "desc": "Super Saver"},
            {"id": "365days", "name": "1 Saal",  "days": 365, "price": 800, "desc": "Best Deal"},
        ],
        "group_plans": [
            {"id": "group_1m", "name": "1 Mahina",  "days": 30, "price": 300, "desc": "Group Shortlink"},
            {"id": "group_2m", "name": "2 Mahine",  "days": 60, "price": 550, "desc": "Best Value"},
        ],
        "upi": UPI_ID,
        "qr_url": "https://envs.sh/GdE.jpg",
        "contact": "@asbhaibsr"
    })

@routes.get("/api/user_status/{user_id}")
async def api_user_status(request):
    try:
        user_id = int(request.match_info["user_id"])
        prem_doc = await premium_col.find_one({"user_id": user_id})
        trial_doc = await free_trial_col.find_one({"user_id": user_id})
        user_doc = await users_col.find_one({"user_id": user_id})
        pending = await payments_col.count_documents({"user_id": user_id, "status": "pending"})
        is_prem, is_trial, expiry_str = False, False, None
        if prem_doc and prem_doc.get("expiry"):
            exp = make_aware(prem_doc["expiry"])
            if now() < exp:
                is_prem = True
                is_trial = prem_doc.get("trial", False)
                expiry_str = exp.astimezone(IST).strftime("%d %b %Y %H:%M")
        return aio_web.json_response({
            "is_premium": is_prem, "is_trial": is_trial,
            "trial_used": bool(trial_doc), "expiry": expiry_str,
            "refer_count": user_doc.get("refer_count", 0) if user_doc else 0,
            "pending_payments": pending
        })
    except Exception as e:
        return aio_web.json_response({"is_premium": False, "error": str(e)})

@routes.get("/api/refer/{user_id}")
async def api_refer(request):
    try:
        user_id = int(request.match_info["user_id"])
        doc = await users_col.find_one({"user_id": user_id})
        refer_count = doc.get("refer_count", 0) if doc else 0
        return aio_web.json_response({
            "refer_count": refer_count,
            "refers_needed": max(0, 10 - (refer_count % 10))
        })
    except Exception as e:
        return aio_web.json_response({"refer_count": 0, "error": str(e)})

@routes.post("/api/submit_payment")
async def api_submit_payment(request):
    import base64, tempfile, os as _os
    try:
        data = await request.json()
    except:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id   = data.get("user_id")
    name      = data.get("name", "Unknown")
    plan_id   = data.get("plan_id")
    amount    = data.get("amount")
    txn_id    = data.get("txn_id", "").strip()
    screenshot = data.get("screenshot", "")
    group_id  = data.get("group_id")
    group_link = data.get("group_link", "")

    if not all([user_id, plan_id, txn_id]):
        return aio_web.json_response({"ok": False, "error": "Saari details bharo!"}, status=400)

    plan_days_map = {"10days":10,"30days":30,"60days":60,"150days":150,"365days":365,"group_1m":30,"group_2m":60}
    days = plan_days_map.get(plan_id, 30)

    existing = await payments_col.find_one({"txn_id": txn_id})
    if existing:
        return aio_web.json_response({"ok": False, "error": "Transaction ID already submitted!"}, status=400)

    is_group_plan = str(plan_id).startswith("group_")
    pay_type = "🏘 GROUP PREMIUM" if is_group_plan else "💎 USER PREMIUM"
    group_info = f"\n🏘 Group: {group_id}\n🔗 {group_link}" if is_group_plan and group_id else ""

    pay_doc = {
        "user_id": user_id, "name": name, "plan_id": plan_id,
        "days": days, "amount": amount, "txn_id": txn_id,
        "screenshot": screenshot[:200] if screenshot else "",
        "status": "pending", "submitted_at": now(),
        "is_group": is_group_plan,
        "group_id": str(group_id) if group_id else None,
        "group_link": group_link or ""
    }
    result = await payments_col.insert_one(pay_doc)
    pay_id = str(result.inserted_id)

    msg_text = (
        f"💰 **Naya Payment — {pay_type}**\n\n"
        f"👤 {name} (`{user_id}`)\n"
        f"📦 Plan: **{plan_id}** ({days} din)\n"
        f"💵 Amount: ₹{amount}\n"
        f"🔖 TXN ID: `{txn_id}`"
        f"{group_info}\n"
        f"🕐 {now_ist().strftime('%d %b %H:%M')} IST"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"pay_approve_{pay_id}_{user_id}_{days}"),
            InlineKeyboardButton("❌ Reject",  callback_data=f"pay_reject_{pay_id}_{user_id}")
        ]
    ])

    if screenshot and screenshot.startswith("data:image"):
        try:
            img_data = base64.b64decode(screenshot.split(",")[1])
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tf:
                tf.write(img_data); tf_path = tf.name
            await bot.send_photo(LOG_CHANNEL, tf_path, caption=msg_text, reply_markup=kb)
            await bot.send_photo(OWNER_ID, tf_path, caption=msg_text, reply_markup=kb)
            _os.unlink(tf_path)
        except:
            await bot.send_message(LOG_CHANNEL, msg_text, reply_markup=kb)
            try: await bot.send_message(OWNER_ID, msg_text, reply_markup=kb)
            except: pass
    else:
        await bot.send_message(LOG_CHANNEL, msg_text, reply_markup=kb)
        try: await bot.send_message(OWNER_ID, msg_text, reply_markup=kb)
        except: pass

    return aio_web.json_response({"ok": True, "message": "✅ Request submit ho gayi! 1-2 ghante mein activate hoga."})

@routes.post("/api/claim_trial")
async def api_claim_trial(request):
    try:
        data = await request.json()
    except:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id = data.get("user_id")
    if not user_id:
        return aio_web.json_response({"ok": False, "error": "User ID missing"}, status=400)

    doc = await free_trial_col.find_one({"user_id": user_id})
    if doc:
        return aio_web.json_response({"ok": False, "message": "Free trial pehle le chuke ho! Premium lo."})

    await free_trial_col.insert_one({"user_id": user_id, "claimed_at": now()})
    expiry = now() + timedelta(minutes=5)
    await premium_col.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "expiry": expiry, "trial": True, "added": now()}},
        upsert=True
    )
    try:
        await bot.send_message(
            user_id,
            "🆓 **Free Trial Shuru!**\n\n**5 minute** ke liye Premium active!\n"
            "Group mein movie search karo abhi!\n\n"
            "⏰ 5 min baad band hoga.\n💎 Continue ke liye Premium lo!"
        )
    except: pass
    await send_log(f"🆓 Free Trial\n👤 `{user_id}`")
    return aio_web.json_response({"ok": True, "message": "Trial shuru! 5 min ke liye premium active."})

@routes.post("/api/help")
async def api_help(request):
    try:
        data = await request.json()
    except:
        return aio_web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    user_id = data.get("user_id")
    name = data.get("name", "Unknown")
    msg_text = data.get("message", "")
    if not msg_text:
        return aio_web.json_response({"ok": False, "error": "No message"}, status=400)

    await help_msgs_col.insert_one({"user_id": user_id, "name": name, "message": msg_text, "time": now()})
    await send_log(f"📩 **Mini App Help**\n\n👤 {name} (`{user_id}`)\n\n💬 {msg_text}")
    return aio_web.json_response({"ok": True})

# Add all routes
aio_app.add_routes(routes)

def run_async(coro):
    """Flask routes se async code run karo — bot ke event loop mein"""
    try:
        loop = bot.loop
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=30)
        else:
            # Fallback
            return asyncio.run(coro)
    except Exception as e:
        logger.error(f"run_async error: {e}")
        raise

# ═══════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════

def run_async(coro):
    """Flask routes se async code run karo — bot ke event loop mein"""
    import asyncio
    try:
        loop = bot.loop
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=30)
        else:
            return asyncio.run(coro)
    except Exception as e:
        logger.error(f"run_async error: {e}")
        raise


async def run_aiohttp_server():
    """aiohttp server start karo — unlimited file streaming"""
    aio_app.router.add_routes(routes)
    runner = aio_web.AppRunner(aio_app)
    await runner.setup()
    site = aio_web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"✅ aiohttp server started on port {PORT}")
