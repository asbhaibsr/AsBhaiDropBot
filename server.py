import os, asyncio, logging
from flask import Flask, Response, render_template_string, abort, stream_with_context, request
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID       = int(os.environ.get("API_ID", "0"))
API_HASH     = os.environ.get("API_HASH", "")
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
FILE_CHANNEL = int(os.environ.get("FILE_CHANNEL", "-1002283182645"))

app_flask = Flask(__name__)

STREAM_PAGE = """<!DOCTYPE html>
<html lang="hi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }} - AsBhai Vault</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI',sans-serif;background:#0a0a0f;color:#e0e0e0;min-height:100vh}
        header{background:linear-gradient(135deg,#1a1a2e,#0f3460);padding:14px 20px;display:flex;align-items:center;gap:12px;border-bottom:2px solid #e94560}
        header h1{font-size:1.1rem;color:#e94560}
        header p{font-size:.75rem;color:#aaa}
        .wrap{max-width:900px;margin:28px auto;padding:0 16px}
        .box{background:#111;border-radius:14px;overflow:hidden;box-shadow:0 8px 40px rgba(233,69,96,.2)}
        video{width:100%;max-height:72vh;background:#000;display:block}
        .info{padding:18px;background:linear-gradient(180deg,#1a1a2e,#111)}
        .ftitle{font-size:1.1rem;font-weight:600;color:#e94560;margin-bottom:6px}
        .fmeta{font-size:.82rem;color:#777}
        .btns{display:flex;gap:10px;margin-top:16px;flex-wrap:wrap}
        .btn{padding:10px 22px;border-radius:8px;border:none;cursor:pointer;font-size:.9rem;text-decoration:none;font-weight:600;display:inline-block;transition:all .2s}
        .p{background:#e94560;color:#fff}
        .s{background:#0f3460;color:#e0e0e0;border:1px solid #e94560}
        .btn:hover{opacity:.85;transform:translateY(-1px)}
        footer{text-align:center;padding:18px;font-size:.78rem;color:#444}
        footer a{color:#e94560;text-decoration:none}
    </style>
</head>
<body>
    <header>
        <div style="font-size:1.5rem">🗂</div>
        <div><h1>AsBhai Vault Stream</h1><p>@asbhaibsr | @asbhai_bsr</p></div>
    </header>
    <div class="wrap">
        <div class="box">
            <video controls autoplay playsinline preload="metadata"
                src="/stream/{{ channel_id }}/{{ msg_id }}/raw">
                Browser video support nahi karta.
            </video>
            <div class="info">
                <div class="ftitle">🗂 {{ title }}</div>
                <div class="fmeta">Powered by AsBhai Vault Bot</div>
                <div class="btns">
                    <a class="btn p" href="/download/{{ channel_id }}/{{ msg_id }}">📥 Download</a>
                    <a class="btn s" href="https://t.me/asbhai_bsr" target="_blank">📢 Channel</a>
                </div>
            </div>
        </div>
    </div>
    <footer>Powered by <a href="https://t.me/asbhaibsr">@asbhaibsr</a> | <a href="https://t.me/asbhai_bsr">@asbhai_bsr</a></footer>
</body>
</html>"""

# Pyrogram client — lazily started
_pyro = None

def get_pyro():
    global _pyro
    if _pyro is None:
        from pyrogram import Client
        _pyro = Client(
            name="stream_worker",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            no_updates=True,
            in_memory=True
        )
    return _pyro

_loop = None

def get_loop():
    global _loop
    if _loop is None or _loop.is_closed():
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
    return _loop

def run(coro):
    loop = get_loop()
    if not get_pyro().is_connected:
        loop.run_until_complete(get_pyro().start())
    return loop.run_until_complete(coro)

async def get_file_info(channel_id, msg_id):
    try:
        msg = await get_pyro().get_messages(channel_id, msg_id)
        if not msg or not msg.document:
            return None
        return {
            "name": msg.document.file_name or f"file_{msg_id}",
            "size": msg.document.file_size or 0,
            "mime": msg.document.mime_type or "application/octet-stream",
            "msg": msg
        }
    except Exception as e:
        logger.error(f"get_file_info: {e}")
        return None

def stream_gen(channel_id, msg_id, offset=0):
    async def _gen():
        msg = await get_pyro().get_messages(channel_id, msg_id)
        async for chunk in get_pyro().stream_media(msg, offset=offset):
            yield chunk
    gen = _gen()
    loop = get_loop()
    while True:
        try:
            yield loop.run_until_complete(gen.__anext__())
        except StopAsyncIteration:
            break
        except Exception as e:
            logger.error(f"stream_gen: {e}")
            break

# ── routes ───────────────────────────────────────────

@app_flask.route("/")
def home():
    return "<h2 style='font-family:sans-serif;color:#e94560;padding:40px'>🗂 AsBhai Vault Stream Server ✅</h2>"

@app_flask.route("/health")
def health():
    return {"status": "ok"}, 200

@app_flask.route("/stream/<int:channel_id>/<int:msg_id>")
def stream_page(channel_id, msg_id):
    info = run(get_file_info(channel_id, msg_id))
    if not info: abort(404)
    return render_template_string(STREAM_PAGE, title=info["name"], channel_id=channel_id, msg_id=msg_id)

@app_flask.route("/stream/<int:channel_id>/<int:msg_id>/raw")
def stream_raw(channel_id, msg_id):
    info = run(get_file_info(channel_id, msg_id))
    if not info: abort(404)
    file_size = info["size"]
    mime = info["mime"]
    range_header = request.headers.get("Range")
    offset = 0
    status = 200
    headers = {"Content-Type": mime, "Accept-Ranges": "bytes",
               "Content-Disposition": f'inline; filename="{info["name"]}"'}
    if range_header:
        try:
            rv = range_header.replace("bytes=", "")
            start, end = rv.split("-")
            offset = int(start) if start else 0
            end = int(end) if end else file_size - 1
            headers["Content-Range"] = f"bytes {offset}-{end}/{file_size}"
            headers["Content-Length"] = str(end - offset + 1)
            status = 206
        except: pass
    else:
        headers["Content-Length"] = str(file_size)
    return Response(stream_with_context(stream_gen(channel_id, msg_id, offset)), status=status, headers=headers, direct_passthrough=True)

@app_flask.route("/download/<int:channel_id>/<int:msg_id>")
def download_file(channel_id, msg_id):
    info = run(get_file_info(channel_id, msg_id))
    if not info: abort(404)
    headers = {
        "Content-Type": info["mime"],
        "Content-Disposition": f'attachment; filename="{info["name"]}"',
        "Content-Length": str(info["size"])
    }
    return Response(stream_with_context(stream_gen(channel_id, msg_id)), status=200, headers=headers, direct_passthrough=True)

if __name__ == "__main__":
    logger.info("🌐 AsBhai Vault Stream Server starting on port 8080...")
    app_flask.run(host="0.0.0.0", port=8080, debug=False, threaded=True)
