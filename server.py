# server.py

import os, asyncio, logging
from flask import Flask, Response, render_template_string, abort, redirect, stream_with_context
from pyrogram import Client
from pyrogram.errors import FloodWait
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", "-1002463804038"))

app_flask = Flask(__name__)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

pyro_client = Client(
    "stream_worker",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    no_updates=True
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#    STREAM PAGE TEMPLATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STREAM_PAGE = """
<!DOCTYPE html>
<html lang="hi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎬 {{ title }} - AsBhai Stream</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', sans-serif;
            background: #0a0a0f;
            color: #e0e0e0;
            min-height: 100vh;
        }
        header {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            padding: 15px 20px;
            display: flex;
            align-items: center;
            gap: 12px;
            border-bottom: 1px solid #e94560;
        }
        header .logo { font-size: 1.5rem; }
        header h1 { font-size: 1.1rem; color: #e94560; }
        header p { font-size: 0.75rem; color: #aaa; }
        .container { max-width: 900px; margin: 30px auto; padding: 0 16px; }
        .video-box {
            background: #111;
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 8px 40px rgba(233,69,96,0.2);
        }
        video {
            width: 100%;
            max-height: 70vh;
            background: #000;
            display: block;
        }
        .info {
            padding: 20px;
            background: linear-gradient(180deg, #1a1a2e, #111);
        }
        .file-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: #e94560;
            margin-bottom: 8px;
        }
        .file-meta { font-size: 0.85rem; color: #888; }
        .btn-row {
            display: flex;
            gap: 12px;
            margin-top: 18px;
            flex-wrap: wrap;
        }
        .btn {
            padding: 10px 22px;
            border-radius: 8px;
            border: none;
            cursor: pointer;
            font-size: 0.9rem;
            text-decoration: none;
            font-weight: 600;
            display: inline-block;
            transition: all 0.2s;
        }
        .btn-primary { background: #e94560; color: #fff; }
        .btn-secondary { background: #0f3460; color: #e0e0e0; border: 1px solid #e94560; }
        .btn:hover { opacity: 0.85; transform: translateY(-1px); }
        .watermark {
            text-align: center;
            padding: 20px;
            font-size: 0.8rem;
            color: #444;
        }
        .watermark a { color: #e94560; text-decoration: none; }
    </style>
</head>
<body>
    <header>
        <div class="logo">🎬</div>
        <div>
            <h1>AsBhai Stream</h1>
            <p>@asbhaibsr | @asbhai_bsr</p>
        </div>
    </header>
    <div class="container">
        <div class="video-box">
            <video
                controls
                autoplay
                playsinline
                preload="metadata"
                src="/stream/{{ channel_id }}/{{ msg_id }}/raw"
            >
                Your browser does not support video playback.
            </video>
            <div class="info">
                <div class="file-title">🎬 {{ title }}</div>
                <div class="file-meta">Powered by AsBhai Bot</div>
                <div class="btn-row">
                    <a class="btn btn-primary" href="/download/{{ channel_id }}/{{ msg_id }}">
                        📥 Download
                    </a>
                    <a class="btn btn-secondary" href="https://t.me/asbhai_bsr" target="_blank">
                        📢 Channel Join Karo
                    </a>
                </div>
            </div>
        </div>
    </div>
    <div class="watermark">
        Powered by <a href="https://t.me/asbhaibsr">@asbhaibsr</a> |
        <a href="https://t.me/asbhai_bsr">@asbhai_bsr</a>
    </div>
</body>
</html>
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#    ASYNC HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_async(coro):
    return loop.run_until_complete(coro)

async def get_file_info(channel_id: int, msg_id: int):
    try:
        msg = await pyro_client.get_messages(channel_id, msg_id)
        if not msg or not msg.document:
            return None
        return {
            "name": msg.document.file_name or f"file_{msg_id}",
            "size": msg.document.file_size,
            "mime": msg.document.mime_type or "application/octet-stream",
            "msg": msg
        }
    except Exception as e:
        logger.error(f"get_file_info: {e}")
        return None

async def stream_generator(channel_id: int, msg_id: int, offset: int = 0, chunk_size: int = 1024 * 1024):
    try:
        msg = await pyro_client.get_messages(channel_id, msg_id)
        if not msg or not msg.document:
            return
        async for chunk in pyro_client.stream_media(msg, offset=offset, chunk_size=chunk_size):
            yield chunk
    except FloodWait as e:
        logger.warning(f"FloodWait: {e.value}s")
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f"stream_generator: {e}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#    ROUTES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app_flask.route("/")
def home():
    return """
    <html><body style='font-family:sans-serif;background:#0a0a0f;color:#e0e0e0;
    display:flex;align-items:center;justify-content:center;min-height:100vh;'>
    <div style='text-align:center'>
    <h1 style='color:#e94560'>🎬 AsBhai Stream Server</h1>
    <p>Bot: <a href='https://t.me/asbhai_bsr' style='color:#e94560'>@asbhai_bsr</a></p>
    <p style='color:#888;margin-top:10px'>Server is running ✅</p>
    </div></body></html>
    """

@app_flask.route("/health")
def health():
    return {"status": "ok", "bot": "AsBhai Movie Bot"}, 200

@app_flask.route("/stream/<int:channel_id>/<int:msg_id>")
def stream_page(channel_id, msg_id):
    info = run_async(get_file_info(channel_id, msg_id))
    if not info:
        abort(404)
    return render_template_string(
        STREAM_PAGE,
        title=info["name"],
        channel_id=channel_id,
        msg_id=msg_id
    )

@app_flask.route("/stream/<int:channel_id>/<int:msg_id>/raw")
def stream_raw(channel_id, msg_id):
    from flask import request
    info = run_async(get_file_info(channel_id, msg_id))
    if not info:
        abort(404)
    
    file_size = info["size"]
    mime_type = info["mime"]
    
    range_header = request.headers.get("Range", None)
    offset = 0
    status = 200
    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'inline; filename="{info["name"]}"',
    }
    
    if range_header:
        try:
            range_val = range_header.replace("bytes=", "")
            start, end = range_val.split("-")
            offset = int(start) if start else 0
            end = int(end) if end else file_size - 1
            length = end - offset + 1
            headers["Content-Range"] = f"bytes {offset}-{end}/{file_size}"
            headers["Content-Length"] = str(length)
            status = 206
        except Exception:
            pass
    else:
        headers["Content-Length"] = str(file_size)
    
    def generate():
        gen = stream_generator(channel_id, msg_id, offset=offset)
        while True:
            try:
                chunk = loop.run_until_complete(gen.__anext__())
                yield chunk
            except StopAsyncIteration:
                break
            except Exception as e:
                logger.error(f"Stream error: {e}")
                break
    
    return Response(
        stream_with_context(generate()),
        status=status,
        headers=headers,
        direct_passthrough=True
    )

@app_flask.route("/download/<int:channel_id>/<int:msg_id>")
def download_file(channel_id, msg_id):
    from flask import request
    info = run_async(get_file_info(channel_id, msg_id))
    if not info:
        abort(404)
    
    file_size = info["size"]
    mime_type = info["mime"]
    
    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f'attachment; filename="{info["name"]}"',
        "Content-Length": str(file_size),
        "Accept-Ranges": "bytes",
    }
    
    def generate():
        gen = stream_generator(channel_id, msg_id, offset=0)
        while True:
            try:
                chunk = loop.run_until_complete(gen.__anext__())
                yield chunk
            except StopAsyncIteration:
                break
            except Exception as e:
                logger.error(f"Download error: {e}")
                break
    
    return Response(
        stream_with_context(generate()),
        status=200,
        headers=headers,
        direct_passthrough=True
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#    STARTUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def start_server():
    logger.info("Starting Pyrogram client for streaming...")
    loop.run_until_complete(pyro_client.start())
    logger.info("✅ Pyrogram stream client ready")
    logger.info("🌐 Starting Flask server on port 8080...")
    app_flask.run(host="0.0.0.0", port=8080, debug=False, threaded=True)

if __name__ == "__main__":
    start_server()
