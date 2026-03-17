# 🎬 AsBhai Movie Bot

**Owner:** @asbhaibsr | **Channel:** @asbhai_bsr

---

## 📁 Files
- `bot.py` — Main Telegram Bot
- `server.py` — Flask Streaming/Download Server
- `.env.example` — Config template
- `requirements.txt` — Dependencies

---

## ⚙️ Setup

### 1. .env file banao
```
cp .env.example .env
```
`.env` mein apni values bharo:
- `API_ID` / `API_HASH` → https://my.telegram.org
- `BOT_TOKEN` → @BotFather se
- `MONGO_URI` → MongoDB Atlas free tier
- `SHORTLINK_API` → Apna shortlink API key

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run karo
```bash
# Bot aur server dono ek saath:
python server.py &
python bot.py
```

---

## 🌐 Koyeb Deploy

1. GitHub par repo push karo
2. Koyeb.com par new service banao
3. Environment variables mein sab .env values dalo
4. Port: **8080**
5. Health check path: `/health`
6. Build command: `pip install -r requirements.txt`
7. Run command: `python server.py & python bot.py`

---

## 🤖 Bot Commands

### Owner Commands (PM mein):
| Command | Description |
|---------|-------------|
| `/addpremium <uid> [days]` | Premium do (default 30 din) |
| `/removepremium <uid>` | Premium hatao |
| `/setdelete <min>` | Auto-delete time set karo |
| `/setdelete on/off` | Auto-delete toggle |
| `/forcesub on/off` | Force subscribe toggle |
| `/shortlink on/off` | Shortlink toggle |
| `/setlimit <n>` | Daily download limit |
| `/setresults <free> <premium>` | Result count |
| `/broadcast users/groups/all <msg>` | Broadcast bhejo |
| `/stats` | Statistics dekho |
| `/settings` | Sab settings dekho |
| `/ping` | Bot speed check |

### User Commands:
| Command | Description |
|---------|-------------|
| `/start` | Bot shuru karo |
| `/premium` | Premium info |
| `/help` | Help |

---

## 💎 Premium System
- Price: ₹250 / 30 din
- UPI: `arsadsaifi8272@ibl`
- Benefits: No shortlink, 5 results/search, unlimited downloads, streaming

---

## 📊 Features
- ✅ Smart search (multi-keyword, fuzzy)
- ✅ Auto-delete (owner configurable)
- ✅ Force subscribe
- ✅ Shortlink protection
- ✅ Daily limit (free users)
- ✅ Premium system
- ✅ Online streaming & download
- ✅ Broadcast with auto-cleanup
- ✅ Inline mode
- ✅ MongoDB stats
- ✅ Group welcome messages
- ✅ Caption/link cleaning

---

*Made with ❤️ by @asbhaibsr*
