import os, time, json, sqlite3, subprocess, tempfile, shutil, re, sys, importlib


def ensure_deps():
    pkgs = ["requests", "yt_dlp"]
    missing = [p for p in pkgs if importlib.util.find_spec(p) is None]
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    if shutil.which("ffmpeg") is None:
        try:
            subprocess.check_call(["apt-get", "update"])
            subprocess.check_call(["apt-get", "install", "-y", "--no-install-recommends", "ffmpeg"])
        except Exception:
            pass


ensure_deps()

import requests
from yt_dlp import YoutubeDL

# ===== ENV =====
BOT_TOKEN    = os.environ["BOT_TOKEN"]
LOG_CHAT     = os.environ.get("LOG_CHAT", "")     # чат/группа для логов (устаревший)
OWNER_ID     = os.environ.get("OWNER_ID", "")     # ID владельца бота для логов
POLL_TIMEOUT = int(os.environ.get("POLL_TIMEOUT", "25"))
DEBUG        = int(os.environ.get("DEBUG", "1"))
RAW_UPDATES  = os.environ.get("RAW_UPDATES", "updates.ndjson")
MEDIA_CACHE_DIR = os.environ.get("MEDIA_CACHE_DIR", "media_cache")
CACHE_TTL_DAYS  = int(os.environ.get("CACHE_TTL_DAYS", "7"))

# ===== Auto owner detection =====
OWNER_FILE = "owner_id.txt"

def get_owner_id():
    """Получает ID владельца из переменной окружения или файла"""
    if OWNER_ID:
        return OWNER_ID
    try:
        with open(OWNER_FILE, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""

def save_owner_id(user_id: str):
    """Сохраняет ID владельца в файл"""
    try:
        with open(OWNER_FILE, "w") as f:
            f.write(str(user_id))
        d("[owner saved]", {"user_id": user_id})
    except Exception as e:
        d("[owner save error]", str(e))

API      = f"https://api.telegram.org/bot{BOT_TOKEN}"
FILE_API = f"https://api.telegram.org/file/bot{BOT_TOKEN}"

os.makedirs(MEDIA_CACHE_DIR, exist_ok=True)

# ===== DB (текст + медиа) =====
db = sqlite3.connect("messages.sqlite3", check_same_thread=False)
db.execute("""
CREATE TABLE IF NOT EXISTS biz_messages(
  bcid       TEXT,      -- '' для обычных, business_connection_id для бизнес
  chat_id    INTEGER,
  msg_id     INTEGER,
  date       INTEGER,
  text       TEXT,
  media_type TEXT,      -- photo|video|document|voice|audio|animation|video_note
  file_id    TEXT,      -- file_id для повторной отправки в лог
  PRIMARY KEY (bcid, chat_id, msg_id)
)
""")
cols = {r[1] for r in db.execute("PRAGMA table_info(biz_messages)").fetchall()}
if "media_type" not in cols:
    db.execute("ALTER TABLE biz_messages ADD COLUMN media_type TEXT")
if "file_id" not in cols:
    db.execute("ALTER TABLE biz_messages ADD COLUMN file_id TEXT")
db.commit()

db.execute("CREATE INDEX IF NOT EXISTS idx_biz_chat_msg ON biz_messages(chat_id, msg_id)")
db.execute("CREATE INDEX IF NOT EXISTS idx_biz_media_type ON biz_messages(media_type)")
db.commit()

# ===== debug/log helpers =====
def _ts() -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    except Exception:
        return ""

def d(msg: str, obj=None):
    if not DEBUG:
        return
    try:
        if obj is None:
            print(f"[{_ts()}] {msg}")
        else:
            try:
                j = json.dumps(obj, ensure_ascii=False)
            except Exception:
                j = str(obj)
            print(f"[{_ts()}] {msg}: {j}")
    except Exception:
        pass

def log_line(path: str, line: str):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print("[log_line error]", repr(e))

def log_json(path: str, obj):
    try:
        j = json.dumps(obj, ensure_ascii=False)
    except Exception:
        j = str(obj)
    log_line(path, j)

# ===== helpers =====
def html_escape(s: str) -> str:
    s = s or ""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def msg_text(m) -> str:
    return (m.get("text") or m.get("caption") or "").strip()

def parse_media(m):
    if "photo" in m and isinstance(m["photo"], list) and m["photo"]:
        ph = max(m["photo"], key=lambda x: x.get("file_size", 0))
        return "photo", ph["file_id"]
    if "video"      in m: return "video",      m["video"]["file_id"]
    if "document"   in m: return "document",   m["document"]["file_id"]
    if "voice"      in m: return "voice",      m["voice"]["file_id"]
    if "audio"      in m: return "audio",      m["audio"]["file_id"]
    if "animation"  in m: return "animation",  m["animation"]["file_id"]
    if "video_note" in m: return "video_note", m["video_note"]["file_id"]
    return None, None

def tg_call(method, **params):
    d("[tg_call start]", {"method": method, "keys": list(params.keys())})
    r = requests.post(f"{API}/{method}", data=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        d("[tg_call fail]", data)
        raise RuntimeError(f"{method} error: {data}")
    d("[tg_call ok]", {"method": method})
    return data["result"]

def tg_upload(method: str, file_field: str, file_path: str, **params):
    d("[tg_upload start]", {"method": method, "file_field": file_field, "file": os.path.basename(file_path)})
    with open(file_path, "rb") as f:
        files = {file_field: (os.path.basename(file_path), f)}
        r = requests.post(f"{API}/{method}", data=params, files=files, timeout=600)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        d("[tg_upload fail]", data)
        raise RuntimeError(f"{method} error: {data}")
    d("[tg_upload ok]", {"method": method})
    return data["result"]

def send_log_html(html: str):
    # Приоритет: сначала владельцу бота, потом в группу (если указана)
    target_chat = get_owner_id() or LOG_CHAT
    if not target_chat:
        print("LOG:", html); return
    try:
        tg_call("sendMessage", chat_id=target_chat, text=html, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        print("send_log error:", e)

def store(bcid, chat_id, msg_id, text, media_type=None, file_id=None):
    db.execute(
        "INSERT OR REPLACE INTO biz_messages(bcid,chat_id,msg_id,date,text,media_type,file_id) VALUES(?,?,?,?,?,?,?)",
        (bcid, chat_id, msg_id, int(time.time()), text or "", media_type, file_id)
    )
    db.commit()
    d("[store]", {"bcid": bcid, "chat": chat_id, "msg": msg_id, "has_text": bool(text), "media": media_type})

def fetch(bcid, chat_id, msg_id):
    # Сначала ищем по точному совпадению bcid + chat_id + msg_id
    row = db.execute(
        "SELECT text, media_type, file_id FROM biz_messages WHERE bcid=? AND chat_id=? AND msg_id=?",
        (bcid, chat_id, msg_id)
    ).fetchone()
    if row:
        d("[fetch hit bcid]")
        return row
    
    # Затем ищем по chat_id + msg_id (без bcid)
    row = db.execute(
        "SELECT text, media_type, file_id FROM biz_messages WHERE chat_id=? AND msg_id=? ORDER BY date DESC LIMIT 1",
        (chat_id, msg_id)
    ).fetchone()
    if row:
        d("[fetch hit generic]")
        return row
    
    # Если не нашли, попробуем найти ближайшие сообщения в том же чате
    if bcid:
        # Для бизнес-сообщений ищем в диапазоне ±10 от указанного msg_id
        row = db.execute(
            "SELECT text, media_type, file_id FROM biz_messages WHERE bcid=? AND chat_id=? AND msg_id BETWEEN ? AND ? ORDER BY ABS(msg_id - ?) ASC LIMIT 1",
            (bcid, chat_id, msg_id - 10, msg_id + 10, msg_id)
        ).fetchone()
        if row:
            d("[fetch hit range bcid]", {"target": msg_id, "found": "nearby"})
            return row
    
    # Ищем ближайшие сообщения без bcid
    row = db.execute(
        "SELECT text, media_type, file_id FROM biz_messages WHERE chat_id=? AND msg_id BETWEEN ? AND ? ORDER BY ABS(msg_id - ?) ASC LIMIT 1",
        (chat_id, msg_id - 10, msg_id + 10, msg_id)
    ).fetchone()
    if row:
        d("[fetch hit range generic]", {"target": msg_id, "found": "nearby"})
        return row
    
    d("[fetch miss]", {"bcid": bcid, "chat": chat_id, "msg": msg_id})
    return None, None, None

def build_chat_name(chat: dict | None) -> str | None:
    if not chat:
        return None
    ctype = chat.get("type")
    if ctype == "private":
        first = chat.get("first_name") or ""
        last  = chat.get("last_name") or ""
        fullname = (first + (" " + last if last else "")).strip()
        return fullname or chat.get("username")
    return chat.get("title")

def actor_link(actor: dict | None, fallback_user_id: int | None, fallback_name: str | None = None) -> str:
    uid = None
    name = None
    if actor:
        uid  = actor.get("id")
        first = actor.get("first_name") or ""
        last  = actor.get("last_name") or ""
        fullname = (first + (" " + last if last else "")).strip()
        name = fullname or actor.get("username")
    if not uid:
        uid = fallback_user_id
    if not name:
        name = fallback_name or (str(uid) if uid else "неизвестно")
    name = html_escape(name)
    return f'<a href="tg://user?id={uid}">{name}</a>' if uid else name

# ===== media cache helpers =====
def _cache_dir_for_chat(chat_id: int) -> str:
    p = os.path.join(MEDIA_CACHE_DIR, str(chat_id))
    os.makedirs(p, exist_ok=True)
    return p

def _cache_meta_path(chat_id: int, msg_id: int) -> str:
    return os.path.join(_cache_dir_for_chat(chat_id), f"{msg_id}.json")

def _cache_file_path(chat_id: int, msg_id: int, src_filename: str) -> str:
    base, ext = os.path.splitext(src_filename)
    if not ext:
        ext = ".bin"
    return os.path.join(_cache_dir_for_chat(chat_id), f"{msg_id}{ext}")

def cache_media_from_message(chat_id: int, msg: dict):
    mtype, fid = parse_media(msg)
    if not (mtype and fid):
        return
    try:
        url, fname = get_file_path(fid)
        tmp = download_file(url, fname)
        dst = _cache_file_path(chat_id, msg.get("message_id") or 0, fname)
        shutil.copyfile(tmp, dst)
        meta = {"media_type": mtype, "file": os.path.basename(dst), "ts": int(time.time())}
        with open(_cache_meta_path(chat_id, msg.get("message_id") or 0), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False)
        d("[cache saved]", {"chat": chat_id, "msg": msg.get("message_id"), "mtype": mtype, "file": dst})
    except Exception as e:
        d("[cache error]", str(e))

# ===== ffmpeg helpers =====
def run_ffmpeg(args: list) -> None:
    d("[ffmpeg]", {"args": args})
    p = subprocess.run(["ffmpeg", "-y"] + args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + (p.stdout or ""))

def make_video_note_square(src_path: str) -> str:
    dst = os.path.join(os.path.dirname(src_path), "circle_640.mp4")
    vf = "scale='if(gt(iw,ih),-2,640)':'if(gt(iw,ih),640,-2)',crop=640:640"
    run_ffmpeg([
        "-i", src_path,
        "-vf", vf,
        "-r", "30",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-profile:v", "baseline",
        "-level:v", "3.1",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-b:a", "96k",
        dst
    ])
    return dst

def make_muted_copy(src_path: str) -> str:
    """Создаёт копию mp4 без аудиодорожки (быстро, без перекодирования видео)."""
    base, ext = os.path.splitext(src_path)
    dst = base + "_muted.mp4"
    run_ffmpeg([
        "-i", src_path,
        "-c:v", "copy",
        "-an",
        dst
    ])
    return dst

def extract_voice_ogg(src_path: str) -> str:
    dst = os.path.join(os.path.dirname(src_path), "voice.ogg")
    run_ffmpeg(["-i", src_path, "-vn", "-c:a", "libopus", "-b:a", "64k", "-ar", "48000", "-ac", "1", dst])
    return dst

def ensure_local_video_from_message(m: dict) -> str | None:
    if "video" in m:
        url, fname = get_file_path(m["video"]["file_id"])
        return download_file(url, fname)
    if "animation" in m:
        url, fname = get_file_path(m["animation"]["file_id"])
        return download_file(url, fname)
    if "document" in m:
        mime = (m["document"].get("mime_type") or "")
        if mime.startswith("video/") or m["document"].get("file_name","").lower().endswith((".mp4",".mov",".mkv",".webm",".m4v")):
            url, fname = get_file_path(m["document"]["file_id"])
            return download_file(url, fname)
    return None

# ===== URL helpers (yt-dlp) =====
def find_urls(text: str) -> list[str]:
    if not text:
        return []
    rx = r'(https?://\S+)'
    return re.findall(rx, text)

def download_video_from_url(url: str) -> str | None:
    tmp = tempfile.mkdtemp(prefix="dlb_url_")
    outtmpl = os.path.join(tmp, "video.%(ext)s")
    ydl_opts = {
        "outtmpl": outtmpl,
        "format": "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b",
        "merge_output_format": "mp4",
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 3,
        "geo_bypass": True,
    }
    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            filepath = ydl.prepare_filename(info)
            base, ext = os.path.splitext(filepath)
            if not ext.lower().endswith(".mp4"):
                merged = base + ".mp4"
                if os.path.exists(merged):
                    filepath = merged
            if os.path.exists(filepath):
                return filepath
    except Exception as e:
        d("[yt-dlp error]", str(e))
    return None

# ===== UI helpers (inline keyboard) =====
def send_media_actions_kb(chat_id: int, reply_to_message_id: int):
    kb = {
        "inline_keyboard": [[
            {"text": "🎯 Кружок", "callback_data": f"c:{chat_id}:{reply_to_message_id}"},
            {"text": "🎵 Аудио",  "callback_data": f"v:{chat_id}:{reply_to_message_id}"},
        ]]
    }
    tg_call("sendMessage", chat_id=chat_id, reply_to_message_id=reply_to_message_id,
            text="Выбери действие:", reply_markup=json.dumps(kb))

# ===== Telegram file helpers =====
def get_file_path(file_id: str) -> tuple[str, str]:
    info = tg_call("getFile", file_id=file_id)
    path = info.get("file_path")
    if not path:
        raise RuntimeError("No file_path from getFile")
    return f"{FILE_API}/{path}", os.path.basename(path)

def download_file(url: str, fname: str) -> str:
    tmp = tempfile.mkdtemp(prefix="dlb_")
    local = os.path.join(tmp, fname)
    d("[download]", {"url": url, "to": local})
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(local, "wb") as f:
            shutil.copyfileobj(r.raw, f)
    return local

# ===== cached sending =====
def send_cached_file_to_log(media_type: str, local_path: str, caption_html: str):
    target_chat = get_owner_id() or LOG_CHAT
    if not target_chat:
        return
    try:
        if media_type == "video_note":
            # 1) подпись отдельным сообщением
            tg_call("sendMessage", chat_id=target_chat, text=caption_html, parse_mode="HTML", disable_web_page_preview=True)
            # 2) заглушаем звук и отправляем кружок без подписи
            muted = make_muted_copy(local_path)
            tg_upload("sendVideoNote", "video_note", muted, chat_id=target_chat, length=640)
            return

        if   media_type == "photo":     tg_upload("sendPhoto",     "photo",     local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        elif media_type == "video":     tg_upload("sendVideo",     "video",     local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        elif media_type == "animation": tg_upload("sendAnimation", "animation", local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        elif media_type == "document":  tg_upload("sendDocument",  "document",  local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        elif media_type == "voice":     tg_upload("sendVoice",     "voice",     local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        elif media_type == "audio":     tg_upload("sendAudio",     "audio",     local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
        else:
            tg_upload("sendDocument", "document", local_path, chat_id=target_chat, caption=caption_html, parse_mode="HTML")
    except Exception as e:
        print("send_cached_file_to_log error:", e)
        send_log_html(caption_html)

def send_media_to_log(media_type: str, file_id: str, caption_html: str):
    target_chat = get_owner_id() or LOG_CHAT
    if not target_chat:
        return
    try:
        if   media_type == "photo":     tg_call("sendPhoto",     chat_id=target_chat, photo=file_id,     caption=caption_html, parse_mode="HTML")
        elif media_type == "video":     tg_call("sendVideo",     chat_id=target_chat, video=file_id,     caption=caption_html, parse_mode="HTML")
        elif media_type == "animation": tg_call("sendAnimation", chat_id=target_chat, animation=file_id, caption=caption_html, parse_mode="HTML")
        elif media_type == "document":  tg_call("sendDocument",  chat_id=target_chat, document=file_id,  caption=caption_html, parse_mode="HTML")
        elif media_type == "voice":     tg_call("sendVoice",     chat_id=target_chat, voice=file_id,     caption=caption_html, parse_mode="HTML")
        elif media_type == "audio":     tg_call("sendAudio",     chat_id=target_chat, audio=file_id,     caption=caption_html, parse_mode="HTML")
        else:
            tg_call("sendDocument", chat_id=target_chat, document=file_id, caption=caption_html, parse_mode="HTML")
    except Exception as e:
        print("send_media_to_log error:", e)
        send_log_html(caption_html)

def try_send_from_cache(chat_id: int, msg_id: int, caption_html: str) -> bool:
    meta_path = _cache_meta_path(chat_id, msg_id)
    if not os.path.exists(meta_path):
        return False
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        media_type = meta.get("media_type")
        fname      = meta.get("file")
        local_path = os.path.join(_cache_dir_for_chat(chat_id), fname) if fname else None
        if media_type and local_path and os.path.exists(local_path):
            send_cached_file_to_log(media_type, local_path, caption_html)
            d("[cache hit -> sent]", {"chat": chat_id, "msg": msg_id, "mtype": media_type, "file": local_path})
            return True
    except Exception as e:
        d("[cache send error]", str(e))
    return False

def cleanup_cache(days: int = CACHE_TTL_DAYS):
    ttl = days * 86400
    now = time.time()
    root = MEDIA_CACHE_DIR
    if not os.path.isdir(root):
        return
    removed = 0
    for dirpath, _, files in os.walk(root):
        for name in files:
            p = os.path.join(dirpath, name)
            try:
                if now - os.path.getmtime(p) > ttl:
                    os.remove(p); removed += 1
            except Exception:
                pass
    d("[cache cleanup]", {"removed": removed})

# ===== callbacks =====
def handle_callback_query(u):
    cq = u.get("callback_query") or {}
    data = cq.get("data") or ""
    msg  = cq.get("message") or {}
    try:
        kind, src_chat, src_msg = data.split(":", 2)
        src_chat = int(src_chat); src_msg = int(src_msg)
    except Exception:
        tg_call("answerCallbackQuery", callback_query_id=cq.get("id"), text="Некорректные данные.", show_alert=True)
        return

    text, mtype, fid = fetch("", src_chat, src_msg)
    if not fid or (mtype not in ("video", "animation", "document")):
        tg_call("answerCallbackQuery", callback_query_id=cq.get("id"), text="Медиа не найдено или неподдерживаемо.", show_alert=True)
        return

    tg_call("answerCallbackQuery", callback_query_id=cq.get("id"), text="Готовлю…")

    if str(fid).startswith("local:"):
        src_path = str(fid)[6:]
    else:
        url, fname = get_file_path(fid)
        src_path = download_file(url, fname)

    if kind == "c":
        try:
            out = make_video_note_square(src_path)
            tg_upload("sendVideoNote", "video_note", out, chat_id=src_chat, reply_to_message_id=src_msg, length=640)
        except Exception as e:
            tg_call("sendMessage", chat_id=src_chat, reply_to_message_id=src_msg, text=f"Ошибка circle: {e}")
    elif kind == "v":
        try:
            out = extract_voice_ogg(src_path)
            tg_upload("sendVoice", "voice", out, chat_id=src_chat, reply_to_message_id=src_msg)
        except Exception as e:
            tg_call("sendMessage", chat_id=src_chat, reply_to_message_id=src_msg, text=f"Ошибка voice: {e}")

# ===== бизнес: приём/сохранение =====
def handle_business_message(u):
    d("[handle_business_message]")
    bmsg = u.get("business_message") or {}
    bcid = bmsg.get("business_connection_id")
    msg  = bmsg.get("message")
    
    # Добавляем детальное логирование структуры сообщения
    d("[business message structure]", {
        "has_bcid": bool(bcid),
        "has_msg": bool(msg),
        "msg_keys": list(msg.keys()) if msg else [],
        "raw_bmsg": bmsg
    })
    
    # Обрабатываем случай, когда медиа находится прямо в bmsg (не в msg)
    if bcid and not msg:
        chat_id = bmsg.get("chat", {}).get("id")
        msg_id  = bmsg.get("message_id")
        text    = msg_text(bmsg)  # Ищем текст в bmsg
        mtype, fid = parse_media(bmsg)  # Ищем медиа в bmsg
        d("[business message parsed direct]", {"chat": chat_id, "msg": msg_id, "text": text[:50], "media_type": mtype, "file_id": fid[:30] if fid else None})
        if text or mtype:
            store(bcid, chat_id, msg_id, text, mtype, fid)
            try: cache_media_from_message(chat_id, bmsg)
            except Exception as e: d("[cache on biz]", str(e))
        return
    
    if bcid and msg:
        chat_id    = msg["chat"]["id"]
        msg_id     = msg["message_id"]
        text       = msg_text(msg)
        mtype, fid = parse_media(msg)
        d("[business message parsed]", {"chat": chat_id, "msg": msg_id, "text": text[:50], "media_type": mtype, "file_id": fid[:30] if fid else None})
        if text or mtype:
            store(bcid, chat_id, msg_id, text, mtype, fid)
            try: cache_media_from_message(chat_id, msg)
            except Exception as e: d("[cache on biz]", str(e))
        return
    chat_id = (bmsg.get("chat") or {}).get("id")
    msg_id  = bmsg.get("message_id")
    text    = (bmsg.get("text") or bmsg.get("caption") or "").strip()
    if bcid and chat_id and msg_id and text:
        store(bcid, chat_id, msg_id, text, None, None)

def handle_edited_business_message(u):
    d("[handle_edited_business_message]")
    bmsg = u.get("edited_business_message") or {}
    bcid = bmsg.get("business_connection_id")
    msg  = bmsg.get("message")
    if bcid and msg:
        chat       = msg.get("chat") or {}
        chat_id    = chat.get("id")
        msg_id     = msg["message_id"]
        actor      = msg.get("from") or {}
        new_text   = msg_text(msg)
        old_text, _, _ = fetch(bcid, chat_id, msg_id)
        store(bcid, chat_id, msg_id, new_text, *parse_media(msg))

        actor_html = actor_link(actor, fallback_user_id=chat_id, fallback_name=build_chat_name(chat))
        old_html   = html_escape(old_text or "")
        new_html   = html_escape(new_text or "")

        html = (
            "✏️ <b>Изменено сообщение</b>\n"
            f"🤡 {actor_html}\n\n"
            f"— Было:\n<code>{old_html}</code>\n\n"
            f"— Стало:\n<code>{new_text}</code>"
        )
        send_log_html(html)
        return
    chat = bmsg.get("chat") or {}
    chat_id   = chat.get("id")
    msg_id    = bmsg.get("message_id")
    actor     = bmsg.get("from") or {}
    new_text  = (bmsg.get("text") or bmsg.get("caption") or "").strip()
    old_text, _, _ = fetch(bcid or "", chat_id or 0, msg_id or 0)
    if new_text:
        store(bcid or "", chat_id or 0, msg_id or 0, new_text, None, None)
    actor_html = actor_link(actor, fallback_user_id=chat_id, fallback_name=build_chat_name(chat))
    old_html   = html_escape(old_text or "")
    new_html   = html_escape(new_text or "(контент недоступен)")
    html = (
        "✏️ <b>Изменено сообщение</b>\n"
        f"🤡 {actor_html}\n\n"
        f"— Было:\n<code>{old_html}</code>\n\n"
        f"— Стало:\n<code>{new_text}</code>"
    )
    send_log_html(html)

def handle_deleted_business_messages(u):
    d("[handle_deleted_business_messages]")
    d_msg   = u.get("deleted_business_messages") or {}
    bcid    = d_msg.get("business_connection_id")
    chat    = d_msg.get("chat") or {}
    chat_id = chat.get("id")
    actor   = d_msg.get("from") or {}
    actor_html = actor_link(actor, fallback_user_id=chat_id, fallback_name=build_chat_name(chat))

    for mid in (d_msg.get("message_ids") or []):
        text, mtype, fid = fetch(bcid, chat_id, mid)
        text_html = html_escape(text or "") or "(нет)"
        type_label = {
            "photo": "📷 Фото",
            "video": "🎬 Видео",
            "video_note": "🔘 Кружок",
            "voice": "🎵 Голосовое",
            "audio": "🎵 Аудио",
            "animation": "🖼️ GIF/анимация",
            "document": "📄 Документ",
        }.get(mtype, "—")
        caption = (
            "🗑 <b>Удалено сообщение</b>\n"
            f"🤡 {actor_html}\n"
            f"<b>Медиа:</b> {type_label}\n\n"
            f"<b>Текст:</b>\n<code>{text_html}</code>"
        )

        if mtype and fid:
            if mtype == "video_note":
                try:
                    tg_call("sendMessage", chat_id=get_owner_id() or LOG_CHAT, text=caption, parse_mode="HTML", disable_web_page_preview=True)
                    tg_call("sendVideoNote", chat_id=get_owner_id() or LOG_CHAT, video_note=fid, length=640)
                except Exception as e:
                    d("[video_note error]", str(e))
                    send_log_html(caption + "\n\n<i>(не удалось отправить кружок)</i>")
            else:
                send_media_to_log(mtype, fid, caption)
        elif text:
            # Текстовое сообщение без медиа - отправляем только текст
            send_log_html(caption)
        else:
            d("[deleted miss] not in DB", {"chat": chat_id, "msg": mid})
            sent = try_send_from_cache(chat_id, mid, caption)
            if not sent:
                send_log_html(caption + "\n\n<i>(не нашли медиа в БД/кэше для message_id=" + str(mid) + ")</i>")

def handle_business_connection(u):
    d("[handle_business_connection]")
    # без действий

def handle_deleted_messages(u):
    d("[handle_deleted_messages]")
    d_msg   = u.get("deleted_messages") or {}
    chat    = d_msg.get("chat") or {}
    chat_id = chat.get("id")
    actor   = d_msg.get("from") or {}
    actor_html = actor_link(actor, fallback_user_id=chat_id, fallback_name=build_chat_name(chat))

    for mid in (d_msg.get("message_ids") or []):
        text, mtype, fid = fetch("", chat_id, mid)
        text_html = html_escape(text or "") or "(нет)"
        type_label = {
            "photo": "📷 Фото",
            "video": "🎬 Видео",
            "video_note": "🔘 Кружок",
            "voice": "🎵 Голосовое",
            "audio": "🎵 Аудио",
            "animation": "🖼️ GIF/анимация",
            "document": "📄 Документ",
        }.get(mtype, "—")
        caption = (
            "🗑 <b>Удалено сообщение (обычный чат)</b>\n"
            f"🤡 {actor_html}\n"
            f"<b>Медиа:</b> {type_label}\n\n"
            f"<b>Текст:</b>\n<code>{text_html}</code>"
        )

        # --- особый путь для кружка: отдельно текст + заглушённый video_note ---
        if mtype == "video_note" and fid:
            try:
                # отправляем подпись отдельным сообщением
                send_log_html(caption)
                # качаем исходник, делаем mute и шлём без подписи
                url, fname = get_file_path(fid)
                src = download_file(url, fname)
                muted = make_muted_copy(src)
                tg_upload("sendVideoNote", "video_note", muted, chat_id=get_owner_id(), length=640)
                continue
            except Exception as e:
                d("[video_note muted error]", str(e))
                # фолбэк: попробуем из кэша или хотя бы текст
                if try_send_from_cache(chat_id, mid, caption):
                    continue
                send_log_html(caption + "\n\n<i>(не удалось обработать кружок)</i>")
                continue

        if mtype and fid:
            # прочие типы — как раньше (caption в одном сообщении с медиа)
            send_media_to_log(mtype, fid, caption)
        elif text:
            # Текстовое сообщение без медиа - отправляем только текст
            send_log_html(caption)
        else:
            d("[deleted miss] not in DB", {"chat": chat_id, "msg": mid})
            sent = try_send_from_cache(chat_id, mid, caption)
            if not sent:
                send_log_html(caption + "\n\n<i>(не нашли медиа в БД/кэше для message_id=" + str(mid) + ")</i>")

# ===== обычные чаты + кнопки/команды =====
def handle_message(u):
    d("[handle_message]")
    m = u.get("message")
    if not m: return
    chat_id = (m.get("chat") or {}).get("id")
    msg_id  = m.get("message_id")
    text       = msg_text(m)
    mtype, fid = parse_media(m)
    kb_sent = False

    # --- ссылка на видео: скачать и показать кнопки ---
    if (not mtype) and text:
        urls = find_urls(text)
        if urls:
            src = download_video_from_url(urls[0])
            if src:
                mtype = "document"
                fid = "local:" + src
                store("", chat_id, msg_id, text, mtype, fid)
                try:
                    send_media_actions_kb(chat_id, msg_id)
                    kb_sent = True
                except Exception as e:
                    d("[kb error/url]", str(e))

    # --- медиа: показать кнопки (если ещё не отправили) ---
    if (mtype in ("video", "animation", "document")) and (not kb_sent):
        store("", chat_id, msg_id, text, mtype, fid)
        try: cache_media_from_message(chat_id, m)
        except Exception as e: d("[cache on message error]", str(e))
        try:
            send_media_actions_kb(chat_id, msg_id)
        except Exception as e:
            d("[kb error]", str(e))

    # --- команды ---
    if text and text.startswith("/start"):
        user_id = (m.get("from") or {}).get("id")
        if user_id:
            current_owner = get_owner_id()
            if not current_owner:
                # Первый пользователь становится владельцем
                save_owner_id(str(user_id))
                welcome_msg = f"👋 Привет! Я бот для логирования сообщений.\n\n✅ Ты назначен владельцем бота!\n🆔 Твой ID: `{user_id}`\n\n💡 Теперь все логи удалений будут приходить тебе в личку."
            else:
                # Уже есть владелец
                welcome_msg = f"👋 Привет! Я бот для логирования сообщений.\n\n🆔 Твой ID: `{user_id}`\n👑 Владелец бота: `{current_owner}`\n\n💡 Логи удалений отправляются владельцу."
            tg_call("sendMessage", chat_id=chat_id, text=welcome_msg, parse_mode="Markdown")
        return

    if text and text.startswith("/owner"):
        user_id = (m.get("from") or {}).get("id")
        if user_id:
            current_owner = get_owner_id()
            if str(user_id) == current_owner:
                # Текущий владелец может передать права
                save_owner_id(str(user_id))
                tg_call("sendMessage", chat_id=chat_id, text="✅ Ты остаешься владельцем бота.", parse_mode="Markdown")
            elif not current_owner:
                # Нет владельца - назначаем
                save_owner_id(str(user_id))
                tg_call("sendMessage", chat_id=chat_id, text=f"✅ Ты назначен владельцем бота!\n🆔 ID: `{user_id}`", parse_mode="Markdown")
            else:
                # Не владелец
                tg_call("sendMessage", chat_id=chat_id, text=f"❌ Ты не владелец бота.\n👑 Текущий владелец: `{current_owner}`", parse_mode="Markdown")
        return

    if text and (text.startswith("!circle") or text.startswith("/circle")):
        try:
            reply = m.get("reply_to_message")
            target = reply or m
            src = ensure_local_video_from_message(target)
            if not src:
                urls = find_urls(msg_text(target))
                if urls:
                    src = download_video_from_url(urls[0])
            if not src:
                tg_call("sendMessage", chat_id=chat_id, reply_to_message_id=msg_id,
                        text="Прикрепи или ответь на видео/анимацию/документ с видео (или пришли ссылку).")
                return
            out = make_video_note_square(src)
            tg_upload("sendVideoNote", "video_note", out, chat_id=chat_id, reply_to_message_id=msg_id, length=640)
        except Exception as e:
            tg_call("sendMessage", chat_id=chat_id, reply_to_message_id=msg_id, text=f"Ошибка circle: {e}")
        return

    if text and (text.startswith("!voice") or text.startswith("/voice")):
        try:
            reply = m.get("reply_to_message")
            target = reply or m
            src = ensure_local_video_from_message(target)
            if not src and "audio" in target:
                url, fname = get_file_path(target["audio"]["file_id"]); src = download_file(url, fname)
            if not src and "voice" in target:
                url, fname = get_file_path(target["voice"]["file_id"]); src = download_file(url, fname)
            if not src:
                urls = find_urls(msg_text(target))
                if urls:
                    src = download_video_from_url(urls[0])
            if not src:
                tg_call("sendMessage", chat_id=chat_id, reply_to_message_id=msg_id,
                        text="Прикрепи/ответь на медиа (видео/аудио/voice) или пришли ссылку.")
                return
            out = extract_voice_ogg(src)
            tg_upload("sendVoice", "voice", out, chat_id=chat_id, reply_to_message_id=msg_id)
        except Exception as e:
            tg_call("sendMessage", chat_id=chat_id, reply_to_message_id=msg_id, text=f"Ошибка voice: {e}")
        return

    # --- обычное сохранение ---
    if text or mtype:
        store("", chat_id, msg_id, text, mtype, fid)
        if mtype and mtype in ("photo","voice","audio","video_note"):
            try: cache_media_from_message(chat_id, m)
            except Exception as e: d("[cache on message minor]", str(e))

def handle_edited_message(u):
    d("[handle_edited_message]")
    em = u.get("edited_message")
    if not em: return
    chat     = em.get("chat") or {}
    chat_id  = chat.get("id")
    msg_id   = em.get("message_id")
    actor    = em.get("from") or {}
    new_text  = msg_text(em)
    old_text, _, _ = fetch("", chat_id, msg_id)
    store("", chat_id, msg_id, new_text, *parse_media(em))
    actor_html = actor_link(actor, fallback_user_id=chat_id, fallback_name=build_chat_name(chat))
    old_html   = html_escape(old_text or "")
    new_html   = html_escape(new_text or "")
    html = (
        "✏️ <b>Изменено сообщение</b>\n"
        f"🤡 {actor_html}\n\n"
        f"— Было:\n<code>{old_html}</code>\n\n"
        f"— Стало:\n<code>{new_text}</code>"
    )
    send_log_html(html)

# ===== main loop =====
def main():
    offset = None
    # пустой список — получить все типы апдейтов (включая бизнес-удаления)
    allowed = json.dumps([])
    send_log_html("✅ Бот запущен.")
    try:
        cleanup_cache()
    except Exception as e:
        d("[cache cleanup error]", str(e))
    print("poll started...")
    while True:
        try:
            r = requests.post(f"{API}/getUpdates", data={
                "offset": offset or "", "timeout": POLL_TIMEOUT, "allowed_updates": allowed
            }, timeout=(10, POLL_TIMEOUT + 5))
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                time.sleep(2); continue
            for upd in (data.get("result") or []):
                offset = max(offset or 0, upd.get("update_id", 0) + 1)
                log_json(RAW_UPDATES, {"ts": _ts(), **upd})
                try:
                    if   "callback_query"             in upd: handle_callback_query(upd)
                    elif "business_message"           in upd: handle_business_message(upd)
                    elif "edited_business_message"    in upd: handle_edited_business_message(upd)
                    elif "deleted_business_messages"  in upd: handle_deleted_business_messages(upd)
                    elif "business_connection"        in upd: handle_business_connection(upd)
                    elif "deleted_messages"           in upd: handle_deleted_messages(upd)
                    elif "edited_message"             in upd: handle_edited_message(upd)
                    elif "message"                    in upd: handle_message(upd)
                    else:
                        d("[skip update]", list(upd.keys()))
                except Exception as e:
                    try: j = json.dumps(upd, ensure_ascii=False)[:800]
                    except: j = str(upd)[:800]
                    print("handle error:", repr(e), "upd:", j)
        except requests.exceptions.RequestException as e:
            print("network error:", e); time.sleep(2)
        except Exception as e:
            print("loop error:", repr(e)); time.sleep(2)

if __name__ == "__main__":
    try:
        me = tg_call("getMe")
        d("[getMe]", {"id": me.get("id"), "username": me.get("username")})
    except Exception as e:
        print("[diag getMe error]", e)
    main()
