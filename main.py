# ============================================================
#  MEDIA DOWNLOADER BOT  -  PRODUCTION (multi-user, self-healing)
# ============================================================
#  * Every user connects THEIR OWN account (own api_id/api_hash)
#  * Live progress: percent, time left, speed, stop button
#  * Auto retry + "Retry failed" / "Continue" button
#  * Broadcast, Block/Unblock, Maintenance mode (owner)
#  * Supervisor restarts the bot if it crashes or hangs
#
#  Run with:   python main.py
# ============================================================

import os
import sys
import signal
import subprocess
import time as _time

RESTART_CODE = 42   # /restart  -> supervisor starts the bot again
FATAL_CODE = 3      # bad token / bad config -> do not loop

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _supervisor():
    """Keeps the bot alive. Restarts it after a crash or a hang."""
    script = os.path.abspath(__file__)
    state = {"child": None, "stopping": False, "stop_at": 0.0}
    crashes = 0

    def _on_signal(signum, _frame):
        state["stopping"] = True
        state["stop_at"] = _time.time()
        child = state["child"]
        if child is not None and child.poll() is None:
            try:
                child.send_signal(signal.SIGTERM)
            except Exception:
                pass

    try:
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
    except Exception:
        pass

    print("🛡  Supervisor running - the bot restarts itself if it stops.")
    print("    Press Ctrl+C to stop everything.\n")

    while True:
        started = _time.time()
        child = subprocess.Popen([sys.executable, script, "--child"], cwd=BASE_DIR)
        state["child"] = child

        while True:
            try:
                code = child.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if state["stopping"] and _time.time() - state["stop_at"] > 30:
                    child.kill()

        if state["stopping"] or code == 0:
            print("Stopped.")
            return

        if code == FATAL_CODE:
            print(
                "\n❌ Fatal error (bad BOT_TOKEN / API_ID / API_HASH / config).\n"
                "   Read the message printed above, fix config.py, then run again.\n"
                "   If the token changed, also delete media_bot.session*"
            )
            return

        if code == RESTART_CODE:
            print("🔄 Restarting bot ...")
            crashes = 0
            _time.sleep(1)
            continue

        crashes = crashes + 1 if _time.time() - started < 60 else 1
        delay = min(5 * crashes, 60)
        print(f"⚠️  Bot stopped (exit code {code}). Restarting in {delay}s ...")
        end = _time.time() + delay
        while _time.time() < end:
            if state["stopping"]:
                return
            _time.sleep(0.5)


if __name__ == "__main__" and "--child" not in sys.argv:
    _supervisor()
    sys.exit(0)


# ------------------------------------------------------------
# Bot process starts here
# ------------------------------------------------------------

import asyncio

# Create the loop BEFORE pyrogram is imported so pyrogram binds to it.
LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)

import itertools
import re
import shutil
import sqlite3
from collections import Counter
from time import time
from typing import Dict, Optional

import psutil
from cryptography.fernet import Fernet, InvalidToken
from pyleaves import Leaves

import pyrogram.errors as _perr
from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    ApiIdInvalid,
    BadRequest,
    FloodWait,
    InputUserDeactivated,
    PasswordHashInvalid,
    PeerIdInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberInvalid,
    SessionPasswordNeeded,
    Unauthorized,
    UserIsBlocked,
)
from pyrogram.types import (
    BotCommand,
    BotCommandScopeChat,
    InlineKeyboardButton as Btn,
    InlineKeyboardMarkup as Markup,
    Message,
    User,
)


# Newer Telegram channels have IDs below pyrogram's old limit (-1002147483647),
# which raises "ValueError: Peer id invalid". Replace the check itself so it
# works whatever the pyrogram version / attribute names are.
import pyrogram.utils as _pyro_utils

BUILD = "2026-10-04-c"


def _get_peer_type(peer_id: int) -> str:
    if peer_id < 0:
        if -2147483647 <= peer_id:
            return "chat"
        if -1007852516352 <= peer_id < -1000000000000:
            return "channel"
    elif 0 < peer_id <= 999999999999:
        return "user"
    raise ValueError(f"Peer id invalid: {peer_id}")


for _attr, _val in (("MIN_CHANNEL_ID", -1007852516352), ("MAX_USER_ID", 999999999999)):
    if hasattr(_pyro_utils, _attr):
        setattr(_pyro_utils, _attr, _val)
_pyro_utils.get_peer_type = _get_peer_type


def _E(name):
    """Error class that may not exist in every pyrogram fork."""
    return getattr(_perr, name, type(f"_Missing_{name}", (Exception,), {}))


AuthKeyDuplicated = _E("AuthKeyDuplicated")
AccessTokenInvalid = _E("AccessTokenInvalid")
ChannelPrivate = _E("ChannelPrivate")
ChannelInvalid = _E("ChannelInvalid")
UsernameNotOccupied = _E("UsernameNotOccupied")
UsernameInvalid = _E("UsernameInvalid")

SESSION_DEAD = (Unauthorized, AuthKeyDuplicated)
NO_ACCESS = (PeerIdInvalid, ChannelPrivate, ChannelInvalid, UsernameNotOccupied, UsernameInvalid)

from helpers.utils import processMediaGroup, progressArgs, send_media
from helpers.forward import check_forward_permission, resolve_forward_chat_id
from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download,
    cleanup_downloads_root,
)
from helpers.msg import (
    getChatMsgID,
    getStoryChatMsgID,
    is_story_link,
    get_file_name,
    get_story_file_name,
    get_raw_text,
)

from config import PyroConf
from logger import LOGGER

START_TIME = time()


# ============================================================
# SETTINGS  (all optional in config.py - sane defaults here)
# ============================================================

def cfg(name, default):
    value = getattr(PyroConf, name, default)
    return default if value is None else value


MAX_BDL_RANGE = cfg("MAX_BDL_RANGE", 500)
MAX_CONCURRENT_DOWNLOADS = max(1, cfg("MAX_CONCURRENT_DOWNLOADS", 4))
PER_USER_DOWNLOADS = max(1, cfg("PER_USER_DOWNLOADS", 2))
FLOOD_WAIT_DELAY = max(0, cfg("FLOOD_WAIT_DELAY", 2))
ITEM_RETRIES = max(0, cfg("ITEM_RETRIES", 2))
ITEM_TIMEOUT = max(60, cfg("ITEM_TIMEOUT", 3600))
PROGRESS_EDIT_EVERY = max(2, cfg("PROGRESS_EDIT_EVERY", 4))
MAX_USER_TASKS = max(1, cfg("MAX_USER_TASKS", 5))
FLOOD_MAX_WAIT = cfg("FLOOD_MAX_WAIT", 600)
LOGIN_TIMEOUT = 600
IDLE_EVICT = 900
BOT_START_TIME = cfg("BOT_START_TIME", START_TIME)

LINE = "━━━━━━━━━━━━━━━━━━"

# result codes of one download
OK, SKIP, FAIL, RETRY, AUTH, STOPPED = "ok", "skip", "fail", "retry", "auth", "stopped"


def _require_config():
    missing = [k for k in ("API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID")
               if not getattr(PyroConf, k, None)]
    if missing:
        print(f"❌ config.py is missing: {', '.join(missing)}")
        sys.exit(FATAL_CODE)


_require_config()

bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=200,
    parse_mode=ParseMode.MARKDOWN,
    max_concurrent_transmissions=1,
    sleep_threshold=30,
)


# ============================================================
# ENCRYPTION KEY  (must stay the SAME forever)
# ============================================================

def load_fernet() -> Fernet:
    key = cfg("ENCRYPTION_KEY", "") or os.getenv("ENCRYPTION_KEY", "")
    if key:
        try:
            return Fernet(key.encode() if isinstance(key, str) else key)
        except Exception:
            print(
                "❌ ENCRYPTION_KEY in config.py is invalid.\n"
                "   Make a valid one with:\n"
                "   python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"\n"
                "   and paste it as a FIXED string in config.py."
            )
            sys.exit(FATAL_CODE)

    # No key given: create one once and keep it in a file.
    path = os.path.join(BASE_DIR, "encryption.key")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return Fernet(f.read().strip())
    new_key = Fernet.generate_key()
    with open(path, "wb") as f:
        f.write(new_key)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    print(f"🔑 Created {path} - back this file up, it protects saved logins.")
    return Fernet(new_key)


# ============================================================
# DATABASE  (users, members, settings)
# ============================================================

DB_PATH = os.getenv("DB_PATH") or os.path.join(BASE_DIR, "users.db")
fernet = load_fernet()

DB = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None, timeout=10)
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA synchronous=NORMAL")
DB.execute("PRAGMA busy_timeout=5000")


def _enc(text: str) -> str:
    return fernet.encrypt(text.encode()).decode()


def _dec(text: str) -> str:
    return fernet.decrypt(text.encode()).decode()


def init_db():
    DB.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "user_id INTEGER PRIMARY KEY, api_id INTEGER NOT NULL,"
        "api_hash TEXT NOT NULL, session TEXT NOT NULL,"
        "channel TEXT, created REAL)"
    )
    DB.execute(
        "CREATE TABLE IF NOT EXISTS members ("
        "user_id INTEGER PRIMARY KEY, first_name TEXT, username TEXT,"
        "first_seen REAL, last_seen REAL, banned INTEGER DEFAULT 0,"
        "ban_reason TEXT, active INTEGER DEFAULT 1)"
    )
    DB.execute(
        "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)"
    )
    try:
        os.chmod(DB_PATH, 0o600)
    except OSError:
        pass


def check_key_health():
    """Loud warning if ENCRYPTION_KEY changed since saved logins were made."""
    row = DB.execute("SELECT value FROM settings WHERE key='key_check'").fetchone()
    if row is None:
        DB.execute("INSERT OR REPLACE INTO settings VALUES ('key_check', ?)", (_enc("ok"),))
    else:
        try:
            _dec(row[0])
        except InvalidToken:
            LOGGER(__name__).error(
                "ENCRYPTION_KEY IS DIFFERENT from the one used before! "
                "Saved logins cannot be read - users must /connect again. "
                "Put the ORIGINAL fixed key in config.py."
            )
    bad = 0
    for (uid,) in DB.execute("SELECT user_id FROM users").fetchall():
        if get_user(uid, quiet=True) is None:
            bad += 1
    if bad:
        LOGGER(__name__).error(f"{bad} saved login(s) cannot be decrypted.")


# ---- settings ----

def get_setting(key, default=""):
    row = DB.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_setting(key, value):
    DB.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, str(value)))


def maintenance_on() -> bool:
    return get_setting("maintenance", "0") == "1"


# ---- connected accounts ----

_DEC_WARNED = set()


def save_user(uid, api_id, api_hash, session):
    old = DB.execute("SELECT channel FROM users WHERE user_id=?", (uid,)).fetchone()
    DB.execute(
        "INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?)",
        (uid, api_id, _enc(api_hash), _enc(session), old[0] if old else None, time()),
    )
    _DEC_WARNED.discard(uid)


def get_user(uid, quiet=False) -> Optional[dict]:
    row = DB.execute(
        "SELECT api_id, api_hash, session, channel FROM users WHERE user_id=?", (uid,)
    ).fetchone()
    if not row:
        return None
    try:
        return {
            "api_id": row[0],
            "api_hash": _dec(row[1]),
            "session": _dec(row[2]),
            "channel": row[3],
        }
    except InvalidToken:
        if not quiet and uid not in _DEC_WARNED:
            _DEC_WARNED.add(uid)
            LOGGER(__name__).error(
                f"Cannot decrypt login of user {uid} - ENCRYPTION_KEY changed?"
            )
        return None


def user_status(uid):
    """(connected, channel). A login that cannot be read counts as NOT connected,
    so the screens never say 'connected' and then ask to connect again."""
    row = get_user(uid)
    return (True, row["channel"]) if row else (False, None)


def has_unreadable_login(uid) -> bool:
    row = DB.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone()
    return bool(row) and get_user(uid, quiet=True) is None


def set_channel(uid, channel: str):
    DB.execute("UPDATE users SET channel=? WHERE user_id=?", (channel, uid))


def delete_user(uid):
    DB.execute("DELETE FROM users WHERE user_id=?", (uid,))


# ---- members (everyone who ever opened the bot) ----

_LAST_TOUCH: Dict[int, float] = {}


def touch_member(user):
    now = time()
    if now - _LAST_TOUCH.get(user.id, 0) < 60:
        return
    _LAST_TOUCH[user.id] = now
    DB.execute(
        "INSERT INTO members(user_id, first_name, username, first_seen, last_seen)"
        " VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET"
        " first_name=excluded.first_name, username=excluded.username,"
        " last_seen=excluded.last_seen, active=1",
        (user.id, user.first_name or "", user.username or "", now, now),
    )


def is_banned(uid) -> bool:
    row = DB.execute("SELECT banned FROM members WHERE user_id=?", (uid,)).fetchone()
    return bool(row and row[0])


def set_banned(uid, flag: bool, reason=""):
    now = time()
    DB.execute(
        "INSERT OR IGNORE INTO members(user_id, first_seen, last_seen) VALUES(?,?,?)",
        (uid, now, now),
    )
    DB.execute(
        "UPDATE members SET banned=?, ban_reason=? WHERE user_id=?",
        (1 if flag else 0, reason if flag else "", uid),
    )


def mark_inactive(uid):
    DB.execute("UPDATE members SET active=0 WHERE user_id=?", (uid,))


def broadcast_ids():
    rows = DB.execute(
        "SELECT user_id FROM members WHERE banned=0 AND active=1"
    ).fetchall()
    return [r[0] for r in rows]


def member_counts():
    one = lambda q: DB.execute(q).fetchone()[0]
    day = time() - 86400
    return {
        "members": one("SELECT COUNT(*) FROM members"),
        "connected": one("SELECT COUNT(*) FROM users"),
        "banned": one("SELECT COUNT(*) FROM members WHERE banned=1"),
        "today": DB.execute(
            "SELECT COUNT(*) FROM members WHERE last_seen>?", (day,)
        ).fetchone()[0],
    }


def recent_members(n=10):
    return DB.execute(
        "SELECT m.user_id, m.first_name, m.username, m.banned,"
        " (SELECT 1 FROM users u WHERE u.user_id=m.user_id)"
        " FROM members m ORDER BY m.last_seen DESC LIMIT ?", (n,)
    ).fetchall()


def banned_list(n=30):
    return DB.execute(
        "SELECT user_id, first_name, username, ban_reason FROM members"
        " WHERE banned=1 ORDER BY last_seen DESC LIMIT ?", (n,)
    ).fetchall()


# ============================================================
# GLOBAL STATE
# ============================================================

USER_CLIENTS: Dict[int, Client] = {}
CLIENT_LOCKS: Dict[int, asyncio.Lock] = {}
USER_LAST_USED: Dict[int, float] = {}

LOGIN: Dict[int, dict] = {}          # /connect in progress
LOGIN_LOCKS: Dict[int, asyncio.Lock] = {}
CHANNEL_WAIT: Dict[int, float] = {}
USER_TASKS: Dict[int, set] = {}
JOBS: Dict[int, "Job"] = {}
RETRY_STORE: Dict[int, dict] = {}
NOTICE_AT: Dict[int, float] = {}

PENDING_BC: Dict[int, dict] = {}
BC = {"running": False, "stop": False}

download_semaphore: Optional[asyncio.Semaphore] = None
USER_SEMS: Dict[int, asyncio.Semaphore] = {}

BG_TASKS: set = set()
DL_IDS = itertools.count(1)          # unique temp folder per download


def spawn(coro):
    """create_task that keeps a strong reference (tasks can be garbage collected)."""
    t = asyncio.create_task(coro)
    BG_TASKS.add(t)
    t.add_done_callback(BG_TASKS.discard)
    return t


def get_user_sem(uid) -> asyncio.Semaphore:
    if uid not in USER_SEMS:
        USER_SEMS[uid] = asyncio.Semaphore(PER_USER_DOWNLOADS)
    return USER_SEMS[uid]


def login_lock(uid) -> asyncio.Lock:
    return LOGIN_LOCKS.setdefault(uid, asyncio.Lock())


def is_owner(uid) -> bool:
    return uid == PyroConf.OWNER_ID


def is_allowed(uid) -> bool:
    allowed = cfg("ALLOWED_USERS", [])
    if is_owner(uid) or not allowed:
        return True
    return uid in allowed


async def _owner_check(_, __, m):
    return bool(getattr(m, "from_user", None)) and is_owner(m.from_user.id)


owner_only = filters.create(_owner_check)


# ============================================================
# SMALL HELPERS
# ============================================================

def title(t: str) -> str:
    return f"**{t}**\n{LINE}\n"


def md_safe(s) -> str:
    s = re.sub(r"[*_`\[\]~|]", "", str(s or ""))
    return s.replace("--", "-")


def short(e, n=120) -> str:
    return md_safe(str(e).split("\n")[0])[:n]


def fmt_time(s) -> str:
    s = int(max(0, s))
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


def bar(current, total, length=14) -> str:
    if total <= 0:
        return "░" * length
    filled = int(length * max(0, min(current / total, 1)))
    return "█" * filled + "░" * (length - filled)


def fmt_channel(channel) -> str:
    return f"`{channel}`" if channel else "⚠️ Not set"


def explain_error(e) -> str:
    if isinstance(e, NO_ACCESS):
        return "No access to this chat – join it with your connected account"
    if isinstance(e, FloodWait):
        return f"Telegram rate limit – wait {fmt_time(getattr(e, 'value', 0) or 0)}"
    if isinstance(e, SESSION_DEAD):
        return "Your login expired – connect again"
    return f"Telegram: {short(e)}"


async def safe_delete(message):
    try:
        if message:
            await message.delete()
    except Exception:
        pass


async def safe_edit(message, text, markup=None):
    """Edit a message; never raises. Sleeps a little on FloodWait."""
    try:
        await message.edit(text, reply_markup=markup)
        return True
    except FloodWait as e:
        await asyncio.sleep(min(int(getattr(e, "value", 1) or 1), 20))
    except Exception:
        pass
    return False


async def qanswer(query, text=None, alert=False):
    """query.answer that never raises (old queries expire)."""
    try:
        await query.answer(text, show_alert=alert)
    except Exception:
        pass


async def cleanup_file(path):
    if not path:
        return
    try:
        cleanup_download(path)
    except Exception as e:
        LOGGER(__name__).warning(f"Cleanup failed for {path}: {e}")


async def flood_call(fn, tries=3):
    """Call an async fn, sleeping through FloodWait."""
    for attempt in range(tries):
        try:
            return await fn()
        except FloodWait as e:
            wait = int(getattr(e, "value", 0) or 0)
            if attempt == tries - 1 or wait > FLOOD_MAX_WAIT:
                raise
            await asyncio.sleep(wait + 1)


def track_task(uid, coro):
    task = asyncio.create_task(coro)
    USER_TASKS.setdefault(uid, set()).add(task)

    def _done(t):
        s = USER_TASKS.get(uid)
        if s is not None:
            s.discard(t)
            if not s:
                USER_TASKS.pop(uid, None)
        if not t.cancelled() and t.exception():
            LOGGER(__name__).error(f"[{uid}] task crashed: {t.exception()!r}")

    task.add_done_callback(_done)
    return task


def running_tasks(uid=None):
    if uid is not None:
        return [t for t in list(USER_TASKS.get(uid, ())) if not t.done()]
    return [t for s in list(USER_TASKS.values()) for t in list(s) if not t.done()]


def _loop_exception_handler(loop, context):
    LOGGER(__name__).error(f"Loop error: {context.get('message')} {context.get('exception')!r}")


LOOP.set_exception_handler(_loop_exception_handler)


# ============================================================
# USER CLIENTS  (each user's own Telegram account)
# ============================================================

def _lock(uid) -> asyncio.Lock:
    return CLIENT_LOCKS.setdefault(uid, asyncio.Lock())


async def _stop_quiet(client):
    for fn in (client.stop, client.disconnect):
        try:
            await asyncio.wait_for(fn(), 15)
            return
        except Exception:
            continue


async def get_user_client(uid) -> Optional[Client]:
    async with _lock(uid):
        client = USER_CLIENTS.get(uid)
        if client and client.is_connected:
            USER_LAST_USED[uid] = time()
            return client
        if client:
            # Stale object: stop it first. Two live clients on the SAME session
            # make Telegram kill the login (AuthKeyDuplicated).
            USER_CLIENTS.pop(uid, None)
            await _stop_quiet(client)

        row = get_user(uid)
        if not row:
            return None

        client = Client(
            f"user_{uid}",
            api_id=row["api_id"],
            api_hash=row["api_hash"],
            session_string=row["session"],
            in_memory=True,
            no_updates=True,
            workers=4,
            max_concurrent_transmissions=1,
            sleep_threshold=30,
        )
        try:
            await asyncio.wait_for(client.start(), 60)
        except SESSION_DEAD:
            LOGGER(__name__).warning(f"Session revoked for user {uid}")
            await _stop_quiet(client)
            delete_user(uid)
            return None
        except BaseException:
            await _stop_quiet(client)
            raise

        USER_CLIENTS[uid] = client
        USER_LAST_USED[uid] = time()
        return client


async def drop_user_client(uid, logout=False):
    async with _lock(uid):
        client = USER_CLIENTS.pop(uid, None)
    if not client:
        return
    try:
        if logout:
            await asyncio.wait_for(client.log_out(), 30)
        else:
            await asyncio.wait_for(client.stop(), 30)
    except Exception:
        await _stop_quiet(client)


async def warm_peers(uc, force=False):
    """A fresh in-memory session knows no chats. Load dialogs once so private
    channel links (t.me/c/...) can be resolved."""
    last = getattr(uc, "_peers_warm", 0)
    if not force and time() - last < 600:
        return
    try:
        count = 0
        async for _ in uc.get_dialogs(limit=300):
            count += 1
    except FloodWait:
        pass
    except Exception as e:
        LOGGER(__name__).warning(f"warm_peers failed: {type(e).__name__}")
    uc._peers_warm = time()


async def get_messages_safe(uc, chat, ids):
    try:
        return await flood_call(lambda: uc.get_messages(chat_id=chat, message_ids=ids))
    except (PeerIdInvalid, KeyError, ValueError):
        await warm_peers(uc, force=True)
        try:
            return await flood_call(lambda: uc.get_messages(chat_id=chat, message_ids=ids))
        except (KeyError, ValueError):
            raise PeerIdInvalid("chat not reachable")


# ============================================================
# CHANNEL TARGET
# ============================================================

def parse_channel_arg(raw: str):
    if not raw:
        return None
    raw = raw.strip()
    for prefix in (
        "https://t.me/", "http://t.me/", "https://telegram.me/", "http://telegram.me/",
    ):
        if raw.lower().startswith(prefix):
            raw = raw[len(prefix):]
            break
    raw = raw.strip("/")
    if not raw:
        return None
    if raw.lstrip("-").isdigit():
        return int(raw)
    if not raw.startswith("@"):
        raw = f"@{raw}"
    return raw


async def resolve_and_check_target(client, raw_channel_id: str):
    parsed = parse_channel_arg(raw_channel_id)
    if parsed is None:
        return None, "Invalid channel ID / username."
    try:
        resolved = await asyncio.wait_for(resolve_forward_chat_id(parsed), 30)
    except Exception as e:
        return None, f"Could not read the channel: {short(e)}"
    try:
        ok, err_msg = await asyncio.wait_for(
            check_forward_permission(client, resolved), 30
        )
    except Exception as e:
        return None, f"Could not check channel access: {short(e)}"
    if not ok:
        return None, err_msg
    return resolved, None


async def prepare(client, message, uid):
    """
    Preflight for every download.
    Returns (user_client, target_chat_id) or None (after telling the user why).
    """
    row = get_user(uid)

    if not row:
        if has_unreadable_login(uid):
            await message.reply(
                "🔒 **Your saved login cannot be read anymore.**\n\n"
                "Please connect again with /connect."
            )
        else:
            await message.reply(
                "🔗 **Connect your account first.**\n\nUse /connect (or tap Connect on /start)."
            )
        return None

    if not row["channel"]:
        await message.reply(
            "📡 **Set your channel first.**\n\n"
            "Use /setchannel — the bot must be admin there with __Post Messages__."
        )
        return None

    target, err = await resolve_and_check_target(client, row["channel"])
    if not target:
        await message.reply(
            f"❌ **Your channel cannot be used.**\n\n{md_safe(err)}\n\nFix it with /setchannel."
        )
        return None

    try:
        uc = await get_user_client(uid)
    except Exception as e:
        LOGGER(__name__).warning(f"[{uid}] client start failed: {type(e).__name__}: {e}")
        await message.reply(
            "⚠️ **Could not connect to your account right now.**\n\nPlease try again in a minute."
        )
        return None

    if not uc:
        await message.reply(
            "🔒 **Your login is no longer valid.**\n\nPlease connect again with /connect."
        )
        return None

    return uc, target


# ============================================================
# SCREENS  (text + buttons)
# ============================================================

def home_text(uid) -> str:
    connected, channel = user_status(uid)
    n = len(running_tasks(uid))
    job = JOBS.get(uid)

    t = title("⚡ Media Downloader")
    t += f"👤 **Account:**  {'✅ Connected' if connected else '❌ Not connected'}\n"
    if connected:
        t += f"📡 **Channel:**  {fmt_channel(channel)}\n"
    if job:
        t += f"⚙️ **Running:**  {job.label} ({job.done}/{job.total})\n"
    elif n:
        t += f"⚙️ **Running:**  {n} download(s)\n"
    t += "\n"

    if not connected:
        if has_unreadable_login(uid):
            t += "⚠️ Your old login could not be read. Tap **Connect Account** to link again."
        else:
            t += "👉 **Next step:** tap **Connect Account**."
    elif not channel:
        t += "👉 **Next step:** tap **Set Channel**."
    else:
        t += "✅ **Ready!** Paste a post link here, or use /dl and /bdl."
    return t


def home_markup(uid) -> Markup:
    connected, channel = user_status(uid)
    rows = []
    if not connected:
        rows.append([Btn("🔗 Connect Account", callback_data="menu_connect")])
    else:
        rows.append([
            Btn("📥 Single", callback_data="menu_single"),
            Btn("📦 Batch", callback_data="menu_batch"),
            Btn("📖 Stories", callback_data="menu_story"),
        ])
        rows.append([
            Btn("📡 Set Channel", callback_data="menu_channel"),
            Btn("📊 My Status", callback_data="menu_status"),
        ])
        if uid in JOBS:
            rows.append([Btn("🛑 Stop Running Job", callback_data="job_stop")])
        rows.append([Btn("🔌 Disconnect", callback_data="menu_disconnect")])
    rows.append([Btn("❓ Help", callback_data="menu_help")])
    if is_owner(uid):
        rows.append([Btn("👑 Admin Panel", callback_data="adm_home")])
    return Markup(rows)


def back_markup(extra=None) -> Markup:
    rows = [extra] if extra else []
    rows.append([Btn("🏠 Home", callback_data="menu_home")])
    return Markup(rows)


def help_text(uid) -> str:
    t = title("📚 Help")
    t += (
        "**1. Setup (once)**\n"
        "/connect – link your Telegram account\n"
        "/setchannel – choose where files are sent\n\n"
        "**2. Download**\n"
        "/dl `<link>` – one post\n"
        "/bdl `<first link> <last link>` – many posts\n"
        "/dls `<story link>` – one story\n"
        "/bdls `<first story> <last story>` – many stories\n"
        "Tip: you can also just paste a post link.\n\n"
        "**3. Control**\n"
        "/me – your status\n"
        "/killall – stop your running downloads\n"
        "/disconnect – log out & delete your data\n"
        "/cancel – cancel connect / set channel\n\n"
        "**Good to know**\n"
        "• Your account must have joined the source channel.\n"
        "• The bot must be admin in your channel.\n"
        f"• Batch limit: {MAX_BDL_RANGE} items.\n"
        "• Failed items are retried automatically."
    )
    if is_owner(uid):
        t += (
            "\n\n**👑 Owner**\n"
            "/admin – admin panel\n"
            "/broadcast – message all users\n"
            "/ban `<id> [reason]` · /unban `<id>` · /banned\n"
            "/users – user list\n"
            "/maintenance `on|off [text]`\n"
            "/stats · /logs · /cleanup · /restart\n"
            "/killall all – stop everyone's jobs"
        )
    return t


def status_text(uid) -> str:
    connected, channel = user_status(uid)
    t = title("📊 My Status")
    t += f"👤 **Account:**  {'✅ Connected' if connected else '❌ Not connected'}\n"
    if connected:
        t += f"📡 **Channel:**  {fmt_channel(channel)}\n"
    job = JOBS.get(uid)
    if job:
        t += f"⚙️ **Job:**  {job.label} – {job.done}/{job.total} done\n"
    else:
        t += f"⚙️ **Running:**  {len(running_tasks(uid))} download(s)\n"
    rs = RETRY_STORE.get(uid)
    if rs:
        t += f"🔁 **Waiting to retry:**  {len(rs['ids'])} item(s)\n"
    return t


GUIDE_SINGLE = (
    title("📥 Single Download")
    + "Send a post link, or use:\n\n"
    "`/dl https://t.me/channel/123`\n\n"
    "Works with photos, videos, files, audio, albums and text posts."
)

GUIDE_BATCH = (
    title("📦 Batch Download")
    + "Give the **first** and **last** post link:\n\n"
    "`/bdl https://t.me/channel/100 https://t.me/channel/150`\n\n"
    f"• Up to {MAX_BDL_RANGE} posts at once\n"
    "• You see live progress and time left\n"
    "• Use 🛑 Stop any time\n"
    "• Failed items can be retried with one tap"
)

GUIDE_STORY = (
    title("📖 Stories")
    + "One story:\n`/dls https://t.me/username/s/12`\n\n"
    "Many stories:\n`/bdls https://t.me/username/s/10 https://t.me/username/s/20`"
)

CONNECT_INTRO = (
    title("🔗 Connect Account")
    + "**Please read first**\n"
    "This links __your__ Telegram account so the bot can read chats you "
    "already have access to. Your login is stored **encrypted** and "
    "removed when you tap Disconnect. Continue only if you trust this bot's owner.\n\n"
    "**Step 1 of 4 – API ID**\n"
    "Open https://my.telegram.org → __API development tools__ and send "
    "your **API ID** (numbers only)."
)


# ============================================================
# ACCESS GATE  (ban / maintenance / allow-list)
# ============================================================

def _notice_ok(uid) -> bool:
    now = time()
    if now - NOTICE_AT.get(uid, 0) < 30:
        return False
    NOTICE_AT[uid] = now
    return True


def _gate_reason(uid):
    if is_owner(uid):
        return None
    if is_banned(uid):
        return "🚫 **You are blocked from using this bot.**"
    if not is_allowed(uid):
        return "⛔ **You are not allowed to use this bot.**"
    if maintenance_on():
        msg = get_setting("maint_msg", "") or "We are updating the bot. Please try again soon."
        return f"🛠 **Maintenance mode**\n\n{md_safe(msg)}"
    return None


@bot.on_message(filters.private, group=-1)
async def access_gate(_, message: Message):
    user = message.from_user
    if not user:
        message.stop_propagation()
    touch_member(user)
    reason = _gate_reason(user.id)
    if reason:
        if _notice_ok(user.id):
            try:
                await message.reply(reason)
            except Exception:
                pass
        message.stop_propagation()


@bot.on_callback_query(group=-1)
async def access_gate_cb(_, query):
    reason = _gate_reason(query.from_user.id)
    if reason:
        await qanswer(query, re.sub(r"[*`_]", "", reason)[:180], alert=True)
        query.stop_propagation()


# ============================================================
# /START  /HELP  /ME
# ============================================================

async def send_home(message, uid, note=""):
    text = (note + "\n\n" if note else "") + home_text(uid)
    await message.reply(text, reply_markup=home_markup(uid), disable_web_page_preview=True)


@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(_, message: Message):
    await send_home(message, message.from_user.id)


@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(_, message: Message):
    await message.reply(
        help_text(message.from_user.id), reply_markup=back_markup(),
        disable_web_page_preview=True,
    )


@bot.on_message(filters.command("me") & filters.private)
async def me_cmd(_, message: Message):
    await message.reply(status_text(message.from_user.id), reply_markup=back_markup())


# ============================================================
# /CONNECT  (login flow)
# ============================================================

async def _disc_temp(state):
    temp = state.pop("client", None)
    if temp:
        try:
            await asyncio.wait_for(temp.disconnect(), 15)
        except Exception:
            pass


async def _end_login(uid):
    state = LOGIN.pop(uid, None)
    if state:
        await _disc_temp(state)


async def start_connect(uid, message):
    if user_status(uid)[0]:
        await send_home(message, uid, "✅ **Already connected.**")
        return
    await _end_login(uid)
    CHANNEL_WAIT.pop(uid, None)
    LOGIN[uid] = {"step": "api_id", "ts": time()}
    await message.reply(
        CONNECT_INTRO,
        reply_markup=Markup([[Btn("❌ Cancel", callback_data="login_cancel")]]),
        disable_web_page_preview=True,
    )


@bot.on_message(filters.command("connect") & filters.private)
async def connect_cmd(_, message: Message):
    await start_connect(message.from_user.id, message)


@bot.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(_, message: Message):
    uid = message.from_user.id
    did = False
    if uid in LOGIN:
        await _end_login(uid)
        did = True
    if CHANNEL_WAIT.pop(uid, None):
        did = True
    if PENDING_BC.pop(uid, None):
        did = True
    if did:
        await send_home(message, uid, "🛑 **Cancelled.**")
    else:
        await message.reply("ℹ️ Nothing to cancel.")


async def login_step(client, message: Message):
    uid = message.from_user.id
    state = LOGIN.get(uid)
    if not state:
        return
    raw = message.text or ""
    text = raw.strip()

    if time() - state["ts"] > LOGIN_TIMEOUT:
        await _end_login(uid)
        await message.reply("⌛ **Login timed out.** Run /connect again.")
        return
    state["ts"] = time()
    step = state["step"]

    # ---- 1. API ID ----
    if step == "api_id":
        if not text.isdigit():
            await message.reply("❌ API ID must be numbers only. Send it again.")
            return
        state["api_id"] = int(text)
        state["step"] = "api_hash"
        await message.reply(
            "**Step 2 of 4 – API hash**\n"
            "Send your **API hash** (32 letters/numbers).\n"
            "__Your message is deleted right after.__"
        )
        return

    # ---- 2. API hash ----
    if step == "api_hash":
        await safe_delete(message)
        if not re.fullmatch(r"[0-9a-fA-F]{32}", text):
            await message.reply("❌ That is not a valid API hash. Send it again.")
            return
        state["api_hash"] = text
        state["step"] = "phone"
        await message.reply(
            "**Step 3 of 4 – Phone number**\n"
            "Send the phone number with country code, for example `+919876543210`."
        )
        return

    # ---- 3. phone ----
    if step == "phone":
        phone = re.sub(r"[^\d+]", "", text)
        digits = re.sub(r"\D", "", phone)
        if len(digits) < 7:
            await message.reply("❌ Invalid phone number. Send it again with country code.")
            return
        if not phone.startswith("+"):
            phone = "+" + digits

        await _disc_temp(state)
        temp = Client(
            f"login_{uid}", api_id=state["api_id"], api_hash=state["api_hash"],
            in_memory=True, no_updates=True,
        )
        state["client"] = temp
        try:
            await asyncio.wait_for(temp.connect(), 30)
            sent = await asyncio.wait_for(temp.send_code(phone), 30)
        except ApiIdInvalid:
            await _end_login(uid)
            await message.reply("❌ **API ID / API hash do not match.** Run /connect and try again.")
            return
        except PhoneNumberInvalid:
            await _disc_temp(state)
            await message.reply("❌ Invalid phone number. Send it again with country code.")
            return
        except FloodWait as e:
            await _end_login(uid)
            await message.reply(f"⏳ Telegram says wait **{fmt_time(e.value)}**. Try /connect later.")
            return
        except Exception as e:
            LOGGER(__name__).error(f"send_code failed for {uid}: {type(e).__name__}: {e}")
            await _end_login(uid)
            await message.reply(f"⚠️ Could not send the code: {short(e)}\n\nTry /connect again.")
            return

        state.update(phone=phone, phone_code_hash=sent.phone_code_hash, step="code")
        await message.reply(
            "**Step 4 of 4 – Login code**\n"
            "Telegram sent you a code.\n\n"
            "⚠️ Type it **with spaces or dashes**, like `1 2 3 4 5` — "
            "Telegram cancels codes sent in plain form."
        )
        return

    # ---- 4. code ----
    if step == "code":
        await safe_delete(message)
        temp = state.get("client")
        if not temp:
            await _end_login(uid)
            await message.reply("⚠️ Login state lost. Run /connect again.")
            return
        code = re.sub(r"\D", "", text)
        if not code:
            await message.reply("❌ Send the code (digits, with spaces).")
            return
        try:
            result = await asyncio.wait_for(
                temp.sign_in(state["phone"], state["phone_code_hash"], code), 30
            )
        except SessionPasswordNeeded:
            state["step"] = "password"
            await message.reply(
                "🔐 **Two-step verification is on.**\n"
                "Send your 2FA password.\n__Your message is deleted right after.__"
            )
            return
        except PhoneCodeInvalid:
            await message.reply("❌ Wrong code. Send it again (with spaces).")
            return
        except PhoneCodeExpired:
            await _end_login(uid)
            await message.reply("⌛ Code expired. Run /connect again.")
            return
        except FloodWait as e:
            await _end_login(uid)
            await message.reply(f"⏳ Telegram says wait **{fmt_time(e.value)}**. Try /connect later.")
            return
        except Exception as e:
            LOGGER(__name__).error(f"sign_in failed for {uid}: {type(e).__name__}: {e}")
            await _end_login(uid)
            await message.reply(f"⚠️ Login failed: {short(e)}\n\nTry /connect again.")
            return
        await _finish_login(uid, message, result)
        return

    # ---- 5. 2FA password ----
    if step == "password":
        await safe_delete(message)
        temp = state.get("client")
        if not temp:
            await _end_login(uid)
            await message.reply("⚠️ Login state lost. Run /connect again.")
            return
        try:
            result = await asyncio.wait_for(temp.check_password(raw), 30)
        except PasswordHashInvalid:
            await message.reply("❌ Wrong password. Send it again.")
            return
        except FloodWait as e:
            await _end_login(uid)
            await message.reply(f"⏳ Telegram says wait **{fmt_time(e.value)}**. Try /connect later.")
            return
        except Exception as e:
            LOGGER(__name__).error(f"check_password failed for {uid}: {type(e).__name__}: {e}")
            await _end_login(uid)
            await message.reply(f"⚠️ Login failed: {short(e)}\n\nTry /connect again.")
            return
        await _finish_login(uid, message, result)


async def _finish_login(uid, message, result):
    state = LOGIN.get(uid)
    if not state or not state.get("client"):
        await _end_login(uid)
        await message.reply("⚠️ Login state lost. Run /connect again.")
        return
    temp: Client = state["client"]

    if not isinstance(result, User):
        await _end_login(uid)
        await message.reply(
            "❌ This number has no Telegram account yet. Register in the Telegram app first."
        )
        return

    try:
        session = await temp.export_session_string()
        save_user(uid, state["api_id"], state["api_hash"], session)
    except Exception as e:
        LOGGER(__name__).error(f"Saving session failed for {uid}: {type(e).__name__}: {e}")
        await _end_login(uid)
        await message.reply("⚠️ Could not save your login. Try /connect again.")
        return

    # make sure no old live client uses a previous session of this user
    await drop_user_client(uid)
    await _end_login(uid)
    LOGGER(__name__).info(f"User {uid} connected.")
    await send_home(
        message, uid,
        f"✅ **Connected as {md_safe(result.first_name)}.**\nNow set the channel where files will be sent.",
    )


# ============================================================
# DISCONNECT
# ============================================================

def stop_job(job):
    job.stop = True
    for w in list(job.workers):
        w.cancel()


async def do_disconnect(uid):
    job = JOBS.get(uid)
    if job:
        stop_job(job)
    for t in running_tasks(uid):
        if not job or t is not job.runner:
            t.cancel()
    try:
        await get_user_client(uid)
    except Exception:
        pass
    await drop_user_client(uid, logout=True)
    delete_user(uid)
    RETRY_STORE.pop(uid, None)
    CHANNEL_WAIT.pop(uid, None)
    USER_SEMS.pop(uid, None)
    await _end_login(uid)


CONFIRM_DISCONNECT = (
    title("🔌 Disconnect?")
    + "This will:\n"
    "• log your account out of this bot\n"
    "• delete your saved login\n"
    "• stop your running downloads\n\n"
    "You can connect again any time."
)


@bot.on_message(filters.command("disconnect") & filters.private)
async def disconnect_cmd(_, message: Message):
    uid = message.from_user.id
    row = DB.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone()
    if not row:
        await message.reply("ℹ️ You are not connected.")
        return
    await message.reply(
        CONFIRM_DISCONNECT,
        reply_markup=Markup([
            [Btn("✅ Yes, disconnect", callback_data="disc_yes"),
             Btn("❌ No", callback_data="menu_home")],
        ]),
    )


# ============================================================
# /SETCHANNEL
# ============================================================

async def apply_channel(client, message, uid, raw):
    target, err = await resolve_and_check_target(client, raw)
    if not target:
        await message.reply(
            f"❌ **Cannot use that channel.**\n\n{md_safe(err)}\n\n"
            "Send another ID/@username, or /cancel."
        )
        return
    set_channel(uid, raw.strip())
    CHANNEL_WAIT.pop(uid, None)
    await send_home(message, uid, "✅ **Channel saved.**")


async def ask_channel(message, uid):
    if not user_status(uid)[0]:
        await message.reply("🔗 Connect your account first with /connect.")
        return
    CHANNEL_WAIT[uid] = time()
    await message.reply(
        title("📡 Set Channel")
        + "Send the channel **ID** or **@username**.\n\n"
        "Examples:\n`-1001234567890`\n`@mychannel`\n\n"
        "The bot must be **admin** there with __Post Messages__.\n"
        "Send /cancel to stop."
    )


@bot.on_message(filters.command("setchannel") & filters.private)
async def setchannel_cmd(client, message: Message):
    uid = message.from_user.id
    if len(message.command) >= 2:
        if not user_status(uid)[0]:
            await message.reply("🔗 Connect your account first with /connect.")
            return
        await apply_channel(client, message, uid, message.command[1])
    else:
        await ask_channel(message, uid)


# ============================================================
# DOWNLOAD ENGINE  (one post / one story)
# Each returns (status, reason)
# ============================================================

async def _download_post(client, message, post_url, forward_chat_id, uc, uid):
    media_path = None
    progress_message = None

    if "?" in post_url:
        post_url = post_url.split("?", 1)[0]

    try:
        chat_id, message_id = getChatMsgID(post_url)

        chat_message = await get_messages_safe(uc, chat_id, message_id)

        if not chat_message or getattr(chat_message, "empty", False):
            return FAIL, "Message not found (deleted or no access)"

        # ---- size check ----
        file_size = None
        if chat_message.document:
            file_size = chat_message.document.file_size
        elif chat_message.video:
            file_size = chat_message.video.file_size
        elif chat_message.audio:
            file_size = chat_message.audio.file_size

        if file_size is not None:
            allowed = await fileSizeLimit(file_size, message, "download", uc.me.is_premium)
            if not allowed:
                return FAIL, "File is bigger than your account limit"

        raw_caption, raw_caption_entities = get_raw_text(
            chat_message.caption, chat_message.caption_entities
        )
        raw_text, raw_text_entities = get_raw_text(chat_message.text, chat_message.entities)

        # ---- album ----
        if chat_message.media_group_id:
            ok = await processMediaGroup(
                chat_message, client, message, forward_chat_id=forward_chat_id
            )
            return (OK, "") if ok else (FAIL, "Could not read this album")

        # ---- single media ----
        has_media = any([
            chat_message.photo, chat_message.video, chat_message.audio,
            chat_message.document, chat_message.voice, chat_message.video_note,
            chat_message.animation, chat_message.sticker,
        ])

        if has_media:
            start_time = time()
            progress_message = await message.reply(
                title("📥 Downloading") + "⏳ Preparing file..."
            )

            filename = get_file_name(message_id, chat_message)
            download_path = get_download_path(next(DL_IDS), filename)

            media_path = await chat_message.download(
                file_name=download_path,
                progress=Leaves.progress_for_pyrogram,
                progress_args=progressArgs("📥 Downloading", progress_message, start_time),
            )

            if not media_path or not os.path.exists(media_path):
                return RETRY, "File was not saved"
            if os.path.getsize(media_path) <= 0:
                return RETRY, "Downloaded file was empty"

            if chat_message.photo:
                media_type = "photo"
            elif chat_message.video or chat_message.video_note or chat_message.animation:
                media_type = "video"
            elif chat_message.audio or chat_message.voice:
                media_type = "audio"
            else:
                media_type = "document"

            await send_media(
                client, message, media_path, media_type,
                raw_caption, raw_caption_entities,
                progress_message, start_time,
                forward_chat_id=forward_chat_id,
            )
            return OK, ""

        if chat_message.poll:
            return SKIP, "Polls cannot be downloaded"

        # ---- text only ----
        if chat_message.text or chat_message.caption:
            text = raw_text or raw_caption
            entities = raw_text_entities if raw_text else raw_caption_entities
            if not text:
                return SKIP, "Empty post"
            try:
                await client.send_message(
                    chat_id=forward_chat_id, text=text, entities=entities or None
                )
            except FloodWait:
                raise
            except BadRequest as e:
                if "ENTITY_TEXT_INVALID" not in str(e):
                    raise
                await client.send_message(chat_id=forward_chat_id, text=text)
            return OK, ""

        return SKIP, "Nothing to download in this post"

    except asyncio.CancelledError:
        raise
    except FloodWait:
        raise
    except NO_ACCESS:
        return FAIL, "No access to this chat – join it with your connected account"
    except SESSION_DEAD:
        delete_user(uid)
        await drop_user_client(uid)
        return AUTH, "Your login expired – connect again"
    except BadRequest as e:
        return FAIL, f"Telegram: {short(e)}"
    except (KeyError, ValueError, IndexError):
        return FAIL, "Invalid Telegram link"
    except Exception as e:
        LOGGER(__name__).exception(f"[{uid}] error on {post_url}")
        return RETRY, f"Unexpected error: {short(e, 80)}"
    finally:
        if media_path:
            await cleanup_file(media_path)
        if progress_message:
            await safe_delete(progress_message)


async def _download_story(client, message, story_url, forward_chat_id, uc, uid):
    media_path = None
    progress_message = None

    if "?" in story_url:
        story_url = story_url.split("?", 1)[0]

    try:
        chat_username, story_id = getStoryChatMsgID(story_url)
        try:
            story = await uc.get_stories(chat_id=chat_username, story_ids=story_id)
        except (PeerIdInvalid, KeyError):
            await warm_peers(uc, force=True)
            story = await uc.get_stories(chat_id=chat_username, story_ids=story_id)

        if not story:
            return FAIL, "Story not found (expired, deleted or no access)"

        if story.video:
            allowed = await fileSizeLimit(
                story.video.file_size, message, "download", uc.me.is_premium
            )
            if not allowed:
                return FAIL, "Story video is bigger than your account limit"

        if not (story.photo or story.video):
            return SKIP, "This story has no photo or video"

        raw_caption, raw_caption_entities = get_raw_text(story.caption, story.caption_entities)

        start_time = time()
        progress_message = await message.reply(
            title("📖 Downloading Story") + "⏳ Preparing story..."
        )

        filename = get_story_file_name(story_id, story, chat_username)
        download_path = get_download_path(next(DL_IDS), filename)

        media_path = await story.download(
            file_name=download_path,
            progress=Leaves.progress_for_pyrogram,
            progress_args=progressArgs("📖 Downloading Story", progress_message, start_time),
        )

        if not media_path or not os.path.exists(media_path):
            return RETRY, "Story file was not saved"
        if os.path.getsize(media_path) <= 0:
            return RETRY, "Downloaded story was empty"

        await send_media(
            client, message, media_path,
            "video" if story.video else "photo",
            raw_caption, raw_caption_entities,
            progress_message, start_time,
            forward_chat_id=forward_chat_id,
        )
        return OK, ""

    except asyncio.CancelledError:
        raise
    except FloodWait:
        raise
    except NO_ACCESS:
        return FAIL, "No access to this user's stories"
    except SESSION_DEAD:
        delete_user(uid)
        await drop_user_client(uid)
        return AUTH, "Your login expired – connect again"
    except BadRequest as e:
        return FAIL, f"Telegram: {short(e)}"
    except (KeyError, ValueError, IndexError):
        return FAIL, "Invalid story link"
    except Exception as e:
        LOGGER(__name__).exception(f"[{uid}] story error on {story_url}")
        return RETRY, f"Unexpected error: {short(e, 80)}"
    finally:
        if media_path:
            await cleanup_file(media_path)
        if progress_message:
            await safe_delete(progress_message)


HANDLERS = {"post": _download_post, "story": _download_story}


async def guarded(kind, client, message, url, target, uc, uid):
    """Slots + timeout around one item.
    The per-user slot is taken FIRST, so one user's queue can never hog
    all global slots; the timeout only counts real work, not waiting."""
    fn = HANDLERS[kind]
    try:
        async with get_user_sem(uid):
            async with download_semaphore:
                try:
                    return await asyncio.wait_for(
                        fn(client, message, url, target, uc, uid), ITEM_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    return RETRY, "Timed out"
    except FloodWait as e:
        wait = int(getattr(e, "value", 0) or 0)
        if wait > FLOOD_MAX_WAIT:
            return FAIL, f"Telegram asked to wait {fmt_time(wait)} – try later"
        await asyncio.sleep(wait + 1)   # slots are already released here
        return RETRY, f"Telegram rate limit ({wait}s)"


async def run_with_retries(kind, client, message, url, target, uc, uid, stop_check=None):
    """Runs one item with automatic retries. Returns (status, reason)."""
    status, reason = RETRY, ""
    for attempt in range(ITEM_RETRIES + 1):
        if stop_check and stop_check():
            return STOPPED, ""
        status, reason = await guarded(kind, client, message, url, target, uc, uid)
        if status != RETRY:
            break
        if attempt < ITEM_RETRIES:
            await asyncio.sleep(3 * (attempt + 1))
    return status, reason


# ============================================================
# SINGLE LINKS  (/dl /dls + pasted links)
# ============================================================

async def run_single(client, message, uc, target, uid, kind, url):
    try:
        status, reason = await run_with_retries(kind, client, message, url, target, uc, uid)
        if status == OK:
            return
        if status == SKIP:
            await message.reply(f"ℹ️ {md_safe(reason)}")
        elif status == AUTH:
            await message.reply(f"🔒 **{md_safe(reason)}**\n\nUse /connect to log in again.")
        elif status == RETRY:
            await message.reply(
                f"❌ **Failed after several tries.**\n{md_safe(reason)}\n\nPlease try again later."
            )
        else:
            await message.reply(f"❌ **Failed.**\n{md_safe(reason)}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        LOGGER(__name__).exception(f"[{uid}] single download crashed")
        try:
            await message.reply(f"⚠️ **Something went wrong.**\n{short(e)}")
        except Exception:
            pass


async def start_single(client, message, uid, kind, url):
    if len(running_tasks(uid)) >= MAX_USER_TASKS:
        await message.reply(
            f"⏳ You already have {MAX_USER_TASKS} downloads running. Wait for one to finish."
        )
        return
    prep = await prepare(client, message, uid)
    if not prep:
        return
    uc, target = prep
    track_task(uid, run_single(client, message, uc, target, uid, kind, url))
    await message.reply("⏳ **Added.** Your file will arrive in your channel.")


@bot.on_message(filters.command("dl") & filters.private)
async def dl_cmd(client, message: Message):
    args = message.command
    if len(args) != 2 or not args[1].startswith("https://t.me/"):
        await message.reply(GUIDE_SINGLE, reply_markup=back_markup())
        return
    kind = "story" if is_story_link(args[1]) else "post"
    await start_single(client, message, message.from_user.id, kind, args[1])


@bot.on_message(filters.command("dls") & filters.private)
async def dls_cmd(client, message: Message):
    args = message.command
    if len(args) != 2 or not is_story_link(args[1]):
        await message.reply(GUIDE_STORY, reply_markup=back_markup())
        return
    await start_single(client, message, message.from_user.id, "story", args[1])


# ============================================================
# BATCH JOBS  (live progress, stop, retry, continue)
# ============================================================

class Job:
    def __init__(self, uid, kind):
        self.uid = uid
        self.kind = kind                       # "post" | "story"
        self.label = "Batch download" if kind == "post" else "Story batch"
        self.phase = "scan"                    # scan | run
        self.scanned = 0
        self.scan_total = 0
        self.total = 0
        self.done = self.ok = self.failed = self.skipped = 0
        self.failures = []                     # (id, reason, status)
        self.inflight = set()
        self.started = time()
        self.stop = False
        self.current = None
        self.panel = None
        self.runner = None
        self.workers = []
        self.fatal = None
        self.error = None


def stop_markup():
    return Markup([[Btn("🛑 Stop", callback_data="job_stop")]])


def job_text(job: Job) -> str:
    t = title(f"{'📦' if job.kind == 'post' else '📖'} {job.label}")

    if job.phase == "scan":
        t += (
            f"🔎 **Checking messages...**\n\n"
            f"{bar(job.scanned, job.scan_total)}  "
            f"{int(job.scanned / job.scan_total * 100) if job.scan_total else 0}%\n"
            f"Checked: {job.scanned} / {job.scan_total}"
        )
        return t

    pct = int(job.done / job.total * 100) if job.total else 100
    elapsed = time() - job.started
    rate = job.done / elapsed if elapsed > 0 and job.done else 0
    left = (job.total - job.done) / rate if rate else None

    t += f"{bar(job.done, job.total)}  **{pct}%**\n\n"
    t += f"📊 **Done:**  {job.done} / {job.total}\n"
    t += f"✅ Saved:  {job.ok}\n"
    if job.skipped:
        t += f"⏭ Skipped:  {job.skipped}\n"
    t += f"❌ Failed:  {job.failed}\n\n"
    t += f"⏱ **Time used:**  {fmt_time(elapsed)}\n"
    t += f"⏳ **Time left:**  {'~' + fmt_time(left) if left is not None else 'calculating...'}\n"
    if rate:
        t += f"⚡ **Speed:**  {rate * 60:.1f} items/min\n"
    if job.current is not None:
        t += f"▶️ **Now:**  #{job.current}\n"
    return t


async def panel_updater(job: Job):
    last = ""
    while True:
        await asyncio.sleep(PROGRESS_EDIT_EVERY)
        text = job_text(job)
        if text != last:
            await safe_edit(job.panel, text, stop_markup())
            last = text


async def scan_posts(uc, chat, first_id, last_id, job: Job):
    """Reads messages in bulk; drops empty ones and duplicate album parts."""
    ids = list(range(first_id, last_id + 1))
    job.scan_total = len(ids)
    keep, seen, skipped = [], set(), 0

    for i in range(0, len(ids), 100):
        if job.stop:
            break
        chunk = ids[i:i + 100]
        msgs = await get_messages_safe(uc, chat, chunk)
        if not isinstance(msgs, list):
            msgs = [msgs]
        for m in msgs:
            if not m or getattr(m, "empty", False):
                skipped += 1
                continue
            if m.media_group_id:
                gid = str(m.media_group_id)
                if gid in seen:
                    skipped += 1
                    continue
                seen.add(gid)
            if not (m.media or m.text or m.caption):
                skipped += 1
                continue
            keep.append(m.id)
        job.scanned = min(i + len(chunk), len(ids))
        await asyncio.sleep(0.4)
    return keep, skipped


async def run_items(client, message, uc, target, job: Job, ids, prefix):
    queue: asyncio.Queue = asyncio.Queue()
    for i in ids:
        queue.put_nowait(i)

    async def worker():
        while not job.stop:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            job.current = item
            job.inflight.add(item)
            try:
                status, reason = await run_with_retries(
                    job.kind, client, message, f"{prefix}/{item}", target, uc, job.uid,
                    stop_check=lambda: job.stop,
                )
            except asyncio.CancelledError:
                raise                      # item stays in inflight -> can be continued
            except Exception as e:
                LOGGER(__name__).exception(f"[{job.uid}] worker error on {item}")
                status, reason = RETRY, f"Unexpected error: {short(e, 80)}"

            if status == STOPPED:
                return                     # item stays in inflight -> can be continued

            job.inflight.discard(item)
            job.done += 1
            if status == OK:
                job.ok += 1
            elif status == SKIP:
                job.skipped += 1
            else:
                job.failed += 1
                job.failures.append((item, reason, status))
                if status == AUTH:
                    job.fatal = reason
                    job.stop = True
                    return
            if FLOOD_WAIT_DELAY:
                await asyncio.sleep(FLOOD_WAIT_DELAY)

    n = max(1, min(PER_USER_DOWNLOADS, len(ids)))
    job.workers = [asyncio.create_task(worker()) for _ in range(n)]
    await asyncio.gather(*job.workers, return_exceptions=True)

    left = list(job.inflight)
    while not queue.empty():
        left.append(queue.get_nowait())
    return left


def finish_text(job: Job, left_count: int) -> str:
    elapsed = fmt_time(time() - job.started)
    if job.fatal:
        head = "⚠️ Stopped – login expired"
    elif job.error:
        head = "⚠️ Stopped – error"
    elif job.stop:
        head = "🛑 Stopped"
    elif job.failed:
        head = "✅ Finished (with some failures)"
    else:
        head = "✅ Finished"

    t = title(f"{'📦' if job.kind == 'post' else '📖'} {head}")
    t += f"📊 **Total:**  {job.total}\n"
    t += f"✅ Saved:  {job.ok}\n"
    if job.skipped:
        t += f"⏭ Skipped:  {job.skipped}\n"
    t += f"❌ Failed:  {job.failed}\n"
    if left_count and job.stop:
        t += f"⏸ Not done:  {left_count}\n"
    t += f"⏱ **Time:**  {elapsed}\n"

    if job.error:
        t += f"\n⚠️ {md_safe(job.error)}\n"
    if job.failures:
        t += "\n**Why some failed**\n"
        for reason, n in Counter(r for _, r, _ in job.failures).most_common(4):
            t += f"• {n}× {md_safe(reason)}\n"
    if job.fatal:
        t += "\nUse /connect to log in again."
    return t


async def batch_runner(client, message, uc, target, job: Job, prefix, ids, scan):
    uid = job.uid
    updater = asyncio.create_task(panel_updater(job))
    left = []
    try:
        if scan:
            chat, first_id, last_id = scan
            ids, skipped = await scan_posts(uc, chat, first_id, last_id, job)
            job.skipped = skipped
            if job.stop:                   # stopped while checking -> nothing started
                ids = []

        job.phase = "run"
        job.total = len(ids)
        job.started = time()

        if ids and not job.stop:
            left = await run_items(client, message, uc, target, job, ids, prefix)
        elif ids:
            left = list(ids)
    except asyncio.CancelledError:
        job.stop = True
    except SESSION_DEAD:
        delete_user(uid)
        await drop_user_client(uid)
        job.stop = True
        job.fatal = "Your login expired – connect again"
    except Exception as e:
        LOGGER(__name__).exception(f"[{uid}] batch crashed")
        job.stop = True
        job.error = explain_error(e)
    finally:
        updater.cancel()
        if JOBS.get(uid) is job:
            JOBS.pop(uid, None)

    retry_ids = {i for i, _, st in job.failures if st in (RETRY, AUTH)}
    retry_ids.update(left)
    markup = None
    if retry_ids:
        RETRY_STORE[uid] = {
            "kind": job.kind, "prefix": prefix,
            "ids": sorted(retry_ids), "origin": message,
        }
        label = "▶️ Continue" if job.stop else "🔁 Retry failed"
        markup = Markup([[Btn(f"{label} ({len(retry_ids)})", callback_data="job_retry")]])
    else:
        RETRY_STORE.pop(uid, None)

    await safe_delete(job.panel)
    try:
        await message.reply(finish_text(job, len(left)), reply_markup=markup)
    except Exception:
        pass


async def launch_batch(client, message, uid, uc, target, kind, prefix, ids=None, scan=None):
    if uid in JOBS:
        await message.reply("⚠️ **You already have a batch running.**\n\nStop it first with 🛑 or /killall.")
        return
    job = Job(uid, kind)
    JOBS[uid] = job
    if ids is not None:
        job.phase = "run"
        job.total = len(ids)
    try:
        job.panel = await message.reply(job_text(job), reply_markup=stop_markup())
    except Exception:
        JOBS.pop(uid, None)
        raise
    job.runner = track_task(
        uid, batch_runner(client, message, uc, target, job, prefix, ids, scan)
    )


@bot.on_message(filters.command("bdl") & filters.private)
async def bdl_cmd(client, message: Message):
    uid = message.from_user.id
    args = message.command

    if len(args) != 3 or not all(a.startswith("https://t.me/") for a in args[1:]):
        await message.reply(GUIDE_BATCH, reply_markup=back_markup())
        return

    first_url, last_url = args[1].split("?")[0], args[2].split("?")[0]
    try:
        c1, first_id = getChatMsgID(first_url)
        c2, last_id = getChatMsgID(last_url)
    except Exception as e:
        await message.reply(f"❌ **Could not read the links.**\n{short(e)}")
        return

    if str(c1).lower() != str(c2).lower():
        await message.reply("❌ **Both links must be from the same channel/group.**")
        return
    if first_id > last_id:
        await message.reply("❌ **Wrong order.** The first link must have the smaller number.")
        return
    total = last_id - first_id + 1
    if total > MAX_BDL_RANGE:
        await message.reply(
            f"❌ **Too many posts.**\nMaximum: {MAX_BDL_RANGE}\nYou asked: {total}"
        )
        return
    if uid in JOBS:
        await message.reply("⚠️ **You already have a batch running.**\n\nStop it first with 🛑 or /killall.")
        return

    prep = await prepare(client, message, uid)
    if not prep:
        return
    uc, target = prep

    prefix = first_url.rsplit("/", 1)[0]
    await launch_batch(client, message, uid, uc, target, "post", prefix,
                       scan=(c1, first_id, last_id))


@bot.on_message(filters.command("bdls") & filters.private)
async def bdls_cmd(client, message: Message):
    uid = message.from_user.id
    args = message.command

    if len(args) != 3 or not all(is_story_link(a) for a in args[1:]):
        await message.reply(GUIDE_STORY, reply_markup=back_markup())
        return

    try:
        c1, first_id = getStoryChatMsgID(args[1].split("?")[0])
        c2, last_id = getStoryChatMsgID(args[2].split("?")[0])
    except Exception as e:
        await message.reply(f"❌ **Could not read the links.**\n{short(e)}")
        return

    if str(c1).lower() != str(c2).lower():
        await message.reply("❌ **Both story links must be from the same user.**")
        return
    if first_id > last_id:
        await message.reply("❌ **Wrong order.** The first link must have the smaller number.")
        return
    total = last_id - first_id + 1
    if total > MAX_BDL_RANGE:
        await message.reply(
            f"❌ **Too many stories.**\nMaximum: {MAX_BDL_RANGE}\nYou asked: {total}"
        )
        return
    if uid in JOBS:
        await message.reply("⚠️ **You already have a batch running.**\n\nStop it first with 🛑 or /killall.")
        return

    prep = await prepare(client, message, uid)
    if not prep:
        return
    uc, target = prep

    await launch_batch(client, message, uid, uc, target, "story",
                       f"https://t.me/{c1}/s", ids=list(range(first_id, last_id + 1)))


@bot.on_message(filters.command("killall") & filters.private)
async def killall_cmd(_, message: Message):
    uid = message.from_user.id
    args = message.command

    if is_owner(uid) and len(args) > 1 and args[1].lower() == "all":
        uids = list(USER_TASKS.keys() | JOBS.keys())
    else:
        uids = [uid]

    stopped = 0
    for u in uids:
        job = JOBS.get(u)
        if job:
            stop_job(job)
            stopped += 1
        for t in running_tasks(u):
            if not job or t is not job.runner:
                t.cancel()
                stopped += 1

    if not stopped:
        await message.reply("ℹ️ Nothing is running.")
    else:
        await message.reply(f"🛑 **Stopped.**\nStopped {stopped} running item(s).")


# ============================================================
# OWNER: ADMIN PANEL, USERS, BAN, MAINTENANCE
# ============================================================

def admin_text() -> str:
    c = member_counts()
    t = title("👑 Admin Panel")
    t += f"👥 **Users:**  {c['members']}   (active 24h: {c['today']})\n"
    t += f"🔗 **Connected:**  {c['connected']}\n"
    t += f"🚫 **Blocked:**  {c['banned']}\n"
    t += f"⚙️ **Running:**  {len(JOBS)} batch · {len(running_tasks())} task(s)\n"
    t += f"🛠 **Maintenance:**  {'🔴 ON' if maintenance_on() else '🟢 OFF'}\n"
    t += f"⏱ **Uptime:**  {fmt_time(time() - BOT_START_TIME)}\n"
    return t


def admin_markup() -> Markup:
    on = maintenance_on()
    return Markup([
        [Btn("📊 Full Stats", callback_data="adm_stats"),
         Btn("👥 Users", callback_data="adm_users")],
        [Btn("🚫 Blocked List", callback_data="adm_banned"),
         Btn("📣 Broadcast", callback_data="adm_broadcast")],
        [Btn("🟢 Turn Maintenance OFF" if on else "🔴 Turn Maintenance ON",
             callback_data="adm_maint")],
        [Btn("🧹 Cleanup Files", callback_data="adm_cleanup"),
         Btn("🔄 Restart Bot", callback_data="adm_restart")],
        [Btn("🏠 Home", callback_data="menu_home")],
    ])


def admin_back() -> Markup:
    return Markup([[Btn("⬅️ Admin Panel", callback_data="adm_home")]])


def users_text() -> str:
    c = member_counts()
    t = title("👥 Users")
    t += f"Total: {c['members']}   Connected: {c['connected']}   Blocked: {c['banned']}\n\n"
    t += "**Recently active**\n"
    for uid, name, uname, banned, conn in recent_members(10):
        who = md_safe(name)[:18] or "—"
        if uname:
            who += f" @{md_safe(uname)}"
        icons = ("🚫" if banned else "") + ("🔗" if conn else "")
        t += f"• {who} `{uid}` {icons}\n"
    t += "\n🔗 = connected   🚫 = blocked\n"
    t += "Block: `/ban <id> [reason]`   Unblock: `/unban <id>`"
    return t


def banned_text() -> str:
    rows = banned_list()
    t = title("🚫 Blocked Users")
    if not rows:
        return t + "No one is blocked."
    for uid, name, uname, reason in rows:
        who = md_safe(name)[:18] or "—"
        if uname:
            who += f" @{md_safe(uname)}"
        t += f"• {who} `{uid}`" + (f" – {md_safe(reason)[:40]}" if reason else "") + "\n"
    t += "\nUnblock: `/unban <id>`"
    return t


def system_stats_text() -> str:
    d_total, d_used, d_free = shutil.disk_usage(BASE_DIR)
    proc = psutil.Process(os.getpid())
    net = psutil.net_io_counters()
    c = member_counts()
    return (
        title("📊 System Stats")
        + "**Bot**\n"
        f"├ Uptime: {fmt_time(time() - BOT_START_TIME)}\n"
        f"├ Users: {c['members']} (connected {c['connected']})\n"
        f"├ Live sessions: {len(USER_CLIENTS)}\n"
        f"├ Batches: {len(JOBS)}\n"
        f"└ Tasks: {len(running_tasks())}\n\n"
        "**Disk**\n"
        f"├ Used: {get_readable_file_size(d_used)}\n"
        f"└ Free: {get_readable_file_size(d_free)}\n\n"
        "**System**\n"
        f"├ CPU: {psutil.cpu_percent(interval=0.3)}%\n"
        f"├ RAM: {psutil.virtual_memory().percent}%\n"
        f"└ Bot RAM: {round(proc.memory_info().rss / 1024 ** 2)} MiB\n\n"
        "**Network**\n"
        f"├ Sent: {get_readable_file_size(net.bytes_sent)}\n"
        f"└ Received: {get_readable_file_size(net.bytes_recv)}\n\n"
        "**Limits**\n"
        f"├ Global slots: {MAX_CONCURRENT_DOWNLOADS}\n"
        f"├ Per user: {PER_USER_DOWNLOADS}\n"
        f"└ Batch max: {MAX_BDL_RANGE}"
    )


async def stats_text_async() -> str:
    return await asyncio.to_thread(system_stats_text)   # does not block the bot


@bot.on_message(filters.command("admin") & filters.private & owner_only)
async def admin_cmd(_, message: Message):
    await message.reply(admin_text(), reply_markup=admin_markup())


@bot.on_message(filters.command("users") & filters.private & owner_only)
async def users_cmd(_, message: Message):
    await message.reply(users_text(), reply_markup=admin_back())


@bot.on_message(filters.command("banned") & filters.private & owner_only)
async def banned_cmd(_, message: Message):
    await message.reply(banned_text(), reply_markup=admin_back())


@bot.on_message(filters.command("stats") & filters.private)
async def stats_cmd(_, message: Message):
    uid = message.from_user.id
    if is_owner(uid):
        try:
            await message.reply(await stats_text_async(), reply_markup=admin_back())
        except Exception:
            LOGGER(__name__).exception("stats")
            await message.reply("❌ Could not read system stats.")
    else:
        await message.reply(status_text(uid), reply_markup=back_markup())


@bot.on_message(filters.command("ban") & filters.private & owner_only)
async def ban_cmd(client, message: Message):
    args = message.command
    if len(args) < 2 or not args[1].lstrip("-").isdigit():
        await message.reply("Usage: `/ban <user_id> [reason]`")
        return
    target = int(args[1])
    if is_owner(target):
        await message.reply("❌ You cannot block yourself.")
        return
    reason = " ".join(args[2:])[:80]
    set_banned(target, True, reason)

    job = JOBS.get(target)
    if job:
        stop_job(job)
    for t in running_tasks(target):
        if not job or t is not job.runner:
            t.cancel()

    try:
        await client.send_message(target, "🚫 **You have been blocked from this bot.**")
    except Exception:
        pass
    await message.reply(f"🚫 **Blocked** `{target}`" + (f"\nReason: {md_safe(reason)}" if reason else ""))


@bot.on_message(filters.command("unban") & filters.private & owner_only)
async def unban_cmd(client, message: Message):
    args = message.command
    if len(args) != 2 or not args[1].lstrip("-").isdigit():
        await message.reply("Usage: `/unban <user_id>`")
        return
    target = int(args[1])
    set_banned(target, False)
    try:
        await client.send_message(target, "✅ **You can use the bot again.**")
    except Exception:
        pass
    await message.reply(f"✅ **Unblocked** `{target}`")


@bot.on_message(filters.command("maintenance") & filters.private & owner_only)
async def maintenance_cmd(_, message: Message):
    args = message.command
    if len(args) < 2 or args[1].lower() not in ("on", "off"):
        await message.reply(
            f"🛠 Maintenance is **{'ON' if maintenance_on() else 'OFF'}**.\n\n"
            "`/maintenance on [message for users]`\n`/maintenance off`"
        )
        return
    if args[1].lower() == "on":
        set_setting("maintenance", "1")
        set_setting("maint_msg", " ".join(args[2:])[:200])
        await message.reply("🔴 **Maintenance ON.** Users see a notice. You can still use the bot.")
    else:
        set_setting("maintenance", "0")
        await message.reply("🟢 **Maintenance OFF.** Everyone can use the bot.")


@bot.on_message(filters.command("logs") & filters.private & owner_only)
async def logs_cmd(_, message: Message):
    path = os.path.join(BASE_DIR, "logs.txt")
    if not os.path.exists(path):
        path = "logs.txt"
    if not os.path.exists(path):
        await message.reply("ℹ️ No log file found.")
        return
    try:
        await message.reply_document(document=path, caption="📜 **Bot logs**")
    except Exception:
        await message.reply("❌ Could not send the log file.")


async def do_cleanup():
    files, freed = cleanup_downloads_root()
    if not files:
        return "🧹 **Already clean.** No temporary files found."
    return f"🧹 **Cleanup done.**\nRemoved {files} file(s), freed {get_readable_file_size(freed)}."


@bot.on_message(filters.command("cleanup") & filters.private & owner_only)
async def cleanup_cmd(_, message: Message):
    try:
        await message.reply(await do_cleanup())
    except Exception:
        LOGGER(__name__).exception("cleanup")
        await message.reply("❌ Cleanup failed.")


async def do_restart():
    LOGGER(__name__).info("Restart requested by owner.")
    await asyncio.sleep(1)
    try:
        await asyncio.wait_for(bot.stop(), 10)
    except Exception:
        pass
    os._exit(RESTART_CODE)


@bot.on_message(filters.command("restart") & filters.private & owner_only)
async def restart_cmd(_, message: Message):
    await message.reply("🔄 **Restarting...** Back in a few seconds.")
    spawn(do_restart())


# ============================================================
# OWNER: BROADCAST
# ============================================================

async def send_payload(client, chat_id, p):
    if p.get("src_id"):
        await client.copy_message(chat_id, p["src_chat"], p["src_id"])
    else:
        try:
            await client.send_message(chat_id, p["text"], disable_web_page_preview=True)
        except FloodWait:
            raise
        except BadRequest:
            await client.send_message(
                chat_id, p["text"], parse_mode=ParseMode.DISABLED,
                disable_web_page_preview=True,
            )


BC_HELP = (
    title("📣 Broadcast")
    + "Send a message to **all users**.\n\n"
    "**Text:**  `/broadcast Hello everyone!`\n"
    "**Photo / video / any message:** send it to this chat, then **reply** to it with `/broadcast`\n\n"
    "You will see a preview and must confirm before it is sent."
)


@bot.on_message(filters.command("broadcast") & filters.private & owner_only)
async def broadcast_cmd(client, message: Message):
    uid = message.from_user.id

    if BC["running"]:
        await message.reply("⚠️ A broadcast is already running.")
        return

    payload = None
    if message.reply_to_message:
        src = message.reply_to_message
        payload = {"src_chat": src.chat.id, "src_id": src.id}
    elif len(message.command) > 1:
        payload = {"text": message.text.split(None, 1)[1]}

    if not payload:
        await message.reply(BC_HELP, reply_markup=admin_back())
        return

    ids = broadcast_ids()
    PENDING_BC[uid] = {"payload": payload, "ids": ids, "ts": time()}

    await message.reply("👇 **Preview** (this is what users will receive):")
    try:
        await send_payload(client, uid, payload)
    except Exception as e:
        PENDING_BC.pop(uid, None)
        await message.reply(f"❌ Could not build the preview: {short(e)}")
        return

    await message.reply(
        f"**Send to {len(ids)} users?**",
        reply_markup=Markup([[
            Btn("✅ Send now", callback_data="bc_send"),
            Btn("❌ Cancel", callback_data="bc_cancel"),
        ]]),
    )


def bc_text(sent, failed, blocked, total, started) -> str:
    done = sent + failed + blocked
    elapsed = time() - started
    rate = done / elapsed if elapsed > 0 and done else 0
    left = (total - done) / rate if rate else None
    return (
        title("📣 Broadcasting")
        + f"{bar(done, total)}  **{int(done / total * 100) if total else 100}%**\n\n"
        f"📊 **Done:**  {done} / {total}\n"
        f"✅ Delivered:  {sent}\n"
        f"🚫 Blocked bot:  {blocked}\n"
        f"❌ Failed:  {failed}\n\n"
        f"⏱ **Time used:**  {fmt_time(elapsed)}\n"
        f"⏳ **Time left:**  {'~' + fmt_time(left) if left is not None else 'calculating...'}"
    )


async def run_broadcast(client, panel, payload, ids):
    BC["running"], BC["stop"] = True, False
    sent = failed = blocked = 0
    started = time()
    last_edit = 0
    stop_btn = Markup([[Btn("🛑 Stop broadcast", callback_data="bc_stop")]])
    try:
        for target in ids:
            if BC["stop"]:
                break
            for attempt in range(2):
                try:
                    await send_payload(client, target, payload)
                    sent += 1
                    break
                except FloodWait as e:
                    await asyncio.sleep(min(int(getattr(e, "value", 1) or 1), 60) + 1)
                    if attempt == 1:
                        failed += 1
                except (UserIsBlocked, InputUserDeactivated, PeerIdInvalid):
                    mark_inactive(target)
                    blocked += 1
                    break
                except Exception:
                    failed += 1
                    break
            if time() - last_edit >= 3:
                last_edit = time()
                await safe_edit(panel, bc_text(sent, failed, blocked, len(ids), started), stop_btn)
            await asyncio.sleep(0.06)
    finally:
        BC["running"] = False
        head = "🛑 Broadcast stopped" if BC["stop"] else "✅ Broadcast finished"
        await safe_edit(
            panel,
            title(head)
            + f"👥 **Audience:**  {len(ids)}\n"
            f"✅ Delivered:  {sent}\n"
            f"🚫 Blocked bot:  {blocked}\n"
            f"❌ Failed:  {failed}\n"
            f"⏱ **Time:**  {fmt_time(time() - started)}",
            admin_back(),
        )


# ============================================================
# BUTTON ROUTER
# ============================================================

@bot.on_callback_query()
async def callbacks(client, query):
    data = query.data or ""
    uid = query.from_user.id
    msg = query.message
    answered = False

    async def ack(text=None, alert=False):
        nonlocal answered
        if not answered:
            answered = True
            await qanswer(query, text, alert)

    try:
        # ------------ user screens ------------
        if data == "menu_home":
            await ack()
            await safe_edit(msg, home_text(uid), home_markup(uid))

        elif data == "menu_help":
            await ack()
            await safe_edit(msg, help_text(uid), back_markup())

        elif data == "menu_single":
            await ack()
            await safe_edit(msg, GUIDE_SINGLE, back_markup())

        elif data == "menu_batch":
            await ack()
            await safe_edit(msg, GUIDE_BATCH, back_markup())

        elif data == "menu_story":
            await ack()
            await safe_edit(msg, GUIDE_STORY, back_markup())

        elif data == "menu_status":
            await ack()
            await safe_edit(msg, status_text(uid), back_markup())

        elif data == "menu_connect":
            await ack()
            if user_status(uid)[0]:
                # stale button: just refresh this screen instead of asking again
                await safe_edit(msg, home_text(uid), home_markup(uid))
            else:
                await start_connect(uid, msg)

        elif data == "login_cancel":
            await ack()
            await _end_login(uid)
            await safe_edit(msg, home_text(uid), home_markup(uid))

        elif data == "menu_channel":
            await ack()
            await ask_channel(msg, uid)

        elif data == "menu_disconnect":
            await ack()
            await safe_edit(
                msg, CONFIRM_DISCONNECT,
                Markup([[Btn("✅ Yes, disconnect", callback_data="disc_yes"),
                         Btn("❌ No", callback_data="menu_home")]]),
            )

        elif data == "disc_yes":
            await ack()
            await safe_edit(msg, "⏳ Disconnecting...")
            await do_disconnect(uid)
            await safe_edit(
                msg, "✅ **Disconnected.**\n\n" + home_text(uid), home_markup(uid)
            )

        # ------------ jobs ------------
        elif data == "job_stop":
            job = JOBS.get(uid)
            if job:
                stop_job(job)
                await ack("Stopping...")
            else:
                await ack("Nothing is running.", True)

        elif data == "job_retry":
            store = RETRY_STORE.pop(uid, None)
            if not store:
                await ack("Nothing to retry.", True)
                return
            if uid in JOBS:
                RETRY_STORE[uid] = store
                await ack("A batch is already running.", True)
                return
            await ack("Starting...")
            origin = store.get("origin") or msg
            prep = await prepare(client, origin, uid)
            if not prep:
                RETRY_STORE[uid] = store      # keep it, user can fix and tap again
                return
            uc, target = prep
            await safe_edit(msg, "🔁 **Retrying...**")
            await launch_batch(client, origin, uid, uc, target, store["kind"],
                               store["prefix"], ids=store["ids"])

        # ------------ owner screens ------------
        elif data.startswith("adm_") or data.startswith("bc_"):
            if not is_owner(uid):
                await ack("Owner only.", True)
                return

            if data == "adm_home":
                await ack()
                await safe_edit(msg, admin_text(), admin_markup())
            elif data == "adm_stats":
                await ack()
                await safe_edit(msg, await stats_text_async(), admin_back())
            elif data == "adm_users":
                await ack()
                await safe_edit(msg, users_text(), admin_back())
            elif data == "adm_banned":
                await ack()
                await safe_edit(msg, banned_text(), admin_back())
            elif data == "adm_broadcast":
                await ack()
                await safe_edit(msg, BC_HELP, admin_back())
            elif data == "adm_maint":
                await ack()
                set_setting("maintenance", "0" if maintenance_on() else "1")
                await safe_edit(msg, admin_text(), admin_markup())
            elif data == "adm_cleanup":
                await ack()
                await safe_edit(msg, await do_cleanup(), admin_back())
            elif data == "adm_restart":
                await ack()
                await safe_edit(
                    msg, "🔄 **Restart the bot?**\nRunning downloads will stop.",
                    Markup([[Btn("✅ Yes, restart", callback_data="adm_restart_yes"),
                             Btn("❌ No", callback_data="adm_home")]]),
                )
            elif data == "adm_restart_yes":
                await ack()
                await safe_edit(msg, "🔄 **Restarting...** Back in a few seconds.")
                spawn(do_restart())

            elif data == "bc_cancel":
                await ack()
                PENDING_BC.pop(uid, None)
                await safe_edit(msg, "❌ **Broadcast cancelled.**", admin_back())
            elif data == "bc_stop":
                BC["stop"] = True
                await ack("Stopping...")
            elif data == "bc_send":
                pend = PENDING_BC.pop(uid, None)
                if not pend:
                    await ack("Nothing to send (expired).", True)
                    return
                if BC["running"]:
                    await ack("A broadcast is already running.", True)
                    return
                await ack("Sending...")
                await safe_edit(msg, bc_text(0, 0, 0, len(pend["ids"]), time()))
                spawn(run_broadcast(client, msg, pend["payload"], pend["ids"]))
            else:
                await ack()
        else:
            await ack()

    except Exception as e:
        LOGGER(__name__).error(f"Callback error ({data}): {type(e).__name__}: {e}")
        await ack("Something went wrong. Try again.", True)
    finally:
        await ack()      # never leave a spinner


# ============================================================
# PLAIN TEXT  (login steps, channel input, pasted links)
# ============================================================

COMMANDS = [
    "start", "help", "me", "connect", "disconnect", "cancel", "setchannel",
    "dl", "dls", "bdl", "bdls", "killall", "stats",
    "admin", "users", "banned", "ban", "unban", "maintenance",
    "broadcast", "logs", "cleanup", "restart",
]

LINK_RE = re.compile(r"https?://t\.me/\S+")


@bot.on_message(filters.private & filters.text & ~filters.command(COMMANDS))
async def text_router(client, message: Message):
    uid = message.from_user.id
    text = (message.text or "").strip()

    if uid in LOGIN:
        # one login message at a time per user (no double sign_in / double send_code)
        async with login_lock(uid):
            if uid in LOGIN:
                await login_step(client, message)
        return

    if uid in CHANNEL_WAIT:
        if time() - CHANNEL_WAIT[uid] > 300:
            CHANNEL_WAIT.pop(uid, None)
        else:
            await apply_channel(client, message, uid, text)
            return

    if text.startswith("/"):
        await message.reply("❓ Unknown command. Use /help to see all commands.")
        return

    links = LINK_RE.findall(text)
    if len(links) == 1:
        url = links[0].rstrip(").,")
        kind = "story" if is_story_link(url) else "post"
        await start_single(client, message, uid, kind, url)
        return
    if len(links) > 1:
        await message.reply("ℹ️ Send **one** link at a time, or use /bdl for a range.")
        return

    await send_home(message, uid)


# ============================================================
# BACKGROUND KEEPERS  (cleanup + watchdog)
# ============================================================

async def janitor():
    """Frees stale logins and idle sessions so nothing piles up."""
    while True:
        await asyncio.sleep(60)
        try:
            now = time()
            for uid, st in list(LOGIN.items()):
                if now - st["ts"] > LOGIN_TIMEOUT:
                    await _end_login(uid)
            for uid in list(LOGIN_LOCKS):
                if uid not in LOGIN and not LOGIN_LOCKS[uid].locked():
                    LOGIN_LOCKS.pop(uid, None)
            for uid, ts in list(CHANNEL_WAIT.items()):
                if now - ts > 300:
                    CHANNEL_WAIT.pop(uid, None)
            for uid, p in list(PENDING_BC.items()):
                if now - p["ts"] > 600:
                    PENDING_BC.pop(uid, None)
            for uid, ts in list(USER_LAST_USED.items()):
                if (now - ts > IDLE_EVICT and uid in USER_CLIENTS
                        and uid not in JOBS and not running_tasks(uid)):
                    await drop_user_client(uid)
                    USER_LAST_USED.pop(uid, None)
            for uid, ts in list(NOTICE_AT.items()):
                if now - ts > 300:
                    NOTICE_AT.pop(uid, None)
            for uid, ts in list(_LAST_TOUCH.items()):
                if now - ts > 3600:
                    _LAST_TOUCH.pop(uid, None)
        except Exception:
            LOGGER(__name__).exception("janitor")


async def watchdog():
    """If Telegram stops answering, exit so the supervisor restarts us."""
    fails = 0
    while True:
        await asyncio.sleep(60)
        try:
            await asyncio.wait_for(bot.get_me(), 30)
            fails = 0
        except Exception as e:
            fails += 1
            LOGGER(__name__).warning(f"Watchdog: no answer from Telegram ({fails}/3): {e}")
            if fails >= 3:
                LOGGER(__name__).error("Watchdog: bot looks stuck - restarting.")
                os._exit(1)


async def set_commands():
    user_cmds = [
        BotCommand("start", "Home – status & buttons"),
        BotCommand("connect", "Connect your Telegram account"),
        BotCommand("setchannel", "Choose where files are sent"),
        BotCommand("dl", "Download one post"),
        BotCommand("bdl", "Download many posts"),
        BotCommand("dls", "Download one story"),
        BotCommand("bdls", "Download many stories"),
        BotCommand("me", "My status"),
        BotCommand("killall", "Stop my running downloads"),
        BotCommand("disconnect", "Log out & delete my data"),
        BotCommand("cancel", "Cancel current step"),
        BotCommand("help", "Help"),
    ]
    owner_cmds = user_cmds + [
        BotCommand("admin", "Admin panel"),
        BotCommand("broadcast", "Message all users"),
        BotCommand("users", "User list"),
        BotCommand("ban", "Block a user"),
        BotCommand("unban", "Unblock a user"),
        BotCommand("banned", "Blocked list"),
        BotCommand("maintenance", "Maintenance on/off"),
        BotCommand("stats", "System stats"),
        BotCommand("logs", "Get log file"),
        BotCommand("cleanup", "Delete temp files"),
        BotCommand("restart", "Restart the bot"),
    ]
    try:
        await bot.set_bot_commands(user_cmds)
    except Exception as e:
        LOGGER(__name__).warning(f"Could not set command menu: {e}")
    try:
        await bot.set_bot_commands(owner_cmds, scope=BotCommandScopeChat(chat_id=PyroConf.OWNER_ID))
    except Exception as e:
        LOGGER(__name__).warning(f"Could not set owner command menu (owner must /start the bot): {e}")


# ============================================================
# MAIN
# ============================================================

async def main():
    global download_semaphore

    init_db()
    check_key_health()
    download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    LOGGER(__name__).info(f"MEDIA DOWNLOADER STARTING (build {BUILD})")
    try:
        ptype = _pyro_utils.get_peer_type(-1002366162620)
        LOGGER(__name__).info(f"Peer-ID patch active (large channel id -> {ptype})")
    except Exception as e:
        LOGGER(__name__).error(f"Peer-ID patch NOT working: {e}")
    LOGGER(__name__).info(
        f"batch max {MAX_BDL_RANGE} | global slots {MAX_CONCURRENT_DOWNLOADS} | "
        f"per-user {PER_USER_DOWNLOADS} | retries {ITEM_RETRIES}"
    )

    try:
        cleanup_downloads_root()      # leftovers from a previous crash
    except Exception:
        pass

    started = False
    for _ in range(5):
        try:
            await bot.start()
            started = True
            break
        except FloodWait as e:
            wait = int(getattr(e, "value", 5) or 5)
            LOGGER(__name__).warning(f"FloodWait on start: sleeping {wait}s")
            await asyncio.sleep(wait + 1)
        except (Unauthorized, AccessTokenInvalid, ApiIdInvalid) as e:
            LOGGER(__name__).error(f"Bot login failed: {e}")
            print(f"❌ Bot login failed: {e}\n   Check BOT_TOKEN / API_ID / API_HASH in config.py")
            sys.exit(FATAL_CODE)
    if not started:
        sys.exit(1)

    await set_commands()
    spawn(janitor())
    spawn(watchdog())

    me = await bot.get_me()
    LOGGER(__name__).info(f"Bot is online as @{me.username}")

    try:
        await idle()
    finally:
        for uid in list(USER_CLIENTS):
            await drop_user_client(uid)
        try:
            await bot.stop()
        except Exception:
            pass
        LOGGER(__name__).info("Media Downloader stopped.")


if __name__ == "__main__":
    try:
        LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        sys.exit(0)
