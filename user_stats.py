import os
import json
import time
import requests
from sys import stderr
from loguru import logger
from datetime import datetime

# --- .env support ---
ENV_PATH = os.getenv("ENV_PATH", ".env")
try:
    from dotenv import load_dotenv  # type: ignore
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH, override=False)
except Exception:
    def _load_env_fallback(path: str):
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"\'')
                os.environ.setdefault(k, v)

    _load_env_fallback(ENV_PATH)

try:
    from msvcrt import getch  # Windows
except Exception:
    def getch():
        try:
            input("Press Enter to exit...")
        except Exception:
            pass

API_BASE = "https://discord.com/api/v9"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
GUILD_ID = str(os.getenv("GUILD_ID", "")).strip()
MODE = os.getenv("MODE", "backfill").lower()  # backfill | incremental

CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoint.json")
EXPORT_PATH = os.getenv("EXPORT_PATH", "users.json")

# optional message logs
MESSAGES_JSONL_PATH = os.getenv("MESSAGES_JSONL_PATH", "messages.jsonl")
LOG_MESSAGES = os.getenv("LOG_MESSAGES", "0") == "1"  # 1 = write messages.jsonl

HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "10"))
LOG_EVERY_PAGES = int(os.getenv("LOG_EVERY_PAGES", "50"))

# member display names (server nick)
FETCH_MEMBER_DISPLAY = os.getenv("FETCH_MEMBER_DISPLAY", "1") == "1"
FALLBACK_MEMBER_FETCH = os.getenv("FALLBACK_MEMBER_FETCH", "1") == "1"
FALLBACK_SLEEP_SEC = float(os.getenv("FALLBACK_SLEEP_SEC", "0.15"))
FALLBACK_LOG_EVERY = int(os.getenv("FALLBACK_LOG_EVERY", "50"))

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <white>{message}</white>")

if not DISCORD_TOKEN:
    print("ERROR: DISCORD_TOKEN is empty (env DISCORD_TOKEN).")
    getch()
    raise SystemExit

if not GUILD_ID or not GUILD_ID.isdigit():
    print("ERROR: GUILD_ID is empty or not numeric (env GUILD_ID).")
    getch()
    raise SystemExit


class NoAccessError(Exception):
    pass


session = requests.Session()
session.headers.update({
    "authorization": DISCORD_TOKEN,
    "user-agent": "Mozilla/5.0",
    "accept-encoding": "gzip, deflate",
})


def get_json(url: str, max_retries: int = 8, timeout: int = 25):
    """Discord GET with retries and rate-limit handling."""
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)

            if r.status_code == 429:
                try:
                    ra = float(r.json().get("retry_after", 1.5))
                except Exception:
                    ra = 1.5
                sleep_s = max(ra, 0.2)
                logger.warning(f"429 rate limit. Sleep {sleep_s:.2f}s")
                time.sleep(sleep_s)
                continue

            if r.status_code == 403:
                try:
                    j = r.json()
                    if j.get("code") == 50001:
                        raise NoAccessError()
                except NoAccessError:
                    raise
                except Exception:
                    pass

            if r.status_code == 404:
                try:
                    j = r.json()
                    if j.get("code") in (10007, 10013):  # Unknown Member/User
                        return None
                except Exception:
                    if "/members/" in url:
                        return None

            if r.status_code >= 400:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                raise RuntimeError(last_err)

            return r.json()

        except NoAccessError:
            raise
        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                logger.error(f"GET failed (attempt {attempt}/{max_retries}): {url} | err={last_err}")
                raise
            sleep_s = min(2 ** attempt, 30)
            logger.warning(f"GET retry (attempt {attempt}/{max_retries}) in {sleep_s}s: {url} | err={last_err}")
            time.sleep(sleep_s)

    raise RuntimeError(last_err or "Unknown error")


def build_avatar_url(author: dict):
    uid = author.get("id")
    ah = author.get("avatar")
    if uid and ah:
        return f"https://cdn.discordapp.com/avatars/{uid}/{ah}.png?size=128"
    return None


def build_tagname(author: dict):
    """
    'тегнейм':
      - если discriminator есть и != "0": username#discriminator
      - иначе: username
    """
    username = (author.get("username") or "").strip()
    disc = str(author.get("discriminator") or "").strip()
    if username and disc and disc != "0":
        return f"{username}#{disc}"
    return username or None


def append_message_jsonl(record: dict):
    with open(MESSAGES_JSONL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            cp = json.load(f)
        logger.info(f"Loaded checkpoint: users={len(cp.get('users', {}))} channels={len(cp.get('channels', {}))}")
    else:
        cp = {"meta": {}, "channels": {}, "users": {}, "channel_names": {}, "member_display": {}}
        logger.info("No checkpoint found. Starting fresh.")

    cp.setdefault("meta", {})
    cp.setdefault("channels", {})
    cp.setdefault("users", {})
    cp.setdefault("channel_names", {})
    cp.setdefault("member_display", {})  # user_id -> server nickname/display name

    return cp


def save_checkpoint(cp, reason: str = ""):
    cp["meta"]["generated_at"] = datetime.utcnow().isoformat() + "Z"
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_PATH)
    logger.info(f"Checkpoint saved{(' ('+reason+')') if reason else ''}: {CHECKPOINT_PATH}")


def ensure_server_name(cp, guild_id: str):
    if cp["meta"].get("guild_name"):
        return
    g = get_json(f"{API_BASE}/guilds/{guild_id}")
    cp["meta"]["guild_id"] = str(guild_id)
    cp["meta"]["guild_name"] = g.get("name")
    logger.success(f"Guild: {cp['meta']['guild_name']} | guild_id={guild_id}")


def list_text_channels(guild_id: str):
    chans = get_json(f"{API_BASE}/guilds/{guild_id}/channels")
    out = []
    for c in chans:
        if c.get("type") == 0:  # text
            out.append({"id": str(c["id"]), "name": c.get("name")})
    logger.info(f"Fetched channels: total={len(chans)} | text={len(out)}")
    return out


def _compute_display_name(member: dict):
    # Prefer server nickname; then global_name; then username
    nick = member.get("nick")
    user = member.get("user") or {}
    global_name = user.get("global_name")
    username = user.get("username")
    return nick or global_name or username


def fetch_all_members_display(cp, guild_id: str):
    """
    Fetch all members using /members?limit=1000&after=...
    NOTE: can return 403 depending on token/permissions.
    """
    out_display = {}
    after = "0"
    total = 0
    pages = 0

    while True:
        url = f"{API_BASE}/guilds/{guild_id}/members?limit=1000&after={after}"
        data = get_json(url)
        if not data:
            break
        pages += 1

        for m in data:
            u = m.get("user") or {}
            uid = str(u.get("id"))
            if not uid:
                continue
            out_display[uid] = _compute_display_name(m)
            total += 1

        after = str((data[-1].get("user") or {}).get("id") or after)
        if pages % 5 == 0:
            logger.info(f"Members fetched: {total} (pages={pages})")
        time.sleep(0.2)

    cp["member_display"] = out_display
    logger.success(f"Fetched member display names: members={len(out_display)}")


def fetch_member_display_fallback(cp, guild_id: str, uid: str):
    try:
        m = get_json(f"{API_BASE}/guilds/{guild_id}/members/{uid}")
    except NoAccessError:
        return "no_access"
    except Exception:
        return "error"
    if not m:
        return "missing"
    cp.setdefault("member_display", {})[uid] = _compute_display_name(m)
    return "ok"


def run_display_fallback_for_seen_users(cp, guild_id: str, reason: str = ""):
    uids = list((cp.get("users") or {}).keys())
    total = len(uids)
    if total == 0:
        return

    processed = ok = missing = no_access = errors = 0
    started = time.time()
    last_log = started

    logger.info(
        f"[DISPLAY-FB] Start fallback for seen users: total={total} sleep={FALLBACK_SLEEP_SEC}s "
        f"{('(' + reason + ')') if reason else ''}"
    )

    for uid in uids:
        processed += 1
        st = fetch_member_display_fallback(cp, guild_id, uid)
        if st == "ok":
            ok += 1
        elif st == "missing":
            missing += 1
        elif st == "no_access":
            no_access += 1
        else:
            errors += 1

        now = time.time()
        if processed % FALLBACK_LOG_EVERY == 0 or (now - last_log) >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            speed = processed / elapsed
            eta_s = (total - processed) / speed if speed > 0 else 0
            logger.info(
                f"[DISPLAY-FB] {processed}/{total} ok={ok} missing={missing} no_access={no_access} errors={errors} "
                f"speed={speed:.2f} users/s eta={eta_s/60:.1f}m"
            )
            last_log = now

        if FALLBACK_SLEEP_SEC > 0:
            time.sleep(FALLBACK_SLEEP_SEC)

    elapsed = max(time.time() - started, 1e-6)
    logger.success(
        f"[DISPLAY-FB] Done: processed={processed} ok={ok} missing={missing} no_access={no_access} errors={errors} "
        f"avg={processed/elapsed:.2f} users/s time={elapsed/60:.1f}m"
    )


def ensure_user(cp, uid: str, tagname: str, pfp: str):
    users = cp["users"]
    if uid not in users:
        users[uid] = {
            "id": uid,
            "tagname": tagname,
            "pfp": pfp,
            "by_channel": {},
        }
    else:
        u = users[uid]
        if tagname:
            u["tagname"] = tagname
        if pfp:
            u["pfp"] = pfp


def commit_channel_aggregate(cp, channel_id: str, channel_agg: dict):
    for uid, info in channel_agg.items():
        ensure_user(cp, uid, info.get("tagname"), info.get("pfp"))
        u = cp["users"][uid]
        bc = u["by_channel"]
        bc[channel_id] = int(bc.get(channel_id, 0)) + int(info.get("count", 0))


def backfill_channel(cp, channel_id: str, channel_name: str):
    logger.info(f"[BACKFILL] Start #{channel_name} ({channel_id})")
    started = time.time()
    last_beat = started

    channel_agg = {}
    pages = 0
    scanned = 0

    try:
        first = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100")
    except NoAccessError:
        logger.warning(f"[BACKFILL] SKIP no access #{channel_name} ({channel_id})")
        cp["channels"][channel_id] = {
            "name": channel_name,
            "last_seen_id": None,
            "backfill_done": False,
            "skipped_no_access": True
        }
        return

    if not first:
        logger.warning(f"[BACKFILL] Empty #{channel_name}")
        cp["channels"][channel_id] = {"name": channel_name, "last_seen_id": None, "backfill_done": True}
        return

    newest_id = max(int(m["id"]) for m in first)

    def consume(messages):
        nonlocal scanned
        scanned += len(messages)
        for msg in messages:
            author = msg.get("author") or {}
            uid = author.get("id")
            if not uid:
                continue

            uid = str(uid)
            tagname = build_tagname(author)
            pfp = build_avatar_url(author)

            if uid not in channel_agg:
                channel_agg[uid] = {"tagname": tagname, "pfp": pfp, "count": 0}
            channel_agg[uid]["count"] += 1
            if tagname:
                channel_agg[uid]["tagname"] = tagname
            if pfp:
                channel_agg[uid]["pfp"] = pfp

            if LOG_MESSAGES:
                try:
                    append_message_jsonl({
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "user_id": uid,
                        "tagname": tagname,
                        "timestamp": msg.get("timestamp"),
                        "content": msg.get("content") or "",
                    })
                except Exception as e:
                    logger.warning(f"Failed to log message: {e}")

    consume(first)
    pages += 1
    last_before = first[-1]["id"]

    while True:
        now = time.time()
        if now - last_beat >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            logger.info(
                f"[BACKFILL] #{channel_name} pages={pages} scanned={scanned} uniq_users={len(channel_agg)} speed={scanned/elapsed:.2f} msg/s"
            )
            last_beat = now

        try:
            data = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100&before={last_before}")
        except NoAccessError:
            logger.warning(f"[BACKFILL] LOST access mid-channel, SKIP #{channel_name}")
            cp["channels"][channel_id] = {
                "name": channel_name,
                "last_seen_id": None,
                "backfill_done": False,
                "skipped_no_access": True
            }
            return

        if not data:
            break

        consume(data)
        pages += 1
        last_before = data[-1]["id"]

        if LOG_EVERY_PAGES and (pages % LOG_EVERY_PAGES == 0):
            elapsed = max(time.time() - started, 1e-6)
            logger.info(
                f"[BACKFILL] #{channel_name} pages={pages} scanned={scanned} uniq_users={len(channel_agg)} speed={scanned/elapsed:.2f} msg/s"
            )

    commit_channel_aggregate(cp, channel_id, channel_agg)
    cp["channels"][channel_id] = {"name": channel_name, "last_seen_id": str(newest_id), "backfill_done": True}

    elapsed = max(time.time() - started, 1e-6)
    logger.success(
        f"[BACKFILL] Done #{channel_name}: scanned={scanned} pages={pages} uniq_users={len(channel_agg)} avg={scanned/elapsed:.2f} msg/s"
    )


def incremental_channel(cp, channel_id: str, channel_name: str):
    st = cp["channels"].get(channel_id) or {"name": channel_name, "last_seen_id": None, "backfill_done": False}
    after_id = st.get("last_seen_id")
    if not after_id:
        return 0

    logger.info(f"[INCR] Start #{channel_name} after={after_id}")

    added = 0
    max_id = int(after_id)
    started = time.time()
    last_beat = started

    while True:
        now = time.time()
        if now - last_beat >= HEARTBEAT_SEC:
            elapsed = max(now - started, 1e-6)
            logger.info(f"[INCR] #{channel_name} added={added} speed={added/elapsed:.2f} msg/s")
            last_beat = now

        try:
            data = get_json(f"{API_BASE}/channels/{channel_id}/messages?limit=100&after={after_id}")
        except NoAccessError:
            logger.warning(f"[INCR] SKIP no access #{channel_name} ({channel_id})")
            cp["channels"][channel_id] = {**st, "skipped_no_access": True}
            break

        if not data:
            break

        for msg in data:
            author = msg.get("author") or {}
            uid = author.get("id")
            if not uid:
                continue
            uid = str(uid)

            tagname = build_tagname(author)
            pfp = build_avatar_url(author)

            ensure_user(cp, uid, tagname, pfp)
            u = cp["users"][uid]
            bc = u["by_channel"]
            bc[channel_id] = int(bc.get(channel_id, 0)) + 1

            if LOG_MESSAGES:
                try:
                    append_message_jsonl({
                        "channel_id": channel_id,
                        "channel_name": channel_name,
                        "user_id": uid,
                        "tagname": tagname,
                        "timestamp": msg.get("timestamp"),
                        "content": msg.get("content") or "",
                    })
                except Exception as e:
                    logger.warning(f"Failed to log message: {e}")

            mid = int(msg["id"])
            if mid > max_id:
                max_id = mid
            added += 1

        after_id = str(max_id)

    cp["channels"][channel_id] = {**st, "last_seen_id": str(max_id), "backfill_done": True}
    logger.success(f"[INCR] Done #{channel_name}: added={added} new_last_seen={max_id}")
    return added


def export_users(cp):
    channel_name_by_id = cp.get("channel_names", {})
    member_display = cp.get("member_display", {})

    out = []
    for uid, u in (cp.get("users") or {}).items():
        mc = {}
        for cid, n in (u.get("by_channel") or {}).items():
            cname = channel_name_by_id.get(cid) or cid
            mc[cname] = int(n)

        out.append({
            "id": u.get("id"),
            "ник_на_сервере": member_display.get(uid) or None,
            "тегнейм": u.get("tagname"),
            "pfp": u.get("pfp"),
            "количество_сообщений_в_разных_каналах": mc,
        })

    out.sort(key=lambda x: sum((x.get("количество_сообщений_в_разных_каналах") or {}).values()), reverse=True)

    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    logger.success(f"Export saved: {EXPORT_PATH} | users={len(out)}")


def main():
    logger.info(f"Boot | MODE={MODE} | guild_id={GUILD_ID}")

    cp = load_checkpoint()
    ensure_server_name(cp, GUILD_ID)

    # Cache channel names
    chans = list_text_channels(GUILD_ID)
    for c in chans:
        cp["channel_names"][str(c["id"])] = c.get("name") or str(c["id"])

    # Try fetch server nicknames (optional)
    bulk_display_ok = not FETCH_MEMBER_DISPLAY
    if FETCH_MEMBER_DISPLAY:
        try:
            fetch_all_members_display(cp, GUILD_ID)
            bulk_display_ok = True
        except Exception as e:
            logger.warning(f"Bulk member display fetch failed: {e}")

    if MODE == "backfill":
        for i, c in enumerate(chans, start=1):
            cid = str(c["id"])
            cname = c.get("name") or cid
            st = cp["channels"].get(cid) or {}
            if st.get("backfill_done") is True or st.get("skipped_no_access") is True:
                continue

            backfill_channel(cp, cid, cname)

            if i % 3 == 0:
                save_checkpoint(cp, reason="during backfill")

        # If bulk display fetch failed, fallback per-user (optional)
        if FETCH_MEMBER_DISPLAY and (not bulk_display_ok) and FALLBACK_MEMBER_FETCH and cp.get("users"):
            run_display_fallback_for_seen_users(cp, GUILD_ID, reason="bulk failed (backfill)")

        save_checkpoint(cp, reason="after backfill")
        export_users(cp)
        logger.success("Backfill complete.")
        return

    # incremental
    total_added = 0
    for c in chans:
        cid = str(c["id"])
        cname = c.get("name") or cid
        total_added += incremental_channel(cp, cid, cname)

    if FETCH_MEMBER_DISPLAY and (not bulk_display_ok) and FALLBACK_MEMBER_FETCH and cp.get("users"):
        run_display_fallback_for_seen_users(cp, GUILD_ID, reason="bulk failed (incremental)")

    save_checkpoint(cp, reason="after incremental")
    export_users(cp)
    logger.success(f"Incremental complete. added_messages={total_added}")


if __name__ == "__main__":
    main()