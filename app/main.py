import os
import json
import re
import time
import uuid
import unicodedata
import logging
from urllib.parse import urlparse, parse_qs, urlencode
from typing import List, Optional, Dict
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from redis.asyncio import Redis
from dotenv import load_dotenv

class AddByUrlRequest(BaseModel):
    roomId: str
    roomTitle: str
    url: str

ENV_PATH = "/etc/nomangho.env"

for k in list(os.environ.keys()):
    if k.startswith("YOUTUBE_") or k in {"REDIS_URL", "HOST", "PORT"}:
        os.environ.pop(k)

load_dotenv(ENV_PATH, override=True)

def _setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logger = logging.getLogger("nomangho")
    logger.setLevel(getattr(logging, level, logging.INFO))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    return logger

log = _setup_logging()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

YT_CLIENTS = []
i = 1
while True:
    cid = os.getenv(f"YOUTUBE_CLIENT_ID{i}")
    csecret = os.getenv(f"YOUTUBE_CLIENT_SECRET{i}")
    if not (cid and csecret):
        break
    crefresh = os.getenv(f"YOUTUBE_REFRESH_TOKEN{i}")
    YT_CLIENTS.append({"id": cid, "secret": csecret, "refresh": crefresh})
    i += 1
if not YT_CLIENTS:
    raise RuntimeError("No YouTube client credentials configured")

YOUTUBE_API_KEYS = [s.strip() for s in os.getenv("YOUTUBE_API_KEYS", "").split(",") if s.strip()]
if YOUTUBE_API_KEYS:
    from itertools import cycle
    _api_key_cycle = cycle(YOUTUBE_API_KEYS)
else:
    _api_key_cycle = None

app = FastAPI(title="Nomangho YouTube Playlist API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis: Redis | None = None

_TRACK_RESOLVE_CACHE_PREFIX = "trackresolve:v1"
_TRACK_RESOLVE_TTL = int(timedelta(hours=12).total_seconds())
_TRACK_RESOLVE_MISS_TTL = int(timedelta(minutes=30).total_seconds())
_TRACK_RESOLVE_THRESHOLD = 0.58
_TRACK_RESOLVE_MISS_SENTINEL = "__MISS__"

class AddRequest(BaseModel):
    roomId: str
    roomTitle: str
    videoId: Optional[str] = None
    url: Optional[str] = None
    trackName: Optional[str] = None
    artist: Optional[str] = None
    durationSec: Optional[int] = Field(None, ge=5, le=60*60*3)

class SearchReq(BaseModel):
    query: str = Field(..., min_length=2, max_length=160)
    channel_hint: Optional[str] = Field(None, max_length=120)
    duration_hint_sec: Optional[int] = Field(None, ge=5, le=60*60*3)
    max_results: int = Field(8, ge=1, le=15)
    region: Optional[str] = "KR"
    lang: Optional[str] = "ko"

class Candidate(BaseModel):
    videoId: str
    title: str
    channelTitle: str
    channelId: str
    durationSec: Optional[int] = None
    score: float

class SearchRes(BaseModel):
    best: Optional[Candidate]
    alternatives: List[Candidate] = []
    reason: Optional[str] = None

@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())
    path = request.url.path
    method = request.method
    client = request.client.host if request.client else "-"
    try:
        body_bytes = await request.body()
        body_preview = body_bytes.decode("utf-8", errors="ignore")
        if len(body_preview) > 500:
            body_preview = body_preview[:500] + "...(truncated)"
    except:
        body_preview = ""
    start = time.time()
    log.info(json.dumps({"type": "request", "rid": rid, "method": method, "path": path, "client": client, "query": dict(request.query_params), "body": body_preview}))
    try:
        response = await call_next(request)
    except Exception as e:
        dur = round((time.time() - start) * 1000, 2)
        log.exception(json.dumps({"type": "error", "rid": rid, "method": method, "path": path, "duration_ms": dur, "error": str(e)}))
        raise
    dur = round((time.time() - start) * 1000, 2)
    log.info(json.dumps({"type": "response", "rid": rid, "method": method, "path": path, "status": response.status_code, "duration_ms": dur}))
    return response

@app.on_event("startup")
async def _startup():
    global redis
    log.info("startup: connecting redis")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        pong = await redis.ping()
        log.info(json.dumps({"type": "redis_ping", "ok": bool(pong)}))
    except Exception as e:
        log.error(json.dumps({"type": "redis_ping_failed", "error": str(e)}))

@app.on_event("shutdown")
async def _shutdown():
    if redis:
        log.info("shutdown: closing redis")
        await redis.aclose()

def _playlist_key(room_id: str) -> str:
    return f"room:{room_id}:playlist"

def _videos_key(room_id: str) -> str:
    return f"room:{room_id}:videos"

def _pending_key(room_id: str) -> str:
    return f"room:{room_id}:pending"

def _playlist_url(pid: str | None) -> str | None:
    return f"https://www.youtube.com/playlist?list={pid}" if pid else None

def _playlist_title(room_title: str | None, room_id: str) -> str:
    base_name = (room_title or "").strip() or (room_id or "").strip() or "Playlist"
    tz = None
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo("Asia/Seoul")
        except Exception:
            tz = None
    now = datetime.now(tz) if tz else datetime.utcnow()
    return f"{now.strftime('%y.%m.%d')} - {base_name}"

def _is_quota_exceeded(detail) -> bool:
    try:
        data = json.loads(detail) if isinstance(detail, str) else detail
        err = data.get("error", {})
        if err.get("code") == 403:
            for e in err.get("errors", []):
                if e.get("reason") == "quotaExceeded":
                    return True
    except Exception:
        return False
    return False

def _extract_video_id(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", s):
        return s
    try:
        u = urlparse(s)
        if u.scheme and u.netloc:
            qs = parse_qs(u.query)
            if "v" in qs and qs["v"]:
                v = qs["v"][0]
                if re.fullmatch(r"[A-Za-z0-9_-]{11}", v):
                    return v
            path = u.path or ""
            m = re.search(r"/(embed|shorts)/([A-Za-z0-9_-]{11})", path)
            if m:
                return m.group(2)
            m2 = re.search(r"/([A-Za-z0-9_-]{11})(?:\?|/|$)", path)
            if m2:
                return m2.group(1)
    except Exception:
        pass
    m3 = re.search(r"([A-Za-z0-9_-]{11})", s)
    return m3.group(1) if m3 else None

def _resolve_video_id(*candidates: str | None) -> Optional[str]:
    for raw in candidates:
        if not raw:
            continue
        vid = _extract_video_id(raw)
        if vid:
            return vid
    return None

async def get_current_index() -> int:
    idx = await redis.get("yt:client_index")
    return int(idx or 0)

async def set_current_index(idx: int):
    await redis.set("yt:client_index", idx % len(YT_CLIENTS))
    log.info(json.dumps({"type": "yt_client_index_set", "index": idx % len(YT_CLIENTS)}))

async def get_access_token() -> str:
    idx = await get_current_index()
    client = YT_CLIENTS[idx]
    token_key = f"yt:access_token:{idx}"
    token = await redis.get(token_key)
    if token:
        return token
    refresh = await redis.get(f"yt:refresh_token:{idx}") or client.get("refresh")
    if not refresh:
        log.error(json.dumps({"type": "yt_refresh_missing", "index": idx}))
        raise HTTPException(status_code=400, detail=f"No refresh_token for client index {idx+1}")
    async with httpx.AsyncClient(timeout=10) as client_http:
        resp = await client_http.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client["id"],
                "client_secret": client["secret"],
                "refresh_token": refresh,
                "grant_type": "refresh_token",
            },
        )
    if resp.status_code != 200:
        try:
            err = resp.json()
            if err.get("error") == "invalid_grant":
                await redis.delete(f"yt:refresh_token:{idx}")
                log.error(json.dumps({"type": "yt_refresh_invalid_grant", "index": idx}))
        except Exception:
            pass
        await set_current_index(idx + 1)
        log.error(json.dumps({
            "type": "yt_token_refresh_failed",
            "index": idx,
            "status": resp.status_code,
            "text": resp.text[:500]
        }))
        raise HTTPException(status_code=500, detail=f"Token refresh failed: {resp.text}")
    data = resp.json()
    token = data["access_token"]
    ttl = max(60, int(data.get("expires_in", 3600)) - 60)
    await redis.set(token_key, token, ex=ttl)
    log.info(json.dumps({"type": "yt_token_refreshed", "index": idx, "ttl": ttl}))
    return token

async def youtube_request(method: str, url: str, **kwargs):
    for _ in range(len(YT_CLIENTS)):
        try:
            token = await get_access_token()
            headers = kwargs.pop("headers", {})
            headers["Authorization"] = f"Bearer {token}"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.request(method, url, headers=headers, **kwargs)
            if resp.status_code == 403 and _is_quota_exceeded(resp.text):
                idx = await get_current_index()
                await set_current_index(idx + 1)
                log.warning(json.dumps({"type": "yt_quota_exceeded_rotate", "prev_index": idx}))
                continue
            log.info(json.dumps({"type": "yt_request", "method": method, "url": url, "status": resp.status_code}))
            return resp
        except HTTPException:
            idx = await get_current_index()
            await set_current_index(idx + 1)
            log.warning(json.dumps({"type": "yt_http_exception_rotate", "prev_index": idx}))
            continue
        except Exception as e:
            log.error(json.dumps({"type": "yt_request_error", "method": method, "url": url, "error": str(e)}))
            raise
    log.error(json.dumps({"type": "yt_all_clients_exhausted"}))
    raise HTTPException(status_code=429, detail="All clients quotaExceeded")

async def _acquire_lock(key: str, ttl: int = 20) -> bool:
    return bool(await redis.set(key, "1", ex=ttl, nx=True))

async def _release_lock(key: str):
    try:
        await redis.delete(key)
    except Exception:
        pass


@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/oauth/refresh-all")
async def oauth_refresh_all():
    results = []
    for idx in range(len(YT_CLIENTS)):
        try:
            token_key = f"yt:access_token:{idx}"
            await redis.delete(token_key)
            _ = await get_access_token()
            results.append({"index": idx, "ok": True})
        except Exception as e:
            results.append({"index": idx, "ok": False, "error": str(e)})
    return {"results": results}

@app.get("/auth/url")
async def auth_url(request: Request):
    raw = request.query_params.get("i")
    idx = int(raw) - 1 if raw and raw.isdigit() else await get_current_index()
    if idx < 0 or idx >= len(YT_CLIENTS):
        idx = 0
    client = YT_CLIENTS[idx]
    redirect_uri = os.getenv("YOUTUBE_REDIRECT_URI", "http://127.0.0.1:5000/oauth2/callback")
    params = {
        "client_id": client["id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/youtube",
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": str(idx)
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    log.info(json.dumps({
    "type": "auth_url_debug",
    "index": idx + 1,
    "client_id": client["id"],
    "client_id_head": client["id"][:20],
    "client_id_tail": client["id"][-24:],
    "redirect_uri": redirect_uri
    }))
    return {"url": url, "index": idx + 1}

@app.get("/oauth2/callback")
async def oauth2_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code:
        return {"error": "missing code"}
    if state is not None and state.isdigit():
        idx = int(state)
    else:
        idx = await get_current_index()
    client = YT_CLIENTS[idx]
    redirect_uri = os.getenv("YOUTUBE_REDIRECT_URI", "http://127.0.0.1:5000/oauth2/callback")
    async with httpx.AsyncClient(timeout=10) as client_http:
        resp = await client_http.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client["id"],
                "client_secret": client["secret"],
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            },
        )
    data = resp.json()
    if "refresh_token" in data:
        await redis.set(f"yt:refresh_token:{idx}", data["refresh_token"])
        log.info(json.dumps({"type": "oauth_store_refresh", "index": idx}))
    else:
        log.warning(json.dumps({"type": "oauth_no_refresh_token", "index": idx, "status": resp.status_code}))
    return {"index": idx + 1, **data}

@app.get("/oauth/callback")
async def oauth_callback_alias(request: Request):
    return await oauth2_callback(request)


async def create_playlist(title: str) -> str:
    body = {
        "snippet": {"title": title, "description": "Auto-created from SyncTube"},
        "status": {"privacyStatus": "public"},
    }
    resp = await youtube_request(
        "POST",
        "https://www.googleapis.com/youtube/v3/playlists?part=snippet,status",
        json=body,
    )
    if resp.status_code != 200:
        log.error(json.dumps({"type": "playlist_create_failed", "status": resp.status_code, "text": resp.text[:500]}))
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    pid = resp.json()["id"]
    log.info(json.dumps({"type": "playlist_created", "playlistId": pid, "title": title}))
    return pid

async def add_to_playlist_items(playlist_id: str, video_id: str):
    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {"kind": "youtube#video", "videoId": video_id},
        }
    }
    resp = await youtube_request(
        "POST",
        "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet",
        json=body,
    )
    if resp.status_code != 200:
        log.error(json.dumps({"type": "playlist_add_failed", "playlistId": playlist_id, "videoId": video_id, "status": resp.status_code, "text": resp.text[:500]}))
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    log.info(json.dumps({"type": "playlist_added", "playlistId": playlist_id, "videoId": video_id}))

async def ensure_playlist_id(room_id: str, room_title: str) -> str:
    pid = await redis.get(_playlist_key(room_id))
    if pid:
        return pid
    title = _playlist_title(room_title, room_id)
    pid = await create_playlist(title)
    await redis.set(_playlist_key(room_id), pid)
    log.info(json.dumps({"type": "playlist_cached", "roomId": room_id, "playlistId": pid, "title": title}))
    return pid

async def _process_add(room_id: str, room_title: str, video_id: str):
    videos_key = _videos_key(room_id)
    pending_key = _pending_key(room_id)
    lock_key = f"lock:add:{room_id}:{video_id}"

    pid = await redis.get(_playlist_key(room_id))
    if not pid:
        pid = await ensure_playlist_id(room_id, room_title)

    if not await _acquire_lock(lock_key, ttl=30):
        log.info(json.dumps({"type": "add_in_progress", "roomId": room_id, "videoId": video_id}))
        return {
            "status": "in_progress",
            "roomId": room_id,
            "playlistId": pid,
            "playlistUrl": _playlist_url(pid),
            "videoId": video_id,
        }

    try:
        try:
            if await _video_in_playlist(pid, video_id):
                await redis.sadd(videos_key, video_id)
                log.info(json.dumps({"type":"add_skipped_already_in_playlist","roomId":room_id,"playlistId":pid,"videoId":video_id}))
                return {
                    "status": "skipped",
                    "roomId": room_id,
                    "playlistId": pid,
                    "playlistUrl": _playlist_url(pid),
                    "videoId": video_id,
                }
        except HTTPException:
            pass

        await add_to_playlist_items(pid, video_id)

        await redis.sadd(videos_key, video_id)
        log.info(json.dumps({"type":"add_success","roomId":room_id,"playlistId":pid,"videoId":video_id}))
        return {
            "status": "added",
            "roomId": room_id,
            "playlistId": pid,
            "playlistUrl": _playlist_url(pid),
            "videoId": video_id,
        }

    except HTTPException as e:
        if e.status_code == 429 and "quotaExceeded" in str(e.detail):
            await redis.lpush(pending_key, video_id)
            log.warning(json.dumps({"type":"add_queued_quota","roomId":room_id,"videoId":video_id,"playlistId":pid}))
            return {
                "status": "queued",
                "reason": "quotaExceeded",
                "roomId": room_id,
                "playlistId": pid,
                "playlistUrl": _playlist_url(pid),
                "videoId": video_id,
            }
        log.error(json.dumps({"type":"add_failed_http","roomId":room_id,"videoId":video_id,"status":e.status_code,"detail":str(e.detail)[:500]}))
        raise
    except Exception as ex:
        log.exception(json.dumps({"type":"add_failed_exception","roomId":room_id,"videoId":video_id,"error":str(ex)}))
        raise
    finally:
        await _release_lock(lock_key)



def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = re.sub(r"[\[\(（【].*?[\]\)）】]", " ", s)
    s = re.sub(r"[^\w\uAC00-\uD7A3\u3040-\u30FF\u4E00-\u9FFF ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _jaro_winkler(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    s1, s2 = a, b
    r = max(len(s1), len(s2)) // 2 - 1
    flags1 = [False] * len(s1)
    flags2 = [False] * len(s2)
    m = 0
    t = 0
    for i, ch in enumerate(s1):
        for j in range(max(0, i - r), min(len(s2), i + r + 1)):
            if not flags2[j] and s2[j] == ch:
                flags1[i] = flags2[j] = True
                m += 1
                break
    if m == 0:
        return 0.0
    k = 0
    for i in range(len(s1)):
        if flags1[i]:
            while not flags2[k]:
                k += 1
            if s1[i] != s2[k]:
                t += 1
            k += 1
    t /= 2
    jw = (m / len(s1) + m / len(s2) + (m - t) / m) / 3
    l = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            l += 1
        else:
            break
    return jw + 0.1 * l * (1 - jw)

def _iso8601_to_sec(dur: str) -> Optional[int]:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", dur or "")
    if not m:
        return None
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s

async def _yt_search_with_key(http: httpx.AsyncClient, key: str, q: str, max_results: int, region: str, lang: str):
    params = {
        "key": key,
        "part": "snippet",
        "type": "video",
        "maxResults": max_results,
        "q": q,
        "regionCode": region,
        "relevanceLanguage": lang,
        "fields": "items(id/videoId,snippet/title,snippet/channelId,snippet/channelTitle)"
    }
    r = await http.get("https://www.googleapis.com/youtube/v3/search", params=params, timeout=10)
    r.raise_for_status()
    log.info(json.dumps({"type": "yt_search_key", "status": r.status_code, "q": q, "count": len(r.json().get("items", []))}))
    return r.json().get("items", [])

async def _yt_videos_with_key(http: httpx.AsyncClient, key: str, ids: List[str]):
    params = {
        "key": key,
        "part": "snippet,contentDetails",
        "id": ",".join(ids),
        "fields": "items(id,snippet/title,snippet/channelId,snippet/channelTitle,contentDetails/duration)"
    }
    r = await http.get("https://www.googleapis.com/youtube/v3/videos", params=params, timeout=10)
    r.raise_for_status()
    log.info(json.dumps({"type": "yt_videos_key", "status": r.status_code, "ids": len(ids)}))
    return r.json().get("items", [])

async def _yt_search_with_oauth(q: str, max_results: int, region: str, lang: str):
    params = {
        "part": "snippet",
        "type": "video",
        "maxResults": str(max_results),
        "q": q,
        "regionCode": region,
        "relevanceLanguage": lang,
        "fields": "items(id/videoId,snippet/title,snippet/channelId,snippet/channelTitle)"
    }
    resp = await youtube_request("GET", "https://www.googleapis.com/youtube/v3/search", params=params)
    if resp.status_code != 200:
        log.error(json.dumps({"type": "yt_search_oauth_failed", "status": resp.status_code, "text": resp.text[:500]}))
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json().get("items", [])
    log.info(json.dumps({"type": "yt_search_oauth", "count": len(data), "q": q}))
    return data

async def _yt_videos_with_oauth(ids: List[str]):
    params = {
        "part": "snippet,contentDetails",
        "id": ",".join(ids),
        "fields": "items(id,snippet/title,snippet/channelId,snippet/channelTitle,contentDetails/duration)"
    }
    resp = await youtube_request("GET", "https://www.googleapis.com/youtube/v3/videos", params=params)
    if resp.status_code != 200:
        log.error(json.dumps({"type": "yt_videos_oauth_failed", "status": resp.status_code, "text": resp.text[:500]}))
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json().get("items", [])
    log.info(json.dumps({"type": "yt_videos_oauth", "count": len(data)}))
    return data

def _score_candidate(
    q_norm: str,
    ch_norm: str,
    cand_title: str,
    cand_ch: str,
    dur_hint: Optional[int],
    dur_sec: Optional[int],
    track_norm: Optional[str] = None
) -> float:
    title_norm = _normalize(cand_title)
    chname_norm = _normalize(cand_ch)
    score = 0.0
    if track_norm and track_norm in title_norm:
        score += 0.55
    score += 0.35 * _jaro_winkler(q_norm, title_norm)
    if ch_norm and (ch_norm == chname_norm or ch_norm in chname_norm or chname_norm in ch_norm):
        score += 0.2
    if dur_hint and dur_sec:
        if 0.9 * dur_hint <= dur_sec <= 1.1 * dur_hint:
            score += 0.1
        else:
            score -= 0.05
    return score


async def _perform_search(req: SearchReq, *, skip_cache: bool = False, track_norm: Optional[str] = None) -> SearchRes:
    q_norm = _normalize(req.query)
    ch_norm = _normalize(req.channel_hint or "")
    if len(q_norm) < 2:
        log.warning(json.dumps({"type": "search_reject_short", "q_norm": q_norm}))
        raise HTTPException(400, detail="query too short after normalization")
    cache_key = f"ytsearch:v3:{req.region}:{req.lang}:{q_norm}|{ch_norm}|{req.max_results}|{track_norm or ''}"
    cached = None if skip_cache else await redis.get(cache_key)
    if cached:
        try:
            res = SearchRes(**json.loads(cached))
            log.info(json.dumps({"type": "search_cache_hit", "key": cache_key}))
            return res
        except Exception:
            await redis.delete(cache_key)
            log.warning(json.dumps({"type": "search_cache_corrupt", "key": cache_key}))
    if _api_key_cycle:
        key = next(_api_key_cycle)
        async with httpx.AsyncClient() as http:
            items = await _yt_search_with_key(http, key, req.query, req.max_results, req.region, req.lang)
            ids = [it["id"]["videoId"] for it in items if it.get("id", {}).get("videoId")]
            meta = await _yt_videos_with_key(http, key, ids) if ids else []
    else:
        items = await _yt_search_with_oauth(req.query, req.max_results, req.region, req.lang)
        ids = [it["id"]["videoId"] for it in items if it.get("id", {}).get("videoId")]
        meta = await _yt_videos_with_oauth(ids) if ids else []
    id2m: Dict[str, dict] = {m["id"]: m for m in meta}
    cands: List[Candidate] = []
    for it in items:
        vid = it["id"]["videoId"]
        m = id2m.get(vid)
        if not m:
            continue
        dur = _iso8601_to_sec(m.get("contentDetails", {}).get("duration"))
        score = _score_candidate(
            q_norm,
            ch_norm,
            m["snippet"]["title"],
            m["snippet"]["channelTitle"],
            req.duration_hint_sec,
            dur,
            track_norm=track_norm,
        )
        cands.append(Candidate(
            videoId=vid,
            title=m["snippet"]["title"],
            channelTitle=m["snippet"]["channelTitle"],
            channelId=m["snippet"]["channelId"],
            durationSec=dur,
            score=round(score, 4)
        ))
    cands.sort(key=lambda x: x.score, reverse=True)
    best = cands[0] if cands else None
    if not best or best.score < 0.62:
        res = SearchRes(best=None, alternatives=cands[:5], reason="ambiguous_or_low_score" if cands else "no_results")
    else:
        res = SearchRes(best=best, alternatives=cands[1:5])
    if not skip_cache:
        await redis.setex(cache_key, int(timedelta(hours=24).total_seconds()), json.dumps(res.dict()))
    log.info(json.dumps({"type": "search_done", "q": req.query, "best_score": float(best.score) if best else None, "alts": len(res.alternatives), "reason": res.reason}))
    return res

@app.post("/search", response_model=SearchRes)
async def search(req: SearchReq):
    return await _perform_search(req)

async def _resolve_track_candidate(track_name: str, artist: str, duration_hint: Optional[int]) -> Candidate:
    track_raw = (track_name or "").strip()
    artist_raw = (artist or "").strip()
    if not track_raw or not artist_raw:
        log.warning(json.dumps({"type": "resolve_missing_fields", "track": track_raw, "artist": artist_raw}))
        raise HTTPException(status_code=400, detail="trackName and artist required")
    track_norm = _normalize(track_raw)
    artist_norm = _normalize(artist_raw)
    if len(track_norm) < 2 or len(artist_norm) < 2:
        log.warning(json.dumps({"type": "resolve_too_short", "track_norm": track_norm, "artist_norm": artist_norm}))
        raise HTTPException(status_code=400, detail="trackName and artist required")
    cache_key = f"{_TRACK_RESOLVE_CACHE_PREFIX}:{track_norm}|{artist_norm}|{duration_hint or 0}"
    cached = await redis.get(cache_key)
    if cached:
        if cached == _TRACK_RESOLVE_MISS_SENTINEL:
            log.info(json.dumps({"type": "resolve_cache_miss_hit", "key": cache_key}))
            raise HTTPException(status_code=404, detail="matching video not found")
        try:
            data = json.loads(cached)
            log.info(json.dumps({"type": "resolve_cache_hit", "key": cache_key}))
            return Candidate(**data)
        except Exception:
            await redis.delete(cache_key)
            log.warning(json.dumps({"type": "resolve_cache_corrupt", "key": cache_key}))
    search_req = SearchReq(
        query=f"{track_raw} {artist_raw}",
        channel_hint=artist_raw,
        duration_hint_sec=duration_hint,
        max_results=8,
    )
    res = await _perform_search(search_req, skip_cache=True, track_norm=_normalize(track_raw))
    best = res.best
    if not best or best.score < _TRACK_RESOLVE_THRESHOLD:
        await redis.setex(cache_key, _TRACK_RESOLVE_MISS_TTL, _TRACK_RESOLVE_MISS_SENTINEL)
        log.info(json.dumps({"type": "resolve_no_confident_match", "key": cache_key, "best_score": float(best.score) if best else None}))
        raise HTTPException(status_code=404, detail="matching video not found")
    title_norm = _normalize(best.title)
    channel_norm = _normalize(best.channelTitle)
    title_similarity = _jaro_winkler(track_norm, title_norm)
    artist_similarity = _jaro_winkler(artist_norm, channel_norm)
    artist_present = artist_norm in title_norm or artist_norm in channel_norm or artist_similarity >= 0.85
    if title_similarity < 0.7 or not artist_present:
        await redis.setex(cache_key, _TRACK_RESOLVE_MISS_TTL, _TRACK_RESOLVE_MISS_SENTINEL)
        log.info(json.dumps({"type": "resolve_reject_similarity", "key": cache_key, "title_sim": title_similarity, "artist_sim": artist_similarity, "artist_present": artist_present}))
        raise HTTPException(status_code=404, detail="matching video not found")
    await redis.setex(cache_key, _TRACK_RESOLVE_TTL, json.dumps(best.dict()))
    log.info(json.dumps({"type": "resolve_ok", "key": cache_key, "videoId": best.videoId, "score": float(best.score)}))
    return best

@app.post("/add")
async def add_track(req: AddRequest):
    room_id = (req.roomId or "").strip()
    room_title = (req.roomTitle or "").strip()
    track_name = (req.trackName or "").strip()
    artist = (req.artist or "").strip()
    if not room_id or not track_name or not artist:
        log.warning(json.dumps({"type": "add_bad_request", "roomId": room_id, "trackName": track_name, "artist": artist}))
        raise HTTPException(status_code=400, detail="roomId/trackName/artist required")
    candidate = await _resolve_track_candidate(track_name, artist, req.durationSec)
    res = await _process_add(room_id, room_title, candidate.videoId)
    return res

@app.post("/add/url")
async def add_by_url(req: AddByUrlRequest):
    room_id = (req.roomId or "").strip()
    room_title = (req.roomTitle or "").strip()
    video_id = _resolve_video_id(req.url)
    if not room_id or not video_id:
        log.warning(json.dumps({"type": "add_url_bad_request", "roomId": room_id, "url": req.url}))
        raise HTTPException(status_code=400, detail="roomId/url required")
    res = await _process_add(room_id, room_title, video_id)
    return res

@app.post("/flush")
async def flush(roomId: str | None = None, maxOps: int = 100):
    rooms = [roomId] if roomId else []
    if not rooms:
        pattern = "room:*:pending"
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
            for k in keys:
                rid = k.split(":")[1]
                rooms.append(rid)
            if cursor == 0:
                break
        rooms = list(dict.fromkeys(rooms))
    processed = []
    for rid in rooms:
        pending_key = _pending_key(rid)
        videos_key = _videos_key(rid)
        pid = await ensure_playlist_id(rid, rid)
        ops = 0
        while ops < maxOps:
            vid = await redis.rpop(pending_key)
            if not vid:
                break
            lock_key = f"lock:add:{rid}:{vid}"
            got_lock = await redis.set(lock_key, "1", ex=30, nx=True)
            if not got_lock:
                await redis.rpush(pending_key, vid)
                continue
            try:
                try:
                    if await _video_in_playlist(pid, vid):
                        await redis.sadd(videos_key, vid)
                        processed.append({"roomId": rid, "videoId": vid, "status": "skipped"})
                        ops += 1
                        continue
                except HTTPException:
                    pass
                await add_to_playlist_items(pid, vid)
                await redis.sadd(videos_key, vid)
                processed.append({"roomId": rid, "videoId": vid, "status": "added"})
            except HTTPException as e:
                if e.status_code == 429 and "quotaExceeded" in str(e.detail):
                    await redis.rpush(pending_key, vid)
                    processed.append({"roomId": rid, "videoId": vid, "status": "queued"})
                    break
                await redis.srem(videos_key, vid)
                processed.append({"roomId": rid, "videoId": vid, "status": "error", "detail": str(e.detail)})
            finally:
                try:
                    await redis.delete(lock_key)
                except Exception:
                    pass
            ops += 1
        log.info(json.dumps({"type": "flush_room_done", "roomId": rid, "processed": len(processed)}))
    return {"processed": processed}

async def _video_in_playlist(playlist_id: str, video_id: str) -> bool:
    params = {
        "part": "snippet",
        "playlistId": playlist_id,
        "videoId": video_id,
        "maxResults": "1",
        "fields": "items/snippet/resourceId/videoId"
    }
    resp = await youtube_request("GET", "https://www.googleapis.com/youtube/v3/playlistItems", params=params)
    if resp.status_code != 200:
        log.error(json.dumps({"type": "playlist_check_failed", "playlistId": playlist_id, "videoId": video_id, "status": resp.status_code, "text": resp.text[:500]}))
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    items = data.get("items", [])
    return bool(items)

def _is_playlist_not_found(detail: str | dict) -> bool:
    try:
        data = json.loads(detail) if isinstance(detail, str) else detail
        err = data.get("error", {})
        if err.get("code") == 404:
            for e in err.get("errors", []):
                if e.get("reason") == "playlistNotFound":
                    return True
    except Exception:
        pass
    return False
