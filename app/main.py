import os
import json
import re
import unicodedata
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

class AddByUrlRequest(BaseModel):
    roomId: str
    roomTitle: str
    url: str

load_dotenv()

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

class AddRequest(BaseModel):
    roomId: str
    roomTitle: str
    videoId: Optional[str] = None
    url: Optional[str] = None

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

@app.on_event("startup")
async def _startup():
    global redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

@app.on_event("shutdown")
async def _shutdown():
    if redis:
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

async def get_access_token() -> str:
    idx = await get_current_index()
    client = YT_CLIENTS[idx]
    token_key = f"yt:access_token:{idx}"
    token = await redis.get(token_key)
    if token:
        return token
    refresh = await redis.get(f"yt:refresh_token:{idx}") or client.get("refresh")
    if not refresh:
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
        await set_current_index(idx + 1)
        raise HTTPException(status_code=500, detail=f"Token refresh failed: {resp.text}")
    data = resp.json()
    token = data["access_token"]
    ttl = max(60, int(data.get("expires_in", 3600)) - 60)
    await redis.set(token_key, token, ex=ttl)
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
                continue
            return resp
        except HTTPException:
            idx = await get_current_index()
            await set_current_index(idx + 1)
            continue
    raise HTTPException(status_code=429, detail="All clients quotaExceeded")

@app.get("/health")
async def health():
    return {"ok": True}

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
    return {"url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params), "index": idx + 1}

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
    return {"index": idx + 1, **data}

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
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()["id"]

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
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

async def ensure_playlist_id(room_id: str, room_title: str) -> str:
    pid = await redis.get(_playlist_key(room_id))
    if pid:
        return pid
    pid = await create_playlist(_playlist_title(room_title, room_id))
    await redis.set(_playlist_key(room_id), pid)
    return pid


async def _process_add(room_id: str, room_title: str, video_id: str):
    videos_key = _videos_key(room_id)
    pending_key = _pending_key(room_id)
    pre_added = await redis.sadd(videos_key, video_id)
    if pre_added == 0:
        pid = await redis.get(_playlist_key(room_id))
        return {
            "status": "skipped",
            "roomId": room_id,
            "playlistId": pid,
            "playlistUrl": _playlist_url(pid),
            "videoId": video_id,
        }

    pid = None
    try:
        pid = await ensure_playlist_id(room_id, room_title)
        await add_to_playlist_items(pid, video_id)
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
            pid = pid or await redis.get(_playlist_key(room_id))
            return {
                "status": "queued",
                "reason": "quotaExceeded",
                "roomId": room_id,
                "playlistId": pid,
                "playlistUrl": _playlist_url(pid),
                "videoId": video_id,
            }
        await redis.srem(videos_key, video_id)
        raise
    except Exception:
        await redis.srem(videos_key, video_id)
        raise

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
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json().get("items", [])

async def _yt_videos_with_oauth(ids: List[str]):
    params = {
        "part": "snippet,contentDetails",
        "id": ",".join(ids),
        "fields": "items(id,snippet/title,snippet/channelId,snippet/channelTitle,contentDetails/duration)"
    }
    resp = await youtube_request("GET", "https://www.googleapis.com/youtube/v3/videos", params=params)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json().get("items", [])

def _score_candidate(q_norm: str, ch_norm: str, cand_title: str, cand_ch: str, dur_hint: Optional[int], dur_sec: Optional[int]) -> float:
    title_norm = _normalize(cand_title)
    chname_norm = _normalize(cand_ch)
    score = 0.0
    if q_norm in title_norm:
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

@app.post("/search", response_model=SearchRes)
async def search(req: SearchReq):
    q_norm = _normalize(req.query)
    ch_norm = _normalize(req.channel_hint or "")
    if len(q_norm) < 2:
        raise HTTPException(400, detail="query too short after normalization")
    cache_key = f"ytsearch:v2:{req.region}:{req.lang}:{q_norm}|{ch_norm}|{req.max_results}"
    cached = await redis.get(cache_key)
    if cached:
        return SearchRes(**json.loads(cached))
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
        score = _score_candidate(q_norm, ch_norm, m["snippet"]["title"], m["snippet"]["channelTitle"], req.duration_hint_sec, dur)
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
    await redis.setex(cache_key, int(timedelta(hours=24).total_seconds()), json.dumps(res.dict()))
    return res

@app.post("/add")
async def add_track(req: AddRequest):
    room_id = (req.roomId or "").strip()
    room_title = (req.roomTitle or "").strip()
    video_id = _resolve_video_id(req.videoId, req.url)
    if not room_id or not video_id:
        raise HTTPException(status_code=400, detail="roomId and video identifier required")
    return await _process_add(room_id, room_title, video_id)

@app.post("/add/url")
async def add_by_url(req: AddByUrlRequest):
    room_id = (req.roomId or "").strip()
    room_title = (req.roomTitle or "").strip()
    video_id = _resolve_video_id(req.url)
    if not room_id or not video_id:
        raise HTTPException(status_code=400, detail="roomId/url required")
    return await _process_add(room_id, room_title, video_id)

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
            try:
                await add_to_playlist_items(pid, vid)
                await redis.sadd(videos_key, vid)
                processed.append({"roomId": rid, "videoId": vid, "status": "added"})
            except HTTPException as e:
                if e.status_code == 429 and "quotaExceeded" in str(e.detail):
                    await redis.rpush(pending_key, vid)
                    ops = maxOps
                    break
                await redis.srem(videos_key, vid)
                processed.append({"roomId": rid, "videoId": vid, "status": "error", "detail": str(e.detail)})
            ops += 1
    return {"processed": processed}
