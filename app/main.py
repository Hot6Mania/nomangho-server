import os
import json
import re
from urllib.parse import urlparse, parse_qs, urlencode

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

YT_CLIENTS = []
i = 1
while True:
    cid = os.getenv(f"YOUTUBE_CLIENT_ID{i}")
    csecret = os.getenv(f"YOUTUBE_CLIENT_SECRET{i}")
    crefresh = os.getenv(f"YOUTUBE_REFRESH_TOKEN{i}")
    if not (cid and csecret and crefresh):
        break
    YT_CLIENTS.append({"id": cid, "secret": csecret, "refresh": crefresh})
    i += 1

if not YT_CLIENTS:
    raise RuntimeError("No YouTube client credentials configured")

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
    videoId: str

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
    async with httpx.AsyncClient(timeout=10) as client_http:
        resp = await client_http.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": client["id"],
                "client_secret": client["secret"],
                "refresh_token": client["refresh"],
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
async def auth_url():
    idx = await get_current_index()
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
    }
    return {"url": "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)}

@app.get("/oauth2/callback")
async def oauth2_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        return {"error": "missing code"}
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
    return data

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
    pid = await create_playlist(room_title or room_id)
    await redis.set(_playlist_key(room_id), pid)
    return pid

@app.post("/add")
async def add_track(req: AddRequest):
    room_id = (req.roomId or "").strip()
    room_title = (req.roomTitle or "").strip()
    raw = (req.videoId or "").strip()
    video_id = _extract_video_id(raw)
    if not room_id or not video_id:
        raise HTTPException(status_code=400, detail="roomId/videoId required")
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
            pid = await redis.get(_playlist_key(room_id))
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
                processed.append(
                    {"roomId": rid, "videoId": vid, "status": "error", "detail": str(e.detail)}
                )
            ops += 1
    return {"processed": processed}
