import os
import json
import re
from urllib.parse import urlparse, parse_qs

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()

CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = FastAPI(title="Nomangho YouTube Playlist API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis: Redis | None = None

class SearchRequest(BaseModel):
    query: str
    maxResults: int = 5

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

async def get_access_token() -> str:
    token = await redis.get("yt:access_token")
    if token:
        return token
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": REFRESH_TOKEN,
                "grant_type": "refresh_token",
            },
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Token refresh failed: {resp.text}")
        data = resp.json()
        token = data["access_token"]
        ttl = max(60, int(data.get("expires_in", 3600)) - 60)
        await redis.set("yt:access_token", token, ex=ttl)
        return token

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/oauth2/callback")
async def oauth2_callback(request: Request):
    return {"query_params": dict(request.query_params)}

@app.post("/search")
async def search_videos(req: SearchRequest):
    token = await get_access_token()
    params = {
        "part": "snippet",
        "q": req.query,
        "type": "video",
        "maxResults": max(1, min(req.maxResults, 10)),
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            "https://www.googleapis.com/youtube/v3/search",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )
        if resp.status_code != 200:
            if resp.status_code == 403 and _is_quota_exceeded(resp.text):
                raise HTTPException(status_code=429, detail="YouTube quotaExceeded")
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        items = resp.json().get("items", [])
        results = []
        for it in items:
            results.append({
                "videoId": it["id"]["videoId"],
                "title": it["snippet"]["title"],
                "channel": it["snippet"]["channelTitle"],
                "thumbnail": (it["snippet"].get("thumbnails", {}).get("default", {}) or {}).get("url"),
            })
        return {"query": req.query, "results": results}

async def create_playlist(title: str) -> str:
    token = await get_access_token()
    body = {
        "snippet": {"title": title, "description": "Auto-created from SyncTube"},
        "status": {"privacyStatus": "public"},
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            "https://www.googleapis.com/youtube/v3/playlists?part=snippet,status",
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )
        if resp.status_code != 200:
            if resp.status_code == 403 and _is_quota_exceeded(resp.text):
                raise HTTPException(status_code=429, detail="YouTube quotaExceeded")
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()["id"]

async def add_to_playlist_items(playlist_id: str, video_id: str):
    token = await get_access_token()
    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {"kind": "youtube#video", "videoId": video_id},
        }
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            "https://www.googleapis.com/youtube/v3/playlistItems?part=snippet",
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )
        if resp.status_code != 200:
            if resp.status_code == 403 and _is_quota_exceeded(resp.text):
                raise HTTPException(status_code=429, detail="YouTube quotaExceeded")
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
        if e.status_code == 429 and str(e.detail) == "YouTube quotaExceeded":
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
                if e.status_code == 429 and str(e.detail) == "YouTube quotaExceeded":
                    await redis.rpush(pending_key, vid)
                    ops = maxOps
                    break
                await redis.srem(videos_key, vid)
                processed.append({"roomId": rid, "videoId": vid, "status": "error", "detail": str(e.detail)})
            ops += 1
    return {"processed": processed}
