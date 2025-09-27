import os
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")

app = FastAPI(title="Nomangho YouTube Playlist API")

# CORS: 확장/클라이언트 접근 허용 (필요한 경우 도메인만 지정 가능)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 필요시 "https://sync-tube.de", "chrome-extension://<ID>" 등으로 좁히기
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# roomId -> playlistId (메모리 캐시; 영속 필요하면 DB로 교체)
room_playlists: dict[str, str] = {}

class SearchRequest(BaseModel):
    query: str
    maxResults: int = 5

class AddRequest(BaseModel):
    roomId: str
    roomTitle: str
    videoId: str

@app.get("/health")
async def health():
    return {"ok": True}

async def get_access_token() -> str:
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
        return resp.json()["access_token"]

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
        "status": {"privacyStatus": "unlisted"},
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            "https://www.googleapis.com/youtube/v3/playlists?part=snippet,status",
            headers={"Authorization": f"Bearer {token}"},
            json=body,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()["id"]

async def add_to_playlist(playlist_id: str, video_id: str):
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
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

@app.post("/add")
async def add_track(req: AddRequest):
    # 1) roomId 매핑 확인
    playlist_id = room_playlists.get(req.roomId)
    # 2) 없으면 새로 생성
    if not playlist_id:
        playlist_id = await create_playlist(req.roomTitle)
        room_playlists[req.roomId] = playlist_id
    # 3) 영상 추가
    await add_to_playlist(playlist_id, req.videoId)
    return {
        "status": "ok",
        "roomId": req.roomId,
        "playlistId": playlist_id,
        "playlistUrl": f"https://www.youtube.com/playlist?list={playlist_id}",
        "videoId": req.videoId,
    }
