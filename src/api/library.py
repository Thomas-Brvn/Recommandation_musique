"""
Service de bibliothèque personnelle : likes et playlists.
Persistance via un fichier JSON local. Entièrement async.
"""
import asyncio
import json
import uuid
from pathlib import Path
from typing import Optional

DATA_FILE = Path(__file__).parent.parent.parent / "data" / "library.json"


class LibraryService:
    _instance: Optional["LibraryService"] = None

    def __init__(self):
        self._data: dict = {}
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "LibraryService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def load(self):
        if DATA_FILE.exists():
            raw = await asyncio.to_thread(DATA_FILE.read_text, encoding="utf-8")
            self._data = json.loads(raw)
            print(f"  - Bibliothèque: {DATA_FILE} ({len(self._data)} utilisateurs)")

    async def _save(self):
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(self._data, ensure_ascii=False, indent=2)
        await asyncio.to_thread(DATA_FILE.write_text, raw, encoding="utf-8")

    def _user(self, user_id: str) -> dict:
        if user_id not in self._data:
            self._data[user_id] = {"likes": [], "playlists": {}}
        return self._data[user_id]

    # ── Likes ────────────────────────────────────────────────────────────

    async def like(self, user_id: str, track: dict) -> None:
        async with self._lock:
            user = self._user(user_id)
            if not any(t["item_id"] == track["item_id"] for t in user["likes"]):
                user["likes"].append(track)
                await self._save()

    async def unlike(self, user_id: str, item_id: int) -> None:
        async with self._lock:
            user = self._user(user_id)
            user["likes"] = [t for t in user["likes"] if t["item_id"] != item_id]
            await self._save()

    async def get_likes(self, user_id: str) -> list:
        return self._user(user_id)["likes"]

    async def is_liked(self, user_id: str, item_id: int) -> bool:
        return any(t["item_id"] == item_id for t in self._user(user_id)["likes"])

    # ── Playlists ─────────────────────────────────────────────────────────

    async def create_playlist(self, user_id: str, name: str) -> dict:
        async with self._lock:
            playlist_id = uuid.uuid4().hex[:8]
            playlist = {"id": playlist_id, "name": name, "tracks": []}
            self._user(user_id)["playlists"][playlist_id] = playlist
            await self._save()
            return playlist

    async def get_playlists(self, user_id: str) -> list:
        return list(self._user(user_id)["playlists"].values())

    async def get_playlist(self, user_id: str, playlist_id: str) -> Optional[dict]:
        return self._user(user_id)["playlists"].get(playlist_id)

    async def rename_playlist(self, user_id: str, playlist_id: str, name: str) -> bool:
        async with self._lock:
            pl = self._user(user_id)["playlists"].get(playlist_id)
            if not pl:
                return False
            pl["name"] = name
            await self._save()
            return True

    async def delete_playlist(self, user_id: str, playlist_id: str) -> bool:
        async with self._lock:
            pls = self._user(user_id)["playlists"]
            if playlist_id not in pls:
                return False
            del pls[playlist_id]
            await self._save()
            return True

    async def add_to_playlist(self, user_id: str, playlist_id: str, track: dict) -> bool:
        async with self._lock:
            pl = self._user(user_id)["playlists"].get(playlist_id)
            if not pl:
                return False
            if not any(t["item_id"] == track["item_id"] for t in pl["tracks"]):
                pl["tracks"].append(track)
                await self._save()
            return True

    async def remove_from_playlist(self, user_id: str, playlist_id: str, item_id: int) -> bool:
        async with self._lock:
            pl = self._user(user_id)["playlists"].get(playlist_id)
            if not pl:
                return False
            pl["tracks"] = [t for t in pl["tracks"] if t["item_id"] != item_id]
            await self._save()
            return True
