"""
Service de catalogue musical - charge et indexe les tracks depuis track_dedup_map.json.
Entièrement async : google-cloud-storage exécuté dans un thread via asyncio.to_thread.
"""
import asyncio
import json
from typing import List, Optional

from google.cloud import storage


class CatalogService:
    _instance: Optional["CatalogService"] = None

    def __init__(self):
        self.tracks: List[dict] = []
        self.is_loaded: bool = False

    @classmethod
    def get_instance(cls) -> "CatalogService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def load_from_gcs(self, bucket: str, key: str, project: str,
                            mappings_key: str = "processed/mappings.json"):
        print(f"  - Catalogue: gs://{bucket}/{key}")
        raw, mappings_raw = await asyncio.gather(
            asyncio.to_thread(self._fetch_gcs, bucket, key, project),
            asyncio.to_thread(self._fetch_gcs, bucket, mappings_key, project),
        )
        dedup_map: dict = json.loads(raw)
        track_to_id: dict = json.loads(mappings_raw).get("track_to_id", {})
        self._build_catalog(dedup_map, track_to_id)
        print(f"Catalogue chargé: {len(self.tracks):,} tracks (alignés sur le modèle)")

    @staticmethod
    def _fetch_gcs(bucket: str, key: str, project: str) -> bytes:
        client = storage.Client(project=project)
        return client.bucket(bucket).blob(key).download_as_bytes()

    def _build_catalog(self, dedup_map: dict, track_to_id: dict):
        canonical_names = sorted(set(dedup_map.values()))
        self.tracks = []
        for name in canonical_names:
            item_id = track_to_id.get(name)
            if item_id is None:
                continue  # track absente du modèle → on ne l'affiche pas
            if " - " in name:
                artist, title = name.split(" - ", 1)
            else:
                artist, title = "Unknown", name
            self.tracks.append(
                {
                    "id": item_id,
                    "canonical_name": name,
                    "artist": artist.strip(),
                    "title": title.strip(),
                }
            )
        self.is_loaded = True

    def search(self, query: str, limit: int = 24) -> List[dict]:
        q = query.lower()
        return [t for t in self.tracks if q in t["canonical_name"].lower()][:limit]

    def get_page(self, page: int = 0, size: int = 48) -> List[dict]:
        start = page * size
        return self.tracks[start : start + size]

    def total(self) -> int:
        return len(self.tracks)
