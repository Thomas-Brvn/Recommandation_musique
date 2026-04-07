"""
Service de catalogue musical - charge et indexe les tracks depuis track_dedup_map.json.
Entièrement async : boto3 exécuté dans un thread via asyncio.to_thread.
"""
import asyncio
import json
from typing import List, Optional

import boto3


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

    async def load_from_s3(self, bucket: str, key: str, region: str):
        print(f"  - Catalogue: s3://{bucket}/{key}")
        raw = await asyncio.to_thread(self._fetch_s3, bucket, key, region)
        dedup_map: dict = json.loads(raw)
        self._build_catalog(dedup_map)
        print(f"Catalogue chargé: {len(self.tracks):,} tracks uniques")

    @staticmethod
    def _fetch_s3(bucket: str, key: str, region: str) -> bytes:
        s3 = boto3.client("s3", region_name=region)
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def _build_catalog(self, dedup_map: dict):
        canonical_names = sorted(set(dedup_map.values()))
        self.tracks = []
        for i, name in enumerate(canonical_names):
            if " - " in name:
                artist, title = name.split(" - ", 1)
            else:
                artist, title = "Unknown", name
            self.tracks.append(
                {
                    "id": i,
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
