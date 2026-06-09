"""
Service de catalogue musical - charge et indexe les tracks depuis track_dedup_map.json.
Entièrement async : google-cloud-storage exécuté dans un thread via asyncio.to_thread.
"""
import asyncio
import json
import unicodedata
from typing import List, Optional

from google.cloud import storage
from rapidfuzz import fuzz, process


def _normalize(s: str) -> str:
    """Minuscules + suppression des accents."""
    return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode()


class CatalogService:
    _instance: Optional["CatalogService"] = None

    def __init__(self):
        self.tracks: List[dict] = []
        self._artist_index: dict[str, List[int]] = {}  # artist_norm → [track indices]
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
        self._artist_index = {}
        for idx, name in enumerate(canonical_names):
            item_id = track_to_id.get(name)
            if item_id is None:
                continue
            if " - " in name:
                artist, title = name.split(" - ", 1)
            else:
                artist, title = "Unknown", name
            artist = artist.strip()
            title  = title.strip()
            self.tracks.append({
                "id": item_id,
                "canonical_name": name,
                "artist": artist,
                "title": title,
                "_artist_norm": _normalize(artist),
                "_title_norm":  _normalize(title),
            })
            key = _normalize(artist)
            self._artist_index.setdefault(key, []).append(len(self.tracks) - 1)
        self.is_loaded = True

    def search(self, query: str, limit: int = 48) -> List[dict]:
        if not query.strip():
            return []

        q = _normalize(query)
        results: dict[int, float] = {}  # track_index → best score

        # 1. Substring exact sur artiste ou titre (score 100)
        for i, t in enumerate(self.tracks):
            if q in t["_artist_norm"] or q in t["_title_norm"]:
                results[i] = 100.0

        # 2. Fuzzy sur les noms d'artistes uniques si peu de résultats exacts
        if len(results) < limit:
            unique_artists = list(self._artist_index.keys())
            matches = process.extract(q, unique_artists, scorer=fuzz.WRatio,
                                      limit=10, score_cutoff=60)
            for artist_norm, score, _ in matches:
                for idx in self._artist_index[artist_norm]:
                    if idx not in results:
                        results[idx] = score * 0.9  # léger malus vs exact

        # 3. Fuzzy sur canonical_name si encore peu de résultats
        if len(results) < 5:
            candidates = [(i, t["_artist_norm"] + " " + t["_title_norm"])
                          for i, t in enumerate(self.tracks)]
            # Limiter la recherche fuzzy globale (trop coûteux sur 450k tracks)
            # On prend les 2000 premiers candidats qui contiennent au moins un token
            q_tokens = q.split()
            pool = [
                (i, text) for i, text in candidates
                if any(tok in text for tok in q_tokens)
            ][:2000]
            if pool:
                indices, texts = zip(*pool)
                matches = process.extract(q, texts, scorer=fuzz.WRatio,
                                          limit=20, score_cutoff=55)
                for _, score, pos in matches:
                    idx = indices[pos]
                    if idx not in results:
                        results[idx] = score * 0.8

        # Tri : score desc, puis artiste alphabétique
        sorted_indices = sorted(results, key=lambda i: (-results[i], self.tracks[i]["_artist_norm"]))

        out = []
        for i in sorted_indices[:limit]:
            t = self.tracks[i]
            out.append({k: v for k, v in t.items() if not k.startswith("_")})
        return out

    def get_page(self, page: int = 0, size: int = 48) -> List[dict]:
        start = page * size
        return self.tracks[start : start + size]

    def total(self) -> int:
        return len(self.tracks)
