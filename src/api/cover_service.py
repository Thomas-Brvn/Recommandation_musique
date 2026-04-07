"""
Récupération des covers d'albums via l'iTunes Search API.
Entièrement async avec httpx. Cache dict en mémoire.
Rate-limit : 1 requête iTunes toutes les 200ms (5/s max) via un sémaphore global.
"""
import asyncio
import time
from typing import Optional

import httpx

_ITUNES_URL = "https://itunes.apple.com/search"
_PLACEHOLDER = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 180 180'%3E%3Crect fill='%23282828' width='180' height='180'/%3E%3Ctext x='50%25' y='50%25' dominant-baseline='middle' text-anchor='middle' font-size='48' fill='%23444'%3E%E2%99%AA%3C/text%3E%3C/svg%3E"

_cache: dict[str, dict] = {}

# Throttle : 1 requête à la fois vers iTunes, min 200ms entre chaque
_itunes_lock = asyncio.Lock()
_last_itunes_call: float = 0.0
_MIN_INTERVAL = 0.2  # secondes


async def get_track_info(artist: str, title: str) -> dict:
    """
    Retourne {"url": cover_url, "preview_url": preview_mp3_or_None}
    via iTunes Search API. Résultat mis en cache en mémoire.
    """
    key = f"{artist}|{title}".lower()
    if key in _cache:
        return _cache[key]

    info = {"url": _PLACEHOLDER, "preview_url": None}

    async with _itunes_lock:
        # Re-check cache (une autre coroutine a peut-être déjà récupéré ce track)
        if key in _cache:
            return _cache[key]

        # Respecter l'intervalle minimum entre requêtes iTunes
        global _last_itunes_call
        elapsed = time.monotonic() - _last_itunes_call
        if elapsed < _MIN_INTERVAL:
            await asyncio.sleep(_MIN_INTERVAL - elapsed)

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    _ITUNES_URL,
                    params={"term": f"{artist} {title}", "entity": "song", "limit": 1},
                )
                results = resp.json().get("results", [])
                if results:
                    hit = results[0]
                    raw = hit.get("artworkUrl100", "")
                    if raw:
                        info["url"] = raw.replace("100x100bb", "600x600bb")
                    info["preview_url"] = hit.get("previewUrl") or None
        except Exception:
            pass
        finally:
            _last_itunes_call = time.monotonic()

    _cache[key] = info
    return info


async def get_cover_url(artist: str, title: str) -> str:
    """Retourne uniquement l'URL de la cover (compat)."""
    return (await get_track_info(artist, title))["url"]
