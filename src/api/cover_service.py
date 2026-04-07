"""
Récupération des covers d'albums via l'iTunes Search API.
Entièrement async avec httpx. Cache dict en mémoire.
"""
from typing import Optional

import httpx

_ITUNES_URL = "https://itunes.apple.com/search"
_PLACEHOLDER = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 180 180'%3E%3Crect fill='%23282828' width='180' height='180'/%3E%3Ctext x='50%25' y='50%25' dominant-baseline='middle' text-anchor='middle' font-size='48' fill='%23444'%3E%E2%99%AA%3C/text%3E%3C/svg%3E"

_cache: dict[str, dict] = {}


async def get_track_info(artist: str, title: str) -> dict:
    """
    Retourne {"url": cover_url, "preview_url": preview_mp3_or_None}
    via iTunes Search API. Résultat mis en cache en mémoire.
    """
    key = f"{artist}|{title}".lower()
    if key in _cache:
        return _cache[key]

    info = {"url": _PLACEHOLDER, "preview_url": None}
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

    _cache[key] = info
    return info


async def get_cover_url(artist: str, title: str) -> str:
    """Retourne uniquement l'URL de la cover (compat)."""
    return (await get_track_info(artist, title))["url"]
