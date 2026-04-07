"""
Récupération des covers d'albums via l'iTunes Search API.
Entièrement async avec httpx. Cache dict en mémoire.
"""
from typing import Optional

import httpx

_ITUNES_URL = "https://itunes.apple.com/search"
_PLACEHOLDER = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 180 180'%3E%3Crect fill='%23282828' width='180' height='180'/%3E%3Ctext x='50%25' y='50%25' dominant-baseline='middle' text-anchor='middle' font-size='48' fill='%23444'%3E%E2%99%AA%3C/text%3E%3C/svg%3E"

_cache: dict[str, str] = {}


async def get_cover_url(artist: str, title: str) -> str:
    """
    Retourne l'URL de la cover (600x600) via iTunes Search API.
    Résultat mis en cache en mémoire.
    """
    key = f"{artist}|{title}".lower()
    if key in _cache:
        return _cache[key]

    url = _PLACEHOLDER
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                _ITUNES_URL,
                params={"term": f"{artist} {title}", "entity": "song", "limit": 1},
            )
            results = resp.json().get("results", [])
            if results:
                raw = results[0].get("artworkUrl100", "")
                if raw:
                    url = raw.replace("100x100bb", "600x600bb")
    except Exception:
        pass

    _cache[key] = url
    return url
