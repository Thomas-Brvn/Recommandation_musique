"""
Récupération des covers + previews via l'API Deezer (publique, sans clé).
- Meilleure couverture des artistes francophones qu'iTunes
- Rate limit ~50 req/5s — bien plus généreux qu'iTunes (20/min)
- Fallback iTunes si Deezer ne trouve rien
"""
import asyncio
import time

import httpx

_DEEZER_URL  = "https://api.deezer.com/search"
_ITUNES_URL  = "https://itunes.apple.com/search"
_PLACEHOLDER = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 180 180'%3E"
    "%3Crect fill='%23282828' width='180' height='180'/%3E"
    "%3Ctext x='50%25' y='50%25' dominant-baseline='middle' text-anchor='middle' "
    "font-size='48' fill='%23444'%3E%E2%99%AA%3C/text%3E%3C/svg%3E"
)

_cache: dict[str, dict] = {}

# 1 requête externe à la fois, min 100ms d'écart (10/s max)
_lock = asyncio.Lock()
_last_call: float = 0.0
_MIN_INTERVAL = 0.1


async def _throttle():
    global _last_call
    elapsed = time.monotonic() - _last_call
    if elapsed < _MIN_INTERVAL:
        await asyncio.sleep(_MIN_INTERVAL - elapsed)
    _last_call = time.monotonic()


async def _deezer(client: httpx.AsyncClient, artist: str, title: str) -> dict:
    resp = await client.get(
        _DEEZER_URL,
        params={"q": f'artist:"{artist}" track:"{title}"', "limit": 1},
    )
    items = resp.json().get("data", [])
    if not items:
        # retry avec recherche simple
        resp = await client.get(
            _DEEZER_URL,
            params={"q": f"{artist} {title}", "limit": 1},
        )
        items = resp.json().get("data", [])
    if items:
        hit = items[0]
        cover = hit.get("album", {}).get("cover_xl") or hit.get("album", {}).get("cover_big") or ""
        preview = hit.get("preview") or None
        if cover:
            return {"url": cover, "preview_url": preview}
    return {}


async def _itunes(client: httpx.AsyncClient, artist: str, title: str) -> dict:
    resp = await client.get(
        _ITUNES_URL,
        params={"term": f"{artist} {title}", "entity": "song", "limit": 1},
    )
    results = resp.json().get("results", [])
    if results:
        hit = results[0]
        raw = hit.get("artworkUrl100", "")
        url = raw.replace("100x100bb", "600x600bb") if raw else ""
        preview = hit.get("previewUrl") or None
        if url:
            return {"url": url, "preview_url": preview}
    return {}


async def get_track_info(artist: str, title: str) -> dict:
    """Retourne {"url": cover_url, "preview_url": mp3_or_None}."""
    key = f"{artist}|{title}".lower()
    if key in _cache:
        return _cache[key]

    info = {"url": _PLACEHOLDER, "preview_url": None}

    async with _lock:
        if key in _cache:
            return _cache[key]
        await _throttle()
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                result = await _deezer(client, artist, title)
                if not result:
                    result = await _itunes(client, artist, title)
                if result:
                    info = result
        except Exception:
            pass

    _cache[key] = info
    return info


async def get_cover_url(artist: str, title: str) -> str:
    return (await get_track_info(artist, title))["url"]
