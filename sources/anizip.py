"""
sources/anizip.py — AniZip client for TVDB episode metadata + images.

AniZip (api.ani.zip) is a free public service that maps AniList IDs to
TheTVDB series IDs and returns TVDB artwork URLs (public CDN, no auth).
No API key, no rate limit (up to 100 concurrent).

Returns:
- TVDB images: Banner, Poster, Fanart, Clearlogo
- Episode titles (multi-language), thumbnails, summaries, air dates
- Cross-refs: MAL, Kitsu, AniDB, IMDB, TVDB IDs

Used as Tier 1 for TVDB images (better quality than TMDB for some anime)
and as fallback episode data when TMDB has nothing.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional

import httpx

log = logging.getLogger("anizip")

ANIZIP_API = "https://api.ani.zip/mappings"
TIMEOUT = 15.0


async def fetch_anizip(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """
    Fetch AniZip mappings for an AniList ID.
    Returns normalized dict. Never raises.
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        res = await client.get(
            ANIZIP_API,
            params={"anilist_id": anilist_id},
            headers={"Accept": "application/json", "User-Agent": "anime-metadata-api/1.0"},
        )
        if res.status_code != 200:
            log.warning("AniZip %d: HTTP %d", anilist_id, res.status_code)
            return _empty(anilist_id)
        raw = res.json()
        if not raw or "episodes" not in raw:
            return _empty(anilist_id)
        return _normalize(raw, anilist_id)
    except Exception as e:
        log.warning("AniZip %d failed: %s", anilist_id, e)
        return _empty(anilist_id)
    finally:
        if close:
            await client.aclose()


def _empty(anilist_id: int) -> dict:
    return {
        "anilist_id": anilist_id,
        "title": "",
        "titleJa": "",
        "images": [],
        "episodes": [],
        "mappings": {},
        "error": "no data",
    }


def _normalize(raw: dict, anilist_id: int) -> dict:
    """Convert AniZip response to our standard format."""
    titles = raw.get("titles", {}) or {}
    episodes_map = raw.get("episodes", {}) or {}
    images = raw.get("images", []) or []
    mappings = raw.get("mappings", {}) or {}

    # Build episodes list (only S01E* — skip specials)
    episodes = []
    today = time.strftime("%Y-%m-%d")
    for ep_key in sorted(episodes_map.keys(), key=lambda k: int(k) if k.isdigit() else 99999):
        ep = episodes_map[ep_key]
        if ep.get("seasonNumber", 1) != 1:
            continue
        num = ep.get("absoluteEpisodeNumber") or ep.get("episodeNumber")
        if num is None:
            continue
        ep_titles = ep.get("title", {}) or {}
        air_date = ep.get("airDate", "") or ""
        episodes.append({
            "id": f"{anilist_id}-{num}",
            "number": num,
            "title": ep_titles.get("en") or ep_titles.get("x-jat") or "",
            "titleJa": ep_titles.get("ja") or "",
            "description": ep.get("overview", "") or "",
            "image": ep.get("image", "") or "",
            "airDate": air_date,
            "duration": ep.get("runtime", 0) or 0,
            "isFiller": bool(ep.get("filler", False)),
            "rating": str(ep.get("rating")) if ep.get("rating") else None,
            "hasAired": bool(air_date) and air_date <= today,
            "source": "anizip",
        })

    # Normalize images — add source tag
    norm_images = []
    for img in images:
        norm_images.append({
            "coverType": img.get("coverType", ""),
            "url": img.get("url", ""),
            "source": "tvdb",
        })

    return {
        "anilist_id": anilist_id,
        "title": titles.get("en") or titles.get("x-jat") or "",
        "titleJa": titles.get("ja") or "",
        "images": norm_images,
        "episodes": episodes,
        "mappings": mappings,
    }
