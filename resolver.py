"""
resolver.py — 4-tier AniList→TMDB ID resolver.

Tries multiple sources in order until we find a TMDB ID:
  Tier 1: Fribb static lookup (O(1), ~39% of AniList)
  Tier 2: TMDB /find by TVDB or IMDb ID (catches ~1% more)
  Tier 3: TMDB /search by name + year (catches new anime like Tomb Raider King)
  Tier 4: Graceful "not on TMDB" (return None, frontend uses AniList fallback)

Caches results for 30 days (ID mappings rarely change).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from cache import tmdb_id_cache
from sources import fribb, tmdb, anilist

log = logging.getLogger("resolver")


async def resolve_tmdb_id(anilist_id: int, anilist_data: Optional[dict] = None) -> dict:
    """
    Resolve an AniList ID to a TMDB ID + type.
    Returns: {
        "tmdb_type": "tv" | "movie" | None,
        "tmdb_id": int | list[int] | None,
        "method": "fribb" | "tmdb_find" | "tmdb_search" | "not_found",
        "fribb_data": {...} | None,  # full Fribb entry for mappings
    }
    """
    cache_key = f"tmdb_id:{anilist_id}"
    cached = tmdb_id_cache.get(cache_key)
    if cached:
        return cached

    result = {"tmdb_type": None, "tmdb_id": None, "method": "not_found", "fribb_data": None}

    # Tier 1: Fribb static lookup
    fribb_data = fribb.lookup(anilist_id)
    if fribb_data:
        result["fribb_data"] = fribb_data
        tmdb_info = fribb_data.get("themoviedb_id") or {}
        if tmdb_info.get("tv"):
            result["tmdb_type"] = "tv"
            result["tmdb_id"] = tmdb_info["tv"]
            result["method"] = "fribb"
            tmdb_id_cache.set(cache_key, result)
            return result
        if tmdb_info.get("movie"):
            result["tmdb_type"] = "movie"
            result["tmdb_id"] = tmdb_info["movie"]
            result["method"] = "fribb"
            tmdb_id_cache.set(cache_key, result)
            return result

    # Get AniList data if not provided (for title + year + format)
    if anilist_data is None:
        anilist_data = await anilist.fetch_anilist(anilist_id)

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Tier 2: TMDB /find by TVDB ID or IMDb ID (from Fribb data)
        if fribb_data:
            tvdb_id = fribb_data.get("thetvdb_id")
            if tvdb_id:
                found = await tmdb.find_by_external_id(str(tvdb_id), "tvdb_id", client)
                if found:
                    result["tmdb_type"], result["tmdb_id"] = found
                    result["method"] = "tmdb_find_tvdb"
                    tmdb_id_cache.set(cache_key, result)
                    return result

            imdb_ids = fribb_data.get("imdb_id") or []
            if imdb_ids:
                found = await tmdb.find_by_external_id(imdb_ids[0], "imdb_id", client)
                if found:
                    result["tmdb_type"], result["tmdb_id"] = found
                    result["method"] = "tmdb_find_imdb"
                    tmdb_id_cache.set(cache_key, result)
                    return result

        # Tier 3: TMDB /search by name + year
        if anilist_data:
            title_en = anilist_data.get("title", {}).get("english")
            title_romaji = anilist_data.get("title", {}).get("romaji")
            year = (anilist_data.get("startDate") or {}).get("year")
            fmt = anilist_data.get("format", "TV")
            # Branch on format: MOVIE → /search/movie, else → /search/tv
            media_type = "movie" if fmt == "MOVIE" else "tv"

            for title in [title_en, title_romaji]:
                if not title:
                    continue
                tmdb_id = await tmdb.search(title, media_type, year, client)
                if tmdb_id:
                    result["tmdb_type"] = media_type
                    result["tmdb_id"] = tmdb_id
                    result["method"] = f"tmdb_search_{media_type}"
                    # Cache search results for shorter time (1 day) — they might be wrong
                    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)
                    return result

    # Tier 4: Not on TMDB
    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)  # cache negative result for 1 day
    return result
