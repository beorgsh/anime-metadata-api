"""
resolver.py — Multi-source AniList→TMDB ID resolver.

Resolution tiers (in order):

  Tier 1 — AniBridge cross-provider mapping (live API, cross-DB verified)
            We look up "anilist:{id}" → fetch relationships → if any
            relationship target is "tmdb_show:{tid}" or "tmdb_movie:{tid}",
            we have our TMDB ID from a community-verified source.

  Tier 2 — Fribb static lookup (in-memory O(1), ~39% AniList coverage)
            The original AniList↔TMDB mapping dataset. Still useful as a
            secondary fallback because its dataset is large and well-maintained.

  Tier 3 — TMDB /find by external ID (TVDB or IMDb, sourced from AniBridge/Fribb)
            Catches ~1% more — useful when the mapping DBs know a TVDB/IMDB
            ID but not the TMDB ID directly.

  Tier 4 — TMDB /search by name + year (catches brand-new anime like Tomb
            Raider King that haven't propagated to mapping DBs yet)

  Tier 5 — Graceful "not on TMDB" (return None — frontend uses AniList fallback)

Caches results for 30 days (ID mappings rarely change).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from cache import tmdb_id_cache
from sources import fribb, tmdb, anilist, anibridge

log = logging.getLogger("resolver")


async def resolve_tmdb_id(
    anilist_id: int,
    anilist_data: Optional[dict] = None,
    anibridge_data: Optional[dict] = None,
) -> dict:
    """
    Resolve an AniList ID to a TMDB ID + type using multiple sources.

    Returns:
        {
            "tmdb_type": "tv" | "movie" | None,
            "tmdb_id": int | list[int] | None,
            "method": "anibridge" | "fribb" | "tmdb_find_tvdb" | "tmdb_find_imdb"
                     | "tmdb_search_tv" | "tmdb_search_movie" | "not_found",
            "fribb_data": {...} | None,         # full Fribb entry (for mappings)
            "anibridge_data": {...} | None,      # AniBridge normalised result (for mappings)
            "tried_sources": [str, ...],        # which sources were tried
        }
    """
    cache_key = f"tmdb_id:{anilist_id}"
    cached = tmdb_id_cache.get(cache_key)
    if cached:
        return cached

    result: dict = {
        "tmdb_type": None,
        "tmdb_id": None,
        "method": "not_found",
        "fribb_data": None,
        "anibridge_data": anibridge_data,
        "tried_sources": [],
    }

    # ── Tier 1: AniBridge cross-provider mapping ──
    try:
        if anibridge_data is None:
            async with httpx.AsyncClient(timeout=15.0) as client:
                anibridge_data = await anibridge.fetch_cross_mappings(anilist_id, client)
                result["anibridge_data"] = anibridge_data
        result["tried_sources"].append("anibridge")
        if anibridge_data:
            cross = anibridge_data.get("cross_ids") or {}
            # Prefer TMDB show over movie for anime TV/OVA entries
            tmdb_shows = cross.get("tmdb_show") or []
            tmdb_movies = cross.get("tmdb_movie") or []
            if tmdb_shows:
                result["tmdb_type"] = "tv"
                result["tmdb_id"] = tmdb_shows[0]
                result["method"] = "anibridge"
                tmdb_id_cache.set(cache_key, result)
                return result
            if tmdb_movies:
                result["tmdb_type"] = "movie"
                result["tmdb_id"] = tmdb_movies[0]
                result["method"] = "anibridge"
                tmdb_id_cache.set(cache_key, result)
                return result
    except Exception as e:
        log.warning("AniBridge resolve %d failed: %s", anilist_id, e)

    # ── Tier 2: Fribb static lookup ──
    fribb_data = fribb.lookup(anilist_id) if fribb.is_loaded() else None
    result["tried_sources"].append("fribb")
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
            result["tmdb_id"] = tmdb_info["movie"][0] if isinstance(tmdb_info["movie"], list) else tmdb_info["movie"]
            result["method"] = "fribb"
            tmdb_id_cache.set(cache_key, result)
            return result

    # ── Fetch AniList data if we still need it (for title + year + format) ──
    if anilist_data is None:
        try:
            anilist_data = await anilist.fetch_anilist(anilist_id)
        except Exception as e:
            log.warning("AniList fetch for resolver %d failed: %s", anilist_id, e)
            anilist_data = {}

    # ── Tier 3: TMDB /find by external ID (TVDB or IMDb) ──
    async with httpx.AsyncClient(timeout=15.0) as client:
        # Try TVDB ID from either AniBridge or Fribb
        tvdb_id = _first_tvdb_id(anibridge_data, fribb_data)
        if tvdb_id:
            result["tried_sources"].append("tmdb_find_tvdb")
            found = await tmdb.find_by_external_id(str(tvdb_id), "tvdb_id", client)
            if found:
                result["tmdb_type"], result["tmdb_id"] = found
                result["method"] = "tmdb_find_tvdb"
                tmdb_id_cache.set(cache_key, result)
                return result

        # Try IMDb ID (from Fribb — AniBridge rarely has IMDb for shows)
        imdb_ids: list[str] = []
        if fribb_data and fribb_data.get("imdb_id"):
            imdb_ids = list(fribb_data["imdb_id"])
        if anibridge_data:
            imdb_from_ab = (anibridge_data.get("cross_ids") or {}).get("imdb_show") or []
            imdb_ids.extend(str(x) for x in imdb_from_ab)
            imdb_from_ab_m = (anibridge_data.get("cross_ids") or {}).get("imdb_movie") or []
            imdb_ids.extend(str(x) for x in imdb_from_ab_m)
        if imdb_ids:
            result["tried_sources"].append("tmdb_find_imdb")
            for imdb_id in imdb_ids[:3]:
                found = await tmdb.find_by_external_id(imdb_id, "imdb_id", client)
                if found:
                    result["tmdb_type"], result["tmdb_id"] = found
                    result["method"] = "tmdb_find_imdb"
                    tmdb_id_cache.set(cache_key, result)
                    return result

        # ── Tier 4: TMDB /search by name + year ──
        if anilist_data:
            title_en = (anilist_data.get("title") or {}).get("english")
            title_romaji = (anilist_data.get("title") or {}).get("romaji")
            year = (anilist_data.get("startDate") or {}).get("year")
            fmt = anilist_data.get("format", "TV")
            media_type = "movie" if fmt == "MOVIE" else "tv"
            result["tried_sources"].append(f"tmdb_search_{media_type}")

            for title in [title_en, title_romaji]:
                if not title:
                    continue
                tmdb_id = await tmdb.search(title, media_type, year, client)
                if tmdb_id:
                    result["tmdb_type"] = media_type
                    result["tmdb_id"] = tmdb_id
                    result["method"] = f"tmdb_search_{media_type}"
                    # Cache search results for shorter time (1 day) — might be wrong
                    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)
                    return result

    # ── Tier 5: Not on TMDB ──
    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)  # cache negative result for 1 day
    return result


def _first_tvdb_id(anibridge_data: Optional[dict], fribb_data: Optional[dict]) -> Optional[str | int]:
    """Pull the first available TVDB series ID from any source."""
    if anibridge_data:
        cross = anibridge_data.get("cross_ids") or {}
        for k in ("tvdb_show", "tvdb_movie"):
            v = cross.get(k) or []
            if v:
                return v[0]
    if fribb_data:
        tvdb = fribb_data.get("thetvdb_id")
        if tvdb:
            return tvdb
    return None
