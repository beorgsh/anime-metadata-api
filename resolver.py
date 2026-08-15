"""
resolver.py — Fribb-first AniList→TMDB ID resolver with smart fallback.

Tiers (in order of speed + reliability):

  Tier 1 — Fribb static lookup (in-memory, O(1), ~39% AniList coverage)
            Fast path — zero network latency. If Fribb has the TMDB ID, use it.

  Tier 2 — AniBridge cross-provider mapping (1 API call, ~150ms)
            Used when Fribb has no entry for this AniList ID.

  Tier 3 — TMDB /find by TVDB or IMDb ID (from Fribb or AniBridge)
            Catches ~1% more.

  Tier 4 — TMDB /search by ROOT title (multi-language, with year hint)
            NEW in v4: Strip season tags from the AniList title before searching.
            Try in order: English (stripped) → Romaji (stripped) → Native (stripped)
            This catches brand-new anime that haven't propagated to mapping DBs.

  Tier 5 — Graceful "not on TMDB" (return None, frontend uses AniList fallback)

Caches results for 30 days (ID mappings rarely change).
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from cache import tmdb_id_cache
from sources import fribb, tmdb, anilist, anibridge
from seasons import strip_season_tag, detect_season_from_title

log = logging.getLogger("resolver")


async def resolve_tmdb_id(
    anilist_id: int,
    anilist_data: Optional[dict] = None,
    fribb_data: Optional[dict] = None,
) -> dict:
    """
    Resolve an AniList ID to a TMDB ID + type.

    Returns:
        {
            "tmdb_type": "tv" | "movie" | None,
            "tmdb_id": int | None,
            "method": "fribb" | "anibridge" | "tmdb_find_tvdb" | "tmdb_find_imdb"
                     | "tmdb_search_tv" | "tmdb_search_movie" | "not_found",
            "fribb_data": {...} | None,
            "tried_sources": [str, ...],
            "search_attempts": [str, ...],  # what title variants we tried (for debugging)
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
        "fribb_data": fribb_data,
        "tried_sources": [],
        "search_attempts": [],
    }

    # ── Tier 1: Fribb (fast path — in-memory O(1)) ──────────────────
    if fribb_data is None and fribb.is_loaded():
        fribb_data = fribb.lookup(anilist_id)
        result["fribb_data"] = fribb_data
    result["tried_sources"].append("fribb")

    if fribb_data:
        tmdb_info = fribb_data.get("themoviedb_id") or {}
        if tmdb_info.get("tv"):
            result["tmdb_type"] = "tv"
            result["tmdb_id"] = tmdb_info["tv"]
            result["method"] = "fribb"
            tmdb_id_cache.set(cache_key, result)
            return result
        if tmdb_info.get("movie"):
            result["tmdb_type"] = "movie"
            movie_id = tmdb_info["movie"]
            if isinstance(movie_id, list):
                movie_id = movie_id[0] if movie_id else None
            result["tmdb_id"] = movie_id
            result["method"] = "fribb"
            tmdb_id_cache.set(cache_key, result)
            return result

    # Fetch AniList data if we still need it
    if anilist_data is None:
        try:
            anilist_data = await anilist.fetch_anilist(anilist_id)
        except Exception as e:
            log.warning("AniList fetch for resolver %d failed: %s", anilist_id, e)
            anilist_data = {}

    async with httpx.AsyncClient(timeout=15.0) as client:
        # ── Tier 2: AniBridge (live cross-DB mapping) ───────────────
        try:
            ab_data = await anibridge.fetch_cross_mappings(anilist_id, client)
            result["tried_sources"].append("anibridge")
            if ab_data:
                cross = ab_data.get("cross_ids") or {}
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

        # ── Tier 3: TMDB /find by external ID (TVDB or IMDb) ───────
        tvdb_id = _first_tvdb_id(fribb_data)
        if tvdb_id:
            result["tried_sources"].append("tmdb_find_tvdb")
            found = await tmdb.find_by_external_id(str(tvdb_id), "tvdb_id", client)
            if found:
                result["tmdb_type"], result["tmdb_id"] = found
                result["method"] = "tmdb_find_tvdb"
                tmdb_id_cache.set(cache_key, result)
                return result

        if fribb_data and fribb_data.get("imdb_id"):
            imdb_ids = list(fribb_data["imdb_id"])
            if imdb_ids:
                result["tried_sources"].append("tmdb_find_imdb")
                for imdb_id in imdb_ids[:3]:
                    found = await tmdb.find_by_external_id(imdb_id, "imdb_id", client)
                    if found:
                        result["tmdb_type"], result["tmdb_id"] = found
                        result["method"] = "tmdb_find_imdb"
                        tmdb_id_cache.set(cache_key, result)
                        return result

        # ── Tier 4: TMDB /search by ROOT title (multi-language) ─────
        # NEW in v4: strip season tags ("Season 2", "2nd Season", "Part 2",
        # "Cour 2", Roman numerals) before searching, so we find the base show.
        if anilist_data:
            title_en = (anilist_data.get("title") or {}).get("english")
            title_romaji = (anilist_data.get("title") or {}).get("romaji")
            title_native = (anilist_data.get("title") or {}).get("native")
            year = (anilist_data.get("startDate") or {}).get("year")
            fmt = anilist_data.get("format", "TV")
            media_type = "movie" if fmt == "MOVIE" else "tv"
            result["tried_sources"].append(f"tmdb_search_{media_type}")

            # Build list of (title, year) variants to try in order
            title_variants = []
            for original_title in [title_en, title_romaji, title_native]:
                if not original_title:
                    continue
                stripped = strip_season_tag(original_title)
                if stripped and stripped != original_title:
                    title_variants.append(stripped)  # stripped first
                if original_title:
                    title_variants.append(original_title)  # original as fallback
            # Dedupe preserving order
            seen = set()
            unique_variants = []
            for t in title_variants:
                if t and t not in seen:
                    seen.add(t)
                    unique_variants.append(t)

            for title in unique_variants:
                result["search_attempts"].append(title)
                # Don't pass year — TMDB returns the show's first-air year, not
                # the season's year. Passing year would filter out shows whose
                # later seasons we want (e.g. Re:Zero S2 in 2020 wouldn't match
                # year=2016).
                tmdb_id = await tmdb.search(title, media_type, None, client)
                if tmdb_id:
                    result["tmdb_type"] = media_type
                    result["tmdb_id"] = tmdb_id
                    result["method"] = f"tmdb_search_{media_type}"
                    # Cache search results for shorter time (1 day) — might be wrong
                    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)
                    return result

    # ── Tier 5: Not on TMDB ──
    tmdb_id_cache.set(cache_key, result, ttl=24 * 3600)
    return result


def _first_tvdb_id(fribb_data: Optional[dict]) -> Optional[str | int]:
    """Pull the first available TVDB series ID from any source."""
    if fribb_data:
        tvdb = fribb_data.get("thetvdb_id")
        if tvdb:
            return tvdb
    return None
