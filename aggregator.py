"""
aggregator.py — Fetches metadata from all sources IN PARALLEL and merges.

Sources (all fetched concurrently — zero added latency):
  1. Fribb — AniList↔TMDB mapping (instant, in-memory)
  2. AniList — title, year, format, episode count, cover image
  3. AniZip — TVDB images (banner, poster, fanart, clearlogo) + episodes
  4. TMDB — episodes with stills, logos, backdrops, posters (if ID resolved)

Merge priority: TMDB > AniZip > AniList (TMDB has the best images for
newer anime; AniZip has TVDB-quality images; AniList is fallback).

Result: unified response matching just4anime.online format + extra fields.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

from cache import metadata_cache
from sources import anilist, anizip, fribb, tmdb
from resolver import resolve_tmdb_id

log = logging.getLogger("aggregator")


async def fetch_all(anilist_id: int) -> dict:
    """
    Fetch metadata for an AniList ID from all sources in parallel.
    Returns the unified response. Cached for 7 days.
    """
    cache_key = f"meta:{anilist_id}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    # Per-id lock so concurrent requests dedupe
    lock = metadata_cache.get_lock(cache_key)
    async with lock:
        # Check cache again inside lock
        cached = metadata_cache.get(cache_key)
        if cached is not None:
            return cached

        result = await _fetch_all_impl(anilist_id)
        metadata_cache.set(cache_key, result)
        return result


async def _fetch_all_impl(anilist_id: int) -> dict:
    """The actual fetch + merge logic."""
    # Step 1: Fetch AniList + AniZip in parallel (these don't need TMDB ID)
    async with httpx.AsyncClient(timeout=15.0) as client:
        anilist_task = asyncio.create_task(anilist.fetch_anilist(anilist_id, client))
        anizip_task = asyncio.create_task(anizip.fetch_anizip(anilist_id, client))

        # We need AniList data for the resolver (title + format + year)
        anilist_data = await anilist_task
        anizip_data = await anizip_task

    # Step 2: Resolve TMDB ID (uses Fribb + TMDB find + TMDB search)
    tmdb_resolution = await resolve_tmdb_id(anilist_id, anilist_data)
    tmdb_type = tmdb_resolution.get("tmdb_type")
    tmdb_id = tmdb_resolution.get("tmdb_id")
    fribb_data = tmdb_resolution.get("fribb_data")

    # Step 3: Fetch TMDB data if we have an ID (TV or Movie)
    tmdb_episodes = []
    tmdb_images = []
    tmdb_details = {}
    if tmdb_type and tmdb_id:
        async with httpx.AsyncClient(timeout=15.0) as client:
            if tmdb_type == "tv":
                # Fetch TV season 1 + images in parallel
                season_task = asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, 1, client))
                images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))
                details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))
                season_data, images_data, details = await asyncio.gather(
                    season_task, images_task, details_task, return_exceptions=True
                )
                if isinstance(season_data, dict) and not isinstance(season_data, Exception):
                    tmdb_episodes = tmdb.extract_episodes(season_data, anilist_id)
                if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                    tmdb_images = tmdb.extract_images(images_data, "tv")
                if isinstance(details, dict) and not isinstance(details, Exception):
                    tmdb_details = details
            elif tmdb_type == "movie":
                # Fetch movie details + images in parallel
                details_task = asyncio.create_task(tmdb.fetch_movie(tmdb_id, client))
                images_task = asyncio.create_task(tmdb.fetch_movie_images(tmdb_id, client))
                details, images_data = await asyncio.gather(details_task, images_task, return_exceptions=True)
                if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                    tmdb_images = tmdb.extract_images(images_data, "movie")
                if isinstance(details, dict) and not isinstance(details, Exception):
                    tmdb_details = details

    # Step 4: Merge all sources
    return _merge(
        anilist_id=anilist_id,
        anilist_data=anilist_data,
        anizip_data=anizip_data,
        fribb_data=fribb_data,
        tmdb_type=tmdb_type,
        tmdb_id=tmdb_id,
        tmdb_episodes=tmdb_episodes,
        tmdb_images=tmdb_images,
        tmdb_details=tmdb_details,
        resolver_method=tmdb_resolution.get("method", "not_found"),
    )


def _merge(
    anilist_id: int,
    anilist_data: dict,
    anizip_data: dict,
    fribb_data: Optional[dict],
    tmdb_type: Optional[str],
    tmdb_id,
    tmdb_episodes: list,
    tmdb_images: list,
    tmdb_details: dict,
    resolver_method: str,
) -> dict:
    """Merge all sources into a unified response."""
    # Title: prefer AniList (most accurate)
    title_en = (anilist_data.get("title") or {}).get("english") or anizip_data.get("title") or ""
    title_ja = (anilist_data.get("title") or {}).get("native") or anizip_data.get("titleJa") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""

    # Format + year from AniList
    fmt = anilist_data.get("format", "TV")
    year = (anilist_data.get("startDate") or {}).get("year")
    mal_id = anilist_data.get("idMal") or (fribb_data or {}).get("mal_id")

    # Episode count: prefer AniList's episodes field, then TMDB, then AniZip
    total_eps = anilist_data.get("episodes") or len(tmdb_episodes) or anizip_data.get("episodeCount", 0) or 0

    # Next airing from AniList
    next_airing = anilist_data.get("nextAiringEpisode") or {}
    next_ep = next_airing.get("episode") if next_airing else None

    # Determine current episode (last aired)
    current_ep = None
    today = time.strftime("%Y-%m-%d")
    all_eps = tmdb_episodes or anizip_data.get("episodes", [])
    for ep in all_eps:
        if ep.get("hasAired"):
            current_ep = ep.get("number")
        else:
            break

    # Images: merge TMDB + AniZip (TMDB takes priority, dedupe by coverType)
    images_by_type = {}
    for img in tmdb_images + anizip_data.get("images", []):
        ct = img.get("coverType")
        if ct and ct not in images_by_type:
            images_by_type[ct] = img
    # Also add AniList cover image as fallback Poster
    if "Poster" not in images_by_type and anilist_data.get("coverImage"):
        cover_url = anilist_data["coverImage"].get("extraLarge") or anilist_data["coverImage"].get("large")
        if cover_url:
            images_by_type["Poster"] = {"coverType": "Poster", "url": cover_url, "source": "anilist"}
    # Add AniList banner as fallback Banner
    if "Banner" not in images_by_type and anilist_data.get("bannerImage"):
        images_by_type["Banner"] = {"coverType": "Banner", "url": anilist_data["bannerImage"], "source": "anilist"}

    images = list(images_by_type.values())

    # Episodes: prefer TMDB (has stills), then AniZip
    episodes = tmdb_episodes or anizip_data.get("episodes", [])

    # If still no episodes but we have total_eps from AniList, generate placeholders
    if not episodes and total_eps:
        episodes = _generate_placeholder_episodes(anilist_id, total_eps, anilist_data, next_ep)

    # Mappings: merge Fribb + AniZip + AniList
    fribb_mappings = {}
    if fribb_data:
        fribb_mappings = {
            "anilist_id": fribb_data.get("anilist_id"),
            "mal_id": fribb_data.get("mal_id"),
            "thetvdb_id": fribb_data.get("thetvdb_id"),
            "themoviedb_id": fribb_data.get("themoviedb_id"),
            "imdb_id": fribb_data.get("imdb_id"),
            "anidb_id": fribb_data.get("anidb_id"),
            "kitsu_id": fribb_data.get("kitsu_id"),
        }
    anizip_mappings = anizip_data.get("mappings", {})
    mappings = {**anizip_mappings, **fribb_mappings}
    if tmdb_id:
        mappings["themoviedb_id_resolved"] = {"type": tmdb_type, "id": tmdb_id}

    # Description
    description = anilist_data.get("description", "") or ""

    # Genres + studios
    genres = anilist_data.get("genres", [])
    studios = [s["name"] for s in (anilist_data.get("studios") or {}).get("nodes", [])]

    # External links (streaming providers)
    external_links = anilist_data.get("externalLinks", [])

    return {
        "id": str(anilist_id),
        "malId": mal_id,
        "tmdbId": tmdb_id,
        "tmdbType": tmdb_type,
        "title": title_en or title_romaji,
        "titleRomaji": title_romaji,
        "titleJa": title_ja,
        "format": fmt,
        "year": year,
        "description": description,
        "genres": genres,
        "studios": studios,
        "totalEpisodes": len(episodes) if episodes else total_eps,
        "currentEpisode": current_ep,
        "nextAiringEpisode": next_ep,
        "nextAiringDate": _format_airing_date(next_airing.get("airingAt")) if next_airing else None,
        "images": images,
        "episodes": episodes,
        "mappings": mappings,
        "externalLinks": external_links,
        "sources": {
            "anilist": bool(anilist_data),
            "anizip": bool(anizip_data.get("episodes")),
            "fribb": bool(fribb_data),
            "tmdb": bool(tmdb_id),
            "tmdb_type": tmdb_type,
            "resolver_method": resolver_method,
        },
    }


def _format_airing_date(ts: int) -> Optional[str]:
    if not ts:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _generate_placeholder_episodes(anilist_id: int, total: int, anilist_data: dict, next_ep: Optional[int]) -> list[dict]:
    """Generate placeholder episodes when no source has episode data."""
    today = time.strftime("%Y-%m-%d")
    start_date = anilist_data.get("startDate") or {}
    year = start_date.get("year")
    month = start_date.get("month")
    day = start_date.get("day")

    episodes = []
    for num in range(1, total + 1):
        aired = next_ep is None or num < next_ep
        air_date = ""
        if year and month and day:
            # Approximate air date (weekly)
            from datetime import date, timedelta
            try:
                start = date(year, month, day)
                ep_date = start + timedelta(weeks=num - 1)
                air_date = ep_date.isoformat()
                aired = air_date <= today
            except Exception:
                pass
        episodes.append({
            "id": f"{anilist_id}-{num}",
            "number": num,
            "title": f"Episode {num}",
            "titleJa": "",
            "description": "",
            "image": "",
            "airDate": air_date,
            "duration": anilist_data.get("duration", 0) or 0,
            "isFiller": False,
            "rating": None,
            "hasAired": aired,
            "source": "generated",
        })
    return episodes
