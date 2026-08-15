"""
aggregator.py — Lean, fast multi-source aggregator.

Pipeline (3 network calls max, parallel where possible):

  Stage 1 (parallel):
    ├─ AniList GraphQL  (1 call, ~100ms) → title, format, year, episodes, season
    └─ Fribb lookup     (in-memory, ~0ms) → tmdb_id, season.tmdb, episode_offset.tmdb

  Stage 2 (if Fribb missed — fallback chain):
    AniBridge → TMDB /find by TVDB/IMDb → TMDB /search by name+year
    (only one of these runs depending on what's missing)

  Stage 3 (parallel, only if TMDB ID found):
    ├─ TMDB TV details  (1 call, ~100ms) → seasons[] for the picker
    └─ TMDB TV season(s) (1 call per season, max 1-2 seasons typically)
    └─ TMDB TV images   (1 call, ~100ms) → logos, backdrops, posters

  Stage 4 (pure-functional, no network):
    seasons.slice_episodes() — apply offset, count, renumber, filter unaired

Total: ~3 calls for the fast path, ~4-5 if Fribb misses.
Jikan / Kitsu / AniZip / AniBridge are NOT in the hot path — they were the
slowdown in v2.
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
from seasons import (
    detect_pattern,
    resolve_target_season,
    slice_episodes,
    get_seasons_summary,
)

log = logging.getLogger("aggregator")


async def fetch_all(
    anilist_id: int,
    *,
    season: Optional[int] = None,
    include_upcoming: bool = False,
    extras: bool = False,
) -> dict:
    """
    Fetch metadata for an AniList ID. Cached for 7 days.

    Args:
        anilist_id: AniList anime ID.
        season: TMDB season number to fetch (1, 2, 3, ...). If None, auto-detect
            from Fribb's mapping. Pass 0 to fetch specials.
        include_upcoming: True = include episodes with future air dates.
            False (default) = only return episodes that have aired.
        extras: True = fetch TMDB season 0 (specials/OVAs) only.
    """
    # Cache key includes season + flags so different views don't collide
    cache_key = f"meta:{anilist_id}:s={season or 'auto'}:up={int(include_upcoming)}:ex={int(extras)}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    lock = metadata_cache.get_lock(cache_key)
    async with lock:
        cached = metadata_cache.get(cache_key)
        if cached is not None:
            return cached

        result = await _fetch_all_impl(
            anilist_id,
            season=season,
            include_upcoming=include_upcoming,
            extras=extras,
        )
        metadata_cache.set(cache_key, result)
        return result


async def _fetch_all_impl(
    anilist_id: int,
    *,
    season: Optional[int] = None,
    include_upcoming: bool = False,
    extras: bool = False,
) -> dict:
    """The actual fetch + slice + merge logic."""

    # ── Stage 1: AniList + Fribb in parallel ──────────────────────
    async with httpx.AsyncClient(timeout=15.0) as client:
        anilist_task = asyncio.create_task(anilist.fetch_anilist(anilist_id, client))
        # Fribb is in-memory so it doesn't need the client, but we wait in parallel
        anilist_data = await anilist_task

    fribb_data = fribb.lookup(anilist_id) if fribb.is_loaded() else None

    # ── Stage 2: Resolve TMDB ID (Fribb-first, with fallbacks) ─────
    tmdb_resolution = await resolve_tmdb_id(anilist_id, anilist_data, fribb_data)
    tmdb_type = tmdb_resolution.get("tmdb_type")
    tmdb_id = tmdb_resolution.get("tmdb_id")
    # Refresh fribb_data in case the resolver enriched it
    fribb_data = tmdb_resolution.get("fribb_data") or fribb_data
    resolver_method = tmdb_resolution.get("method", "not_found")

    # ── Stage 3: TMDB fetch (parallel: details + season + images) ──
    tmdb_episodes: list[dict] = []
    tmdb_images: list[dict] = []
    tmdb_details: dict = {}
    slice_offset = 0
    slice_count: Optional[int] = None
    continuous_numbering = False
    pattern = "not_found"

    if tmdb_type == "tv" and tmdb_id:
        async with httpx.AsyncClient(timeout=15.0) as client:
            details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))
            images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))

            # We need the TV details first to know how many seasons there are
            details = await details_task
            if isinstance(details, dict) and details:
                tmdb_details = details

            # Decide which season(s) to fetch
            if extras:
                # /extras endpoint: always fetch TMDB season 0 (specials)
                target_seasons = [0]
                slice_offset = 0
                slice_count = None
                pattern = "extras"
                continuous_numbering = False
            else:
                decision = resolve_target_season(
                    anilist_id=anilist_id,
                    anilist_data=anilist_data,
                    fribb_data=fribb_data,
                    tmdb_details=tmdb_details,
                    explicit_season=season,
                )
                target_seasons = decision["season_numbers"]
                slice_offset = decision["episode_offset"]
                slice_count = decision["episode_count"]
                pattern = decision["pattern"]
                continuous_numbering = decision["continuous_numbering"]

            # Fetch all required TMDB seasons in parallel
            season_tasks = [
                asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, s, client))
                for s in target_seasons
            ]
            season_results = await asyncio.gather(*season_tasks, return_exceptions=True)

            # Extract + concatenate episodes from each season
            for sn, sd in zip(target_seasons, season_results):
                if isinstance(sd, dict) and not isinstance(sd, Exception):
                    season_eps = tmdb.extract_episodes(sd, anilist_id)
                    # For Pattern B (multi-season continuous), apply season offset
                    if continuous_numbering and len(target_seasons) > 1:
                        # Calculate the running offset based on TMDB season's
                        # position in the seasons[] array (each prior season
                        # contributes its episode_count to the offset).
                        seasons_meta = [s for s in (tmdb_details.get("seasons") or [])
                                       if s.get("season_number", 0) > 0]
                        running_offset = 0
                        for sm in seasons_meta:
                            if sm.get("season_number") == sn:
                                break
                            running_offset += sm.get("episode_count", 0) or 0
                        for ep in season_eps:
                            ep["number"] = ep["number"] + running_offset
                            ep["season"] = sn
                    tmdb_episodes.extend(season_eps)

            images_data = await images_task
            if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                tmdb_images = tmdb.extract_images(images_data, "tv")

    elif tmdb_type == "movie" and tmdb_id:
        async with httpx.AsyncClient(timeout=15.0) as client:
            details_task = asyncio.create_task(tmdb.fetch_movie(tmdb_id, client))
            images_task = asyncio.create_task(tmdb.fetch_movie_images(tmdb_id, client))
            details, images_data = await asyncio.gather(details_task, images_task, return_exceptions=True)
            if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                tmdb_images = tmdb.extract_images(images_data, "movie")
            if isinstance(details, dict) and not isinstance(details, Exception):
                tmdb_details = details
                # Build a single "episode" representing the movie itself
                import time as _time
                air_date = details.get("release_date", "") or ""
                today = _time.strftime("%Y-%m-%d")
                tmdb_episodes = [{
                    "id": f"{anilist_id}-1",
                    "number": 1,
                    "title": details.get("title") or anilist_data.get("title", {}).get("english", "Movie"),
                    "titleJa": "",
                    "description": details.get("overview", "") or "",
                    "image": (tmdb.extract_images(images_data, "movie")[0].get("url") if isinstance(images_data, dict) and tmdb_images else ""),
                    "airDate": air_date,
                    "duration": details.get("runtime", 0) or 0,
                    "isFiller": False,
                    "rating": str(details.get("vote_average")) if details.get("vote_average") else None,
                    "hasAired": bool(air_date) and air_date <= today,
                    "source": "tmdb",
                }]
        pattern = "movie"
    else:
        pattern = "not_found"

    # ── Stage 4: Slice + renumber + filter (pure-functional) ───────
    if tmdb_type == "tv":
        sliced = slice_episodes(
            tmdb_episodes,
            offset=slice_offset,
            count=slice_count,
            continuous_numbering=continuous_numbering,
            include_upcoming=include_upcoming,
            anilist_id=anilist_id,
        )
    else:
        # Movies: just one "episode" (already built above)
        sliced = tmdb_episodes

    # Detect the actual pattern (for the response)
    if tmdb_type == "movie":
        detected_pattern = "movie"
    elif tmdb_type == "tv":
        detected_pattern = detect_pattern(
            anilist_id=anilist_id,
            anilist_data=anilist_data,
            fribb_data=fribb_data,
            tmdb_type=tmdb_type,
            tmdb_id=tmdb_id,
            tmdb_details=tmdb_details,
            extras_mode=extras,
        )
    else:
        detected_pattern = "not_found"

    # Sibling AniList IDs (Pattern A discovery)
    sibling_anilist_ids: list[int] = []
    if tmdb_type == "tv" and tmdb_id:
        sibling_anilist_ids = fribb.lookup_siblings_by_tmdb_tv(int(tmdb_id))
        # If only 1 sibling (just us), don't bother listing
        if len(sibling_anilist_ids) <= 1:
            sibling_anilist_ids = []

    # Build seasons summary
    seasons_summary: list[dict] = []
    if tmdb_type == "tv" and tmdb_details:
        seasons_summary = get_seasons_summary(
            anilist_id=anilist_id,
            tmdb_details=tmdb_details,
            fribb_data=fribb_data,
            sibling_anilist_ids=sibling_anilist_ids,
        )

    # ── Stage 5: Merge + return ───────────────────────────────────
    return _merge(
        anilist_id=anilist_id,
        anilist_data=anilist_data or {},
        fribb_data=fribb_data,
        tmdb_type=tmdb_type,
        tmdb_id=tmdb_id,
        tmdb_episodes=sliced,
        tmdb_images=tmdb_images,
        tmdb_details=tmdb_details,
        resolver_method=resolver_method,
        resolver_tried=tmdb_resolution.get("tried_sources", []),
        pattern=detected_pattern,
        seasons_summary=seasons_summary,
        sibling_anilist_ids=sibling_anilist_ids,
        extras_mode=extras,
        include_upcoming=include_upcoming,
    )


# ─── Merge ────────────────────────────────────────────────────────────


def _merge(
    *,
    anilist_id: int,
    anilist_data: dict,
    fribb_data: Optional[dict],
    tmdb_type: Optional[str],
    tmdb_id,
    tmdb_episodes: list[dict],
    tmdb_images: list[dict],
    tmdb_details: dict,
    resolver_method: str,
    resolver_tried: list,
    pattern: str,
    seasons_summary: list[dict],
    sibling_anilist_ids: list[int],
    extras_mode: bool,
    include_upcoming: bool,
) -> dict:
    # Titles
    title_en = (anilist_data.get("title") or {}).get("english") or ""
    title_ja = (anilist_data.get("title") or {}).get("native") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""

    fmt = anilist_data.get("format", "TV")
    year = (anilist_data.get("startDate") or {}).get("year")
    mal_id = anilist_data.get("idMal") or (fribb_data or {}).get("mal_id")

    # AniList's episode count (the verification anchor)
    anilist_episodes_field = anilist_data.get("episodes")

    # Final episode count = how many we actually returned
    total_eps_returned = len(tmdb_episodes) if tmdb_episodes else 0

    # Verification: compare what we returned vs AniList's count (free check, no extra calls)
    verification = _lightweight_verification(
        anilist_episodes_field, total_eps_returned, anilist_data.get("status"),
        pattern, extras_mode,
    )

    # Next airing info from AniList
    next_airing = anilist_data.get("nextAiringEpisode") or {}
    next_ep = next_airing.get("episode") if next_airing else None

    # Current episode (last aired in the returned list)
    current_ep = None
    for ep in tmdb_episodes:
        if ep.get("hasAired"):
            current_ep = ep.get("number")
        else:
            break

    # Images: TMDB priority, fallback to AniList cover/banner
    images_by_type: dict[str, dict] = {}
    for img in tmdb_images:
        ct = img.get("coverType")
        if ct and ct not in images_by_type:
            images_by_type[ct] = img
    if "Poster" not in images_by_type and anilist_data.get("coverImage"):
        cover_url = anilist_data["coverImage"].get("extraLarge") or anilist_data["coverImage"].get("large")
        if cover_url:
            images_by_type["Poster"] = {"coverType": "Poster", "url": cover_url, "source": "anilist"}
    if "Banner" not in images_by_type and anilist_data.get("bannerImage"):
        images_by_type["Banner"] = {"coverType": "Banner", "url": anilist_data["bannerImage"], "source": "anilist"}

    images = list(images_by_type.values())

    # Episodes (already sliced + renumbered + filtered)
    episodes = tmdb_episodes

    # If no TMDB episodes but we have AniList count, generate placeholders
    if not episodes and anilist_episodes_field and not extras_mode:
        episodes = _generate_placeholder_episodes(anilist_id, anilist_episodes_field, anilist_data, next_ep)

    # Mappings (single source of truth: Fribb's canonical table + extras from AniList)
    mappings: dict = {}
    if fribb_data:
        mappings = {
            "anilist_id": fribb_data.get("anilist_id") or anilist_id,
            "mal_id": fribb_data.get("mal_id") or mal_id,
            "thetvdb_id": fribb_data.get("thetvdb_id"),
            "themoviedb_id": fribb_data.get("themoviedb_id"),
            "imdb_id": fribb_data.get("imdb_id"),
            "anidb_id": fribb_data.get("anidb_id"),
            "kitsu_id": fribb_data.get("kitsu_id"),
        }
    else:
        mappings = {
            "anilist_id": anilist_id,
            "mal_id": mal_id,
        }
    if tmdb_id:
        mappings["themoviedb_id_resolved"] = {"type": tmdb_type, "id": tmdb_id, "method": resolver_method}

    # Description
    description = anilist_data.get("description", "") or ""

    # Genres + studios
    genres = anilist_data.get("genres", [])
    studios = [s["name"] for s in (anilist_data.get("studios") or {}).get("nodes", [])]
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

        # Episode count: what we actually returned
        "totalEpisodes": total_eps_returned,
        # AniList's stated count (for cross-reference)
        "anilistEpisodeCount": anilist_episodes_field,
        "currentEpisode": current_ep,
        "nextAiringEpisode": next_ep,
        "nextAiringDate": _format_airing_date(next_airing.get("airingAt")) if next_airing else None,

        "images": images,
        "episodes": episodes,
        "mappings": mappings,
        "externalLinks": external_links,

        # Season info — the new "own brain" output
        "seasons": seasons_summary,
        "siblingAnilistIds": sibling_anilist_ids,
        "pattern": pattern,

        # Lightweight verification (free, no extra calls)
        "verification": verification,

        # Source flags
        "sources": {
            "anilist": bool(anilist_data),
            "fribb": bool(fribb_data),
            "tmdb": bool(tmdb_id),
            "tmdb_type": tmdb_type,
            "resolver_method": resolver_method,
            "resolver_tried": resolver_tried,
        },

        # View flags
        "view": {
            "extras_mode": extras_mode,
            "include_upcoming": include_upcoming,
            "explicit_season": season_param(extras_mode, anilist_id, fribb_data),
        },
    }


def _lightweight_verification(
    anilist_eps: Optional[int],
    returned_count: int,
    anilist_status: Optional[str],
    pattern: str,
    extras_mode: bool,
) -> dict:
    """Free verification: compare AniList's stated count vs what we returned.
    No extra network calls — just compares data we already have."""
    if extras_mode:
        return {
            "field": "episodes",
            "anilist_count": anilist_eps,
            "returned_count": returned_count,
            "match": None,  # extras don't have a comparable count
            "note": "extras mode — count comparison skipped",
        }

    if anilist_eps is None:
        return {
            "field": "episodes",
            "anilist_count": None,
            "returned_count": returned_count,
            "match": None,
            "note": "AniList has no episode count (likely ongoing series)",
        }

    match = (anilist_eps == returned_count)
    return {
        "field": "episodes",
        "anilist_count": anilist_eps,
        "returned_count": returned_count,
        "match": match,
        "note": "ok" if match else (
            f"mismatch: AniList says {anilist_eps}, we returned {returned_count} "
            f"(pattern={pattern}, status={anilist_status})"
        ),
    }


def season_param(extras_mode: bool, anilist_id: int, fribb_data: Optional[dict]) -> Optional[int]:
    """Return the explicit season number for the response view block."""
    if extras_mode:
        return 0
    if fribb_data:
        season_info = fribb_data.get("season") or {}
        return season_info.get("tmdb")
    return None


def _format_airing_date(ts: int) -> Optional[str]:
    if not ts:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _generate_placeholder_episodes(anilist_id: int, total: int, anilist_data: dict, next_ep: Optional[int]) -> list[dict]:
    """Generate placeholder episodes when no TMDB data is available."""
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
