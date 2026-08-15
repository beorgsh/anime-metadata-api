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

from cache import metadata_cache, offset_cache
from sources import anilist, anizip, fribb, tmdb
from sources.anizip import fetch_anizip as _fetch_anizip
from sources import anibridge_local
from resolver import resolve_tmdb_id
from seasons import (
    detect_pattern,
    _safe_int,
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
    shared_client: Optional[httpx.AsyncClient] = None,
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
        shared_client: Optional shared httpx.AsyncClient (reused across requests
            for TLS connection pooling). If None, a fresh client is created.
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
            shared_client=shared_client,
        )
        metadata_cache.set(cache_key, result)
        return result



def _within_30_days(air_date: str, today: str) -> bool:
    """Check if air_date is within the last 30 days from today."""
    try:
        from datetime import date
        d_air = date.fromisoformat(air_date)
        d_today = date.fromisoformat(today)
        return (d_today - d_air).days <= 30
    except Exception:
        return False



def _cross_verify_and_filter(
    episodes: list[dict],
    anilist_data: dict,
    anilist_next_airing: Optional[int],
    include_upcoming: bool,
) -> list[dict]:
    """Cross-verify episode list with AniList data and filter unaired episodes.

    This is the API's "own brain" for episode count verification:

    1. If AniList says nextAiringEpisode=N (ongoing):
       - Episodes 1..N-1 should have aired
       - Cap at N-1, BUT allow episodes that TMDB says aired within last 30 days
         (catches AniList schedule lag, e.g. Re:Zero S4 EP 13 aired per TMDB
         but AniList still says nextAiring=13)
    2. If AniList is FINISHED (no nextAiring):
       - Return all episodes in the range
       - Cross-verify: count should match AniList's `episodes` field
         (if mismatch, log a warning but still return what we have)
    3. Filter unaired episodes by default (air_date > today)
       - Exception: if include_upcoming=True, keep all

    Returns the filtered + verified episode list.
    """
    if not episodes:
        return episodes

    today = time.strftime("%Y-%m-%d")

    # ── Step 1: Apply nextAiring cap (with 30-day grace period) ──
    if anilist_next_airing and anilist_next_airing > 0:
        capped = []
        for ep in episodes:
            ep_num = ep.get("number", 0)
            if ep_num < anilist_next_airing:
                # Below cap — always include
                capped.append(ep)
            else:
                # At or past cap — only include if TMDB says it aired recently
                air_date = ep.get("airDate", "") or ""
                if air_date and air_date <= today and _within_30_days(air_date, today):
                    capped.append(ep)
                elif include_upcoming:
                    capped.append(ep)
        episodes = capped

    # ── Step 2: Filter unaired episodes (unless include_upcoming) ──
    if not include_upcoming:
        filtered = []
        for ep in episodes:
            air_date = ep.get("airDate", "") or ""
            if not air_date:
                # No air date — include it (might be a special or unknown)
                filtered.append(ep)
            elif air_date <= today:
                # Already aired
                filtered.append(ep)
            elif _within_30_days(air_date, today):
                # Airing soon (within 30 days) — include it
                filtered.append(ep)
            # else: future episode, skip
        episodes = filtered

    # ── Step 3: Cross-verify count ──
    anilist_eps = None
    try:
        anilist_eps = int((anilist_data or {}).get("episodes") or 0)
    except (TypeError, ValueError):
        pass

    if anilist_eps and anilist_eps > 0 and len(episodes) > 0:
        if anilist_next_airing:
            # Ongoing — our count should be <= anilist_eps and = nextAiring-1 (approximately)
            expected_aired = anilist_next_airing - 1
            if len(episodes) > anilist_eps:
                log.warning("AniList %s: returned %d episodes but AniList says only %d total",
                           anilist_data.get("id"), len(episodes), anilist_eps)
                episodes = episodes[:anilist_eps]
        else:
            # Finished — count should match AniList exactly
            if len(episodes) != anilist_eps:
                log.warning("AniList %s: returned %d episodes but AniList says %d (diff=%d)",
                           anilist_data.get("id"), len(episodes), anilist_eps,
                           len(episodes) - anilist_eps)

    # ── Step 4: Renumber ONLY for Pattern A (per-entry 1..N) ──
    # For Pattern B (continuous TMDB numbering), DON'T renumber — the episode
    # numbers are already correct (1, 2, ..., 1173). Renumbering would break them.
    # We detect Pattern A vs B by checking if the first episode is at 1 AND the
    # last episode is NOT at len(episodes). If last == len, it's already 1..N (Pattern A).
    # If last > len, it's continuous (Pattern B).
    if episodes:
        first_num = episodes[0].get("number", 0)
        last_num = episodes[-1].get("number", 0)
        if first_num == 1 and last_num == len(episodes):
            # Already 1..N — Pattern A, no renumbering needed
            pass
        elif first_num == 1 and last_num > len(episodes):
            # Pattern B (continuous) — don't renumber
            pass
        else:
            # Pattern A with filtering — renumber 1..N
            for i, ep in enumerate(episodes, 1):
                ep["number"] = i
                ep["id"] = f"{anilist_data.get('id', 'unknown')}-{i}"

    return episodes


async def _fetch_all_impl(
    anilist_id: int,
    *,
    season: Optional[int] = None,
    include_upcoming: bool = False,
    extras: bool = False,
    shared_client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """Fast-path fetch using AniBridge local mappings.

    When AniBridge has the mapping (6988 AniList IDs):
      1. AniList GraphQL + TMDB season + TMDB images — ALL IN PARALLEL
      2. Cross-verify (pure-functional)
      3. Return
      Total: ~300ms (one network round-trip's worth of latency)

    When AniBridge misses:
      Fall back to Fribb → resolver → AniZip → chain walk
      Total: ~1-2s
    """
    own_client = False
    if shared_client is None:
        shared_client = httpx.AsyncClient(timeout=15.0)
        own_client = True

    try:
        # ── Step 0: AniBridge lookup (in-memory, 0ms) ──
        ab_entries = None
        if anibridge_local.is_loaded():
            ab_entries = anibridge_local.lookup_anilist(anilist_id)

        if ab_entries:
            # ── FAST PATH: AniBridge has the mapping ──
            return await _fast_path(
                anilist_id, ab_entries, shared_client,
                season=season, include_upcoming=include_upcoming, extras=extras,
            )
        else:
            # ── SLOW PATH: AniBridge misses → Fribb + resolver fallbacks ──
            return await _slow_path(
                anilist_id, shared_client,
                season=season, include_upcoming=include_upcoming, extras=extras,
            )
    finally:
        if own_client:
            await shared_client.aclose()


async def _fast_path(
    anilist_id: int,
    ab_entries: list[dict],
    client: httpx.AsyncClient,
    *,
    season: Optional[int],
    include_upcoming: bool,
    extras: bool,
) -> dict:
    """Fast path: AniBridge has the mapping.

    Fires AniList GraphQL + TMDB season + TMDB images ALL IN PARALLEL.
    No resolver, no Fribb, no AniZip, no chain walk.
    """
    first_ab = ab_entries[0]
    tmdb_id = first_ab["tmdb_id"]
    tmdb_season = first_ab["tmdb_season"]
    is_identity = anibridge_local.is_identity_mapping(first_ab)
    ab_start, ab_end = anibridge_local.get_tmdb_episode_range(first_ab)

    # ── Fire ALL 3 calls in PARALLEL ──
    anilist_task = asyncio.create_task(
        anilist.fetch_anilist_with_relations(anilist_id, client)
    )

    # For Pattern B (multi-season identity), fetch ALL TMDB seasons in parallel
    # Use TMDB's own seasons[] list (from details), not just AniBridge's list.
    # AniBridge might not have the latest season (e.g. One Piece S23).
    if is_identity and len(ab_entries) > 1:
        # Fetch TMDB details FIRST to get the complete seasons list
        details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))
        images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))

        anilist_data, anilist_relations = await anilist_task
        details = await details_task

        # Get ALL non-special TMDB seasons (not just AniBridge's list)
        all_tmdb_seasons = []
        if isinstance(details, dict):
            for s in details.get("seasons", []):
                sn = s.get("season_number", 0)
                if sn > 0:  # Skip specials (S0)
                    all_tmdb_seasons.append(sn)

        # Fetch all TMDB seasons in parallel
        season_tasks = [
            asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, sn, client))
            for sn in all_tmdb_seasons
        ]
        season_results = await asyncio.gather(*season_tasks, return_exceptions=True)
        images_data = await images_task
    elif extras:
        # Extras: fetch season 0 (specials)
        season_task = asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, 0, client))
        images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))
        details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))

        anilist_data, anilist_relations = await anilist_task
        details = await details_task
        season_results = [await season_task]
        images_data = await images_task
    else:
        # Pattern A or single-entry: fetch 1 TMDB season
        season_task = asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, tmdb_season, client))
        images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))
        details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))

        anilist_data, anilist_relations = await anilist_task
        details = await details_task
        season_results = [await season_task]
        images_data = await images_task

    # ── Process episodes ──
    tmdb_episodes = []
    for i, sd in enumerate(season_results):
        if isinstance(sd, dict) and not isinstance(sd, Exception):
            sn = all_tmdb_seasons[i] if (is_identity and len(ab_entries) > 1 and i < len(all_tmdb_seasons)) else tmdb_season
            eps = tmdb.extract_episodes(sd, anilist_id)
            for ep in eps:
                ep["season"] = sn
            tmdb_episodes.extend(eps)

    # Filter to AniBridge's exact TMDB episode range
    if not is_identity or len(ab_entries) == 1:
        # Pattern A or single-entry: filter to exact range
        ab_end_real = ab_end if ab_end < 999999 else 999999
        tmdb_episodes = [e for e in tmdb_episodes
            if ab_start <= e.get("number", 0) <= ab_end_real]

    # For Pattern A: renumber 1..N
    if not is_identity:
        for i, ep in enumerate(tmdb_episodes, 1):
            ep["number"] = i
            ep["id"] = f"{anilist_id}-{i}"

    # ── Cross-verify + filter (pure-functional, ~0ms) ──
    anilist_next_airing = None
    _nae = (anilist_data or {}).get("nextAiringEpisode") or {}
    if isinstance(_nae, dict):
        anilist_next_airing = _nae.get("episode")

    tmdb_episodes = _cross_verify_and_filter(
        tmdb_episodes, anilist_data, anilist_next_airing, include_upcoming
    )

    # ── Extract images ──
    tmdb_images = []
    if isinstance(images_data, dict):
        tmdb_images = tmdb.extract_images(images_data, "tv")

    # AniList image fallbacks
    images_by_type = {}
    for img in tmdb_images:
        ct = img.get("coverType")
        if ct and ct not in images_by_type:
            images_by_type[ct] = img
    if "Poster" not in images_by_type and anilist_data.get("coverImage"):
        cu = anilist_data["coverImage"].get("extraLarge") or anilist_data["coverImage"].get("large")
        if cu:
            images_by_type["Poster"] = {"coverType": "Poster", "url": cu, "source": "anilist"}
    if "Banner" not in images_by_type and anilist_data.get("bannerImage"):
        images_by_type["Banner"] = {"coverType": "Banner", "url": anilist_data["bannerImage"], "source": "anilist"}

    images = list(images_by_type.values())

    # ── Build response ──
    title_en = (anilist_data.get("title") or {}).get("english") or ""
    title_ja = (anilist_data.get("title") or {}).get("native") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""
    fmt = anilist_data.get("format", "TV")
    year = (anilist_data.get("startDate") or {}).get("year")
    mal_id = anilist_data.get("idMal")
    anilist_eps_field = anilist_data.get("episodes")
    next_airing = anilist_data.get("nextAiringEpisode") or {}
    next_ep = next_airing.get("episode") if next_airing else None

    # Determine pattern
    if extras:
        pattern = "extras"
    elif not is_identity:
        pattern = "pattern_a_anibridge"
    elif len(ab_entries) > 1:
        pattern = "pattern_b_anibridge"
    else:
        pattern = "pattern_c_anibridge"

    # Sibling AniList IDs
    sibling_ids = []
    if fribb.is_loaded():
        sibling_ids = fribb.lookup_siblings_by_tmdb_tv(int(tmdb_id))
        if len(sibling_ids) <= 1:
            sibling_ids = []

    # Seasons summary
    seasons_summary = []
    if isinstance(details, dict) and details:
        seasons_summary = get_seasons_summary(
            anilist_id=anilist_id,
            tmdb_details=details,
            fribb_data=None,
            sibling_anilist_ids=sibling_ids,
        )

    # Mappings
    mappings = {
        "anilist_id": anilist_id,
        "mal_id": mal_id,
        "themoviedb_id_resolved": {"type": "tv", "id": tmdb_id, "method": "anibridge_local"},
    }

    # Verification
    verification = _lightweight_verification(
        anilist_eps_field, len(tmdb_episodes), anilist_data.get("status"),
        pattern, extras,
    )

    current_ep = None
    for ep in tmdb_episodes:
        if ep.get("hasAired"):
            current_ep = ep.get("number")
        else:
            break

    return {
        "id": str(anilist_id),
        "malId": mal_id,
        "tmdbId": tmdb_id,
        "tmdbType": "tv",
        "title": title_en or title_romaji,
        "titleRomaji": title_romaji,
        "titleJa": title_ja,
        "format": fmt,
        "year": year,
        "description": anilist_data.get("description", "") or "",
        "genres": anilist_data.get("genres", []),
        "studios": [s["name"] for s in (anilist_data.get("studios") or {}).get("nodes", [])],
        "totalEpisodes": len(tmdb_episodes),
        "anilistEpisodeCount": anilist_eps_field,
        "currentEpisode": current_ep,
        "nextAiringEpisode": next_ep,
        "nextAiringDate": _format_airing_date(next_airing.get("airingAt")) if next_airing else None,
        "images": images,
        "episodes": tmdb_episodes,
        "mappings": mappings,
        "externalLinks": anilist_data.get("externalLinks", []),
        "seasons": seasons_summary,
        "siblingAnilistIds": sibling_ids,
        "pattern": pattern,
        "verification": verification,
        "sources": {
            "anilist": bool(anilist_data),
            "anibridge_local": True,
            "fribb": False,
            "tmdb": True,
            "tmdb_type": "tv",
            "resolver_method": "anibridge_local",
            "resolver_tried": ["anibridge_local"],
        },
        "view": {
            "extras_mode": extras,
            "include_upcoming": include_upcoming,
            "explicit_season": season,
        },
    }


async def _slow_path(
    anilist_id: int,
    client: httpx.AsyncClient,
    *,
    season: Optional[int],
    include_upcoming: bool,
    extras: bool,
) -> dict:
    """Slow path: AniBridge misses → Fribb + resolver + fallbacks.

    This is the OLD logic — kept for anime not in AniBridge's database.
    """
    # Stage 1: AniList + Fribb
    anilist_data, anilist_relations = await anilist.fetch_anilist_with_relations(
        anilist_id, client
    )
    fribb_data = fribb.lookup(anilist_id) if fribb.is_loaded() else None

    # Stage 2: Resolve TMDB ID (Fribb → AniBridge API → TMDB /search)
    tmdb_resolution = await resolve_tmdb_id(anilist_id, anilist_data, fribb_data)
    tmdb_type = tmdb_resolution.get("tmdb_type")
    tmdb_id = tmdb_resolution.get("tmdb_id")
    fribb_data = tmdb_resolution.get("fribb_data") or fribb_data
    resolver_method = tmdb_resolution.get("method", "not_found")

    # Stage 3: TMDB fetch
    tmdb_episodes = []
    tmdb_images = []
    tmdb_details = {}
    slice_offset = 0
    slice_count = None
    continuous_numbering = False
    pattern = "not_found"

    anilist_next_airing = None
    _nae = (anilist_data or {}).get("nextAiringEpisode") or {}
    if isinstance(_nae, dict):
        anilist_next_airing = _nae.get("episode")

    if tmdb_type == "tv" and tmdb_id:
        details = await tmdb.fetch_tv(tmdb_id, client)
        if isinstance(details, dict) and details:
            tmdb_details = details

        images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))

        if extras:
            target_seasons = [0]
            pattern = "extras"
        else:
            decision = resolve_target_season(
                anilist_id=anilist_id,
                anilist_data=anilist_data,
                fribb_data=fribb_data,
                tmdb_details=tmdb_details,
                explicit_season=season,
                calculated_offset=None,
            )
            target_seasons = decision["season_numbers"]
            slice_offset = decision["episode_offset"]
            slice_count = decision["episode_count"]
            pattern = decision["pattern"]
            continuous_numbering = decision["continuous_numbering"]

        season_tasks = [asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, s, client)) for s in target_seasons]
        season_results = await asyncio.gather(*season_tasks, return_exceptions=True)

        for sn, sd in zip(target_seasons, season_results):
            if isinstance(sd, dict) and not isinstance(sd, Exception):
                season_eps = tmdb.extract_episodes(sd, anilist_id)
                for ep in season_eps:
                    ep["season"] = sn
                tmdb_episodes.extend(season_eps)

        images_data = await images_task
        if isinstance(images_data, dict):
            tmdb_images = tmdb.extract_images(images_data, "tv")

        # AniZip fallback for Pattern A
        is_pattern_a = bool((fribb_data and (fribb_data.get("episode_offset") or {}).get("tmdb"))
                             or detect_season_from_title((anilist_data.get("title") or {}).get("english") or "")
                             or detect_season_from_title((anilist_data.get("title") or {}).get("romaji") or ""))
        if is_pattern_a:
            try:
                _az = await _fetch_anizip(anilist_id, client)
                if _az and _az.get("episodes"):
                    tmdb_episodes = list(_az["episodes"])
                    slice_offset = 0
                    slice_count = None
                    continuous_numbering = False
                    pattern = "pattern_a_anizip"
            except Exception as e:
                log.warning("AniZip fetch for %d failed: %s", anilist_id, e)

        # Slice + filter
        sliced = slice_episodes(
            tmdb_episodes,
            offset=slice_offset,
            count=slice_count,
            continuous_numbering=continuous_numbering,
            include_upcoming=include_upcoming,
            anilist_id=anilist_id,
            anilist_next_airing=anilist_next_airing,
        )
        tmdb_episodes = sliced

    elif tmdb_type == "movie" and tmdb_id:
        details = await tmdb.fetch_movie(tmdb_id, client)
        images_data = await tmdb.fetch_movie_images(tmdb_id, client)
        if isinstance(images_data, dict):
            tmdb_images = tmdb.extract_images(images_data, "movie")
        if isinstance(details, dict):
            tmdb_details = details
            import time as _time
            air_date = tmdb_details.get("release_date", "") or ""
            today = _time.strftime("%Y-%m-%d")
            tmdb_episodes = [{
                "id": f"{anilist_id}-1", "number": 1,
                "title": tmdb_details.get("title") or "Movie",
                "titleJa": "", "description": tmdb_details.get("overview", "") or "",
                "image": (tmdb_images[0].get("url") if tmdb_images else ""),
                "airDate": air_date,
                "duration": tmdb_details.get("runtime", 0) or 0,
                "isFiller": False, "hasAired": bool(air_date) and air_date <= today,
                "source": "tmdb",
            }]
        pattern = "movie"
    else:
        pattern = "not_found"

    # Generate placeholders if no episodes but AniList has count
    if not tmdb_episodes and anilist_data.get("episodes"):
        tmdb_episodes = _generate_placeholder_episodes(
            anilist_id, anilist_data["episodes"], anilist_data, anilist_next_airing
        )

    # Detect pattern for response
    detected_pattern = pattern
    if tmdb_type == "tv" and tmdb_details:
        detected_pattern = detect_pattern(
            anilist_id=anilist_id, anilist_data=anilist_data, fribb_data=fribb_data,
            tmdb_type=tmdb_type, tmdb_id=tmdb_id, tmdb_details=tmdb_details, extras_mode=extras,
        )

    # Build seasons + siblings
    sibling_ids = []
    if tmdb_type == "tv" and tmdb_id and fribb.is_loaded():
        sibling_ids = fribb.lookup_siblings_by_tmdb_tv(int(tmdb_id))
        if len(sibling_ids) <= 1:
            sibling_ids = []

    seasons_summary = []
    if tmdb_type == "tv" and tmdb_details:
        seasons_summary = get_seasons_summary(
            anilist_id=anilist_id, tmdb_details=tmdb_details,
            fribb_data=fribb_data, sibling_anilist_ids=sibling_ids,
        )

    # Merge
    return _merge(
        anilist_id=anilist_id, anilist_data=anilist_data or {},
        fribb_data=fribb_data, tmdb_type=tmdb_type, tmdb_id=tmdb_id,
        tmdb_episodes=tmdb_episodes, tmdb_images=tmdb_images, tmdb_details=tmdb_details,
        resolver_method=resolver_method, resolver_tried=tmdb_resolution.get("tried_sources", []),
        pattern=detected_pattern, seasons_summary=seasons_summary,
        sibling_anilist_ids=sibling_ids, extras_mode=extras, include_upcoming=include_upcoming,
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
