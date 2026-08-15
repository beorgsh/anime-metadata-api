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


async def _fetch_all_impl(
    anilist_id: int,
    *,
    season: Optional[int] = None,
    include_upcoming: bool = False,
    extras: bool = False,
    shared_client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """The actual fetch + slice + merge logic."""

    # ── Stage 1: AniList (combined Media + Relations in ONE call) + Fribb ──
    # Optimization: fetch_anilist_with_relations uses GraphQL aliases to get
    # both Media and its relations in ONE round-trip. Saves ~200ms+700ms = 900ms
    # vs the old two-call approach.
    # Use the shared client if provided (saves TLS handshake).
    if shared_client is not None:
        anilist_data, anilist_relations = await anilist.fetch_anilist_with_relations(
            anilist_id, shared_client
        )
    else:
        async with httpx.AsyncClient(timeout=15.0) as client:
            anilist_data, anilist_relations = await anilist.fetch_anilist_with_relations(anilist_id, client)

    fribb_data = fribb.lookup(anilist_id) if fribb.is_loaded() else None
    fribb_season = None
    fribb_offset = 0
    if fribb_data:
        fribb_season = (fribb_data.get('season') or {}).get('tmdb')
        fribb_offset = (fribb_data.get('episode_offset') or {}).get('tmdb') or 0

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
    anilist_relations_used = False
    anilist_next_airing = None
    _nae = (anilist_data or {}).get('nextAiringEpisode') or {}
    if isinstance(_nae, dict):
        anilist_next_airing = _nae.get('episode')

    if tmdb_type == "tv" and tmdb_id:
        target_seasons = []
        season_results = []
        # Use a context manager only if we don't have a shared client
        async def _do_tmdb_fetch(client: httpx.AsyncClient):
            nonlocal tmdb_episodes, tmdb_images, tmdb_details, slice_offset, slice_count, continuous_numbering, pattern, anilist_relations_used, target_seasons, season_results

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
                # ── Decision tree (v4.1 — lazy chain walk) ──
                # First, try resolve_target_season WITHOUT the calculated offset.
                # Most cases (Pattern A_fribb, A_title, B, C) don't need it.
                # Only if Pattern A_chain would be selected do we actually walk
                # the prequel chain (saving 1-3 AniList calls for the common case).
                decision = resolve_target_season(
                    anilist_id=anilist_id,
                    anilist_data=anilist_data,
                    fribb_data=fribb_data,
                    tmdb_details=tmdb_details,
                    explicit_season=season,
                    calculated_offset=None,  # try without chain offset first
                )

                # AniZip (TVDB) fallback for Pattern A critical cases

                anizip_provided = False

                _is_pa = bool(fribb_offset) or bool(fribb_season)

                if not _is_pa:

                    _te = (anilist_data.get('title') or {}).get('english') or ''

                    _tr = (anilist_data.get('title') or {}).get('romaji') or ''

                    from seasons import detect_season_from_title as _dst

                    if _dst(_te) or _dst(_tr):

                        _is_pa = True

                if _is_pa:

                    try:

                        _az = await _fetch_anizip(anilist_id, client)

                        if _az and _az.get('episodes'):

                            _az_eps = _az['episodes']

                            if len(_az_eps) >= 1:

                                pattern = 'pattern_a_anizip'

                                tmdb_episodes = list(_az_eps)

                                if anilist_next_airing and anilist_next_airing > 0:

                                    _td = time.strftime('%Y-%m-%d')

                                    tmdb_episodes = [ep for ep in tmdb_episodes

                                        if ep.get('number', 0) < anilist_next_airing

                                        or (ep.get('airDate', '') and ep.get('airDate') <= _td)]

                                slice_offset = 0

                                slice_count = None

                                continuous_numbering = False

                                anizip_provided = True

                    except Exception as e:

                        log.warning('AniZip fetch for %d failed: %s', anilist_id, e)


                # Quick check: do we need the chain offset?
                title_en = (anilist_data.get("title") or {}).get("english") or ""
                title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""
                from seasons import detect_season_from_title
                title_season = detect_season_from_title(title_en) or detect_season_from_title(title_romaji)

                fribb_has_offset = (
                    fribb_data and
                    (fribb_data.get("episode_offset") or {}).get("tmdb")
                )
                needs_chain_offset = (
                    not fribb_has_offset
                    and title_season is not None
                    and decision["pattern"] in ("pattern_c", "pattern_b")
                )

                if needs_chain_offset:
                    # Walk the prequel chain to calculate the offset
                    # (use the relations we already fetched in Stage 1 — saves another call)
                    offset_cache_key = f"chain_offset:{anilist_id}"
                    cached_offset = offset_cache.get(offset_cache_key)
                    if cached_offset is not None:
                        calculated_offset = cached_offset.get("offset")
                    else:
                        try:
                            offset_result = await anilist.calculate_chain_offset(
                                anilist_id,
                                client,
                                known_relations=anilist_relations,  # reuse Stage 1 data
                            )
                            calculated_offset = offset_result.get("offset") or 0
                            offset_cache.set(offset_cache_key, offset_result)
                            anilist_relations_used = True
                        except Exception as e:
                            log.warning("Chain offset calc for %d failed: %s", anilist_id, e)
                            calculated_offset = None

                    # Re-run resolve_target_season with the calculated offset
                    if calculated_offset and calculated_offset > 0:
                        decision = resolve_target_season(
                            anilist_id=anilist_id,
                            anilist_data=anilist_data,
                            fribb_data=fribb_data,
                            tmdb_details=tmdb_details,
                            explicit_season=season,
                            calculated_offset=calculated_offset,
                        )

                target_seasons = decision["season_numbers"]
                slice_offset = decision["episode_offset"]
                slice_count = decision["episode_count"]
                pattern = decision["pattern"]
                continuous_numbering = decision["continuous_numbering"]

            if anizip_provided:
                target_seasons = []
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
                    # Tag each episode with its season number
                    for ep in season_eps:
                        ep["season"] = sn
                    tmdb_episodes.extend(season_eps)

            images_data = await images_task
            if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                tmdb_images = tmdb.extract_images(images_data, "tv")

        if shared_client is not None:
            await _do_tmdb_fetch(shared_client)
        else:
            async with httpx.AsyncClient(timeout=15.0) as client:
                await _do_tmdb_fetch(client)

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
    # AniList's nextAiringEpisode.episode tells us what's "next to air" — so
    # episodes 1..next_airing-1 are the actually-aired ones. We pass this as a
    # cap to slice_episodes so we don't return TMDB's extra episodes that have
    # future air dates (e.g. Doraemon 2005: AniList says next=929, TMDB has
    # 1464 "aired" episodes including recaps).
    anilist_next_airing = None
    nae = (anilist_data or {}).get("nextAiringEpisode") or {}
    if isinstance(nae, dict):
        anilist_next_airing = nae.get("episode")

    if tmdb_type == "tv":
        sliced = slice_episodes(
            tmdb_episodes,
            offset=slice_offset,
            count=slice_count,
            continuous_numbering=continuous_numbering,
            include_upcoming=include_upcoming,
            anilist_id=anilist_id,
            anilist_next_airing=anilist_next_airing,
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
