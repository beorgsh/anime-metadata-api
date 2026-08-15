"""
aggregator.py — Fetches metadata from all sources IN PARALLEL, then merges
and verifies it.

Pipeline
========

  Stage 1 — Parallel fetch
    ┌─ AniList GraphQL  (primary metadata: title, format, episodes, season)
    ├─ AniZip           (TVDB episodes + images)
    ├─ AniBridge        (cross-provider mapping + verified `units` count)
    └─ Jikan (MAL)      (MAL episode count + season + status verifier)

    Kitsu is fetched in Stage 2 (only if AniList returned a mal_id OR if we
    need an extra verifier). Kitsu's /mappings endpoint requires a known
    external ID, so it goes after we have one.

  Stage 2 — TMDB resolution
    resolver.resolve_tmdb_id() is now multi-source (AniBridge → Fribb →
    TMDB /find → TMDB /search).

  Stage 3 — TMDB episode + image fetch (if resolved)

  Stage 4 — Optional Kitsu fetch (parallel with TMDB)

  Stage 5 — Cross-source verification
    verifier.verify_episode_count() — own-logic majority vote
    verifier.verify_season()         — own-logic majority vote (AniList primary)
    verifier.verify_status()         — own-logic majority vote

  Stage 6 — Merge

Merge priorities (images):  TMDB > AniZip > AniList
Merge priorities (episodes): TMDB > AniZip > generated placeholder
Merge priorities (titles):  AniList > AniZip > AniBridge
Merge priorities (mappings): combined — every source's mappings are surfaced
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

from cache import metadata_cache
from sources import anilist, anizip, fribb, tmdb, anibridge, jikan, kitsu
from resolver import resolve_tmdb_id
from verifier import verify_episode_count, verify_season, verify_status

log = logging.getLogger("aggregator")


async def fetch_all(anilist_id: int) -> dict:
    """
    Fetch metadata for an AniList ID from all sources in parallel, verify
    it, and return the unified response. Cached for 7 days.
    """
    cache_key = f"meta:{anilist_id}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    # Per-id lock so concurrent requests dedupe
    lock = metadata_cache.get_lock(cache_key)
    async with lock:
        cached = metadata_cache.get(cache_key)
        if cached is not None:
            return cached

        result = await _fetch_all_impl(anilist_id)
        metadata_cache.set(cache_key, result)
        return result


async def _fetch_all_impl(anilist_id: int) -> dict:
    """The actual fetch + verify + merge logic."""

    # ── Stage 1: Parallel fetch of independent sources ────────────
    async with httpx.AsyncClient(timeout=15.0) as client:
        anilist_task = asyncio.create_task(anilist.fetch_anilist(anilist_id, client))
        anizip_task = asyncio.create_task(anizip.fetch_anizip(anilist_id, client))
        anibridge_task = asyncio.create_task(anibridge.fetch_cross_mappings(anilist_id, client))

        anilist_data, anizip_data, anibridge_data = await asyncio.gather(
            anilist_task, anizip_task, anibridge_task,
        )

    # Jikan needs a MAL ID — pull from AniList.idMal or AniBridge.cross_ids.mal
    mal_id = (anilist_data or {}).get("idMal")
    if (not mal_id) and anibridge_data:
        mal_ids = (anibridge_data.get("cross_ids") or {}).get("mal") or []
        if mal_ids:
            mal_id = mal_ids[0]

    jikan_data: dict = {}
    if mal_id:
        async with httpx.AsyncClient(timeout=15.0) as client:
            jikan_data = await jikan.fetch_jikan(mal_id, client)

    # ── Stage 2: Resolve TMDB ID (multi-source) ─────────────────────
    tmdb_resolution = await resolve_tmdb_id(
        anilist_id,
        anilist_data=anilist_data,
        anibridge_data=anibridge_data,
    )
    tmdb_type = tmdb_resolution.get("tmdb_type")
    tmdb_id = tmdb_resolution.get("tmdb_id")
    fribb_data = tmdb_resolution.get("fribb_data")

    # ── Stages 3 + 4: TMDB fetch + Kitsu fetch (parallel) ──────────
    tmdb_episodes: list[dict] = []
    tmdb_images: list[dict] = []
    tmdb_details: dict = {}

    kitsu_task: Optional[asyncio.Task] = None
    if mal_id or anilist_id:
        # Kitsu can resolve by AniList ID directly, so we always try it
        kitsu_task = asyncio.create_task(_fetch_kitsu_safe(anilist_id))

    if tmdb_type and tmdb_id:
        async with httpx.AsyncClient(timeout=15.0) as client:
            if tmdb_type == "tv":
                # Determine season(s) from Fribb's season.tmdb field (if available)
                fribb_season = None
                if fribb_data:
                    season_info = fribb_data.get("season") or {}
                    fribb_season = season_info.get("tmdb")

                details_task = asyncio.create_task(tmdb.fetch_tv(tmdb_id, client))
                images_task = asyncio.create_task(tmdb.fetch_tv_images(tmdb_id, client))

                if fribb_season is not None:
                    season_task = asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, fribb_season, client))
                    season_data, images_data, details = await asyncio.gather(
                        season_task, images_task, details_task, return_exceptions=True
                    )
                    if isinstance(season_data, dict) and not isinstance(season_data, Exception):
                        tmdb_episodes = tmdb.extract_episodes(season_data, anilist_id)
                else:
                    details = await details_task
                    if isinstance(details, dict) and details:
                        all_seasons = [s["season_number"] for s in details.get("seasons", [])
                                       if s.get("season_number", 0) > 0]
                        if all_seasons:
                            season_tasks = [
                                asyncio.create_task(tmdb.fetch_tv_season(tmdb_id, s, client))
                                for s in all_seasons
                            ]
                            season_results = await asyncio.gather(*season_tasks, return_exceptions=True)
                            episode_offset = 0
                            for i, sd in enumerate(season_results):
                                if isinstance(sd, dict) and not isinstance(sd, Exception):
                                    season_eps = tmdb.extract_episodes(sd, anilist_id)
                                    for ep in season_eps:
                                        ep["number"] = ep["number"] + episode_offset
                                        ep["season"] = all_seasons[i]
                                        ep["season_episode"] = ep["number"] - episode_offset
                                    tmdb_episodes.extend(season_eps)
                                    episode_offset += len(season_eps)
                    images_data = await images_task

                if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                    tmdb_images = tmdb.extract_images(images_data, "tv")
                if isinstance(details, dict) and not isinstance(details, Exception):
                    tmdb_details = details
            elif tmdb_type == "movie":
                details_task = asyncio.create_task(tmdb.fetch_movie(tmdb_id, client))
                images_task = asyncio.create_task(tmdb.fetch_movie_images(tmdb_id, client))
                details, images_data = await asyncio.gather(details_task, images_task, return_exceptions=True)
                if isinstance(images_data, dict) and not isinstance(images_data, Exception):
                    tmdb_images = tmdb.extract_images(images_data, "movie")
                if isinstance(details, dict) and not isinstance(details, Exception):
                    tmdb_details = details

    # Await Kitsu (always best-effort)
    kitsu_data: dict = {}
    if kitsu_task is not None:
        kitsu_data = await kitsu_task

    # ── Stage 5: Cross-source verification ─────────────────────────
    tmdb_episode_count = len(tmdb_episodes) if tmdb_episodes else None
    anilist_episodes = (anilist_data or {}).get("episodes")
    anibridge_units = (anibridge_data or {}).get("units")
    jikan_episodes = (jikan_data or {}).get("episodes")
    kitsu_episode_count = (kitsu_data or {}).get("episode_count")
    anizip_episode_count = anizip_data.get("episodeCount") or len(anizip_data.get("episodes") or [])

    episode_verdict = verify_episode_count(
        anilist_episodes=anilist_episodes,
        anibridge_units=anibridge_units,
        jikan_episodes=jikan_episodes,
        kitsu_episode_count=kitsu_episode_count,
        anizip_episode_count=anizip_episode_count or None,
        tmdb_episode_count=tmdb_episode_count,
    )

    anilist_season = (anilist_data or {}).get("season")
    anilist_season_year = (anilist_data or {}).get("seasonYear")
    anilist_start_year = ((anilist_data or {}).get("startDate") or {}).get("year")
    anibridge_release = (anibridge_data or {}).get("release")
    jikan_season = (jikan_data or {}).get("season")
    jikan_year = (jikan_data or {}).get("year")
    kitsu_season = (kitsu_data or {}).get("season")
    kitsu_year = (kitsu_data or {}).get("year")

    season_verdict = verify_season(
        anilist_season=anilist_season,
        anilist_year=anilist_season_year,
        anibridge_release=anibridge_release,
        jikan_season=jikan_season,
        jikan_year=jikan_year,
        kitsu_season=kitsu_season,
        kitsu_year=kitsu_year,
        fallback_year=anilist_start_year,
    )

    status_verdict = verify_status(
        anilist_status=(anilist_data or {}).get("status"),
        anibridge_release=anibridge_release,
        jikan_status=(jikan_data or {}).get("status"),
        kitsu_status=(kitsu_data or {}).get("status"),
    )

    # ── Stage 6: Merge everything ──────────────────────────────────
    return _merge(
        anilist_id=anilist_id,
        anilist_data=anilist_data or {},
        anizip_data=anizip_data,
        fribb_data=fribb_data,
        anibridge_data=anibridge_data or {},
        jikan_data=jikan_data,
        kitsu_data=kitsu_data,
        tmdb_type=tmdb_type,
        tmdb_id=tmdb_id,
        tmdb_episodes=tmdb_episodes,
        tmdb_images=tmdb_images,
        tmdb_details=tmdb_details,
        resolver_method=tmdb_resolution.get("method", "not_found"),
        resolver_tried=tmdb_resolution.get("tried_sources", []),
        episode_verdict=episode_verdict,
        season_verdict=season_verdict,
        status_verdict=status_verdict,
    )


async def _fetch_kitsu_safe(anilist_id: int) -> dict:
    """Best-effort Kitsu fetch — never raises."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            return await kitsu.fetch_kitsu_by_anilist(anilist_id, client)
    except Exception as e:
        log.warning("Kitsu fetch %d failed: %s", anilist_id, e)
        return {}


# ─── Merge ────────────────────────────────────────────────────────────


def _merge(
    *,
    anilist_id: int,
    anilist_data: dict,
    anizip_data: dict,
    fribb_data: Optional[dict],
    anibridge_data: dict,
    jikan_data: dict,
    kitsu_data: dict,
    tmdb_type: Optional[str],
    tmdb_id,
    tmdb_episodes: list[dict],
    tmdb_images: list[dict],
    tmdb_details: dict,
    resolver_method: str,
    resolver_tried: list,
    episode_verdict: dict,
    season_verdict: dict,
    status_verdict: dict,
) -> dict:
    """Merge all sources into a unified response."""

    # ── Titles ──
    title_en = (anilist_data.get("title") or {}).get("english") or anizip_data.get("title") or ""
    title_ja = (anilist_data.get("title") or {}).get("native") or anizip_data.get("titleJa") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""

    # ── Format + year ──
    fmt = anilist_data.get("format", "TV")
    year = (anilist_data.get("startDate") or {}).get("year")
    mal_id = anilist_data.get("idMal") or (fribb_data or {}).get("mal_id") or _first(anibridge_data.get("cross_ids", {}).get("mal"))

    # ── Episode count (use the verdict) ──
    total_eps = episode_verdict.get("value") or 0

    # ── Next airing ──
    next_airing = anilist_data.get("nextAiringEpisode") or {}
    next_ep = next_airing.get("episode") if next_airing else None

    # ── Current episode (last aired) ──
    current_ep = None
    today = time.strftime("%Y-%m-%d")
    all_eps = tmdb_episodes or anizip_data.get("episodes", [])
    for ep in all_eps:
        if ep.get("hasAired"):
            current_ep = ep.get("number")
        else:
            break

    # ── Images ──
    images_by_type: dict[str, dict] = {}
    for img in tmdb_images + anizip_data.get("images", []):
        ct = img.get("coverType")
        if ct and ct not in images_by_type:
            images_by_type[ct] = img
    # AniList cover fallback Poster
    if "Poster" not in images_by_type and anilist_data.get("coverImage"):
        cover_url = anilist_data["coverImage"].get("extraLarge") or anilist_data["coverImage"].get("large")
        if cover_url:
            images_by_type["Poster"] = {"coverType": "Poster", "url": cover_url, "source": "anilist"}
    # AniList banner fallback Banner
    if "Banner" not in images_by_type and anilist_data.get("bannerImage"):
        images_by_type["Banner"] = {"coverType": "Banner", "url": anilist_data["bannerImage"], "source": "anilist"}
    # AniBridge images (extra fallbacks)
    for img in anibridge_data.get("images") or []:
        ct = _anibridge_image_kind(img.get("kind"))
        url = img.get("url")
        if ct and url and ct not in images_by_type:
            images_by_type[ct] = {"coverType": ct, "url": url, "source": "anibridge"}
    # Jikan image fallback Poster
    if "Poster" not in images_by_type and jikan_data.get("image_url"):
        images_by_type["Poster"] = {"coverType": "Poster", "url": jikan_data["image_url"], "source": "jikan"}
    # Kitsu poster fallback
    if "Poster" not in images_by_type and kitsu_data.get("poster_image"):
        images_by_type["Poster"] = {"coverType": "Poster", "url": kitsu_data["poster_image"], "source": "kitsu"}

    images = list(images_by_type.values())

    # ── Episodes (prefer TMDB, then AniZip) ──
    episodes = tmdb_episodes or anizip_data.get("episodes", [])

    # If still no episodes but we have total_eps, generate placeholders
    if not episodes and total_eps:
        episodes = _generate_placeholder_episodes(anilist_id, total_eps, anilist_data, next_ep)

    # ── Mappings ──
    mappings: dict = {}

    # From AniZip (TVDB-centred cross-refs)
    anizip_mappings = anizip_data.get("mappings", {}) or {}
    mappings.update(anizip_mappings)

    # From Fribb (canonical AniList↔* table)
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
        # Only overwrite null entries — AniZip already had a chance
        for k, v in fribb_mappings.items():
            if v is not None and not mappings.get(k):
                mappings[k] = v

    # From AniBridge (cross-provider verified, most authoritative)
    if anibridge_data:
        cross = anibridge_data.get("cross_ids") or {}
        ab_map = {
            "anilist_id": anilist_id,
            "mal_id": _first(cross.get("mal")),
            "anidb_id": _first(cross.get("anidb")),
            "kitsu_id": _first(cross.get("kitsu")),
            "thetvdb_id": _first(cross.get("tvdb_show")) or _first(cross.get("tvdb_movie")),
            "themoviedb_id": _tmdb_obj(cross),
        }
        for k, v in ab_map.items():
            if v is not None and not mappings.get(k):
                mappings[k] = v

    # Add the resolved TMDB ID for visibility
    if tmdb_id:
        mappings["themoviedb_id_resolved"] = {"type": tmdb_type, "id": tmdb_id, "method": resolver_method}

    # ── Description ──
    description = (
        anilist_data.get("description")
        or anibridge_data.get("synopsis")
        or jikan_data.get("synopsis")
        or kitsu_data.get("synopsis")
        or ""
    )

    # ── Genres + studios ──
    genres = list(dict.fromkeys(  # dedupe preserving order
        (anilist_data.get("genres") or [])
        + (jikan_data.get("genres") or [])
        + (anibridge_data.get("genres") or [])
    ))
    studios = list(dict.fromkeys(
        [s["name"] for s in (anilist_data.get("studios") or {}).get("nodes", [])]
        + (jikan_data.get("studios") or [])
    ))

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
        # Verified fields (the "own brain" output)
        "totalEpisodes": total_eps,
        "episodeVerification": episode_verdict,
        "season": season_verdict.get("season"),
        "seasonYear": season_verdict.get("year"),
        "seasonVerification": season_verdict,
        "status": status_verdict.get("value"),
        "statusVerification": status_verdict,
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
            "anibridge": bool(anibridge_data.get("anibridge_meta")),
            "jikan": bool(jikan_data),
            "kitsu": bool(kitsu_data),
            "tmdb": bool(tmdb_id),
            "tmdb_type": tmdb_type,
            "resolver_method": resolver_method,
            "resolver_tried": resolver_tried,
        },
        "verification": {
            "episodes": episode_verdict,
            "season": season_verdict,
            "status": status_verdict,
            "summary": {
                "episode_count_agreed": episode_verdict.get("confidence") == "high",
                "season_agreed": season_verdict.get("confidence") == "high",
                "status_agreed": status_verdict.get("confidence") == "high",
                "sources_with_episode_data": [
                    s for s, v in (episode_verdict.get("all_sources") or {}).items() if v
                ],
                "sources_with_season_data": [
                    s for s, v in (season_verdict.get("all_seasons") or {}).items() if v
                ],
            },
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


def _first(seq) -> Optional[object]:
    """Return the first element of a list (or scalar), or None."""
    if seq is None:
        return None
    if isinstance(seq, (list, tuple)):
        return seq[0] if seq else None
    return seq


def _tmdb_obj(cross: dict) -> Optional[dict]:
    """Build a themoviedb_id object that mirrors Fribb's shape."""
    if not cross:
        return None
    tv = cross.get("tmdb_show") or []
    movie = cross.get("tmdb_movie") or []
    out = {}
    if tv:
        out["tv"] = tv[0]
    if movie:
        out["movie"] = movie[0]
    return out or None


def _anibridge_image_kind(kind: Optional[str]) -> Optional[str]:
    """Map AniBridge image kinds → our standard coverType."""
    if not kind:
        return None
    k = kind.lower()
    if k == "poster":
        return "Poster"
    if k in ("banner", "backdrop"):
        return "Banner"
    if k in ("logo", "clearlogo"):
        return "Clearlogo"
    if k in ("fanart", "background"):
        return "Fanart"
    return None
