"""
sources/kitsu.py — Kitsu public API client.

Kitsu (https://kitsu.app) is another anime metadata provider with a
free JSON:API at https://kitsu.app/api/edge/.

Lookup strategy:
    Kitsu's public mappings endpoint lets us resolve an AniList ID → Kitsu ID,
    so we can do a clean cross-DB lookup without name-search.

    GET /edge/mappings?filter[externalSite]=anilist&filter[externalId]={id}&include=item

    (We also try the myanimelist/anilist filter pair as a fallback.)

Returns normalised dict. Never raises.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

log = logging.getLogger("kitsu")

KITSU_BASE = "https://kitsu.app/api/edge"
TIMEOUT = 15.0
UA = "anime-metadata-api/1.0"


async def fetch_kitsu_by_anilist(
    anilist_id: int,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """
    Resolve an AniList ID → Kitsu ID via the mappings endpoint, then fetch
    the full Kitsu anime record.

    Returns {} on any failure (never raises).

    Returned dict shape:
        {
            "kitsu_id": int,
            "title": str,
            "canonical_title": str,
            "titles": {"en": str, "en_jp": str, "ja_jp": str},
            "synopsis": str,
            "subtype": str,        # TV, movie, OVA, ONA, special, music
            "episode_count": int | None,   # ← Kitsu-reported episode count (verifier!)
            "episode_length": int | None,  # minutes
            "total_length": int | None,
            "status": str,         # current, finished, upcoming
            "start_date": str | None,
            "end_date": str | None,
            "season": str | None,  # winter|spring|summer|fall  ← season verifier!
            "year": int | None,
            "average_rating": float | None,
            "popularity_rank": int | None,
            "rating_rank": int | None,
            "age_rating": str | None,
            "poster_image": str | None,
            "cover_image": str | None,
            "source": "kitsu",
        }
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        # Step 1: lookup Kitsu ID via mappings
        kitsu_id = await _resolve_kitsu_id(anilist_id, client)
        if not kitsu_id:
            return {}

        # Step 2: fetch the anime record
        res = await client.get(
            f"{KITSU_BASE}/anime/{kitsu_id}",
            headers={"Accept": "application/vnd.api+json", "User-Agent": UA},
        )
        if res.status_code != 200:
            log.warning("Kitsu anime/%d: HTTP %d", kitsu_id, res.status_code)
            return {}
        data = res.json().get("data") or {}
        attrs = data.get("attributes") or {}
        return _normalise(kitsu_id, attrs)
    except Exception as e:
        log.warning("Kitsu %d failed: %s", anilist_id, e)
        return {}
    finally:
        if close:
            await client.aclose()


async def _resolve_kitsu_id(anilist_id: int, client: httpx.AsyncClient) -> Optional[int]:
    """Use Kitsu's /mappings endpoint to find the Kitsu anime ID for an AniList ID."""
    try:
        res = await client.get(
            f"{KITSU_BASE}/mappings",
            params={
                "filter[externalSite]": "anilist/anime",
                "filter[externalId]": str(anilist_id),
                "include": "item",
                "page[limit]": "5",
            },
            headers={"Accept": "application/vnd.api+json", "User-Agent": UA},
        )
        if res.status_code != 200:
            return None
        payload = res.json()
        # included[] contains the items referenced from mappings
        included = payload.get("included") or []
        for item in included:
            if item.get("type") == "anime":
                kid = item.get("id")
                if kid:
                    try:
                        return int(kid)
                    except (TypeError, ValueError):
                        pass
        # Sometimes the item link is on the mapping itself
        for m in payload.get("data") or []:
            rels = (m.get("relationships") or {}).get("item") or {}
            kid = rels.get("data", {}).get("id")
            if kid:
                try:
                    return int(kid)
                except (TypeError, ValueError):
                    pass
        return None
    except Exception as e:
        log.warning("Kitsu mapping lookup %d failed: %s", anilist_id, e)
        return None


def _normalise(kitsu_id: int, a: dict) -> dict:
    titles = a.get("titles") or {}
    return {
        "kitsu_id": kitsu_id,
        "title": a.get("canonicalTitle") or a.get("slug") or "",
        "canonical_title": a.get("canonicalTitle"),
        "titles": {
            "en": titles.get("en"),
            "en_jp": titles.get("en_jp"),
            "ja_jp": titles.get("ja_jp"),
        },
        "synopsis": a.get("synopsis"),
        "subtype": a.get("subtype"),
        "episode_count": a.get("episodeCount"),
        "episode_length": a.get("episodeLength"),
        "total_length": a.get("totalLength"),
        "status": a.get("status"),
        "start_date": a.get("startDate"),
        "end_date": a.get("endDate"),
        "season": a.get("season"),  # winter/spring/summer/fall
        "year": _year_from_date(a.get("startDate")),
        "average_rating": _safe_float(a.get("averageRating")),
        "popularity_rank": a.get("popularityRank"),
        "rating_rank": a.get("ratingRank"),
        "age_rating": a.get("ageRating"),
        "poster_image": a.get("posterImage", {}).get("large") if isinstance(a.get("posterImage"), dict) else None,
        "cover_image": a.get("coverImage", {}).get("large") if isinstance(a.get("coverImage"), dict) else None,
        "source": "kitsu",
    }


def _year_from_date(s: Optional[str]) -> Optional[int]:
    if not s or not isinstance(s, str):
        return None
    try:
        return int(s[:4])
    except (TypeError, ValueError):
        return None


def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def stats() -> dict:
    return {"api": KITSU_BASE}
