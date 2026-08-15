"""
sources/anilist.py — AniList GraphQL client.

AniList is the primary anime database. We use it for:
- Title (English, romaji, native) — for TMDB search fallback
- Format (TV, MOVIE, OVA, etc.) — to branch TMDB endpoint
- Start year — to narrow TMDB search
- Episode count + nextAiringEpisode — for generating placeholder episodes
- Cover image — fallback poster when TMDB has nothing
- IDMal — for cross-reference

No API key required. Rate limit: 90 req/min (we only need 1 req/anime).
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

log = logging.getLogger("anilist")

ANILIST_URL = "https://graphql.anilist.co"
TIMEOUT = 15.0


async def fetch_anilist(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """
    Fetch anime metadata from AniList.
    Returns {} on failure (never raises).
    """
    gql = """
    query ($id: Int) {
        Media(id: $id, type: ANIME) {
            id
            idMal
            title { romaji english native }
            coverImage { large extraLarge color }
            bannerImage
            description(asHtml: false)
            format
            episodes
            duration
            status
            season
            seasonYear
            startDate { year month day }
            endDate { year month day }
            nextAiringEpisode { episode airingAt timeUntilAiring }
            genres
            averageScore
            meanScore
            popularity
            studios(isMain: true) { nodes { name } }
            externalLinks { url site type }
        }
    }
    """
    try:
        close = False
        if client is None:
            client = httpx.AsyncClient(timeout=TIMEOUT)
            close = True
        try:
            res = await client.post(
                ANILIST_URL,
                json={"query": gql, "variables": {"id": anilist_id}},
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "anime-metadata-api/1.0",
                    "Accept": "application/json",
                },
            )
            if res.status_code != 200:
                log.warning("AniList %d: HTTP %d", anilist_id, res.status_code)
                return {}
            data = res.json().get("data", {}).get("Media")
            if not data:
                return {}
            return data
        finally:
            if close:
                await client.aclose()
    except Exception as e:
        log.warning("AniList %d fetch failed: %s", anilist_id, e)
        return {}
