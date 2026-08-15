"""
sources/anilist.py — AniList GraphQL client (v4.1 — speed-optimized).

Key optimizations:
  - SINGLE GraphQL query using aliases fetches both Media (title, episodes,
    status, etc.) AND its relations (prequels/sequels) in ONE round-trip.
    This eliminates the separate fetch_relations() call.
  - The artificial per-request delay (0.7s) was REMOVED from the main fetch.
    It was overly conservative — for a single user request that makes just
    1 AniList call, the 30 req/min limit is nowhere near saturated.
  - The chain walk still uses a small delay (0.2s) to dodge the burst limiter.
  - Shared httpx.AsyncClient at the app level (reused across requests) — saves
    TLS handshake overhead.

Rate limit context (verified by research):
  - AniList GraphQL at graphql.anilist.co allows 30 req/min per IP (degraded
    from the normal 90/min — currently active per AniList admin).
  - NO array-batching support — but GraphQL aliases DO work for combining
    multiple top-level queries into one request.
  - Burst limiter catches rapid-fire requests even under the per-minute cap.
  - NO alternative endpoints, NO token bypass, NO community proxy.

No API key required.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

log = logging.getLogger("anilist")

ANILIST_URL = "https://graphql.anilist.co"
TIMEOUT = 15.0

# AniList allows 30 req/min per IP (currently degraded from 90/min).
# For SINGLE-user requests (1 AniList call), we don't need to throttle — the
# limit is nowhere near saturated. We DO throttle the chain walk (which makes
# multiple sequential calls) to dodge the burst limiter.
_CHAIN_WALK_DELAY = 0.2  # seconds between chain-walk requests

# Module-level semaphore — limits concurrent AniList calls across all coroutines.
# 5 concurrent is safe for stress tests (60 req/min sustained, well under 30/min
# per the per-IP window... actually wait, 5 concurrent × 1 call each burst is
# fine because the per-IP window is 30/min = 1 every 2s sustained).
_ANILIST_SEM = asyncio.Semaphore(5)


async def _anilist_post(client: httpx.AsyncClient, query: str, variables: dict,
                        retries: int = 3) -> Optional[dict]:
    """Make an AniList GraphQL POST with retry-on-429 (no artificial delay)."""
    async with _ANILIST_SEM:
        for attempt in range(retries + 1):
            try:
                res = await client.post(
                    ANILIST_URL,
                    json={"query": query, "variables": variables},
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "anime-metadata-api/4.1 (+https://github.com/historiesofhistory-arch/anime-metadata-api)",
                        "Accept": "application/json",
                    },
                )
                if res.status_code == 200:
                    return res.json()
                if res.status_code == 429:
                    # Rate-limited — exponential backoff
                    wait = 2 ** (attempt + 1)
                    log.warning("AniList 429 rate-limited, waiting %ds", wait)
                    await asyncio.sleep(wait)
                    continue
                if res.status_code in (500, 502, 503, 504):
                    wait = 1 * (attempt + 1)
                    log.warning("AniList %d, retrying in %ds", res.status_code, wait)
                    await asyncio.sleep(wait)
                    continue
                log.warning("AniList HTTP %d: %s", res.status_code, res.text[:200])
                return None
            except Exception as e:
                log.warning("AniList fetch error (attempt %d): %s", attempt + 1, e)
                await asyncio.sleep(1)
        return None


# ─── Combined main + relations query ─────────────────────────────────
# Uses GraphQL aliases to fetch Media + its relations in ONE round-trip.
# This is the speed-optimized single-call path.
_MEDIA_WITH_RELATIONS_QUERY = """
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
    Relations: Media(id: $id, type: ANIME) {
        id
        relations {
            edges {
                relationType(version: 2)
                node {
                    id
                    title { romaji english native }
                    format
                    episodes
                    type
                    startDate { year }
                }
            }
        }
    }
}
"""


async def fetch_anilist_with_relations(
    anilist_id: int,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[dict, dict]:
    """Fetch anime metadata AND its relations in ONE GraphQL round-trip.

    Returns (anilist_data, relations_data).
    Both default to {} on any failure (never raises).

    This is the FAST path — single HTTP request, no artificial delay.
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        payload = await _anilist_post(client, _MEDIA_WITH_RELATIONS_QUERY, {"id": anilist_id})
        if not payload:
            return {}, _empty_relations(anilist_id)
        data = payload.get("data", {})
        media = data.get("Media") or {}
        relations_media = data.get("Relations") or {}
        edges = (relations_media.get("relations") or {}).get("edges") or []
        relations = _normalise_relations(anilist_id, edges)
        return media, relations
    finally:
        if close:
            await client.aclose()


async def fetch_anilist(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch anime metadata from AniList. Returns {} on failure (never raises).

    NOTE: For maximum speed, prefer fetch_anilist_with_relations() which gets
    both Media + Relations in one call. This function is kept for backwards
    compatibility.
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
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        payload = await _anilist_post(client, gql, {"id": anilist_id})
        if not payload:
            return {}
        data = payload.get("data", {}).get("Media")
        return data or {}
    finally:
        if close:
            await client.aclose()


async def fetch_relations(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch this anime's relations (prequels/sequels/etc.).

    NOTE: For maximum speed, prefer fetch_anilist_with_relations() which gets
    both Media + Relations in one call. This function is kept for backwards
    compatibility and for chain walk (where we only need relations).
    """
    gql = """
    query ($id: Int) {
        Media(id: $id, type: ANIME) {
            id
            relations {
                edges {
                    relationType(version: 2)
                    node {
                        id
                        title { romaji english native }
                        format
                        episodes
                        type
                        startDate { year }
                    }
                }
            }
        }
    }
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        payload = await _anilist_post(client, gql, {"id": anilist_id})
        if not payload:
            return _empty_relations(anilist_id)
        media = payload.get("data", {}).get("Media") or {}
        edges = (media.get("relations") or {}).get("edges") or []
        return _normalise_relations(anilist_id, edges)
    finally:
        if close:
            await client.aclose()


def _normalise_relations(anilist_id: int, edges: list) -> dict:
    """Convert raw AniList relations.edges into a normalised dict."""
    prequels: list[dict] = []
    sequels: list[dict] = []
    side_stories: list[dict] = []
    all_anime: list[dict] = []

    for edge in edges:
        rel_type = edge.get("relationType")
        node = edge.get("node") or {}
        if node.get("type") != "ANIME":
            continue
        entry = {
            "anilist_id": node.get("id"),
            "title_en": (node.get("title") or {}).get("english"),
            "title_romaji": (node.get("title") or {}).get("romaji"),
            "title_native": (node.get("title") or {}).get("native"),
            "episodes": node.get("episodes"),
            "format": node.get("format"),
            "type": node.get("type"),
            "start_year": (node.get("startDate") or {}).get("year"),
            "relation": rel_type,
        }
        all_anime.append(entry)
        if rel_type == "PREQUEL":
            prequels.append(entry)
        elif rel_type == "SEQUEL":
            sequels.append(entry)
        elif rel_type == "SIDE_STORY":
            side_stories.append(entry)

    return {
        "anilist_id": anilist_id,
        "prequels": prequels,
        "sequels": sequels,
        "side_stories": side_stories,
        "all_anime_relations": all_anime,
    }


def _empty_relations(anilist_id: int) -> dict:
    return {
        "anilist_id": anilist_id,
        "prequels": [],
        "sequels": [],
        "side_stories": [],
        "all_anime_relations": [],
    }


async def trace_prequel_chain(
    anilist_id: int,
    client: Optional[httpx.AsyncClient] = None,
    max_depth: int = 10,
    known_relations: Optional[dict] = None,
) -> list[dict]:
    """Walk the PREQUEL chain from this anime back to the root season.

    Args:
        known_relations: if you've already fetched relations for anilist_id,
            pass them here to avoid a redundant API call (saves ~200ms).

    Returns a list of prequel entries, ordered from the IMMEDIATE prequel
    (index 0) to the ROOT season (last index).

    The chain stops when:
      - We hit max_depth
      - A node has no PREQUEL
      - We loop back to a visited node (cycle detection)

    Never raises — returns what it has on error.
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        chain: list[dict] = []
        visited: set[int] = {anilist_id}
        current_id = anilist_id
        current_relations = known_relations

        for depth in range(max_depth):
            if current_relations is None:
                current_relations = await fetch_relations(current_id, client)
                await asyncio.sleep(_CHAIN_WALK_DELAY)
            prequels = current_relations.get("prequels") or []
            if not prequels:
                break  # this is the root season

            # Take the first prequel (most common case — one prequel per entry).
            prequel = _pick_best_prequel(prequels)
            if not prequel:
                break

            next_id = prequel.get("anilist_id")
            if not next_id or next_id in visited:
                break  # cycle detected

            chain.append(prequel)
            visited.add(next_id)
            current_id = next_id
            current_relations = None  # force re-fetch for the next iteration

        return chain
    finally:
        if close:
            await client.aclose()


def _pick_best_prequel(prequels: list[dict]) -> Optional[dict]:
    """Pick the best prequel from a list of prequel relations.

    Preference order:
      1. TV format
      2. ONA format
      3. Anything else (last resort)
    """
    if not prequels:
        return None
    for fmt in ("TV", "ONA"):
        for p in prequels:
            if p.get("format") == fmt:
                return p
    return prequels[0]


async def calculate_chain_offset(
    anilist_id: int,
    client: Optional[httpx.AsyncClient] = None,
    known_relations: Optional[dict] = None,
) -> dict:
    """Calculate the episode offset for an AniList entry by walking its prequel chain.

    Args:
        known_relations: if you've already fetched relations for anilist_id,
            pass them here to skip a redundant API call.

    Returns:
        {
            "anilist_id": int,
            "offset": int,                    # total episodes from all prequels
            "chain": [...],                   # the prequel chain (root last)
            "total_prequel_episodes": int,    # same as offset (alias)
            "prequel_count": int,
            "warnings": [str, ...],           # any issues encountered
        }
    """
    chain = await trace_prequel_chain(anilist_id, client, known_relations=known_relations)
    offset = 0
    warnings: list[str] = []

    for prequel in chain:
        eps = prequel.get("episodes")
        if eps is None:
            warnings.append(
                f"Prequel AniList {prequel.get('anilist_id')} "
                f"({prequel.get('title_en') or prequel.get('title_romaji')}) has episodes=null "
                f"(likely ongoing) — skipping from offset sum"
            )
            continue
        try:
            eps_int = int(eps)
            if eps_int > 0:
                offset += eps_int
        except (TypeError, ValueError):
            warnings.append(f"Prequel AniList {prequel.get('anilist_id')} has invalid episodes={eps!r}")

    return {
        "anilist_id": anilist_id,
        "offset": offset,
        "chain": chain,
        "total_prequel_episodes": offset,
        "prequel_count": len(chain),
        "warnings": warnings,
    }


def stats() -> dict:
    """For /health endpoint."""
    return {
        "api": ANILIST_URL,
        "rate_limit": "30 req/min per IP (degraded from 90/min — active per AniList admin)",
        "concurrency": 5,
        "chain_walk_delay_s": _CHAIN_WALK_DELAY,
        "supports_array_batching": False,
        "supports_aliases": True,  # we use this to combine Media + Relations
    }
