"""
sources/anilist.py — AniList GraphQL client (v4).

In v4, AniList is the BACKBONE for season/episode mapping. We use it for:
- Title (English, romaji, native) — used for TMDB search
- Format (TV, MOVIE, OVA, etc.) — branch TMDB endpoint
- Start year — narrow TMDB search
- Episode count + nextAiringEpisode — for slicing + verification
- Cover image — fallback poster
- IDMal — for cross-reference
- Relations (PREQUEL / SEQUEL chain) — to compute episode offsets when Fribb
  has no mapping (e.g. Hell Mode S2 → trace prequel → S1 episodes=12 → offset=12)

Rate limit: AniList GraphQL allows 90 req/min per IP. We use a Semaphore
(concurrency=5) + retry-with-backoff on 429 to stay safely under the limit.

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

# Concurrency control: AniList allows 90 req/min = 1.5 req/sec per IP.
# With concurrency=5 and average response time of 200ms, we hit ~25 req/sec
# sustained which is way over the limit. We add a 700ms delay between
# requests per worker to stay safely under.
_CONCURRENCY_LIMIT = 5
_PER_REQUEST_DELAY = 0.7  # seconds

# Module-level semaphore so all callers share the rate limit
_sem = asyncio.Semaphore(_CONCURRENCY_LIMIT)


async def _anilist_post(client: httpx.AsyncClient, query: str, variables: dict,
                        retries: int = 3) -> Optional[dict]:
    """Make an AniList GraphQL POST with rate-limit awareness."""
    async with _sem:
        for attempt in range(retries + 1):
            try:
                await asyncio.sleep(_PER_REQUEST_DELAY)
                res = await client.post(
                    ANILIST_URL,
                    json={"query": query, "variables": variables},
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "anime-metadata-api/4.0 (+https://github.com/historiesofhistory-arch/anime-metadata-api)",
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


async def fetch_anilist(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch anime metadata from AniList. Returns {} on failure (never raises)."""
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

    Returns a normalised dict:
        {
            "anilist_id": int,
            "prequels":  [{"anilist_id", "title_en", "title_romaji", "episodes", "format", "type"}, ...],
            "sequels":   [{"anilist_id", "title_en", "title_romaji", "episodes", "format", "type"}, ...],
            "side_stories": [...],
            "all_anime_relations": [...],
        }

    Only includes relations where node.type == "ANIME" (skips MANGA/NOVEL).
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

        prequels: list[dict] = []
        sequels: list[dict] = []
        side_stories: list[dict] = []
        all_anime: list[dict] = []

        for edge in edges:
            rel_type = edge.get("relationType")
            node = edge.get("node") or {}
            # Only follow anime relations (skip MANGA/NOVEL)
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
    finally:
        if close:
            await client.aclose()


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
) -> list[dict]:
    """Walk the PREQUEL chain from this anime back to the root season.

    Returns a list of prequel entries, ordered from the IMMEDIATE prequel
    (index 0) to the ROOT season (last index). Each entry is the dict from
    fetch_relations().prequels[0].

    Example for Re:Zero S2 Part 2 (AniList 119661):
        [
            {"anilist_id": 108632, "title_en": "Re:Zero Season 2", "episodes": 13, ...},  # immediate prequel
            {"anilist_id": 21355,  "title_en": "Re:Zero",         "episodes": 25, ...},   # root (S1)
        ]

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

        for depth in range(max_depth):
            rels = await fetch_relations(current_id, client)
            prequels = rels.get("prequels") or []
            if not prequels:
                break  # this is the root season

            # Take the first prequel (most common case — one prequel per entry).
            # If there are multiple, pick the one that's a TV/ONA (not MOVIE/SPECIAL).
            prequel = _pick_best_prequel(prequels)
            if not prequel:
                break

            next_id = prequel.get("anilist_id")
            if not next_id or next_id in visited:
                break  # cycle detected

            chain.append(prequel)
            visited.add(next_id)
            current_id = next_id

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
) -> dict:
    """Calculate the episode offset for an AniList entry by walking its prequel chain.

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
    chain = await trace_prequel_chain(anilist_id, client)
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
        "rate_limit": "90 req/min per IP",
        "concurrency": _CONCURRENCY_LIMIT,
        "per_request_delay_s": _PER_REQUEST_DELAY,
    }
