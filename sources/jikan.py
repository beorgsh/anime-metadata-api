"""
sources/jikan.py — Jikan v4 client (unofficial MyAnimeList API).

Jikan (https://jikan.moe) is a public REST API that exposes MyAnimeList
data without requiring an API key. We use it as a cross-verification source
for episode count, status, and air dates.

Rate limits (public):
    - 3 req/sec, 60 req/min
    - 1 req per anime is well within limits.

Endpoints used:
    GET /v4/anime/{mal_id}                    — anime details (episodes, status, dates)
    GET /v4/anime/{mal_id}/full                — full record (includes external + relations)

Returns normalised dict. Never raises.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

log = logging.getLogger("jikan")

JIKAN_BASE = "https://api.jikan.moe/v4"
TIMEOUT = 15.0
UA = "anime-metadata-api/1.0"


async def fetch_jikan(mal_id: int, client: Optional[httpx.AsyncClient] = None,
                      retries: int = 2) -> dict:
    """
    Fetch anime metadata from Jikan (MAL).

    Returns {} on any failure (never raises). Retries on 429 / 5xx with
    exponential backoff (1s, 2s) since Jikan/MAL goes down frequently.

    Returned dict shape:
        {
            "mal_id": int,
            "title": str,
            "title_english": str | None,
            "title_japanese": str | None,
            "synopsis": str | None,
            "type": str | None,            # TV, Movie, OVA, ONA, Special, Music
            "episodes": int | None,        # ← MAL-reported episode count (verifier!)
            "status": str | None,           # "Finished Airing", "Currently Airing", "Not yet aired"
            "airing": bool,
            "aired": {"from": "YYYY-MM-DD" | None, "to": "YYYY-MM-DD" | None},
            "season": str | None,           # Winter, Spring, Summer, Fall  ← season verifier!
            "year": int | None,             # MAL year
            "score": float | None,
            "rank": int | None,
            "popularity": int | None,
            "members": int | None,
            "favorites": int | None,
            "duration": str | None,         # "24 min per ep"
            "rating": str | None,
            "genres": [str, ...],
            "studios": [str, ...],
            "image_url": str | None,
            "source": "jikan",
        }
    """
    import asyncio as _asyncio

    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        for attempt in range(retries + 1):
            try:
                res = await client.get(
                    f"{JIKAN_BASE}/anime/{mal_id}/full",
                    headers={"Accept": "application/json", "User-Agent": UA},
                )
            except Exception as e:
                log.warning("Jikan %d network error (attempt %d): %s", mal_id, attempt + 1, e)
                if attempt < retries:
                    await _asyncio.sleep(1.0 * (attempt + 1))
                    continue
                return {}

            if res.status_code == 200:
                data = res.json().get("data") or {}
                if not data:
                    return {}
                return _normalise(data)

            if res.status_code == 404:
                return {}

            # 429 / 5xx → retry with backoff
            if res.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                log.warning("Jikan %d: HTTP %d (attempt %d) — retrying", mal_id, res.status_code, attempt + 1)
                await _asyncio.sleep(1.0 * (attempt + 1))
                continue

            log.warning("Jikan %d: HTTP %d", mal_id, res.status_code)
            return {}
        return {}
    finally:
        if close:
            await client.aclose()


def _normalise(d: dict) -> dict:
    aired = d.get("aired") or {}
    return {
        "mal_id": d.get("mal_id"),
        "title": d.get("title") or "",
        "title_english": d.get("title_english"),
        "title_japanese": d.get("title_japanese"),
        "synopsis": d.get("synopsis"),
        "type": d.get("type"),
        "episodes": d.get("episodes"),
        "status": d.get("status"),
        "airing": bool(d.get("airing")),
        "aired": {
            "from": (aired.get("from") or "")[:10] if aired.get("from") else None,
            "to": (aired.get("to") or "")[:10] if aired.get("to") else None,
        },
        "season": d.get("season"),
        "year": d.get("year"),
        "score": d.get("score"),
        "rank": d.get("rank"),
        "popularity": d.get("popularity"),
        "members": d.get("members"),
        "favorites": d.get("favorites"),
        "duration": d.get("duration"),
        "rating": d.get("rating"),
        "genres": [g.get("name") for g in (d.get("genres") or []) if g.get("name")],
        "studios": [s.get("name") for s in (d.get("studios") or []) if s.get("name")],
        "image_url": (d.get("images") or {}).get("jpg", {}).get("large_image_url")
                     or (d.get("images") or {}).get("jpg", {}).get("image_url"),
        "source": "jikan",
    }


def stats() -> dict:
    return {"api": JIKAN_BASE, "rate_limit": "3 req/sec, 60 req/min"}
