"""
sources/tmdb.py — TMDB API client.

Requires the TMDB_API_KEY environment variable to be set. The API key is
not hardcoded in the source — get one from https://www.themoviedb.org/settings/api
and add it to your environment (e.g. Render env vars, .env file, etc.).

Endpoints used:
- /tv/{id}              — TV series details
- /tv/{id}/season/{n}   — episodes with stills + titles + overviews
- /tv/{id}/images       — logos, backdrops, posters
- /movie/{id}           — movie details
- /movie/{id}/images    — logos, backdrops, posters
- /find/{external_id}   — lookup by TVDB/IMDb ID
- /search/tv            — search by name (fallback for new anime)
- /search/movie         — search by name (fallback for movies)

Image CDN: https://image.tmdb.org/t/p/{size}{path} — fully public, no key.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

log = logging.getLogger("tmdb")

# TMDB API key is loaded from the TMDB_API_KEY environment variable.
# Do NOT hardcode it in source. Get one from:
# https://www.themoviedb.org/settings/api
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"
TIMEOUT = 15.0


def _get_key() -> str:
    """Return the TMDB API key from the environment."""
    import os
    key = os.environ.get("TMDB_API_KEY")
    if not key:
        raise RuntimeError(
            "TMDB_API_KEY environment variable is not set. "
            "Get an API key from https://www.themoviedb.org/settings/api "
            "and set it in your environment."
        )
    return key


def _img(path: Optional[str], size: str = "original") -> Optional[str]:
    """Build a TMDB image URL from a path."""
    if not path:
        return None
    return f"{TMDB_IMAGE_BASE}/{size}{path}"


async def _get(path: str, params: Optional[dict] = None, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Make a TMDB API GET request."""
    params = params or {}
    params["api_key"] = _get_key()
    url = f"{TMDB_BASE}{path}"
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        res = await client.get(url, params=params, headers={"User-Agent": "anime-metadata-api/1.0"})
        if res.status_code != 200:
            log.warning("TMDB %s: HTTP %d", path, res.status_code)
            return {}
        return res.json()
    except Exception as e:
        log.warning("TMDB %s failed: %s", path, e)
        return {}
    finally:
        if close:
            await client.aclose()


async def find_by_external_id(external_id: str, source: str, client: Optional[httpx.AsyncClient] = None) -> Optional[tuple[str, int]]:
    """
    TMDB /find — lookup by external ID.
    source: "tvdb_id" | "imdb_id" | "freebase_mid" | ...
    Returns (type, tmdb_id) where type is "tv" or "movie", or None.
    """
    data = await _get(f"/find/{external_id}", {"external_source": source}, client)
    tv_results = data.get("tv_results", [])
    if tv_results:
        return ("tv", tv_results[0]["id"])
    movie_results = data.get("movie_results", [])
    if movie_results:
        return ("movie", movie_results[0]["id"])
    return None


async def search(
    query: str, 
    media_type: str = "tv", 
    year: Optional[int] = None,
    client: Optional[httpx.AsyncClient] = None
) -> Optional[int]:
    """
    TMDB search with anime prioritization:
    - Filters for Japanese anime (original_language: 'ja' / origin_country: 'JP')
    - Matches first_air_date_year (1999 for One Piece)
    """
    params = {
        "query": query,
        "language": "en-US",
        "page": 1,
        "include_adult": "false"
    }
    if year:
        if media_type == "tv":
            params["first_air_date_year"] = year
        else:
            params["year"] = year

    data = await _get(f"/search/{media_type}", params, client)
    results = data.get("results", [])

    # If search with year returned nothing, retry without year restriction
    if not results and year:
        params.pop("first_air_date_year", None)
        params.pop("year", None)
        data = await _get(f"/search/{media_type}", params, client)
        results = data.get("results", [])

    if not results:
        return None

    # 1. Prioritize Japanese anime (original_language == 'ja' or JP origin)
    for item in results:
        orig_lang = item.get("original_language", "")
        countries = item.get("origin_country", [])
        if orig_lang == "ja" or "JP" in countries:
            return item["id"]

    # 2. Fallback to first result if no Japanese show found
    return results[0]["id"]


async def fetch_tv(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch TV series details."""
    return await _get(f"/tv/{tmdb_id}", {"language": "en-US"}, client)


async def fetch_tv_season(tmdb_id: int, season: int = 1, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch TV season episodes (with stills + titles + overviews)."""
    return await _get(f"/tv/{tmdb_id}/season/{season}", {"language": "en-US"}, client)


async def fetch_tv_images(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch TV images (logos, backdrops, posters)."""
    return await _get(f"/tv/{tmdb_id}/images", {"include_image_language": "en,null"}, client)


async def fetch_movie(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch movie details."""
    return await _get(f"/movie/{tmdb_id}", {"language": "en-US"}, client)


async def fetch_movie_images(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Fetch movie images (logos, backdrops, posters)."""
    return await _get(f"/movie/{tmdb_id}/images", {"include_image_language": "en,null"}, client)


def extract_images(images_data: dict, media_type: str = "tv") -> list[dict]:
    """
    Convert TMDB images response to our standard format:
    [{coverType: "Clearlogo"|"Banner"|"Poster"|"Fanart", url, source: "tmdb"}, ...]
    """
    out = []
    # Logos → Clearlogo
    for logo in images_data.get("logos", []):
        url = _img(logo.get("file_path"), "original")
        if url:
            out.append({"coverType": "Clearlogo", "url": url, "source": "tmdb",
                       "language": logo.get("iso_639_1")})
    # Backdrops → Banner + Fanart (first one is banner, rest are fanart)
    backdrops = images_data.get("backdrops", [])
    for i, b in enumerate(backdrops[:5]):  # limit to 5
        url = _img(b.get("file_path"), "original")
        if url:
            cover_type = "Banner" if i == 0 else "Fanart"
            out.append({"coverType": cover_type, "url": url, "source": "tmdb",
                       "language": b.get("iso_639_1")})
    # Posters → Poster
    for p in images_data.get("posters", [])[:3]:  # limit to 3
        url = _img(p.get("file_path"), "original")
        if url:
            out.append({"coverType": "Poster", "url": url, "source": "tmdb",
                       "language": p.get("iso_639_1")})
    return out


def extract_episodes(season_data: dict, anilist_id: int) -> list[dict]:
    """
    Convert TMDB season response to our standard episode format:
    [{id, number, title, titleJa, description, image, airDate, duration, isFiller, rating, hasAired, source}, ...]
    """
    import time
    out = []
    today = time.strftime("%Y-%m-%d")
    for ep in season_data.get("episodes", []):
        num = ep.get("episode_number")
        if num is None:
            continue
        air_date = ep.get("air_date", "") or ""
        out.append({
            "id": f"{anilist_id}-{num}",
            "number": num,
            "title": ep.get("name", "") or f"Episode {num}",
            "titleJa": "",  # TMDB doesn't have Japanese titles per episode
            "description": ep.get("overview", "") or "",
            "image": _img(ep.get("still_path"), "original") or "",
            "airDate": air_date,
            "duration": ep.get("runtime", 0) or 0,
            "isFiller": False,  # TMDB doesn't track filler
            "rating": str(ep.get("vote_average")) if ep.get("vote_average") else None,
            "hasAired": bool(air_date) and air_date <= today,
            "source": "tmdb",
            "tmdb_episode_id": ep.get("id"),
        })
    return out
