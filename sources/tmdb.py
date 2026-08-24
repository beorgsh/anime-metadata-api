"""
sources/tmdb.py — TMDB API client.
Supports both TMDB API Key (v3) and TMDB Read Access Token (v4).
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

log = logging.getLogger("tmdb")

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p"
TIMEOUT = 15.0


def _get_auth_headers_and_params(params: Optional[dict] = None) -> tuple[dict, dict]:
    """
    Detects if TMDB_API_KEY is a v4 Bearer token (starts with eyJ...) 
    or a standard v3 API key (32-hex chars).
    """
    key = os.environ.get("TMDB_API_KEY", "").strip().strip('"').strip("'")
    if not key:
        log.error("TMDB_API_KEY is not set in environment variables!")
        raise RuntimeError("TMDB_API_KEY is not set.")

    params = params or {}
    headers = {
        "User-Agent": "anime-metadata-api/1.0",
        "Accept": "application/json",
    }

    if key.startswith("eyJ"):
        # TMDB v4 Bearer Token
        headers["Authorization"] = f"Bearer {key}"
    else:
        # TMDB v3 API Key
        params["api_key"] = key

    return headers, params


def _img(path: Optional[str], size: str = "original") -> Optional[str]:
    if not path:
        return None
    return f"{TMDB_IMAGE_BASE}/{size}{path}"


async def _get(path: str, params: Optional[dict] = None, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Make a TMDB API GET request."""
    try:
        headers, query_params = _get_auth_headers_and_params(params)
    except RuntimeError:
        return {}

    url = f"{TMDB_BASE}{path}"
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        res = await client.get(url, params=query_params, headers=headers)
        if res.status_code != 200:
            log.warning("TMDB %s: HTTP %d | Response: %s", path, res.status_code, res.text)
            return {}
        return res.json()
    except Exception as e:
        log.warning("TMDB %s failed: %s", path, e)
        return {}
    finally:
        if close:
            await client.aclose()


async def find_by_external_id(external_id: str, source: str, client: Optional[httpx.AsyncClient] = None) -> Optional[tuple[str, int]]:
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

    if not results and year:
        params.pop("first_air_date_year", None)
        params.pop("year", None)
        data = await _get(f"/search/{media_type}", params, client)
        results = data.get("results", [])

    if not results:
        return None

    for item in results:
        orig_lang = item.get("original_language", "")
        countries = item.get("origin_country", [])
        if orig_lang == "ja" or "JP" in countries:
            return item["id"]

    return results[0]["id"]


async def fetch_tv(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    return await _get(f"/tv/{tmdb_id}", {"language": "en-US"}, client)


async def fetch_tv_season(tmdb_id: int, season: int = 1, client: Optional[httpx.AsyncClient] = None) -> dict:
    return await _get(f"/tv/{tmdb_id}/season/{season}", {"language": "en-US"}, client)


async def fetch_tv_images(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    return await _get(f"/tv/{tmdb_id}/images", {"include_image_language": "en,null"}, client)


async def fetch_movie(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    return await _get(f"/movie/{tmdb_id}", {"language": "en-US"}, client)


async def fetch_movie_images(tmdb_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    return await _get(f"/movie/{tmdb_id}/images", {"include_image_language": "en,null"}, client)


def extract_images(images_data: dict, media_type: str = "tv") -> list[dict]:
    out = []
    for logo in images_data.get("logos", []):
        url = _img(logo.get("file_path"), "original")
        if url:
            out.append({"coverType": "Clearlogo", "url": url, "source": "tmdb",
                       "language": logo.get("iso_639_1")})
    backdrops = images_data.get("backdrops", [])
    for i, b in enumerate(backdrops[:5]):
        url = _img(b.get("file_path"), "original")
        if url:
            cover_type = "Banner" if i == 0 else "Fanart"
            out.append({"coverType": cover_type, "url": url, "source": "tmdb",
                       "language": b.get("iso_639_1")})
    for p in images_data.get("posters", [])[:3]:
        url = _img(p.get("file_path"), "original")
        if url:
            out.append({"coverType": "Poster", "url": url, "source": "tmdb",
                       "language": p.get("iso_639_1")})
    return out


def extract_episodes(season_data: dict, anilist_id: int) -> list[dict]:
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
            "titleJa": "",
            "description": ep.get("overview", "") or "",
            "image": _img(ep.get("still_path"), "original") or "",
            "airDate": air_date,
            "duration": ep.get("runtime", 0) or 0,
            "isFiller": False,
            "rating": str(ep.get("vote_average")) if ep.get("vote_average") else None,
            "hasAired": bool(air_date) and air_date <= today,
            "source": "tmdb",
            "tmdb_episode_id": ep.get("id"),
        })
    return out
