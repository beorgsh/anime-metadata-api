"""
sources/fribb.py — AniList↔TMDB static mapping via Fribb/anime-lists.

Fribb/anime-lists is the de-facto standard AniList↔TMDB mapping dataset.
It's built by joining manami-project/anime-offline-database (AniList IDs)
with Anime-Lists/anime-lists (TMDB IDs) on the AniDB ID — AniDB is the
sync point between AniList and TMDB.

- 20,687 entries with AniList IDs
- ~39% have direct TMDB ID (TMDB simply doesn't have the other 61%)
- Updated weekly (automated, every Monday)
- Handles MOVIES (separate themoviedb_id.movie array) and TV (themoviedb_id.tv)
- Loaded into memory at startup — O(1) lookup, zero runtime API calls

We also try anibridge/anibridge-mappings as a secondary source for
higher coverage (daily updates, more sources merged).
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

import httpx

log = logging.getLogger("fribb")

FRIBB_INDEX_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/master/indices/anilist_index.json"
FRIBB_FULL_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json"

# Local cache paths
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
_INDEX_PATH = os.path.join(_DATA_DIR, "anilist_index.json")
_FULL_PATH = os.path.join(_DATA_DIR, "anime-list-mini.json")

# In-memory lookup tables (loaded once at startup)
_index: dict[str, dict] = {}  # anilist_id (str) → {"anime-list": [position]}
_full: list = []               # list of anime entries
_reverse_index: dict[int, list[int]] = {}  # tmdb_tv_id → [anilist_id, ...]
_reverse_built: bool = False
_loaded_at: float = 0.0
_REFRESH_INTERVAL = 7 * 24 * 3600  # refresh weekly


def _download(url: str, path: str) -> bool:
    """Download a file with progress logging."""
    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            with client.stream("GET", url) as r:
                if r.status_code != 200:
                    log.warning("Failed to download %s: HTTP %d", url, r.status_code)
                    return False
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)
        size = os.path.getsize(path)
        log.info("Downloaded %s → %s (%d bytes)", url, path, size)
        return True
    except Exception as e:
        log.warning("Download error for %s: %s", url, e)
        return False


def _load_from_disk() -> bool:
    """Load the JSON files from disk into memory."""
    global _index, _full, _loaded_at, _reverse_built, _reverse_index
    try:
        if not os.path.exists(_INDEX_PATH) or not os.path.exists(_FULL_PATH):
            return False
        with open(_INDEX_PATH) as f:
            _index = json.load(f)
        with open(_FULL_PATH) as f:
            _full = json.load(f)
        _loaded_at = time.time()
        _reverse_built = False
        _reverse_index = {}
        log.info("Fribb loaded: %d index entries, %d full entries", len(_index), len(_full))
        return True
    except Exception as e:
        log.warning("Load from disk failed: %s", e)
        return False


def _download_and_load() -> bool:
    """Download Fribb data and load it into memory."""
    ok1 = _download(FRIBB_INDEX_URL, _INDEX_PATH)
    ok2 = _download(FRIBB_FULL_URL, _FULL_PATH)
    if ok1 and ok2:
        return _load_from_disk()
    return False


def is_loaded() -> bool:
    """Check if the mapping data is loaded in memory."""
    return bool(_index and _full)


def ensure_loaded(force_refresh: bool = False) -> bool:
    """
    Ensure the Fribb data is loaded. Downloads if needed.
    Returns True if data is available (in memory).
    """
    global _loaded_at
    if force_refresh:
        return _download_and_load()
    if is_loaded() and (time.time() - _loaded_at < _REFRESH_INTERVAL):
        return True
    if _load_from_disk():
        return True
    return _download_and_load()


def lookup(anilist_id: int) -> Optional[dict]:
    """
    Look up an AniList ID in the Fribb mapping.

    Returns:
        {
            "anilist_id": int,
            "mal_id": int | None,
            "thetvdb_id": int | None,
            "themoviedb_id": {"tv": int} | {"movie": [int, ...]} | None,
            "imdb_id": [str] | None,
            "anidb_id": int | None,
            "kitsu_id": int | None,
            "type": "TV" | "MOVIE" | "OVA" | "SPECIAL" | ...,
            "season": {"tvdb": int, "tmdb": int} | None,
            "episode_offset": {"tvdb": int, "tmdb": int} | None,
        }
        or None if not found.
    """
    if not is_loaded():
        return None
    entry = _index.get(str(anilist_id))
    if not entry:
        return None
    positions = entry.get("anime-list", [])
    if not positions:
        return None
    pos = positions[0]
    if pos < 0 or pos >= len(_full):
        return None
    item = _full[pos]
    item.setdefault("anilist_id", anilist_id)
    return item


def get_tmdb_id(anilist_id: int) -> Optional[tuple[str | None, int | list]]:
    """
    Convenience: return (tmdb_type, tmdb_id) for an AniList ID.
    tmdb_type is "tv" or "movie". tmdb_id is an int (for TV) or list of
    ints (for movie — some anime movies map to multiple TMDB entries).
    Returns None if not found or no TMDB mapping.
    """
    item = lookup(anilist_id)
    if not item:
        return None
    tmdb = item.get("themoviedb_id") or {}
    if tmdb.get("tv"):
        return ("tv", tmdb["tv"])
    if tmdb.get("movie"):
        return ("movie", tmdb["movie"])
    return None


def _build_reverse_index() -> None:
    """Build tmdb_tv_id → [anilist_id, ...] reverse index from the main index.
    Lazy-built on first call to lookup_siblings_by_tmdb_tv()."""
    global _reverse_built, _reverse_index
    if _reverse_built or not is_loaded():
        return
    _reverse_index = {}
    for aid_str, entry in _index.items():
        positions = entry.get("anime-list", []) if isinstance(entry, dict) else []
        if not positions:
            continue
        pos = positions[0]
        if pos < 0 or pos >= len(_full):
            continue
        item = _full[pos]
        if not isinstance(item, dict):
            continue
        tmdb_info = item.get("themoviedb_id") or {}
        tmdb_tv = tmdb_info.get("tv")
        if tmdb_tv:
            try:
                _reverse_index.setdefault(int(tmdb_tv), []).append(int(aid_str))
            except (TypeError, ValueError):
                pass
    _reverse_built = True
    log.info("Fribb reverse index built: %d TMDB TV IDs", len(_reverse_index))


def lookup_siblings_by_tmdb_tv(tmdb_tv_id: int) -> list[int]:
    """Return all AniList IDs that map to the same TMDB TV ID.
    This is how we discover that Re:Zero (TMDB 65942) has 4 AniList entries
    (21355, 108632, 119661, 163134) even though TMDB only has 1 season.
    """
    if not is_loaded():
        return []
    _build_reverse_index()
    return sorted(_reverse_index.get(int(tmdb_tv_id), []))


def stats() -> dict:
    """Return stats for /health endpoint."""
    return {
        "loaded": is_loaded(),
        "index_entries": len(_index),
        "full_entries": len(_full),
        "reverse_index_entries": len(_reverse_index) if _reverse_built else 0,
        "loaded_at": _loaded_at,
        "age_seconds": time.time() - _loaded_at if _loaded_at else 0,
    }
