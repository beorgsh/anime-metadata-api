"""
sources/anibridge_local.py — Local AniBridge mappings database (v2).

Loads the AniBridge mappings.json file (downloaded from
https://github.com/anibridge/anibridge-mappings/releases) into memory
at startup for O(1) lookup.

The mappings provide EXACT episode range mappings between providers:
  e.g. {"39-50": "1-12"} means TMDB episodes 39-50 = AniList episodes 1-12
  e.g. {"1089-": "1089-"} means TMDB S22 episodes from 1089 onwards = AniList episodes from 1089 onwards (ongoing)

This is the PRIMARY source for episode mapping. Fribb is secondary (its
offsets are off by 1 for some anime like Re:Zero S2P2/S3/S4). AniZip
(TVDB) is tertiary fallback.

File: data/anibridge_mappings.json (~14MB, loaded into memory at startup)
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Optional

log = logging.getLogger("anibridge_local")

_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
_MAPPINGS_PATH = os.path.join(_DATA_DIR, "anibridge_mappings.json")

# In-memory indexes
_mappings: dict = {}
# Reverse index: anilist_id → list of {tmdb_id, tmdb_season, tmdb_range, anilist_range}
# (list because some anime span multiple TMDB seasons, like One Piece or Naruto)
_anilist_index: dict[int, list[dict]] = {}
_loaded_at: float = 0.0
_REFRESH_INTERVAL = 7 * 24 * 3600

ANIBRIDGE_URL = "https://github.com/anibridge/anibridge-mappings/releases/download/v3/mappings.json"


def _download(url: str, path: str) -> bool:
    try:
        import httpx
        with httpx.Client(timeout=120.0, follow_redirects=True) as client:
            with client.stream("GET", url) as r:
                if r.status_code != 200:
                    log.warning("Failed to download %s: HTTP %d", url, r.status_code)
                    return False
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)
        size = os.path.getsize(path)
        log.info("Downloaded AniBridge mappings: %s (%d bytes)", path, size)
        return True
    except Exception as e:
        log.warning("Download error for AniBridge mappings: %s", e)
        return False


def _parse_range(range_str: str) -> tuple[int, int]:
    """Parse a range string like "39-50" → (39, 50).
    Handles open-ended ranges like "1089-" → (1089, 999999).
    Returns (0, 0) if parsing fails."""
    try:
        parts = range_str.split("-")
        start = int(parts[0])
        if len(parts) >= 2 and parts[1]:
            end = int(parts[1])
        else:
            end = 999999  # Open-ended (ongoing)
        return start, end
    except (ValueError, IndexError):
        return 0, 0


def _load_from_disk() -> bool:
    """Load the mappings JSON into memory and build the reverse index."""
    global _mappings, _loaded_at, _anilist_index
    try:
        if not os.path.exists(_MAPPINGS_PATH):
            return False
        with open(_MAPPINGS_PATH) as f:
            _mappings = json.load(f)
        _loaded_at = time.time()

        # Build reverse index: anilist_id → list of mapping entries
        _anilist_index = {}
        for key, val in _mappings.items():
            if key == "$meta" or not isinstance(val, dict):
                continue
            # Only process tmdb_show:* keys (these have anilist sub-mappings)
            if not key.startswith("tmdb_show:"):
                continue
            parts = key.split(":")
            if len(parts) < 3:
                continue
            try:
                tmdb_id = int(parts[1])
                tmdb_season = int(parts[2].lstrip("s"))
            except (ValueError, IndexError):
                continue

            for sub_key, mapping in val.items():
                if not sub_key.startswith("anilist:"):
                    continue
                try:
                    aid = int(sub_key.split(":")[1])
                except (ValueError, IndexError):
                    continue
                if not isinstance(mapping, dict) or not mapping:
                    continue

                # Take the first range pair
                tmdb_range = list(mapping.keys())[0]
                anilist_range = mapping[tmdb_range]

                entry = {
                    "tmdb_id": tmdb_id,
                    "tmdb_season": tmdb_season,
                    "tmdb_range": tmdb_range,
                    "anilist_range": anilist_range,
                    "tmdb_type": "tv",
                }
                _anilist_index.setdefault(aid, []).append(entry)

        # Sort each AniList ID's entries by TMDB season number
        for aid in _anilist_index:
            _anilist_index[aid].sort(key=lambda e: e["tmdb_season"])

        log.info("AniBridge loaded: %d total entries, %d AniList IDs indexed",
                  len(_mappings), len(_anilist_index))
        return True
    except Exception as e:
        log.warning("Load AniBridge mappings failed: %s", e)
        return False


def _download_and_load() -> bool:
    if _download(ANIBRIDGE_URL, _MAPPINGS_PATH):
        return _load_from_disk()
    return False


def is_loaded() -> bool:
    return bool(_anilist_index)


def ensure_loaded(force_refresh: bool = False) -> bool:
    global _loaded_at
    if force_refresh:
        return _download_and_load()
    if is_loaded() and (time.time() - _loaded_at < _REFRESH_INTERVAL):
        return True
    if _load_from_disk():
        return True
    return _download_and_load()


def lookup_anilist(anilist_id: int) -> Optional[list[dict]]:
    """Look up an AniList ID. Returns a LIST of mapping entries (one per TMDB season).

    Each entry: {
        "tmdb_id": int,
        "tmdb_season": int,
        "tmdb_range": "39-50" or "1089-" (open-ended),
        "anilist_range": "1-12" or "1089-" (open-ended),
        "tmdb_type": "tv",
    }

    For Pattern A (e.g. Re:Zero S2P2): returns 1 entry with TMDB range "39-50"
    For Pattern B (e.g. One Piece): returns 22+ entries, one per TMDB season
    """
    if not is_loaded():
        return None
    return _anilist_index.get(int(anilist_id))


def get_tmdb_episode_range(entry: dict) -> tuple[int, int]:
    """Get (start, end) TMDB episode numbers for a mapping entry.
    Open-ended ranges return (start, 999999)."""
    return _parse_range(entry.get("tmdb_range", "0-0"))


def get_anilist_episode_range(entry: dict) -> tuple[int, int]:
    """Get (start, end) AniList episode numbers for a mapping entry."""
    return _parse_range(entry.get("anilist_range", "0-0"))


def is_identity_mapping(entry: dict) -> bool:
    """Check if the TMDB range equals the AniList range (identity mapping).
    This means Pattern B (One Piece, Naruto) — TMDB episode numbers = AniList episode numbers."""
    return entry.get("tmdb_range") == entry.get("anilist_range")


def stats() -> dict:
    return {
        "loaded": is_loaded(),
        "total_entries": len(_mappings),
        "anilist_indexed": len(_anilist_index),
        "loaded_at": _loaded_at,
        "age_seconds": time.time() - _loaded_at if _loaded_at else 0,
    }
