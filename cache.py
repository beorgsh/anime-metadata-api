"""
cache.py — In-memory TTL cache with per-key asyncio locks.

No external database, no disk persistence. Cache lives in process memory
and rebuilds naturally on restart. Designed for millions of requests
with minimal overhead (dict lookup = ~0ms).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Optional


class TTLCache:
    def __init__(self, default_ttl: int = 7 * 24 * 3600):
        self._store: dict[str, tuple[float, Any]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._default_ttl = default_ttl

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() > expires_at:
            # Expired — remove
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        self._store[key] = (time.monotonic() + ttl, value)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)
        self._locks.pop(key, None)

    def get_lock(self, key: str) -> asyncio.Lock:
        """Per-key lock — so concurrent requests for the same key dedupe."""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def stats(self) -> dict:
        now = time.monotonic()
        valid = sum(1 for exp, _ in self._store.values() if exp > now)
        return {
            "entries": valid,
            "default_ttl_seconds": self._default_ttl,
        }

    def clear(self) -> None:
        self._store.clear()
        self._locks.clear()


# Global cache instances
metadata_cache = TTLCache(default_ttl=7 * 24 * 3600)  # 7 days for metadata
tmdb_id_cache = TTLCache(default_ttl=30 * 24 * 3600)   # 30 days for ID mappings (rarely change)
relations_cache = TTLCache(default_ttl=30 * 24 * 3600)  # 30 days for AniList prequel/sequel chain
offset_cache = TTLCache(default_ttl=30 * 24 * 3600)     # 30 days for calculated episode offsets
