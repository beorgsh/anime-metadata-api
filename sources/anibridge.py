"""
sources/anibridge.py — AniBridge mappings client.

AniBridge (https://mappings.anibridge.eliasbenb.dev) is a community-maintained
cross-provider anime mapping service that aggregates data from multiple
metadata providers (AniList, MAL, AniDB, Kitsu, TMDB, TVDB, IMDB, ...).

Live API (no key, no rate-limit issues):
    GET https://metadata.anibridge.eliasbenb.dev/api/metadata/{descriptor}

Descriptor format:
    "{provider}:{id}"           — e.g. "anilist:126891", "mal:21", "anidb:10000"
    "{provider}:{id}:{scope}"    — e.g. "anidb:10000:R" (scoped, like seasons)

Response shape (truncated):
    {
      "metadata": {
        "kind": "show" | "movie",
        "id": {"descriptor": "...", "provider": "...", "provider_id": "...", "scope": null},
        "titles": {"display": ..., "main": ..., "original": ..., "aliases": [...], "franchise": ...},
        "synopsis": "...",
        "release": {"start_date": "YYYY-MM-DD", "end_date": ..., "status": "ongoing|finished|..."},
        "runtime": {"minutes": int, "basis": "derived|explicit"},
        "units": int,             # ← episode count (cross-provider verified!)
        "classification": {"is_adult": bool, "genres": [...]},
        "ratings": {"average": float|null, "popularity": float},
        "images": [{"kind": "poster|banner", "url": "..."}],
        "scopes": {<scope-id>: {<metadata>}} | null,
        "relationships": [{"kind": "twin|sequel|prequel|...", "target": {"descriptor": "...", "kind": "..."}}],
        "source": "https://..."
      },
      "cache": {"updated_at": ..., "expires_at": ..., "stale": bool, "source": "cache|origin"}
    }

This module provides:
- async fetch_anibridge(descriptor) -> dict     — fetch metadata for any provider:id
- async fetch_by_anilist(anilist_id) -> dict    — convenience wrapper for "anilist:{id}"
- async fetch_cross_mappings(anilist_id) -> dict — fetch by anilist, then chase all
                                                   relationship targets to build full
                                                   cross-provider mapping table
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

log = logging.getLogger("anibridge")

ANIBRIDGE_API = "https://metadata.anibridge.eliasbenb.dev/api/metadata"
TIMEOUT = 15.0
UA = "anime-metadata-api/1.0 (+https://github.com/historiesofhistory-arch/anime-metadata-api)"

# Provider prefixes recognised by AniBridge (per their JSON schema).
SUPPORTED_PROVIDERS = (
    "anilist", "mal", "anidb", "kitsu",
    "tmdb_movie", "tmdb_show", "tvdb_movie", "tvdb_show",
    "imdb_movie", "imdb_show",
)


async def fetch_anibridge(descriptor: str, client: Optional[httpx.AsyncClient] = None) -> dict:
    """
    Fetch AniBridge metadata for a descriptor.

    descriptor: e.g. "anilist:126891", "mal:21", "anidb:10000", "anidb:10000:R"
    Returns {} on any failure (never raises).
    """
    close = False
    if client is None:
        client = httpx.AsyncClient(timeout=TIMEOUT)
        close = True
    try:
        res = await client.get(
            f"{ANIBRIDGE_API}/{descriptor}",
            headers={"Accept": "application/json", "User-Agent": UA},
        )
        if res.status_code == 404:
            return {}
        if res.status_code != 200:
            log.warning("AniBridge %s: HTTP %d", descriptor, res.status_code)
            return {}
        payload = res.json()
        meta = payload.get("metadata") or {}
        return meta
    except Exception as e:
        log.warning("AniBridge %s failed: %s", descriptor, e)
        return {}
    finally:
        if close:
            await client.aclose()


async def fetch_by_anilist(anilist_id: int, client: Optional[httpx.AsyncClient] = None) -> dict:
    """Convenience: fetch AniBridge metadata for an AniList ID."""
    return await fetch_anibridge(f"anilist:{anilist_id}", client)


async def fetch_cross_mappings(
    anilist_id: int,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """
    Fetch AniBridge metadata for an AniList ID, then chase all relationship
    targets to build a full cross-provider mapping table.

    Returns:
        {
            "anilist_id": int,
            "anibridge_meta": {...} | {},        # raw AniBridge metadata for the AniList entry
            "units": int | None,                 # cross-provider-verified episode count
            "release": {...} | None,             # release info (status, start/end dates)
            "runtime_minutes": int | None,
            "titles": {...} | None,
            "synopsis": str | None,
            "ratings": {...} | None,
            "genres": [...],
            "images": [...],
            "cross_ids": {                       # cross-provider IDs gathered from relationships
                "anilist": [int, ...],
                "mal": [int, ...],
                "anidb": [str, ...],             # anidb can have scoped IDs like "10000:R"
                "kitsu": [int, ...],
                "tmdb_show": [int, ...],
                "tmdb_movie": [int, ...],
                "tvdb_show": [int, ...],
                "tvdb_movie": [int, ...],
                "imdb_show": [str, ...],
                "imdb_movie": [str, ...],
            },
            "relationships": [
                {"kind": "twin|sequel|prequel|...", "provider": str, "id": str},
                ...
            ],
            "sources_used": [str, ...],          # which relationship descriptors we fetched
        }
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(timeout=TIMEOUT)
    try:
        meta = await fetch_by_anilist(anilist_id, client)
        result = _normalise_meta(meta, anilist_id)
        if not meta:
            return result

        # Chase all relationship targets in parallel — each one tells us a sibling
        # anime on another provider (e.g. the MAL twin, the AniDB twin, etc.).
        relationships = meta.get("relationships") or []
        targets = []
        for rel in relationships:
            tgt = rel.get("target") or {}
            desc = tgt.get("descriptor")
            if not desc:
                continue
            # Only chase provider:id form (skip scoped forms like "anidb:10000:R")
            # to keep request count bounded — scoped entries are already in `scopes`.
            parts = desc.split(":")
            if len(parts) != 2:
                continue
            provider = parts[0]
            if provider not in SUPPORTED_PROVIDERS:
                continue
            targets.append((rel.get("kind", "related"), desc))

        if targets:
            fetched = await asyncio.gather(
                *[fetch_anibridge(desc, client) for _, desc in targets],
                return_exceptions=True,
            )
            for (kind, desc), child in zip(targets, fetched):
                if isinstance(child, Exception) or not child:
                    continue
                result["sources_used"].append(desc)
                result["relationships"].append({
                    "kind": kind,
                    "provider": desc.split(":", 1)[0],
                    "id": desc.split(":", 1)[1],
                })
                # Merge cross-provider IDs from this relationship
                tgt_provider = desc.split(":", 1)[0]
                tgt_id = desc.split(":", 1)[1]
                _add_cross_id(result["cross_ids"], tgt_provider, tgt_id)
                # Also pull IDs from the child's own `id` block (in case it's a multi-id entry)
                child_id_block = child.get("id") or {}
                child_provider = child_id_block.get("provider")
                child_pid = child_id_block.get("provider_id")
                if child_provider and child_pid:
                    _add_cross_id(result["cross_ids"], child_provider, child_pid)
                # If child exposes its own relationships (e.g. "this anidb id is also a mal id"),
                # surface them too — sometimes AniBridge returns nested twins.
                for sub_rel in child.get("relationships") or []:
                    sub_desc = (sub_rel.get("target") or {}).get("descriptor")
                    if not sub_desc:
                        continue
                    parts = sub_desc.split(":")
                    if len(parts) != 2:
                        continue
                    _add_cross_id(result["cross_ids"], parts[0], parts[1])

        # Dedupe + sort cross_ids
        for k, v in result["cross_ids"].items():
            seen = []
            for x in v:
                if x not in seen:
                    seen.append(x)
            result["cross_ids"][k] = seen

        return result
    finally:
        if own_client:
            await client.aclose()


def _add_cross_id(cross_ids: dict, provider: str, raw_id: str) -> None:
    """Push a raw provider id into cross_ids, with type coercion."""
    if provider not in SUPPORTED_PROVIDERS:
        return
    bucket = cross_ids.setdefault(provider, [])
    # Coerce to int where it makes sense (anilist, mal, kitsu, tmdb_*, tvdb_*)
    if provider in ("anilist", "mal", "kitsu", "tmdb_movie", "tmdb_show", "tvdb_movie", "tvdb_show"):
        try:
            bucket.append(int(raw_id))
            return
        except (TypeError, ValueError):
            pass
    bucket.append(raw_id)


def _normalise_meta(meta: dict, anilist_id: int) -> dict:
    """Build a normalised result dict from a single AniBridge metadata payload."""
    if not meta:
        return {
            "anilist_id": anilist_id,
            "anibridge_meta": {},
            "units": None,
            "release": None,
            "runtime_minutes": None,
            "titles": None,
            "synopsis": None,
            "ratings": None,
            "genres": [],
            "images": [],
            "cross_ids": {"anilist": [anilist_id]} if anilist_id else {},
            "relationships": [],
            "sources_used": [],
        }

    units = meta.get("units")
    if units is None:
        units = None
    else:
        try:
            units = int(units)
        except (TypeError, ValueError):
            units = None

    runtime = (meta.get("runtime") or {}).get("minutes")
    try:
        runtime_minutes = int(runtime) if runtime is not None else None
    except (TypeError, ValueError):
        runtime_minutes = None

    cross_ids: dict[str, list] = {}
    if anilist_id:
        cross_ids["anilist"] = [anilist_id]
    # Self-id (in case AniBridge returns a different provider as primary)
    id_block = meta.get("id") or {}
    self_provider = id_block.get("provider")
    self_pid = id_block.get("provider_id")
    if self_provider and self_pid and self_provider != "anilist":
        _add_cross_id(cross_ids, self_provider, str(self_pid))

    return {
        "anilist_id": anilist_id,
        "anibridge_meta": meta,
        "units": units,
        "release": meta.get("release"),
        "runtime_minutes": runtime_minutes,
        "titles": meta.get("titles"),
        "synopsis": meta.get("synopsis"),
        "ratings": meta.get("ratings"),
        "genres": ((meta.get("classification") or {}).get("genres") or []),
        "images": meta.get("images") or [],
        "cross_ids": cross_ids,
        "relationships": [],
        "sources_used": [],
    }


def stats() -> dict:
    """Static stats for /health endpoint (no in-memory state)."""
    return {
        "api": ANIBRIDGE_API,
        "providers": list(SUPPORTED_PROVIDERS),
    }
