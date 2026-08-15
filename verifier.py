"""
verifier.py — Cross-source episode-count & season verification.

This module implements the API's "own brain" for verifying two pieces of
metadata that historically come back wrong from any single source:

  1. Episode count (totalEpisodes)
     - AniList is often null or stale for ongoing series
     - TMDB sometimes has stale season counts after a show has been renewed
     - AniZip occasionally returns 0 for very new shows
     - MAL (via Jikan) is usually reliable for finished series
     - Kitsu occasionally disagrees with everyone else
     - AniBridge returns a cross-provider-verified `units` field

  2. Season (winter/spring/summer/fall + year)
     - AniList GraphQL has its own `season` + `seasonYear`
     - MAL (Jikan) reports `season` + `year`
     - Kitsu reports `season` + start-date year
     - AniBridge aggregates the above + release.start_date
     - TMDB reports a TV series' "seasons" array (different concept — TMDB
       "season" = a physical season, not the broadcast season; we don't use
       TMDB for the broadcast-season check)

Strategy (majority vote + confidence-weighted fallback):

  For episode count:
    - Collect (value, source, weight) tuples from every source that returned a
      non-null, non-zero value.
    - If 2+ sources agree, return that value with confidence="high" + list the
      agreeing sources.
    - If sources disagree and only one of them is the AniList primary, return
      the AniList value with confidence="medium_low" + a `discrepancies` array.
    - If AniList is null, return the highest-weighted value with
      confidence="medium".
    - If no source has data, return None with confidence="none".

  For season:
    - Normalise all to lowercase ("winter" | "spring" | "summer" | "fall").
    - Same majority-vote logic.
    - If sources disagree, prefer AniList > AniBridge > MAL > Kitsu > start-date inference.

  For season year:
    - Same majority-vote logic over integer years.

This module is intentionally pure-functional: it takes already-fetched data
from each source and produces a verdict. It makes zero HTTP calls.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Optional

log = logging.getLogger("verifier")


# ─── Public entry point ──────────────────────────────────────────────


def verify_episode_count(
    *,
    anilist_episodes: Optional[int],
    anibridge_units: Optional[int],
    jikan_episodes: Optional[int],
    kitsu_episode_count: Optional[int],
    anizip_episode_count: Optional[int],
    tmdb_episode_count: Optional[int],
) -> dict:
    """
    Cross-verify the total episode count of an anime across all available sources.

    Returns:
        {
            "value": int | None,                  # final agreed-upon count
            "confidence": "high" | "medium" | "medium_low" | "low" | "none",
            "sources": [str, ...],                  # sources that agreed on `value`
            "all_sources": {source: int, ...},     # what each source said
            "discrepancies": [                      # only populated when confidence < high
                {"source": str, "value": int},
                ...
            ],
            "method": "majority" | "anilist_only" | "single_source" | "none",
        }
    """
    all_sources = {
        "anilist": anilist_episodes,
        "anibridge": anibridge_units,
        "jikan": jikan_episodes,
        "kitsu": kitsu_episode_count,
        "anizip": anizip_episode_count,
        "tmdb": tmdb_episode_count,
    }

    # Weights — AniList is the user's "primary graph URL" so it gets a slight
    # preference in tiebreaks, but majority still wins outright.
    weights = {
        "anilist": 3,
        "anibridge": 3,  # already cross-provider-verified upstream
        "jikan": 2,
        "kitsu": 2,
        "anizip": 2,
        "tmdb": 2,
    }

    # Drop nulls and zeros (a "0" almost always means "unknown", not "0 episodes")
    candidates: list[tuple[str, int, int]] = []
    for src, val in all_sources.items():
        if val is None:
            continue
        try:
            ival = int(val)
        except (TypeError, ValueError):
            continue
        if ival <= 0:
            continue
        candidates.append((src, ival, weights.get(src, 1)))

    if not candidates:
        return _empty_verdict(all_sources, "none")

    # Majority vote (≥2 distinct sources agreeing on the same value)
    value_counts: Counter[int] = Counter()
    value_sources: dict[int, list[str]] = {}
    for src, val, _ in candidates:
        value_counts[val] += 1
        value_sources.setdefault(val, []).append(src)

    # Find the value with the most supporting sources (tie-break by total weight)
    best_val, best_count = max(
        value_counts.items(),
        key=lambda kv: (kv[1], sum(weights.get(s, 1) for s in value_sources[kv[0]])),
    )

    if best_count >= 2:
        return {
            "value": best_val,
            "confidence": "high",
            "sources": value_sources[best_val],
            "all_sources": _coerce_ints(all_sources),
            "discrepancies": [
                {"source": s, "value": v}
                for s, v in _coerce_ints(all_sources).items()
                if v is not None and v != best_val
            ],
            "method": "majority",
        }

    # Single source only — pick by weight
    candidates.sort(key=lambda x: x[2], reverse=True)
    only_src, only_val = candidates[0][0], candidates[0][1]
    is_anilist_only = only_src == "anilist"

    return {
        "value": only_val,
        "confidence": "medium_low" if is_anilist_only else "medium",
        "sources": [only_src],
        "all_sources": _coerce_ints(all_sources),
        "discrepancies": [],
        "method": "anilist_only" if is_anilist_only else "single_source",
    }


def verify_season(
    *,
    anilist_season: Optional[str],
    anilist_year: Optional[int],
    anibridge_release: Optional[dict],
    jikan_season: Optional[str],
    jikan_year: Optional[int],
    kitsu_season: Optional[str],
    kitsu_year: Optional[int],
    fallback_year: Optional[int],
) -> dict:
    """
    Cross-verify the broadcast season (winter|spring|summer|fall) and year.

    AniList GraphQL is treated as the primary source ("analyst graph URL")
    per the user's requirement; AniBridge, MAL, and Kitsu act as verifiers.

    If AniList is null but another source has data, we fall back to that.

    Returns:
        {
            "season": "winter"|"spring"|"summer"|"fall"|None,
            "year": int | None,
            "confidence": "high"|"medium"|"medium_low"|"low"|"none",
            "season_sources": [str, ...],
            "year_sources": [str, ...],
            "all_seasons": {source: str, ...},
            "all_years": {source: int, ...},
            "method": "majority"|"anilist_only"|"single_source"|"none",
        }
    """
    # ── season (string) ──
    season_inputs = {
        "anilist": _norm_season(anilist_season),
        "anibridge": _season_from_release(anibridge_release),
        "jikan": _norm_season(jikan_season),
        "kitsu": _norm_season(kitsu_season),
    }
    season_verdict = _vote_str(season_inputs, primary="anilist")

    # ── year (int) ──
    year_inputs = {
        "anilist": anilist_year,
        "anibridge": _year_from_release(anibridge_release),
        "jikan": jikan_year,
        "kitsu": kitsu_year,
        "fallback": fallback_year,  # from AniList startDate.year if AniList seasonYear was null
    }
    year_verdict = _vote_int(year_inputs, primary="anilist")

    return {
        "season": season_verdict["value"],
        "year": year_verdict["value"],
        "confidence": _min_confidence(season_verdict["confidence"], year_verdict["confidence"]),
        "season_sources": season_verdict["sources"],
        "year_sources": year_verdict["sources"],
        "all_seasons": season_inputs,
        "all_years": _coerce_ints(year_inputs),
        "method": season_verdict["method"] if season_verdict["value"] else year_verdict["method"],
    }


def verify_status(
    *,
    anilist_status: Optional[str],
    anibridge_release: Optional[dict],
    jikan_status: Optional[str],
    kitsu_status: Optional[str],
) -> dict:
    """
    Cross-verify airing status (FINISHED | RELEASING | NOT_YET_RELEASED | CANCELLED | ...).

    Returns:
        {
            "value": "FINISHED"|"RELEASING"|"NOT_YET_RELEASED"|"CANCELLED"|"UNKNOWN"|None,
            "confidence": "high"|"medium"|"medium_low"|"none",
            "sources": [str, ...],
            "all_sources": {source: str, ...},
            "method": "majority"|"anilist_only"|"single_source"|"none",
        }
    """
    inputs = {
        "anilist": _norm_status(anilist_status),
        "anibridge": _status_from_release(anibridge_release),
        "jikan": _status_from_jikan(jikan_status),
        "kitsu": _status_from_kitsu(kitsu_status),
    }
    verdict = _vote_str(inputs, primary="anilist")
    return {
        "value": verdict["value"],
        "confidence": verdict["confidence"],
        "sources": verdict["sources"],
        "all_sources": inputs,
        "method": verdict["method"],
    }


# ─── Internal helpers ────────────────────────────────────────────────


def _coerce_ints(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if v is None:
            out[k] = None
            continue
        try:
            iv = int(v)
            out[k] = iv if iv > 0 else None
        except (TypeError, ValueError):
            out[k] = None
    return out


def _norm_season(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    s = s.strip().upper()
    table = {
        "WINTER": "winter", "WINT": "winter",
        "SPRING": "spring", "SPR": "spring",
        "SUMMER": "summer", "SUM": "summer",
        "FALL": "fall", "AUTUMN": "fall", "AUT": "fall",
    }
    return table.get(s)


def _norm_status(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    s = s.upper().strip()
    if "FINISH" in s or "ENDED" in s:
        return "FINISHED"
    if "RELEAS" in s or "AIRING" in s and "NOT" not in s and "CANCEL" not in s:
        return "RELEASING"
    if "NOT YET" in s or "UPCOMING" in s:
        return "NOT_YET_RELEASED"
    if "CANCEL" in s or "DISCONTIN" in s:
        return "CANCELLED"
    return None


def _status_from_release(rel: Optional[dict]) -> Optional[str]:
    if not rel or not isinstance(rel, dict):
        return None
    status = (rel.get("status") or "").upper()
    if status in ("FINISHED", "ENDED"):
        return "FINISHED"
    if status in ("ONGOING", "CONTINUING"):
        return "RELEASING"
    if status in ("UPCOMING", "NOT_YET_RELEASED"):
        return "NOT_YET_RELEASED"
    if status in ("CANCELLED", "DISCONTINUED"):
        return "CANCELLED"
    return None


def _status_from_jikan(s: Optional[str]) -> Optional[str]:
    return _norm_status(s)


def _status_from_kitsu(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.lower()
    if s == "finished":
        return "FINISHED"
    if s == "current":
        return "RELEASING"
    if s == "upcoming":
        return "NOT_YET_RELEASED"
    if s in ("cancelled", "cancelled_canceled"):
        return "CANCELLED"
    return None


def _season_from_release(rel: Optional[dict]) -> Optional[str]:
    if not rel or not isinstance(rel, dict):
        return None
    start = rel.get("start_date")
    if not start or not isinstance(start, str):
        return None
    return _season_from_date(start)


def _year_from_release(rel: Optional[dict]) -> Optional[int]:
    if not rel or not isinstance(rel, dict):
        return None
    start = rel.get("start_date")
    if not start or not isinstance(start, str):
        return None
    try:
        return int(start[:4])
    except (TypeError, ValueError):
        return None


def _season_from_date(date_str: str) -> Optional[str]:
    """Infer broadcast season from a YYYY-MM-DD string."""
    try:
        m = int(date_str[5:7])
    except (TypeError, ValueError, IndexError):
        return None
    if m in (12, 1, 2):
        return "winter"
    if m in (3, 4, 5):
        return "spring"
    if m in (6, 7, 8):
        return "summer"
    if m in (9, 10, 11):
        return "fall"
    return None


def _vote_str(inputs: dict[str, Optional[str]], primary: str = "anilist") -> dict:
    """Majority vote on string values. Falls back to `primary` if no majority."""
    cleaned = {k: v for k, v in inputs.items() if v}
    if not cleaned:
        return {"value": None, "confidence": "none", "sources": [], "method": "none"}

    counts: Counter[str] = Counter(cleaned.values())
    top_val, top_n = counts.most_common(1)[0]
    sources_for_top = [k for k, v in cleaned.items() if v == top_val]

    if top_n >= 2:
        return {
            "value": top_val,
            "confidence": "high",
            "sources": sources_for_top,
            "method": "majority",
        }
    # No majority — prefer the primary if it had data, else the single non-null source
    if primary in cleaned:
        return {
            "value": cleaned[primary],
            "confidence": "medium_low",
            "sources": [primary],
            "method": "anilist_only" if primary == "anilist" else "single_source",
        }
    # Pick the only available source
    only_src = next(iter(cleaned))
    return {
        "value": cleaned[only_src],
        "confidence": "medium",
        "sources": [only_src],
        "method": "single_source",
    }


def _vote_int(inputs: dict[str, Optional[int]], primary: str = "anilist") -> dict:
    cleaned = {}
    for k, v in inputs.items():
        if v is None:
            continue
        try:
            iv = int(v)
        except (TypeError, ValueError):
            continue
        if iv > 0:
            cleaned[k] = iv
    if not cleaned:
        return {"value": None, "confidence": "none", "sources": [], "method": "none"}

    counts: Counter[int] = Counter(cleaned.values())
    top_val, top_n = counts.most_common(1)[0]
    sources_for_top = [k for k, v in cleaned.items() if v == top_val]

    if top_n >= 2:
        return {
            "value": top_val,
            "confidence": "high",
            "sources": sources_for_top,
            "method": "majority",
        }
    if primary in cleaned:
        return {
            "value": cleaned[primary],
            "confidence": "medium_low",
            "sources": [primary],
            "method": "anilist_only" if primary == "anilist" else "single_source",
        }
    only_src = next(iter(cleaned))
    return {
        "value": cleaned[only_src],
        "confidence": "medium",
        "sources": [only_src],
        "method": "single_source",
    }


_CONFIDENCE_RANK = {
    "high": 5,
    "medium": 4,
    "medium_low": 3,
    "low": 2,
    "none": 0,
}


def _min_confidence(a: str, b: str) -> str:
    """Take the weaker of two confidence labels."""
    ra, rb = _CONFIDENCE_RANK.get(a, 0), _CONFIDENCE_RANK.get(b, 0)
    return a if ra <= rb else b


def _empty_verdict(all_sources: dict, confidence: str = "none") -> dict:
    return {
        "value": None,
        "confidence": confidence,
        "sources": [],
        "all_sources": _coerce_ints(all_sources),
        "discrepancies": [],
        "method": "none",
    }
