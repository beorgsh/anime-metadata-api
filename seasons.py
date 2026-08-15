"""
seasons.py — Season detection + episode slicing logic (the API's "own brain"
for handling the 3 multi-season patterns in the wild).

Patterns we handle
==================

PATTERN A — "single TMDB season, multiple AniList entries"
    TMDB merges all seasons into one giant S1. AniList splits them into
    separate entries (e.g. Re:Zero S1 / S2 Part 1 / S2 Part 2 / S3 are 4
    separate AniList IDs but all map to TMDB 65942 with one 85-episode S1).

    Detection:
      - Fribb's `themoviedb_id.tv` is the same for ≥2 different AniList IDs.
      - Fribb's `episode_offset.tmdb` is present (e.g. 26 for S2P1, 38 for S2P2).
      - AniList's `episodes` count is small (e.g. 25 for S1).

    Slicing:
      offset = episode_offset.tmdb or 0
      count  = anilist.episodes (e.g. 25)
      → take TMDB episodes [offset+1 .. offset+count] and renumber 1..count.

PATTERN B — "single AniList entry, multiple TMDB seasons"
    AniList has one entry for the entire series. TMDB has many seasons.
    e.g. One Piece: AniList 21, TMDB 37854 (23 seasons, 1181 episodes).

    Detection:
      - Fribb has only `themoviedb_id.tv`, no `season` field.
      - AniList's `episodes` is null (ongoing) OR doesn't match any single
        TMDB season's episode_count.
      - TMDB `details.seasons[]` has >1 non-special season.

    Slicing:
      → fetch ALL non-special TMDB seasons in parallel.
      → concatenate episodes with continuous numbering (TMDB-style).
      → if AniList.episodes is null AND status == RELEASING: filter out
         episodes whose air_date > today (upcoming episodes).
      → if AniList.episodes is non-null: slice to that count.

PATTERN C — "simple: 1 AniList entry, 1 TMDB season"
    e.g. Cowboy Bebop: AniList 1, TMDB 30991 (1 season, 26 episodes).

    Detection:
      - Fribb has season.tmdb = 1, no episode_offset.
      - AniList episodes count matches TMDB S1 episode_count.
      - TMDB `details.seasons[]` (non-special) has exactly 1 entry.

    Slicing:
      → fetch TMDB S1.
      → if AniList.episodes is non-null, slice to that count.
      → else use TMDB S1 episodes as-is.

MOVIES — TMDB `/movie/{id}` instead of `/tv/{id}/season/{n}`.

NONE — couldn't resolve a TMDB ID. Use AniList-only fallback.

This module is pure-functional: it makes zero HTTP calls. All it does is
inspect the data already fetched and decide what to slice/return.
"""
from __future__ import annotations

import time
from typing import Optional

from sources import fribb


# ─── Public API ─────────────────────────────────────────────────────


def detect_pattern(
    *,
    anilist_id: int,
    anilist_data: dict,
    fribb_data: Optional[dict],
    tmdb_type: Optional[str],
    tmdb_id,
    tmdb_details: dict,
    extras_mode: bool = False,
) -> str:
    """Return one of: 'movie', 'pattern_a', 'pattern_b', 'pattern_c', 'extras', 'not_found'.

    Pattern A: Multiple AniList entries map to the SAME TMDB TV ID and Fribb
               gives us an episode_offset for each (e.g. Re:Zero S1/S2/S2P2/S3).
               Or: AniList splits a single TMDB season into multiple entries.
    Pattern B: One AniList entry maps to a TMDB series with MANY seasons that
               should be concatenated (e.g. One Piece).
    Pattern C: One AniList entry, one TMDB season — the simple case.
    """
    if extras_mode:
        return "extras"
    if tmdb_type == "movie":
        return "movie"
    if not (tmdb_type and tmdb_id):
        return "not_found"

    # Discover if other AniList IDs share the same TMDB TV ID (Pattern A hint)
    siblings = fribb.lookup_siblings_by_tmdb_tv(int(tmdb_id)) if tmdb_id else []

    # The DECISIVE Pattern A signal is: Fribb's episode_offset.tmdb is present
    # for THIS entry OR for a sibling. If any sibling has an offset, AniList
    # has split this anime into multiple entries — so even the first entry
    # (offset=0, which Fribb doesn't bother recording) is Pattern A.
    # (e.g. Re:Zero S1 has no offset in Fribb but S2P1 has offset=26.)
    if fribb_data:
        offset_info = fribb_data.get("episode_offset") or {}
        if offset_info.get("tmdb"):
            return "pattern_a"
    if len(siblings) >= 2:
        # Check if any sibling has an episode_offset (Pattern A signal)
        for sibling_aid in siblings:
            if sibling_aid == anilist_id:
                continue
            sib_data = fribb.lookup(sibling_aid)
            if sib_data and (sib_data.get("episode_offset") or {}).get("tmdb"):
                return "pattern_a"

    # Inspect TMDB's seasons array (non-special only)
    tmdb_seasons = [s for s in (tmdb_details.get("seasons") or []) if s.get("season_number", 0) > 0]

    # If TMDB has only 1 non-special season, we're in Pattern C — even if there
    # are sibling AniList IDs (those siblings might be recap movies / specials,
    # not actual season splits).
    if len(tmdb_seasons) <= 1:
        return "pattern_c"

    # >1 TMDB season. Pattern B if AniList considers this a single entry that
    # spans all seasons (no offset, no Fribb season info). Pattern A only if
    # AniList explicitly split this into multiple entries that map to the same TMDB ID
    # AND Fribb gave us an offset — which we already returned above.
    # So this branch is Pattern B.
    return "pattern_b"


def resolve_target_season(
    *,
    anilist_id: int,
    anilist_data: dict,
    fribb_data: Optional[dict],
    tmdb_details: dict,
    explicit_season: Optional[int] = None,
) -> dict:
    """
    Decide which TMDB season(s) to fetch and how to slice the result.

    Returns:
        {
            "season_numbers": [int, ...],  # TMDB season(s) to fetch
            "episode_offset": int,        # 0-indexed offset into the fetched episodes
            "episode_count": int|None,     # how many episodes to keep (None = all)
            "pattern": str,
            "continuous_numbering": bool,  # True = TMDB-style (1..N), False = renumber per-entry (1..N per AniList entry)
        }
    """
    # Movie — no season concept
    if (tmdb_details or {}).get("name") is not None and not (tmdb_details.get("seasons")):
        return _result([0], 0, None, "movie", False)

    # Explicit season override (?season=2)
    if explicit_season is not None:
        return _result([explicit_season], 0, None, "explicit", False)

    # Pattern A detection — Fribb has season + maybe offset
    if fribb_data:
        season_info = fribb_data.get("season") or {}
        offset_info = fribb_data.get("episode_offset") or {}
        tmdb_season = season_info.get("tmdb")
        tmdb_offset = offset_info.get("tmdb") or 0
        if tmdb_season is not None:
            anilist_eps = _safe_int(anilist_data.get("episodes"))
            count = anilist_eps if anilist_eps and anilist_eps > 0 else None
            return _result([int(tmdb_season)], int(tmdb_offset), count, "pattern_a", False)

    # Pattern B / C detection — look at TMDB details
    tmdb_seasons = [s for s in (tmdb_details.get("seasons") or []) if s.get("season_number", 0) > 0]
    if not tmdb_seasons:
        # No non-special seasons — try S1 anyway as a last resort
        return _result([1], 0, None, "fallback_s1", False)

    anilist_eps = _safe_int(anilist_data.get("episodes"))
    anilist_year = _safe_int(((anilist_data.get("startDate") or {}).get("year")))

    # If TMDB has only 1 non-special season, we're in Pattern C — easy.
    if len(tmdb_seasons) == 1:
        s = tmdb_seasons[0]
        count = anilist_eps  # use AniList as the truth
        return _result([s["season_number"]], 0, count, "pattern_c", False)

    # >1 TMDB season — try to match AniList entry to ONE TMDB season
    # Match by year first (most reliable for split-cour anime), then by episode count.
    # Special case: if AniList has no episode count (ongoing series like One Piece),
    # we want ALL TMDB seasons (Pattern B), not just S1.
    if anilist_eps is None:
        # Ongoing series — fetch all seasons, continuous numbering
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, None, "pattern_b", True)

    # AniList gave us an episode count — check if it equals the SUM of all TMDB
    # seasons' episode_counts. If yes, this is Pattern B (e.g. Naruto: AniList=220,
    # TMDB has 4 seasons summing to 220). Don't try to match a single season.
    total_tmdb_eps = sum(s.get("episode_count", 0) or 0 for s in tmdb_seasons)
    if anilist_eps == total_tmdb_eps:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, anilist_eps, "pattern_b", True)

    # AniList count is between 1 and total_tmdb_eps — maybe AniList is a subset
    # (e.g. Doraemon 2005: AniList says next=929, but TMDB has 1464 "aired" eps).
    # In this case, we fetch all seasons and cap at anilist_eps.
    if anilist_eps > 0 and anilist_eps < total_tmdb_eps:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, anilist_eps, "pattern_b", True)

    # AniList count > total TMDB eps — TMDB might be incomplete; fetch all and
    # return what we have (cap at anilist_eps to allow growth).
    if anilist_eps > total_tmdb_eps:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, None, "pattern_b", True)

    # Try matching to a single TMDB season (Pattern A inferred — split-cour where
    # Fribb didn't have offset info)
    best_season = _match_anilist_to_tmdb_season(
        tmdb_seasons, anilist_eps=anilist_eps, anilist_year=anilist_year,
    )
    if best_season is not None:
        return _result([best_season], 0, anilist_eps, "pattern_a_inferred", False)

    # Couldn't match a single season — fetch ALL non-special seasons (Pattern B)
    season_numbers = [s["season_number"] for s in tmdb_seasons]
    return _result(season_numbers, 0, anilist_eps, "pattern_b", True)


def slice_episodes(
    tmdb_episodes: list[dict],
    *,
    offset: int = 0,
    count: Optional[int] = None,
    continuous_numbering: bool = False,
    include_upcoming: bool = False,
    anilist_id: int,
    anilist_next_airing: Optional[int] = None,
) -> list[dict]:
    """
    Apply offset, count, renumbering, and unaired-episode filtering to a list
    of TMDB episodes.

    Args:
        tmdb_episodes: episodes returned by tmdb.extract_episodes() (or
            concatenated from multiple seasons for Pattern B).
        offset: Fribb's episode_offset.tmdb. This is a 1-INDEXED TMDB
            episode_number indicating where this AniList entry starts
            (e.g. offset=26 means "starts at TMDB episode 26"). A value of
            0 or None means "starts at the beginning" (no offset).
        count: max number of episodes to keep. None = keep all (filter by air date only).
        continuous_numbering: True = keep TMDB's continuous numbering (1..N for
            the whole series, e.g. One Piece). False = renumber 1..count for
            this AniList entry (e.g. Re:Zero S2 starts at 1, not 26).
        include_upcoming: False (default) = drop episodes whose air_date > today.
        anilist_id: used to build the episode ID.
        anilist_next_airing: AniList's `nextAiringEpisode.episode` value.
            If set, we cap the returned episodes at (anilist_next_airing - 1)
            because that's AniList's authoritative "last aired" episode.
            This catches the case where TMDB has future-dated episodes that
            are actually recaps/specials/announcements (e.g. Doraemon 2005).
    """
    if not tmdb_episodes:
        return []

    # Sort by original number ascending (TMDB seasons come in order)
    try:
        eps_sorted = sorted(tmdb_episodes, key=lambda e: e.get("number", 0))
    except Exception:
        eps_sorted = list(tmdb_episodes)

    # Fribb's episode_offset.tmdb is 1-INDEXED — it's the TMDB episode_number
    # where this AniList entry starts. So if offset=26, we want TMDB episode 26
    # to be our first episode. In a 0-indexed Python list, episode N lives at
    # index N-1. So we convert: start_index = max(0, offset - 1).
    if offset and offset > 0:
        start = max(0, offset - 1)
    else:
        start = 0
    if count is not None and count > 0:
        end = start + count
    else:
        end = len(eps_sorted)
    sliced = eps_sorted[start:end]

    today = time.strftime("%Y-%m-%d")
    out: list[dict] = []
    for new_idx, ep in enumerate(sliced, start=1):
        # Filter unaired episodes by default
        air_date = ep.get("airDate") or ""
        if not include_upcoming and air_date and air_date > today:
            continue

        # Renumber
        if continuous_numbering:
            new_number = ep.get("number", new_idx)
        else:
            new_number = new_idx

        # ── Cap by AniList's nextAiringEpisode ──
        # AniList says "next episode to air is N" → that means episodes 1..N-1
        # have aired. If we've already returned N-1 episodes, stop. This catches
        # cases where TMDB has extra episodes (recaps, multi-part specials, etc.)
        # that aren't real AniList episodes.
        if anilist_next_airing and anilist_next_airing > 0:
            if continuous_numbering:
                # Continuous: episode number = TMDB's number. Cap at next_airing-1.
                if ep.get("number", 0) >= anilist_next_airing:
                    continue
            else:
                # Per-entry renumbered: cap at next_airing-1 in the renumbered space.
                if new_number >= anilist_next_airing:
                    continue

        new_ep = dict(ep)  # shallow copy
        new_ep["number"] = new_number
        new_ep["id"] = f"{anilist_id}-{new_number}"
        # Preserve the original TMDB episode number as a cross-reference
        if ep.get("number") != new_number:
            new_ep["tmdbEpisodeNumber"] = ep.get("number")
        out.append(new_ep)
    return out


def get_seasons_summary(
    *,
    anilist_id: int,
    tmdb_details: dict,
    fribb_data: Optional[dict],
    sibling_anilist_ids: list[int],
) -> list[dict]:
    """
    Build the `seasons` array for the response — gives the frontend enough
    info to render a "Season 1 / Season 2 / Specials" picker.

    Each entry: {
        "season_number": int,
        "name": str,
        "air_date": str,
        "episode_count": int,
        "is_specials": bool,                  # True for TMDB S0
        "anilist_ids": [int, ...],            # sibling AniList entries mapped to this TMDB series
        "is_current": bool,                   # True for the season this AniList ID maps to
    }
    """
    out: list[dict] = []
    current_season = None
    if fribb_data:
        season_info = fribb_data.get("season") or {}
        current_season = season_info.get("tmdb")

    for s in (tmdb_details.get("seasons") or []):
        sn = s.get("season_number", 0)
        is_specials = sn == 0
        out.append({
            "season_number": sn,
            "name": s.get("name") or ("Specials" if is_specials else f"Season {sn}"),
            "air_date": s.get("air_date") or "",
            "episode_count": s.get("episode_count", 0) or 0,
            "is_specials": is_specials,
            "anilist_ids": sibling_anilist_ids,
            "is_current": (sn == current_season) if current_season is not None else False,
        })
    return out


# ─── Internal helpers ───────────────────────────────────────────────


def _result(seasons: list[int], offset: int, count: Optional[int],
            pattern: str, continuous: bool) -> dict:
    return {
        "season_numbers": seasons,
        "episode_offset": offset,
        "episode_count": count,
        "pattern": pattern,
        "continuous_numbering": continuous,
    }


def _safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        iv = int(v)
        return iv if iv > 0 else None
    except (TypeError, ValueError):
        return None


def _match_anilist_to_tmdb_season(
    tmdb_seasons: list[dict],
    *,
    anilist_eps: Optional[int],
    anilist_year: Optional[int],
) -> Optional[int]:
    """Try to match an AniList entry to exactly one TMDB season.
    Strategy: year match first (most reliable for split-cour anime), then
    episode-count match if year is missing.
    """
    if not tmdb_seasons:
        return None

    # 1. Year match: find the TMDB season whose air_date year == AniList year
    if anilist_year:
        candidates = []
        for s in tmdb_seasons:
            air = s.get("air_date") or ""
            if air and air.startswith(str(anilist_year)):
                candidates.append(s)
        if len(candidates) == 1:
            return candidates[0]["season_number"]
        if len(candidates) > 1 and anilist_eps:
            # Tie-break by episode count
            for c in candidates:
                if c.get("episode_count") == anilist_eps:
                    return c["season_number"]
            # If still ambiguous, take the first
            return candidates[0]["season_number"]
        if candidates:
            return candidates[0]["season_number"]

    # 2. Episode count match: TMDB season whose episode_count == AniList eps
    if anilist_eps:
        for s in tmdb_seasons:
            if s.get("episode_count") == anilist_eps:
                return s["season_number"]

    return None
