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
      - Fribb's `episode_offset.tmdb` is present (most reliable signal).
      - OR: AniList title contains "Season N" / "Nth Season" / "Part N" /
        "Cour N", AND TMDB has a season N to fetch.
      - OR: AniList's startDate matches TMDB's season N air_date.
      - OR: Multiple AniList IDs share the same TMDB TV ID.

    Slicing:
      offset = episode_offset.tmdb or 0
      count  = anilist.episodes (e.g. 25)
      → take TMDB episodes [offset+1 .. offset+count] and renumber 1..count.

PATTERN B — "single AniList entry, multiple TMDB seasons"
    AniList has one entry for the entire series. TMDB has many seasons.
    e.g. One Piece: AniList 21, TMDB 37854 (23 seasons, 1181 episodes).
    e.g. Naruto: AniList 20, TMDB 46260 (4 seasons, 220 episodes total).

    Detection:
      - Fribb has only `themoviedb_id.tv`, no `season` field.
      - AniList title does NOT contain "Season N" / "Part N" (otherwise
        we'd be in Pattern A).
      - AniList's `episodes` count == SUM of all TMDB seasons' episode_counts.
      - OR: AniList's `episodes` is null AND series is RELEASING.

    Slicing:
      → fetch ALL non-special TMDB seasons in parallel.
      → RENUMBER continuously 1..N (sort by season_number then episode_number).
         We don't trust TMDB's own episode_number across seasons because some
         shows reset per season (Hell Mode) and others continue (One Piece).
      → if AniList.episodes is non-null: cap at that count.
      → if AniList.nextAiringEpisode is set: cap at nextAiring - 1 (only
         aired episodes).
      → else (no count, no nextAiring): cap at "aired today" via air_date filter.

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

import re
import time
from typing import Optional

from sources import fribb


# Patterns to detect season number from AniList title (english or romaji)
# Order matters: longer patterns first.
_SEASON_TITLE_PATTERNS = [
    # "Season 2", "season 3"
    (re.compile(r'\bSeason\s+(\d+)\b', re.IGNORECASE), 'season'),
    # "2nd Season", "3rd Season", "1st Season"
    (re.compile(r'(\d+)(?:st|nd|rd|th)\s+Season', re.IGNORECASE), 'season'),
    # "Part 2", "Part II"
    (re.compile(r'\bPart\s+(\d+)\b', re.IGNORECASE), 'part'),
    # "Cour 2", "Cour II"
    (re.compile(r'\bCour\s+(\d+)\b', re.IGNORECASE), 'cour'),
    # "II", "III" (Roman numerals at end of title)
    # Only match if it's at the end AND not part of a word
    (re.compile(r'\s(II|III|IV|V|VI|VII|VIII|IX|X)\s*$', re.IGNORECASE), 'roman'),
]


def detect_season_from_title(title: Optional[str]) -> Optional[int]:
    """Parse an AniList title for season number.
    Returns int (1-indexed) or None if no season indicator found.
    """
    if not title:
        return None
    for pat, kind in _SEASON_TITLE_PATTERNS:
        m = pat.search(title)
        if m:
            val = m.group(1)
            if kind == 'roman':
                roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
                             'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10}
                return roman_map.get(val.upper())
            else:
                try:
                    n = int(val)
                    if 1 <= n <= 50:  # sanity check
                        return n
                except (TypeError, ValueError):
                    pass
    return None


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

    # >1 TMDB season. Detect Pattern A via title (e.g. "Foo Season 2",
    # "Foo 2nd Season", "Foo Part 2", "Foo Cour 2") — these are split-cour
    # entries that map to ONE specific TMDB season.
    title_en = (anilist_data.get("title") or {}).get("english") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""
    title_season = detect_season_from_title(title_en) or detect_season_from_title(title_romaji)
    if title_season is not None and title_season <= len(tmdb_seasons):
        return "pattern_a"

    # Pattern A via date matching: AniList's startDate matches a TMDB season's
    # air_date AND AniList count < total TMDB count (split-cour hint).
    anilist_eps = _safe_int(anilist_data.get("episodes"))
    total_tmdb_eps = sum(s.get("episode_count", 0) or 0 for s in tmdb_seasons)
    anilist_start = anilist_data.get("startDate") or {}
    if (anilist_eps is not None and anilist_eps < total_tmdb_eps
            and anilist_start.get("year") and anilist_start.get("month") and anilist_start.get("day")):
        anilist_date_str = f"{anilist_start['year']:04d}-{anilist_start['month']:02d}-{anilist_start['day']:02d}"
        for s in tmdb_seasons:
            air = s.get("air_date") or ""
            if air == anilist_date_str:
                return "pattern_a"

    # AniList count == sum of all TMDB seasons → Pattern B (e.g. Naruto)
    if anilist_eps is not None and anilist_eps == total_tmdb_eps:
        return "pattern_b"

    # AniList count > total TMDB eps → Pattern B (TMDB incomplete)
    if anilist_eps is not None and anilist_eps > total_tmdb_eps:
        return "pattern_b"

    # AniList count is null (ongoing like One Piece) → Pattern B
    if anilist_eps is None:
        return "pattern_b"

    # AniList count < total TMDB eps but no title/date match — could be
    # a hidden split-cour. Try matching count to a single TMDB season.
    for s in tmdb_seasons:
        if s.get("episode_count") == anilist_eps:
            return "pattern_a"

    # Default: Pattern B
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
    anilist_start = anilist_data.get("startDate") or {}

    # Calculate total TMDB episodes (sum of all non-special seasons)
    total_tmdb_eps = sum(s.get("episode_count", 0) or 0 for s in tmdb_seasons)

    # If TMDB has only 1 non-special season, we're in Pattern C — easy.
    if len(tmdb_seasons) == 1:
        s = tmdb_seasons[0]
        count = anilist_eps  # use AniList as the truth
        return _result([s["season_number"]], 0, count, "pattern_c", False)

    # >1 TMDB season. Check Pattern B FIRST (AniList count == sum of all TMDB
    # seasons' episode_counts). This catches Naruto (AniList=220, TMDB total=220)
    # before any title/date detection can falsely route to Pattern A.
    if anilist_eps is not None and anilist_eps == total_tmdb_eps:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, anilist_eps, "pattern_b", True)

    # AniList count > total TMDB eps — TMDB might be incomplete; fetch all.
    if anilist_eps is not None and anilist_eps > total_tmdb_eps:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, None, "pattern_b", True)

    # AniList has no episode count — likely ongoing. Pattern B with all seasons.
    if anilist_eps is None:
        season_numbers = [s["season_number"] for s in tmdb_seasons]
        return _result(season_numbers, 0, None, "pattern_b", True)

    # ── Title-based season detection (Pattern A inferred from title) ──
    # AniList titles like "Foo Season 2", "Foo 2nd Season", "Foo Part 2",
    # "Foo Cour 2" → we know this is a split-cour entry. Match the season
    # number from the title to a TMDB season.
    title_en = (anilist_data.get("title") or {}).get("english") or ""
    title_romaji = (anilist_data.get("title") or {}).get("romaji") or ""
    title_season = detect_season_from_title(title_en) or detect_season_from_title(title_romaji)

    if title_season is not None and title_season <= len(tmdb_seasons):
        # Found "Season N" in title and TMDB has season N. Use it.
        # This catches Hell Mode S2, Demon Slayer S2, Attack on Titan S2, etc.
        target = tmdb_seasons[title_season - 1]  # title_season is 1-indexed
        count = anilist_eps  # use AniList's count
        return _result([target["season_number"]], 0, count, "pattern_a_title", False)

    # ── Date-based season detection (Pattern A inferred from air date) ──
    # If AniList's startDate matches a specific TMDB season's air_date exactly,
    # we know this entry belongs to that season (even without title info).
    # Only used when AniList count < total TMDB count (split-cour hint).
    if anilist_eps is not None and anilist_eps < total_tmdb_eps:
        if anilist_start.get("year") and anilist_start.get("month") and anilist_start.get("day"):
            anilist_date_str = f"{anilist_start['year']:04d}-{anilist_start['month']:02d}-{anilist_start['day']:02d}"
            for s in tmdb_seasons:
                air = s.get("air_date") or ""
                if air == anilist_date_str:
                    count = anilist_eps
                    return _result([s["season_number"]], 0, count, "pattern_a_date", False)

    # AniList count < total TMDB eps but no title or date match.
    # Try matching AniList's count to a single TMDB season's episode_count
    # (Pattern A inferred — split-cour where Fribb didn't have offset info
    # and the title didn't contain "Season N").
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

    # Sort episodes. For Pattern B (continuous_numbering=True), we sort by
    # (season_number, episode_number) to keep S1 episodes before S2 episodes
    # even when TMDB's season numbers reset (e.g. Hell Mode S1=1..12, S2=1..12
    # — without season-aware sort, eps 1,1,2,2,... would be interleaved).
    # For Pattern A/C, sort by episode_number alone is fine (single season).
    try:
        if continuous_numbering:
            eps_sorted = sorted(tmdb_episodes, key=lambda e: (
                e.get("season", 0) or 0,
                e.get("number", 0) or 0,
            ))
        else:
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

        # Renumber:
        # - For continuous_numbering (Pattern B): always renumber 1..N based on
        #   position. We DON'T trust TMDB's number because some shows reset
        #   per season (Hell Mode S1=1..12, S2=1..12). Sorting by (season, ep)
        #   above puts them in the right order, then we just number sequentially.
        # - For per-entry renumbering (Pattern A): use new_idx (1..count).
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
