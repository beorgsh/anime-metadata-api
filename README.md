# Anime Metadata API

Multi-source anime metadata API. Input an **AniList ID** → get back sliced + renumbered episode titles, thumbnails, banners, clearlogos, posters, and cross-database mappings.

**v3.0.0 — lean + fast + multi-season aware.** The pipeline now does just 3 network calls (AniList + Fribb + TMDB) instead of v2's 7-source fan-out, and the API's "own brain" (`seasons.py`) handles all 3 multi-season patterns correctly.

## The 3 multi-season patterns

| Pattern | Example | What it means |
|---------|---------|----------------|
| **A** | Re:Zero (AniList 21355, 108632, 119661, 163134) | AniList splits a show into multiple entries. TMDB merges them into one 85-episode S1. Fribb's `episode_offset.tmdb` tells us where each AniList entry starts (S1=0, S2P1=26, S2P2=38, S3=50). We slice + renumber 1..N per AniList entry. |
| **B** | One Piece (AniList 21) | One AniList entry, many TMDB seasons (23). We fetch all seasons in parallel, concatenate, continuous numbering, filter out unaired episodes. AniList says next is 1174 → we return 1173 aired. |
| **C** | Cowboy Bebop, FMA: Brotherhood | One AniList entry, one TMDB season — the simple case. |

The response includes a `pattern` field so the frontend knows what's going on.

## Sources (lean v3 pipeline)

| # | Source | Role | Latency |
|---|--------|------|---------|
| 1 | **AniList GraphQL** | Primary metadata: title, format, year, episode count, season, status | ~100ms |
| 2 | **Fribb** (in-memory) | AniList↔TMDB mapping + `season.tmdb` + `episode_offset.tmdb` + reverse index (TMDB→siblings) | ~0ms |
| 3 | **TMDB** | Episodes with stills + images (only the right season, sliced) | ~100ms |

**Fallback chain** (only runs if Fribb misses):
- AniBridge (1 call, ~150ms)
- TMDB `/find` by TVDB/IMDb ID (1 call)
- TMDB `/search` by name + year (1 call)

**Dropped from hot path** (they were v2's slowdown):
- Jikan/MAL (often 504s)
- Kitsu (extra round-trip)
- AniZip (TMDB images are usually faster)
- AniBridge cross-mapping chase (only used as resolver fallback)
- Heavy multi-source verification (replaced with a free AniList count check)

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/episodes/{anilist_id}` | Default view — auto-detect the right TMDB season from Fribb's mapping. |
| `GET` | `/api/episodes/{anilist_id}?season=N` | Explicit TMDB season (N=1, 2, 3, ...). Pass 0 for specials. |
| `GET` | `/api/episodes/{anilist_id}?include_upcoming=true` | Include episodes with future air dates. Default: only return aired episodes. |
| `GET` | `/api/episodes/{anilist_id}/extras` | TMDB season 0 (specials / OVAs / recaps) only. |
| `GET` | `/api/search?query=...&limit=10` | Search AniList by name. |
| `GET` | `/health` | Service health + per-source stats. |
| `GET` | `/` | Web playground UI. |
| `GET` | `/docs` | OpenAPI / Swagger UI. |

## Response shape (truncated)

```jsonc
{
  "success": true,
  "data": {
    "id": "21355",
    "title": "Re:ZERO -Starting Life in Another World-",
    "format": "TV",
    "year": 2016,
    "totalEpisodes": 25,            // ← actually returned (sliced + renumbered)
    "anilistEpisodeCount": 25,      // ← AniList's stated count (for cross-ref)
    "currentEpisode": 25,
    "pattern": "pattern_a",          // ← which multi-season pattern was detected
    "seasons": [                     // ← TMDB seasons summary for the picker
      {
        "season_number": 0,
        "name": "Specials",
        "air_date": "2016-04-05",
        "episode_count": 78,
        "is_specials": true,
        "anilist_ids": [21355, 100049, 108632, 119661, 163134, 189046]
      },
      {
        "season_number": 1,
        "name": "Season 1",
        "air_date": "2016-04-04",
        "episode_count": 85,
        "is_specials": false,
        "anilist_ids": [21355, 100049, 108632, 119661, 163134, 189046],
        "is_current": true          // ← this AniList entry maps to this season
      }
    ],
    "siblingAnilistIds": [21355, 100049, 108632, 119661, 163134, 189046],
    "episodes": [
      {
        "id": "21355-1",
        "number": 1,                 // ← renumbered per AniList entry (1..25, not 1..85)
        "title": "The End of the Beginning and the Beginning of the End",
        "image": "https://image.tmdb.org/...",
        "airDate": "2016-04-04",
        "hasAired": true,
        "source": "tmdb"
      }
      /* ... 24 more ... */
    ],
    "mappings": {
      "anilist_id": 21355,
      "mal_id": 31240,
      "anidb_id": 11370,
      "thetvdb_id": null,
      "themoviedb_id": {"tv": 65942},
      "themoviedb_id_resolved": {"type": "tv", "id": 65942, "method": "fribb"}
    },
    "sources": {
      "anilist": true,
      "fribb": true,
      "tmdb": true,
      "tmdb_type": "tv",
      "resolver_method": "fribb",
      "resolver_tried": ["fribb"]
    },
    "verification": {
      "field": "episodes",
      "anilist_count": 25,
      "returned_count": 25,
      "match": true,
      "note": "ok"
    },
    "view": {
      "extras_mode": false,
      "include_upcoming": false,
      "explicit_season": 1
    }
  },
  "meta": {
    "source": "multi",
    "resolver": "fribb",
    "pattern": "pattern_a",
    "cacheTTL": 604800,
    "cacheStats": {"entries": 0, "default_ttl_seconds": 604800}
  }
}
```

## Architecture

```
   Stage 1 (parallel):
       ├─ AniList GraphQL  (1 call) → title, format, year, episodes, season
       └─ Fribb (in-memory)         → tmdb_id + season.tmdb + episode_offset.tmdb
                                       (reverse index: TMDB→sibling AniList IDs)

   Stage 2 (only if Fribb missed):
       AniBridge → TMDB /find by TVDB/IMDb → TMDB /search by name+year → None

   Stage 3 (parallel, only if TMDB ID found):
       ├─ TMDB TV details   (1 call) → seasons[] for the picker
       ├─ TMDB TV season(s)  (1+ calls in parallel) → episodes
       └─ TMDB TV images    (1 call) → logos, backdrops, posters

   Stage 4 (pure-functional, no network):
       seasons.slice_episodes() — apply offset, count, renumber, filter unaired
```

## How multi-season detection works (`seasons.py`)

The "own brain" is a pure-functional module that decides which TMDB season(s)
to fetch and how to slice them. It uses 3 signals in order:

1. **Fribb's `episode_offset.tmdb`** (Pattern A decisive signal):
   - If Fribb says "AniList 108632 starts at TMDB episode 26", we know AniList
     has split this anime into multiple entries. Slice `[26..26+count]` from
     TMDB S1, renumber 1..count.

2. **Sibling discovery via reverse Fribb index**:
   - If ≥2 AniList IDs map to the same TMDB TV ID, this is a Pattern A series
     even if THIS entry has no offset (the first entry has offset=0, which
     Fribb doesn't bother recording).

3. **TMDB `seasons[]` array**:
   - 1 non-special season → Pattern C (simple).
   - >1 non-special season + AniList `episodes` is null → Pattern B (fetch all,
     continuous numbering, filter unaired).
   - >1 non-special season + AniList `episodes` is a number → try to match by
     year + episode_count, fall back to Pattern B.

## Episode renumbering

- **Pattern A** (Re:Zero): Each AniList entry starts at episode 1. So Re:Zero S2P1 = eps 1..13 (renumbered from TMDB 26..38). The original TMDB number is preserved in `tmdbEpisodeNumber`.
- **Pattern B** (One Piece): Continuous TMDB numbering. EP 1 = "I'm Luffy!", EP 1173 = the last aired.
- **Pattern C** (Cowboy Bebop): 1..N where N = AniList episode count.

## Unaired episode filtering

Default behavior: episodes with `air_date > today` are filtered out.

- One Piece: AniList says `nextAiringEpisode: 1174`. TMDB has episodes up to #2328. We return 1173 aired episodes.
- Tomb Raider King: AniList says 12 total, only 6 have aired. We return 6 (the unaired 7-12 are dropped).
- Pass `?include_upcoming=true` to opt into seeing all announced episodes.

## Run locally

```bash
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8000
# → http://localhost:8000
```

## Run with Docker

```bash
docker build -t anime-metadata-api .
docker run -p 8000:8000 anime-metadata-api
# → http://localhost:8000
```

## Deploy on Render

The included `render.yaml` deploys the pre-built image from `ghcr.io/historiesofhistory-arch/anime-metadata-api:latest`. The GitHub Actions workflow in `.github/workflows/docker-publish.yml` rebuilds the image on every push to `main`.

## Caching

Two in-memory TTL caches (`cache.py`):

| Cache | TTL | Purpose |
|-------|-----|---------|
| `metadata_cache` | 7 days | Final merged response per `(anilist_id, season, include_upcoming, extras)` |
| `tmdb_id_cache` | 30 days | AniList → TMDB ID + type + resolution method (1 day if resolved via TMDB `/search`, which can be wrong) |

Cache key includes the `season` + `include_upcoming` + `extras` flags so different views of the same anime don't collide. Warm-cache responses are <1ms.

## Environment variables

| Var | Default | Description |
|-----|---------|-------------|
| `PORT` | `8000` | HTTP port |
| `PYTHONUNBUFFERED` | `1` | Python stdout buffering |
| `TMDB_API_KEY` | **(required)** | TMDB API key — get one from https://www.themoviedb.org/settings/api |

## Test cases (verified working)

| AniList ID | Title | Pattern | Expected | Got | ✓ |
|---|---|---|---|---|---|
| 21355 | Re:Zero S1 | A | 25 eps | 25 | ✓ |
| 108632 | Re:Zero S2 Part 1 | A | 13 eps (renumbered from 26-38) | 13 | ✓ |
| 119661 | Re:Zero S2 Part 2 | A | 12 eps (renumbered from 39-50) | 12 | ✓ |
| 163134 | Re:Zero S3 | A | 16 eps (renumbered from 51-66) | 16 | ✓ |
| 21 | One Piece | B | 1173 aired (next: 1174) | 1173 | ✓ |
| 1 | Cowboy Bebop | C | 26 eps | 26 | ✓ |
| 5114 | FMA: Brotherhood | C | 64 eps | 64 | ✓ |
| 154587 | Frieren | A (has S2 sibling) | 28 eps | 28 | ✓ |
| 199 | Spirited Away | Movie | 1 ep | 1 | ✓ |
| 184356 | Tomb Raider King | C (ongoing) | 6 aired of 12 | 6 | ✓ |
| 21355/extras | Re:Zero Extras | Extras | 78 specials | 78 | ✓ |
