# Anime Metadata API

Multi-source anime metadata API. Input an **AniList ID** → get back verified episode titles, thumbnails, banners, clearlogos, posters, and cross-database mappings.

**What changed in v2.0.0**: We no longer depend solely on Fribb for AniList↔TMDB resolution. The API now fans out to **7 sources in parallel**, then runs its **own verification logic** (`verifier.py`) to cross-check episode count, season, and airing status, and surfaces a confidence verdict + a list of discrepancies in every response.

## Sources (v2)

| # | Source | Role | Key | Notes |
|---|--------|------|-----|-------|
| 1 | **AniList GraphQL** | Primary metadata (title, format, year, episode count, season, status) | none | The "graph URL" used as the verification anchor |
| 2 | **AniBridge** | Live cross-provider mapping API (AniList ↔ AniDB ↔ MAL ↔ Kitsu ↔ TMDB ↔ TVDB ↔ IMDB) + cross-verified `units` count | none | New in v2. Replaces Fribb as the primary mapping source |
| 3 | **Jikan / MyAnimeList** | Episode count + season + status verifier | none | Rate-limited (3 req/sec) |
| 4 | **Kitsu** | Episode count + season + status verifier | none | Resolves AniList → Kitsu via the `/mappings` endpoint |
| 5 | **AniZip** | TVDB images (banner, poster, fanart, clearlogo) + TVDB episodes | none | |
| 6 | **TMDB** | Episode stills + logos + backdrops + posters | bundled demo key (override via `TMDB_API_KEY`) | Used for both TV and Movie entries |
| 7 | **Fribb / anime-lists** | Secondary AniList↔TMDB static mapping fallback | none | Loaded into memory at startup (~weekly refresh) |

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/episodes/{anilist_id}` | Fetch verified metadata for an anime |
| `GET` | `/api/search?query=...&limit=10` | Search AniList by name |
| `GET` | `/health` | Service health + per-source stats |
| `GET` | `/` | Web playground UI |
| `GET` | `/docs` | OpenAPI / Swagger UI |

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

## Architecture

```
                     ┌─────────────────────────────────────────────────┐
                     │                aggregator.fetch_all              │
                     │                                                 │
   Stage 1 (parallel) ─┬─ AniList GraphQL  ──┐                        │
                       ├─ AniZip              │                        │
                       ├─ AniBridge (live)    │                        │
                       └─ Jikan (MAL)         │                        │
                                              │                        │
   Stage 2: Resolver (multi-source) ◄─────────┘                        │
              AniBridge ─► Fribb ─► TMDB/find ─► TMDB/search ─► None  │
                                                                       │
   Stage 3+4 (parallel):                                              │
       ├─ TMDB (tv/movie details + season episodes + images)          │
       └─ Kitsu (resolves AniList → Kitsu → episode count + season)    │
                                                                       │
   Stage 5: Cross-source verification (verifier.py — "own brain")     │
       ├─ verify_episode_count()  ← majority vote                     │
       ├─ verify_season()         ← AniList primary + majority         │
       └─ verify_status()         ← majority vote                     │
                                                                       │
   Stage 6: Merge + return                                            │
                     └─────────────────────────────────────────────────┘
```

## Verification logic (`verifier.py`)

The verification module is intentionally **pure-functional** — it makes zero HTTP calls and just operates on the data already fetched by `aggregator.py`. This keeps the "own brain" logic unit-testable and side-effect-free.

### Episode count (`verify_episode_count`)

1. Collect `(value, source, weight)` tuples from every source that returned a non-null, non-zero count.
2. **Majority vote** — if ≥ 2 distinct sources agree on the same value, that value wins with `confidence = "high"` and the agreeing sources are listed.
3. If only one source returned data, that value is used with `confidence = "medium"` (or `"medium_low"` if that source was AniList alone, since AniList's `episodes` field is sometimes stale for ongoing shows).
4. If sources disagree, the discrepancies are surfaced in the response (`verification.episodes.discrepancies`).
5. AniList's `episodes` field gets a slight weight preference in tie-breaks (per the user's requirement that AniList GraphQL be the verification anchor), but majority always wins outright.

### Season (`verify_season`)

1. Normalise every source's season value to lowercase `winter | spring | summer | fall`.
2. Sources: AniList (primary), AniBridge (via `release.start_date`), Jikan, Kitsu.
3. Same majority-vote logic.
4. If no source has a season but a start-date exists, the season is inferred from the start-date's month.
5. Year is verified with the same logic.

### Status (`verify_status`)

Same majority-vote logic, normalising every source's status to `FINISHED | RELEASING | NOT_YET_RELEASED | CANCELLED`.

## Response shape (truncated)

```jsonc
{
  "success": true,
  "data": {
    "id": "154587",
    "title": "Frieren: Beyond Journey's End",
    "titleRomaji": "Sousou no Frieren",
    "titleJa": "葬送のフリーレン",
    "format": "TV",
    "year": 2023,
    "totalEpisodes": 28,                       // ← verdict.value
    "episodeVerification": {
      "value": 28,
      "confidence": "high",
      "sources": ["anilist", "anibridge", "jikan", "kitsu", "tmdb"],
      "all_sources": {"anilist": 28, "anibridge": 28, "jikan": 28, "kitsu": 28, "anizip": 28, "tmdb": 28},
      "discrepancies": [],
      "method": "majority"
    },
    "season": "fall",
    "seasonYear": 2023,
    "seasonVerification": { /* ... */ },
    "status": "FINISHED",
    "statusVerification": { /* ... */ },
    "images": [
      {"coverType": "Banner", "url": "...", "source": "tmdb"},
      {"coverType": "Poster", "url": "...", "source": "tmdb"},
      {"coverType": "Clearlogo", "url": "...", "source": "tmdb"}
    ],
    "episodes": [
      {
        "id": "154587-1",
        "number": 1,
        "title": "The Journey's End",
        "image": "https://image.tmdb.org/...",
        "airDate": "2023-09-29",
        "duration": 47,
        "hasAired": true,
        "source": "tmdb"
      }
      /* ... */
    ],
    "mappings": {
      "anilist_id": 154587,
      "mal_id": 52991,
      "anidb_id": 18124,
      "kitsu_id": 44891,
      "thetvdb_id": 425268,
      "themoviedb_id": {"tv": 114461},
      "themoviedb_id_resolved": {"type": "tv", "id": 114461, "method": "anibridge"}
    },
    "sources": {
      "anilist": true,
      "anibridge": true,
      "jikan": true,
      "kitsu": true,
      "anizip": true,
      "fribb": true,
      "tmdb": true,
      "tmdb_type": "tv",
      "resolver_method": "anibridge",
      "resolver_tried": ["anibridge", "fribb", "tmdb_find_tvdb", "tmdb_search_tv"]
    },
    "verification": {
      "episodes": { /* same as episodeVerification */ },
      "season":   { /* same as seasonVerification */ },
      "status":   { /* same as statusVerification */ },
      "summary": {
        "episode_count_agreed": true,
        "season_agreed": true,
        "status_agreed": true,
        "sources_with_episode_data": ["anilist","anibridge","jikan","kitsu","anizip","tmdb"],
        "sources_with_season_data":  ["anilist","anibridge","jikan","kitsu"]
      }
    }
  },
  "meta": {
    "source": "multi",
    "resolver": "anibridge",
    "cacheTTL": 604800,
    "cacheStats": {"entries": 0, "default_ttl_seconds": 604800}
  }
}
```

## Why we no longer depend on Fribb alone

In v1, the resolver was:

```
Fribb (in-memory, ~39% coverage)  →  TMDB /find by TVDB/IMDb  →  TMDB /search by name  →  None
```

If Fribb was unavailable (failed to download, indexing error, behind on weekly refresh, missing the show), the resolver fell straight to the slow path of TMDB search-by-name, which is unreliable for shows with common titles or romanised vs English name differences.

In v2:

```
AniBridge (live, cross-DB)  →  Fribb (in-memory)  →  TMDB /find by TVDB/IMDb (AniBridge + Fribb)  →  TMDB /search  →  None
```

Each tier fills gaps the previous one couldn't:
- AniBridge is live, community-maintained, and chases all relationship targets to build a complete cross-provider ID table.
- Fribb is still useful as an in-memory fallback because it has wide coverage and zero network latency.
- TMDB `/find` still catches the case where neither AniBridge nor Fribb returned a TMDB ID but they returned a TVDB/IMDb ID.
- TMDB `/search` is the final fallback for brand-new anime that haven't propagated to any mapping DB yet.

## Caching

Two in-memory TTL caches (`cache.py`):

| Cache | TTL | Purpose |
|-------|-----|---------|
| `metadata_cache` | 7 days | Final merged response per AniList ID |
| `tmdb_id_cache` | 30 days | AniList → TMDB ID + type + resolution method (1 day if resolved via TMDB `/search`, which can be wrong) |

Caches are in-memory only — no database, no disk persistence. They rebuild naturally on restart.

## Environment variables

| Var | Default | Description |
|-----|---------|-------------|
| `PORT` | `8000` | HTTP port |
| `PYTHONUNBUFFERED` | `1` | Python stdout buffering |
| `TMDB_API_KEY` | bundled demo key | Override with your own TMDB API key (recommended for production) |

## License

See the repository for license details. Source data licenses:

- AniList — free public API, no key required.
- AniBridge — community-maintained, see https://github.com/anibridge/anibridge-mappings
- Fribb/anime-lists — community dataset.
- Jikan — open-source MAL wrapper.
- Kitsu — public JSON:API.
- TMDB — requires attribution per their TOS.
