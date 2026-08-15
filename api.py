"""
api.py — Anime Metadata API

Endpoint: GET /api/episodes/{anilist_id}
Returns: episode titles, thumbnails, banners, clearlogos, posters, mappings
         for any anime by AniList ID.

Sources (multi-source, fetched in parallel):
  1. Fribb/anime-lists — AniList↔TMDB static mapping (in-memory, O(1))
  2. AniList GraphQL — title, year, format, episode count, cover image
  3. AniZip (api.ani.zip) — TVDB images + episode data
  4. TMDB — episodes with stills, logos, backdrops, posters (TV + Movies)

100% keyless (uses TMDB's official documentation key).
7-day in-memory cache. No database. Handles TV, movies, OVAs, new anime.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from dotenv import load_dotenv

from aggregator import fetch_all
from cache import metadata_cache, tmdb_id_cache
from sources import fribb, anilist as anilist_source

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Download + load Fribb mapping data at startup."""
    log.info("Loading Fribb AniList↔TMDB mapping data...")
    # Run in thread pool to not block event loop
    loop = asyncio.get_event_loop()
    ok = await loop.run_in_executor(None, fribb.ensure_loaded)
    if ok:
        log.info("Fribb loaded: %s", fribb.stats())
    else:
        log.warning("Fribb failed to load — will use TMDB search fallback only")

    # Schedule weekly refresh
    async def refresh_fribb():
        while True:
            await asyncio.sleep(7 * 24 * 3600)  # 7 days
            log.info("Refreshing Fribb data...")
            await loop.run_in_executor(None, fribb.ensure_loaded, True)

    asyncio.create_task(refresh_fribb())
    yield


app = FastAPI(
    title="Anime Metadata API",
    version="1.0.0",
    description="Bulletproof anime metadata API — AniList ID in, episode titles/thumbnails/banners/clearlogos out.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── API endpoints ──────────────────────────────────────────────────

@app.get("/api/episodes/{anilist_id}")
async def get_episodes(anilist_id: int):
    """
    Returns episode names, thumbnails, banners, clearlogos, posters, and
    cross-references for any anime by AniList ID.

    Sources data from Fribb (AniList↔TMDB mapping), AniList GraphQL,
    AniZip (TVDB images), and TMDB (episode stills, logos, backdrops).
    All fetched in parallel. Cached for 7 days.
    """
    if anilist_id <= 0:
        raise HTTPException(status_code=400, detail="anilist_id must be a positive integer")

    try:
        data = await fetch_all(anilist_id)
        return {
            "success": True,
            "data": data,
            "meta": {
                "source": "multi",
                "resolver": data.get("sources", {}).get("resolver_method"),
                "cacheTTL": 604800,
                "cacheStats": metadata_cache.stats(),
            },
        }
    except Exception as e:
        log.exception("Failed to fetch %d", anilist_id)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search")
async def search_anime(query: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    """Search AniList for anime by name. Returns AniList IDs for use with /api/episodes/{id}."""
    gql = """
    query ($search: String, $perPage: Int) {
        Page(page: 1, perPage: $perPage) {
            media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
                id
                title { romaji english }
                coverImage { large }
                format
                seasonYear
                episodes
            }
        }
    }
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                "https://graphql.anilist.co",
                json={"query": gql, "variables": {"search": query, "perPage": limit}},
                headers={"Content-Type": "application/json", "User-Agent": "anime-metadata-api/1.0"},
            )
            if res.status_code != 200:
                raise HTTPException(status_code=502, detail="AniList search failed")
            media = res.json().get("data", {}).get("Page", {}).get("media", [])
            return {
                "success": True,
                "results": [
                    {
                        "anilistId": m["id"],
                        "title": m["title"].get("english") or m["title"].get("romaji"),
                        "titleRomaji": m["title"].get("romaji"),
                        "cover": m.get("coverImage", {}).get("large"),
                        "format": m.get("format"),
                        "year": m.get("seasonYear"),
                        "episodes": m.get("episodes"),
                    }
                    for m in media
                ],
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    """Health check + stats."""
    return {
        "status": "ok",
        "fribb": fribb.stats(),
        "metadata_cache": metadata_cache.stats(),
        "tmdb_id_cache": tmdb_id_cache.stats(),
    }


@app.get("/")
async def home():
    """Responsive homepage with test UI."""
    return HTMLResponse(_HOMEPAGE_HTML)


# ─── Homepage HTML ──────────────────────────────────────────────────

_HOMEPAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Anime Metadata API</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e1a;--surface:rgba(255,255,255,0.04);--border:rgba(255,255,255,0.08);--blue:#3b82f6;--green:#10b981;--amber:#f59e0b;--red:#ef4444;--text:#e2e8f0;--muted:#64748b;--mono:'JetBrains Mono',monospace}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;line-height:1.6}
.container{max-width:1100px;margin:0 auto;padding:24px 16px}
header{text-align:center;padding:40px 0 30px}
header h1{font-size:clamp(1.8rem,4vw,2.6rem);font-weight:700;background:linear-gradient(135deg,#3b82f6,#10b981);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px}
header p{color:var(--muted);font-size:1rem;max-width:600px;margin:0 auto}
.badges{display:flex;justify-content:center;gap:8px;margin-top:16px;flex-wrap:wrap}
.badge{background:var(--surface);border:1px solid var(--border);padding:4px 12px;border-radius:999px;font-size:.78rem;color:var(--muted)}
.badge.live{color:var(--green);border-color:rgba(16,185,129,.3)}
.badge.live::before{content:'';display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin-right:6px;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.search-box{display:flex;gap:8px;margin:24px 0;flex-wrap:wrap}
input[type=text],input[type=number]{flex:1;min-width:200px;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:12px 16px;border-radius:10px;font-size:1rem;outline:none}
input:focus{border-color:var(--blue)}
button{background:var(--blue);color:white;border:none;padding:12px 24px;border-radius:10px;font-size:.95rem;font-weight:500;cursor:pointer;transition:all .15s}
button:hover{background:#2563eb;transform:translateY(-1px)}
button:disabled{opacity:.5;cursor:not-allowed}
button.secondary{background:var(--surface);border:1px solid var(--border);color:var(--text)}
button.secondary:hover{background:rgba(255,255,255,.08)}
.test-buttons{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:20px}
.test-buttons button{padding:6px 12px;font-size:.82rem;background:var(--surface);border:1px solid var(--border);color:var(--text)}
.test-buttons button:hover{border-color:var(--blue);color:var(--blue)}
.status{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:20px;font-size:.85rem}
.status.loading{border-color:var(--amber);color:var(--amber)}
.status.error{border-color:var(--red);color:#fca5a5}
.status.success{border-color:var(--green);color:#6ee7b7}
#result{display:none}
.result-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;margin-bottom:16px}
.result-header{padding:20px;position:relative}
.result-header img.banner{width:100%;height:180px;object-fit:cover;border-radius:10px;margin-bottom:14px}
.result-header img.logo{max-height:60px;max-width:300px;margin-bottom:10px}
.result-header h2{font-size:1.4rem;margin-bottom:4px}
.result-header .meta{color:var(--muted);font-size:.85rem;display:flex;gap:12px;flex-wrap:wrap}
.result-header .meta span{display:inline-flex;align-items:center;gap:4px}
.result-body{padding:0 20px 20px}
.images-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-top:16px}
.image-item{background:rgba(0,0,0,.3);border-radius:8px;overflow:hidden}
.image-item img{width:100%;height:120px;object-fit:cover;display:block}
.image-item .label{padding:6px 8px;font-size:.72rem;color:var(--muted);font-family:var(--mono)}
.episodes-list{margin-top:20px}
.episodes-list h3{font-size:.9rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
.episode{display:flex;gap:12px;padding:10px;background:rgba(0,0,0,.2);border-radius:8px;margin-bottom:8px;align-items:start}
.episode img{width:120px;height:68px;object-fit:cover;border-radius:6px;flex-shrink:0;background:#1a1a2e}
.episode-info{flex:1;min-width:0}
.episode-info .ep-num{font-family:var(--mono);font-size:.78rem;color:var(--blue);font-weight:600}
.episode-info .ep-title{font-size:.92rem;margin:2px 0}
.episode-info .ep-desc{font-size:.78rem;color:var(--muted);display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.episode-info .ep-meta{font-size:.72rem;color:var(--muted);margin-top:4px}
.mappings{background:rgba(0,0,0,.3);border-radius:8px;padding:14px;margin-top:16px;font-family:var(--mono);font-size:.78rem;color:var(--muted);overflow-x:auto}
.mappings b{color:var(--text)}
.json-view{background:rgba(0,0,0,.4);border:1px solid var(--border);border-radius:8px;padding:14px;font-family:var(--mono);font-size:.74rem;color:#94a3b8;overflow-x:auto;max-height:400px;overflow-y:auto;margin-top:12px}
footer{text-align:center;padding:30px 0;color:var(--muted);font-size:.8rem}
footer a{color:var(--blue);text-decoration:none}
@media(max-width:600px){.episode{flex-direction:column}.episode img{width:100%;height:140px}.images-grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Anime Metadata API</h1>
    <p>Enter an AniList ID to get episode titles, thumbnails, banners, clearlogos, and cross-references. Multi-source: Fribb + AniList + AniZip + TMDB.</p>
    <div class="badges">
      <span class="badge live">Live</span>
      <span class="badge">100% keyless</span>
      <span class="badge">TV + Movies</span>
      <span class="badge">7-day cache</span>
    </div>
  </header>

  <div class="search-box">
    <input type="number" id="anilistId" placeholder="Enter AniList ID (e.g. 154587)" onkeydown="if(event.key==='Enter')fetchData()">
    <button onclick="fetchData()">Fetch</button>
    <button class="secondary" onclick="searchAnime()">Search by name</button>
  </div>

  <div class="test-buttons">
    <span style="color:var(--muted);font-size:.8rem;align-self:center;margin-right:4px">Quick test:</span>
    <button onclick="quickTest(154587)">Frieren (TV)</button>
    <button onclick="quickTest(21)">One Piece (TV)</button>
    <button onclick="quickTest(184356)">Tomb Raider King (new)</button>
    <button onclick="quickTest(199)">Spirited Away (Movie)</button>
    <button onclick="quickTest(164)">Princess Mononoke (Movie)</button>
    <button onclick="quickTest(101922)">Demon Slayer</button>
  </div>

  <div id="status" class="status" style="display:none"></div>
  <div id="result"></div>

  <footer>
    <p>Endpoint: <code>GET /api/episodes/{anilist_id}</code> · <a href="/health">Health</a> · <a href="/docs">API Docs</a></p>
  </footer>
</div>

<script>
const API = '';

async function fetchData() {
  const id = document.getElementById('anilistId').value.trim();
  if (!id) { showStatus('error', 'Please enter an AniList ID'); return; }
  showStatus('loading', `Fetching metadata for AniList ID ${id}...`);
  document.getElementById('result').innerHTML = '';
  try {
    const r = await fetch(`${API}/api/episodes/${id}`);
    const d = await r.json();
    if (!r.ok || !d.success) {
      showStatus('error', d.detail || `HTTP ${r.status}`);
      return;
    }
    showStatus('success', `✅ Loaded in ${d.meta.cacheStats ? '(cached)' : ''} — resolver: ${d.meta.resolver || 'n/a'}`);
    renderResult(d.data);
  } catch(e) {
    showStatus('error', 'Network error: ' + e.message);
  }
}

function quickTest(id) {
  document.getElementById('anilistId').value = id;
  fetchData();
}

async function searchAnime() {
  const q = prompt('Enter anime name to search:');
  if (!q) return;
  showStatus('loading', `Searching for "${q}"...`);
  try {
    const r = await fetch(`${API}/api/search?query=${encodeURIComponent(q)}&limit=10`);
    const d = await r.json();
    if (!d.success || !d.results.length) {
      showStatus('error', 'No results found');
      return;
    }
    showStatus('success', `Found ${d.results.length} results — click an ID to fetch`);
    let html = '<div class="result-card"><div class="result-body"><h3>Search results</h3><div style="margin-top:12px">';
    d.results.forEach(r => {
      html += `<div style="display:flex;gap:12px;padding:8px;background:rgba(0,0,0,.2);border-radius:6px;margin-bottom:6px;cursor:pointer" onclick="document.getElementById('anilistId').value=${r.anilistId};fetchData()">`;
      if (r.cover) html += `<img src="${r.cover}" style="width:40px;height:56px;object-fit:cover;border-radius:4px">`;
      html += `<div><div style="font-weight:500">${r.title}</div><div style="font-size:.78rem;color:var(--muted)">ID: ${r.anilistId} · ${r.format} · ${r.year||'?'} · ${r.episodes||'?'} eps</div></div></div>`;
    });
    html += '</div></div></div>';
    document.getElementById('result').innerHTML = html;
  } catch(e) {
    showStatus('error', e.message);
  }
}

function renderResult(data) {
  const logo = (data.images.find(i=>i.coverType==='Clearlogo')||{}).url;
  const banner = (data.images.find(i=>i.coverType==='Banner')||{}).url;
  const poster = (data.images.find(i=>i.coverType==='Poster')||{}).url;
  let html = '<div class="result-card">';
  html += '<div class="result-header">';
  if (banner) html += `<img class="banner" src="${banner}" onerror="this.style.display='none'">`;
  if (logo) html += `<img class="logo" src="${logo}" onerror="this.style.display='none'">`;
  html += `<h2>${data.title || data.titleRomaji || 'Unknown'}</h2>`;
  if (data.titleJa && data.titleJa !== data.title) html += `<div style="color:var(--muted);font-size:.9rem">${data.titleJa}</div>`;
  html += '<div class="meta">';
  html += `<span>AniList: ${data.id}</span>`;
  if (data.malId) html += `<span>MAL: ${data.malId}</span>`;
  if (data.tmdbId) html += `<span>TMDB: ${data.tmdbId} (${data.tmdbType})</span>`;
  if (data.format) html += `<span>${data.format}</span>`;
  if (data.year) html += `<span>${data.year}</span>`;
  html += `<span>${data.totalEpisodes} eps</span>`;
  if (data.currentEpisode) html += `<span style="color:var(--green)">EP ${data.currentEpisode} aired</span>`;
  if (data.nextAiringEpisode) html += `<span style="color:var(--amber)">Next: EP ${data.nextAiringEpisode}</span>`;
  html += '</div>';
  // Sources
  html += '<div class="meta" style="margin-top:8px;font-size:.72rem">';
  const s = data.sources || {};
  html += `<span>Resolver: ${s.resolver_method || '?'}</span>`;
  if (s.anilist) html += `<span style="color:var(--green)">AniList ✓</span>`;
  if (s.fribb) html += `<span style="color:var(--green)">Fribb ✓</span>`;
  if (s.anizip) html += `<span style="color:var(--green)">AniZip ✓</span>`;
  if (s.tmdb) html += `<span style="color:var(--green)">TMDB ✓</span>`;
  html += '</div>';
  html += '</div>';

  html += '<div class="result-body">';

  // Description
  if (data.description) {
    const desc = data.description.replace(/<br>/g,' ').replace(/<[^>]+>/g,'').slice(0,300);
    html += `<p style="color:var(--muted);font-size:.85rem;margin-bottom:12px">${desc}...</p>`;
  }

  // Images
  if (data.images.length > 1) {
    html += '<div class="images-grid">';
    data.images.forEach(img => {
      html += `<div class="image-item"><img src="${img.url}" onerror="this.parentNode.style.display='none'"><div class="label">${img.coverType} (${img.source})</div></div>`;
    });
    html += '</div>';
  }

  // Episodes
  if (data.episodes && data.episodes.length) {
    html += '<div class="episodes-list"><h3>Episodes (' + data.episodes.length + ')</h3>';
    data.episodes.slice(0, 20).forEach(ep => {
      html += '<div class="episode">';
      if (ep.image) html += `<img src="${ep.image}" onerror="this.style.display='none'">`;
      else html += '<div style="width:120px;height:68px;background:#1a1a2e;border-radius:6px;flex-shrink:0"></div>';
      html += '<div class="episode-info">';
      html += `<div class="ep-num">EP ${ep.number}${ep.hasAired ? '' : ' (upcoming)'}</div>`;
      html += `<div class="ep-title">${ep.title || '—'}</div>`;
      if (ep.description) html += `<div class="ep-desc">${ep.description.slice(0,120)}</div>`;
      html += `<div class="ep-meta">${ep.airDate||''} ${ep.duration? '· '+ep.duration+'min':''} ${ep.source?'· '+ep.source:''}</div>`;
      html += '</div></div>';
    });
    if (data.episodes.length > 20) {
      html += `<div style="text-align:center;padding:8px;color:var(--muted);font-size:.82rem">+ ${data.episodes.length - 20} more episodes</div>`;
    }
    html += '</div>';
  }

  // Mappings
  if (data.mappings && Object.keys(data.mappings).length) {
    html += '<div class="mappings"><b>Mappings:</b> ' + JSON.stringify(data.mappings, null, 2) + '</div>';
  }

  // Raw JSON
  html += `<details><summary style="cursor:pointer;color:var(--muted);font-size:.82rem;margin-top:12px">View raw JSON</summary><pre class="json-view">${JSON.stringify(data, null, 2).replace(/</g,'&lt;')}</pre></details>`;

  html += '</div></div>';
  document.getElementById('result').innerHTML = html;
}

function showStatus(type, msg) {
  const el = document.getElementById('status');
  el.style.display = 'block';
  el.className = 'status ' + type;
  el.textContent = msg;
}
</script>
</body>
</html>"""
