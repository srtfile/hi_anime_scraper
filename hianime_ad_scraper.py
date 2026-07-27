"""
hianime_scraper.py
──────────────────
GitHub Actions-compatible scraper for hianime.ad / vibeplayer.site.
Reads input_urls_list.txt, scrapes episode stream data, saves results
locally in the repo (committed back to GitHub by the workflow).

MAL ID is fetched automatically from the Jikan API (api.jikan.moe/v4)
and embedded in every record and episode key.

Input file  : input_urls_list.txt
Processed   : already_processed_urls_list.txt
Output      : hianime_streams_list.json  (auto-splits at 3 MB →
              hianime_streams_list_2.json, _3.json …)

Usage (workflow calls this):
  python hianime_scraper.py --batch 50
  python hianime_scraper.py --batch all
  python hianime_scraper.py --no-streams --batch 100
  python hianime_scraper.py --url "https://hianime.ad/watch/naruto/ep-1 to 10"
"""

import sys
import re
import os
import json
import time
import math
import argparse
from pathlib import Path
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    sys.exit("[!] 'requests' not installed.  Run:  pip install requests")


# ─── Constants ────────────────────────────────────────────────────────────────

BASE_DIR          = Path(__file__).parent
INPUT_FILE        = BASE_DIR / "input_urls_list.txt"
PROCESSED_FILE    = BASE_DIR / "already_processed_urls_list.txt"
OUTPUT_STEM       = "hianime_streams_list"
MAX_OUTPUT_BYTES  = 3 * 1024 * 1024          # 3 MB hard cap per file
REQUEST_TIMEOUT   = 20
INTER_EP_DELAY    = float(os.environ.get("SCRAPE_DELAY", "1.0"))

# Jikan (MAL) API base — free, no key required
JIKAN_BASE        = "https://api.jikan.moe/v4"
# Cache: slug → {"mal_id": int|None, "jikan_episodes": {ep_num: title}}
# Avoids hitting Jikan more than once per series in a single run.
_MAL_CACHE: dict  = {}


# ─── HTTP session ─────────────────────────────────────────────────────────────

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

session = requests.Session()
session.headers.update({
    "User-Agent":      _UA,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})
M3U8_HEADERS = {"User-Agent": _UA, "Referer": "https://vibeplayer.site/"}


# ═══════════════════════════════════════════════════════════════════════════════
# JIKAN / MAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _jikan_get(path: str, params: dict | None = None, retries: int = 3) -> dict | None:
    """
    GET https://api.jikan.moe/v4/<path> with retry + rate-limit handling.
    Jikan allows ~3 req/s; we sleep briefly after every call.
    """
    url = f"{JIKAN_BASE}/{path}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                time.sleep(0.4)          # stay well under the 3 req/s cap
                return r.json()
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 2 * attempt))
                print(f"      [Jikan] 429 — waiting {wait}s …", flush=True)
                time.sleep(wait)
                continue
            if r.status_code in (500, 503):
                time.sleep(2 * attempt)
                continue
            print(f"      [Jikan] HTTP {r.status_code} for {url}", flush=True)
            return None
        except Exception as e:
            print(f"      [Jikan] request error (attempt {attempt}): {e}", flush=True)
            time.sleep(2)
    return None


def jikan_search_mal_id(anime_name: str) -> int | None:
    """
    Search Jikan for `anime_name` and return the MAL ID of the best match.
    Returns None if nothing found or the request fails.
    """
    data = _jikan_get("anime", params={"q": anime_name, "limit": 1})
    if not data:
        return None
    results = data.get("data", [])
    if not results:
        return None
    return results[0].get("mal_id")


def jikan_get_episode_titles(mal_id: int) -> dict[int, str]:
    """
    Fetch all episode titles from Jikan for the given MAL ID.
    Returns {episode_number: title_str}.  May be empty if MAL has no episode list.
    Handles multi-page responses automatically.
    """
    titles: dict[int, str] = {}
    page = 1
    while True:
        data = _jikan_get(f"anime/{mal_id}/episodes", params={"page": page})
        if not data:
            break
        for ep in data.get("data", []):
            ep_num = ep.get("mal_id")      # Jikan uses mal_id as episode number here
            title  = ep.get("title") or ep.get("title_romanji") or ""
            if ep_num and title:
                titles[int(ep_num)] = title
        pagination = data.get("pagination", {})
        if not pagination.get("has_next_page"):
            break
        page += 1
        time.sleep(0.4)
    return titles


def get_mal_data_for_slug(slug: str, anime_name: str) -> dict:
    """
    Return a dict with 'mal_id' (int|None) and 'jikan_episodes' ({ep_num: title})
    for the given slug.  Results are cached so each series is only fetched once
    per process.

    Cache key is the slug (stable) rather than the anime_name (can vary by URL).
    """
    if slug in _MAL_CACHE:
        return _MAL_CACHE[slug]

    print(f"  [MAL] Searching Jikan for: {anime_name!r}", flush=True)
    mal_id = jikan_search_mal_id(anime_name)

    if mal_id:
        print(f"  [MAL] Found MAL ID: {mal_id}", flush=True)
        time.sleep(0.5)
        print(f"  [MAL] Fetching episode titles (MAL ID {mal_id}) …", flush=True)
        jikan_eps = jikan_get_episode_titles(mal_id)
        print(f"  [MAL] Got {len(jikan_eps)} episode title(s) from Jikan", flush=True)
    else:
        print(f"  [MAL] MAL ID not found for {anime_name!r}", flush=True)
        jikan_eps = {}

    result = {"mal_id": mal_id, "jikan_episodes": jikan_eps}
    _MAL_CACHE[slug] = result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# INPUT PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def expand_line(line: str) -> list[str]:
    """
    Expand one input line into a list of fully-formed episode URLs.

    Supported formats
    ─────────────────
    https://hianime.ad/watch/one-piece/ep-1 to 220   → 220 URLs
    https://hianime.ad/watch/naruto-shippuden/ep-50  → 1 URL
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return []

    # Range: ep-N to M
    m = re.match(
        r'(https://hianime\.ad/watch/[^/]+/ep-)(\d+)\s+to\s+(\d+)',
        line, re.IGNORECASE
    )
    if m:
        prefix = m.group(1)
        start  = int(m.group(2))
        end    = int(m.group(3))
        if start > end:
            print(f"  [!] Bad range (start > end), skipping: {line}", flush=True)
            return []
        return [f"{prefix}{ep}" for ep in range(start, end + 1)]

    # Single episode
    m = re.match(r'(https://hianime\.ad/watch/[^/]+/ep-\d+)', line, re.IGNORECASE)
    if m:
        return [m.group(1)]

    print(f"  [?] Unrecognised line, skipping: {line}", flush=True)
    return []


def parse_input_file(path: Path) -> list[str]:
    """Return deduplicated, ordered list of episode URLs from the input file."""
    if not path.exists():
        sys.exit(
            f"[!] Input file not found: {path}\n"
            f"    Create it with one URL or range per line."
        )
    urls: list[str] = []
    seen: set[str]  = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        for u in expand_line(raw):
            if u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


def parse_inline_urls(text: str) -> list[str]:
    """Expand comma- or newline-separated URL strings (from --url CLI arg)."""
    urls: list[str] = []
    seen: set[str]  = set()
    for part in re.split(r'[,\n]', text):
        for u in expand_line(part.strip()):
            if u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


# ═══════════════════════════════════════════════════════════════════════════════
# PROCESSED URL TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

def load_processed(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        ln.strip()
        for ln in path.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.startswith("#")
    }


def mark_processed(path: Path, url: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(url + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT MANAGER  (auto-split at MAX_OUTPUT_BYTES)
# ═══════════════════════════════════════════════════════════════════════════════

class OutputManager:
    """
    Appends episode records to hianime_streams_list.json.
    Automatically starts hianime_streams_list_2.json (then _3, _4 …)
    when the current file would exceed 3 MB after adding a new record.

    Records already in existing files are loaded on startup so we can
    safely resume a partial run without losing anything.
    """

    def __init__(self, base_dir: Path, stem: str, max_bytes: int):
        self.base_dir  = base_dir
        self.stem      = stem
        self.max_bytes = max_bytes
        self._chunk    = 1
        self._records: list[dict] = []
        self._resume()

    def _chunk_path(self, n: int) -> Path:
        name = self.stem if n == 1 else f"{self.stem}_{n}"
        return self.base_dir / f"{name}.json"

    def _current_path(self) -> Path:
        return self._chunk_path(self._chunk)

    def _encode(self, records: list) -> bytes:
        return json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8")

    def _flush(self) -> None:
        self._current_path().write_bytes(self._encode(self._records))

    def _would_overflow(self, new_rec: dict) -> bool:
        return len(self._encode(self._records + [new_rec])) > self.max_bytes

    def _resume(self) -> None:
        """Find the last existing chunk and load it for appending."""
        n = 1
        while self._chunk_path(n).exists():
            n += 1
        last = n - 1
        if last < 1:
            self._chunk   = 1
            self._records = []
            return
        self._chunk = last
        try:
            data = json.loads(self._chunk_path(last).read_text(encoding="utf-8"))
            self._records = data if isinstance(data, list) else []
            sz = self._chunk_path(last).stat().st_size
            print(
                f"[*] Resuming → {self._chunk_path(last).name} "
                f"({len(self._records)} records, {sz // 1024} KB)",
                flush=True
            )
        except Exception:
            self._records = []

    def add(self, record: dict) -> None:
        """Append a record; roll over to next chunk file if needed."""
        if self._records and self._would_overflow(record):
            self._flush()
            old = self._current_path()
            self._chunk  += 1
            self._records = []
            print(
                f"\n[+] Chunk full → {old.name} sealed "
                f"({old.stat().st_size // 1024} KB)",
                flush=True
            )
            print(f"[+] New chunk: {self._current_path().name}\n", flush=True)

        self._records.append(record)
        self._flush()          # write after every record = crash-safe

    def summary(self) -> str:
        lines = []
        n = 1
        while self._chunk_path(n).exists():
            p = self._chunk_path(n)
            try:
                cnt = len(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                cnt = "?"
            lines.append(f"  {p.name}  ({p.stat().st_size // 1024} KB, {cnt} records)")
            n += 1
        return "\n".join(lines) if lines else "  (no output files yet)"

    def all_output_paths(self) -> list[Path]:
        paths, n = [], 1
        while self._chunk_path(n).exists():
            paths.append(self._chunk_path(n))
            n += 1
        return paths


# ═══════════════════════════════════════════════════════════════════════════════
# URL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_slug(url: str) -> str | None:
    m = re.search(r'/watch/([^/]+)/ep-\d+', url)
    return m.group(1) if m else None


def get_ep_num(url: str) -> int | None:
    m = re.search(r'/ep-(\d+)', url)
    return int(m.group(1)) if m else None


def slug_to_anime_name(slug: str) -> str:
    """Convert a URL slug to a readable anime name for Jikan searching."""
    return slug.replace("-", " ").title()


# ═══════════════════════════════════════════════════════════════════════════════
# EPISODE PAGE PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def parse_episode_page(ep_url: str) -> tuple[int | None, dict]:
    """
    Fetch the episode page and extract all embed entries grouped by server type.

    Returns
    -------
    (db_id, embeds)

    embeds structure:
    {
      "sub":  { "HD-1": {"embed_url": ..., "subtitle": ..., "is_hd2": bool, "external": bool} },
      "dub":  { … },
      "hsub": { … },
    }
    """
    r = session.get(ep_url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    html = r.text

    db_m  = re.search(r'api-ani\.cimovix\.store/episode/(\d+)', html)
    db_id = int(db_m.group(1)) if db_m else None

    embeds: dict[str, dict] = {}

    # Primary: vibeplayer.site server blocks
    blocks = re.findall(
        r'<div class="ps__-list server-items" data-id="([^"]+)">(.*?)</div>\s*<div class="clearfix',
        html, re.DOTALL,
    )
    for server_type, block_html in blocks:
        embeds.setdefault(server_type, {})

        # vibeplayer.site embeds
        for embed_full, label in re.findall(
            r'data-video="(https://vibeplayer\.site/[^"]+)"[^>]*>([^<]+)<', block_html
        ):
            label     = label.strip()
            embed_url = embed_full.split("?sub=")[0].split("?")[0]
            subtitle  = embed_full.split("?sub=")[1] if "?sub=" in embed_full else None
            is_hd2    = bool(re.search(r'/ag[0-9a-f]{30,}h$', embed_url))
            embeds[server_type][label] = {
                "embed_url": embed_url,
                "subtitle":  subtitle,
                "is_hd2":    is_hd2,
                "external":  False,
            }

        # External players
        for pattern, _name in [
            (r'otakuhg\.site/e/([A-Za-z0-9]+)',        "StreamHG"),
            (r'otakuvid\.online/embed/([A-Za-z0-9]+)', "Earnvids"),
            (r'playmogo\.com/e/([A-Za-z0-9]+)',         "Doodstream"),
        ]:
            domain = pattern.split(r'\.')[0]
            for m in re.finditer(
                r'data-video="(https://' + domain.replace(r'\.', r'\\.') + r'[^"]+)"[^>]*>([^<]+)<',
                block_html,
            ):
                lbl = m.group(2).strip()
                embeds[server_type].setdefault(lbl, {
                    "embed_url": m.group(1).split("?")[0],
                    "subtitle":  None,
                    "is_hd2":    False,
                    "external":  True,
                })

    return db_id, embeds


# ═══════════════════════════════════════════════════════════════════════════════
# VIBEPLAYER.SITE  m3u8 RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_vibeplayer(embed_url: str) -> dict | None:
    try:
        r = session.get(embed_url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"      [!] embed fetch: {e}", flush=True)
        return None

    html = r.text
    m = re.search(r'const src\s*=\s*"(https://vibeplayer\.site/public/stream/[^"]+)"', html)
    if not m:
        m = re.search(r'"(https://vibeplayer\.site/public/stream/[^"]+\.m3u8)"', html)
    if not m:
        return None

    master_url = m.group(1)
    sub_m      = re.search(r'const subtitle\s*=\s*"([^"]+)"', html)
    subtitle   = sub_m.group(1) if sub_m and sub_m.group(1) else None

    try:
        mr   = session.get(master_url, headers=M3U8_HEADERS, timeout=REQUEST_TIMEOUT)
        mr.raise_for_status()
    except Exception:
        return {"master_m3u8": master_url, "variants": [], "subtitle": subtitle}

    base     = master_url.rsplit("/", 1)[0] + "/"
    variants = []
    lines    = mr.text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            res_m  = re.search(r'RESOLUTION=(\d+x\d+)', line)
            bw_m   = re.search(r'BANDWIDTH=(\d+)', line)
            name_m = re.search(r'NAME="([^"]+)"', line)
            if i + 1 < len(lines) and not lines[i + 1].startswith("#"):
                pl   = lines[i + 1].strip()
                full = pl if pl.startswith("http") else base + pl
                vid_m = re.search(r'(\d+)_\d+\.m3u8', pl)
                variants.append({
                    "name":              name_m.group(1)      if name_m else (res_m.group(1) if res_m else "unknown"),
                    "resolution":        res_m.group(1)       if res_m  else "unknown",
                    "bandwidth_kbps":    int(bw_m.group(1)) // 1000 if bw_m else 0,
                    "internal_video_id": vid_m.group(1)       if vid_m  else None,
                    "url":               full,
                })

    variants.sort(key=lambda x: x["bandwidth_kbps"], reverse=True)
    return {"master_m3u8": master_url, "variants": variants, "subtitle": subtitle}


def resolve_embed(embed_info: dict) -> dict:
    """Resolve one embed entry to its stream data. Always returns a dict."""
    return {"embed_url": embed_info["embed_url"]}


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLE EPISODE SCRAPE
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_episode(ep_url: str, resolve_streams: bool = True) -> dict:  # resolve_streams kept for CLI compat
    """
    Scrape one episode URL and return a record dict.

    Record shape
    ────────────
    {
      "episode_url":    "https://hianime.ad/watch/naruto/ep-1",
      "slug":           "naruto",
      "anime_name":     "Naruto",
      "mal_id":         20,
      "episode_number": 1,
      "episode_title":  "Enter: Naruto Uzumaki!",
      "scraped_at":     "2026-07-27T10:00:00+00:00",
      "embeds": {
        "sub":  { "HD-1": {"embed_url": "…"} },
        "dub":  { "HD-1": {"embed_url": "…"} },
        "hsub": { "StreamHG": {"embed_url": "…"}, "Earnvids": {"embed_url": "…"} }
      },
      "error": null
    }
    """
    slug     = get_slug(ep_url)
    ep_num   = get_ep_num(ep_url)
    now      = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ── MAL lookup (cached per slug) ─────────────────────────────────────────
    anime_name = slug_to_anime_name(slug) if slug else "Unknown"
    mal_data   = get_mal_data_for_slug(slug, anime_name) if slug else {"mal_id": None, "jikan_episodes": {}}
    mal_id     = mal_data["mal_id"]
    jikan_eps  = mal_data["jikan_episodes"]

    # Episode title from Jikan, fall back to None
    ep_title = jikan_eps.get(ep_num) if ep_num else None

    record: dict = {
        "episode_url":    ep_url,
        "slug":           slug,
        "anime_name":     anime_name,
        "mal_id":         mal_id,
        "episode_number": ep_num,
        "episode_title":  ep_title,
        "scraped_at":     now,
        "embeds":         {},
        "error":          None,
    }

    try:
        _db_id, embeds_raw = parse_episode_page(ep_url)

        for server_type, servers in embeds_raw.items():
            record["embeds"].setdefault(server_type, {})
            for label, embed_info in servers.items():
                embed_url = embed_info["embed_url"]

                record["embeds"][server_type][label] = {"embed_url": embed_url}

        counts = "  ".join(
            f"{st.upper()}:{len(sv)}" for st, sv in record["embeds"].items()
        )
        mal_tag = f"MAL:{mal_id}" if mal_id else "MAL:?"
        print(f"  [OK] ep-{ep_num}  {counts}  {mal_tag}", flush=True)

    except Exception as exc:
        record["error"] = str(exc)
        print(f"  [!!] ep-{ep_num} FAILED: {exc}", flush=True)

    return record


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH SELECTION
# ═══════════════════════════════════════════════════════════════════════════════

def select_batch(pending: list[str], batch: str) -> list[str]:
    batch = batch.strip().lower()
    total = len(pending)
    if batch == "all":
        return pending
    if batch == "half":
        return pending[: max(1, math.ceil(total / 2))]
    if batch.startswith("first_"):
        try:
            n = int(batch.split("_", 1)[1])
            return pending[:n]
        except ValueError:
            pass
    try:
        return pending[: int(batch)]
    except ValueError:
        pass
    print(f"  [!] Unknown batch '{batch}', running all", flush=True)
    return pending


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    all_urls:        list[str],
    processed_path:  Path,
    output_dir:      Path,
    batch:           str,
    delay:           float,
    resolve_streams: bool,
) -> int:
    """
    Core pipeline. Returns exit code (0 = success, 1 = some errors).

    Steps
    ─────
    1. Subtract already-processed URLs
    2. Apply batch limit
    3. Scrape each episode (MAL ID fetched once per series, cached)
    4. Append record to output JSON (auto-split at 3 MB)
    5. Mark URL as processed
    """
    print("=" * 65, flush=True)
    print("  hianime.ad  —  Stream Scraper  (with MAL ID)", flush=True)
    print("=" * 65, flush=True)
    print(flush=True)

    if not all_urls:
        print("[!] No URLs to process.", flush=True)
        return 1

    print(f"[*] {len(all_urls)} total URLs after expansion", flush=True)

    processed = load_processed(processed_path)
    pending   = [u for u in all_urls if u not in processed]
    print(
        f"[*] {len(all_urls) - len(pending)} already processed, "
        f"{len(pending)} pending",
        flush=True
    )

    if not pending:
        print("[✓] All URLs already processed. Nothing to do.", flush=True)
        return 0

    selected  = select_batch(pending, batch)
    remaining = len(pending) - len(selected)
    print(f"[*] Batch '{batch}': running {len(selected)} URL(s)", flush=True)
    if remaining > 0:
        print(f"[*] {remaining} URL(s) deferred to next run", flush=True)
    print(flush=True)

    out      = OutputManager(output_dir, OUTPUT_STEM, MAX_OUTPUT_BYTES)
    ok_count = 0
    er_count = 0
    total    = len(selected)

    for i, ep_url in enumerate(selected, 1):
        slug   = get_slug(ep_url)  or "?"
        ep_num = get_ep_num(ep_url) or "?"
        pct    = i / total * 100
        print(f"[{i}/{total}  {pct:.1f}%]  {slug}  ep-{ep_num}", flush=True)

        record = scrape_episode(ep_url, resolve_streams=resolve_streams)
        out.add(record)
        mark_processed(processed_path, ep_url)

        if record.get("error"):
            er_count += 1
        else:
            ok_count += 1

        if i < total:
            time.sleep(delay)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(flush=True)
    print("=" * 65, flush=True)
    print(f"[✓] Done!  {ok_count} OK  /  {er_count} errors  /  {total} total", flush=True)
    if remaining > 0:
        print(f"[*] {remaining} URL(s) still pending — next run will continue", flush=True)
    print(flush=True)
    print("Output files:", flush=True)
    print(out.summary(), flush=True)
    print(flush=True)
    print(f"Processed log : {processed_path}", flush=True)
    print("=" * 65, flush=True)

    summary_path = output_dir / "run_summary.json"
    summary_path.write_text(
        json.dumps({
            "ok":        ok_count,
            "errors":    er_count,
            "total":     total,
            "remaining": remaining,
            "outputs":   [str(p.name) for p in out.all_output_paths()],
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, indent=2),
        encoding="utf-8",
    )

    return 0 if er_count == 0 else 1


# ═══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape hianime.ad stream URLs and save to local JSON files."
    )
    parser.add_argument(
        "--input", default=str(INPUT_FILE),
        help="Input file with URL list (default: input_urls_list.txt)",
    )
    parser.add_argument(
        "--url",
        help=(
            'Inline URL(s) instead of --input file. '
            'E.g. "https://hianime.ad/watch/naruto/ep-1 to 10". '
            'Comma or newline-separate multiple entries.'
        ),
    )
    parser.add_argument(
        "--batch", default="all",
        help=(
            "How many pending URLs to process this run. "
            "Values: all | half | first_N | N  (default: all)"
        ),
    )
    parser.add_argument(
        "--delay", type=float, default=INTER_EP_DELAY,
        help="Seconds between requests (default: 1.0, or $SCRAPE_DELAY env var)",
    )
    parser.add_argument(
        "--no-streams", action="store_true",
        help="Skip m3u8 resolution — only collect embed URLs (much faster)",
    )
    parser.add_argument(
        "--no-mal", action="store_true",
        help="Skip Jikan/MAL lookup entirely (faster, mal_id will be null)",
    )
    args = parser.parse_args()

    # Patch: if --no-mal, replace the lookup function with a no-op
    if args.no_mal:
        global get_mal_data_for_slug
        def get_mal_data_for_slug(slug, anime_name):   # noqa: F811
            return {"mal_id": None, "jikan_episodes": {}}
        print("[*] MAL lookup disabled (--no-mal)", flush=True)

    if args.url:
        all_urls = parse_inline_urls(args.url)
    else:
        all_urls = parse_input_file(Path(args.input))

    sys.exit(run(
        all_urls        = all_urls,
        processed_path  = PROCESSED_FILE,
        output_dir      = BASE_DIR,
        batch           = args.batch,
        delay           = args.delay,
        resolve_streams = not args.no_streams,
    ))


if __name__ == "__main__":
    main()
