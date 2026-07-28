"""
hianime_scraper.py
──────────────────
GitHub Actions-compatible scraper for hianime.ad / vibeplayer.site.
Reads input_urls_list.txt, scrapes episode embed URLs, saves results
locally in the repo (committed back to GitHub by the workflow).

Input file  : input_urls_list.txt
Processed   : already_processed_urls_list.txt
Output      : hianime_streams_list.json  (auto-splits at 3 MB →
              hianime_streams_list_2.json, _3.json …)

Output record format:
  {
    "serial_no":                    1,
    "mal_id/anilist_id/tmdb_id":    null,          ← fill in manually later
    "episode_url":                  "https://hianime.ad/watch/one-piece/ep-1",
    "anime_name":                   "One Piece",
    "hsub_streamhg_embed_url_ep_1": "https://otakuhg.site/e/…",
    "hsub_earnvids_embed_url_ep_1": "https://otakuvid.online/embed/…",
    "sub_streamhg_embed_url_ep_1":  "https://otakuhg.site/e/…",
    "sub_someother_embed_url_ep_1": "https://somenewsite.com/…",   ← catch-all
    ...
  }

  All data-video="…" URLs found on the page are captured — including any
  new or unknown streaming hosts — via a catch-all pass after the known
  domain patterns. Keys for unknown hosts are derived from their domain name.

  After every run the JSON output is automatically sorted by anime slug
  (alphabetical) then by episode number (ascending), and serial_no is
  re-assigned to match the new order.  Existing chunk files are rewritten
  in-place so git diff only shows the new records.

Usage:
  python hianime_scraper.py --batch 50
  python hianime_scraper.py --batch all
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

BASE_DIR         = Path(__file__).parent
INPUT_FILE       = BASE_DIR / "input_urls_list_maker_with_MAL_and_myanimelist_url.txt"
PROCESSED_FILE   = BASE_DIR / "already_processed_urls_list.txt"
OUTPUT_STEM      = "hianime_streams_list"
MAX_OUTPUT_BYTES = 3 * 1024 * 1024          # 3 MB hard cap per file
REQUEST_TIMEOUT  = 20
INTER_EP_DELAY   = float(os.environ.get("SCRAPE_DELAY", "1.0"))

# Global serial number counter (incremented for every record written)
_SERIAL = 0


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


# ═══════════════════════════════════════════════════════════════════════════════
# INPUT PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def expand_line(line: str) -> list[tuple[str, int | None]]:
    """
    Expand one input line into a list of (episode_url, mal_id) tuples.

    Supported formats
    ─────────────────
    https://hianime.ad/watch/one-piece/ep-1 to 220 | MAL_ID: 21   → 220 tuples
    https://hianime.ad/watch/naruto-shippuden/ep-50                → 1 tuple
    1. https://hianime.ad/watch/summer/ep-1 to 2 | MAL_ID: 1692 | https://…
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return []

    # Strip leading list number (e.g. "1. " or "42. ")
    line = re.sub(r'^\d+\.\s*', '', line)

    # Extract optional MAL_ID anywhere after a pipe
    mal_id: int | None = None
    mal_m = re.search(r'\|\s*MAL_ID\s*:\s*(\d+)', line, re.IGNORECASE)
    if mal_m:
        mal_id = int(mal_m.group(1))

    # Strip everything from the first pipe onward so URL matching is clean
    url_part = line.split('|')[0].strip()

    m = re.match(
        r'(https://hianime\.ad/watch/[^/]+/ep-)(\d+)\s+to\s+(\d+)',
        url_part, re.IGNORECASE,
    )
    if m:
        prefix = m.group(1)
        start, end = int(m.group(2)), int(m.group(3))
        if start > end:
            print(f"  [!] Bad range (start > end), skipping: {line}", flush=True)
            return []
        return [(f"{prefix}{ep}", mal_id) for ep in range(start, end + 1)]

    m = re.match(r'(https://hianime\.ad/watch/[^/]+/ep-\d+)', url_part, re.IGNORECASE)
    if m:
        return [(m.group(1), mal_id)]

    print(f"  [?] Unrecognised line, skipping: {line}", flush=True)
    return []


def parse_input_file(path: Path) -> list[tuple[str, int | None]]:
    if not path.exists():
        sys.exit(f"[!] Input file not found: {path}\n    Create it with one URL or range per line.")
    entries: list[tuple[str, int | None]] = []
    seen: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        for url, mal_id in expand_line(raw):
            if url not in seen:
                seen.add(url)
                entries.append((url, mal_id))
    return entries


def parse_inline_urls(text: str) -> list[tuple[str, int | None]]:
    entries: list[tuple[str, int | None]] = []
    seen: set[str] = set()
    for part in re.split(r'[,\n]', text):
        for url, mal_id in expand_line(part.strip()):
            if url not in seen:
                seen.add(url)
                entries.append((url, mal_id))
    return entries


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
        n = 1
        while self._chunk_path(n).exists():
            n += 1
        last = n - 1
        if last < 1:
            self._chunk, self._records = 1, []
            return
        self._chunk = last
        try:
            data = json.loads(self._chunk_path(last).read_text(encoding="utf-8"))
            self._records = data if isinstance(data, list) else []
            sz = self._chunk_path(last).stat().st_size
            print(
                f"[*] Resuming → {self._chunk_path(last).name} "
                f"({len(self._records)} records, {sz // 1024} KB)",
                flush=True,
            )
        except Exception:
            self._records = []

    def add(self, record: dict) -> None:
        if self._records and self._would_overflow(record):
            self._flush()
            old = self._current_path()
            self._chunk += 1
            self._records = []
            print(f"\n[+] Chunk full → {old.name} sealed ({old.stat().st_size // 1024} KB)", flush=True)
            print(f"[+] New chunk: {self._current_path().name}\n", flush=True)
        self._records.append(record)
        self._flush()

    def summary(self) -> str:
        lines, n = [], 1
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
    return slug.replace("-", " ").title()


# ═══════════════════════════════════════════════════════════════════════════════
# SERVER LABEL → CLEAN KEY
# Maps the label shown on hianime (e.g. "StreamHG", "Earnvids", "HD-1") to
# a short, lowercase, underscore-safe token used in the flat output key.
# ═══════════════════════════════════════════════════════════════════════════════

# Known label → key token. Anything not listed falls through to _label_key().
_LABEL_MAP = {
    "streamhg":   "streamhg",
    "earnvids":   "earnvids",
    "doodstream": "doodstream",
    "hd-1":       "hd1",
    "hd-2":       "hd2",
    "hd1":        "hd1",
    "hd2":        "hd2",
}

def _label_key(label: str) -> str:
    norm = label.lower().strip()
    return _LABEL_MAP.get(norm, re.sub(r'[^a-z0-9]+', '_', norm).strip('_'))


# ═══════════════════════════════════════════════════════════════════════════════
# EPISODE PAGE PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def parse_episode_page(ep_url: str) -> dict:
    """
    Fetch the episode page and return a flat dict of ALL embed URLs found.

    Strategy (in order, per server-type block):
    1. vibeplayer.site embeds  → key = "{server_type}_{label}"
    2. Known external domains  → key = "{server_type}_{label_name}"
       (otakuhg.site=streamhg, otakuvid.online=earnvids, playmogo.com=doodstream)
    3. Catch-all for ANY other data-video="https://…" URL not yet captured
       → key = "{server_type}_{sanitised_domain}[_{n}]"

    Return format:
    {
      "hsub_streamhg": "https://…",
      "hsub_earnvids": "https://…",
      "sub_hd1":       "https://…",
      "sub_someother": "https://…",   ← previously missed
      …
    }
    """
    r = session.get(ep_url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    html = r.text

    flat: dict[str, str] = {}

    # Known external domain → short label token
    _KNOWN_DOMAINS: list[tuple[str, str]] = [
        (r'otakuhg\.site',    "streamhg"),
        (r'otakuvid\.online', "earnvids"),
        (r'playmogo\.com',    "doodstream"),
    ]

    def _domain_label(url: str) -> str:
        """Derive a short key token from any URL's domain."""
        m = re.search(r'https?://(?:www\.)?([^/]+)', url)
        if not m:
            return "unknown"
        domain = m.group(1)
        # Strip common TLD noise to keep keys short
        domain = re.sub(r'\.(com|net|org|site|online|live|tv|io|cc|me)$', '', domain)
        return re.sub(r'[^a-z0-9]+', '_', domain.lower()).strip('_')

    # Primary server blocks
    blocks = re.findall(
        r'<div class="ps__-list server-items" data-id="([^"]+)">(.*?)</div>\s*<div class="clearfix',
        html, re.DOTALL,
    )
    for server_type, block_html in blocks:
        st = server_type.lower().strip()

        # ── 1. vibeplayer.site embeds (carry an explicit label in the element) ──
        for embed_full, label in re.findall(
            r'data-video="(https://vibeplayer\.site/[^"]+)"[^>]*>([^<]+)<', block_html
        ):
            embed_url = embed_full.split("?sub=")[0].split("?")[0]
            key = f"{st}_{_label_key(label.strip())}"
            flat[key] = embed_url

        # ── 2. Known external players ──────────────────────────────────────────
        for domain_pat, label_name in _KNOWN_DOMAINS:
            for m in re.finditer(
                r'data-video="(https://' + domain_pat + r'[^"]+)"',
                block_html,
            ):
                key = f"{st}_{label_name}"
                flat.setdefault(key, m.group(1).split("?")[0])

        # ── 3. Catch-all: every other data-video URL not yet captured ──────────
        already_captured = set(flat.values())
        for m in re.finditer(r'data-video="(https?://[^"]+)"', block_html):
            url_raw = m.group(1).split("?")[0]
            if url_raw in already_captured:
                continue
            # Skip vibeplayer (handled above) and known domains (handled above)
            if re.search(r'vibeplayer\.site|otakuhg\.site|otakuvid\.online|playmogo\.com', url_raw):
                continue
            base_key = f"{st}_{_domain_label(url_raw)}"
            # Disambiguate if the same server-type already has a URL from this domain
            if base_key not in flat:
                flat[base_key] = url_raw
            else:
                n = 2
                while f"{base_key}_{n}" in flat:
                    n += 1
                flat[f"{base_key}_{n}"] = url_raw
            already_captured.add(url_raw)

    return flat


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLE EPISODE SCRAPE
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_episode(ep_url: str, mal_id: int | None = None) -> dict:
    """
    Scrape one episode and return a flat record dict.

    Output shape:
    {
      "serial_no":                    1,
      "mal_id/anilist_id/tmdb_id":    1692,
      "episode_no":                   1,
      "episode_url":                  "https://hianime.ad/watch/one-piece/ep-1",
      "anime_name":                   "One Piece",
      "hsub_streamhg_embed_url_ep_1": "https://otakuhg.site/e/…",
      "hsub_earnvids_embed_url_ep_1": "https://otakuvid.online/embed/…",
      "sub_hd1_embed_url_ep_1":       "https://vibeplayer.site/…",
      ...
    }
    """
    global _SERIAL
    _SERIAL += 1

    slug     = get_slug(ep_url)
    ep_num   = get_ep_num(ep_url)
    anime    = slug_to_anime_name(slug) if slug else "Unknown"

    record: dict = {
        "serial_no":                 _SERIAL,
        "mal_id/anilist_id/tmdb_id": mal_id,   # populated from MAL_ID in input file
        "episode_no":                ep_num,
        "episode_url":               ep_url,
        "anime_name":                anime,
    }

    try:
        flat = parse_episode_page(ep_url)

        # Attach each embed URL with a fully-qualified flat key
        # e.g. "hsub_streamhg" + ep_num=1 → "hsub_streamhg_embed_url_ep_1"
        for base_key, embed_url in flat.items():
            full_key = f"{base_key}_embed_url_ep_{ep_num}"
            record[full_key] = embed_url

        counts = ", ".join(f"{k}={v}" for k, v in flat.items()) if flat else "no embeds"
        print(f"  [OK] ep-{ep_num}  {len(flat)} embed(s)", flush=True)

    except Exception as exc:
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
            return pending[: int(batch.split("_", 1)[1])]
        except ValueError:
            pass
    try:
        return pending[: int(batch)]
    except ValueError:
        pass
    print(f"  [!] Unknown batch '{batch}', running all", flush=True)
    return pending


# ═══════════════════════════════════════════════════════════════════════════════
# POST-RUN SORT
# ═══════════════════════════════════════════════════════════════════════════════

def sort_output_files(output_dir: Path, stem: str, max_bytes: int) -> list[Path]:
    """
    After a run completes, collect every record from all chunk files, sort them
    by anime slug then by episode number, re-assign serial_no, re-split into
    ≤max_bytes chunks, and write them back.

    Sort order
    ──────────
    Primary  : anime slug extracted from episode_url  (alphabetical)
               e.g.  /watch/one-piece/ep-5  → "one-piece"
    Secondary: episode number (numeric ascending)
               e.g.  ep-5  →  5

    Returns the list of output paths that were written.
    """

    def _chunk_path(n: int) -> Path:
        name = stem if n == 1 else f"{stem}_{n}"
        return output_dir / f"{name}.json"

    # ── 1. Collect all existing records ───────────────────────────────────────
    all_records: list[dict] = []
    n = 1
    while _chunk_path(n).exists():
        try:
            data = json.loads(_chunk_path(n).read_text(encoding="utf-8"))
            if isinstance(data, list):
                all_records.extend(data)
        except Exception as exc:
            print(f"  [!] sort: could not read {_chunk_path(n).name}: {exc}", flush=True)
        n += 1

    if not all_records:
        print("[*] sort: no records found — skipping sort", flush=True)
        return []

    # ── 2. Sort ───────────────────────────────────────────────────────────────
    def _sort_key(rec: dict) -> tuple:
        url  = rec.get("episode_url", "")
        slug_m = re.search(r'/watch/([^/]+)/ep-', url)
        ep_m   = re.search(r'/ep-(\d+)', url)
        slug   = slug_m.group(1) if slug_m else ""
        ep_num = int(ep_m.group(1)) if ep_m else 0
        return (slug, ep_num)

    all_records.sort(key=_sort_key)

    # ── 3. Re-assign serial_no (1-based, contiguous after sort) ───────────────
    for idx, rec in enumerate(all_records, 1):
        rec["serial_no"] = idx

    # ── 4. Re-split into chunks and write ─────────────────────────────────────
    def _encode(recs: list) -> bytes:
        return json.dumps(recs, indent=2, ensure_ascii=False).encode("utf-8")

    # Delete old chunk files first so stale extras don't linger
    old_n = 1
    while _chunk_path(old_n).exists():
        _chunk_path(old_n).unlink()
        old_n += 1

    chunk_num   = 1
    chunk_recs: list[dict] = []
    written:    list[Path] = []

    for rec in all_records:
        if chunk_recs and len(_encode(chunk_recs + [rec])) > max_bytes:
            p = _chunk_path(chunk_num)
            p.write_bytes(_encode(chunk_recs))
            written.append(p)
            print(
                f"  [sort] wrote {p.name}  ({p.stat().st_size // 1024} KB, "
                f"{len(chunk_recs)} records)",
                flush=True,
            )
            chunk_num += 1
            chunk_recs = []
        chunk_recs.append(rec)

    if chunk_recs:
        p = _chunk_path(chunk_num)
        p.write_bytes(_encode(chunk_recs))
        written.append(p)
        print(
            f"  [sort] wrote {p.name}  ({p.stat().st_size // 1024} KB, "
            f"{len(chunk_recs)} records)",
            flush=True,
        )

    print(
        f"[✓] Sort complete — {len(all_records)} records across "
        f"{len(written)} file(s), grouped by anime then episode",
        flush=True,
    )
    return written


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    all_entries:    list[tuple[str, int | None]],
    processed_path: Path,
    output_dir:     Path,
    batch:          str,
    delay:          float,
) -> int:
    print("=" * 65, flush=True)
    print("  hianime.ad  —  Stream Scraper", flush=True)
    print("=" * 65, flush=True)
    print(flush=True)

    if not all_entries:
        print("[!] No URLs to process.", flush=True)
        return 1

    print(f"[*] {len(all_entries)} total URLs after expansion", flush=True)

    processed = load_processed(processed_path)
    pending   = [(u, mid) for u, mid in all_entries if u not in processed]
    print(
        f"[*] {len(all_entries) - len(pending)} already processed, "
        f"{len(pending)} pending",
        flush=True,
    )

    if not pending:
        print("[✓] All URLs already processed. Nothing to do.", flush=True)
        return 0

    # select_batch works on plain URL list; preserve mal_id mapping
    pending_urls = [u for u, _ in pending]
    mal_id_map   = {u: mid for u, mid in pending}

    selected_urls = select_batch(pending_urls, batch)
    selected      = [(u, mal_id_map[u]) for u in selected_urls]
    remaining     = len(pending) - len(selected)
    print(f"[*] Batch '{batch}': running {len(selected)} URL(s)", flush=True)
    if remaining > 0:
        print(f"[*] {remaining} URL(s) deferred to next run", flush=True)
    print(flush=True)

    out      = OutputManager(output_dir, OUTPUT_STEM, MAX_OUTPUT_BYTES)
    ok_count = 0
    er_count = 0
    total    = len(selected)

    for i, (ep_url, mal_id) in enumerate(selected, 1):
        slug   = get_slug(ep_url)  or "?"
        ep_num = get_ep_num(ep_url) or "?"
        pct    = i / total * 100
        print(f"[{i}/{total}  {pct:.1f}%]  {slug}  ep-{ep_num}  (MAL:{mal_id})", flush=True)

        record = scrape_episode(ep_url, mal_id)
        out.add(record)
        mark_processed(processed_path, ep_url)

        embed_keys = [k for k in record if k.endswith("_embed_url_ep_" + str(get_ep_num(ep_url)))]
        if embed_keys:
            ok_count += 1
        else:
            er_count += 1

        if i < total:
            time.sleep(delay)

    print(flush=True)
    print("=" * 65, flush=True)
    print(f"[✓] Done!  {ok_count} OK  /  {er_count} errors  /  {total} total", flush=True)
    if remaining > 0:
        print(f"[*] {remaining} URL(s) still pending — next run will continue", flush=True)
    print(flush=True)

    # ── Sort all output files by anime slug then episode number ───────────────
    print("[*] Sorting output by anime → episode …", flush=True)
    sorted_paths = sort_output_files(output_dir, OUTPUT_STEM, MAX_OUTPUT_BYTES)
    final_paths  = sorted_paths if sorted_paths else out.all_output_paths()

    print(flush=True)
    print("Output files:", flush=True)
    for p in final_paths:
        try:
            cnt = len(json.loads(p.read_text(encoding="utf-8")))
        except Exception:
            cnt = "?"
        print(f"  {p.name}  ({p.stat().st_size // 1024} KB, {cnt} records)", flush=True)
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
            "outputs":   [str(p.name) for p in final_paths],
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
        description="Scrape hianime.ad embed URLs and save to local JSON files."
    )
    parser.add_argument(
        "--input", default=str(INPUT_FILE),
        help="Input file with URL list (default: input_urls_list_maker_with_MAL_and_myanimelist_url.txt)",
    )
    parser.add_argument(
        "--url",
        help=(
            'Inline URL(s) instead of --input file. '
            'E.g. "https://hianime.ad/watch/naruto/ep-1 to 10".'
        ),
    )
    parser.add_argument(
        "--batch", default="all",
        help="How many pending URLs to process: all | half | first_N | N  (default: all)",
    )
    parser.add_argument(
        "--delay", type=float, default=INTER_EP_DELAY,
        help="Seconds between requests (default: 1.0, or $SCRAPE_DELAY env var)",
    )
    args = parser.parse_args()

    all_entries = parse_inline_urls(args.url) if args.url else parse_input_file(Path(args.input))

    sys.exit(run(
        all_entries    = all_entries,
        processed_path = PROCESSED_FILE,
        output_dir     = BASE_DIR,
        batch          = args.batch,
        delay          = args.delay,
    ))


if __name__ == "__main__":
    main()
