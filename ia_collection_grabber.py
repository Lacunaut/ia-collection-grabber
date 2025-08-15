#!/usr/bin/env python3
import asyncio
import csv
import json
import os
import re
import sys
import time
import random
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# -------- Media rules --------
VIDEO_EXTS = {
    ".mp4", ".mkv", ".mov", ".avi", ".wmv", ".mpg", ".mpeg",
    ".m4v", ".ts", ".flv", ".3gp", ".divx", ".webm"
}
AUDIO_PREFS = [".wav", ".flac", ".mp3", ".ogg"]
AUDIO_EXTS = set(AUDIO_PREFS + [".aiff", ".aac", ".m4a", ".wma", ".oga"])

# -------- Binaries --------
IA_BIN = "ia"
ARIA2_BIN = "aria2c"

# -------- Defaults (Balanced) --------
DEFAULT_WORKERS = 4          # concurrent items
DEFAULT_ARIA_X = 8           # aria2 connections per server
DEFAULT_ARIA_S = 8           # aria2 splits
START_JITTER_SEC = (0.05, 0.25)  # small random delay before each start

# Base aria2 args; we add -x/-s/--max-connection-per-server per call
ARIA2_BASE = [
    "--continue=true",
    "--auto-file-renaming=false",
    "--file-allocation=none",
    "--summary-interval=0",
    "--min-split-size=1M",
]

# -------- Helpers --------
def is_windows() -> bool:
    return os.name == "nt"

def extract_collection_id(s: str) -> str:
    s = s.strip()
    m = re.search(r"/details/([^/?#]+)", s)
    if m:
        return m.group(1)
    return s

async def run_cmd(args: List[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out_b, err_b = await proc.communicate()
    return proc.returncode, out_b.decode("utf-8", "replace"), err_b.decode("utf-8", "replace")

async def require_binary(name: str) -> None:
    finder = "where" if is_windows() else "which"
    code, _, _ = await run_cmd([finder, name])
    if code != 0:
        print(f"[FATAL] Required tool not found: {name}")
        sys.exit(1)
    print(f"[ok] Found {name}")

def _size_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def item_page_url(identifier: str) -> str:
    return f"https://archive.org/details/{identifier}"

def file_download_url(identifier: str, filename: str) -> str:
    return f"https://archive.org/download/{identifier}/{filename}"

def looks_like_identifier(s: str) -> bool:
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9_\-\.]+$", s))

# polite UA for metadata requests
def install_polite_ua():
    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", "IA-personal-archiver (contact: local)") ]
    urllib.request.install_opener(opener)

async def ia_search_identifiers(collection: str, media_mode: str, query_extra: Optional[str]) -> List[str]:
    if media_mode == "video":
        med = "mediatype:movies"
    elif media_mode == "audio":
        med = "mediatype:audio"
    else:
        med = "(mediatype:movies OR mediatype:audio)"
    q = f'collection:{collection} AND {med}'
    if query_extra:
        q = f"({q}) AND ({query_extra})"
    print(f"[ia] search: {q}")
    code, out, err = await run_cmd([IA_BIN, "search", q, "--itemlist"])
    if code != 0:
        raise RuntimeError(err.strip() or out.strip())
    ids = [ln.strip() for ln in out.splitlines() if ln.strip()]
    ids = [i for i in ids if looks_like_identifier(i)]
    return ids

# --- Metadata via HTTP MDAPI (robust) ---
class RateGate:
    """Shared backoff gate when rate-limited is detected."""
    def __init__(self):
        self._until = 0.0
        self._lock = asyncio.Lock()

    async def wait_if_needed(self):
        delay = self._until - time.time()
        if delay > 0:
            print(f"[pause] Backing off for {int(delay)}s due to rate limiting")
            await asyncio.sleep(delay)

    async def backoff(self, seconds: int):
        async with self._lock:
            self._until = max(self._until, time.time() + seconds)

RATE_GATE = RateGate()

def _retry_after_seconds(e: urllib.error.HTTPError, default: int = 90) -> int:
    try:
        ra = e.headers.get("Retry-After")
        if not ra:
            return default
        try:
            return int(ra)
        except ValueError:
            return default
    except Exception:
        return default

async def ia_metadata(identifier: str) -> Dict:
    await RATE_GATE.wait_if_needed()
    url = f"https://archive.org/metadata/{identifier}"
    def _fetch() -> str:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.read().decode("utf-8", "replace")
    try:
        text = await asyncio.to_thread(_fetch)
        return json.loads(text)
    except urllib.error.HTTPError as e:
        if e.code in (429, 503):
            back = _retry_after_seconds(e, 90)
            await RATE_GATE.backoff(back)
        raise
    except Exception as http_err:
        # fallback via CLI, last resort
        code, out, err = await run_cmd([IA_BIN, "metadata", identifier])
        if code != 0:
            raise RuntimeError(f"metadata via HTTP failed ({http_err}); CLI failed: {err.strip() or out.strip()}")
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            raise RuntimeError("metadata not JSON; update internetarchive or use MDAPI")

def pick_best_file(files: List[Dict], media_mode: str) -> Tuple[Optional[Dict], Optional[str]]:
    cands = []
    for f in files:
        name = f.get("name") or ""
        if not name or name.endswith("/"):
            continue
        ext = Path(name).suffix.lower()
        if ext in {".txt", ".xml", ".json", ".gz", ".zip", ".sha1", ".md5", ".srt", ".vtt", ".nfo"}:
            continue
        size = _size_int(f.get("size"))
        cands.append({"name": name, "ext": ext, "size": size, "format": f.get("format")})

    if not cands:
        return None, "no_candidate_files"

    videos = [c for c in cands if c["ext"] in VIDEO_EXTS]
    audios = [c for c in cands if c["ext"] in AUDIO_EXTS]

    if media_mode == "video":
        if not videos:
            return None, "filtered_out_no_video"
        return max(videos, key=lambda c: c["size"] or 0), None

    if media_mode == "audio":
        if not audios:
            return None, "filtered_out_no_audio"
        for ext in AUDIO_PREFS:
            subset = [a for a in audios if a["ext"] == ext]
            if subset:
                return max(subset, key=lambda c: c["size"] or 0), None
        return max(audios, key=lambda c: c["size"] or 0), None

    if videos:
        return max(videos, key=lambda c: c["size"] or 0), None
    if audios:
        for ext in AUDIO_PREFS:
            subset = [a for a in audios if a["ext"] == ext]
            if subset:
                return max(subset, key=lambda c: c["size"] or 0), None
        return max(audios, key=lambda c: c["size"] or 0), None

    return None, "no_video_or_audio"

def local_already_ok(dest_dir: Path, filename: str, expected_size: Optional[int]) -> bool:
    target = dest_dir / filename
    if not target.exists() or expected_size is None:
        return False
    try:
        return target.stat().st_size == expected_size
    except Exception:
        return False

def rate_limited_errtext(s: str) -> Optional[int]:
    """Return suggested backoff seconds if the aria2 error suggests throttling."""
    text = s.lower()
    if "429" in text or "too many requests" in text:
        return 90
    if "503" in text or "slowdown" in text or "service temporarily unavailable" in text:
        return 90
    return None

async def aria2_download(url: str, out_dir: Path, x: int, s: int) -> Tuple[bool, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    args = [ARIA2_BIN, *ARIA2_BASE,
            "--max-connection-per-server", str(x),
            "-x", str(x), "-s", str(s), "-j", "1",
            "--dir", str(out_dir), url]
    returncode, out, err = await run_cmd(args)
    if returncode == 0:
        return True, ""
    return False, err.strip() or out.strip()

# -------- Worker (one item) --------
async def process_identifier(identifier: str, out_root: Path, log_writer, media_mode: str,
                            aria_x: int, aria_s: int):
    """
    Returns dict with metrics:
      {"bytes": int or 0, "seconds": float or 0.0, "status": "ok|skip|fail"}
    """
    page = item_page_url(identifier)
    print(f"\n[item] {identifier}")
    t0 = time.perf_counter()

    # small random jitter before each item start
    await asyncio.sleep(random.uniform(*START_JITTER_SEC))

    # allow one retry if rate-limited
    for attempt in (1, 2):
        try:
            await RATE_GATE.wait_if_needed()
            meta = await ia_metadata(identifier)
            files = meta.get("files") or []
            if not files:
                print(f"[skip] no files in metadata")
                log_writer.writerow([identifier, "SKIP", "no_files_in_metadata", page, ""])
                return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "skip"}

            best, reason = pick_best_file(files, media_mode)
            if not best:
                print(f"[skip] {reason}")
                log_writer.writerow([identifier, "SKIP", reason or "selection_failed", page, ""])
                return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "skip"}

            name = best["name"]
            ext = best["ext"]
            sz = best["size"]
            url = file_download_url(identifier, name)

            dest_dir = out_root / identifier
            if local_already_ok(dest_dir, name, sz):
                print(f"[skip] already present and size matches -> {name} ({sz} bytes)")
                return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "skip"}

            size_s = "unknown" if sz is None else f"{sz} bytes"
            print(f"[choose] {name}  ext={ext}  size={size_s}")
            print(f"[url]    {url}")

            d0 = time.perf_counter()
            ok, err = await aria2_download(url, dest_dir, aria_x, aria_s)
            dsec = time.perf_counter() - d0

            if ok:
                print(f"[ok]     downloaded -> {dest_dir / name}")
                return {"bytes": int(sz or 0), "seconds": dsec, "status": "ok"}
            else:
                # detect polite backoff conditions
                back = rate_limited_errtext(err)
                if back and attempt == 1:
                    await RATE_GATE.backoff(back)
                    print(f"[warn] rate limited. backing off {back}s then retrying once")
                    continue  # retry after backoff
                print(f"[fail]   aria2c: {err[:300]}")
                log_writer.writerow([identifier, "FAIL", f"aria2_error: {err[:500]}", page, url])
                return {"bytes": 0, "seconds": dsec, "status": "fail"}

        except urllib.error.HTTPError as e:
            # metadata stage throttled
            if e.code in (429, 503) and attempt == 1:
                back = _retry_after_seconds(e, 90)
                await RATE_GATE.backoff(back)
                print(f"[warn] metadata rate limited. backing off {back}s then retrying once")
                continue
            print(f"[fail]   HTTPError during metadata: {e}")
            log_writer.writerow([identifier, "FAIL", f"metadata_http_{e.code}", page, ""])
            return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}
        except Exception as e:
            print(f"[fail]   exception: {type(e).__name__}: {str(e)[:300]}")
            log_writer.writerow([identifier, "FAIL", f"exception: {type(e).__name__}: {str(e)[:500]}", page, ""])
            return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}

    # should not reach
    return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}

# -------- Fixed-concurrency scheduler --------
async def schedule_fixed(identifiers: List[str], out_root: Path, log_writer, media_mode: str,
                         workers: int, aria_x: int, aria_s: int):
    sem = asyncio.Semaphore(max(1, workers))
    total = len(identifiers)
    done_cnt = 0

    async def one(iid: str):
        async with sem:
            return await process_identifier(iid, out_root, log_writer, media_mode, aria_x, aria_s)

    # kick off a pipeline of tasks while preserving the limit
    running = set()
    it = iter(identifiers)
    for _ in range(min(workers, total)):
        try:
            running.add(asyncio.create_task(one(next(it))))
        except StopIteration:
            break

    while running:
        done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            _ = t.result()
            done_cnt += 1
            print(f"[prog] {done_cnt}/{total} complete")
            try:
                running.add(asyncio.create_task(one(next(it))))
            except StopIteration:
                pass

# -------- Main (interactive) --------
async def main():
    print("Internet Archive best media grabber (static, polite)")
    print("Uses ia for search, MDAPI for metadata, aria2c for downloads.")

    # polite UA for metadata requests
    install_polite_ua()

    await require_binary(IA_BIN)
    await require_binary(ARIA2_BIN)

    raw_input_id = input("Collection ID or IA URL: ").strip()
    collection = extract_collection_id(raw_input_id)
    if not collection:
        print("Collection ID is required.")
        sys.exit(2)
    print(f"[info] Using collection ID: {collection}")

    choice = input("Download type (v=video only, a=audio only, b=both) [b]: ").strip().lower()
    if choice == "v":
        media_mode = "video"
    elif choice == "a":
        media_mode = "audio"
    else:
        media_mode = "both"
    print(f"[plan] Mode: {media_mode}")

    # Static profile with optional overrides
    def ask_int(prompt: str, default: int, lo: int, hi: int) -> int:
        s = input(f"{prompt} [{default}]: ").strip()
        if not s:
            return default
        try:
            v = int(s)
            return max(lo, min(hi, v))
        except Exception:
            return default

    workers = ask_int("Concurrent items (workers)", DEFAULT_WORKERS, 1, 24)
    aria_x = ask_int("aria2 connections per server (-x)", DEFAULT_ARIA_X, 1, 16)
    aria_s = ask_int("aria2 splits (-s)", DEFAULT_ARIA_S, 1, 16)
    print(f"[plan] Workers={workers}  aria2: -x {aria_x}  -s {aria_s}  (max-connection-per-server={aria_x})")

    # Main folder named exactly as the collection ID
    out_root = Path.cwd() / collection
    print(f"[plan] Output root will be: {out_root}")

    extra = input("Optional extra IA search constraint (ENTER for none): ").strip() or None

    # Fetch identifiers
    print("[step] Searching collection...")
    try:
        identifiers = await ia_search_identifiers(collection, media_mode, extra)
    except Exception as e:
        print(f"[fatal] search failed: {e}")
        sys.exit(3)

    if not identifiers:
        print("[done] No matching items found.")
        return

    print(f"[info] Found {len(identifiers)} items")

    # Prepare logging (overwrite each run)
    out_root.mkdir(parents=True, exist_ok=True)
    log_path = out_root / "download_log.csv"
    log_file = open(log_path, "w", newline="", encoding="utf-8")
    log_writer = csv.writer(log_file)
    log_writer.writerow(["identifier", "action", "reason", "item_url", "file_url"])
    print(f"[log]  {log_path}")

    try:
        await schedule_fixed(identifiers, out_root, log_writer, media_mode, workers, aria_x, aria_s)
    finally:
        log_file.flush()
        log_file.close()

    print("\n[done] All items processed.")
    print(f"[log]  See {log_path} for skips and failures.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
