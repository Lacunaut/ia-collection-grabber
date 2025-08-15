#!/usr/bin/env python3
"""
ITEM PROCESSING WORKER MODULE
=============================

This module contains the main worker function that processes a single Internet Archive item.
It orchestrates the complete workflow for one item:
1. Fetch metadata about the item
2. Choose the best file to download
3. Check if we already have it locally
4. Download the file if needed
5. Handle errors and retries
6. Log the results

Think of this as the "item processor" that takes one item ID and handles everything
needed to download the best file from that item.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
import random
import time
import csv
import urllib.error
from pathlib import Path
from typing import Dict

# Import our modules
from config import START_JITTER_SEC
from utils import item_page_url, file_download_url, _retry_after_seconds
from ia_client import ia_metadata, RATE_GATE
from file_selector import pick_best_file, local_already_ok
from downloader import aria2_download, rate_limited_errtext

# ============================================================================
# MAIN WORKER FUNCTION
# ============================================================================

async def process_identifier(identifier: str, out_root: Path, log_writer, media_mode: str,
                            aria_x: int, aria_s: int) -> Dict:
    """
    Process a single Internet Archive item from start to finish.
    
    Args:
        identifier (str): The IA item identifier to process
        out_root (Path): Root directory for downloads
        log_writer: CSV writer for logging results
        media_mode (str): "video", "audio", or "both"
        aria_x (int): aria2 connections per server
        aria_s (int): aria2 splits
    
    Returns:
        Dict: Metrics about the processing:
        {
            "bytes": int,      # Number of bytes downloaded (0 if skipped/failed)
            "seconds": float,  # Time spent processing
            "status": str      # "ok", "skip", or "fail"
        }
    
    This function implements the complete workflow for one item:
    1. Add small random delay (jitter) to be polite
    2. Fetch item metadata from IA
    3. Choose the best file to download
    4. Check if we already have it locally
    5. Download if needed
    6. Handle errors and retries
    7. Log the results
    
    The function includes retry logic for rate limiting and comprehensive error handling.
    """
    
    # Create the item's page URL for logging purposes
    page = item_page_url(identifier)
    
    # Print a header for this item
    print(f"\n[item] {identifier}")
    
    # Record the start time for performance metrics
    t0 = time.perf_counter()
    
    # Step 1: Add small random delay (jitter)
    # This prevents all workers from starting downloads at exactly the same time
    # which could overwhelm the servers
    await asyncio.sleep(random.uniform(*START_JITTER_SEC))
    
    # Step 2: Try to process the item (with retry logic)
    # We allow one retry if we get rate limited
    for attempt in (1, 2):
        try:
            # Step 2a: Check if we need to wait due to rate limiting
            await RATE_GATE.wait_if_needed()
            
            # Step 2b: Fetch metadata about this item
            # This tells us what files are available in the item
            meta = await ia_metadata(identifier)
            
            # Step 2c: Get the list of files from the metadata
            files = meta.get("files") or []
            
            # Step 2d: Check if the item has any files
            if not files:
                print(f"[skip] no files in metadata")
                # Log the skip and return metrics
                log_writer.writerow([identifier, "SKIP", "no_files_in_metadata", page, ""])
                return {
                    "bytes": 0, 
                    "seconds": time.perf_counter() - t0, 
                    "status": "skip"
                }
            
            # Step 2e: Choose the best file to download
            best, reason = pick_best_file(files, media_mode)
            
            if not best:
                # No suitable file found
                print(f"[skip] {reason}")
                log_writer.writerow([identifier, "SKIP", reason or "selection_failed", page, ""])
                return {
                    "bytes": 0, 
                    "seconds": time.perf_counter() - t0, 
                    "status": "skip"
                }
            
            # Step 2f: Extract file information
            name = best["name"]      # Filename (e.g., "movie.mp4")
            ext = best["ext"]        # Extension (e.g., ".mp4")
            sz = best["size"]        # File size in bytes
            url = file_download_url(identifier, name)  # Direct download URL
            
            # Step 2g: Check if we already have this file locally
            dest_dir = out_root / identifier  # Directory for this item
            if local_already_ok(dest_dir, name, sz):
                print(f"[skip] already present and size matches -> {name} ({sz} bytes)")
                return {
                    "bytes": 0, 
                    "seconds": time.perf_counter() - t0, 
                    "status": "skip"
                }
            
            # Step 2h: Display what we're going to download
            size_s = "unknown" if sz is None else f"{sz} bytes"
            print(f"[choose] {name}  ext={ext}  size={size_s}")
            print(f"[url]    {url}")
            
            # Step 2i: Download the file
            d0 = time.perf_counter()  # Start timing the download
            ok, err = await aria2_download(url, dest_dir, aria_x, aria_s)
            dsec = time.perf_counter() - d0  # Calculate download time
            
            if ok:
                # Download succeeded
                print(f"[ok]     downloaded -> {dest_dir / name}")
                return {
                    "bytes": int(sz or 0), 
                    "seconds": dsec, 
                    "status": "ok"
                }
            else:
                # Download failed - check if it's rate limiting
                back = rate_limited_errtext(err)
                
                if back and attempt == 1:
                    # Rate limited and this is our first attempt
                    # Set the rate limit and retry once
                    await RATE_GATE.backoff(back)
                    print(f"[warn] rate limited. backing off {back}s then retrying once")
                    continue  # Go to next attempt
                
                # Not rate limited, or this was our second attempt
                # Log the failure and return
                print(f"[fail]   aria2c: {err[:300]}")  # Show first 300 chars of error
                log_writer.writerow([identifier, "FAIL", f"aria2_error: {err[:500]}", page, url])
                return {
                    "bytes": 0, 
                    "seconds": dsec, 
                    "status": "fail"
                }
        
        except urllib.error.HTTPError as e:
            # HTTP error during metadata fetching
            if e.code in (429, 503) and attempt == 1:
                # Rate limited during metadata fetch
                back = _retry_after_seconds(e, 90)
                await RATE_GATE.backoff(back)
                print(f"[warn] metadata rate limited. backing off {back}s then retrying once")
                continue  # Retry after backoff
            print(f"[fail]   HTTPError during metadata: {e}")
            log_writer.writerow([identifier, "FAIL", f"metadata_http_{e.code}", page, ""])
            return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}
            
        except Exception as e:
            # Any other error
            print(f"[fail]   exception: {type(e).__name__}: {str(e)[:300]}")
            log_writer.writerow([identifier, "FAIL", f"exception: {type(e).__name__}: {str(e)[:500]}", page, ""])
            return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}
    
    # Should not reach here, but just in case
    return {"bytes": 0, "seconds": time.perf_counter() - t0, "status": "fail"}
