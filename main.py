#!/usr/bin/env python3
"""
MAIN APPLICATION MODULE
=======================

This is the main entry point for the Internet Archive Collection Grabber.
It handles user interaction, validates inputs, sets up logging, and coordinates
the entire download workflow.

Think of this as the "conductor" that orchestrates all the other modules
to work together as a complete application.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
import csv
import sys
from pathlib import Path

# Import our modules
from config import DEFAULT_WORKERS, DEFAULT_ARIA_X, DEFAULT_ARIA_S
from utils import extract_collection_id, require_binary, install_polite_ua
from ia_client import ia_search_identifiers
from scheduler import schedule_fixed

# ============================================================================
# USER INPUT HELPERS
# ============================================================================

def ask_int(prompt: str, default: int, lo: int, hi: int) -> int:
    """
    Ask the user for an integer input with validation.
    
    Args:
        prompt (str): The question to ask the user
        default (int): Default value if user just presses Enter
        lo (int): Minimum allowed value
        hi (int): Maximum allowed value
    
    Returns:
        int: The user's input (or default) within the valid range
    
    This function:
    1. Shows the prompt with the default value in brackets
    2. Gets user input
    3. If input is empty, uses the default
    4. Validates the input is within the allowed range
    5. Returns the validated value
    
    Example:
        workers = ask_int("How many workers?", 4, 1, 24)
        # Shows: "How many workers? [4]: "
        # User can type a number or just press Enter for 4
    """
    
    # Show the prompt with default value
    s = input(f"{prompt} [{default}]: ").strip()
    
    # If user just pressed Enter, use default
    if not s:
        return default
    
    try:
        # Try to convert input to integer
        v = int(s)
        # Clamp to valid range (min of max, max of min)
        return max(lo, min(hi, v))
    except Exception:
        # If conversion failed, use default
        return default

# ============================================================================
# MAIN APPLICATION
# ============================================================================

async def main():
    """
    Main application function that orchestrates the entire download process.
    
    This function:
    1. Shows welcome message and explains what the program does
    2. Sets up polite HTTP requests
    3. Checks that required tools are installed
    4. Gets user input (collection, media type, settings)
    5. Searches for items in the collection
    6. Sets up logging
    7. Processes all items
    8. Shows completion message
    
    The function handles all the high-level coordination between modules.
    """
    
    # Step 1: Welcome and explanation
    print("Internet Archive best media grabber (static, polite)")
    print("Uses ia for search, MDAPI for metadata, aria2c for downloads.")
    print()
    
    # Step 2: Set up polite HTTP requests
    # This tells servers we're a legitimate tool, not a bot
    install_polite_ua()
    
    # Step 3: Check that required tools are installed
    # The program needs 'ia' and 'aria2c' to be available
    await require_binary("ia")
    await require_binary("aria2c")
    print()
    
    # Step 4: Get collection information
    raw_input_id = input("Collection ID or IA URL: ").strip()
    collection = extract_collection_id(raw_input_id)
    
    if not collection:
        print("Collection ID is required.")
        sys.exit(2)
    
    print(f"[info] Using collection ID: {collection}")
    
    # Step 5: Get media type preference
    choice = input("Download type (v=video only, a=audio only, b=both) [b]: ").strip().lower()
    
    if choice == "v":
        media_mode = "video"
    elif choice == "a":
        media_mode = "audio"
    else:
        media_mode = "both"
    
    print(f"[plan] Mode: {media_mode}")
    
    # Step 6: Get performance settings
    # These control how fast and how many downloads happen at once
    workers = ask_int("Concurrent items (workers)", DEFAULT_WORKERS, 1, 24)
    aria_x = ask_int("aria2 connections per server (-x)", DEFAULT_ARIA_X, 1, 16)
    aria_s = ask_int("aria2 splits (-s)", DEFAULT_ARIA_S, 1, 16)
    
    print(f"[plan] Workers={workers}  aria2: -x {aria_x}  -s {aria_s}  (max-connection-per-server={aria_x})")
    
    # Step 7: Set up output directory
    # Main folder named exactly as the collection ID
    out_root = Path.cwd() / collection
    print(f"[plan] Output root will be: {out_root}")
    
    # Step 8: Get optional search constraints
    extra = input("Optional extra IA search constraint (ENTER for none): ").strip() or None
    
    # Step 9: Search for items in the collection
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
    
    # Step 10: Set up logging
    # Create output directory and log file
    out_root.mkdir(parents=True, exist_ok=True)
    log_path = out_root / "download_log.csv"
    
    # Open log file for writing
    log_file = open(log_path, "w", newline="", encoding="utf-8")
    log_writer = csv.writer(log_file)
    
    # Write CSV header
    log_writer.writerow(["identifier", "action", "reason", "item_url", "file_url"])
    print(f"[log]  {log_path}")
    
    # Step 11: Process all items
    try:
        await schedule_fixed(identifiers, out_root, log_writer, media_mode, workers, aria_x, aria_s)
    finally:
        # Always close the log file, even if there's an error
        log_file.flush()
        log_file.close()
    
    # Step 12: Completion message
    print("\n[done] All items processed.")
    print(f"[log]  See {log_path} for skips and failures.")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        # Run the main function
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\nInterrupted.")
        sys.exit(1)
