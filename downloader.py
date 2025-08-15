#!/usr/bin/env python3
"""
DOWNLOAD MANAGEMENT MODULE
=========================

This module handles the actual downloading of files using the aria2 downloader.
It's responsible for:
1. Setting up aria2 download commands
2. Detecting rate limiting in download errors
3. Managing the download process

Think of this as the "download engine" that takes a URL and saves the file to disk.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
from pathlib import Path
from typing import Tuple, Optional

# Import our configuration and utilities
from config import ARIA2_BIN, ARIA2_BASE
from utils import run_cmd

# ============================================================================
# ERROR ANALYSIS
# ============================================================================

def rate_limited_errtext(s: str) -> Optional[int]:
    """
    Analyze aria2 error text to detect rate limiting.
    
    Args:
        s (str): The error message from aria2
    
    Returns:
        Optional[int]: Suggested backoff time in seconds, or None if not rate limited
    
    This function looks for common rate limiting indicators in aria2 error messages.
    When it finds them, it suggests how long to wait before retrying.
    
    Example:
        backoff = rate_limited_errtext("HTTP 429 Too Many Requests")
        if backoff:
            print(f"Rate limited, wait {backoff} seconds")
    """
    
    # Convert to lowercase for easier pattern matching
    text = s.lower()
    
    # Check for HTTP 429 (Too Many Requests)
    if "429" in text or "too many requests" in text:
        return 90  # Wait 90 seconds
    
    # Check for HTTP 503 (Service Unavailable) and similar messages
    if ("503" in text or 
        "slowdown" in text or 
        "service temporarily unavailable" in text):
        return 90  # Wait 90 seconds
    
    # No rate limiting detected
    return None

# ============================================================================
# DOWNLOAD EXECUTION
# ============================================================================

async def aria2_download(url: str, out_dir: Path, x: int, s: int) -> Tuple[bool, str]:
    """
    Download a file using aria2 with specified settings.
    
    Args:
        url (str): The URL of the file to download
        out_dir (Path): Directory to save the file in
        x (int): Number of connections per server (-x parameter)
        s (int): Number of splits (-s parameter)
    
    Returns:
        Tuple[bool, str]: (success, error_message)
        - success: True if download succeeded, False if failed
        - error_message: Empty string if successful, error details if failed
    
    This function:
    1. Creates the output directory if it doesn't exist
    2. Builds the aria2 command with all necessary parameters
    3. Runs aria2 and waits for it to complete
    4. Returns success/failure status
    
    Example:
        success, error = await aria2_download(
            "https://archive.org/download/item/file.mp4",
            Path("./downloads/item"),
            8,  # 8 connections
            8   # 8 splits
        )
        if success:
            print("Download completed!")
        else:
            print(f"Download failed: {error}")
    """
    
    # Step 1: Ensure the output directory exists
    # mkdir(parents=True, exist_ok=True) creates all necessary parent directories
    # and doesn't error if the directory already exists
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 2: Build the aria2 command
    # Start with the aria2 binary name
    args = [ARIA2_BIN]
    
    # Add all the base arguments (from config.py)
    args.extend(ARIA2_BASE)
    
    # Add the connection and split parameters
    args.extend([
        "--max-connection-per-server", str(x),  # Maximum connections to server
        "-x", str(x),  # Number of connections (same as above)
        "-s", str(s),  # Number of splits
        "-j", "1",     # Download only 1 file at a time
        "--dir", str(out_dir),  # Output directory
        url            # The URL to download
    ])
    
    # Step 3: Execute the aria2 command
    # run_cmd runs the command and captures all output
    returncode, out, err = await run_cmd(args)
    
    # Step 4: Check the result
    if returncode == 0:
        # aria2 returned 0, which means success
        return True, ""
    else:
        # aria2 returned non-zero, which means failure
        # Return the error message (prefer stderr, fall back to stdout)
        error_msg = err.strip() or out.strip()
        return False, error_msg
