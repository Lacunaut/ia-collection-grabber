#!/usr/bin/env python3
"""
UTILITY FUNCTIONS MODULE
========================

This module contains small, reusable helper functions that are used throughout the program.
These are like "tools in a toolbox" - each does one specific job that other parts of the
program need.

Think of these as the basic building blocks that other modules use to do their work.
"""

# ============================================================================
# IMPORTS
# ============================================================================
# These are the Python libraries we need for these utility functions

import os          # For operating system functions (like checking if we're on Windows)
import re          # For regular expressions (pattern matching in text)
import asyncio     # For running commands asynchronously (without blocking)
import urllib.request  # For making HTTP requests with custom headers
import sys         # For exiting the program
import shutil      # For checking disk space
from pathlib import Path  # For working with file paths
from typing import List, Tuple, Optional  # For type hints (telling Python what types we expect)

# ============================================================================
# SYSTEM DETECTION
# ============================================================================

def is_windows() -> bool:
    """
    Check if we're running on Windows or not.
    
    Returns:
        bool: True if running on Windows, False otherwise (Linux, Mac, etc.)
    
    Why we need this:
    - Different operating systems have different commands
    - Windows uses "where" to find programs, others use "which"
    - This helps us use the right command for the current system
    """
    # os.name == "nt" is how Python identifies Windows
    # "nt" stands for "New Technology" (Windows NT)
    return os.name == "nt"

# ============================================================================
# URL AND IDENTIFIER PROCESSING
# ============================================================================

def extract_collection_id(s: str) -> str:
    """
    Extract a collection ID from a full Internet Archive URL or just return the ID if it's already clean.
    
    Args:
        s (str): Either a full URL like "https://archive.org/details/mycollection" 
                or just the collection ID like "mycollection"
    
    Returns:
        str: The clean collection ID
    
    Examples:
        extract_collection_id("https://archive.org/details/movies") -> "movies"
        extract_collection_id("movies") -> "movies"
        extract_collection_id("  movies  ") -> "movies"
    """
    # Remove any whitespace from the beginning and end
    s = s.strip()
    
    # Look for a pattern like "/details/SOMETHING" in the URL
    # r"/details/([^/?#]+)" is a regular expression that means:
    # - "/details/" - literally match these characters
    # - ([^/?#]+) - capture one or more characters that are NOT /, ?, or #
    # - This captures the collection ID part
    m = re.search(r"/details/([^/?#]+)", s)
    
    if m:
        # If we found a match, return the captured group (the collection ID)
        return m.group(1)
    
    # If no match found, assume it's already a clean collection ID
    return s

def item_page_url(identifier: str) -> str:
    """
    Create the URL for viewing an item's page on Internet Archive.
    
    Args:
        identifier (str): The item's unique identifier
    
    Returns:
        str: The full URL to the item's page
    
    Example:
        item_page_url("movie123") -> "https://archive.org/details/movie123"
    """
    return f"https://archive.org/details/{identifier}"

def file_download_url(identifier: str, filename: str) -> str:
    """
    Create the direct download URL for a specific file in an item.
    
    Args:
        identifier (str): The item's unique identifier
        filename (str): The name of the file to download
    
    Returns:
        str: The direct download URL for the file
    
    Example:
        file_download_url("movie123", "movie.mp4") -> "https://archive.org/download/movie123/movie.mp4"
    """
    return f"https://archive.org/download/{identifier}/{filename}"

def looks_like_identifier(s: str) -> bool:
    """
    Check if a string looks like a valid Internet Archive identifier.
    
    Args:
        s (str): The string to check
    
    Returns:
        bool: True if it looks like a valid identifier, False otherwise
    
    Rules for valid identifiers:
    - Must start with a letter or number
    - Can contain letters, numbers, underscores, hyphens, and dots
    - Must be at least 1 character long
    
    Examples:
        looks_like_identifier("movie123") -> True
        looks_like_identifier("movie_123") -> True
        looks_like_identifier("movie-123") -> True
        looks_like_identifier("movie.123") -> True
        looks_like_identifier("") -> False
        looks_like_identifier("movie 123") -> False (space not allowed)
    """
    # Regular expression pattern:
    # ^ - start of string
    # [A-Za-z0-9] - first character must be letter or number
    # [A-Za-z0-9_\-\.]+ - rest can be letters, numbers, underscore, hyphen, or dot
    # $ - end of string
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9_\-\.]+$", s))

# ============================================================================
# COMMAND EXECUTION
# ============================================================================

async def run_cmd(args: List[str]) -> Tuple[int, str, str]:
    """
    Run a command-line program and capture its output.
    
    Args:
        args (List[str]): List of command arguments
                         First item is the program name, rest are arguments
    
    Returns:
        Tuple[int, str, str]: (return_code, stdout, stderr)
        - return_code: 0 means success, non-zero means error
        - stdout: The normal output from the command
        - stderr: Any error messages from the command
    
    Example:
        result = await run_cmd(["ls", "-la"])
        code, output, errors = result
        if code == 0:
            print("Command succeeded:", output)
        else:
            print("Command failed:", errors)
    """
    # Create a subprocess (run another program)
    # asyncio.create_subprocess_exec runs the command without blocking our program
    proc = await asyncio.create_subprocess_exec(
        *args,  # Unpack the arguments list into separate arguments
        stdout=asyncio.subprocess.PIPE,  # Capture the normal output
        stderr=asyncio.subprocess.PIPE   # Capture any error output
    )
    
    # Wait for the command to finish and get its output
    # communicate() waits for the process to complete
    out_b, err_b = await proc.communicate()
    
    # Convert the binary output to text and return everything
    # decode("utf-8", "replace") converts bytes to text, replacing invalid characters
    return proc.returncode, out_b.decode("utf-8", "replace"), err_b.decode("utf-8", "replace")

async def require_binary(name: str) -> None:
    """
    Check if a required program is installed on the system.
    If not found, exit the program with an error.
    
    Args:
        name (str): The name of the program to check for
    
    Raises:
        SystemExit: If the program is not found
    
    Example:
        await require_binary("aria2c")  # Check if aria2c is installed
    """
    # Choose the right command based on the operating system
    # Windows uses "where", other systems use "which"
    finder = "where" if is_windows() else "which"
    
    # Try to find the program
    code, _, _ = await run_cmd([finder, name])
    
    if code != 0:
        # Program not found - exit with error
        print(f"[FATAL] Required tool not found: {name}")
        print(f"Please install {name} and try again.")
        sys.exit(1)
    
    # Program found - let user know
    print(f"[ok] Found {name}")

# ============================================================================
# DATA CONVERSION HELPERS
# ============================================================================

def _size_int(x) -> Optional[int]:
    """
    Safely convert a value to an integer, returning None if it fails.
    
    Args:
        x: The value to convert (could be string, number, or anything)
    
    Returns:
        Optional[int]: The integer value, or None if conversion failed
    
    This is used for converting file sizes from strings to numbers.
    Sometimes file sizes might be missing or invalid, so we need to handle that safely.
    
    Example:
        _size_int("123") -> 123
        _size_int(123) -> 123
        _size_int("abc") -> None
        _size_int(None) -> None
    """
    try:
        return int(x)
    except Exception:
        # If anything goes wrong (not a number, None, etc.), return None
        return None

# ============================================================================
# DISK SPACE MONITORING
# ============================================================================

def get_disk_space_percentage(path: Path) -> float:
    """
    Get the percentage of free disk space for a given path.
    
    Args:
        path (Path): The path to check disk space for
    
    Returns:
        float: Percentage of free disk space (0.0 to 100.0)
    
    This function checks how much free space is available on the disk
    where the given path is located. It's used to prevent downloads
    from filling up the disk completely.
    
    Example:
        free_space = get_disk_space_percentage(Path("./downloads"))
        if free_space < 2.0:
            print("Low disk space!")
    """
    try:
        # Get disk usage statistics for the path
        # total, used, free = shutil.disk_usage(path)
        usage = shutil.disk_usage(path)
        
        # Calculate percentage of free space
        # free_space_percentage = (free / total) * 100
        free_space_percentage = (usage.free / usage.total) * 100
        
        return free_space_percentage
        
    except Exception as e:
        # If we can't check disk space, assume it's okay
        # This prevents the program from crashing if there are permission issues
        print(f"[warn] Could not check disk space: {e}")
        return 100.0  # Assume plenty of space

def should_skip_download_for_space(path: Path, threshold: float = 2.0) -> bool:
    """
    Check if we should skip a download due to low disk space.
    
    Args:
        path (Path): The path where the download would go
        threshold (float): Minimum free space percentage (default 2.0%)
    
    Returns:
        bool: True if download should be skipped due to low space
    
    This function checks if there's enough free disk space to safely
    proceed with a download. It uses a threshold to ensure we don't
    completely fill up the disk.
    
    Example:
        if should_skip_download_for_space(Path("./downloads")):
            print("Skipping download - low disk space")
    """
    free_space = get_disk_space_percentage(path)
    return free_space < threshold

# ============================================================================
# HTTP REQUEST CONFIGURATION
# ============================================================================

def install_polite_ua():
    """
    Set up a polite User-Agent for HTTP requests to Internet Archive.
    
    A User-Agent tells the server what program is making the request.
    Using a polite User-Agent helps the server know we're a legitimate tool,
    not a bot or scraper.
    
    This function only needs to be called once at the start of the program.
    """
    # Create a new HTTP request opener (the thing that makes web requests)
    opener = urllib.request.build_opener()
    
    # Add a custom header that identifies our program
    # This tells the server "we're a personal archiving tool"
    opener.addheaders = [("User-Agent", "IA-personal-archiver (contact: local)")]
    
    # Install this opener as the default for all future HTTP requests
    # Now all urllib.request calls will use our polite User-Agent
    urllib.request.install_opener(opener)
