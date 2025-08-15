#!/usr/bin/env python3
"""
INTERNET ARCHIVE API CLIENT MODULE
==================================

This module handles all communication with the Internet Archive (IA).
It's responsible for:
1. Searching for items in collections
2. Fetching metadata about items
3. Being polite to IA servers (rate limiting)

Think of this as the "translator" between our program and the Internet Archive.
It knows how to talk to IA's servers and handles all the web requests.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import json              # For parsing JSON responses from IA
import urllib.request    # For making HTTP requests
import urllib.error      # For handling HTTP errors
import asyncio           # For async operations
import time              # For timing and delays
from typing import Dict, List, Optional

# Import our own modules
from config import IA_BIN  # The IA command-line tool name
from utils import run_cmd, looks_like_identifier  # Helper functions

# ============================================================================
# RATE LIMITING SYSTEM
# ============================================================================

class RateGate:
    """
    A shared rate limiting mechanism to be polite to Internet Archive servers.
    
    This class ensures that if we get rate limited (server says "slow down"),
    all parts of our program wait before making more requests.
    
    Think of it like a traffic light that all our requests check before proceeding.
    """
    
    def __init__(self):
        """
        Initialize the rate gate.
        
        _until: The timestamp when we can start making requests again
        _lock: A lock to prevent multiple parts of the program from
               updating the rate limit at the same time
        """
        self._until = 0.0  # When we can resume (0.0 means no delay)
        self._lock = asyncio.Lock()  # Thread-safe lock for updates
    
    async def wait_if_needed(self):
        """
        Wait if we're currently rate limited.
        
        If the server told us to wait, this function will pause the program
        until it's okay to make more requests.
        """
        # Calculate how long we need to wait
        delay = self._until - time.time()
        
        if delay > 0:
            # We need to wait
            print(f"[pause] Backing off for {int(delay)}s due to rate limiting")
            await asyncio.sleep(delay)  # Wait for the specified time
    
    async def backoff(self, seconds: int):
        """
        Set a rate limit delay.
        
        Args:
            seconds (int): How many seconds to wait before making more requests
        
        This is called when the server tells us we're making too many requests.
        """
        async with self._lock:
            # Set the wait time, but don't make it shorter than any existing wait
            # This prevents one part of the program from overriding another's backoff
            self._until = max(self._until, time.time() + seconds)

# Create a global rate gate that all parts of the program share
RATE_GATE = RateGate()

# ============================================================================
# HTTP ERROR HANDLING
# ============================================================================

def _retry_after_seconds(e: urllib.error.HTTPError, default: int = 90) -> int:
    """
    Extract the "Retry-After" time from an HTTP error response.
    
    Args:
        e (urllib.error.HTTPError): The HTTP error that occurred
        default (int): Default wait time if server doesn't specify one
    
    Returns:
        int: How many seconds to wait before retrying
    
    When a server rate limits us, it usually sends a "Retry-After" header
    telling us how long to wait. This function extracts that information.
    """
    try:
        # Try to get the Retry-After header from the error response
        ra = e.headers.get("Retry-After")
        
        if not ra:
            # No Retry-After header found, use default
            return default
        
        try:
            # Convert the header value to an integer
            return int(ra)
        except ValueError:
            # Header exists but isn't a valid number, use default
            return default
            
    except Exception:
        # Something went wrong reading the headers, use default
        return default

# ============================================================================
# INTERNET ARCHIVE SEARCH
# ============================================================================

async def ia_search_identifiers(collection: str, media_mode: str, query_extra: Optional[str]) -> List[str]:
    """
    Search for items in an Internet Archive collection that match our media criteria.
    
    Args:
        collection (str): The collection ID to search in
        media_mode (str): "video", "audio", or "both" - what type of media to look for
        query_extra (Optional[str]): Additional search constraints (optional)
    
    Returns:
        List[str]: List of item identifiers that match our criteria
    
    This function uses the IA command-line tool to search for items.
    It builds a search query and filters the results to only include valid identifiers.
    
    Example:
        ids = await ia_search_identifiers("movies", "video", None)
        # Returns list of movie item IDs in the "movies" collection
    """
    # Build the media type part of the search query
    if media_mode == "video":
        med = "mediatype:movies"  # IA uses "movies" for all video content
    elif media_mode == "audio":
        med = "mediatype:audio"   # Audio content
    else:
        # "both" mode - look for either video or audio
        med = "(mediatype:movies OR mediatype:audio)"
    
    # Build the complete search query
    # Format: "collection:COLLECTION_NAME AND (media_type_criteria)"
    q = f'collection:{collection} AND {med}'
    
    # Add extra search constraints if provided
    if query_extra:
        # Wrap both parts in parentheses and combine with AND
        q = f"({q}) AND ({query_extra})"
    
    # Show the user what we're searching for
    print(f"[ia] search: {q}")
    
    # Run the IA search command
    # --itemlist flag tells IA to return just the identifiers, not full metadata
    code, out, err = await run_cmd([IA_BIN, "search", q, "--itemlist"])
    
    if code != 0:
        # Search failed - raise an error with the error message
        raise RuntimeError(err.strip() or out.strip())
    
    # Process the results
    # Split the output into lines and remove empty lines
    ids = [ln.strip() for ln in out.splitlines() if ln.strip()]
    
    # Filter to only include valid-looking identifiers
    # This removes any malformed results
    ids = [i for i in ids if looks_like_identifier(i)]
    
    return ids

# ============================================================================
# METADATA FETCHING
# ============================================================================

async def ia_metadata(identifier: str) -> Dict:
    """
    Fetch metadata for a specific Internet Archive item.
    
    Args:
        identifier (str): The item's unique identifier
    
    Returns:
        Dict: The item's metadata (files, title, description, etc.)
    
    This function tries to get metadata via HTTP first (faster),
    then falls back to the IA command-line tool if that fails.
    
    It includes rate limiting and retry logic to be polite to IA servers.
    """
    # Check if we need to wait due to rate limiting
    await RATE_GATE.wait_if_needed()
    
    # Build the metadata URL
    # IA provides metadata as JSON at this URL
    url = f"https://archive.org/metadata/{identifier}"
    
    # Define the actual HTTP request function
    # We put this in a separate function so we can run it in a thread
    def _fetch() -> str:
        """
        Make the HTTP request to get metadata.
        
        Returns:
            str: The JSON response as text
        
        This function runs in a separate thread to avoid blocking our async code.
        """
        with urllib.request.urlopen(url, timeout=30) as r:
            # Read the response and convert to text
            return r.read().decode("utf-8", "replace")
    
    try:
        # Try to fetch metadata via HTTP
        # asyncio.to_thread runs the HTTP request in a separate thread
        # This prevents the HTTP request from blocking our async program
        text = await asyncio.to_thread(_fetch)
        
        # Parse the JSON response into a Python dictionary
        return json.loads(text)
        
    except urllib.error.HTTPError as e:
        # HTTP request failed
        if e.code in (429, 503):
            # Rate limited (429) or service unavailable (503)
            # Calculate how long to wait
            back = _retry_after_seconds(e, 90)
            
            # Tell the rate gate to make everyone wait
            await RATE_GATE.backoff(back)
            
            # Re-raise the error so the caller can handle it
            raise
        else:
            # Some other HTTP error - re-raise it
            raise
            
    except Exception as http_err:
        # HTTP request failed for some other reason
        # Fall back to using the IA command-line tool
        
        print(f"[warn] HTTP metadata failed, trying CLI fallback: {http_err}")
        
        # Try to get metadata via the IA command-line tool
        code, out, err = await run_cmd([IA_BIN, "metadata", identifier])
        
        if code != 0:
            # Both HTTP and CLI failed
            raise RuntimeError(
                f"metadata via HTTP failed ({http_err}); "
                f"CLI failed: {err.strip() or out.strip()}"
            )
        
        try:
            # Parse the CLI output as JSON
            return json.loads(out)
        except json.JSONDecodeError:
            # CLI returned something that's not valid JSON
            raise RuntimeError("metadata not JSON; update internetarchive or use MDAPI")
