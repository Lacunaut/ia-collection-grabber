#!/usr/bin/env python3
"""
FILE SELECTION MODULE
====================

This module is responsible for analyzing the files in an Internet Archive item
and choosing the best one to download based on our preferences.

Think of it as a "smart file picker" that looks at all available files
and selects the one that best matches what we want (video, audio, or both).

It also checks if we already have the file locally to avoid re-downloading.
"""

# ============================================================================
# IMPORTS
# ============================================================================

from pathlib import Path  # For working with file paths
from typing import Dict, List, Optional, Tuple

# Import our configuration
from config import VIDEO_EXTS, AUDIO_EXTS, AUDIO_PREFS
from utils import _size_int

# ============================================================================
# FILE SELECTION LOGIC
# ============================================================================

def pick_best_file(files: List[Dict], media_mode: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Choose the best file to download from a list of available files.
    
    Args:
        files (List[Dict]): List of file metadata dictionaries from IA
                           Each dict has keys like "name", "size", "format"
        media_mode (str): "video", "audio", or "both" - what we're looking for
    
    Returns:
        Tuple[Optional[Dict], Optional[str]]: (best_file, reason)
        - best_file: The selected file metadata, or None if no suitable file
        - reason: Why no file was selected (if best_file is None)
    
    This function implements our file selection strategy:
    1. Filter out non-media files (text, metadata, etc.)
    2. Separate video and audio files
    3. Choose the largest file of the preferred type
    4. For audio, prefer certain formats over others
    
    Example:
        files = [
            {"name": "movie.mp4", "size": "1000000", "format": "MPEG4"},
            {"name": "movie.txt", "size": "100", "format": "Text"},
            {"name": "movie.wav", "size": "500000", "format": "WAV"}
        ]
        result = pick_best_file(files, "video")
        # Returns ({"name": "movie.mp4", ...}, None) - video file selected
    """
    
    # Step 1: Create a list of candidate files
    # We'll analyze each file and decide if it's worth considering
    cands = []
    
    for f in files:
        # Get the filename from the file metadata
        name = f.get("name") or ""
        
        # Skip files without names or directory entries (end with /)
        if not name or name.endswith("/"):
            continue
        
        # Get the file extension (like .mp4, .wav, etc.)
        ext = Path(name).suffix.lower()
        
        # Skip non-media files that we don't want to download
        # These are usually metadata, text files, or compressed archives
        if ext in {".txt", ".xml", ".json", ".gz", ".zip", ".sha1", ".md5", ".srt", ".vtt", ".nfo"}:
            continue
        
        # Get the file size and convert to integer
        size = _size_int(f.get("size"))
        
        # Add this file to our candidates list
        cands.append({
            "name": name,
            "ext": ext,
            "size": size,
            "format": f.get("format")
        })
    
    # If no candidates found, return None with reason
    if not cands:
        return None, "no_candidate_files"
    
    # Step 2: Separate files by type
    # Create lists of video and audio files from our candidates
    videos = [c for c in cands if c["ext"] in VIDEO_EXTS]
    audios = [c for c in cands if c["ext"] in AUDIO_EXTS]
    
    # Step 3: Choose based on media mode
    if media_mode == "video":
        # We only want video files
        if not videos:
            return None, "filtered_out_no_video"
        
        # Choose the largest video file
        # max() with key=lambda finds the item with the highest "size" value
        # If size is None, it's treated as 0
        return max(videos, key=lambda c: c["size"] or 0), None
    
    elif media_mode == "audio":
        # We only want audio files
        if not audios:
            return None, "filtered_out_no_audio"
        
        # For audio, we have preferences for certain formats
        # Try each preferred format in order
        for ext in AUDIO_PREFS:
            # Find all audio files with this preferred extension
            subset = [a for a in audios if a["ext"] == ext]
            if subset:
                # Found files with this preferred format
                # Choose the largest one
                return max(subset, key=lambda c: c["size"] or 0), None
        
        # No preferred formats found, choose the largest audio file overall
        return max(audios, key=lambda c: c["size"] or 0), None
    
    else:
        # "both" mode - prefer video, fall back to audio
        if videos:
            # We have video files, choose the largest
            return max(videos, key=lambda c: c["size"] or 0), None
        
        if audios:
            # No video files, but we have audio files
            # Use the same audio preference logic as audio mode
            for ext in AUDIO_PREFS:
                subset = [a for a in audios if a["ext"] == ext]
                if subset:
                    return max(subset, key=lambda c: c["size"] or 0), None
            return max(audios, key=lambda c: c["size"] or 0), None
        
        # No video or audio files found
        return None, "no_video_or_audio"

# ============================================================================
# LOCAL FILE CHECKING
# ============================================================================

def local_already_ok(dest_dir: Path, filename: str, expected_size: Optional[int]) -> bool:
    """
    Check if we already have the file locally and it's the right size.
    
    Args:
        dest_dir (Path): The directory where the file should be
        filename (str): The name of the file to download
        expected_size (Optional[int]): The expected file size in bytes
    
    Returns:
        bool: True if the file exists and has the expected size, False otherwise
    
    This prevents re-downloading files we already have.
    We check both that the file exists AND that it's the right size
    (in case a previous download was incomplete).
    
    Example:
        if local_already_ok(Path("./downloads"), "movie.mp4", 1000000):
            print("File already exists, skipping download")
    """
    
    # Build the full path to where the file should be
    target = dest_dir / filename
    
    # If the file doesn't exist, we definitely need to download it
    if not target.exists():
        return False
    
    # If we don't know the expected size, we can't verify it's correct
    # In this case, we'll re-download to be safe
    if expected_size is None:
        return False
    
    try:
        # Get the actual file size and compare with expected
        actual_size = target.stat().st_size
        return actual_size == expected_size
        
    except Exception:
        # Something went wrong checking the file (permissions, etc.)
        # Assume we need to download it
        return False
