#!/usr/bin/env python3
"""
CONFIGURATION MODULE
===================

This module contains ALL the configuration constants and settings used throughout the program.
Think of this as the "settings file" where we define what the program should do and how it should behave.

Every other module will import these constants to know:
- What file types to look for
- How many downloads to run at once
- What tools to use for downloading
- How to behave politely towards servers

This keeps all settings in one place, making it easy to change behavior without hunting through code.
"""

# ============================================================================
# MEDIA TYPE DEFINITIONS
# ============================================================================
# These sets and lists define what file types we consider "video" or "audio"
# The program uses these to decide which files to download from each item

# VIDEO_EXTS: A set of file extensions that we consider video files
# A "set" is like a list but faster to search and no duplicates allowed
# We use a set because we need to quickly check "is this file a video?"
VIDEO_EXTS = {
    ".mp4",    # Most common video format, good quality, widely supported
    ".mkv",    # High quality container format, often used for movies
    ".mov",    # Apple's video format
    ".avi",    # Older but still common video format
    ".wmv",    # Windows Media Video format
    ".mpg",    # MPEG video format
    ".mpeg",   # Alternative MPEG extension
    ".m4v",    # iTunes video format
    ".ts",     # Transport Stream, often used for streaming
    ".flv",    # Flash Video (older format)
    ".3gp",    # Mobile video format
    ".divx",   # DivX video codec format
    ".webm"    # Web-optimized video format
}

# AUDIO_PREFS: A LIST (not set) of audio formats in order of preference
# We prefer these formats over others when multiple audio files exist
# Order matters - first in list is most preferred
AUDIO_PREFS = [
    ".wav",    # Uncompressed audio, highest quality but large files
    ".flac",   # Lossless compression, high quality, smaller than WAV
    ".mp3",    # Most common audio format, good compression
    ".ogg"     # Open source audio format, good quality
]

# AUDIO_EXTS: A set of ALL audio file extensions we recognize
# This combines our preferred formats with additional audio formats
# We use set() to convert the list to a set, then add more extensions
AUDIO_EXTS = set(AUDIO_PREFS + [
    ".aiff",   # Apple's uncompressed audio format
    ".aac",    # Advanced Audio Codec, good quality
    ".m4a",    # iTunes audio format
    ".wma",    # Windows Media Audio format
    ".oga"     # Ogg audio format (alternative extension)
])

# ============================================================================
# EXTERNAL TOOL NAMES
# ============================================================================
# These are the names of external programs our script needs to run
# The script will check if these are installed on your system

IA_BIN = "ia"        # The Internet Archive command-line tool
ARIA2_BIN = "aria2c" # The aria2 download manager tool

# ============================================================================
# PERFORMANCE SETTINGS (DEFAULTS)
# ============================================================================
# These control how fast and how many downloads happen at once
# Higher numbers = faster but more server load
# Lower numbers = slower but more polite to servers

DEFAULT_WORKERS = 4          # How many items to process at the same time
                             # Each "worker" downloads one item at a time
                             # 4 is a good balance between speed and politeness

DEFAULT_ARIA_X = 8           # How many connections aria2 makes to the server
                             # More connections = faster download but more server load
                             # 8 is a reasonable default

DEFAULT_ARIA_S = 8           # How many pieces aria2 splits each file into
                             # More pieces = faster download but more complexity
                             # 8 pieces is a good balance

# START_JITTER_SEC: Random delay before starting each download
# This prevents all downloads from starting at exactly the same time
# (0.05, 0.25) means wait between 0.05 and 0.25 seconds randomly
# This is very polite to servers and prevents overwhelming them
START_JITTER_SEC = (0.05, 0.25)

# ============================================================================
# ARIA2 DOWNLOADER SETTINGS
# ============================================================================
# These are the base command-line arguments we always give to aria2
# aria2 is the tool that actually downloads the files

ARIA2_BASE = [
    "--continue=true",           # If download was interrupted, continue from where it left off
    "--auto-file-renaming=false", # Don't rename files if there's a conflict
    "--file-allocation=none",     # Don't pre-allocate disk space (faster)
    "--summary-interval=0",       # Don't show progress summary (we handle our own output)
    "--min-split-size=1M",       # Don't split files smaller than 1 megabyte
]
