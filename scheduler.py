#!/usr/bin/env python3
"""
CONCURRENCY SCHEDULER MODULE
============================

This module manages the concurrent processing of multiple Internet Archive items.
It controls how many items are processed at the same time to balance speed
with being polite to the servers.

Think of this as the "traffic controller" that ensures we don't overwhelm
the Internet Archive servers while still processing items efficiently.
"""

# ============================================================================
# IMPORTS
# ============================================================================

import asyncio
from typing import List
from pathlib import Path

# Import our worker function
from worker import process_identifier

# ============================================================================
# FIXED CONCURRENCY SCHEDULER
# ============================================================================

async def schedule_fixed(identifiers: List[str], out_root: Path, log_writer, media_mode: str,
                         workers: int, aria_x: int, aria_s: int):
    """
    Process multiple items with a fixed number of concurrent workers.
    
    Args:
        identifiers (List[str]): List of item identifiers to process
        out_root (Path): Root directory for downloads
        log_writer: CSV writer for logging results
        media_mode (str): "video", "audio", or "both"
        workers (int): Number of concurrent workers (items to process at once)
        aria_x (int): aria2 connections per server
        aria_s (int): aria2 splits
    
    This function:
    1. Creates a semaphore to limit concurrent workers
    2. Starts the initial batch of workers
    3. As each worker completes, starts a new one
    4. Continues until all items are processed
    5. Shows progress updates
    
    The semaphore ensures we never have more than 'workers' items
    being processed at the same time, which helps us be polite to servers.
    """
    
    # Create a semaphore to limit concurrent workers
    # A semaphore is like a pool of permits - only 'workers' permits available
    # Each worker needs a permit to start, and returns it when done
    sem = asyncio.Semaphore(max(1, workers))
    
    # Track progress and statistics
    total = len(identifiers)
    done_cnt = 0
    disk_space_skips = 0  # Track how many items were skipped due to low disk space
    disk_full = False  # Flag to stop starting new downloads when disk is full
    
    # Define the worker function that uses the semaphore
    async def one(iid: str):
        """
        Process one item with semaphore control.
        
        Args:
            iid (str): The item identifier to process
        
        Returns:
            The result from process_identifier
        """
        # Acquire a permit from the semaphore (blocks if none available)
        async with sem:
            # Process the item and return the result
            return await process_identifier(iid, out_root, log_writer, media_mode, aria_x, aria_s)
    
    # Step 1: Start the initial batch of workers
    # We start up to 'workers' tasks immediately
    running = set()  # Set to track running tasks
    it = iter(identifiers)  # Iterator over the identifiers
    
    # Start the first batch of workers
    for _ in range(min(workers, total)):
        try:
            # Get the next identifier and create a task for it
            next_id = next(it)
            task = asyncio.create_task(one(next_id))
            running.add(task)
        except StopIteration:
            # No more identifiers to process
            break
    
    # Step 2: Process tasks as they complete
    while running:
        # Wait for at least one task to complete
        # asyncio.wait returns (done_tasks, pending_tasks)
        done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        
        # Process completed tasks
        for task in done:
            # Get the result (this will raise any exceptions that occurred)
            result = task.result()
            done_cnt += 1
            
            # Track disk space skips
            if result.get("status") == "skip" and result.get("reason") == "insufficient_disk_space":
                disk_space_skips += 1
                if not disk_full:
                    # First disk space error - stop starting new downloads
                    disk_full = True
                    print(f"[stop] Disk space insufficient - stopping new downloads")
                    print(f"[info] Current downloads will continue to completion")
            
            # Show progress
            print(f"[prog] {done_cnt}/{total} complete")
            
            # Try to start a new task ONLY if disk is not full and there are more identifiers
            if not disk_full:
                try:
                    next_id = next(it)
                    new_task = asyncio.create_task(one(next_id))
                    running.add(new_task)
                except StopIteration:
                    # No more identifiers to process
                    pass
    
    # All tasks completed (or stopped early due to disk space)
    if disk_full:
        print(f"[done] Processing stopped early after {done_cnt}/{total} items")
        print(f"[summary] {disk_space_skips} items skipped due to insufficient disk space")
        print(f"[final] Cannot continue - disk is full. Free up space before running again.")
    else:
        print(f"[done] All {total} items processed")
        # Show disk space summary if any items were skipped
        if disk_space_skips > 0:
            print(f"[summary] {disk_space_skips} items skipped due to insufficient disk space")
