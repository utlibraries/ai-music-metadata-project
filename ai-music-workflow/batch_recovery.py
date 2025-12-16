#!/usr/bin/env python3
"""
Batch Recovery Utility

This script helps manage and recover interrupted OpenAI batch jobs.
Use this when a batch processing job was interrupted (e.g., power outage, computer shutdown).

Usage:
    python batch_recovery.py list           # List all active batches
    python batch_recovery.py resume <ID>    # Resume a specific batch
    python batch_recovery.py cleanup        # Clean up completed batches from state
"""

import sys
import os
import argparse
from datetime import datetime

# Add the script directory to path for imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_batch_processor(workflow_type='cd'):
    """Get the appropriate BatchProcessor for the workflow type."""
    if workflow_type == 'cd':
        workflow_dir = os.path.join(SCRIPT_DIR, 'cd-processing')
    else:
        workflow_dir = os.path.join(SCRIPT_DIR, 'lp-processing')

    if workflow_dir not in sys.path:
        sys.path.insert(0, workflow_dir)

    # Import after adding to path (type: ignore to suppress linting warnings)
    from batch_processor import BatchProcessor  # type: ignore
    return BatchProcessor()

def list_batches(workflow_type='cd'):
    """List all active batches."""
    bp = get_batch_processor(workflow_type)
    active_batches = bp.list_active_batches()

    if not active_batches:
        print("No active batches found.")
        return

    print(f"\n{'='*80}")
    print(f"ACTIVE BATCHES ({workflow_type.upper()} workflow)")
    print(f"{'='*80}\n")

    for batch in active_batches:
        print(f"Batch ID: {batch['batch_id']}")
        print(f"  Status: {batch['status']}")
        print(f"  Description: {batch['description']}")
        print(f"  Request Count: {batch['request_count']}")

        created_at = batch.get('created_at')
        if created_at:
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at)
            print(f"  Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")

        print()

def resume_batch(batch_id, workflow_type='cd'):
    """Resume an interrupted batch."""
    bp = get_batch_processor(workflow_type)

    print(f"\n{'='*80}")
    print(f"RESUMING BATCH: {batch_id}")
    print(f"{'='*80}\n")

    results = bp.resume_batch(batch_id, max_wait_hours=24, check_interval_minutes=5)

    if results:
        print(f"\n{'='*80}")
        print(f"BATCH COMPLETED SUCCESSFULLY")
        print(f"{'='*80}")
        print(f"Retrieved {len(results)} results")
        print(f"\nNote: Results have been downloaded. You may need to re-run your workflow script")
        print(f"to properly process and save these results to your spreadsheet.")
    else:
        print(f"\n{'='*80}")
        print(f"BATCH RECOVERY FAILED")
        print(f"{'='*80}")
        print(f"The batch could not be recovered. Check the error messages above.")

def cleanup_batches(workflow_type='cd'):
    """Clean up completed batches from state."""
    bp = get_batch_processor(workflow_type)

    print(f"\n{'='*80}")
    print(f"CLEANING UP COMPLETED BATCHES")
    print(f"{'='*80}\n")

    bp.cleanup_completed_batches()

    print(f"\nCleanup complete!")

def main():
    parser = argparse.ArgumentParser(
        description='Manage and recover interrupted OpenAI batch jobs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # List all active CD batches
  python batch_recovery.py list

  # List all active LP batches
  python batch_recovery.py list --type lp

  # Resume a specific batch
  python batch_recovery.py resume batch_abc123xyz456

  # Clean up completed batches
  python batch_recovery.py cleanup
        '''
    )

    parser.add_argument(
        'command',
        choices=['list', 'resume', 'cleanup'],
        help='Command to execute'
    )

    parser.add_argument(
        'batch_id',
        nargs='?',
        help='Batch ID to resume (required for resume command)'
    )

    parser.add_argument(
        '--type',
        choices=['cd', 'lp'],
        default='cd',
        help='Workflow type: cd or lp (default: cd)'
    )

    args = parser.parse_args()

    try:
        if args.command == 'list':
            list_batches(args.type)

        elif args.command == 'resume':
            if not args.batch_id:
                parser.error("resume command requires a batch_id argument")
            resume_batch(args.batch_id, args.type)

        elif args.command == 'cleanup':
            cleanup_batches(args.type)

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
