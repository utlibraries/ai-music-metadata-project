"""
Simple retry utility for workflow steps.
Provides retry logic with exponential backoff and failure logging.
"""

import time
from typing import Callable, Tuple, Any, Optional


def retry_api_call(
    func: Callable,
    *args,
    max_retries: int = 3,
    base_wait: int = 30,
    barcode: str = "unknown",
    **kwargs
) -> Tuple[bool, Any, Optional[str]]:
    """
    Execute a function with automatic retry and exponential backoff.

    Args:
        func: Function to execute
        *args: Positional arguments for function
        max_retries: Maximum number of retry attempts (default: 3)
        base_wait: Base wait time in seconds, doubles each retry (default: 30s)
        barcode: Barcode for logging (default: "unknown")
        **kwargs: Keyword arguments for function

    Returns:
        Tuple of (success: bool, result: Any, error_message: Optional[str])

    Example:
        success, response, error = retry_api_call(
            client.chat.completions.create,
            model="gpt-4o",
            messages=[...],
            barcode="123456"
        )
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)

            if attempt > 0:
                print(f"   ✓ Succeeded on retry attempt {attempt + 1}")

            return (True, result, None)

        except Exception as e:
            last_error = str(e)
            error_type = type(e).__name__

            if attempt < max_retries - 1:
                wait_time = base_wait * (2 ** attempt)
                print(f"   ✗ Attempt {attempt + 1} failed ({error_type}): {last_error[:100]}")
                print(f"   ⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ✗ All {max_retries} attempts failed for barcode {barcode}")
                print(f"   Final error: {last_error[:200]}")

    return (False, None, last_error)


def log_failure(barcode: str, step: str, error: str, notes: str = ""):
    """
    Simple console logging for failures.
    Can be extended later if needed.

    Args:
        barcode: Item barcode
        step: Step name (e.g., 'step1', 'step3')
        error: Error message
        notes: Additional notes
    """
    print(f"\n{'='*60}")
    print(f"FAILURE LOGGED - {step.upper()}")
    print(f"{'='*60}")
    print(f"Barcode: {barcode}")
    print(f"Error: {error[:300]}")
    if notes:
        print(f"Notes: {notes}")
    print(f"{'='*60}\n")
