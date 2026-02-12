"""
Alma API Utilities for Holdings Verification

Provides functions to verify if OCLC numbers exist in Alma,
used to replace unreliable OCLC holdings data.

Key function:
    check_oclc_in_alma(oclc_number) -> (exists: bool, mms_id: str | None)

Environment Variables:
    ALMA_SANDBOX_API_KEY - Required for API authentication
    ALMA_REGION - Optional, defaults to "api-na" (North America)
"""

import os
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Tuple, Dict, Any


class AlmaRateLimiter:
    """Rate limiter to stay within Alma API limits (20 req/sec)."""

    def __init__(self, max_requests_per_second: int = 20):
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0.0

    def wait_if_needed(self):
        """Wait if necessary to avoid exceeding rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


# Global rate limiter instance
_rate_limiter = AlmaRateLimiter()


def get_alma_config() -> Dict[str, str]:
    """
    Load Alma API configuration from environment variables.

    Returns:
        Dict with 'api_key', 'region', and 'base_url'

    Raises:
        ValueError if ALMA_SANDBOX_API_KEY is not set
    """
    api_key = os.environ.get("ALMA_SANDBOX_API_KEY")
    if not api_key:
        raise ValueError("ALMA_SANDBOX_API_KEY environment variable is required")

    region = os.environ.get("ALMA_REGION", "api-na")
    base_url = f"https://{region}.hosted.exlibrisgroup.com/almaws/v1"

    return {
        "api_key": api_key,
        "region": region,
        "base_url": base_url
    }


def get_alma_headers(api_key: str) -> Dict[str, str]:
    """Return standard headers for Alma XML API."""
    return {
        "Authorization": f"apikey {api_key}",
        "Accept": "application/xml",
        "Content-Type": "application/xml"
    }


def _alma_request_with_retry(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    max_retries: int = 3,
    timeout: int = 60
) -> Optional[requests.Response]:
    """
    Make Alma API request with exponential backoff retry.

    Args:
        url: API endpoint URL
        headers: Request headers
        params: Query parameters
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds

    Returns:
        Response object if successful, None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            _rate_limiter.wait_if_needed()
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:  # Rate limited
                wait_time = 30 * (2 ** attempt)
                print(f"Alma API rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            # Return response for caller to handle status codes
            return response

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)
                print(f"Alma API timeout, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            print(f"Alma API timeout after {max_retries} attempts")
            return None

        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)
                print(f"Alma API connection error, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            print(f"Alma API connection error after {max_retries} attempts")
            return None

    return None


def check_oclc_in_alma(oclc_number: str) -> Tuple[bool, Optional[str]]:
    """
    Check if an OCLC number exists in Alma.

    Searches Alma using the other_system_id field to find bibliographic
    records with the given OCLC number.

    Args:
        oclc_number: OCLC number to search for (with or without prefix)

    Returns:
        Tuple of (exists: bool, mms_id: Optional[str])
        - exists: True if the OCLC number was found in Alma
        - mms_id: The MMS ID of the found record, or None if not found
    """
    try:
        config = get_alma_config()
    except ValueError as e:
        print(f"Alma API not configured: {e}")
        return False, None

    url = f"{config['base_url']}/bibs"
    headers = get_alma_headers(config['api_key'])

    # Clean OCLC number - remove prefix if present
    oclc_num = oclc_number.replace("(OCoLC)", "").strip()

    # Try multiple search formats (some records use prefix, some don't)
    search_formats = [
        f"(OCoLC){oclc_num}",
        oclc_num
    ]

    for search_term in search_formats:
        params = {
            "other_system_id": search_term,
            "limit": "1"
        }

        response = _alma_request_with_retry(url, headers, params)

        if response is None:
            # API error - can't determine, return False
            continue

        try:
            if response.status_code == 400:
                # Bad request - try next format
                continue

            response.raise_for_status()

            root = ET.fromstring(response.text)
            total_records = root.find('total_record_count')

            if total_records is not None and int(total_records.text) > 0:
                bib = root.find('.//bib')
                if bib is not None:
                    mms_id = bib.find('mms_id')
                    if mms_id is not None:
                        return True, mms_id.text

        except requests.exceptions.HTTPError as e:
            print(f"Alma API error checking OCLC {oclc_number}: {e}")
            continue
        except ET.ParseError as e:
            print(f"Error parsing Alma response for OCLC {oclc_number}: {e}")
            continue

    return False, None


def verify_holdings_in_alma(oclc_number: str) -> Dict[str, Any]:
    """
    Verify if institution holds this OCLC number in Alma.

    This is a higher-level function that returns a structured result
    suitable for storing in the workflow JSON.

    Args:
        oclc_number: OCLC number to verify

    Returns:
        Dict with verification results:
        {
            "oclc_number_checked": str,
            "alma_verified": bool,
            "mms_id": Optional[str],
            "verified_at": str (ISO timestamp),
            "verification_source": "alma"
        }
    """
    exists, mms_id = check_oclc_in_alma(oclc_number)

    return {
        "oclc_number_checked": oclc_number,
        "alma_verified": exists,
        "mms_id": mms_id,
        "verified_at": datetime.now().isoformat(),
        "verification_source": "alma"
    }
