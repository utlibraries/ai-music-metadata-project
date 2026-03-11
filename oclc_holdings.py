"""
OCLC Holdings Management Script

WHAT IT DOES:
  - Reads a list of OCLC numbers (format: no:XXXXXXX or plain numbers)
  - Sets or unsets your institution's holdings in OCLC WorldCat
  - Writes a CSV report to AI_Music_Operations/oclc-holdings/lp/ or /cd/
  - Auto-refreshes OCLC token every 15 minutes

USAGE:
  py oclc_holdings.py path/to/oclc-numbers.txt --action set --format lp [--yes] [--report]
  py oclc_holdings.py path/to/oclc-numbers.txt --action unset --format cd [--yes] [--report]

  --action   'set' to add holdings, 'unset' to remove holdings (required)
  --format   'lp' or 'cd' — determines output subfolder (required)
  --report   Preview only, no changes made
  --yes      Skip confirmation prompt

REQUIRED ENVIRONMENT VARIABLES:
  OCLC_CLIENT_ID                Your OCLC API client ID
  OCLC_SECRET                   Your OCLC API client secret
  OCLC_INSTITUTION_SYMBOL       Your institution's OCLC symbol
  AI_MUSIC_OPERATIONS_DIR       Path to your local operations output folder
                                e.g. C:\Users\you\Documents\AI_Music_Operations

HOW TO SET ENVIRONMENT VARIABLES (PowerShell):
  $env:OCLC_CLIENT_ID = "your_client_id"
  $env:OCLC_SECRET = "your_secret"
  $env:OCLC_INSTITUTION_SYMBOL = "your_institution_symbol"
  $env:AI_MUSIC_OPERATIONS_DIR = "C:\Users\you\Documents\AI_Music_Operations"
"""

import os
import requests
import csv
import time
import argparse
from datetime import datetime


# ====== CONFIG ======
def get_required_env(var):
    """Get required environment variable or raise a clear error."""
    val = os.environ.get(var)
    if not val:
        raise SystemExit(f"Error: {var} environment variable is required but not set.\n"
                         f"Set it in PowerShell: $env:{var} = 'your_value'")
    return val

client_id     = get_required_env("OCLC_CLIENT_ID")
client_secret = get_required_env("OCLC_SECRET")
INSTITUTION   = get_required_env("OCLC_INSTITUTION_SYMBOL")
OPERATIONS_DIR = get_required_env("AI_MUSIC_OPERATIONS_DIR")

METADATA_API  = "https://metadata.api.oclc.org/worldcat"


# ====== AUTH ======
def get_token():
    """Authenticate with OCLC and return an access token."""
    r = requests.post(
        "https://oauth.oclc.org/token",
        data={"grant_type": "client_credentials", "scope": "WorldCatMetadataAPI"},
        auth=(client_id, client_secret),
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ====== HOLDINGS FUNCTIONS ======
def set_holding(oclc_num, token):
    """Set institution holding for an OCLC number."""
    url = f"{METADATA_API}/manage/institution/holdings/{oclc_num}/set"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    r = requests.post(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def unset_holding(oclc_num, token):
    """Unset institution holding for an OCLC number."""
    url = f"{METADATA_API}/manage/institution/holdings/{oclc_num}/unset"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    r = requests.post(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


# ====== HELPERS ======
def load_oclc_numbers(filepath):
    """Load OCLC numbers from file. Supports 'no:XXXXXXX' and plain number formats."""
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = [l.strip() for l in f if l.strip()]
    numbers = []
    for line in lines:
        num = line.replace('no:', '').strip()
        if num:
            numbers.append(num)
    return numbers


def get_output_path(fmt, action, total):
    """Build output CSV path under AI_MUSIC_OPERATIONS_DIR."""
    subfolder = os.path.join(OPERATIONS_DIR, "oclc-holdings", fmt)
    os.makedirs(subfolder, exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')
    filename = f"{date_str}_{fmt.upper()}-{total}-holdings-{action}.csv"
    return os.path.join(subfolder, filename)


# ====== MAIN ======
def main():
    parser = argparse.ArgumentParser(description='Set or unset OCLC WorldCat holdings')
    parser.add_argument('input_file', help='Path to OCLC numbers file (no:XXXXXXX format)')
    parser.add_argument('--action', required=True, choices=['set', 'unset'],
                        help='set = add holdings, unset = remove holdings')
    parser.add_argument('--format', required=True, choices=['lp', 'cd'],
                        help='lp or cd — determines output subfolder')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--report', action='store_true', help='Preview only, no changes made')
    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        raise SystemExit(f"Error: File not found: {args.input_file}")

    oclc_numbers = load_oclc_numbers(args.input_file)
    total = len(oclc_numbers)

    print(f"\n{'='*50}")
    print(f"OCLC Holdings Management")
    print(f"{'='*50}")
    print(f"Institution  : {INSTITUTION}")
    print(f"Format       : {args.format.upper()}")
    print(f"Action       : {args.action.upper()} holdings")
    print(f"Total records: {total}")
    print(f"Input file   : {args.input_file}")
    print(f"\nSample (first 5):")
    for n in oclc_numbers[:5]:
        print(f"  no:{n}")

    if args.report:
        print(f"\n--report flag set. No changes will be made.")
        print(f"This would {args.action} holdings for {total} OCLC records.")
        csv_path = get_output_path(args.format, args.action, total)
        print(f"Output would be saved to: {csv_path}")
        return

    if not args.yes:
        resp = input(f"\nProceed to {args.action.upper()} holdings for {total} records in OCLC? Type 'yes' to continue: ").strip().lower()
        if resp != 'yes':
            raise SystemExit("Aborted by user.")

    # Authenticate
    print(f"\nAuthenticating with OCLC...")
    token = get_token()
    token_time = time.time()
    print(f"Authentication successful\n")

    results = []
    success, failed = 0, 0

    for i, oclc_num in enumerate(oclc_numbers, 1):
        # Refresh token every 15 minutes
        if time.time() - token_time > 900:
            print("  Refreshing OCLC token...")
            try:
                token = get_token()
                token_time = time.time()
                print("  Token refreshed successfully")
            except Exception as e:
                print(f"  WARNING: Token refresh failed: {e}")

        print(f"[{i}/{total}] OCLC #{oclc_num}", end=" ... ")

        try:
            if args.action == 'set':
                response = set_holding(oclc_num, token)
            else:
                response = unset_holding(oclc_num, token)

            print(f"OK — {response.get('message', 'Success')}")
            results.append({
                'oclc': oclc_num,
                'action': args.action,
                'status': 'success',
                'response': response.get('message', str(response))
            })
            success += 1

        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:100]}"
            print(f"FAILED: {error_msg}")
            results.append({
                'oclc': oclc_num,
                'action': args.action,
                'status': 'error',
                'response': error_msg
            })
            failed += 1

        except Exception as e:
            print(f"FAILED: {e}")
            results.append({
                'oclc': oclc_num,
                'action': args.action,
                'status': 'error',
                'response': str(e)
            })
            failed += 1

        time.sleep(0.3)

    # Write CSV report to AI_Music_Operations folder
    csv_path = get_output_path(args.format, args.action, total)
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['oclc', 'action', 'status', 'response'])
        w.writeheader()
        w.writerows(results)

    print(f"\n{'='*50}")
    print(f"DONE")
    print(f"{'='*50}")
    print(f"Successfully {args.action}: {success}")
    print(f"Failed                  : {failed}")
    print(f"Report saved to         : {csv_path}")


if __name__ == "__main__":
    main()