"""
Step 3c: Create New OCLC Records for Approved Original Cataloging Items

Reads the cataloger decisions CSV from Step 3b review.
For each Approved record:
  1. Checks OCLC for any existing record (dedup — HIGHLY PROHIBITED to duplicate)
  2. Creates a new bibliographic record in OCLC WorldCat via Metadata API
  3. Receives the assigned OCLC number
  4. Sets IXA holdings on the new OCLC number
  5. Saves the OCLC number back to workflow JSON for Step 3d

For Rejected/Hold records: logs and skips. No Alma interaction here.

Reuses: find_latest_results_folder, get_workflow_json_path, load_workflow_json,
        save_workflow_json, log_error, get_current_timestamp, get_file_path_config
"""

import os
import json
import csv
import time
import datetime
import requests
import xml.etree.ElementTree as ET
import argparse

from shared_utilities import find_latest_results_folder, get_workflow_json_path
from cd_workflow_config import get_file_path_config, get_current_timestamp
from json_workflow import load_workflow_json, save_workflow_json, log_error

METADATA_API = "https://metadata.api.oclc.org/worldcat"


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_metadata_token():
    """Get OCLC Metadata API token (WorldCatMetadataAPI scope)."""
    r = requests.post(
        "https://oauth.oclc.org/token",
        data={"grant_type": "client_credentials", "scope": "WorldCatMetadataAPI"},
        auth=(os.getenv("OCLC_CLIENT_ID"), os.getenv("OCLC_SECRET")),
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]


def get_search_token():
    """Get OCLC Discovery API token (wcapi scope) for duplicate checking."""
    r = requests.post(
        "https://oauth.oclc.org/token",
        data={"grant_type": "client_credentials", "scope": "wcapi"},
        auth=(os.getenv("OCLC_CLIENT_ID"), os.getenv("OCLC_SECRET")),
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ── Dedup check ───────────────────────────────────────────────────────────────

def check_oclc_for_existing(marc_fields, search_token):
    """
    DEDUPLICATION: Search OCLC WorldCat before creating a new record.
    Returns existing OCLC number if found, None if clear to create.

    Checks by:
    1. UPC (field_028 if it contains a 12-digit number)
    2. Title + contributor exact phrase search
    """
    base = "https://americas.discovery.api.oclc.org/worldcat/search/v2/bibs"
    headers = {"Authorization": f"Bearer {search_token}", "Accept": "application/json"}

    # Extract title for search
    title_raw = marc_fields.get("field_245", "") or ""
    # Strip MARC subfield codes for searching
    title_clean = title_raw.replace("$b", "").replace("$c", "").replace("$a", "").strip()
    title_clean = title_clean.rstrip("./,;").strip()

    contributor = ""
    for key in ("field_100", "field_110"):
        val = marc_fields.get(key, "") or ""
        if val:
            # Strip relator ($e ...) for search
            contributor = val.split("$e")[0].strip().rstrip(",").strip()
            break

    if not title_clean:
        return None  # Can't check without a title

    # Build search queries
    queries = []
    if title_clean and contributor:
        queries.append(f'"{title_clean}" "{contributor}"')
    if title_clean:
        queries.append(f'"{title_clean}"')

    # Also check UPC if present in field_028
    field_028 = marc_fields.get("field_028", "") or ""
    import re
    upc_match = re.search(r'\b(\d{12,13})\b', field_028)
    if upc_match:
        queries.insert(0, upc_match.group(1))  # UPC is highest priority

    for q in queries[:3]:  # Max 3 queries
        try:
            params = {"q": q, "limit": 5, "itemType": "music", "itemSubType": "music-cd"}
            r = requests.get(base, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("numberOfRecords", 0) > 0:
                for rec in data.get("bibRecords", []):
                    oclc_num = (rec.get("identifier") or {}).get("oclcNumber")
                    if oclc_num:
                        print(f"    DEDUP: Found existing OCLC record {oclc_num} for query: {q[:50]}")
                        return oclc_num
            time.sleep(0.3)
        except Exception as e:
            print(f"    Dedup search warning: {e}")
            continue

    return None  # No existing record found — safe to create


# ── Build MARCXML ─────────────────────────────────────────────────────────────

def build_marcxml_from_generated_fields(marc_fields, cataloging_institution):
    """
    Convert AI-generated MARC fields dict to MARCXML for OCLC submission.
    Builds a complete, valid MARC record.
    """
    ns = "http://www.loc.gov/MARC21/slim"
    root = ET.Element(f"{{{ns}}}record")
    root.set("xmlns", ns)

    def cf(tag, text):
        if text:
            el = ET.SubElement(root, "controlfield")
            el.set("tag", tag)
            el.text = str(text)

    def df(tag, ind1=" ", ind2=" "):
        el = ET.SubElement(root, "datafield")
        el.set("tag", tag); el.set("ind1", ind1); el.set("ind2", ind2)
        return el

    def sf(df_el, code, text):
        if text is not None and str(text).strip():
            s = ET.SubElement(df_el, "subfield")
            s.set("code", code)
            s.text = str(text)

    def parse_subfields(value_str, df_el):
        """Parse a string with $a, $b, $c etc into proper subfields."""
        if not value_str:
            return
        import re
        # Split on subfield delimiters — $a, $b, etc.
        parts = re.split(r'\s*\$([a-z2])\s*', str(value_str).strip())
        if len(parts) == 1:
            # No subfield codes — put everything in $a
            sf(df_el, "a", parts[0].strip())
        else:
            # First part before any $ goes to $a if non-empty
            if parts[0].strip():
                sf(df_el, "a", parts[0].strip())
            # Remaining pairs: code, value, code, value...
            for i in range(1, len(parts) - 1, 2):
                code = parts[i]
                val = parts[i + 1].strip() if i + 1 < len(parts) else ""
                if val:
                    sf(df_el, code, val)

    # Leader
    leader = ET.SubElement(root, "leader")
    leader.text = marc_fields.get("leader", "00000njm a2200000 i 4500")

    # 003 - OCoLC
    cf("003", "OCoLC")

    # 005 - Date/time of latest transaction
    cf("005", datetime.datetime.now().strftime("%Y%m%d%H%M%S.0"))

    # 007 - Physical description fixed
    cf("007", marc_fields.get("field_007", "sd fsngnnmmned"))

    # 008 - Fixed length — extract year from field_264
    pub_year = "    "
    field_264 = marc_fields.get("field_264", "") or ""
    import re
    year_match = re.search(r'\b(19|20)\d{2}\b', field_264)
    if year_match:
        pub_year = year_match.group(0)
    today6 = datetime.datetime.now().strftime("%y%m%d")
    cf("008", f"{today6}s{pub_year}    xxu                 eng d")

    # 040 - Cataloging source
    oclc_symbol = os.environ.get("OCLC_INSTITUTION_SYMBOL", "IXA")
    f040 = df("040")
    sf(f040, "a", oclc_symbol)
    sf(f040, "b", "eng")
    sf(f040, "e", "rda")
    sf(f040, "c", oclc_symbol)
    field_028 = marc_fields.get("field_028")
    if field_028:
        f028 = df("028", "0", "2")
        parts = str(field_028).split("$b")
        sf(f028, "a", parts[0].strip())
        if len(parts) > 1:
            sf(f028, "b", parts[1].strip())

    # 100 - Personal name main entry
    field_100 = marc_fields.get("field_100")
    if field_100:
        f100 = df("100", "1", " ")
        parse_subfields(field_100, f100)

    # 110 - Corporate/group main entry (only if no 100)
    elif marc_fields.get("field_110"):
        f110 = df("110", "2", " ")
        parse_subfields(marc_fields["field_110"], f110)

    # 245 - Title statement
    field_245 = marc_fields.get("field_245")
    if field_245:
        ind1 = "1" if (marc_fields.get("field_100") or marc_fields.get("field_110")) else "0"
        f245 = df("245", ind1, "0")
        parse_subfields(field_245, f245)

    # 246 - Variant title
    field_246 = marc_fields.get("field_246")
    if field_246:
        f246 = df("246", "1", " ")
        sf(f246, "a", str(field_246).strip())

    # 264 - Production/publication
    field_264 = marc_fields.get("field_264")
    if field_264:
        f264 = df("264", " ", "1")
        parse_subfields(field_264, f264)

    # 300 - Physical description
    f300 = df("300")
    phys = marc_fields.get("field_300", "1 audio disc : $b digital ; $c 4 3/4 in.")
    parse_subfields(phys, f300)

    # 336/337/338 - RDA triplet
    for tag, a_val, b_val in [
        ("336", "performed music", "prm"),
        ("337", "audio", "s"),
        ("338", "audio disc", "sd")
    ]:
        f = df(tag)
        sf(f, "a", a_val); sf(f, "b", b_val); sf(f, "2", "rdacontent" if tag == "336" else ("rdamedia" if tag == "337" else "rdacarrier"))

    # 500 notes
    for key in ("field_500_general", "field_500_kut", "field_500_ai"):
        val = marc_fields.get(key)
        if val:
            f500 = df("500")
            sf(f500, "a", str(val).strip())

    # 505 - Contents
    field_505 = marc_fields.get("field_505")
    if field_505:
        f505 = df("505", "0", " ")
        sf(f505, "a", str(field_505).strip())

    # 518 - Recording info
    field_518 = marc_fields.get("field_518")
    if field_518:
        f518 = df("518")
        sf(f518, "a", str(field_518).strip())

    # 588 - Source of description
    field_588 = marc_fields.get("field_588")
    if field_588:
        f588 = df("588")
        sf(f588, "a", str(field_588).strip())

    # 650 - Subjects
    for subj in (marc_fields.get("field_650") or []):
        if subj:
            f650 = df("650", " ", "0")
            parse_subfields(str(subj), f650)

    # 700 - Added entries
    for ae in (marc_fields.get("field_700") or []):
        if ae:
            f700 = df("700", "1", " ")
            parse_subfields(str(ae), f700)

    return ET.tostring(root, encoding="unicode")


# ── Create OCLC record ────────────────────────────────────────────────────────

def create_oclc_record(marcxml, token):
    """
    POST new bibliographic record to OCLC Metadata API.
    Returns the assigned OCLC number.
    """
    url = f"{METADATA_API}/manage/bibs"
    headers = {
        "Authorization": f"Bearer {token}",
        "content-type": "application/marcxml+xml",
        "Accept": "application/marcxml+xml"
    }

    r = requests.post(url, headers=headers, data=marcxml.encode("utf-8"), timeout=60)
    r.raise_for_status()

    # Parse response to get assigned OCLC number
    root = ET.fromstring(r.text)
    ns = {"marc": "http://www.loc.gov/MARC21/slim"}

    # Look for 001 control field (OCLC number)
    for cf in root.findall(".//marc:controlfield[@tag='001']", ns):
        return cf.text.strip()

    # Fallback: look in 035 field
    for df_el in root.findall(".//marc:datafield[@tag='035']", ns):
        for sf_el in df_el.findall("marc:subfield[@code='a']", ns):
            val = sf_el.text or ""
            clean = val.replace("(OCoLC)", "").strip()
            if clean:
                return clean

    raise ValueError(f"Could not extract OCLC number from response: {r.text[:200]}")


def set_oclc_holding(oclc_number, token):
    """Set IXA holding on newly created OCLC record."""
    url = f"{METADATA_API}/manage/institution/holdings/{oclc_number}/set"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = requests.post(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Load approved decisions ───────────────────────────────────────────────────

def load_approved_decisions(csv_path):
    """
    Load approved barcodes from either:
    1. Cataloger decisions CSV from Step 3b HTML review (has Barcode/Cataloger Decision columns)
    2. Batch-ready file from Step 3b auto-approval (format: PENDING|barcode|title)
    Returns list of approved barcodes and decisions dict.
    """
    approved = []
    all_decisions = {}

    # Detect file type by extension and content
    is_batch_ready = csv_path.endswith('.txt')

    if is_batch_ready:
        # Batch-ready file: PENDING|barcode|title
        with open(csv_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or '|' not in line:
                    continue
                parts = line.split('|', 2)
                if len(parts) >= 2:
                    barcode = parts[1].strip()
                    title = parts[2].strip() if len(parts) > 2 else ''
                    approved.append(barcode)
                    all_decisions[barcode] = {
                        "decision": "Approved",
                        "notes": "Auto-approved via batch-ready file",
                        "cataloger": "AI-pipeline",
                        "review_date": __import__('datetime').datetime.now().strftime('%Y-%m-%d')
                    }
        print(f"Batch-ready file loaded: {len(approved)} auto-approved records")
    else:
        # Cataloger decisions CSV from HTML review
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                barcode = str(row.get("Barcode", "")).strip()
                decision = str(row.get("Cataloger Decision", "")).strip()
                notes = str(row.get("Notes", "")).strip()
                all_decisions[barcode] = {"decision": decision, "notes": notes,
                                           "cataloger": row.get("Cataloger", ""),
                                           "review_date": row.get("Review Date", "")}
                if decision == "Approved":
                    approved.append(barcode)
        print(f"Decisions CSV loaded: {len(all_decisions)} total, {len(approved)} approved")

    return approved, all_decisions


# ── Save OCLC number to workflow JSON ─────────────────────────────────────────

def save_oclc_result_to_json(workflow_json_path, barcode, oclc_number, holding_set, decision_data):
    """Save assigned OCLC number and result back to workflow JSON."""
    data = load_workflow_json(workflow_json_path)

    if str(barcode) not in data.get("records", {}):
        return

    if "step3b_original_cataloging" not in data["records"][str(barcode)]:
        data["records"][str(barcode)]["step3b_original_cataloging"] = {}

    data["records"][str(barcode)]["step3b_original_cataloging"].update({
        "status": "oclc_created",
        "assigned_oclc_number": oclc_number,
        "holding_set_in_oclc": holding_set,
        "cataloger_decision": decision_data.get("decision"),
        "cataloger": decision_data.get("cataloger"),
        "review_date": decision_data.get("review_date"),
        "cataloger_notes": decision_data.get("notes"),
        "oclc_created_at": datetime.datetime.now().isoformat()
    })

    data["records"][str(barcode)]["updated_at"] = datetime.datetime.now().isoformat()
    save_workflow_json(workflow_json_path, data)


# ── Report ────────────────────────────────────────────────────────────────────

def create_oclc_report(results_folder, processing_results, current_ts):
    """
    Create report of OCLC record creation.
    Saves to deliverables/ and AI_Music_Operations/original-cataloging/
    """
    ops_dir = os.environ.get("AI_MUSIC_OPERATIONS_DIR")
    successful = [r for r in processing_results if r.get("oclc_created")]
    failed = [r for r in processing_results if not r.get("oclc_created")]
    skipped = [r for r in processing_results if r.get("skipped")]

    lines = [
        "=" * 65,
        "AI MUSIC METADATA PROJECT — OCLC RECORD CREATION REPORT",
        "=" * 65,
        f"Generated:        {current_ts}",
        f"Total submitted:  {len(processing_results)}",
        f"OCLC records created: {len(successful)}",
        f"Failed:           {len(failed)}",
        f"Skipped/rejected: {len(skipped)}",
        "",
        "CREATED RECORDS:",
        "-" * 40,
    ]

    for r in successful:
        lines.append(f"  {r['barcode']}  |  OCLC: {r['oclc_number']}  |  Holdings set: {'Yes' if r.get('holding_set') else 'No'}")

    if failed:
        lines += ["", "FAILED:", "-" * 40]
        for r in failed:
            lines.append(f"  {r['barcode']}  |  {r.get('error', 'Unknown error')[:60]}")

    lines += [
        "",
        "NEXT STEP: Run step_3d_original_catalog_alma_import.py",
        "=" * 65,
    ]

    report_text = "\n".join(lines)
    deliverables = os.path.join(results_folder, "deliverables")
    filename = f"oclc-record-creation-report-{current_ts}.txt"
    report_path = os.path.join(deliverables, filename)

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report_text)
    print(f"Report saved: {report_path}")

    if ops_dir:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        ops_folder = os.path.join(ops_dir, "original-cataloging", date_str)
        os.makedirs(ops_folder, exist_ok=True)
        ops_path = os.path.join(ops_folder, filename)
        with open(ops_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        print(f"Report also saved to: {ops_path}")

    return report_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Create OCLC records for approved original cataloging items")
    parser.add_argument("csv_path", help="Path to cataloger decisions CSV from Step 3b review")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    args = parser.parse_args()

    if not os.path.isfile(args.csv_path):
        raise SystemExit(f"CSV not found: {args.csv_path}")

    print("=" * 65)
    print("STEP 3c: CREATE OCLC RECORDS FOR APPROVED ORIGINAL CATALOGING")
    print("=" * 65)

    file_paths = get_file_path_config()
    current_ts = get_current_timestamp()

    results_folder = find_latest_results_folder(file_paths["results_prefix"])
    if not results_folder:
        raise SystemExit("No results folder found.")

    data_folder = os.path.join(results_folder, "data")
    workflow_json_path = get_workflow_json_path(data_folder)
    workflow_data = load_workflow_json(workflow_json_path)

    approved_barcodes, all_decisions = load_approved_decisions(args.csv_path)

    if not approved_barcodes:
        print("No approved records found in CSV. Exiting.")
        return

    cataloging_institution = os.environ.get("ALMA_CATALOGING_INSTITUTION", "IXA")

    print(f"\nApproved for OCLC submission: {len(approved_barcodes)}")
    print(f"Cataloging institution: {cataloging_institution}")
    print(f"Results folder: {results_folder}")

    if not args.yes:
        resp = input(f"\nCreate {len(approved_barcodes)} OCLC records and set IXA holdings? Type 'yes' to continue: ").strip().lower()
        if resp != 'yes':
            raise SystemExit("Aborted.")

    # Authenticate
    print("\nAuthenticating with OCLC...")
    meta_token = get_metadata_token()
    search_token = get_search_token()
    token_time = time.time()
    print("Authentication successful.")

    processing_results = []
    total = len(approved_barcodes)

    for i, barcode in enumerate(approved_barcodes, 1):
        print(f"\n[{i}/{total}] Processing barcode: {barcode}")

        # Refresh tokens every 14 minutes
        if time.time() - token_time > 840:
            print("  Refreshing OCLC tokens...")
            try:
                meta_token = get_metadata_token()
                search_token = get_search_token()
                token_time = time.time()
                print("  Tokens refreshed.")
            except Exception as e:
                print(f"  WARNING: Token refresh failed: {e}")

        record = workflow_data.get("records", {}).get(str(barcode), {})
        step3b = record.get("step3b_original_cataloging", {})
        marc_fields = step3b.get("marc_fields", {})

        if not marc_fields:
            print(f"  SKIP: No MARC fields found for {barcode}")
            processing_results.append({"barcode": barcode, "oclc_created": False,
                                        "skipped": True, "error": "No MARC fields"})
            continue

        result = {"barcode": barcode, "oclc_created": False, "skipped": False}

        try:
            # DEDUP CHECK — mandatory
            print(f"  Checking OCLC for existing records...")
            existing_oclc = check_oclc_for_existing(marc_fields, search_token)
            if existing_oclc:
                print(f"  DEDUP BLOCKED: Record already exists in OCLC as {existing_oclc}")
                print(f"  Saving existing OCLC number — will import that record instead.")
                save_oclc_result_to_json(
                    workflow_json_path, barcode, existing_oclc, False,
                    {**all_decisions.get(barcode, {}), "dedup_note": f"Existing OCLC record found: {existing_oclc}"}
                )
                result.update({"oclc_created": True, "oclc_number": existing_oclc,
                                "holding_set": False, "note": "Existing record — no new record created"})
                processing_results.append(result)
                time.sleep(0.5)
                continue

            # Build MARCXML
            print(f"  Building MARCXML...")
            marcxml = build_marcxml_from_generated_fields(marc_fields, cataloging_institution)

            # Create OCLC record
            print(f"  Submitting to OCLC Metadata API...")
            oclc_number = create_oclc_record(marcxml, meta_token)
            print(f"  OCLC record created: {oclc_number}")

            # Save to workflow JSON FIRST before attempting holdings
            # This ensures OCLC number is never lost even if holdings fails
            save_oclc_result_to_json(workflow_json_path, barcode, oclc_number, False,
                                      all_decisions.get(barcode, {}))

            # Set IXA holdings
            print(f"  Setting IXA holdings on {oclc_number}...")
            holdings_num = oclc_number.lstrip("on") if isinstance(oclc_number, str) and oclc_number.startswith("on") else oclc_number
            holding_resp = set_oclc_holding(holdings_num, meta_token)
            holding_set = True
            print(f"  Holdings set: {holding_resp.get('message', 'OK')}")

            # Update JSON with holdings status
            save_oclc_result_to_json(workflow_json_path, barcode, oclc_number, holding_set,
                                      all_decisions.get(barcode, {}))

            result.update({"oclc_created": True, "oclc_number": oclc_number, "holding_set": holding_set})

        except requests.exceptions.HTTPError as e:
            err = f"HTTP {e.response.status_code}: {e.response.text[:100]}"
            print(f"  ERROR: {err}")
            log_error(results_folder, "step3c", barcode, "oclc_api_error", err)
            result["error"] = err
        except Exception as e:
            print(f"  ERROR: {e}")
            log_error(results_folder, "step3c", barcode, "unexpected_error", str(e))
            result["error"] = str(e)

        processing_results.append(result)
        time.sleep(1.0)  # Rate limiting

    # Create report
    report_path = create_oclc_report(results_folder, processing_results, current_ts)

    created = sum(1 for r in processing_results if r.get("oclc_created"))
    failed = sum(1 for r in processing_results if not r.get("oclc_created") and not r.get("skipped"))

    print(f"\n{'='*65}")
    print(f"STEP 3c COMPLETE")
    print(f"{'='*65}")
    print(f"OCLC records created/found: {created}")
    print(f"Failed:                     {failed}")
    print(f"Report: {report_path}")
    print(f"\nNext: Run step_3d_original_catalog_alma_import.py")


if __name__ == "__main__":
    main()
