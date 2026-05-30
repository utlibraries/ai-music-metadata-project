"""
Step 3d: Import Approved Original Catalog Records into Alma

Reads workflow JSON for records with an assigned OCLC number from Step 3c.
For each record:
  1. DEDUP: Checks Alma — if OCLC number already exists, skips (HIGHLY PROHIBITED to duplicate)
  2. Builds MARCXML with full AI-generated MARC fields + assigned OCLC 035 field
  3. Creates bib record in Alma
  4. Unsuppresses bib (visible in Primo)
  5. Creates holdings record at configured library/location
  6. Creates item record with barcode, CD policy, KUT internal note
  7. Writes receipt CSV to AI_Music_Operations/original-cataloging/

Internal note on ALL original records: "KUT Collection AI Assisted Cataloging — Original Record"
This distinguishes them from copy cataloging records in Alma.

Reuses: find_latest_results_folder, get_workflow_json_path, load_workflow_json,
        save_workflow_json, log_error, get_current_timestamp, get_file_path_config
        check_if_oclc_exists_in_alma from alma_batch_upload_cd (dedup — reuse exactly)
"""

from generate_batch_report import generate_batch_report
import os
import csv
import time
import datetime
import requests
import xml.etree.ElementTree as ET
import argparse

from shared_utilities import find_latest_results_folder, get_workflow_json_path
from cd_workflow_config import get_file_path_config, get_current_timestamp
from json_workflow import load_workflow_json, save_workflow_json, log_error

# Reuse the exact same dedup function from the existing upload script
from alma_batch_upload_cd import (
    check_if_oclc_exists_in_alma,
    create_holding,
    get_holdings,
    unsuppress_bib
)


# ── Config from environment ───────────────────────────────────────────────────

def get_required_env(var):
    val = os.environ.get(var)
    if not val:
        raise SystemExit(f"Error: {var} environment variable is required but not set")
    return val


alma_api_key = get_required_env("ALMA_SANDBOX_API_KEY")
LIBRARY_CODE = get_required_env("ALMA_LIBRARY_CODE")
LOCATION_CODE = get_required_env("ALMA_LOCATION_CODE")
ITEM_POLICY_CODE = get_required_env("ALMA_CD_ITEM_POLICY")
CATALOGING_INSTITUTION = get_required_env("ALMA_CATALOGING_INSTITUTION")
ALMA_REGION = os.environ.get("ALMA_REGION", "api-na")

# Original records get a distinct internal note
ORIG_INTERNAL_NOTE = "KUT Collection AI Assisted Cataloging — Original Record"

ALMA_BASE = f"https://{ALMA_REGION}.hosted.exlibrisgroup.com/almaws/v1"
HEADERS_XML = {
    "Authorization": f"apikey {alma_api_key}",
    "Accept": "application/xml",
    "Content-Type": "application/xml"
}


# ── Build MARCXML for Alma ────────────────────────────────────────────────────

def build_alma_marcxml(marc_fields, oclc_number, barcode, cataloging_institution):
    """
    Build MARCXML for Alma import.
    Identical structure to alma_batch_upload_cd.py but sourced from
    AI-generated fields + assigned OCLC number rather than WorldCat lookup.
    """
    ns = "http://www.loc.gov/MARC21/slim"
    root = ET.Element(f"{{{ns}}}record")
    root.set("xmlns", ns)

    import re

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
        if not value_str:
            return
        parts = re.split(r'\s*\$([a-z2])\s*', str(value_str).strip())
        if len(parts) == 1:
            sf(df_el, "a", parts[0].strip())
        else:
            if parts[0].strip():
                sf(df_el, "a", parts[0].strip())
            for i in range(1, len(parts) - 1, 2):
                code = parts[i]
                val = parts[i + 1].strip() if i + 1 < len(parts) else ""
                if val:
                    sf(df_el, code, val)

    # Leader
    leader = ET.SubElement(root, "leader")
    leader.text = marc_fields.get("leader", "00000njm a2200000 i 4500")

    # 001 - OCLC number
    cf("001", oclc_number)
    cf("003", "OCoLC")
    cf("005", datetime.datetime.now().strftime("%Y%m%d%H%M%S.0"))
    cf("007", marc_fields.get("field_007", "sd fsngnnmmned"))

    # 008
    pub_year = "    "
    field_264 = marc_fields.get("field_264", "") or ""
    year_match = re.search(r'\b(19|20)\d{2}\b', field_264)
    if year_match:
        pub_year = year_match.group(0)
    today6 = datetime.datetime.now().strftime("%y%m%d")
    cf("008", f"{today6}s{pub_year}    xxu                 eng d")

    # 035 - OCLC system control number
    f035 = df("035")
    sf(f035, "a", f"(OCoLC){oclc_number}")

    # 040
    f040 = df("040")
    sf(f040, "a", cataloging_institution)
    sf(f040, "b", "eng")
    sf(f040, "e", "rda")
    sf(f040, "c", cataloging_institution)

    # 028
    field_028 = marc_fields.get("field_028")
    if field_028:
        f028 = df("028", "0", "2")
        parts = str(field_028).split("$b")
        sf(f028, "a", parts[0].strip())
        if len(parts) > 1:
            sf(f028, "b", parts[1].strip())

    # 100 / 110
    field_100 = marc_fields.get("field_100")
    if field_100:
        f100 = df("100", "1", " ")
        parse_subfields(field_100, f100)
    elif marc_fields.get("field_110"):
        f110 = df("110", "2", " ")
        parse_subfields(marc_fields["field_110"], f110)

    # 245
    field_245 = marc_fields.get("field_245")
    if field_245:
        ind1 = "1" if (marc_fields.get("field_100") or marc_fields.get("field_110")) else "0"
        f245 = df("245", ind1, "0")
        parse_subfields(field_245, f245)

    # 246
    field_246 = marc_fields.get("field_246")
    if field_246:
        f246 = df("246", "1", " ")
        sf(f246, "a", str(field_246).strip())

    # 264
    field_264_val = marc_fields.get("field_264")
    if field_264_val:
        f264 = df("264", " ", "1")
        parse_subfields(field_264_val, f264)

    # 300
    f300 = df("300")
    parse_subfields(marc_fields.get("field_300", "1 audio disc : $b digital ; $c 4 3/4 in."), f300)

    # RDA triplet 336/337/338
    for tag, a_val, b_val, two_val in [
        ("336", "performed music", "prm", "rdacontent"),
        ("337", "audio", "s", "rdamedia"),
        ("338", "audio disc", "sd", "rdacarrier")
    ]:
        f = df(tag)
        sf(f, "a", a_val); sf(f, "b", b_val); sf(f, "2", two_val)

    # 500 notes
    for key in ("field_500_general", "field_500_kut"):
        val = marc_fields.get(key)
        if val:
            f500 = df("500")
            sf(f500, "a", str(val).strip())

    # 500 - AI generated note (always present on original records)
    f500_ai = df("500")
    sf(f500_ai, "a", "AI-generated catalog record. Cataloger review completed.")

    # 505
    field_505 = marc_fields.get("field_505")
    if field_505:
        f505 = df("505", "0", " ")
        sf(f505, "a", str(field_505).strip())

    # 518
    field_518 = marc_fields.get("field_518")
    if field_518:
        f518 = df("518")
        sf(f518, "a", str(field_518).strip())

    # 588
    field_588 = marc_fields.get("field_588")
    if field_588:
        f588 = df("588")
        sf(f588, "a", str(field_588).strip())

    # 650
    for subj in (marc_fields.get("field_650") or []):
        if subj:
            f650 = df("650", " ", "0")
            parse_subfields(str(subj), f650)

    # 700
    for ae in (marc_fields.get("field_700") or []):
        if ae:
            f700 = df("700", "1", " ")
            parse_subfields(str(ae), f700)

    return ET.tostring(root, encoding="unicode")


# ── Alma import functions ─────────────────────────────────────────────────────

def import_bib_to_alma(marcxml):
    """Import MARCXML bib to Alma. Returns MMS ID."""
    url = f"{ALMA_BASE}/bibs"
    bib_xml = f"<bib>{marcxml}</bib>"
    r = requests.post(url, headers=HEADERS_XML, data=bib_xml, timeout=120)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    mms_id = root.find("mms_id")
    if mms_id is not None:
        return mms_id.text
    raise RuntimeError("No MMS ID returned from Alma")


def create_item(mms_id, holding_id, barcode, item_policy_code):
    """Create item record. Internal note marks it as original AI record."""
    url = f"{ALMA_BASE}/bibs/{mms_id}/holdings/{holding_id}/items"
    today = datetime.datetime.now().strftime("%Y-%m-%d") + "Z"

    data = f"""<item>
  <holding_data>
    <holding_id>{holding_id}</holding_id>
    <in_temp_location>false</in_temp_location>
  </holding_data>
  <item_data>
    <barcode>{barcode}</barcode>
    <physical_material_type>CD</physical_material_type>
    <policy><value>{item_policy_code}</value></policy>
    <arrival_date>{today}</arrival_date>
    <internal_note_2>{ORIG_INTERNAL_NOTE}</internal_note_2>
    <process_type>PHYSICAL_PROCESSING</process_type>
  </item_data>
</item>"""

    r = requests.post(url, headers=HEADERS_XML, data=data, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    pid = root.find(".//pid")
    if pid is not None:
        return pid.text
    raise RuntimeError("Item creation failed — no PID returned")


# ── Get records ready for import ──────────────────────────────────────────────

def get_records_ready_for_alma(workflow_data):
    """
    Find all records with status 'oclc_created' in step3b data.
    These have an assigned OCLC number and are ready for Alma import.
    """
    ready = []
    for barcode, record in workflow_data.get("records", {}).items():
        step3b = record.get("step3b_original_cataloging", {})
        if (step3b.get("status") == "oclc_created" and
                step3b.get("assigned_oclc_number") and
                step3b.get("marc_fields")):
            ready.append({
                "barcode": barcode,
                "oclc_number": step3b["assigned_oclc_number"].lstrip("on") if isinstance(step3b["assigned_oclc_number"], str) and step3b["assigned_oclc_number"].startswith("on") else step3b["assigned_oclc_number"],
                "marc_fields": step3b["marc_fields"]
            })
    return ready


# ── Save Alma result to workflow JSON ─────────────────────────────────────────

def save_alma_result_to_json(workflow_json_path, barcode, mms_id, holding_id, item_pid):
    """Save Alma import result to workflow JSON."""
    data = load_workflow_json(workflow_json_path)
    if str(barcode) not in data.get("records", {}):
        return

    data["records"][str(barcode)]["step3b_original_cataloging"].update({
        "status": "imported_to_alma",
        "alma_mms_id": mms_id,
        "alma_holding_id": holding_id,
        "alma_item_pid": item_pid,
        "alma_import_at": datetime.datetime.now().isoformat()
    })
    data["records"][str(barcode)]["updated_at"] = datetime.datetime.now().isoformat()
    save_workflow_json(workflow_json_path, data)


# ── Receipt CSV ───────────────────────────────────────────────────────────────

def write_receipt_csv(results, results_folder, current_ts):
    """
    Write receipt CSV with all created record IDs.
    Saves to:
      1. deliverables/original-cataloging-alma-ids-[timestamp].csv
      2. AI_Music_Operations/original-cataloging/[date]/
    """
    ops_dir = os.environ.get("AI_MUSIC_OPERATIONS_DIR")
    filename = f"original-cataloging-alma-ids-{current_ts}.csv"

    deliverables = os.path.join(results_folder, "deliverables")
    report_path = os.path.join(deliverables, filename)

    fieldnames = ["Barcode", "OCLC Number", "MMS ID", "Holding ID", "Item ID",
                  "Status", "Internal Note", "Import Date"]

    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({
                "Barcode": r.get("barcode"),
                "OCLC Number": r.get("oclc_number"),
                "MMS ID": r.get("mms_id", ""),
                "Holding ID": r.get("holding_id", ""),
                "Item ID": r.get("item_pid", ""),
                "Status": r.get("status"),
                "Internal Note": ORIG_INTERNAL_NOTE,
                "Import Date": datetime.datetime.now().strftime("%Y-%m-%d")
            })

    print(f"Receipt CSV saved: {report_path}")

    if ops_dir:
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        ops_folder = os.path.join(ops_dir, "original-cataloging", date_str)
        os.makedirs(ops_folder, exist_ok=True)
        ops_path = os.path.join(ops_folder, filename)
        import shutil
        shutil.copy2(report_path, ops_path)
        print(f"Receipt also saved to: {ops_path}")

    return report_path


def create_alma_report(results_folder, processing_results, current_ts):
    """Create plain text report of Alma import."""
    ops_dir = os.environ.get("AI_MUSIC_OPERATIONS_DIR")
    successful = [r for r in processing_results if r.get("status") == "success"]
    skipped = [r for r in processing_results if r.get("status") == "already_exists"]
    failed = [r for r in processing_results if r.get("status") == "error"]

    lines = [
        "=" * 65,
        "AI MUSIC METADATA PROJECT — ORIGINAL CATALOGING ALMA IMPORT REPORT",
        "=" * 65,
        f"Generated:              {current_ts}",
        f"Total processed:        {len(processing_results)}",
        f"Successfully imported:  {len(successful)}",
        f"Already in Alma (dedup blocked): {len(skipped)}",
        f"Failed:                 {len(failed)}",
        f"Internal note applied:  {ORIG_INTERNAL_NOTE}",
        "",
        "IMPORTED RECORDS:",
        "-" * 40,
    ]

    for r in successful:
        lines.append(f"  {r['barcode']}  |  OCLC: {r['oclc_number']}  |  MMS: {r.get('mms_id','')}  |  Item: {r.get('item_pid','')}")

    if skipped:
        lines += ["", "DEDUP BLOCKED (already in Alma):", "-" * 40]
        for r in skipped:
            lines.append(f"  {r['barcode']}  |  OCLC: {r['oclc_number']}  |  MMS: {r.get('existing_mms','')}")

    if failed:
        lines += ["", "FAILED:", "-" * 40]
        for r in failed:
            lines.append(f"  {r['barcode']}  |  {r.get('error','')[:60]}")

    lines += ["", "Records are now discoverable in Primo VE after next indexing (24-48h).", "=" * 65]
    report_text = "\n".join(lines)

    deliverables = os.path.join(results_folder, "deliverables")
    filename = f"original-cataloging-alma-import-report-{current_ts}.txt"
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
    parser = argparse.ArgumentParser(description="Import approved original catalog records to Alma")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--report", action="store_true", help="Preview only — no Alma changes")
    args = parser.parse_args()

    print("=" * 65)
    print("STEP 3d: IMPORT ORIGINAL CATALOG RECORDS TO ALMA")
    print("=" * 65)

    file_paths = get_file_path_config()
    current_ts = get_current_timestamp()

    results_folder = find_latest_results_folder(file_paths["results_prefix"])
    if not results_folder:
        raise SystemExit("No results folder found.")

    data_folder = os.path.join(results_folder, "data")
    workflow_json_path = get_workflow_json_path(data_folder)
    workflow_data = load_workflow_json(workflow_json_path)

    # Get records ready for Alma
    ready_records = get_records_ready_for_alma(workflow_data)

    if not ready_records:
        print("No records ready for Alma import.")
        print("Run step_3c_oclc_original_record.py first to create OCLC records.")
        return

    print(f"\nRecords ready for import: {len(ready_records)}")
    print(f"Library: {LIBRARY_CODE}, Location: {LOCATION_CODE}")
    print(f"Item policy: {ITEM_POLICY_CODE}")
    print(f"Internal note: {ORIG_INTERNAL_NOTE}")
    print(f"Results folder: {results_folder}")

    # Show preview
    print("\nFirst 5 records:")
    for r in ready_records[:5]:
        print(f"  {r['barcode']}  |  OCLC: {r['oclc_number']}")

    if args.report:
        print(f"\n--report mode. No changes made.")
        return

    if not args.yes:
        resp = input(f"\nImport {len(ready_records)} original catalog records to PRODUCTION Alma? Type 'yes': ").strip().lower()
        if resp != 'yes':
            raise SystemExit("Aborted.")

    processing_results = []
    total = len(ready_records)

    for i, rec in enumerate(ready_records, 1):
        barcode = rec["barcode"]
        oclc_number = rec["oclc_number"]
        marc_fields = rec["marc_fields"]

        print(f"\n[{i}/{total}] Barcode: {barcode} | OCLC: {oclc_number}")
        result = {"barcode": barcode, "oclc_number": oclc_number}

        # ── RESUME: skip if already successfully imported in previous run ────
        import json as _json, glob as _glob
        _data_folder = os.path.join(results_folder, "data")
        _json_files  = _glob.glob(os.path.join(_data_folder, "*.json"))
        if not _json_files:
            _json_files = _glob.glob(os.path.join(results_folder, "*.json"))
        _already_done = False
        if _json_files:
            try:
                with open(sorted(_json_files)[-1]) as _jf:
                    _wf = _json.load(_jf)
                _step3d = _wf.get("records",{}).get(str(barcode),{}).get("step3d_alma_import",{})
                if _step3d.get("mms_id") and _step3d.get("status") == "success":
                    print(f"  RESUME: Already in Alma (MMS: {_step3d['mms_id']}) — skipping")
                    result.update({"status":"already_exists","existing_mms":_step3d["mms_id"]})
                    processing_results.append(result)
                    _already_done = True
            except Exception:
                pass
        if _already_done:
            continue

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # DEDUP — mandatory, reuse exact function from alma_batch_upload_cd
                print(f"  Checking Alma for existing OCLC {oclc_number}...")
                existing_mms = check_if_oclc_exists_in_alma(oclc_number)
                if existing_mms:
                    print(f"  DEDUP BLOCKED: Already in Alma (MMS: {existing_mms})")
                    result.update({"status": "already_exists", "existing_mms": existing_mms})
                    break

                # Build MARCXML
                print(f"  Building MARCXML...")
                marcxml = build_alma_marcxml(marc_fields, oclc_number, barcode, CATALOGING_INSTITUTION)

                # Import bib
                print(f"  Importing bib to Alma...")
                mms_id = import_bib_to_alma(marcxml)
                print(f"  MMS ID: {mms_id}")

                # Unsuppress
                unsuppress_bib(mms_id)
                print(f"  Bib unsuppressed — visible in Primo")

                # Holdings
                holding_id = get_holdings(mms_id, LOCATION_CODE)
                if holding_id:
                    print(f"  Found existing holding: {holding_id}")
                else:
                    holding_id = create_holding(mms_id, LIBRARY_CODE, LOCATION_CODE)
                    print(f"  Created holding: {holding_id}")

                # Item
                item_pid = create_item(mms_id, holding_id, barcode, ITEM_POLICY_CODE)
                print(f"  Item created: {barcode}")

                # Save to workflow JSON
                save_alma_result_to_json(workflow_json_path, barcode, mms_id, holding_id, item_pid)

                result.update({
                    "status": "success",
                    "mms_id": mms_id,
                    "holding_id": holding_id,
                    "item_pid": item_pid
                })
                print(f"  SUCCESS")
                break

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait = 30 * (2 ** attempt)
                    print(f"  Timeout (attempt {attempt+1}/{max_retries}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    result.update({"status": "error", "error": str(e)})
                    log_error(results_folder, "step3d", barcode, "timeout", str(e))
                    print(f"  ERROR after {max_retries} attempts: {e}")

            except Exception as e:
                result.update({"status": "error", "error": str(e)})
                log_error(results_folder, "step3d", barcode, "unexpected_error", str(e))
                print(f"  ERROR: {e}")
                break

        processing_results.append(result)
        time.sleep(2.0)

    # Write receipt and report
    receipt_path = write_receipt_csv(processing_results, results_folder, current_ts)
    report_path = create_alma_report(results_folder, processing_results, current_ts)

    successful = sum(1 for r in processing_results if r.get("status") == "success")
    skipped = sum(1 for r in processing_results if r.get("status") == "already_exists")
    failed = sum(1 for r in processing_results if r.get("status") == "error")

    print(f"\n{'='*65}")
    print(f"STEP 3d COMPLETE")
    print(f"{'='*65}")
    print(f"Successfully imported:  {successful}")
    print(f"Dedup blocked:          {skipped}")
    print(f"Failed:                 {failed}")
    print(f"Receipt CSV:  {receipt_path}")
    print(f"Report:       {report_path}")

    if successful > 0:
        print(f"\nRecords are now in production Alma.")
        print(f"They will appear in Primo VE within 24-48 hours after indexing.")
        if skipped == 0:
            print(f"OCLC holdings were set in Step 3c — no further action needed.")

    # Auto-generate complete batch report
    try:
        ops_dir = os.environ.get("AI_MUSIC_OPERATIONS_DIR", "")
        if ops_dir and results_folder:
            print(f"\nGenerating complete batch report...")
            generate_batch_report(results_folder, ops_dir)
    except Exception as rpt_err:
        print(f"Note: Report generation skipped: {rpt_err}")


if __name__ == "__main__":
    main()
