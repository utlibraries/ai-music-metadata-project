import csv, glob, openpyxl, datetime, os, sys

def generate_batch_report(results_folder, ops_dir):
    """
    Generate complete batch processing report from source data files.
    Called automatically at end of Step 3d.
    """
    today = datetime.date.today()
    now   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    deliverables = os.path.join(results_folder, "deliverables")

    alma_imports_dir = os.path.join(ops_dir, "alma-imports", "cd")
    import_csvs = sorted(glob.glob(
        os.path.join(alma_imports_dir, f"{today}_CD-*-records-alma-import.csv")))

    orig_id_files = sorted(glob.glob(
        os.path.join(deliverables, "original-cataloging-alma-ids-*.csv")))

    sort_files = sorted(glob.glob(
        os.path.join(deliverables, "cd-workflow-sorting-*.xlsx")))

    if not import_csvs and not orig_id_files:
        print("Report: No import files found yet — skipping")
        return None

    lines = []
    lines.append("=" * 110)
    lines.append("AI MUSIC METADATA PROJECT — BATCH COMPLETE PROCESSING REPORT")
    lines.append("University of Texas Libraries — KUT Radio Collection")
    lines.append("=" * 110)
    lines.append(f"Generated:    {now}")
    lines.append(f"Batch folder: {results_folder}")
    lines.append("")

    # SECTION 1: COPY CATALOGING
    lines.append("=" * 110)
    lines.append("SECTION 1: COPY CATALOGING — IN PRODUCTION ALMA")
    lines.append("Discoverable in Primo VE. Scan barcode to confirm physical item.")
    lines.append("=" * 110)
    lines.append(f"{'MMS ID':<24} {'Holding ID':<24} {'Item ID':<24} {'OCLC':<14} {'Barcode':<22} {'OCLC Source':<22} Title")
    lines.append("-" * 150)

    copy_barcodes = set()
    copy_rows = []
    for fname in import_csvs:
        try:
            with open(fname) as f:
                for row in csv.DictReader(f):
                    bc = row.get('Barcode','').strip()
                    if bc and bc not in copy_barcodes:
                        copy_barcodes.add(bc)
                        copy_rows.append(row)
                        lines.append(
                            f"{row.get('MMS ID',''):<24} "
                            f"{row.get('Holding ID',''):<24} "
                            f"{row.get('Item ID',''):<24} "
                            f"{row.get('OCLC',''):<14} "
                            f"{row.get('Barcode',''):<22} "
                            f"{row.get('OCLC Source',''):<22} "
                            f"{row.get('Title','')[:35]}")
        except Exception as e:
            lines.append(f"  Warning: Could not read {fname}: {e}")

    lines.append(f"\nSubtotal: {len(copy_rows)} records | IXA Holdings: SET")
    lines.append("")

    # SECTION 2 & 3: ORIGINAL CATALOGING
    orig_success = []
    orig_dedup   = []
    orig_dedup_barcodes = set()
    if orig_id_files:
        lines.append("=" * 110)
        lines.append("SECTION 2: ORIGINAL CATALOGING — IN PRODUCTION ALMA (NEW OCLC RECORDS)")
        lines.append("New WorldCat records created by UT Libraries. IXA holdings set.")
        lines.append("=" * 110)
        lines.append(f"{'Barcode':<22} {'OCLC Number':<16} {'MMS ID':<24} {'Holding ID':<24} {'Item ID':<24} Status")
        lines.append("-" * 120)

        with open(sorted(orig_id_files)[-1]) as f:
            for row in csv.DictReader(f):
                bc  = row.get('Barcode','').strip()
                mms = row.get('MMS ID','').strip()
                sts = row.get('Status','').strip()
                if not bc: continue
                if sts == 'success' and mms:
                    orig_success.append(row)
                    lines.append(
                        f"{bc:<22} {row.get('OCLC Number',''):<16} "
                        f"{mms:<24} {row.get('Holding ID',''):<24} "
                        f"{row.get('Item ID',''):<24} {sts}")
                else:
                    orig_dedup.append(row)
                    orig_dedup_barcodes.add(bc)

        lines.append(f"\nSubtotal: {len(orig_success)} records | IXA Holdings: SET during Step 3c")
        lines.append("")

        if orig_dedup:
            lines.append("=" * 110)
            lines.append("SECTION 3: ORIGINAL CATALOGING — DEDUP BLOCKED (ALREADY IN ALMA)")
            lines.append("=" * 110)
            lines.append(f"{'Barcode':<22} {'OCLC Number':<16} Status")
            lines.append("-" * 60)
            for row in orig_dedup:
                bc = row.get('Barcode','').strip()
                lines.append(f"{bc:<22} {row.get('OCLC Number',''):<16} {row.get('Status','')}")
            lines.append(f"\nSubtotal: {len(orig_dedup)} records")
            lines.append("")

    # SECTIONS 4 & 5: IXA HELD + DUPLICATES
    ixa_rows = []
    dup_rows = []
    total_scan = 0
    if sort_files:
        wb = openpyxl.load_workbook(sorted(sort_files)[-1], read_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0]: continue
            total_scan += 1
            bc    = str(row[0]).strip()
            group = str(row[1]).strip() if row[1] else ''
            oclc  = str(row[2]).strip() if row[2] else ''
            title = str(row[3]).strip() if row[3] else ''
            conf  = str(row[6]).strip() if row[6] else '0'
            if bc in orig_dedup_barcodes:
                continue
            if 'IXA' in group:
                ixa_rows.append((bc, oclc, conf, title))
            elif 'Duplicate' in group:
                dup_rows.append((bc, oclc, conf, title))
        wb.close()

        lines.append("=" * 110)
        lines.append("SECTION 4: ALREADY HELD BY UT LIBRARIES (IXA) — CORRECTLY EXCLUDED")
        lines.append("=" * 110)
        lines.append(f"{'Barcode':<22} {'OCLC':<14} {'Confidence':<12} Title")
        lines.append("-" * 90)
        for bc, oclc, conf, title in sorted(ixa_rows):
            lines.append(f"{bc:<22} {oclc:<14} {conf+'%':<12} {title[:50]}")
        lines.append(f"\nSubtotal: {len(ixa_rows)} records")
        lines.append("")

        lines.append("=" * 110)
        lines.append("SECTION 5: WITHIN-BATCH DUPLICATES — CORRECTLY EXCLUDED")
        lines.append("=" * 110)
        lines.append(f"{'Barcode':<22} {'OCLC':<14} {'Confidence':<12} Title")
        lines.append("-" * 90)
        for bc, oclc, conf, title in sorted(dup_rows):
            lines.append(f"{bc:<22} {oclc:<14} {conf+'%':<12} {title[:50]}")
        lines.append(f"\nSubtotal: {len(dup_rows)} records")
        lines.append("")

    # SECTION 6: FAILED
    failed_barcodes = []
    oclc_reports = sorted(glob.glob(os.path.join(deliverables, "oclc-record-creation-report-*.txt")))
    alma_reports = sorted(glob.glob(os.path.join(deliverables, "original-cataloging-alma-import-report-*.txt")))

    seen_failed = set()
    for report_file in oclc_reports + alma_reports:
        with open(report_file) as f:
            in_failed = False
            for line in f:
                if 'FAILED' in line and ('---' in line or ':' in line):
                    in_failed = True
                    continue
                if in_failed and '059173' in line:
                    parts = line.strip().split('|')
                    bc = parts[0].strip()
                    reason = parts[1].strip() if len(parts) > 1 else 'See report'
                    if bc not in seen_failed:
                        seen_failed.add(bc)
                        failed_barcodes.append((bc, reason[:80]))
                if in_failed and 'NEXT STEP' in line:
                    break

    if failed_barcodes:
        lines.append("=" * 110)
        lines.append("SECTION 6: FAILED — MANUAL ATTENTION REQUIRED")
        lines.append("=" * 110)
        lines.append(f"{'Barcode':<22} Reason")
        lines.append("-" * 90)
        for bc, reason in sorted(failed_barcodes):
            lines.append(f"{bc:<22} {reason}")
        lines.append(f"\nSubtotal: {len(failed_barcodes)} records")
        lines.append("")

    # SUMMARY
    total_alma = len(copy_rows) + len(orig_success)
    total_excl = len(ixa_rows) + len(dup_rows) + len(orig_dedup)
    total_fail = len(failed_barcodes)
    grand      = total_alma + total_excl + total_fail

    lines.append("=" * 110)
    lines.append("BATCH FINAL SUMMARY")
    lines.append("=" * 110)
    lines.append(f"Total CDs scanned:                                {total_scan}")
    lines.append(f"Total accounted for:                              {grand}")
    lines.append(f"")
    lines.append(f"IN PRODUCTION ALMA:")
    lines.append(f"  Copy cataloging (OCLC matched >=70%):           {len(copy_rows)}")
    lines.append(f"  Original cataloging (new OCLC records):         {len(orig_success)}")
    lines.append(f"  TOTAL IN ALMA:                                  {total_alma}")
    lines.append(f"")
    lines.append(f"EXCLUDED — NO ACTION NEEDED:")
    lines.append(f"  Already held by IXA:                            {len(ixa_rows)}")
    lines.append(f"  Within-batch duplicates:                        {len(dup_rows)}")
    lines.append(f"  Original cataloging dedup blocked:              {len(orig_dedup)}")
    lines.append(f"  TOTAL EXCLUDED:                                 {total_excl}")
    lines.append(f"")
    lines.append(f"FAILED — MANUAL ATTENTION REQUIRED:               {total_fail}")
    lines.append(f"")
    lines.append(f"WHY NO CATALOGER REVIEW (if applicable):")
    lines.append(f"  Threshold: 70% (approved by Whit Williams & Corey Halaychik)")
    lines.append(f"  Records 1-69% with OCLC match require review.")
    lines.append(f"  If zero appear above, all matched records scored >=70% and")
    lines.append(f"  all no-match records went directly to original cataloging.")
    lines.append(f"")
    lines.append(f"OCLC HOLDINGS:")
    lines.append(f"  Copy cataloging: SET via OCLC Holdings tool")
    lines.append(f"  Original cataloging: SET during Step 3c")
    lines.append(f"  Reports: {ops_dir}/oclc-holdings/cd/")
    lines.append("")
    lines.append("=" * 110)
    lines.append(f"Report generated: {now}")
    lines.append("Automation & Integration Librarian: Kayode Ishola")
    lines.append("University of Texas Libraries — KUT Radio Collection")
    lines.append("=" * 110)

    out = os.path.join(deliverables, f"batch-complete-report-{today}.txt")
    with open(out, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE REPORT GENERATED")
    print(f"{'='*60}")
    print(f"  In Alma:   {total_alma} ({len(copy_rows)} copy + {len(orig_success)} original)")
    print(f"  Excluded:  {total_excl}")
    print(f"  Failed:    {total_fail}")
    print(f"  Total:     {grand} of {total_scan}")
    print(f"  Report:    {out}")
    print(f"{'='*60}\n")
    return out


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate batch complete report")
    parser.add_argument("results_folder", help="Path to results folder")
    parser.add_argument("--ops-dir", default=os.environ.get("AI_MUSIC_OPERATIONS_DIR",""))
    args = parser.parse_args()
    generate_batch_report(args.results_folder, args.ops_dir)
