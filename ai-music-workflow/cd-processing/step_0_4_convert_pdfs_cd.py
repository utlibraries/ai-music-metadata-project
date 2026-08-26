#!/usr/bin/env python3
"""
Step 0.4: Convert PDF scans to JPEGs before Step 0.5 filename validation.

Run this BEFORE step_.5_cd.py whenever your image batch contains PDFs.

Each PDF page becomes a separate JPEG using the existing naming convention:
    Page 1 -> [barcode]a.jpg  (FRONT COVER)
    Page 2 -> [barcode]b.jpg  (BACK COVER)
    Page 3 -> [barcode]c.jpg  (ADDITIONAL IMAGE, if present)

Original PDFs are moved to a pdf-originals/ subfolder so re-running
this script never double-converts the same file.

Usage:
    python step_0_4_convert_pdfs_cd.py
"""

import os
import re
import sys
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    print("ERROR: PyMuPDF is not installed.")
    print("Fix:   pip install PyMuPDF")
    sys.exit(1)

from cd_workflow_config import get_file_path_config

# ── Rendering settings ────────────────────────────────────────────────────────
# 300 DPI keeps UPC barcodes, catalog numbers, and track listings legible for
# GPT-4o. PDFs are internally 72 DPI, so scale factor = 300/72 ≈ 4.17x.
RENDER_DPI   = 300
JPEG_QUALITY = 90

# ── Naming convention (keep in sync with step_.5_cd.py) ──────────────────────
DIGITS_COUNT   = 15
BARCODE_PREFIX = "059173"
LETTERS        = "abcdefghijklmnopqrstuvwxyz"


def extract_barcode(stem: str) -> str:
    """Strip everything that isn't a digit from the filename stem."""
    return re.sub(r"\D", "", stem)


def validate_barcode(barcode: str, original_filename: str):
    """
    Apply the same barcode rules as step_.5_cd.py.
    Returns (is_valid: bool, reason: str).
    """
    if len(barcode) != DIGITS_COUNT:
        return False, (
            f"barcode '{barcode}' is {len(barcode)} digits — "
            f"expected {DIGITS_COUNT}"
        )
    if not barcode.startswith(BARCODE_PREFIX):
        return False, (
            f"barcode '{barcode}' does not start with '{BARCODE_PREFIX}'"
        )
    return True, ""


def convert_pdf(pdf_path: Path, output_dir: Path, barcode: str) -> list:
    """
    Render every page of a PDF to a JPEG.
    Returns the list of filenames created.
    Raises FileExistsError if any output file already exists (safe re-run guard).
    """
    doc = fitz.open(pdf_path)
    num_pages = len(doc)

    if num_pages > len(LETTERS):
        doc.close()
        raise ValueError(
            f"PDF has {num_pages} pages but only {len(LETTERS)} "
            f"letter suffixes are defined."
        )

    # Pre-check: refuse to overwrite existing JPEGs before touching anything
    planned = []
    for i in range(num_pages):
        out_name = f"{barcode}{LETTERS[i]}.jpg"
        out_path = output_dir / out_name
        if out_path.exists():
            doc.close()
            raise FileExistsError(
                f"'{out_name}' already exists — move or delete it first."
            )
        planned.append((i, out_path, out_name))

    zoom   = RENDER_DPI / 72
    matrix = fitz.Matrix(zoom, zoom)
    created = []

    for page_index, out_path, out_name in planned:
        page = doc[page_index]
        pix  = page.get_pixmap(matrix=matrix)
        pix.save(str(out_path), jpg_quality=JPEG_QUALITY)
        created.append(out_name)

    doc.close()
    return created


def main() -> bool:
    file_paths    = get_file_path_config()
    images_folder = Path(file_paths["images_folder"])

    if not images_folder.exists():
        print(f"ERROR: images folder not found: {images_folder}")
        return False

    # Collect PDFs (case-insensitive extension)
    pdf_files = sorted(
        f for f in images_folder.iterdir()
        if f.is_file() and f.suffix.lower() == ".pdf"
    )

    if not pdf_files:
        print("No PDF files found in images folder — nothing to convert.")
        return True

    print(f"Found {len(pdf_files)} PDF file(s) in: {images_folder}")
    print("=" * 70)

    archive_dir = images_folder / "pdf-originals"
    archive_dir.mkdir(exist_ok=True)

    converted = []   # (pdf_name, [jpeg_names])
    skipped   = []   # (pdf_name, reason)

    for pdf_path in pdf_files:
        barcode            = extract_barcode(pdf_path.stem)
        is_valid, reason   = validate_barcode(barcode, pdf_path.name)

        if not is_valid:
            print(f"  SKIP  '{pdf_path.name}' — {reason}")
            skipped.append((pdf_path.name, reason))
            continue

        try:
            created = convert_pdf(pdf_path, images_folder, barcode)
            label   = " + ".join(
                ["FRONT COVER", "BACK COVER", "ADDITIONAL"][: len(created)]
                + (created[3:] if len(created) > 3 else [])
            )
            print(f"  OK    '{pdf_path.name}' -> {', '.join(created)}  ({label})")
            converted.append((pdf_path.name, created))

            # Archive the original PDF so re-runs skip it safely
            pdf_path.rename(archive_dir / pdf_path.name)

        except Exception as exc:
            print(f"  ERROR '{pdf_path.name}' — {exc}")
            skipped.append((pdf_path.name, str(exc)))

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("CONVERSION SUMMARY")
    print(f"  Converted : {len(converted)}")
    print(f"  Skipped   : {len(skipped)}")

    if skipped:
        print("\nFiles that need attention before Step 0.5 will pass:")
        for name, reason in skipped:
            print(f"    {name}: {reason}")
        print(
            f"\n  Expected barcode format: {DIGITS_COUNT} digits "
            f"starting with '{BARCODE_PREFIX}'"
        )
        print("  Fix the filenames, then re-run this script.")
        return False

    print("\nAll PDFs converted. You can now run step_.5_cd.py safely.")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
