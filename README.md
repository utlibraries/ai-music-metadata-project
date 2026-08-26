# AI Music Metadata Project

Developed by Hannah Moutran and Kayode Ishola, UT Austin Libraries.

## Overview
Automates metadata extraction and OCLC matching for CD and LP collections. This project uses AI for basic metadata extraction from images and for analyzing OCLC match results. It then searches OCLC WorldCat using the generated metadata and creates ready-to-use cataloging files.

Optionally, users can generate an HTML review interface. The upside is that it provides a convenient way to review results before moving forward. The downside is that it is served locally on your computer; to support this, the script copies all required images into the results folder, making it best for batches of 500 items or less. To support the review work done using the HTML site, there is a script to incorporate the cataloger decisions into the cataloging files (details below). 

Another optional component is the batch upload to Alma Sandbox, which is designed to use the generated alma-batch-upload CSV file as the input set.

**Separate workflows for CDs and LPs** - each format has its own processing folder with dedicated scripts and configurations.

***Note: This repository is under active development.***


---

## Processing Pipeline

0. **Step 0.4** *(NEW)*: Convert PDF scans to JPEGs — for batches scanned as multi-page PDFs. Runs automatically before Step 0.5.
1. **Step 0.5**: Validate image file naming
2. **Step 1**: Extract metadata from images using AI
3. **Step 1.5**: Clean and normalize extracted metadata
4. **Step 2**: Query OCLC WorldCat API
5. **Step 3**: AI analysis of OCLC matches with confidence scoring
6. **Step 4**: Verify track listings and publication years
7. **Step 5**: Create final output files organized in subfolders
8. **Step 6** (optional, but included in run script if approved): Generate HTML review interface with images.  Also creates a decisions-history spreadsheet, necessary to track changes to output files. 
9. **Step 7** (not in run script): Creates an 'original-outputs' folder and copies original cataloging files to it.  Updates the decisions-history spreadsheet with cataloger decisions and updates cataloging files, including the batch upload file and sorting spreadsheet.  
9. **Alma Batch Processing** (not in run script): Takes the high confidence matches not already held by the institution and uses the OCLC number to create bibliographic, holding, and item records in Alma.   

*****The Alma batch upload scripts are provided for sandbox experimentation only.*****


---
## Features
- **AI Metadata Extraction**: LLM extracts title, artist, publisher, tracks, dates, and physical description from CD/LP images
- **OCLC Integration**: Automated WorldCat searches return up to 10 matching records per item
- **AI Match Analysis**: LLM evaluates matches, assigns confidence scores, and briefly explains reasoning
- **Additional Verification**: Automatic track listing and publication year validation
- **PDF Support**: Batches scanned as multi-page PDFs are automatically converted to JPEGs before processing
- **Web UI**: Browser-based interface for running the pipeline, monitoring progress, reviewing batches, and triggering Alma imports without using the terminal
- **Batch Processing**: 50% cost savings for batches over 10 items (automatic)
- **HTML Review Interface** (Optional but a very convenient tool): Visual review of matches with images.  Make decisions on the page, then export decisions to CSV and process using script 7 to automatically edit cataloging files.
- **Alma Batch Uploads**: Creates new bibs, holdings, and items by importing bibliographic information from OCLC. Intended for experimentation in Alma SANDBOX and excluded from the automated run script.
---

## Installation

1. **Clone repository**
   ```bash
   git clone https://github.com/utlibraries/ai-music-metadata-project.git
   cd ai-music-metadata-project
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set environment variables**

   **Required for main workflow:**
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   export OCLC_CLIENT_ID="your-oclc-client-id"
   export OCLC_SECRET="your-oclc-secret"
   ```

   **Required for Alma batch upload (sandbox only):**
   ```bash
   export ALMA_SANDBOX_API_KEY="your-alma-sandbox-api-key"
   export ALMA_LIBRARY_CODE="your-library-code"
   export ALMA_LOCATION_CODE="your-location-code"
   export ALMA_CD_ITEM_POLICY="your-cd-item-policy"
   export ALMA_LP_ITEM_POLICY="your-lp-item-policy"
   export ALMA_CATALOGING_INSTITUTION="your-cataloging-institution"
   ```

   **Optional for Alma batch upload:**
   ```bash
   export ALMA_REGION="api-na"
   export ALMA_INTERNAL_NOTE_2="AI-assisted cataloging"
   ```
   These default to "api-na" (North America) and "AI-assisted cataloging" if not set.

4. **If using Portkey** (optional)

   If your organization routes OpenAI calls through [Portkey](https://portkey.ai/), set `"enabled": True` in the `PORTKEY_CONFIG` dictionary near the top of your format's config file:

   **CD workflow**: `ai-music-workflow/cd-processing/cd_workflow_config.py`

   **LP workflow**: `ai-music-workflow/lp-processing/lp_workflow_config.py`

   ```python
   PORTKEY_CONFIG = {
       "enabled": True,   # Default is False (direct OpenAI API)
       "api_key_env": "PORTKEY_API_KEY",
       "virtual_key_env": "PORTKEY_VIRTUAL_KEY"
   }
   ```

   Then set your Portkey credentials as environment variables:
   ```bash
   export PORTKEY_API_KEY="your-portkey-api-key"
   export PORTKEY_VIRTUAL_KEY="your-portkey-virtual-key"
   ```

   Both values can be found in the Portkey dashboard. The **virtual key** is the slug for your OpenAI provider connection (e.g. `your-org-name-openai`).

   When enabled, all OpenAI calls — including batch processing — are routed through the Portkey gateway. If you are calling OpenAI directly, leave `"enabled": False` and set `OPENAI_API_KEY` as usual.

---

## Quick Start

### Option A: Web UI (recommended)

**For CDs and LPs:**
```bash
cd ai-music-workflow/cd-processing
python3 app.py
```
Open **http://localhost:8000** in a browser. The UI runs the full pipeline, streams live output, and provides access to all workflow steps including PDF conversion.

### Option B: Terminal

### Run Workflow - Steps 0.4 - 6 

**For CDs:**
```bash
python ai-music-workflow/cd-processing/run_cd_processing.py
```

**For LPs:**
```bash
python ai-music-workflow/lp-processing/run_lp_processing.py
```

The run script will:
- Automatically choose batch vs. real-time processing (you can change threshold in configuration file)
- Prompt in terminal for whether to generate HTML review interface (Step 6)
- Run processing steps in sequence, not including step 7 (to incorporate cataloger decision CSV into cataloging files) and batch upload script
- Create organized output files

---

## Image Input Files

### Organization
Place all images for a collection in a single folder.

**Example path:** 
`ai-music-metadata-project/ai-music-workflow/cd-processing/cd-image-folders/cd-scans-100/`

The workflow will automatically generate an outputs folder with organized results.

### Naming Convention
Images must be named with barcode + letter suffix:
**Examples:**
- `[barcode]a.jpeg`- Front image (required)
- `[barcode]b.jpeg`- Back image (optional)
- `[barcode]c.jpeg`- Additional image (optional)

### Format
- **Supported**: JPEG (.jpg, .jpeg), PNG (.png), or PDF (.pdf) — PDFs are converted to JPEGs automatically via Step 0.4
- **Aim for metadata clarity**: Images with clear, legible text, minimal glare, multiple elements for the pipeline to use when generating metadata/searching for item
- **Recommendation**: JPEG files, which will be faster and cheaper to process 

---

## Output Files

### `deliverables/` folder - Working files for catalogers

1. **sorting-spreadsheet-[date].xlsx**
   - ALL ITEMS categorized: High Confidence, Held by Library, Low Confidence, Duplicates
   - Use to physically organize materials

2. **batch-upload-alma-[cd/lp]-[timestamp].txt**
   - HIGH CONFIDENCE matches ready for import
   - Format: `OCLC_NUMBER|BARCODE|TITLE`

3. **tracking-spreadsheet-catalogers-[date].xlsx**
   - Interactive tracking for LOW CONFIDENCE items
   - Yellow highlighting for items needing review
   - Dropdown status menu, auto-populated OCLC numbers

4. **low-confidence-matches-review-[date].xlsx**
   - Detailed review information for each LOW CONFIDENCE item
   - AI-generated metadata, suggested matches, alternatives

5. **marc-formatted-low-confidence-matches-[date].xlsx**
   - Basic MARC records for original cataloging
   - Based on AI-extracted metadata
   - For LOW CONFIDENCE items only 

6. **decisions-history.xlsx**
   - Only created if user opts in to generate the HTML review interface
   - Initially contains only AI decisions, automatically edited if user makes decisions, downloads the CSV file of their decisions and uses script 7 to process the CSV
   - If automatically edited, the newest decisions are prioritized, older decisions are kept in Decisions History worksheet

### `guides/` folder - Documentation

- **CATALOGER_GUIDE.txt** - How to use workflow outputs
- **TECHNICAL_GUIDE.txt** - Quality control and troubleshooting

### `data/` folder - Complete 'Run' Workflow tracking

- **full-workflow-data-[cd/lp]-[timestamp].json** - Complete processing log
- **full-workflow-data-[cd/lp]-[timestamp].xlsx** - Excel version with thumbnails

### `logs/` folder - Contains all main workflow logs 
- Including API response logs, token usage logs, error logs, and metrics

### Main results folder (if HTML is generated)

- **review-index-[date].html** - Start page for visual review
- **review-page-[#]-[date].html** - Individual review pages
- **images/** - Copies of all processed images

---

## Automatic Optimization

The system automatically chooses processing mode based on batch size.  The threshold can be changed in the Configuration file. Both methods produce identical quality results.

---

## Batch Recovery

If your batch processing is interrupted (power outage, computer shutdown), you can recover it:

**List active batches:**
```bash
python ai-music-workflow/batch_recovery.py list
```

**Resume an interrupted batch:**
```bash
python ai-music-workflow/batch_recovery.py resume batch_abc123xyz456
```

**Clean up completed batches:**
```bash
python ai-music-workflow/batch_recovery.py cleanup
```

Batch IDs are automatically saved to `~/.ai-music-batch-state/` when submitted. Your batches continue processing on OpenAI's servers even if your script stops, and you can resume them anytime within 24 hours.

---

## Configuration

Edit format-specific config files to customize:

**CD workflow**: `cd-processing/cd_workflow_config.py`

**LP workflow**: `lp-processing/lp_workflow_config.py`

Settings include:
- Model selection for each step (OpenAI models only)
- Image folder paths
- Batch Processing Threshold

---

## Best Practices

### Before Processing
0. **Convert PDFs** - If your batch was scanned as PDFs, run Step 0.4 first (automatically runs via the run script)
1. **Validate file naming** - Run Step 0.5 pre-check (this will automatically run if using the run script)
2. **Use clear images** - Legible text, minimal glare, good lighting
3. **Test small batches** 

### During Processing
5. **Use run script** - Ensures all core steps execute correctly
6. **Monitor large jobs** - Check periodically for errors
7. **Allow time for batch** - Up to 24 hours per AI step (usually much faster!)

### After Processing
8. **Review outputs** - Start with sorting spreadsheet
9. **Verify high confidence** - Check before batch upload
10. **Document issues** - Note patterns for workflow improvement

---

## HTML Review Interface

### When to Use
- Visual interface to assess AI matches
- For batch sizes of 500 items or fewer

### How to Use
1. Choose "yes" when prompted during workflow run
2. Wait for Step 6 to complete
3. **Download entire results folder** to your computer
4. Unzip if compressed
5. Open `review-index-[date].html` in web browser
6. Make decisions and add notes
7. **Export to CSV** to save your work
8. Run Script 7 to automatically edit output files with cataloger decisions and to save decisions history - prompts in terminal for paths to cataloger decisions CSV and results folder

### Important Notes
- HTML runs locally 
- Decisions stored in browser local storage only
- **Must export to CSV to permanently save decisions**
- Not recommended for batches over 500 items (large folder size)
- Use JPEG images when possible (smaller files)
- Items may be sorted by confidence and then put back in their original order. 

---

## Support

**Questions, ideas, comments?**  
Kayode Ishola - kayode.ishola@austin.utexas.edu
Hannah Moutran - hlm2454@my.utexas.edu
---

## License

MIT License

---
## Original Cataloging Workflow (Steps 3b, 3c, 3d)

For records where no OCLC WorldCat match is found, the pipeline includes an integrated original cataloging workflow. This handles items that would otherwise remain in the backlog, consistent with a minimum viable record policy.

### When it runs

After Step 5 identifies records in the Cataloger Review (Low Confidence) group with no OCLC number, run each script in sequence:

    python ai-music-workflow/cd-processing/step_3b_original_cataloging.py
    python ai-music-workflow/cd-processing/step_3c_oclc_original_record.py path/to/decisions.csv
    python ai-music-workflow/cd-processing/step_3d_original_catalog_alma_import.py

### Step 3b — AI MARC Generation and Review Interface

- Reads Step 1 extracted metadata for all no-match records
- Uses GPT-4.1 to generate complete MARC records (100/110, 245, 264, 300, 336-338, 500, 505, 518, 588, 650, 700)
- All generated records include a 500 note marking them as AI-generated note, and a 588 source of description note with the generation date
- Creates an HTML review interface showing each CD image alongside the generated MARC record
- Cataloger approves, rejects, or holds each record and exports decisions to CSV
- Uses batch processing automatically for cost savings
- Saves a processing report to deliverables/ and AI_Music_Operations/original-cataloging/

### Step 3c — OCLC Record Creation

- Reads the approved decisions CSV from Step 3b review
- Deduplication: Searches OCLC by title, contributor, and UPC before creating any record — uses existing record if found rather than creating a duplicate
- Creates new bibliographic records in OCLC WorldCat via the Metadata API (WorldCatMetadataAPI:manage_bibs scope required)
- OCLC assigns a new OCLC number to each created record
- Sets your  holdings on all newly created records immediately
- Saves the assigned OCLC numbers to the workflow JSON for Step 3d
- Saves a report to deliverables/ and AI_Music_Operations/original-cataloging/

Required additional environment variable: OCLC_INSTITUTION_SYMBOL — your institution OCLC symbol

Your OCLC APIKey must have the WorldCatMetadataAPI scope enabled.

### Step 3d — Alma Import

- Reads workflow JSON for records with an assigned OCLC number from Step 3c
- Deduplication: Checks Alma for each OCLC number before creating any record — skips if already held
- Builds MARCXML with full AI-generated MARC fields and the assigned OCLC number in the 035 field
- Creates bib, holdings, and item records in Alma using the same library/location codes as the main workflow
- Internal note on all original records: AI Assisted Cataloging — Original Record
- Unsuppresses bib immediately — records are discoverable in Primo VE within 24-48 hours
- Writes a receipt CSV to deliverables/ and AI_Music_Operations/original-cataloging/[date]/

### OCLC Holdings for Original Records

Original records receive an OCLC number at creation time in Step 3c and holdings are set immediately. No separate oclc_holdings.py run is needed for these records.

### Output Files — Original Cataloging

| File | Location | Contents |
|------|----------|----------|
| original-catalog-index-[ts].html | results folder | HTML review interface entry point |
| original-cataloging-report-[ts].txt | deliverables/ | List of all processed records with titles and status |
| oclc-record-creation-report-[ts].txt | deliverables/ | OCLC numbers assigned per record |
| original-cataloging-alma-ids-[ts].csv | deliverables/ | MMS ID, Holding ID, Item ID per imported record |

All reports are also archived to AI_Music_Operations/original-cataloging/[date]/.
