# AI Music Metadata Project

## Overview
Automates metadata extraction and OCLC matching for CD and LP collections. This project uses AI for basic metadata extraction from images and for analyzing OCLC match results. It then searches OCLC WorldCat using the generated metadata and creates ready-to-use cataloging files.

Optionally, users can generate an HTML review interface. The upside is that it provides a convenient way to review results before moving forward. The downside is that it is served locally on your computer; to support this, the script copies all required images into the results folder, making it best for batches of 500 items or less. To support the review work done using the HTML site, there is a script to incorporate the cataloger decisions into the cataloging files (details below). 

Another optional component is the batch upload to Alma Sandbox, which is designed to use the generated alma-batch-upload CSV file as the input set.

**Separate workflows for CDs and LPs** - each format has its own processing folder with dedicated scripts and configurations.

***Note: This repository is under active development.***


---

## Processing Pipeline

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

---

## Quick Start

### Run Workflow - Steps .5 - 6 

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
- `39015012345678a.jpeg`- Front image (required)
- `39015012345678b.jpeg`- Back image (optional)
- `39015012345678c.jpeg`- Additional image (optional)

### Format
- **Supported**: JPEG (.jpg, .jpeg) or PNG (.png)
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
Hannah Moutran - hlm2454@my.utexas.edu

---

## License

MIT License

---