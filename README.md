# AI Music Metadata Project

## Overview
Automates metadata extraction and OCLC matching for CD and LP collections. The workflow uses AI to extract metadata from images, searches OCLC WorldCat, analyzes matches, and creates ready-to-use cataloging files.

**Separate workflows for CDs and LPs** - each format has its own processing folder with dedicated scripts and configurations.

**Note: This repository is under active development.**
---

## Features
- **AI Metadata Extraction**: GPT-4o reads CD/LP images and extracts title, artist, publisher, tracks, dates, and physical description
- **OCLC Integration**: Automated WorldCat searches return up to 10 matching records
- **AI Match Analysis**: GPT-4.1-mini evaluates matches, assigns confidence scores, and explains reasoning
- **Verification**: Automatic track listing and publication year validation
- **Batch Processing**: 50% cost savings for batches over 10 items (automatic)
- **HTML Review Interface** (Optional): Visual review of matches with images

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
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   export OCLC_CLIENT_ID="your-oclc-client-id"
   export OCLC_SECRET="your-oclc-secret"
   ```

---

## Quick Start

### Run Complete Workflow

**For CDs:**
```bash
python ai-music-workflow/cd-processing/run_cd_processing.py
```

**For LPs:**
```bash
python ai-music-workflow/lp-processing/run_lp_processing.py
```

The script will:
- Automatically choose batch vs. real-time processing
- Prompt whether to generate HTML review interface (Step 6)
- Run all processing steps in sequence
- Create organized output files

### Force Processing Mode (Optional)

**Force batch processing** (50% cost savings):
```bash
USE_BATCH_PROCESSING=true python run_cd_processing.py
```

**Force real-time processing** (faster for small batches):
```bash
USE_BATCH_PROCESSING=false python run_cd_processing.py
```

---

## Image Directory Structure

Save each collection of images in its own subfolder within `[cd/lp]-image-folders/`.

**Example path:** 
`ai-music-metadata-project/ai-music-workflow/cd-processing/cd-image-folders/cd-scans-100/`

The workflow will automatically generate an outputs folder with organized results.

---

## Image Requirements

### Naming Convention
Images must be named with barcode + letter suffix:
- `barcode_a.jpeg` - Front image (required)
- `barcode_b.jpeg` - Back image (optional)
- `barcode_c.jpeg` - Additional image (optional)

**Examples:**
- `39015012345678a.jpeg`
- `39015012345678b.jpeg`
- `39015012345678c.jpeg`

### Format
- **Supported**: JPEG (.jpg, .jpeg) or PNG (.png)
- **Best quality**: Clear, legible text, minimal glare
- **Recommendation**: JPEG for smaller file sizes (especially if generating HTML)

### Organization
Place all images for a collection in a single folder:
```
cd-image-folders/
└── spring2024_collection/
    ├── barcode1a.jpeg
    ├── barcode1b.jpeg
    ├── barcode2a.jpeg
    └── ...
```

---

## Processing Pipeline

1. **Step 0.5**: Validate image file naming (optional pre-check)
2. **Step 1**: Extract metadata from images using AI
3. **Step 1.5**: Clean and normalize extracted metadata
4. **Step 2**: Query OCLC WorldCat API
5. **Step 3**: AI analysis of OCLC matches with confidence scoring
6. **Step 4**: Verify track listings and publication years
7. **Step 5**: Create final output files organized in subfolders
8. **Step 6** (Optional): Generate HTML review interface with images

---

## Output Files

### `deliverables/` folder - Working files for catalogers

1. **sorting-spreadsheet-[date].xlsx**
   - All items categorized: High Confidence, Held by Library, Low Confidence, Duplicates
   - Use to physically organize materials

2. **batch-upload-alma-[cd/lp]-[timestamp].txt**
   - High-confidence matches ready for import
   - Format: `OCLC_NUMBER|BARCODE|TITLE`

3. **tracking-spreadsheet-catalogers-[date].xlsx**
   - Interactive tracking for low-confidence items
   - Yellow highlighting for items needing review
   - Dropdown status menu, auto-populated OCLC numbers

4. **low-confidence-matches-review-[date].txt**
   - Detailed review information for each low-confidence item
   - AI-generated metadata, suggested matches, alternatives

5. **marc-formatted-low-confidence-matches-[date].txt**
   - Basic MARC records for original cataloging
   - Based on AI-extracted metadata

### `guides/` folder - Documentation

- **CATALOGER_GUIDE.txt** - How to use workflow outputs
- **TECHNICAL_GUIDE.txt** - Quality control and troubleshooting

### `data/` folder - Workflow tracking

- **full-workflow-data-[cd/lp]-[timestamp].json** - Complete processing log
- **full-workflow-data-[cd/lp]-[timestamp].xlsx** - Excel version with thumbnails
- **logs/** - API responses, token usage, errors, metrics

### Main results folder (if HTML is generated)

- **review-index-[date].html** - Start page for visual review
- **review-page-[#]-[date].html** - Individual review pages
- **images/** - Copies of all processed images

---

## Automatic Optimization

The system automatically chooses processing mode based on batch size:

| Batch Size | Method | Benefits |
|------------|--------|----------|
| ≤10 items | Real-time | Faster results (minutes) |
| >10 items | Batch | 50% cost savings, higher rate limits |

Both methods produce identical quality results.

---

## Configuration

Edit format-specific config files to customize:

**CD workflow**: `cd-processing/cd_workflow_config.py`

**LP workflow**: `lp-processing/lp_workflow_config.py`

Settings include:
- Model selection for each step (OpenAI models only)
- Image folder paths

---

## Best Practices

### Before Processing
1. **Validate file naming** - Run Step 0.5 pre-check (this will automatically run if using the run script)
2. **Use clear images** - Legible text, minimal glare, good lighting
3. **Remove duplicates** - Check for duplicate barcodes
4. **Test small batches** - Try 10-20 items first

### During Processing
5. **Use run script** - Ensures all steps execute correctly
6. **Monitor large jobs** - Check periodically for errors
7. **Allow time for batch** - Up to 24 hours per AI step (usually much faster)

### After Processing
8. **Review outputs** - Start with sorting spreadsheet
9. **Verify high confidence** - Spot-check before batch upload
10. **Document issues** - Note patterns for workflow improvement

---

## HTML Review Interface

### When to Use
- Visual verification of AI matches
- Batch sizes under 500 items
- When you have time to download and review locally

### How to Use
1. Choose "yes" when prompted during workflow run
2. Wait for Step 6 to complete
3. **Download entire results folder** to your computer
4. Unzip if compressed
5. Open `review-index-[date].html` in web browser
6. Make decisions and add notes
7. **Export to CSV** to save your work

### Important Notes
- HTML runs locally (no internet connection needed for viewing)
- Decisions stored in browser local storage only
- **Must export to CSV to permanently save decisions**
- Not recommended for batches over 500 items (large folder size)
- Use JPEG images when possible (smaller files)

---

## Troubleshooting

**For troubleshooting guidance, see TECHNICAL_GUIDE.txt in the guides folder.**

---

## Support

**Questions or issues?**
Contact: Hannah Moutran - hlm2454@my.utexas.edu

**Before contacting:**
1. Check error logs in `data/logs/`
2. Review TECHNICAL_GUIDE.txt
3. Try a small test batch to isolate issues
4. Note your batch size, format (CD/LP), and specific errors

---

## License

MIT License

---