# AI Music Metadata Project

## Overview
This project automates metadata extraction and analysis from CD and LP images. It processes images of the items, generates initial metadata, searches for matches and cross-references this information with OCLC WorldCat records. CD and LP processing use separate workflows in dedicated folders with format-specific input and output directories. 

## Features
- **Image Description**: OpenAI LLM (default is GPT-4o) creates initial metadata, extracting fields such as title, artist, publisher, tracks, physical description, etc. from CD images
- **OCLC API Integration**: AI-generated metadata is used to automatically generate queries of OCLC Worldcat Search API, returning no more than 10 total results and truncating excessively long result contents
- **AI Analysis**: AI-generated metadata and OCLC results are sent to LLM (default is GPT-4o-mini) and it is prompted to choose the best match, give a confidence score based on similarity strength, and provide reasoning for those choices
- **Track and Year Verification**: Programmatically verifies track listings and publication years between metadata and OCLC records
- **OpenAI Batch Processing**: Automatic cost optimization with 50% savings for runs with over 10 image groups (a group could be between 1-3 images if they are named with the same barcode)

## Installation
1. Clone this repository to your local machine
2. Install the required dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   export OCLC_CLIENT_ID="your-oclc-client-id"  
   export OCLC_SECRET="your-oclc-secret"
   ```

## Quick Start
```bash
# Run the complete pipeline - for CDs or LPs 
python ai-music-workflow/cd-processing/run_cd_processing.py 
python ai-music-workflow/lp-processing/run_lp_processing.py

# Force batch processing for cost savings
USE_BATCH_PROCESSING=true python ai-music-workflow/cd-processing/run_cd_processing.py
USE_BATCH_PROCESSING=true python ai-music-workflow/cd-processing/run_lp_processing.py

# Force real-time processing for immediate results
USE_BATCH_PROCESSING=false python ai-music-workflow/cd-processing/run_cd_processing.py
USE_BATCH_PROCESSING=false python ai-music-workflow/cd-processing/run_lp_processing.py
```

## How It Works

### Image Format
CD and LP images should be stored as groups of either PNG or JPEG files, named following this style:
- Front image: `[barcode]a.jpeg`
- Back image: `[barcode]b.jpeg`
- Optional third image: `[barcode]c.jpeg`
Each barcode will create an image group that will be processed as one item.  

### Configurations 
Model configurations, file paths (including the images folder to process), and other important settings, such as OCLC search parameters, can be found in the format-specific config file.

### Directory Structure 
ai-music-metadata-project/ai-music-workflow
├── cd-processing/                   # CD workflow folder
│   ├── run_cd_processing.py        # Main CD processing script
│   ├── [other CD script files...]
│   ├── cd-image-folders/              # CD image folders go here
│   │   └── your_collection_name/        # Individual collection folder
│   │       ├── barcode1a.jpeg     # Front image
│   │       ├── barcode1b.jpeg     # Back image
│   │       ├── barcode1c.jpeg     # Optional third image
│   │       └── ...
│   └── cd-output-folders/             # Auto-generated CD outputs - this folder is auto-generated
│       └── [timestamped-folders]/
├── lp-processing/                   # LP workflow folder  
│   ├── run_lp_processing.py        # Main LP processing script
│   ├── [other LP script files...]
│   ├── lp-image-folders/              # LP image folders go here
│   │   └── your_collection_name/        # Individual collection folder
│   │       ├── barcode1a.jpeg     # Front image
│   │       ├── barcode1b.jpeg     # Back image
│   │       ├── barcode1c.jpeg     # Optional third image
│   │       └── ...
│   └── lp-output-folders/             # Auto-generated LP outputs - this folder is auto-generated
│       └── [timestamped-folders]/
└── Requirements, README, License, Technical Guide, Cataloger Guide, gitignore      

### Processing Pipeline
1. **Step 1**: Extract metadata from CD images using Large Language Model (LLM)
2. **Step 1.5**: Clean up publication numbers and dates
3. **Step 2**: Query OCLC API with the extracted metadata
4. **Step 3**: Use LLM to analyze OCLC results and assign confidence scores
5. **Step 4**: Verify track listings and publication years to validate matches
6. **Step 5**: Create final output files 

### Automatic Optimization
The system automatically chooses the best processing method:
- **≤10 items**: Uses real-time processing for faster results
- **>10 items**: Uses batch processing for cost savings

### Benefits of Batch Processing
- **50% Cost Reduction** on OpenAI API calls
- **Higher Rate Limits** for large collections
- **Same Quality** results as real-time processing
- **Less Time** when processing very large batches (24 hours or less for each AI step, usually much less)

## Outputs
- **Full Workflow**: JSON file and excel spreadsheet containing Input Images (thumbnails - excel only), Item Barcode, AI-Generated Metadata, OCLC Queries, OCLC Results, LLM-Suggested OCLC # with Confidence Score, LLM Explanation, Other Potential Matches, Track and Year Verification Results, and Library Holdings Status at our institution (Match Held at IXA?)

- **Cataloging Tools**:  
The workflow creates several key files for catalogers and/or other library professionals:
- Cataloger Guide: Full explanation of all the outputs generated by the workflow and how to use them efficiently 
- Technical Guide: Documentation on the various logs and how they can be used to improve and monitor workflow
- Sorting Spreadsheet: Categorizes all items into High Confidence, Held by UT Libraries, Low Confidence, and Duplicates groups (to help with sorting physical items)
- Batch Upload File: Pipe-delimited file for high-confidence matches ready for Library Services Platform import
- Cataloger Review Spreadsheet: Tracking spreadsheet for next steps taken with low-confidence matches
- Low Confidence Review Text File: Initial AI-generated metadata, AI-Suggested OCLC Match, and alternative matches for each low confidence item
- MARC Text File: Basic MARC records to be used for faster processing in the case of original cataloging

- **Logs Folder**: OCLC API search log, LLM response logs, LLM token usage logs, error logs 

## Best Practices
1. **Use the main run script** for automatic optimization and to insure that no steps are accidentally skipped
2. **Use clear images** - legible, glare-free image text will produce best results
3. **Clean data first** - remove duplicate or invalid images
4. **Test with small batches** before processing large collections
5. **Plan timing** - allow extra time for batch processing
6. **Monitor large jobs** - check status periodically for larger batches

## Troubleshooting
- **Batch jobs stuck**: Check logs for errors, verify API quota, try smaller test batch
- **Mixed results**: Review individual response logs, check for data quality issues
- **Cost concerns**: Use automatic mode, clean data first, remove duplicates

## Contact
This repository is a work in progress!  
Please direct questions, ideas, or comments to: **Hannah Moutran** - hlm2454@my.utexas.edu