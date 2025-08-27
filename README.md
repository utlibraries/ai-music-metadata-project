# AI Music Metadata Project

## Overview
This project automates the extraction and analysis of metadata from CD images. It processes images of compact discs, generates metadata using OpenAI's GPT-4o-mini model, searches for matches and cross-references this information with OCLC WorldCat records. The final output is LLM-generated metadata and OCLC record matches.

**NEW**: Now supports **OpenAI Batch Processing** for 50% cost savings and improved efficiency when processing large collections.

## Features
- **Image Description**: OpenAI's GPT-4o-mini extracts metadata fields such as title, artist, publisher, tracks, physical description, etc. from CD images
- **OCLC API Integration**: Uses AI-generated metadata to automatically query OCLC, returning up to five results
- **AI Analysis**: GPT-4o-mini compares previously generated metadata to OCLC results
- **Track and Year Verification**: Programmatically verifies track listings and publication years between metadata and OCLC records
- **Excel Report Generation**: Outputs all data into an Excel file for detailed analysis
- **Batch Processing**: Automatic cost optimization with 50% savings for large collections

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
   
   # Optional: Batch processing control
   export USE_BATCH_PROCESSING="auto"  # auto, true, or false
   ```

## Quick Start
```bash
# Run the complete pipeline (recommended)
python run_cd_processing.py

# Force batch processing for cost savings
USE_BATCH_PROCESSING=true python run_cd_processing.py

# Force real-time processing for immediate results
USE_BATCH_PROCESSING=false python run_cd_processing.py
```

## How It Works

### Image Format
CD images should be stored as pairs of either PNG or JPEG files, named following this style:
- Front image: `[barcode]a.jpeg`
- Back image: `[barcode]b.jpeg`
- Optional third image: `[barcode]c.jpeg`

### Processing Pipeline
1. **Step 1**: Extract metadata from CD images using GPT-4o ✅ *Supports batch processing*
2. **Step 1.5**: Clean up publication numbers and dates
3. **Step 2**: Query OCLC API with the extracted metadata
4. **Step 3**: Use GPT-4o to analyze OCLC results and assign confidence scores ✅ *Supports batch processing*
5. **Step 4**: Verify track listings and publication years to validate matches
6. **Step 5**: Create simplified spreadsheet with key fields and formatted OCLC results

## Batch Processing

### Automatic Optimization
The system automatically chooses the best processing method:
- **≤10 items**: Uses real-time processing for faster results
- **>10 items**: Uses batch processing for cost savings

### Benefits
- **50% Cost Reduction** on OpenAI API calls
- **Higher Rate Limits** for large collections
- **Same Quality** results as real-time processing
- **Reduced Monitoring** requirements

### Processing Times
- **Real-Time**: Immediate results, interactive progress
- **Batch**: Up to 24 hours (usually much faster), hands-off processing

## Project Structure
- `ai-music-step-1-cd.py`: Extracts metadata from CD images using GPT-4o-mini
- `ai-music-step-1.5-cd.py`: Cleans up pub numbers and dates
- `ai-music-step-2-cd.py`: Queries OCLC API with the extracted metadata
- `ai-music-step-3-cd.py`: Uses GPT-4o to analyze OCLC results and assign confidence scores
- `ai-music-step-4-cd.py`: Verifies track listings and publication year to validate matches
- `ai-music-step-5-cd.py`: Creates simplified spreadsheet with formatted OCLC results
- `query-testing-oclc.py`: Script to test OCLC queries directly in the terminal
- `run_cd_processing.py`: **Main run script with automatic batch processing optimization**

## Output
The final Excel file (Step 4) contains:
- Input Images (thumbnails)
- Barcode and AI-Generated Metadata
- OCLC Query and API Results
- LLM-Assessed Correct OCLC # with Confidence Score
- LLM Explanation and Other Potential Matches
- Track and Year Verification Results
- Library Holdings Status at our institution (Match Held at IXA?)

Additional outputs include a review spreadsheet with low confidence results, an Alma batch upload spreadsheet that contains only high confidence results that are not held by IXA, a spreadsheet organized to help sort physical items quickly according to returned results, and OpenAI API usage logs.

## Best Practices
1. **Use the main run script** (`run_cd_processing.py`) for automatic optimization
2. **Use automatic mode** - let the system choose based on batch size
3. **Clean data first** - remove duplicate or invalid images before processing
4. **Test with small batches** before processing large collections
5. **Plan timing** - allow extra time for batch processing
6. **Monitor large jobs** - check status periodically for batches >50 items

## Troubleshooting
- **Batch jobs stuck**: Check logs for errors, verify API quota, try smaller test batch
- **Mixed results**: Review individual response logs, check for data quality issues
- **Cost concerns**: Use automatic mode, clean data first, remove duplicates

For detailed error information, check the logs in the `logs` folder.

## Contact
For questions or support: **Hannah Moutran** - hlm2454@my.utexas.edu