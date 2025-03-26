# AI Music Metadata Project
## Overview
This project automates the extraction and analysis of metadata from CD images. It processes images of compact discs, generates metadata using OpenAI's GPT-4o-mini model, searches for matches and cross-references this information with OCLC WorldCat records. The final output is LLM-generated metadata and OCLC record matches.

## Features
- **Image Description**: Open AI's GPT-4o-mini extracts metadata fields such as title, artist, publisher, tracks, physical description, etc. from CD images
- **OCLC API Integration**: Uses AI-generated metadata to automatically query OCLC, returning up to five results
- **AI Analysis**: GPT-4o-mini compares previously generated metadata to OCLC results
- **Track and Year Verification**: Programatically verifies track listings and publication years between metadata and OCLC records, providing a buffer against any assumptions or overgeneralizations made by the LLM
- **Excel Report Generation**: Outputs all data into an Excel file for detailed analysis

## Prerequisites
To use this project, you must have access to:
- **OpenAI API** for LLM integration
- **OCLC Search API** for querying OCLC WorldCat records

## Installation
1. Clone this repository to your local machine
2. Install the required dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Environment Variables
To run this project, you will need to set up the following environment variables:
- `OCLC_CLIENT_ID` and `OCLC_SECRET`: Credentials for accessing the OCLC API
- `OPENAI_HMRC_API_KEY`: Your OpenAI API key

## Project Structure
- `ai-music-step-1-cd.py`: Extracts metadata from CD images using GPT-4o-mini
- `ai-music-step-1.5-cd.py`: Cleans up pub numbers and dates - only allows for standalone years
- `ai-music-step-2-cd.py`: Queries OCLC API with the extracted metadata
- `ai-music-step-3-cd.py`: Uses GPT-4o to analyze OCLC results and assign confidence scores
- `ai-music-step-4-cd.py`: Verifies track listings and publication year to validate the matches
- `ai-music-step-5-cd.py`: Creates a simplified spreadsheet with key fields and formatted OCLC results for easier detailed review of results.  
- `query-testing-oclc.py`: This is a script to test OCLC queries directly in the terminal - can be helpful for testing querying strategies and editing automated queries in step 2 to fit your collection.

This project processes CD images stored as pairs of JPEG files. Each CD is represented by two images: one for the front cover and one for the back cover. A third image may be used if necessary.  The image groups follow a specific naming convention:

Front image: [barcode]a.jpeg
Back image: [barcode]b.jpeg
Optional third image: [barcode]c.jpeg

The starter kit here contains:
- A  small set of scanned images of 5 CDs
- A larger set of scanned images of 100 CDs
- A folder will be created to store the excel files created by running the code

## Output
**Excel files for Steps 1-4**: 
These build on each other - the file created in Step 4 contains the following columns:
1. Input Images: Thumbnails of the CD images used for extraction
2. Barcode: The unique identifier for each CD
3. AI-Generated Metadata: The metadata extracted by GPT-4o-mini
4. OCLC Query: Queries generated from the metadata
5. OCLC API Results: Results retrieved from the OCLC API
6. LLM-Assessed Correct OCLC #: The most likely correct OCLC number, according to the LLM
7. LLM Confidence Score: A confidence score (0-100%) for the assessment
8. LLM Explanation: A detailed explanation for the assessment
9. Other Potential Matches: Alternative OCLC numbers that might be matches
10. Track Verification Results: Analysis of track listing similarity between metadata and OCLC record
11. Year Verification Results: Analysis of publication year similarity between metadata and OCLC record
12. Match Held at IXA?: States whether or not the match is held at our library (Y/N)
13. Potential Matches at IXA?: States whether or not potential matches listed are held at our library (Y/N)

**Additional Outputs:** 
- Review spreadsheet that includes the barcodes, OCLC matches, confidence score, other potential matches, and formatted OCLC bibliographic information for the OCLC matches. 
- A log of the OpenAI API usage for Step 1.
- A log of the OpenAI API usage for Step 3.  

## Contact
For any questions, please reach out to:
- **Hannah Moutran** - hlm2454@my.utexas.edu