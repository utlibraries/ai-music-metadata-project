# AI Music Metadata Project
## Overview
This project automates the extraction and analysis of metadata from CD images. It processes images of compact discs, generates metadata using OpenAI's GPT-4o-mini model, searches for matches and cross-references this information with OCLC WorldCat records. The final output is an Excel file with LLM-generated metadata and OCLC record matches.

## Features
- **Image Description**: Open AI's GPT-4o-mini extracts metadata fields such as title, artist, publisher, tracks, physical description, etc. from CD images
- **OCLC API Integration**: Uses AI-generated metadata to automatically query OCLC, returning up to five results
- **AI Analysis**: GPT-4o-mini compares previously generated metadata to OCLC results
- **Track Verification**: Programatically verifies track listings between metadata and OCLC records, providing a buffer against any assumptions or overgeneralizations made by the AI
- **Excel Report Generation**: Outputs all data into an Excel file for detailed analysis

## Prerequisites
To use this project, you must have access to:
- **OpenAI API** for GPT-4o-mini integration
- **OCLC Search API** for querying OCLC WorldCat records

Make sure to have the following installed:
- Python 3.7 or higher
- Pip (Python package manager)

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
- `ai-music-step-1.5-cd.py`: Cleans up pub numbers to leave only UPCs
- `ai-music-step-2-cd.py`: Queries OCLC API with the extracted metadata
- `ai-music-step-3-cd.py`: Uses GPT-4o-mini to analyze OCLC results and assign confidence scores
- `ai-music-step-4-cd.py`: Verifies track listings to validate the matches
- `ai-music-step-4-track-and-year-cd.py`: Verifies track listings and publication year to validate the matches (this is an option, but will result in many more non-matches)
- `query-testing-oclc.py`: This is a script to test OCLC queries directly in the terminal - can be helpful for testing querying strategies

The project processes CD images stored as pairs of JPEG files. Each CD is represented by two images: one for the front cover and one for the back cover. A third image may be used if necessary.  The image groups follow a specific naming convention:

Front image: [barcode]a.jpeg
Back image: [barcode]b.jpeg
Optional third image: [barcode]c.jpeg

The starter kit here contains:
- A  small set of scanned images of 5 CDs
- A larger set of scanned images of 100 CDs
- A folder will be created to store the excel files created by running the code

## Output
The final Excel file contains the following columns:
- **Input Images**: Thumbnails of the CD images used for extraction
- **Barcode**: The unique identifier for each CD
- **AI-Generated Metadata**: The metadata extracted by GPT-4o-mini
- **OCLC Query**: Queries generated from the metadata
- **OCLC API Results**: Results retrieved from the OCLC API
- **LLM-Assessed Correct OCLC #**: The most likely correct OCLC number
- **LLM Confidence Score**: A confidence score (0-100%) for the assessment
- **LLM Explanation**: A detailed explanation for the assessment
- **Other Potential Matches**: Alternative OCLC numbers that might be matches
- **Track Verification Results**: Analysis of track listing similarity between metadata and OCLC record
- **Year Verification Results**: Analysis of publication year similarity between metadata and OCLC record

## Contact
For any questions, please reach out to:
- **Hannah Moutran** - hlm2454@my.utexas.edu