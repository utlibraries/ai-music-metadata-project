import os
import glob
from openai import OpenAI
import openpyxl
from openpyxl.styles import Alignment
from datetime import datetime
import re

def find_latest_results_folder(prefix):
    """
    Find the latest folder that starts with the given prefix.
    For example, if prefix is 
    "final-workflow/on-demand-processing-cd/cd-output-folders/results-cd-5-",
    this function will search in "final-workflow/on-demand-processing-cd/cd-output-folders"
    for folders starting with that prefix and return the one with the most recent modification time.
    """
    base_dir = os.path.dirname(prefix)
    folder_prefix = os.path.basename(prefix)
    pattern = os.path.join(base_dir, folder_prefix + "*")
    
    matching_folders = glob.glob(pattern)
    if not matching_folders:
        return None

    latest_folder = max(matching_folders, key=os.path.getmtime)
    return latest_folder

# Load the API key from environment variable
client = OpenAI(api_key=os.getenv('OPENAI_HMRC_API_KEY'))

# Specify the folder prefix (adjust if needed)
base_dir_prefix = "final-workflow/on-demand-processing-cd/cd-output-folders/results-cd-5-"

# Find the latest results folder using the prefix.
results_folder = find_latest_results_folder(base_dir_prefix)
if not results_folder:
    print("No results folder found! Run the first script first.")
    exit()
    
print(f"Using results folder: {results_folder}")

# Look for step 2 files in the results folder
step2_files = [f for f in os.listdir(results_folder)
               if f.startswith('ai-music-step-2-') and f.endswith('.xlsx')]

if not step2_files:
    print("No step 2 files found in the results folder!")
    exit()
    
# Get the latest step 2 file
latest_file = max(step2_files)
workbook_path = os.path.join(results_folder, latest_file)

print(f"Processing file: {workbook_path}")

# Load the workbook and select the active worksheet
wb = openpyxl.load_workbook(workbook_path)
sheet = wb.active

# Define the columns
METADATA_COLUMN = 'E'
OCLC_RESULTS_COLUMN = 'G'
RESULT_COLUMN = 'H'
CONFIDENCE_SCORE_COLUMN = 'I'
EXPLANATION_COLUMN = 'J'
OTHER_MATCHES_COLUMN = 'K'  # New column for other potential matches

# Add headers for the new columns
sheet[f'{RESULT_COLUMN}1'] = 'LLM-Assessed Correct OCLC #'
sheet[f'{CONFIDENCE_SCORE_COLUMN}1'] = 'LLM Confidence Score'
sheet[f'{EXPLANATION_COLUMN}1'] = 'LLM Explanation'
sheet[f'{OTHER_MATCHES_COLUMN}1'] = 'Other Potential Matches'

# Set column widths
sheet.column_dimensions[RESULT_COLUMN].width = 30
sheet.column_dimensions[CONFIDENCE_SCORE_COLUMN].width = 20
sheet.column_dimensions[EXPLANATION_COLUMN].width = 40
sheet.column_dimensions[OTHER_MATCHES_COLUMN].width = 70

# Loop through the rows in the spreadsheet
for row in range(2, sheet.max_row + 1):  # Row 1 is the header
    metadata = sheet[f'{METADATA_COLUMN}{row}'].value
    oclc_results = sheet[f'{OCLC_RESULTS_COLUMN}{row}'].value

    if metadata and oclc_results and oclc_results != "No matching records found":
        prompt = (
f'''Analyze the following OCLC results based on the given metadata and determine which result is most likely correct. At times, there will be more than one record that seems to fit the criteria. This is because there are many duplicate records in OCLC. In that case, choose the best match, but if all things are more or less equal, prioritize records that have more holdings, and also records that are held by IXA. If there is no likely match, write "No matching records found".

**Important Instructions**:
1. Confidence Score: 0% indicates no confidence, and 100% indicates high confidence that we have found the correct OCLC number.
2. ***Key Fields***:
   - Title, artist/performer, and publisher are very important factors.
   - Titles in non-Latin scripts that match the metadata in meaning or transliteration should also be considered equivalent, as long as other key fields align.
   - Format: it is essential that this match. If it is marked in OCLC as an LP or digital music, for example, it is not the same record. The result must match the physical object described in the metadata (e.g., CD, CD-ROM, Enhanced CD, 2 audio discs, etc.). Records with vague or incomplete information (e.g., "audio disc" without specific format details like "4.75 in") should be scored lower than those with precise matches. If there is reason to believe that it may be a different version, check track listings and UPC if available.
   - Track Listings if available: if the metadata includes track listings, these should be compared to the OCLC record. These should be mostly identical, but minor differences are acceptable, such as punctuation or capitalization. If the track listings are significantly different, this is likely not the correct record.
   - UPC: if available, this should be compared to the OCLC record. If the UPC is different, this is likely not the correct record.
3. Holdings: If there are multiple records that match the metadata, prioritize records with more holdings.
4. Avoid Cognitive Bias:
   - Explicitly compare all records and do not default to the first-listed record without a thorough evaluation of all options.
5. If there is no likely match, write "No matching records found".

Format for Response:
- Your response must follow this format exactly:
  1. OCLC number: [number or 'No matching records found']
  2. Confidence score: [%]
  3. Explanation: [List of things that match as key value pairs. If there are multiple records that could be a match, explain why you chose the one you did. If there are no matches, explain why.]
  4. Other potential good matches: [List of other OCLC numbers that could be good matches, if applicable. No explanation, just numbers separated by commas.]

Metadata: {metadata}

OCLC Results: {oclc_results}
''')
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Read through the metadata and OCLC results, and determine if one of the OCLC records is a good match."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.5
            )

            analysis_result = response.choices[0].message.content.strip()

            oclc_number = "Not found"
            confidence_score = "0"
            explanation = "Could not parse response"
            other_matches = ""

            try:
                if "OCLC number:" in analysis_result:
                    oclc_part = analysis_result.split("OCLC number:")[1].split("\n")[0].strip()
                    oclc_number = ''.join(char for char in oclc_part if char.isdigit())

                if "Confidence score:" in analysis_result:
                    confidence_part = analysis_result.split("Confidence score:")[1].split("%")[0].strip()
                    try:
                        confidence_score = int(float(confidence_part))
                        confidence_score = min(100, max(0, confidence_score))
                    except ValueError:
                        confidence_score = 0

                try:
                    confidence_score = min(100, int(confidence_score))
                except ValueError:
                    confidence_score = 0

                if "Explanation:" in analysis_result:
                    explanation_parts = analysis_result.split("Explanation:")[1].split("Other potential good matches:")
                    explanation = explanation_parts[0].strip()
                    if explanation.endswith("4."):
                        explanation = explanation[:-2].strip()
                    explanation = re.sub(r'\s+\d+\.\s*$', '', explanation)

                if "Other potential good matches:" in analysis_result:
                    other_matches_part = analysis_result.split("Other potential good matches:")[1].strip()
                    if other_matches_part and oclc_results:
                        potential_matches = [num.strip() for num in other_matches_part.split(',') if num.strip()]
                        formatted_matches = []
                        for match_num in potential_matches:
                            match_info = ""
                            if f"OCLC Number: {match_num}" in oclc_results:
                                record_section = oclc_results.split(f"OCLC Number: {match_num}")[1].split("----------------------------------------")[0]
                                held_by_ixa = "No"
                                if "Held by IXA: Yes" in record_section:
                                    held_by_ixa = "Yes"
                                total_holdings = "0"
                                if "Total Institutions Holding:" in record_section:
                                    holdings_part = record_section.split("Total Institutions Holding:")[1].split("\n")[0].strip()
                                    total_holdings = holdings_part
                                title = ""
                                if "- Main Title:" in record_section:
                                    title_part = record_section.split("- Main Title:")[1].split("\n")[0].strip()
                                    title = title_part
                                specific_format = ""
                                if "- specificFormat:" in record_section:
                                    format_part = record_section.split("- specificFormat:")[1].split("\n")[0].strip()
                                    specific_format = format_part
                                if specific_format.strip() == "CD":
                                    match_info = f"OCLC: {match_num} | IXA: {held_by_ixa} | Holdings: {total_holdings}"
                                    if specific_format:
                                        match_info += f" | Format: {specific_format}"
                                    if title:
                                        match_info += f" | Title: {title}"
                                    if match_info:
                                        formatted_matches.append(match_info)
                        other_matches = "\n".join(formatted_matches) if formatted_matches else other_matches_part
                    else:
                        other_matches = other_matches_part

            except Exception as parsing_error:
                print(f"Error parsing response in row {row}: {parsing_error}")

            result_cell = sheet[f'{RESULT_COLUMN}{row}']
            confidence_cell = sheet[f'{CONFIDENCE_SCORE_COLUMN}{row}']
            explanation_cell = sheet[f'{EXPLANATION_COLUMN}{row}']
            other_matches_cell = sheet[f'{OTHER_MATCHES_COLUMN}{row}']

            result_cell.value = oclc_number if isinstance(oclc_number, str) else int(oclc_number)
            confidence_cell.value = confidence_score
            explanation_cell.value = explanation
            other_matches_cell.value = other_matches

            result_cell.alignment = Alignment(wrap_text=True)
            confidence_cell.alignment = Alignment(wrap_text=True)
            explanation_cell.alignment = Alignment(wrap_text=True)
            other_matches_cell.alignment = Alignment(wrap_text=True)

            print(f"\n--- Processed row {row}/{sheet.max_row} ---")
            print(f"OCLC Number: {result_cell.value}")
            print(f"Confidence Score: {confidence_cell.value}%")
            print(f"Explanation: {explanation_cell.value}")
            if other_matches:
                print(f"Other Potential Matches: \n{other_matches}")
            print("-----------------------------------")

        except Exception as e:
            print(f"Error processing row {row}: {e}")

current_date = datetime.now().strftime("%Y-%m-%d")
output_file = f"ai-music-step-3-cd-5-{current_date}.xlsx"
full_output_path = os.path.join(results_folder, output_file)
    
wb.save(full_output_path)
print(f"Results saved to {full_output_path}")
print("Summary: Process completed.")
