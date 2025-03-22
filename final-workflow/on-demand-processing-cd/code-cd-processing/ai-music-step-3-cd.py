import os
import glob
import time
from openai import OpenAI
import openpyxl
from openpyxl.styles import Alignment
from datetime import datetime
import re

def find_latest_results_folder(prefix):
    # Get the parent directory of the prefix
    base_dir = os.path.dirname(prefix)
    pattern = os.path.join(base_dir, "results-*")
    
    matching_folders = glob.glob(pattern)
    if not matching_folders:
        return None

    latest_folder = max(matching_folders)
    
    return latest_folder

# Load the API key from environment variable
client = OpenAI(api_key=os.getenv('OPENAI_HMRC_API_KEY'))

def main():
    # Start timing the entire script execution
    script_start_time = time.time()
    
    # Specify the folder prefix (adjust if needed)
    base_dir_prefix = "final-workflow/on-demand-processing-cd/cd-output-folders/results-"

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

    # Create a summary sheet for token and timing metrics
    if "TokenSummary" not in wb.sheetnames:
        summary_sheet = wb.create_sheet("TokenSummary")
    else:
        summary_sheet = wb["TokenSummary"]
    
    # Set up the summary sheet headers
    summary_headers = [
        "Total Rows Processed", "Total API Calls", "Failed API Calls",
        "Total Processing Time (s)", "API Time (s)", "Average Time per Call (s)",
        "Total Prompt Tokens", "Total Completion Tokens", "Total Tokens",
        "Average Tokens per Call", "Estimated Cost ($0.003/1K)", "Date Completed"
    ]
    summary_sheet.append(summary_headers)
    
    for col, header in enumerate(summary_headers, start=1):
        summary_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 20

    # Define the columns
    METADATA_COLUMN = 'E'
    OCLC_RESULTS_COLUMN = 'G'
    RESULT_COLUMN = 'H'
    CONFIDENCE_SCORE_COLUMN = 'I'
    EXPLANATION_COLUMN = 'J'
    OTHER_MATCHES_COLUMN = 'K'  # Column for other potential matches
    PROCESSING_TIME_COLUMN = 'L'  # New column for processing time
    PROMPT_TOKENS_COLUMN = 'M'   # New column for prompt tokens
    COMPLETION_TOKENS_COLUMN = 'N'  # New column for completion tokens
    TOTAL_TOKENS_COLUMN = 'O'    # New column for total tokens


    # Add headers for all columns including new ones
    sheet[f'{RESULT_COLUMN}1'] = 'LLM-Assessed Correct OCLC #'
    sheet[f'{CONFIDENCE_SCORE_COLUMN}1'] = 'LLM Confidence Score'
    sheet[f'{EXPLANATION_COLUMN}1'] = 'LLM Explanation'
    sheet[f'{OTHER_MATCHES_COLUMN}1'] = 'Other Potential Matches'
    sheet[f'{PROCESSING_TIME_COLUMN}1'] = 'Processing Time (s)'
    sheet[f'{PROMPT_TOKENS_COLUMN}1'] = 'Prompt Tokens'
    sheet[f'{COMPLETION_TOKENS_COLUMN}1'] = 'Completion Tokens'
    sheet[f'{TOTAL_TOKENS_COLUMN}1'] = 'Total Tokens'

    # Set column widths
    sheet.column_dimensions[RESULT_COLUMN].width = 30
    sheet.column_dimensions[CONFIDENCE_SCORE_COLUMN].width = 20
    sheet.column_dimensions[EXPLANATION_COLUMN].width = 40
    sheet.column_dimensions[OTHER_MATCHES_COLUMN].width = 70
    sheet.column_dimensions[PROCESSING_TIME_COLUMN].width = 18
    sheet.column_dimensions[PROMPT_TOKENS_COLUMN].width = 15
    sheet.column_dimensions[COMPLETION_TOKENS_COLUMN].width = 18
    sheet.column_dimensions[TOTAL_TOKENS_COLUMN].width = 15

    # Initialize counters for summary
    total_rows = 0
    successful_calls = 0
    failed_calls = 0
    total_api_time = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0

    for row in range(2, sheet.max_row + 1):  # Row 1 is the header
        row_start_time = time.time()
        metadata = sheet[f'{METADATA_COLUMN}{row}'].value
        oclc_results = sheet[f'{OCLC_RESULTS_COLUMN}{row}'].value

        # Skip rows with missing data or "No matching records" message
        if not metadata or not oclc_results or oclc_results == "No matching records found" or oclc_results.strip() == "":
            print(f"Skipping row {row}: Missing data or no OCLC results")
            # Explicitly mark these rows as skipped in the results
            sheet[f'{RESULT_COLUMN}{row}'].value = "No OCLC data to process"
            sheet[f'{CONFIDENCE_SCORE_COLUMN}{row}'].value = 0
            sheet[f'{EXPLANATION_COLUMN}{row}'].value = "Skipped: No valid OCLC results to analyze"
            # Increment the skipped counter for summary metrics
            total_rows += 1  # Still count this as a processed row
            failed_calls += 1  # Count as a "failed" call for metrics
            continue
        
        # Only rows with valid data will reach this point
        total_rows += 1
            
        prompt = (
f'''Analyze the following OCLC results based on the given metadata and determine which result is the best match. Methodically go through each record, choose the top 3, then consider them again and choose the record that matches the most elements in the metadata. If two or more records tie for best match, prioritize records that have more holdings and that are held by IXA. If there is no likely match, write "No matching records found".

**Important Instructions**:
1. Confidence Score: 0% indicates no confidence, and 100% indicates high confidence that we have found the correct OCLC number. At or below 80%, the record will be checked by a cataloger. 
2. ***Key Fields in order of importance***:
   - UPC/Product Code (exact match is HIGHEST priority if available in both metadata and OCLC record)
   - Title 
   - Artist/Performer
   - Contributors (some matching - not all need to match)
   - Publisher Name (exact match preferred; corporate ownership relationships like "Columbia is part of Sony" should NOT be considered a match)
   - Physical Description (should make sense for a CD)
   - Content (track listings - these should be mostly similar, with small differences in spelling or punctuation)
   - Year (this is a key field only if present in the metadata, AND not marked as being a sticker date)
3. ***Notes on Matching Special Cases***:
   - Titles in non-Latin scripts that match in meaning or transliteration should also be considered equivalent.
   - Date: If marked as a 'sticker date', this is the date of acquisition, while the date in the OCLC record is the copyright date.
   - Different releases in different regions (e.g., Japanese release vs. US release) should be treated as different records even if title and content match.
5. If there is no likely match, write "No matching records found" and set the confidence score as 0.

Format for Response:
- Your response must follow this format exactly:
  1. OCLC number: [number or 'No matching records found']
  2. Confidence score: [%]
  3. Explanation: [List of things that match as key value pairs. If there are multiple records that could be a match, explain why you chose the one you did. If there are no matches, explain why.]
  4. Other potential good matches: [List of other OCLC numbers that could be good matches, if applicable. No explanation, just numbers separated by commas.]
  
Once you have responded, go back through the response that you wrote and carefully verify each piece of information. If you find a mistake, look for a better record. If there isn't one, reduce the confidence score to 80% or lower. If there is one, once again carefully verify all the facts that support your choice. If you still can't find a match, write "No matching records found" and set the confidence score as 0.

Metadata: {metadata}

OCLC Results: {oclc_results}
''')
        try:
                # Time the API call
                api_call_start = time.time()
                response = client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": "You are a music cataloger.  You are very knowledgeable about music cataloging best practices, and also have incredible attention to detail.  Read through the metadata and OCLC results carefully, and determine which of the OCLC results looks like the best match. If there is no likely match, write 'No matching records found'.  If you make a mistake, you would feel very bad about it, so you always double check your work."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=1000,
                    temperature=0.5
                )
                api_call_duration = time.time() - api_call_start
                total_api_time += api_call_duration
                
                # Extract token usage
                prompt_tokens = response.usage.prompt_tokens
                completion_tokens = response.usage.completion_tokens
                tokens_used = prompt_tokens + completion_tokens
                
                # Add to totals
                total_prompt_tokens += prompt_tokens
                total_completion_tokens += completion_tokens
                total_tokens += tokens_used
                
                successful_calls += 1

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

                # Calculate total processing time for this row
                row_duration = time.time() - row_start_time

                # Update cells with results and metrics
                result_cell = sheet[f'{RESULT_COLUMN}{row}']
                confidence_cell = sheet[f'{CONFIDENCE_SCORE_COLUMN}{row}']
                explanation_cell = sheet[f'{EXPLANATION_COLUMN}{row}']
                other_matches_cell = sheet[f'{OTHER_MATCHES_COLUMN}{row}']
                time_cell = sheet[f'{PROCESSING_TIME_COLUMN}{row}']
                prompt_tokens_cell = sheet[f'{PROMPT_TOKENS_COLUMN}{row}']
                completion_tokens_cell = sheet[f'{COMPLETION_TOKENS_COLUMN}{row}']
                total_tokens_cell = sheet[f'{TOTAL_TOKENS_COLUMN}{row}']

                result_cell.value = oclc_number if isinstance(oclc_number, str) else int(oclc_number)
                confidence_cell.value = confidence_score
                explanation_cell.value = explanation
                other_matches_cell.value = other_matches
                time_cell.value = round(row_duration, 2)
                prompt_tokens_cell.value = prompt_tokens
                completion_tokens_cell.value = completion_tokens
                total_tokens_cell.value = tokens_used

                # Set cell alignment
                for cell in [result_cell, confidence_cell, explanation_cell, other_matches_cell, 
                            time_cell, prompt_tokens_cell, completion_tokens_cell, total_tokens_cell]:
                    cell.alignment = Alignment(wrap_text=True)

                print(f"\n--- Processed row {row}/{sheet.max_row} ---")
                print(f"OCLC Number: {result_cell.value}")
                print(f"Confidence Score: {confidence_cell.value}%")
                print(f"Processing Time: {round(row_duration, 2)}s (API: {round(api_call_duration, 2)}s)")
                print(f"Tokens: {tokens_used} (Prompt: {prompt_tokens}, Completion: {completion_tokens})")
                print(f"Explanation: {explanation_cell.value}")
                if other_matches:
                    print(f"Other Potential Matches: \n{other_matches}")
                print("-----------------------------------")

        except Exception as e:
                failed_calls += 1
                print(f"Error processing row {row}: {e}")

    # Calculate script metrics
    script_duration = time.time() - script_start_time
    avg_call_time = total_api_time / successful_calls if successful_calls > 0 else 0
    avg_tokens_per_call = total_tokens / successful_calls if successful_calls > 0 else 0
    estimated_cost = (total_tokens / 1000) * 0.003  # Estimate based on $0.003 per 1K tokens
    
    # Add summary data
    summary_sheet.append([
        total_rows,
        successful_calls,
        failed_calls,
        round(script_duration, 2),
        round(total_api_time, 2),
        round(avg_call_time, 2),
        total_prompt_tokens,
        total_completion_tokens,
        total_tokens,
        round(avg_tokens_per_call, 2),
        round(estimated_cost, 4),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ])

    # Create a token usage log file
    log_file_path = os.path.join(results_folder, "oclc_token_usage_log.txt")
    with open(log_file_path, "w") as log_file:
        log_file.write(f"Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Total rows processed: {total_rows}\n")
        log_file.write(f"Successful API calls: {successful_calls}\n")
        log_file.write(f"Failed API calls: {failed_calls}\n")
        log_file.write(f"Total script execution time: {round(script_duration, 2)} seconds\n")
        log_file.write(f"Total API time: {round(total_api_time, 2)} seconds\n")
        log_file.write(f"Average API call time: {round(avg_call_time, 2)} seconds\n")
        log_file.write(f"Total prompt tokens: {total_prompt_tokens}\n")
        log_file.write(f"Total completion tokens: {total_completion_tokens}\n")
        log_file.write(f"Total tokens: {total_tokens}\n")
        log_file.write(f"Average tokens per call: {round(avg_tokens_per_call, 2)}\n")
        log_file.write(f"Estimated cost ($0.003 per 1K tokens): ${round(estimated_cost, 4)}\n")

    current_date = datetime.now().strftime("%Y-%m-%d")
    output_file = f"ai-music-step-3-{current_date}.xlsx"
    full_output_path = os.path.join(results_folder, output_file)
        
    wb.save(full_output_path)
    print(f"\nResults saved to {full_output_path}")
    print(f"Token usage log saved to {log_file_path}")
    print("\nSummary:")
    print(f"- Total rows processed: {total_rows}")
    print(f"- Successful API calls: {successful_calls}")
    print(f"- Failed API calls: {failed_calls}")
    print(f"- Total script execution time: {round(script_duration, 2)} seconds")
    print(f"- Total API time: {round(total_api_time, 2)} seconds")
    print(f"- Total tokens used: {total_tokens} (Prompt: {total_prompt_tokens}, Completion: {total_completion_tokens})")
    print(f"- Estimated cost: ${round(estimated_cost, 4)}")

if __name__ == "__main__":
    main()