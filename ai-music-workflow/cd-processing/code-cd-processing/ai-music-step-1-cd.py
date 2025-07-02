import os
import base64
import re
import time
from datetime import datetime
from io import BytesIO
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from openpyxl.drawing.image import Image
from openai import OpenAI
from collections import defaultdict
from token_logging import create_token_usage_log, log_individual_response


client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

def get_llm_prompt():
    return """Analyze these images of a compact disc and extract the following key metadata fields in the specified format. You are a music cataloger, and know that you are responsible for the accuracy of the information you produce.  If ANY information is unclear, partially visible, or not visible: mark it as 'Not visible' in the metadadata. If you have reason to believe that a sticker may be covering part of a key field, like the title or primary contributor, either mark it as 'Not visible' or make an educated guess based on the visible information and note that it may be partially obscured in parentheses.

Match this format:
Title Information:
  - Main Title: [Main Title in original language if using latin characters.  Transliterated if in non-latin characters.]
  - Subtitle: [Subtitle in original language if using latin characters.  Transliterated if in non-latin characters.]
  - Primary Contributor: [Artist/Performer Name]
  - Additional Contributors: [arrangers, engineers, producers, session musicians]
Publishers:
  - Name: [Publisher Name - please list all publisher, label, series, and distributor names visible on the disc]
  - Place: [Place of publication if available]
  - Numbers: [UPC/EAN/ISBN]
Dates:
  - publicationDate: [Record all dates as printed on the disc - separate multiple dates with commas]
Language:
  - sungLanguage: [Languages of sung text]
  - printedLanguage: [All languages of printed text]
Format:
  - generalFormat: [Sound Recording]
  - specificFormat: [CD, CD-ROM, Enhanced CD, etc.]
  - materialTypes: [List of Material Types]
Physical Description:
  - size: [4.75" (standard CD)]
  - material: [aluminum/polycarbonate]
  - labelDesign: [Description of disc face design and color]
  - physicalCondition: [Condition notes]
  - specialFeatures: [Booklet details, packaging type (jewel case/digipak), inserts, bonus materials]
Contents:
  - tracks: [
      {
        "number": [Track number],
        "title": [Title in original language],
        "titleTransliteration": [Title transliteration if applicable],
      }
    ]
Notes:
  - generalNotes: [{'text': [Note Text]}]

***Important: These are images of CD's that were donated by a university radio station to our library. Handwritten information on white stickers should be ignored.  When in doubt, mark fields in the metadata as 'Not visible'*** 
 
Analyze the provided images and return metadata formatted exactly as above. Pay special attention to capturing only text that is clearly legible."""

def group_images_by_barcode(folder_path):
    """Group image files by their barcode number."""
    image_groups = defaultdict(list)
    
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(('.jpg', '.jpeg', '.png')):  
            # Extract barcode number (everything before the letter)
            match = re.match(r'(\d+)[abc]\.png', filename.lower())
            if match:
                barcode = match.group(1)
                image_groups[barcode].append(os.path.join(folder_path, filename))
            else:
                # Try matching for jpg/jpeg format as fallback
                match = re.match(r'(\d+)[abc]\.jpe?g', filename.lower())
                if match:
                    barcode = match.group(1)
                    image_groups[barcode].append(os.path.join(folder_path, filename))
                else:
                    print(f"Filename does not match pattern: {filename}")
    
    # Sort files within each group by the letter (a, b, c)
    for barcode in image_groups:
        image_groups[barcode].sort(key=lambda x: os.path.basename(x).lower()[-5])  # Sort by the letter before extension
        
    return image_groups

def process_folder(folder_path, wb, results_folder_path):
    model_name = "gpt-4o-mini-2024-07-18"  
    ws = wb.active
    headers = ['Input Image 1', 'Input Image 2', 'Input Image 3', 'Barcode', 'AI-Generated Metadata']
    ws.append(headers)

    for col, header in enumerate(headers, start=1):
        if col == 4:  # Barcode column
            ws.column_dimensions[get_column_letter(col)].width = 17
        else:
            ws.column_dimensions[get_column_letter(col)].width = 30 if col <= 3 else 52

    # Add a summary worksheet
    summary_ws = wb.create_sheet(title="Summary")
    summary_ws.append(['Total Items', 'Items with Issues', 'Total Time (s)', 
                       'Total Prompt Tokens', 'Total Completion Tokens', 'Total Tokens',
                       'Average Time per Item (s)', 'Average Tokens per Item'])

    # Create logs folder within the results folder
    logs_folder_path = os.path.join(results_folder_path, "logs")
    if not os.path.exists(logs_folder_path):
        os.makedirs(logs_folder_path)

    # Create a temporary workbook for periodic saving (no images)
    temp_wb = Workbook()
    temp_ws = temp_wb.active
    temp_ws.append(headers)
    for col, header in enumerate(headers, start=1):
        if col == 4:  # Barcode column
            temp_ws.column_dimensions[get_column_letter(col)].width = 15
        else:
            temp_ws.column_dimensions[get_column_letter(col)].width = 30 if col <= 3 else 52
            
    # Create temp summary sheet
    temp_summary_ws = temp_wb.create_sheet(title="Summary")
    temp_summary_ws.append(['Total Items', 'Items with Issues', 'Total Time (s)', 
                           'Total Prompt Tokens', 'Total Completion Tokens', 'Total Tokens',
                           'Average Time per Item (s)', 'Average Tokens per Item'])
    
    # Temporary file path
    temp_output_file = "temp_cd_metadata_progress.xlsx"
    temp_output_path = os.path.join(results_folder_path, temp_output_file)

    image_groups = group_images_by_barcode(folder_path)
    total_items = len(image_groups)
    items_with_issues = 0
    processed_items = 0
    
    # Token and time tracking
    total_prompt_tokens = 0
    total_completion_tokens = 0
    total_tokens = 0
    total_time = 0

    # REPLACE THIS SECTION IN ai-music-step-1-cd.py
    # Find the line that starts: "for barcode, image_paths in sorted(image_groups.items()):"
    # Replace from that line until the end of the main processing loop

    # Also ADD this print statement right before the for loop:
    print(f"\n🎯 STEP 1: METADATA EXTRACTION")
    print(f"Found {total_items} CD image groups to process")
    print(f"Starting metadata extraction using {model_name}...")
    print("-" * 50)

    for barcode, image_paths in sorted(image_groups.items()):
        processed_items += 1
        item_start_time = time.time()
        # The row number in the Excel sheet will be processed_items + 1 (accounting for header row)
        row_number = processed_items + 1

        # Enhanced progress display
        print(f"\n📀 Processing CD {processed_items}/{total_items}")
        print(f"   Barcode: {barcode}")
        print(f"   Images: {len(image_paths)} files")
        print(f"   Progress: {(processed_items/total_items)*100:.1f}%")

        try:
            # Take up to first 3 images for each barcode
            image_paths = image_paths[:3]
            prompt_text = get_llm_prompt()
            uploaded_files_info = ""

            for i, img_path in enumerate(image_paths):
                # Determine image type based on filename
                filename = os.path.basename(img_path).lower()
                if filename.endswith('a.png') or filename.endswith('a.jpg') or filename.endswith('a.jpeg'):
                    image_type = "FRONT COVER"
                elif filename.endswith('b.png') or filename.endswith('b.jpg') or filename.endswith('b.jpeg'):
                    image_type = "BACK COVER"
                elif filename.endswith('c.png') or filename.endswith('c.jpg') or filename.endswith('c.jpeg'):
                    image_type = "ADDITIONAL IMAGE"
                else:
                    image_type = "IMAGE"
                
                uploaded_files_info += f"[Image {i+1} - {image_type}: {img_path}]\n"
                print(f"   📸 {image_type}: {os.path.basename(img_path)}")

            prompt = prompt_text + "\n" + uploaded_files_info

            try:
                print(f"   🤖 Calling OpenAI API...")
                
                base64_images = []
                for img_path in image_paths:
                    with open(img_path, "rb") as image_file:
                        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
                        base64_images.append(base64_image)

                # Start API call timing
                api_start_time = time.time()
                
                # Determine content type based on file extension
                content_types = []
                for img_path in image_paths:
                    ext = os.path.splitext(img_path)[1].lower()
                    if ext == '.png':
                        content_types.append("image/png")
                    else:
                        content_types.append("image/jpeg")  # Default to jpeg for jpg/jpeg
                
                # Create messages with appropriate content types
                image_contents = []
                for i, base64_image in enumerate(base64_images):
                    image_contents.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:{content_types[i]};base64,{base64_image}"}
                    })
                
                response = client.chat.completions.create(
                    model=model_name,
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            *image_contents
                        ]
                    }],
                    max_tokens=2000
                )
                
                # Calculate API call duration
                api_duration = time.time() - api_start_time
                
                # Extract token information
                prompt_tokens = response.usage.prompt_tokens
                completion_tokens = response.usage.completion_tokens
                total_item_tokens = prompt_tokens + completion_tokens
                
                # Update totals
                total_prompt_tokens += prompt_tokens
                total_completion_tokens += completion_tokens
                total_tokens += total_item_tokens

                metadata_output = response.choices[0].message.content.strip()
                
                # Enhanced success output
                print(f"   ✅ API call successful!")
                print(f"   ⏱️  API time: {api_duration:.2f}s")
                print(f"   🎯 Tokens used: {total_item_tokens:,} (P:{prompt_tokens:,}, C:{completion_tokens:,})")
                
                # Preview of extracted metadata (first 100 chars)
                preview = metadata_output[:100].replace('\n', ' ')
                print(f"   📄 Metadata preview: {preview}...")
                
                # Log individual response
                log_individual_response(
                    logs_folder_path=logs_folder_path,
                    script_name="metadata_creation",
                    row_number=row_number,
                    barcode=barcode,
                    response_text=metadata_output,
                    model_name=model_name,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    processing_time=api_duration
                )
                
                row_data = [
                    '', '', '', barcode, metadata_output
                ]
                ws.append(row_data)
                
                # Also add to temporary workbook (without images)
                temp_ws.append(row_data)

                # Add thumbnail images to main Excel workbook only
                for i, img_path in enumerate(image_paths, start=1):
                    img = PILImage.open(img_path)
                    img.thumbnail((200, 200))

                    output = BytesIO()
                    img.save(output, format='PNG')
                    output.seek(0)

                    img_openpyxl = Image(output)
                    img_openpyxl.anchor = ws.cell(row=ws.max_row, column=i).coordinate
                    ws.add_image(img_openpyxl)

                ws.row_dimensions[ws.max_row].height = 215

                for cell in ws[ws.max_row]:
                    cell.alignment = Alignment(vertical='top', wrap_text=True)
                
                for cell in temp_ws[temp_ws.max_row]:
                    cell.alignment = Alignment(vertical='top', wrap_text=True)

            except Exception as e:
                print(f"   ❌ API call failed: {str(e)}")
                error_message = f"Error: {str(e)}"
                ws.append(['', '', '', barcode, error_message])
                temp_ws.append(['', '', '', barcode, error_message])
                items_with_issues += 1
                
                # Log errors to the response log
                log_individual_response(
                    logs_folder_path=logs_folder_path,
                    script_name="metadata_creation",
                    row_number=row_number,
                    barcode=barcode,
                    response_text=f"ERROR: {str(e)}",
                    model_name=model_name,
                    prompt_tokens=0,
                    completion_tokens=0,
                    processing_time=0
                )

        except Exception as e:
            print(f"   ❌ Processing failed: {str(e)}")
            error_message = f"Error: {str(e)}"
            ws.append(['', '', '', barcode, error_message])
            temp_ws.append(['', '', '', barcode, error_message])
            items_with_issues += 1
            
            # Log errors to the response log
            log_individual_response(
                logs_folder_path=logs_folder_path,
                script_name="metadata_creation",
                row_number=row_number,
                barcode=barcode,
                response_text=f"ERROR processing: {str(e)}",
                model_name=model_name,
                prompt_tokens=0,
                completion_tokens=0,
                processing_time=0
            )

        # Calculate timing and enhanced log progress (moved outside both try blocks)
        item_duration = time.time() - item_start_time
        total_time += item_duration
        
        # Enhanced completion summary
        print(f"   ⌚ Total item time: {item_duration:.2f}s")
        print(f"   📊 Running totals: {total_tokens:,} tokens, ${(total_tokens/1000)*0.00015:.4f} cost")
        
        # Progress bar
        progress = processed_items / total_items
        bar_length = 30
        filled_length = int(bar_length * progress)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        print(f"   Progress: |{bar}| {progress*100:.1f}% ({processed_items}/{total_items})")
        
        # ETA calculation
        if processed_items > 1:
            avg_time = total_time / processed_items
            remaining_items = total_items - processed_items
            eta_seconds = remaining_items * avg_time
            eta_minutes = eta_seconds / 60
            print(f"   🕒 ETA: ~{eta_minutes:.1f} minutes remaining")
        
        # Save temporary workbook every 10 rows with enhanced messaging
        if processed_items % 10 == 0:
            print(f"   💾 Saving progress checkpoint...")
            # Update summary data in temp workbook
            avg_time = total_time / processed_items if processed_items > 0 else 0
            avg_tokens = total_tokens / processed_items if processed_items > 0 else 0
            
            # Clear old summary data and add new
            for row in temp_summary_ws.iter_rows(min_row=2, max_row=2):
                for cell in row:
                    cell.value = None
            
            temp_summary_ws.append([
                processed_items, 
                items_with_issues, 
                round(total_time, 2),
                total_prompt_tokens,
                total_completion_tokens,
                total_tokens,
                round(avg_time, 2),
                round(avg_tokens, 2)
            ])
            
            # Save temporary progress
            try:
                temp_wb.save(temp_output_path)
                print(f"   ✅ Progress saved ({processed_items}/{total_items} items)")
            except Exception as save_error:
                print(f"   ⚠️  Warning: Could not save temporary progress: {save_error}")

    # At the very end of the function, REPLACE the existing print statements with:
    print(f"\n🎉 STEP 1 COMPLETED!")
    print(f"✅ Successfully processed: {total_items - items_with_issues}/{total_items} CDs")
    print(f"❌ Items with issues: {items_with_issues}")
    print(f"⏱️  Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"🎯 Total tokens: {total_tokens:,}")
    print(f"💰 Estimated cost: ${(total_tokens/1000)*0.00015:.4f}")
    
    # Clean up temporary file
    try:
        if os.path.exists(temp_output_path):
            os.remove(temp_output_path)
            print(f"Temporary progress file removed: {temp_output_path}")
    except Exception as remove_error:
        print(f"Warning: Could not remove temporary progress file: {remove_error}")
    
    return total_items, items_with_issues, total_time, total_prompt_tokens, total_completion_tokens, total_tokens

def main():
    start_time = time.time()
    
    base_dir = "ai-music-workflow/cd-processing"
    images_folder = os.path.join(base_dir, "cd-image-folders/cd-scans-5")
    base_dir_outputs = os.path.join(base_dir, "cd-output-folders")
    
    # Create results folder with today's date
    current_date = datetime.now().strftime("%Y-%m-%d")
    results_folder_name = f"results-{current_date}"
    results_folder_path = os.path.join(base_dir_outputs, results_folder_name)

    # Create the folder if it doesn't exist
    if not os.path.exists(results_folder_path):
        os.makedirs(results_folder_path)
    
    # Create logs folder within the results folder
    logs_folder_path = os.path.join(results_folder_path, "logs")
    if not os.path.exists(logs_folder_path):
        os.makedirs(logs_folder_path)
    
    wb = Workbook()
    total_items, items_with_issues, total_time, total_prompt_tokens, total_completion_tokens, total_tokens = process_folder(images_folder, wb, results_folder_path)

    for row in wb.active.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top')

    wb.active.freeze_panes = 'A2'

    # Save output to the results folder
    output_file = f"cd-metadata-ai-{current_date}.xlsx"
    full_output_path = os.path.join(results_folder_path, output_file)

    wb.save(full_output_path)
    
    total_execution_time = time.time() - start_time
    
    # Calculate token breakdown
    model_name = "gpt-4o-mini-2024-07-18"
    
    # Create the token usage log
    create_token_usage_log(
        logs_folder_path=logs_folder_path,
        script_name="metadata_creation",
        model_name=model_name,
        total_items=total_items,
        items_with_issues=items_with_issues,
        total_time=total_time,
        total_prompt_tokens=total_prompt_tokens,
        total_completion_tokens=total_completion_tokens,
        total_cached_tokens=0
    )
    
    print(f"Results saved to {full_output_path}")
    print(f"Summary: Processed {total_items} items, {items_with_issues} with issues.")
    print(f"Total execution time: {round(total_execution_time, 2)} seconds")
    print(f"Total OpenAI API time: {round(total_time, 2)} seconds")
    print(f"Total tokens used: {total_tokens}")
    print(f"Token usage log created in: {logs_folder_path}")
    
if __name__ == "__main__":
    main()