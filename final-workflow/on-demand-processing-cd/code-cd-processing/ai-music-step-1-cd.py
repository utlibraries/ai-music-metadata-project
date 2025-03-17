import os
import base64
import re
from datetime import datetime
from io import BytesIO
from PIL import Image as PILImage
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from openpyxl.drawing.image import Image
from openai import OpenAI
from collections import defaultdict

client = OpenAI(api_key=os.getenv('OPENAI_HMRC_API_KEY'))

def get_llm_prompt():
    return """Analyze these images of a compact disc and extract the following key metadata fields in the specified format. If any information is unclear, partially visible, or not visible: mark it as 'Not visible' in the metadadata. 

Match this format:
Title Information:
  - Main Title: [Main Title in original language if using latin characters.  Transliterated if in non-latin characters.]
  - Subtitle: [Subtitle in original language if using latin characters.  Transliterated if in non-latin characters.]
  - Primary Contributor: [Artist/Performer Name]
  - Additional Contributors:
    - Arrangers: [List of arrangers]
    - Engineers: [List of engineers]
    - Producers: [List of producers]
    - Session Musicians: [List of session musicians with instruments]
Publishers:
  - Name: [Publisher Name]
  - Place: [Place of publication if available]
  - Numbers: [Catalog numbers, UPC (from the original CD packaging, not from the university barcode sticker), ISRC codes]
Dates:
  - publicationDate: [Publication Year if shown]
  - recordingDate: [Recording Date if shown]
  - recordingLocation: [Recording studio/location if shown]
Language:
  - sungLanguage: [Languages of sung text]
  - printedLanguage: [All languages of printed text]
Format:
  - generalFormat: [Sound Recording]
  - specificFormat: [CD, CD-ROM, Enhanced CD, etc.]
  - materialTypes: [List of Material Types]
Sound Characteristics:
  - soundConfiguration: [stereo/multi-channel/DDD/ADD/AAD]
  - recordingType: [digital]
  - specialPlayback: [HDCD, DTS, etc. if applicable]
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
        "composer": [Composer name],
        "lyricist": [Lyricist name],
        "duration": [Duration if shown],
        "isrc": [ISRC code if present]
      }
    ]
Series:
  - seriesTitle: [Series name if any]
  - seriesNumber: [Number within series]
Subject Information:
  - genre: [Musical genre/style]
  - geographic: [Geographic origins]
  - timePeriod: [Time period]
Notes:
  - generalNotes: [{'text': [Note Text]}]
  - performerNotes: [List of Performer Notes]
  - participantNote: [Participant Note]
  - technicalNotes: [CD pressing plant, mastering information]

Analyze the provided images and return metadata formatted exactly like the example above. Pay special attention to capturing all visible details, including small text on disc surfaces, back inserts, and booklets. Report any uncertainty or partially visible information in the notes section."""


def group_images_by_barcode(folder_path):
    """Group image files by their barcode number."""
    image_groups = defaultdict(list)
    
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(('.jpg', '.jpeg')):  
            # Extract barcode number (everything before the letter)
            match = re.match(r'(\d+)[abc]\.jpe?g', filename)
            if match:
                barcode = match.group(1)
                image_groups[barcode].append(os.path.join(folder_path, filename))
            else:
                print(f"Filename does not match pattern: {filename}")
    
    # Sort files within each group by the letter (a, b, c)
    for barcode in image_groups:
        image_groups[barcode].sort(key=lambda x: os.path.basename(x)[-5])  # Sort by the letter before .jpg/.jpeg
        
    return image_groups


def process_folder(folder_path, wb, results_folder_path):
    ws = wb.active
    headers = ['Input Image 1', 'Input Image 2', 'Input Image 3', 'Barcode', 'AI-Generated Metadata']
    ws.append(headers)

    for col, header in enumerate(headers, start=1):
        if col == 4:  # Barcode column
            ws.column_dimensions[get_column_letter(col)].width = 15
        else:
            ws.column_dimensions[get_column_letter(col)].width = 30 if col <= 3 else 52

    image_groups = group_images_by_barcode(folder_path)
    total_items = len(image_groups)
    items_with_issues = 0
    processed_items = 0

    for barcode, image_paths in sorted(image_groups.items()):
        processed_items += 1

        try:
            # Take up to first 3 images for each barcode
            image_paths = image_paths[:3]
            prompt_text = get_llm_prompt()
            uploaded_files_info = ""

            for img_path in image_paths:
                uploaded_files_info += f"[Image file path: {img_path}]\n"

            prompt = prompt_text + "\n" + uploaded_files_info

            try:
                base64_images = []
                for img_path in image_paths:
                    with open(img_path, "rb") as image_file:
                        base64_image = base64.b64encode(image_file.read()).decode('utf-8')
                        base64_images.append(base64_image)

                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            *[{
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                            } for base64_image in base64_images]
                        ]
                    }],
                    max_tokens=2000
                )

                metadata_output = response.choices[0].message.content.strip()
                print(f"Extracted Metadata for barcode {barcode}: {metadata_output}")

                row_data = ['', '', '', barcode, metadata_output]
                ws.append(row_data)

                # Add thumbnail images to Excel
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

            except Exception as e:
                print(f"Error generating content for barcode {barcode}: {str(e)}")
                ws.append(['', '', '', barcode, f"Error: {str(e)}"])
                items_with_issues += 1

        except Exception as e:
            print(f"Error processing barcode {barcode}: {str(e)}")
            ws.append(['', '', '', barcode, f"Error: {str(e)}"])
            items_with_issues += 1

        print(f"Processed {processed_items}/{total_items} items. Current barcode: {barcode}")

    print(f"Processed {total_items} items. {items_with_issues} items had issues.")
    return total_items, items_with_issues


def main():
    base_dir = "final-workflow/on-demand-processing-cd"
    images_folder = os.path.join(base_dir, "cd-input-folders/cd-scans-5")
    base_dir_outputs = os.path.join(base_dir, "cd-output-folders")
    
    # Create results folder with today's date
    current_date = datetime.now().strftime("%Y-%m-%d")
    results_folder_name = f"results-{current_date}"
    results_folder_path = os.path.join(base_dir_outputs, results_folder_name)

    # Create the folder if it doesn't exist
    if not os.path.exists(results_folder_path):
        os.makedirs(results_folder_path)
    
    wb = Workbook()
    total_items, items_with_issues = process_folder(images_folder, wb, results_folder_path)

    for row in wb.active.iter_rows():
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical='top')

    wb.active.freeze_panes = 'A2'

    # Save output to the results folder
    output_file = f"ai-music-step-1-{current_date}.xlsx"
    full_output_path = os.path.join(results_folder_path, output_file)

    wb.save(full_output_path)
    print(f"Results saved to {full_output_path}")
    print(f"Summary: Processed {total_items} items, {items_with_issues} with issues.")
    
if __name__ == "__main__":
    main()