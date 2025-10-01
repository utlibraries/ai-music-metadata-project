"""
Shared Utilities for AI Music Metadata Project

Common functions used across the 6-step LP processing workflow including
file operations, data parsing, validation, and batch processing helpers.
Core utilities for metadata extraction, OCLC number normalization,
and workflow state management.
"""

import os
import glob
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

def find_latest_results_folder(prefix: str) -> Optional[str]:
    """
    Find the most recent results folder based on the given prefix.
    
    Args:
        prefix: Base path prefix like "ai-music-workflow/lp-processing/lp-output-folders/results-"
    
    Returns:
        Path to the latest results folder, or None if not found
    """
    base_dir = os.path.dirname(prefix)
    pattern = os.path.join(base_dir, "results-*")
    
    matching_folders = glob.glob(pattern)
    if not matching_folders:
        return None
    
    return max(matching_folders)

def get_workflow_json_path(results_folder):
    """
    Find the actual workflow JSON file in the results folder instead of assuming today's date.
    """
    # Look for files matching the pattern
    json_files = [f for f in os.listdir(results_folder) 
                  if f.startswith("full-workflow-data-lp-") and f.endswith(".json")]
    
    if not json_files:
        # Fallback to creating with current date if none found
        current_date = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(results_folder, f"full-workflow-data-lp-{current_date}.json")
    
    # Return the most recent one if multiple exist
    latest_json = max(json_files)
    return os.path.join(results_folder, latest_json)

def find_latest_lp_metadata_file(results_folder: str) -> Optional[str]:
    """
    Find the most recent full-workflow-data-lp Excel file in the results folder.
    
    Args:
        results_folder: Path to results folder
    
    Returns:
        Path to the latest LP metadata file, or None if not found
    """
    files = [f for f in os.listdir(results_folder) 
             if f.startswith("full-workflow-data-lp") and f.endswith(".xlsx")]
    if not files:
        return None
    latest_file = max(files)
    return os.path.join(results_folder, latest_file)

def get_bib_info_from_workflow(oclc_number: str, workflow_json_path: str) -> Dict[str, Any]:
    """
    Extract bibliographic information from formatted OCLC results in workflow JSON.
    
    Args:
        oclc_number: OCLC number to search for
        workflow_json_path: Path to workflow JSON file
    
    Returns:
        Dictionary with title, contributors, publication_date, and full_record_text
    """
    try:
        import json
        with open(workflow_json_path, 'r', encoding='utf-8') as f:
            workflow_data = json.load(f)
        
        for barcode, record_data in workflow_data.get("records", {}).items():
            step2_data = record_data.get("step2_detailed_data", {})
            formatted_results = step2_data.get("formatted_oclc_results", "")
            
            oclc_pattern = rf"OCLC Number: {re.escape(oclc_number)}\n\n(.*?)(?=\n-{{40}}\nOCLC Number:|\Z)"
            match = re.search(oclc_pattern, formatted_results, re.DOTALL)
            
            if match:
                record_text = match.group(1)
                
                title_match = re.search(r"Title Information:\s*\n\s*- Main Title: (.+?)(?:\n|$)", record_text)
                title = title_match.group(1) if title_match else "No title available"
                
                contributors = []
                contributor_matches = re.findall(r"Contributors:\s*\n((?:\s*- .+?\n)*)", record_text)
                if contributor_matches:
                    contributor_lines = contributor_matches[0].strip().split('\n')
                    for line in contributor_lines:
                        if line.strip().startswith('- '):
                            contributor = line.strip()[2:].split(' (')[0]
                            contributors.append(contributor)
                
                date_match = re.search(r"- publicationDate: (.+?)(?:\n|$)", record_text)
                pub_date = date_match.group(1) if date_match else "No date available"
                
                return {
                    "title": title,
                    "contributors": contributors,
                    "publication_date": pub_date,
                    "full_record_text": record_text
                }
        
        return {"error": "OCLC record not found in workflow data"}
        
    except Exception as e:
        return {"error": str(e)}

def extract_metadata_fields(metadata_str: str) -> Dict[str, Any]:
    """
    Parse AI-generated metadata string into structured fields for JSON storage.
    Handles both JSON format and text format responses.
    
    Args:
        metadata_str: Raw AI-generated metadata text
    
    Returns:
        Dictionary with structured metadata fields
    """
    if not metadata_str:
        return {}
    
    fields = {
        "title_information": {
            "main_title": None,
            "subtitle": None,
            "primary_contributor": None,
            "additional_contributors": []
        },
        "publishers": {
            "name": None,
            "place": None,
            "numbers": None
        },
        "dates": {
            "publication_date": None
        },
        "language": {
            "sung_language": None,
            "printed_language": None
        },
        "format": {
            "general_format": None,
            "specific_format": None,
            "material_types": []
        },
        "physical_description": {
            "size": None,
            "material": None,
            "label_design": None,
            "physical_condition": None,
            "special_features": None
        },
        "contents": {
            "tracks": []
        },
        "notes": {
            "general_notes": []
        }
    }
    
    def clean_value(value: str) -> Optional[str]:
        """Clean extracted values and return None for invalid entries."""
        if not value:
            return None
        
        # Remove leading/trailing whitespace and dashes
        cleaned = value.strip().lstrip('-').strip()
        
        # Check for invalid values
        invalid_indicators = [
            "not visible", "not available", "n/a", "unavailable", 
            "unknown", "[none]", "none", "not present", "not listed", 
            "not applicable", "unclear", "partially visible"
        ]
        
        if cleaned.lower() in invalid_indicators:
            return None
            
        return cleaned if cleaned else None
    
    # Try to parse as JSON first
    try:
        # Look for JSON content between ```json and ``` or just try to parse the whole thing
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', metadata_str, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # Try to find JSON-like structure
            json_match = re.search(r'(\{.*\})', metadata_str, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = metadata_str
        
        import json
        parsed_json = json.loads(json_str)
        
        # Extract from JSON structure
        if "Title Information" in parsed_json:
            title_info = parsed_json["Title Information"]
            fields["title_information"]["main_title"] = clean_value(title_info.get("Main Title"))
            fields["title_information"]["subtitle"] = clean_value(title_info.get("Subtitle"))
            fields["title_information"]["primary_contributor"] = clean_value(title_info.get("Primary Contributor"))
            
            additional = title_info.get("Additional Contributors")
            if additional and clean_value(additional):
                if isinstance(additional, list):
                    fields["title_information"]["additional_contributors"] = [clean_value(c) for c in additional if clean_value(c)]
                else:
                    contrib_list = [c.strip() for c in str(additional).split(',') if clean_value(c.strip())]
                    fields["title_information"]["additional_contributors"] = contrib_list
        
        if "Publishers" in parsed_json:
            pub_info = parsed_json["Publishers"]
            fields["publishers"]["name"] = clean_value(pub_info.get("Name"))
            fields["publishers"]["place"] = clean_value(pub_info.get("Place"))
            fields["publishers"]["numbers"] = clean_value(pub_info.get("Numbers"))
        
        if "Dates" in parsed_json:
            date_info = parsed_json["Dates"]
            fields["dates"]["publication_date"] = clean_value(date_info.get("publicationDate"))
        
        if "Language" in parsed_json:
            lang_info = parsed_json["Language"]
            fields["language"]["sung_language"] = clean_value(lang_info.get("sungLanguage"))
            fields["language"]["printed_language"] = clean_value(lang_info.get("printedLanguage"))
        
        if "Format" in parsed_json:
            format_info = parsed_json["Format"]
            fields["format"]["general_format"] = clean_value(format_info.get("generalFormat"))
            fields["format"]["specific_format"] = clean_value(format_info.get("specificFormat"))
            
            material_types = format_info.get("materialTypes")
            if material_types and clean_value(str(material_types)):
                if isinstance(material_types, list):
                    fields["format"]["material_types"] = [clean_value(m) for m in material_types if clean_value(m)]
                else:
                    fields["format"]["material_types"] = [clean_value(str(material_types))]
        
        if "Physical Description" in parsed_json:
            phys_info = parsed_json["Physical Description"]
            fields["physical_description"]["size"] = clean_value(phys_info.get("size"))
            fields["physical_description"]["material"] = clean_value(phys_info.get("material"))
            fields["physical_description"]["label_design"] = clean_value(phys_info.get("labelDesign"))
            fields["physical_description"]["physical_condition"] = clean_value(phys_info.get("physicalCondition"))
            fields["physical_description"]["special_features"] = clean_value(phys_info.get("specialFeatures"))
        
        if "Contents" in parsed_json:
            content_info = parsed_json["Contents"]
            tracks = content_info.get("tracks")
            if tracks and isinstance(tracks, list):
                for track in tracks:
                    if isinstance(track, dict) and "number" in track and "title" in track:
                        track_title = clean_value(track.get("title"))
                        if track_title:
                            try:
                                track_num = int(track.get("number", 0))
                                fields["contents"]["tracks"].append({
                                    "number": track_num,
                                    "title": track_title
                                })
                            except (ValueError, TypeError):
                                continue
        
        if "Notes" in parsed_json:
            notes_info = parsed_json["Notes"]
            notes = notes_info.get("generalNotes", [])
            if isinstance(notes, list):
                for note in notes:
                    if isinstance(note, dict) and "text" in note:
                        note_text = clean_value(note.get("text"))
                        if note_text:
                            fields["notes"]["general_notes"].append({"text": note_text})
        
        return fields
        
    except (json.JSONDecodeError, KeyError, AttributeError):
        # Fall back to regex parsing for non-JSON format
        pass
    
    # Original regex-based parsing as fallback
    def clean_value(value: str) -> Optional[str]:
        """Clean extracted values and return None for invalid entries."""
        if not value:
            return None
        
        # Remove leading/trailing whitespace and dashes
        cleaned = value.strip().lstrip('-').strip()
        
        # Check for invalid values
        invalid_indicators = [
            "not visible", "not available", "n/a", "unavailable", 
            "unknown", "[none]", "none", "not present", "not listed", 
            "not applicable", "unclear", "partially visible"
        ]
        
        if cleaned.lower() in invalid_indicators:
            return None
            
        return cleaned if cleaned else None
    
    # Extract title information
    title_match = re.search(r'Main Title:\s*(.+)', metadata_str, re.IGNORECASE)
    if title_match:
        fields["title_information"]["main_title"] = clean_value(title_match.group(1))
    
    subtitle_match = re.search(r'Subtitle:\s*(.+)', metadata_str, re.IGNORECASE)
    if subtitle_match:
        fields["title_information"]["subtitle"] = clean_value(subtitle_match.group(1))
    
    contributor_match = re.search(r'Primary Contributor:\s*(.+)', metadata_str, re.IGNORECASE)
    if contributor_match:
        fields["title_information"]["primary_contributor"] = clean_value(contributor_match.group(1))
    
    additional_match = re.search(r'Additional Contributors:\s*(.+?)(?=\n[A-Z]|$)', metadata_str, re.IGNORECASE | re.DOTALL)
    if additional_match:
        additional_text = clean_value(additional_match.group(1))
        if additional_text:
            # Split by common delimiters and clean up
            contributors = []
            for c in re.split(r'[,;]', additional_text):
                cleaned_contrib = c.strip()
                if cleaned_contrib and not cleaned_contrib.lower() in ["not applicable", "not visible", "none"]:
                    contributors.append(cleaned_contrib)
            fields["title_information"]["additional_contributors"] = contributors
    
    # Extract publishers - more flexible matching
    pub_name_match = re.search(r'(?:Publishers?|Name):\s*(.+?)(?=\n\s*-\s*Place:|$)', metadata_str, re.DOTALL | re.IGNORECASE)
    if pub_name_match:
        pub_name = clean_value(pub_name_match.group(1).split('\n')[0])
        fields["publishers"]["name"] = pub_name
    
    pub_place_match = re.search(r'Place:\s*(.+)', metadata_str, re.IGNORECASE)
    if pub_place_match:
        fields["publishers"]["place"] = clean_value(pub_place_match.group(1))
    
    pub_numbers_match = re.search(r'Numbers:\s*(.+)', metadata_str, re.IGNORECASE)
    if pub_numbers_match:
        fields["publishers"]["numbers"] = clean_value(pub_numbers_match.group(1))
    
    # Extract dates
    date_match = re.search(r'publicationDate:\s*(.+)', metadata_str, re.IGNORECASE)
    if date_match:
        fields["dates"]["publication_date"] = clean_value(date_match.group(1))
    
    # Extract language
    sung_lang_match = re.search(r'sungLanguage:\s*(.+)', metadata_str, re.IGNORECASE)
    if sung_lang_match:
        fields["language"]["sung_language"] = clean_value(sung_lang_match.group(1))
    
    printed_lang_match = re.search(r'printedLanguage:\s*(.+)', metadata_str, re.IGNORECASE)
    if printed_lang_match:
        fields["language"]["printed_language"] = clean_value(printed_lang_match.group(1))
    
    # Extract format
    general_format_match = re.search(r'generalFormat:\s*(.+)', metadata_str, re.IGNORECASE)
    if general_format_match:
        fields["format"]["general_format"] = clean_value(general_format_match.group(1))
    
    specific_format_match = re.search(r'specificFormat:\s*(.+)', metadata_str, re.IGNORECASE)
    if specific_format_match:
        fields["format"]["specific_format"] = clean_value(specific_format_match.group(1))
    
    # Extract material types
    material_types_match = re.search(r'materialTypes:\s*(.+)', metadata_str, re.IGNORECASE)
    if material_types_match:
        material_types_text = clean_value(material_types_match.group(1))
        if material_types_text:
            # Handle both list format and simple text
            if '[' in material_types_text and ']' in material_types_text:
                # Extract from list format
                list_content = re.search(r'\[(.*?)\]', material_types_text)
                if list_content:
                    types = [t.strip().strip('"\'') for t in list_content.group(1).split(',')]
                    fields["format"]["material_types"] = [t for t in types if t]
            else:
                # Simple text format
                fields["format"]["material_types"] = [material_types_text]
    
    # Extract physical description
    size_match = re.search(r'size:\s*(.+)', metadata_str, re.IGNORECASE)
    if size_match:
        fields["physical_description"]["size"] = clean_value(size_match.group(1))
    
    material_match = re.search(r'material:\s*(.+)', metadata_str, re.IGNORECASE)
    if material_match:
        fields["physical_description"]["material"] = clean_value(material_match.group(1))
    
    label_design_match = re.search(r'labelDesign:\s*(.+)', metadata_str, re.IGNORECASE)
    if label_design_match:
        fields["physical_description"]["label_design"] = clean_value(label_design_match.group(1))
    
    condition_match = re.search(r'physicalCondition:\s*(.+)', metadata_str, re.IGNORECASE)
    if condition_match:
        fields["physical_description"]["physical_condition"] = clean_value(condition_match.group(1))
    
    features_match = re.search(r'specialFeatures:\s*(.+)', metadata_str, re.IGNORECASE)
    if features_match:
        fields["physical_description"]["special_features"] = clean_value(features_match.group(1))
    
    # Extract tracks - handle the actual YAML-like format from the raw metadata
    tracks_section = re.search(r'tracks:\s*\[(.*?)\]', metadata_str, re.DOTALL)
    if tracks_section:
        tracks_content = tracks_section.group(1)
        
        # Look for individual track objects in the JSON-like format
        track_pattern = r'\{\s*"number":\s*(\d+),\s*"title":\s*([^,}]+?)(?:,\s*"titleOriginalLanguage":[^}]*)?\s*\}'
        track_matches = re.finditer(track_pattern, tracks_content, re.DOTALL)
        
        for match in track_matches:
            try:
                track_number = int(match.group(1))
                track_title_raw = match.group(2).strip()
                
                # Clean up the track title (remove quotes and extra whitespace)
                track_title = clean_value(track_title_raw.strip('"\''))
                
                if track_title:
                    fields["contents"]["tracks"].append({
                        "number": track_number,
                        "title": track_title
                    })
            except (ValueError, TypeError):
                continue

    # If the above didn't work, try a more flexible approach for tracks
    if not fields["contents"]["tracks"]:
        # Look for tracks in a simpler format within the raw metadata
        # This handles cases where tracks might be listed differently
        track_lines = re.finditer(r'(\d+),\s*"title":\s*([^,\n]+)', metadata_str)
        
        for match in track_lines:
            try:
                track_number = int(match.group(1))
                track_title = clean_value(match.group(2).strip('",'))
                
                if track_title and track_title not in ["Not applicable", "Not visible"]:
                    fields["contents"]["tracks"].append({
                        "number": track_number,
                        "title": track_title
                    })
            except (ValueError, TypeError):
                continue

    # Final fallback: look for any pattern that has numbers followed by titles
    if not fields["contents"]["tracks"]:
        # Try to find track listings in various formats
        for line in metadata_str.split('\n'):
            # Look for lines that might contain track info
            track_match = re.search(r'(\d+)[,:\s]*([A-Za-z][^,\n]{2,})', line)
            if track_match:
                try:
                    track_number = int(track_match.group(1))
                    track_title = clean_value(track_match.group(2))
                    
                    # Only add if it looks like a real track title (not metadata fields)
                    if (track_title and 
                        not any(keyword in track_title.lower() for keyword in 
                            ['not visible', 'not applicable', 'date:', 'year:', 'format:', 'language:'])):
                        fields["contents"]["tracks"].append({
                            "number": track_number,
                            "title": track_title
                        })
                except (ValueError, TypeError):
                    continue
    
    # Extract notes
    notes_match = re.search(r'generalNotes:\s*\[(.*?)\]', metadata_str, re.DOTALL)
    if notes_match:
        notes_content = notes_match.group(1)
        note_objects = re.finditer(r'\{[\'"]text[\'"]\s*:\s*([^}]+)\}', notes_content)
        for match in note_objects:
            note_text = clean_value(match.group(1).strip('\'"'))
            if note_text:
                fields["notes"]["general_notes"].append({"text": note_text})
    
    return fields

def parse_alternative_matches(other_matches_text: str) -> List[Dict[str, Any]]:
    """
    Parse the "Other Potential Matches" text to extract OCLC numbers and details.
    
    Args:
        other_matches_text: Text containing alternative match information
    
    Returns:
        List of dictionaries with alternative match details
    """
    if not other_matches_text or other_matches_text.lower().strip() in ["none", "none.", "no other potential good matches.", "n/a"]:
        return []
    
    matches = []
    
    # Look for structured match information (OCLC: number | IXA: status | Holdings: count)
    structured_matches = re.finditer(
        r'OCLC:\s*(\d+)\s*\|\s*IXA:\s*(Yes|No|Y|N)\s*\|\s*Holdings:\s*(\d+)',
        other_matches_text,
        re.IGNORECASE
    )
    
    for match in structured_matches:
        oclc_number = match.group(1)
        ixa_status = match.group(2).upper() in ['YES', 'Y']
        holdings_count = int(match.group(3))
        
        matches.append({
            "oclc_number": oclc_number,
            "held_by_ixa": ixa_status,
            "total_holdings": holdings_count,
            "source": "structured"
        })
    
    # If no structured matches found, look for basic OCLC numbers
    if not matches:
        oclc_numbers = re.findall(r'OCLC(?:\s+Number)?:?\s*(\d{8,10})', other_matches_text, re.IGNORECASE)
        
        for oclc_num in oclc_numbers:
            matches.append({
                "oclc_number": oclc_num,
                "held_by_ixa": None,
                "total_holdings": None,
                "source": "basic_extraction"
            })
    
    return matches

def extract_confidence_and_explanation(analysis_result: str) -> Tuple[float, str, List[str]]:
    """
    Extract confidence score, explanation, and alternative matches from AI analysis result.
    
    Args:
        analysis_result: Raw AI analysis response text
    
    Returns:
        Tuple of (confidence_score, explanation, alternative_oclc_numbers)
    """
    confidence_score = 0.0
    explanation = "Could not parse response"
    alternative_matches = []
    
    try:
        # Extract confidence score
        if "Confidence score:" in analysis_result:
            confidence_part = analysis_result.split("Confidence score:")[1].split("%")[0].strip()
            try:
                confidence_score = float(confidence_part)
                confidence_score = min(100, max(0, confidence_score))
            except ValueError:
                confidence_score = 0.0
        
        # Extract explanation
        if "Explanation:" in analysis_result:
            explanation_parts = analysis_result.split("Explanation:")[1].split("Other potential good matches:")
            explanation = explanation_parts[0].strip()
            if explanation.endswith("4."):
                explanation = explanation[:-2].strip()
            explanation = re.sub(r'\s+\d+\.\s*$', '', explanation)
        
        # Extract the selected OCLC number first
        selected_oclc = None
        if "OCLC number:" in analysis_result:
            oclc_match = re.search(r'OCLC number:\s*(\d{7,10})', analysis_result, re.IGNORECASE)
            if oclc_match:
                selected_oclc = oclc_match.group(1)
        
        # Find all 7-10 digit numbers in the response using a more flexible pattern
        # This pattern looks for digit sequences that might have punctuation before/after
        all_oclc_numbers = re.findall(r'(?:^|[^\d])(\d{7,10})(?:[^\d]|$)', analysis_result, re.MULTILINE)
        
        # Remove duplicates and exclude the selected OCLC number
        seen = set()
        for oclc in all_oclc_numbers:
            if oclc not in seen and oclc != selected_oclc:
                seen.add(oclc)
                alternative_matches.append(oclc)
    
    except Exception as e:
        print(f"Error parsing analysis result: {e}")
    
    return confidence_score, explanation, alternative_matches

def safe_float_convert(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float, returning default if conversion fails.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
    
    Returns:
        Float value or default
    """
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def safe_int_convert(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to int, returning default if conversion fails.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
    
    Returns:
        Integer value or default
    """
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def normalize_oclc_number(oclc_value: Any) -> Optional[str]:
    """
    Normalize OCLC number to a standard string format.
    
    Args:
        oclc_value: Raw OCLC number value
    
    Returns:
        Normalized OCLC number string or None if invalid
    """
    if not oclc_value:
        return None
    
    oclc_str = str(oclc_value).strip()
    
    # Check for invalid values
    invalid_values = [
        "", "not found", "error processing", "no oclc data to process",
        "no matching records found", "n/a", "none"
    ]
    
    if oclc_str.lower() in invalid_values:
        return None
    
    # Extract digits only
    digits_only = re.sub(r'\D', '', oclc_str)
    
    # OCLC numbers should be 8-10 digits
    if len(digits_only) >= 8 and len(digits_only) <= 10:
        return digits_only
    
    return None

def get_barcode_from_filename(filename: str) -> Optional[str]:
    """
    Extract barcode from image filename using regex patterns.
    
    Args:
        filename: Image filename
    
    Returns:
        Extracted barcode or None if not found
    """
    # Try matching for png format (e.g., "123456a.png")
    match = re.match(r'(\d+)[abc]\.png', filename.lower())
    if match:
        return match.group(1)
    
    # Try matching for jpg/jpeg format (e.g., "123456a.jpg")
    match = re.match(r'(\d+)[abc]\.jpe?g', filename.lower())
    if match:
        return match.group(1)
    
    return None

def group_images_by_barcode(folder_path: str) -> Dict[str, List[str]]:
    """
    Group image files by their barcode number.
    
    Args:
        folder_path: Path to folder containing images
    
    Returns:
        Dictionary mapping barcode to list of image paths
    """
    image_groups = {}
    
    if not os.path.exists(folder_path):
        return image_groups
    
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
            barcode = get_barcode_from_filename(filename)
            if barcode:
                if barcode not in image_groups:
                    image_groups[barcode] = []
                image_groups[barcode].append(os.path.join(folder_path, filename))
    
    # Sort files within each group by the letter (a, b, c)
    for barcode in image_groups:
        image_groups[barcode].sort(key=lambda x: os.path.basename(x).lower()[-5])
    
    return image_groups

def create_batch_summary(total_items: int, successful_items: int, failed_items: int,
                        total_time: float, total_tokens: int, estimated_cost: float,
                        processing_mode: str) -> Dict[str, Any]:
    """
    Create a standardized batch processing summary.
    
    Args:
        total_items: Total number of items processed
        successful_items: Number of successfully processed items
        failed_items: Number of failed items
        total_time: Total processing time in seconds
        total_tokens: Total tokens used
        estimated_cost: Estimated cost in dollars
        processing_mode: Processing mode (BATCH or INDIVIDUAL)
    
    Returns:
        Dictionary with batch summary information
    """
    return {
        "total_items": total_items,
        "successful_items": successful_items,
        "failed_items": failed_items,
        "success_rate": (successful_items / total_items * 100) if total_items > 0 else 0,
        "total_time_seconds": total_time,
        "total_time_minutes": total_time / 60,
        "average_time_per_item": total_time / total_items if total_items > 0 else 0,
        "total_tokens": total_tokens,
        "average_tokens_per_item": total_tokens / total_items if total_items > 0 else 0,
        "estimated_cost_dollars": estimated_cost,
        "cost_per_item": estimated_cost / total_items if total_items > 0 else 0,
        "processing_mode": processing_mode,
        "timestamp": datetime.now().isoformat()
    }