"""
JSON Workflow Management for AI Music Metadata Project

Manages structured logging and state tracking for the 6-step LP processing workflow.
Maintains processing status, timestamps, results, and audit trails for each LP record.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional, List

def initialize_workflow_json(results_folder_path: str) -> str:
    """
    Initialize the main workflow JSON file for a processing batch.
    
    Returns:
        str: Path to the created JSON file
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    json_file = f"lp-metadata-workflow-{current_date}.json"
    json_path = os.path.join(results_folder_path, json_file)
    
    initial_structure = {
        "batch_info": {
            "created_at": datetime.now().isoformat(),
            "batch_date": current_date,
            "total_records": 0,
            "completed_records": 0,
            "workflow_version": "1.0"
        },
        "records": {}
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(initial_structure, f, indent=2, ensure_ascii=False)
    
    return json_path

def load_workflow_json(json_path: str) -> Dict[str, Any]:
    """Load existing workflow JSON file."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"batch_info": {}, "records": {}}

def save_workflow_json(json_path: str, data: Dict[str, Any]):
    """Save workflow JSON file with proper formatting."""
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def update_record_step1(json_path: str, barcode: str, raw_metadata: str, 
                       extracted_fields: Dict[str, Any], model: str, 
                       prompt_tokens: int, completion_tokens: int, processing_time: float):
    """Update JSON with Step 1 metadata extraction results."""
    data = load_workflow_json(json_path)
    
    if barcode not in data["records"]:
        data["records"][barcode] = {
            "barcode": barcode,
            "processing_status": "in_progress",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
    
    data["records"][barcode]["step1_metadata_extraction"] = {
        "raw_ai_metadata": raw_metadata,
        "extracted_fields": extracted_fields,
        "processing_info": {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "processing_time_seconds": processing_time,
            "completed_at": datetime.now().isoformat()
        }
    }
    
    data["records"][barcode]["updated_at"] = datetime.now().isoformat()
    save_workflow_json(json_path, data)

def update_record_step15_cleaning(json_path: str, barcode: str, 
                                 changes_made: Dict[str, bool], 
                                 upc_extracted: Optional[str] = None):
    """Update JSON with Step 1.5 metadata cleaning results."""
    data = load_workflow_json(json_path)
    
    if barcode in data["records"]:
        data["records"][barcode]["step1_5_metadata_cleaning"] = {
            "numbers_edited": changes_made.get("numbers_edited", False),
            "date_edited": changes_made.get("date_edited", False),
            "valid_numbers_extracted": upc_extracted,
            "completed_at": datetime.now().isoformat()
        }
        
        data["records"][barcode]["updated_at"] = datetime.now().isoformat()
        save_workflow_json(json_path, data)

def update_record_step2(json_path: str, barcode: str, queries_attempted: int, 
                       total_records_found: int):
    """Update JSON with Step 2 OCLC search results."""
    data = load_workflow_json(json_path)
    
    if barcode in data["records"]:
        data["records"][barcode]["step2_oclc_search"] = {
            "queries_attempted": queries_attempted,
            "total_records_found": total_records_found,
            "completed_at": datetime.now().isoformat()
        }
        
        data["records"][barcode]["updated_at"] = datetime.now().isoformat()
        save_workflow_json(json_path, data)

def update_record_step3(json_path: str, barcode: str, selected_oclc: str, 
                       initial_confidence: float, explanation: str, 
                       alternative_matches: List[str], model: str,
                       prompt_tokens: int, completion_tokens: int, processing_time: float):
    """Update JSON with Step 3 AI analysis results."""
    data = load_workflow_json(json_path)
    
    if barcode in data["records"]:
        data["records"][barcode]["step3_ai_analysis"] = {
            "selected_oclc_number": selected_oclc,
            "confidence_score": {
                "initial": initial_confidence,
                "final": initial_confidence  # Will be updated in step 4 if needed
            },
            "explanation": explanation,
            "alternative_matches": alternative_matches,
            "processing_info": {
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "processing_time_seconds": processing_time,
                "completed_at": datetime.now().isoformat()
            }
        }
        
        data["records"][barcode]["updated_at"] = datetime.now().isoformat()
        save_workflow_json(json_path, data)

def update_record_step4(json_path: str, barcode: str, track_similarity: float,
                       track_details: str, year_match_status: str, year_details: str,
                       ixa_selected: str, ixa_alternatives: str, 
                       confidence_adjusted: bool, adjustment_reason: Optional[str],
                       previous_confidence: float, new_confidence: float):
    """Update JSON with Step 4 verification results."""
    data = load_workflow_json(json_path)
    
    if barcode in data["records"]:
        data["records"][barcode]["step4_verification"] = {
            "track_verification": {
                "similarity_score": track_similarity,
                "details": track_details
            },
            "year_verification": {
                "match_status": year_match_status,
                "details": year_details
            },
            "ixa_holdings": {
                "selected_match": ixa_selected,
                "alternative_matches": ixa_alternatives
            },
            "confidence_adjustments": {
                "adjusted": confidence_adjusted,
                "reason": adjustment_reason,
                "previous_score": previous_confidence,
                "new_score": new_confidence
            },
            "completed_at": datetime.now().isoformat()
        }
        
        # Update final confidence score in step 3 data
        if "step3_ai_analysis" in data["records"][barcode]:
            data["records"][barcode]["step3_ai_analysis"]["confidence_score"]["final"] = new_confidence
        
        data["records"][barcode]["updated_at"] = datetime.now().isoformat()
        save_workflow_json(json_path, data)

def update_record_step5(json_path: str, barcode: str, sort_group: str, 
                       final_oclc_number: str, is_duplicate: bool, 
                       oclc_title: str, oclc_author: str, oclc_date: str):
    """Update JSON with Step 5 final classification results."""
    data = load_workflow_json(json_path)
    
    if barcode in data["records"]:
        data["records"][barcode]["step5_final_classification"] = {
            "sort_group": sort_group,
            "oclc_number": final_oclc_number,
            "oclc_title": oclc_title,
            "oclc_author": oclc_author,
            "oclc_publication_date": oclc_date,
            "is_duplicate": is_duplicate,
            "completed_at": datetime.now().isoformat()
        }
        
        data["records"][barcode]["processing_status"] = "completed"
        data["records"][barcode]["updated_at"] = datetime.now().isoformat()
        
        # Update batch info
        data["batch_info"]["completed_records"] = len([r for r in data["records"].values() 
                                                      if r.get("processing_status") == "completed"])
        
        save_workflow_json(json_path, data)

def log_oclc_data(results_folder_path: str, oclc_number: str, bib_data: Dict[str, Any], 
                  holdings_data: Dict[str, Any]):
    """Log OCLC bibliographic and holdings data to separate file."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    oclc_file = f"oclc-bibliographic-data-{current_date}.json"
    oclc_path = os.path.join(results_folder_path, oclc_file)
    
    # Load existing data
    try:
        with open(oclc_path, 'r', encoding='utf-8') as f:
            oclc_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        oclc_data = {}
    
    # Add new data
    oclc_data[oclc_number] = {
        "bibliographic_data": bib_data,
        "holdings_data": holdings_data,
        "retrieved_at": datetime.now().isoformat()
    }
    
    # Save
    with open(oclc_path, 'w', encoding='utf-8') as f:
        json.dump(oclc_data, f, indent=2, ensure_ascii=False)

def log_oclc_api_search(results_folder_path: str, barcode: str, queries: List[str], 
                       raw_api_responses: List[Dict[str, Any]], formatted_results: str,
                       query_log: str, queries_attempted: int, total_records_found: int):
    """Log comprehensive OCLC API search data to logs folder."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Save in logs subfolder
    logs_folder = os.path.join(results_folder_path, "logs")
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)
    
    search_file = f"oclc-api-search-log-{current_date}.json"
    search_path = os.path.join(logs_folder, search_file)
    
    # Load existing data
    try:
        with open(search_path, 'r', encoding='utf-8') as f:
            search_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        search_data = {}
    
    # Add comprehensive search data
    search_data[barcode] = {
        "timestamp": datetime.now().isoformat(),
        "queries_attempted": queries_attempted,
        "total_records_found": total_records_found,
        "query_details": {
            "queries_sent": queries,
            "query_execution_log": query_log
        },
        "api_responses": {
            "raw_responses": raw_api_responses,  # Direct from OCLC API
            "formatted_for_excel": formatted_results  # What goes in the spreadsheet
        },
        "summary": {
            "has_results": total_records_found > 0,
            "unique_queries_count": len(queries),
            "processing_status": "completed"
        }
    }
    
    # Save
    with open(search_path, 'w', encoding='utf-8') as f:
        json.dump(search_data, f, indent=2, ensure_ascii=False)

def log_error(results_folder_path: str, step: str, barcode: str, error_type: str, 
              error_message: str, additional_context: Optional[Dict[str, Any]] = None):
    """Log errors to separate error file."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    error_file = f"error-log-{current_date}.json"
    error_path = os.path.join(results_folder_path, error_file)
    
    # Load existing data
    try:
        with open(error_path, 'r', encoding='utf-8') as f:
            error_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        error_data = []
    
    # Add new error
    error_entry = {
        "timestamp": datetime.now().isoformat(),
        "step": step,
        "barcode": barcode,
        "error_type": error_type,
        "error_message": error_message,
        "additional_context": additional_context or {}
    }
    
    error_data.append(error_entry)
    
    # Save
    with open(error_path, 'w', encoding='utf-8') as f:
        json.dump(error_data, f, indent=2, ensure_ascii=False)

def log_processing_metrics(results_folder_path: str, step: str, batch_metrics: Dict[str, Any]):
    """Log processing metrics to logs folder."""
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Save in logs subfolder
    logs_folder = os.path.join(results_folder_path, "logs")
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)
    
    metrics_file = f"processing-metrics-{current_date}.json"
    metrics_path = os.path.join(logs_folder, metrics_file)  # Changed to logs folder
    
    # Load existing data
    try:
        with open(metrics_path, 'r', encoding='utf-8') as f:
            metrics_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        metrics_data = {}
    
    # Add new metrics
    metrics_data[step] = {
        **batch_metrics,
        "logged_at": datetime.now().isoformat()
    }
    
    # Save
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics_data, f, indent=2, ensure_ascii=False)