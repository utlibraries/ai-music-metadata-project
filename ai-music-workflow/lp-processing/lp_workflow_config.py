"""
Configuration settings for LP metadata workflow processing.
"""

import datetime
from typing import Dict, Any

# Model configurations for each step
MODEL_CONFIGS = {
    "step1_metadata_extraction": {
        "model": "gpt-4o",
        "max_tokens": 2000,
        "temperature": 0.0,
        "batch_threshold": 10  # Use batch processing if more than this many items
    },
    "step3_ai_analysis": {
        "model": "gpt-4.1-mini",
        "max_tokens": 1500,
        "temperature": 0.5,
        "batch_threshold": 10
    }
}

# File path configurations
FILE_PATHS = {
    "base_dir": "ai-music-workflow/lp-processing",
    "images_folder": "lp-image-folders/lp-scans-5",
    "output_folders": "lp-output-folders",
    "results_folder_prefix": "results-",
    "logs_subfolder": "logs"
}

# Processing thresholds and parameters
PROCESSING_THRESHOLDS = {
    "confidence": {
        "high_confidence": 80,  # Threshold for high confidence matches
        "review_threshold": 79,  # Below this requires manual review
        "minimum_score": 0      # Minimum possible confidence score
    },
    "duplicate_detection": {
        "title_similarity_threshold": 0.9,  # Threshold for similar titles
        "oclc_number_proximity": 5,         # OCLC numbers within this range considered similar
        "confidence_threshold_for_duplicates": 80  # Only consider high confidence items for duplicate detection
    }
}

# OCLC API configuration
OCLC_CONFIG = {
    "api_endpoints": {
        "base_url": "https://americas.discovery.api.oclc.org/worldcat/search/v2",
        "search_endpoint": "/bibs",
        "holdings_endpoint": "/bibs-holdings",
        "single_bib_endpoint": "/bibs/{oclc_number}"
    },
    "search_parameters": {
        "item_type": "music",
        "item_sub_type": "music-lp",
        "in_catalog_language": "eng",
        "default_limit": 10,
        "max_results_threshold": 1000,  # Skip queries with more results than this
    }
}

# Workflow file naming patterns
FILE_NAMING = {
    "oclc_data_json": "oclc-bibliographic-data-{timestamp}.json",
    "error_log_json": "error-log-{timestamp}.json",
    "processing_metrics_json": "processing-metrics-{timestamp}.json",
    "batch_upload_alma": "batch-upload-alma-lp-{timestamp}.txt",
}

# Excel formatting configuration
EXCEL_CONFIG = {
    "column_widths": {
        "barcode": 17,
        "metadata": 52,
        "oclc_query": 52,
        "oclc_results": 52,
        "llm_assessed_oclc": 30,
        "confidence_score": 20,
        "explanation": 40,
        "other_matches": 70,
        "verification_results": 40,
        "year_verification": 40,
        "ixa_holding": 20,
        "other_ixa_holding": 25,
        "processing_time": 18,
        "tokens": 15
    },
    "formatting": {
        "wrap_text": True,
        "vertical_alignment": "top",
        "freeze_panes": "A2",
        "thumbnail_size": (200, 200),
        "row_height_with_images": 215
    }
}

# Step-specific configurations
STEP_CONFIGS = {
    "step1": {
        "max_images_per_item": 3,
        "image_types": {
            "a": "FRONT COVER",
            "b": "BACK COVER", 
            "c": "ADDITIONAL IMAGE"
        }
    },
    "step4": {
        "track_verification": {
            "minimum_tracks_for_verification": 3,
        },
        "year_verification": {
            "allow_missing_years": True,
            "exact_match_required": True,
            "reissue_handling": "use_later_year"
        }
    },
    "step5": {
        "sort_groups": {
            "alma_batch_upload": "Alma Batch Upload (High Confidence)",
            "held_by_ixa": "Held by UT Libraries (IXA)",
            "cataloger_review": "Cataloger Review (Low Confidence)",
            "duplicate": "Duplicate"
        },
        "alma_export": {
            "delimiter": "|",
            "include_headers": False,
            "encoding": "utf-8"
        }
    }
}

def get_current_timestamp() -> str:
    """Get current timestamp for file naming."""
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def get_current_date() -> str:
    """Get current date for file naming."""
    return datetime.datetime.now().strftime("%Y-%m-%d")

def get_step_config(step_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific step.
    
    Args:
        step_name: Name of the step (e.g., 'step1', 'step2', etc.)
    
    Returns:
        Configuration dictionary for the step
    """
    return STEP_CONFIGS.get(step_name, {})

def get_model_config(step_name: str) -> Dict[str, Any]:
    """
    Get model configuration for a specific step.
    
    Args:
        step_name: Name of the step for model configuration
    
    Returns:
        Model configuration dictionary
    """
    model_key = f"{step_name}_metadata_extraction" if step_name == "step1" else f"{step_name}_ai_analysis"
    return MODEL_CONFIGS.get(model_key, MODEL_CONFIGS["step1_metadata_extraction"])

def get_file_path_config() -> Dict[str, str]:
    """
    Get file path configuration with resolved paths.
    
    Returns:
        Dictionary with file path configurations
    """
    import os
    base_dir = FILE_PATHS["base_dir"]
    
    return {
        "base_dir": base_dir,
        "images_folder": os.path.join(base_dir, FILE_PATHS["images_folder"]),
        "output_base": os.path.join(base_dir, FILE_PATHS["output_folders"]),
        "results_prefix": os.path.join(base_dir, FILE_PATHS["output_folders"], FILE_PATHS["results_folder_prefix"]),
        "logs_subfolder": FILE_PATHS["logs_subfolder"]
    }

def get_threshold_config(category: str) -> Dict[str, Any]:
    """
    Get threshold configuration for a specific category.
    
    Args:
        category: Category of thresholds (e.g., 'confidence', 'verification')
    
    Returns:
        Threshold configuration dictionary
    """
    return PROCESSING_THRESHOLDS.get(category, {})