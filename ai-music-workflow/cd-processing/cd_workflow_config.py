"""
Configuration settings for CD metadata workflow processing.
"""

import datetime
from typing import Dict, Any

# Model configurations for each step
MODEL_CONFIGS = {
    "step1_metadata_extraction": {
        "model": "gpt-4.1-mini",
        "max_tokens": 4000,
        "temperature": 0.0,
        "batch_threshold": 10  # Use batch processing if more than this many items
    },
    "step3_ai_analysis": {
        "model": "gpt-4.1",
        "max_tokens": 4000,
        "temperature": 0.5,
        "batch_threshold": 10
    }
}

# File path configurations
FILE_PATHS = {
    "base_dir": "ai-music-workflow/cd-processing",
    "images_folder": "cd-image-folders/cd-scans-5",
    "output_folders": "cd-output-folders",
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
    "verification": {
        "track_similarity_threshold": 80,  # Track similarity percentage threshold
        "track_count_ratio_threshold": 0.7 # Minimum ratio for track count comparison
    },
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
        "item_sub_type": "music-cd",
        "in_catalog_language": "eng",
        "default_limit": 10,
        "max_results_threshold": 1000,  # Skip queries with more results than this
    }
}

# Workflow file naming patterns
FILE_NAMING = {
    "sort_groups_all": "cd-workflow-sorting-{timestamp}.xlsx",
    "batch_upload_alma": "batch-upload-alma-cd-{timestamp}.txt",
    "temp_progress": "temp_cd_metadata_progress.xlsx"
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

def uses_max_completion_tokens(model_name: str) -> bool:
    """
    Determine if a model uses max_completion_tokens instead of max_tokens.

    OpenAI models from 2024-08-06 onwards use max_completion_tokens.
    Older models use max_tokens.

    Args:
        model_name: Name of the OpenAI model

    Returns:
        True if model uses max_completion_tokens, False if it uses max_tokens
    """
    # Models that use max_completion_tokens (newer models)
    new_models = [
        "gpt-5",
        "gpt-5-mini",
        "gpt-5.1",
        "chatgpt-4o-latest",
        "gpt-4o-2024-08-06",
        "gpt-4o-mini-2024-07-18"
    ]

    # Check if model name starts with any of the new model prefixes
    for new_model in new_models:
        if model_name.startswith(new_model):
            return True

    # Check for date-based versioning (models from 2024-08-06 onwards)
    if "2024-08-" in model_name or "2024-09-" in model_name or "2024-1" in model_name or "2025-" in model_name:
        return True

    # All other models use max_tokens
    return False

def supports_temperature_param(model_name: str) -> bool:
    """
    Determine if a model supports custom temperature values.

    Some newer models (like gpt-5-mini) only support the default temperature of 1.

    Args:
        model_name: Name of the OpenAI model

    Returns:
        True if model supports custom temperature, False otherwise
    """
    # Models that don't support custom temperature
    no_temp_models = [
        "gpt-5-mini",
    ]

    for no_temp_model in no_temp_models:
        if model_name.startswith(no_temp_model):
            return False

    return True

def get_token_limit_param(model_name: str, max_tokens: int) -> Dict[str, int]:
    """
    Get the appropriate token limit parameter for a model.

    Args:
        model_name: Name of the OpenAI model
        max_tokens: Token limit value

    Returns:
        Dictionary with either 'max_tokens' or 'max_completion_tokens' as key
    """
    if uses_max_completion_tokens(model_name):
        return {"max_completion_tokens": max_tokens}
    else:
        return {"max_tokens": max_tokens}

def get_temperature_param(model_name: str, temperature: float) -> Dict[str, float]:
    """
    Get the temperature parameter if supported by the model.

    Args:
        model_name: Name of the OpenAI model
        temperature: Desired temperature value

    Returns:
        Dictionary with 'temperature' key if supported, empty dict otherwise
    """
    if supports_temperature_param(model_name):
        return {"temperature": temperature}
    else:
        return {}

def validate_environment() -> Dict[str, bool]:
    """
    Validate that required environment variables and configurations are set.

    Returns:
        Dictionary with validation results
    """
    import os

    validation_results = {
        "openai_api_key": bool(os.getenv('OPENAI_API_KEY')),
        "oclc_client_id": bool(os.getenv('OCLC_CLIENT_ID')),
        "oclc_secret": bool(os.getenv('OCLC_SECRET')),
        "base_directory_exists": os.path.exists(FILE_PATHS["base_dir"]),
        "config_is_valid": True
    }

    # Additional validation logic can be added here
    validation_results["all_valid"] = all(validation_results.values())

    return validation_results