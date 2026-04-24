"""
Configuration settings for CD metadata workflow processing.
"""

import datetime
import os
from typing import Dict, Any

# Portkey gateway configuration
# Set enabled to True to route individual (non-batch) OpenAI calls through Portkey.
# Requires PORTKEY_API_KEY and PORTKEY_VIRTUAL_KEY environment variables.
# Batch processing always uses OpenAI directly (Portkey does not support the Batch API).
PORTKEY_CONFIG = {
    "enabled": False,
    "api_key_env": "PORTKEY_API_KEY",
    "virtual_key_env": "PORTKEY_VIRTUAL_KEY"
}

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
    "images_folder": "local-cd-image-folders/cd-scans-10",
    "output_folders": "cd-output-folders",
    "results_folder_prefix": "results-",
    "logs_subfolder": "logs"
}

# Processing thresholds and parameters
PROCESSING_THRESHOLDS = {
    "confidence": {
        "high_confidence": 70,  # Threshold for high confidence matches
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

def get_openai_client():
    """
    Return an API client for OpenAI calls.

    If PORTKEY_CONFIG['enabled'] is True and the required Portkey environment
    variables are set, returns a Portkey client (routes calls through the
    Portkey AI gateway). Otherwise returns a standard OpenAI client.

    Note: batch processing in batch_processor.py always uses OpenAI directly.
    """
    if PORTKEY_CONFIG.get("enabled"):
        portkey_api_key = os.getenv(PORTKEY_CONFIG["api_key_env"])
        portkey_virtual_key = os.getenv(PORTKEY_CONFIG["virtual_key_env"])
        if portkey_api_key and portkey_virtual_key:
            try:
                from portkey_ai import Portkey
                return Portkey(api_key=portkey_api_key, virtual_key=portkey_virtual_key)
            except ImportError:
                print("Warning: portkey_ai package not installed. Falling back to OpenAI.")
        else:
            print("Warning: Portkey enabled but PORTKEY_API_KEY/PORTKEY_VIRTUAL_KEY not set. Falling back to OpenAI.")

    from openai import OpenAI
    return OpenAI(api_key=os.getenv('OPENAI_API_KEY'))


def validate_environment() -> Dict[str, bool]:
    """
    Validate that required environment variables and configurations are set.

    Returns:
        Dictionary with validation results
    """
    import os

    using_portkey = PORTKEY_CONFIG.get("enabled", False)
    validation_results = {
        "openai_api_key": using_portkey or bool(os.getenv('OPENAI_API_KEY')),
        "oclc_client_id": bool(os.getenv('OCLC_CLIENT_ID')),
        "oclc_secret": bool(os.getenv('OCLC_SECRET')),
        "base_directory_exists": os.path.exists(FILE_PATHS["base_dir"]),
        "config_is_valid": True
    }
    if using_portkey:
        validation_results["portkey_api_key"] = bool(os.getenv(PORTKEY_CONFIG["api_key_env"]))
        validation_results["portkey_virtual_key"] = bool(os.getenv(PORTKEY_CONFIG["virtual_key_env"]))

    # Additional validation logic can be added here
    validation_results["all_valid"] = all(validation_results.values())

    return validation_results