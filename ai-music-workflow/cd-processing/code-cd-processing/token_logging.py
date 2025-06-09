import os
from datetime import datetime

# Pricing data for OpenAI models (per 1M tokens)
OPENAI_PRICING = {
    "gpt-4o-2024-08-06": {
        "input": 2.50,
        "cached_input": 1.25,
        "output": 10.00
    },
    "gpt-4o-mini-2024-07-18": {
        "input": 0.15,
        "cached_input": 0.075,
        "output": 0.60
    }
}

def calculate_cost(model, prompt_tokens, completion_tokens, cached_tokens=0):
    """
    Calculate estimated cost based on model and token usage.
    
    Args:
        model (str): Model name (e.g., "gpt-4o-mini-2024-07-18", "gpt-4o-2024-08-06")
        prompt_tokens (int): Number of prompt tokens
        completion_tokens (int): Number of completion tokens
        cached_tokens (int): Number of cached input tokens (optional)
    
    Returns:
        dict: Cost breakdown with total, input_cost, output_cost, cached_cost
    """
    if model not in OPENAI_PRICING:
        # Default to gpt-4o-mini pricing if model not found
        pricing = OPENAI_PRICING["gpt-4o-mini-2024-07-18"]
    else:
        pricing = OPENAI_PRICING[model]
    
    # Calculate costs (convert to dollars from per-1M pricing)
    regular_input_tokens = max(0, prompt_tokens - cached_tokens)
    input_cost = (regular_input_tokens / 1000000) * pricing["input"]
    cached_cost = (cached_tokens / 1000000) * pricing["cached_input"]
    output_cost = (completion_tokens / 1000000) * pricing["output"]
    total_cost = input_cost + cached_cost + output_cost
    
    return {
        "total_cost": total_cost,
        "input_cost": input_cost,
        "output_cost": output_cost,
        "cached_cost": cached_cost
    }

def create_token_usage_log(logs_folder_path, script_name, model_name, total_items, items_with_issues, 
                          total_time, total_prompt_tokens, total_completion_tokens, 
                          total_cached_tokens=0, additional_metrics=None):
    """
    Create a standardized token usage log file.
    
    Args:
        logs_folder_path (str): Path to logs folder
        script_name (str): Name of the script (e.g., "metadata_creation", "metadata_analysis")
        model_name (str): OpenAI model used
        total_items (int): Total number of items processed
        items_with_issues (int): Number of items that had issues
        total_time (float): Total processing time in seconds
        total_prompt_tokens (int): Total prompt tokens used
        total_completion_tokens (int): Total completion tokens used
        total_cached_tokens (int): Total cached tokens used (optional)
        additional_metrics (dict): Any additional metrics to include
    """
    total_tokens = total_prompt_tokens + total_completion_tokens
    cost_breakdown = calculate_cost(model_name, total_prompt_tokens, total_completion_tokens, total_cached_tokens)
    
    # Calculate averages
    successful_items = total_items - items_with_issues
    avg_time = total_time / total_items if total_items > 0 else 0
    avg_tokens = total_tokens / successful_items if successful_items > 0 else 0
    avg_cost = cost_breakdown["total_cost"] / successful_items if successful_items > 0 else 0
    
    log_file_path = os.path.join(logs_folder_path, f"token_usage_log_{script_name}.txt")
    
    with open(log_file_path, "w") as log_file:
        log_file.write(f"OpenAI API Usage Log - {script_name.replace('_', ' ').title()}\n")
        log_file.write("="*60 + "\n")
        log_file.write(f"Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Processing Summary
        log_file.write("PROCESSING SUMMARY:\n")
        log_file.write("-" * 30 + "\n")
        log_file.write(f"Total items processed: {total_items}\n")
        log_file.write(f"Successful items: {successful_items}\n")
        log_file.write(f"Items with issues: {items_with_issues}\n")
        log_file.write(f"Success rate: {((successful_items / total_items) * 100):.1f}%\n" if total_items > 0 else "Success rate: 0%\n")
        log_file.write(f"Total processing time: {total_time:.2f} seconds\n")
        log_file.write(f"Average time per item: {avg_time:.2f} seconds\n\n")
        
        # Model Information
        log_file.write("MODEL INFORMATION:\n")
        log_file.write("-" * 30 + "\n")
        log_file.write(f"Model used: {model_name}\n")
        log_file.write(f"Input token rate: ${OPENAI_PRICING.get(model_name, OPENAI_PRICING['gpt-4o-mini-2024-07-18'])['input']:.2f} per 1M tokens\n")
        log_file.write(f"Output token rate: ${OPENAI_PRICING.get(model_name, OPENAI_PRICING['gpt-4o-mini-2024-07-18'])['output']:.2f} per 1M tokens\n")
        if total_cached_tokens > 0:
            log_file.write(f"Cached input rate: ${OPENAI_PRICING.get(model_name, OPENAI_PRICING['gpt-4o-mini-2024-07-18'])['cached_input']:.2f} per 1M tokens\n")
        log_file.write("\n")
        
        # Token Usage
        log_file.write("TOKEN USAGE:\n")
        log_file.write("-" * 30 + "\n")
        log_file.write(f"Total prompt tokens: {total_prompt_tokens:,}\n")
        log_file.write(f"Total completion tokens: {total_completion_tokens:,}\n")
        if total_cached_tokens > 0:
            log_file.write(f"Total cached tokens: {total_cached_tokens:,}\n")
        log_file.write(f"Total tokens: {total_tokens:,}\n")
        log_file.write(f"Average tokens per successful item: {avg_tokens:.1f}\n\n")
        
        # Cost Breakdown
        log_file.write("COST BREAKDOWN:\n")
        log_file.write("-" * 30 + "\n")
        log_file.write(f"Input token cost: ${cost_breakdown['input_cost']:.4f}\n")
        log_file.write(f"Output token cost: ${cost_breakdown['output_cost']:.4f}\n")
        if total_cached_tokens > 0:
            log_file.write(f"Cached input cost: ${cost_breakdown['cached_cost']:.4f}\n")
        log_file.write(f"Total estimated cost: ${cost_breakdown['total_cost']:.4f}\n")
        log_file.write(f"Average cost per successful item: ${avg_cost:.4f}\n\n")
        
        # Additional metrics if provided
        if additional_metrics:
            log_file.write("ADDITIONAL METRICS:\n")
            log_file.write("-" * 30 + "\n")
            for key, value in additional_metrics.items():
                log_file.write(f"{key}: {value}\n")
            log_file.write("\n")
        
        # Efficiency Metrics
        log_file.write("EFFICIENCY METRICS:\n")
        log_file.write("-" * 30 + "\n")
        tokens_per_second = total_tokens / total_time if total_time > 0 else 0
        cost_per_minute = (cost_breakdown["total_cost"] / total_time) * 60 if total_time > 0 else 0
        log_file.write(f"Tokens processed per second: {tokens_per_second:.1f}\n")
        log_file.write(f"Cost per minute of processing: ${cost_per_minute:.4f}\n")
        
        if successful_items > 0:
            log_file.write(f"Items processed per minute: {(successful_items / total_time) * 60:.1f}\n" if total_time > 0 else "Items processed per minute: 0\n")

def log_individual_response(logs_folder_path, script_name, row_number, barcode, response_text, 
                           model_name, prompt_tokens, completion_tokens, processing_time, 
                           cached_tokens=0, additional_info=None):
    """
    Log individual LLM responses with token and cost information.
    
    Args:
        logs_folder_path (str): Path to logs folder
        script_name (str): Name of the script
        row_number (int): Row number being processed
        barcode (str): Barcode identifier
        response_text (str): Full LLM response
        model_name (str): OpenAI model used
        prompt_tokens (int): Prompt tokens for this call
        completion_tokens (int): Completion tokens for this call
        processing_time (float): Processing time for this call
        cached_tokens (int): Cached tokens for this call (optional)
        additional_info (dict): Any additional information to log
    """
    total_tokens = prompt_tokens + completion_tokens + cached_tokens
    cost_breakdown = calculate_cost(model_name, prompt_tokens, completion_tokens, cached_tokens)
    
    log_file_path = os.path.join(logs_folder_path, f"full_responses_log_{script_name}.txt")
    
    # Check if this is the first entry (create header)
    file_exists = os.path.exists(log_file_path)
    
    with open(log_file_path, "a") as log_file:
        if not file_exists:
            log_file.write(f"LLM Full Responses Log - {script_name.replace('_', ' ').title()}\n")
            log_file.write(f"Created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write("="*80 + "\n\n")
        
        log_file.write(f"Row {row_number} - Barcode {barcode} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write("-"*80 + "\n")
        log_file.write(f"MODEL: {model_name}\n")
        log_file.write(f"PROCESSING TIME: {processing_time:.2f}s\n")
        log_file.write(f"TOKENS: {total_tokens:,} (Prompt: {prompt_tokens:,}, Completion: {completion_tokens:,}")
        if cached_tokens > 0:
            log_file.write(f", Cached: {cached_tokens:,}")
        log_file.write(")\n")
        log_file.write(f"ESTIMATED COST: ${cost_breakdown['total_cost']:.4f}\n")
        
        if additional_info:
            for key, value in additional_info.items():
                log_file.write(f"{key.upper()}: {value}\n")
        
        log_file.write(f"\nLLM RESPONSE:\n{response_text}\n\n")
        log_file.write("="*80 + "\n\n")