import os
from datetime import datetime
from model_pricing import calculate_cost, get_model_info

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
    
    # Determine if batch processing was used (from additional_metrics)
    is_batch = False
    if additional_metrics and "Processing mode" in additional_metrics:
        is_batch = additional_metrics["Processing mode"] == "BATCH"
    
    # Calculate cost using centralized pricing
    total_cost = calculate_cost(
        model_name=model_name,
        prompt_tokens=total_prompt_tokens,
        completion_tokens=total_completion_tokens,
        is_batch=is_batch
    )
    
    # Get model info for display
    model_info = get_model_info(model_name)
    if not model_info:
        # Fallback if model not found
        model_info = {
            "input_per_1k": 0.00015,
            "output_per_1k": 0.0006,
            "batch_discount": 0.5
        }
    
    # Calculate individual cost components for display
    input_cost = (total_prompt_tokens / 1000) * model_info["input_per_1k"]
    output_cost = (total_completion_tokens / 1000) * model_info["output_per_1k"]
    if is_batch:
        input_cost *= model_info["batch_discount"]
        output_cost *= model_info["batch_discount"]
    
    # Calculate averages
    successful_items = total_items - items_with_issues
    avg_time = total_time / total_items if total_items > 0 else 0
    avg_tokens = total_tokens / successful_items if successful_items > 0 else 0
    avg_cost = total_cost / successful_items if successful_items > 0 else 0
    
    log_file_path = os.path.join(logs_folder_path, f"{script_name}_token_usage_log.txt")
    
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
        log_file.write(f"Input token rate: ${model_info['input_per_1k']:.5f} per 1K tokens\n")
        log_file.write(f"Output token rate: ${model_info['output_per_1k']:.5f} per 1K tokens\n")
        if is_batch:
            log_file.write(f"Batch discount applied: {model_info['batch_discount']*100:.0f}%\n")
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
        log_file.write(f"Input token cost: ${input_cost:.4f}")
        if is_batch:
            log_file.write(f" (batch discounted)\n")
        else:
            log_file.write(f"\n")
        log_file.write(f"Output token cost: ${output_cost:.4f}")
        if is_batch:
            log_file.write(f" (batch discounted)\n")
        else:
            log_file.write(f"\n")
        log_file.write(f"Total actual cost: ${total_cost:.4f}\n")
        log_file.write(f"Average cost per successful item: ${avg_cost:.4f}\n")
        
        # Show savings if batch processing was used
        if is_batch:
            regular_cost = calculate_cost(model_name, total_prompt_tokens, total_completion_tokens, is_batch=False)
            savings = regular_cost - total_cost
            log_file.write(f"Regular API cost would have been: ${regular_cost:.4f}\n")
            log_file.write(f"Batch processing savings: ${savings:.4f}\n")
        log_file.write("\n")
        
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
        cost_per_minute = (total_cost / total_time) * 60 if total_time > 0 else 0
        log_file.write(f"Tokens processed per second: {tokens_per_second:.1f}\n")
        log_file.write(f"Cost per minute of processing: ${cost_per_minute:.4f}\n")
        
        if successful_items > 0:
            items_per_minute = (successful_items / total_time) * 60 if total_time > 0 else 0
            log_file.write(f"Items processed per minute: {items_per_minute:.1f}\n")

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
    
    # Calculate cost for individual call (assume not batch for individual logging)
    individual_cost = calculate_cost(
        model_name=model_name,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        is_batch=False  # Individual calls are not batch
    )
    
    log_file_path = os.path.join(logs_folder_path, f"{script_name}_full_responses_log.txt")
    
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
        log_file.write(f"ESTIMATED COST: ${individual_cost:.4f}\n")
        
        if additional_info:
            for key, value in additional_info.items():
                log_file.write(f"{key.upper()}: {value}\n")
        
        log_file.write(f"\nLLM RESPONSE:\n{response_text}\n\n")
        log_file.write("="*80 + "\n\n")