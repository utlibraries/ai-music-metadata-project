"""
Centralized model pricing configuration for OpenAI models.
Updated as of July 2025 - verify current pricing at https://openai.com/pricing
"""

MODEL_PRICING = {
    # GPT-4o models
    "gpt-4o": {
        "input_per_1k": 0.0025,
        "output_per_1k": 0.01,
        "batch_discount": 0.5
    },
    "gpt-4o-2024-08-06": {
        "input_per_1k": 0.0025,
        "output_per_1k": 0.01,
        "batch_discount": 0.5
    },
    
    # GPT-4o-mini models
    "gpt-4o-mini": {
        "input_per_1k": 0.00015,
        "output_per_1k": 0.0006,
        "batch_discount": 0.5
    },
    "gpt-4o-mini-2024-07-18": {
        "input_per_1k": 0.00015,
        "output_per_1k": 0.0006,
        "batch_discount": 0.5
    },
    
    # GPT-4.1 models
    "gpt-4.1": {
        "input_per_1k": 0.002,
        "output_per_1k": 0.008,
        "batch_discount": 0.5
    },
    "gpt-4.1-2025-04-14": {
        "input_per_1k": 0.002,
        "output_per_1k": 0.008,
        "batch_discount": 0.5
    },
    
    # GPT-4.1-mini models
    "gpt-4.1-mini": {
        "input_per_1k": 0.0004,
        "output_per_1k": 0.0016,
        "batch_discount": 0.5
    },
    "gpt-4.1-mini-2025-04-14": {
        "input_per_1k": 0.0004,
        "output_per_1k": 0.0016,
        "batch_discount": 0.5
    }
}

def calculate_cost(model_name, prompt_tokens, completion_tokens, is_batch=False):
    """
    Calculate the cost for a given model and token usage.
    
    Args:
        model_name (str): The model name
        prompt_tokens (int): Number of input tokens
        completion_tokens (int): Number of output tokens
        is_batch (bool): Whether this was a batch request (for discount)
    
    Returns:
        float: Total cost in USD
    """
    if model_name not in MODEL_PRICING:
        print(f"⚠️  Warning: Unknown model '{model_name}', using GPT-4o-mini pricing as fallback")
        model_name = "gpt-4o-mini"
    
    pricing = MODEL_PRICING[model_name]
    
    input_cost = (prompt_tokens / 1000) * pricing["input_per_1k"]
    output_cost = (completion_tokens / 1000) * pricing["output_per_1k"]
    total_cost = input_cost + output_cost
    
    if is_batch:
        total_cost *= pricing["batch_discount"]
    
    return total_cost

def get_model_info(model_name):
    """
    Get pricing information for a model.
    
    Args:
        model_name (str): The model name
    
    Returns:
        dict: Pricing information
    """
    if model_name not in MODEL_PRICING:
        print(f"⚠️  Warning: Unknown model '{model_name}'")
        return None
    
    return MODEL_PRICING[model_name]

def estimate_cost(model_name, estimated_prompt_tokens, estimated_completion_tokens, is_batch=False):
    """
    Estimate cost before making API calls.
    
    Args:
        model_name (str): The model name
        estimated_prompt_tokens (int): Estimated input tokens
        estimated_completion_tokens (int): Estimated output tokens
        is_batch (bool): Whether this will be a batch request
    
    Returns:
        dict: Cost breakdown
    """
    regular_cost = calculate_cost(model_name, estimated_prompt_tokens, estimated_completion_tokens, is_batch=False)
    batch_cost = calculate_cost(model_name, estimated_prompt_tokens, estimated_completion_tokens, is_batch=True)
    
    return {
        "regular_cost": regular_cost,
        "batch_cost": batch_cost,
        "savings": regular_cost - batch_cost,
        "savings_percentage": ((regular_cost - batch_cost) / regular_cost) * 100 if regular_cost > 0 else 0
    }