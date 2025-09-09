"""
OpenAI Batch Processing Module for AI Music Metadata Project

This module provides batch processing capabilities for OpenAI API calls,
offering significant cost savings (50% discount) and higher rate limits
for large-scale CD metadata processing.

"""

import os
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from openai import OpenAI
import tempfile

# Custom module
from model_pricing import estimate_cost


class BatchProcessor:
    """
    Handles OpenAI Batch API operations with robust error handling and monitoring.
    
    Features:
    - Automatic batch submission and monitoring
    - Cost tracking and logging
    - Error recovery and retry logic
    - Progress monitoring with status updates
    - Integration with existing token logging system
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize batch processor with OpenAI client."""
        self.client = OpenAI(api_key=api_key or os.getenv('OPENAI_API_KEY'))
        self.batch_jobs = {}  # Track active batch jobs
        
    def should_use_batch(self, num_requests: int, force_batch: bool = False) -> bool:
        """
        Determine whether to use batch processing based on request count and settings.
        
        Args:
            num_requests: Number of API requests to process
            force_batch: Override automatic decision
            
        Returns:
            True if batch processing should be used
        """
        if force_batch:
            return True
            
        # Check environment variable
        use_batch_env = os.getenv('USE_BATCH_PROCESSING', 'auto').lower()
        
        if use_batch_env == 'true':
            return True
        elif use_batch_env == 'false':
            return False
        else:  # auto mode
            # Use batch for >10 requests (cost effective threshold)
            return num_requests > 10
    
    def create_batch_requests(self, requests_data: List[Dict[str, Any]], 
                            custom_id_prefix: str = "req") -> List[Dict[str, Any]]:
        """
        Convert request data into OpenAI batch format.
        
        Args:
            requests_data: List of dictionaries containing request parameters
            custom_id_prefix: Prefix for custom request IDs
            
        Returns:
            List of formatted batch requests
        """
        batch_requests = []
        
        for i, req_data in enumerate(requests_data):
            batch_request = {
                "custom_id": f"{custom_id_prefix}_{i}_{uuid.uuid4().hex[:8]}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": req_data.get("model", "gpt-4o-mini-2024-07-18"),
                    "messages": req_data["messages"],
                    "max_tokens": req_data.get("max_tokens", 2000),
                    "temperature": req_data.get("temperature", 0)
                }
            }
            
            # Add optional parameters if present
            if "response_format" in req_data:
                batch_request["body"]["response_format"] = req_data["response_format"]
                
            batch_requests.append(batch_request)
            
        return batch_requests
    
    def estimate_batch_cost(self, batch_requests: List[Dict[str, Any]], model_name: str) -> Dict[str, float]:
        """
        Estimate the cost of processing a batch of requests.
        
        Args:
            batch_requests: List of request dictionaries
            model_name: Model name to use for pricing
            
        Returns:
            Dictionary with cost estimates and savings information
        """
        # More sophisticated token estimation based on request content
        total_estimated_prompt_tokens = 0
        total_estimated_completion_tokens = 0
        
        for request in batch_requests:
            # Estimate prompt tokens based on message content
            prompt_text = ""
            image_count = 0
            
            for message in request.get("messages", []):
                content = message.get("content", "")
                if isinstance(content, str):
                    prompt_text += content
                elif isinstance(content, list):
                    # Handle multi-modal content (text + images)
                    for item in content:
                        if item.get("type") == "text":
                            prompt_text += item.get("text", "")
                        elif item.get("type") == "image_url":
                            image_count += 1
            
            # Rough token estimation: ~4 characters per token
            estimated_prompt_tokens = len(prompt_text) // 4
            
            # Images add significant tokens - rough estimate based on OpenAI pricing
            # High-res images can be 1000+ tokens each
            estimated_prompt_tokens += image_count * 1000
            
            # Add baseline for system messages and formatting
            estimated_prompt_tokens += 100
            
            total_estimated_prompt_tokens += estimated_prompt_tokens
            
            # Estimate completion tokens based on max_tokens setting
            max_tokens = request.get("max_tokens", 2000)
            # Assume we'll use about 60% of max tokens on average
            estimated_completion_tokens = int(max_tokens * 0.6)
            total_estimated_completion_tokens += estimated_completion_tokens
        
        print(f"📊 Token Estimation:")
        print(f"   Estimated prompt tokens: {total_estimated_prompt_tokens:,}")
        print(f"   Estimated completion tokens: {total_estimated_completion_tokens:,}")
        print(f"   Total estimated tokens: {total_estimated_prompt_tokens + total_estimated_completion_tokens:,}")
        
        return estimate_cost(
            model_name=model_name,
            estimated_prompt_tokens=total_estimated_prompt_tokens,
            estimated_completion_tokens=total_estimated_completion_tokens,
            is_batch=True
        )
    
    def submit_batch(self, batch_requests: List[Dict[str, Any]], 
                    description: str = "") -> str:
        """
        Submit a batch job to OpenAI and return the batch ID.
        
        Args:
            batch_requests: List of formatted batch requests
            description: Optional description for the batch job
            
        Returns:
            Batch job ID
        """
        # Create temporary file for batch requests
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for request in batch_requests:
                f.write(json.dumps(request) + '\n')
            temp_file_path = f.name
        
        try:
            print(f"📤 Uploading batch file with {len(batch_requests)} requests...")
            
            # Upload the batch file
            with open(temp_file_path, 'rb') as f:
                batch_input_file = self.client.files.create(
                    file=f,
                    purpose="batch"
                )
            
            # Create the batch job
            batch_job = self.client.batches.create(
                input_file_id=batch_input_file.id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
                metadata={
                    "description": description,
                    "created_at": datetime.now().isoformat(),
                    "request_count": str(len(batch_requests))
                }
            )
            
            # Store batch job info
            self.batch_jobs[batch_job.id] = {
                "created_at": datetime.now(),
                "request_count": len(batch_requests),
                "description": description,
                "input_file_id": batch_input_file.id,
                "temp_file_path": temp_file_path
            }
            
            print(f"✅ Batch job submitted successfully!")
            print(f"   Batch ID: {batch_job.id}")
            print(f"   Requests: {len(batch_requests)}")
            print(f"   Status: {batch_job.status}")
            
            return batch_job.id
            
        except Exception as e:
            print(f"❌ Failed to submit batch job: {str(e)}")
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            raise
    
    def check_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        Check the status of a batch job.
        
        Args:
            batch_id: ID of the batch job
            
        Returns:
            Dictionary containing batch status information
        """
        try:
            batch_job = self.client.batches.retrieve(batch_id)
            
            status_info = {
                "id": batch_job.id,
                "status": batch_job.status,
                "created_at": batch_job.created_at,
                "request_counts": batch_job.request_counts,
                "metadata": batch_job.metadata
            }
            
            # Add completion info if available
            if hasattr(batch_job, 'completed_at') and batch_job.completed_at:
                status_info["completed_at"] = batch_job.completed_at
                status_info["output_file_id"] = batch_job.output_file_id
                
            if hasattr(batch_job, 'error_file_id') and batch_job.error_file_id:
                status_info["error_file_id"] = batch_job.error_file_id
                
            return status_info
            
        except Exception as e:
            print(f"❌ Failed to check batch status: {str(e)}")
            return {"error": str(e)}
    
    def wait_for_completion(self, batch_id: str, 
                          max_wait_hours: int = 24,
                          check_interval_minutes: int = 5) -> Optional[List[Dict[str, Any]]]:
        """
        Wait for batch completion and return results.
        
        Args:
            batch_id: ID of the batch job
            max_wait_hours: Maximum hours to wait for completion
            check_interval_minutes: Minutes between status checks
            
        Returns:
            List of batch results or None if failed/timeout
        """
        start_time = datetime.now()
        max_wait_time = timedelta(hours=max_wait_hours)
        check_interval = timedelta(minutes=check_interval_minutes)
        
        print(f"⏳ Waiting for batch completion (ID: {batch_id})")
        print(f"   Max wait time: {max_wait_hours} hours")
        print(f"   Check interval: {check_interval_minutes} minutes")
        
        last_check = datetime.now()
        
        while datetime.now() - start_time < max_wait_time:
            # Check status
            status_info = self.check_batch_status(batch_id)
            
            if "error" in status_info:
                print(f"❌ Error checking batch status: {status_info['error']}")
                return None
            
            status = status_info["status"]
            request_counts = status_info.get("request_counts", {})
            
            # Print progress update
            if datetime.now() - last_check >= check_interval:
                print(f"🔄 Batch Status: {status}")
                if request_counts:
                    total = getattr(request_counts, "total", 0)
                    completed = getattr(request_counts, "completed", 0)
                    failed = getattr(request_counts, "failed", 0)
                    print(f"   Progress: {completed}/{total} completed, {failed} failed")
                last_check = datetime.now()
            
            # Check if completed
            if status == "completed":
                print(f"✅ Batch completed successfully!")
                return self._retrieve_batch_results(batch_id, status_info)
            
            elif status == "failed":
                print(f"❌ Batch failed!")
                self._handle_batch_errors(batch_id, status_info)
                return None
            
            elif status in ["expired", "cancelled"]:
                print(f"⚠️ Batch {status}!")
                return None
            
            # Wait before next check
            time.sleep(60)  # Check every minute, but only print updates per interval
        
        print(f"⏰ Timeout waiting for batch completion after {max_wait_hours} hours")
        return None
    
    def _retrieve_batch_results(self, batch_id: str, 
                              status_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve and parse batch results from completed job."""
        try:
            output_file_id = status_info.get("output_file_id")
            if not output_file_id:
                print(f"❌ No output file ID found for batch {batch_id}")
                return []
            
            print(f"📥 Downloading batch results...")
            
            # Download the results file
            result_content = self.client.files.content(output_file_id)
            
            # Parse JSONL results
            results = []
            for line in result_content.text.strip().split('\n'):
                if line.strip():
                    result = json.loads(line)
                    results.append(result)
            
            print(f"✅ Retrieved {len(results)} batch results")
            
            # Clean up temporary file if it exists
            if batch_id in self.batch_jobs:
                temp_file_path = self.batch_jobs[batch_id].get("temp_file_path")
                if temp_file_path and os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    
            return results
            
        except Exception as e:
            print(f"❌ Failed to retrieve batch results: {str(e)}")
            return []
    
    def _handle_batch_errors(self, batch_id: str, status_info: Dict[str, Any]):
        """Handle and log batch errors."""
        try:
            error_file_id = status_info.get("error_file_id")
            if error_file_id:
                error_content = self.client.files.content(error_file_id)
                print(f"📄 Batch Error Details:")
                print(error_content.text)
            else:
                print(f"❌ Batch failed but no error file available")
                
        except Exception as e:
            print(f"❌ Failed to retrieve error details: {str(e)}")
    
    def process_batch_results(self, results: List[Dict[str, Any]], 
                            custom_id_mapping: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process batch results and map them back to original requests.
        
        Args:
            results: Raw batch results from OpenAI
            custom_id_mapping: Mapping of custom IDs to original data
            
        Returns:
            Dictionary mapping custom IDs to processed results
        """
        processed_results = {}
        total_prompt_tokens = 0
        total_completion_tokens = 0
        successful_results = 0
        failed_results = 0
        
        for result in results:
            custom_id = result.get("custom_id")
            
            if "response" in result and result["response"]:
                # Successful result
                response = result["response"]
                body = response.get("body", {})
                
                if "choices" in body and body["choices"]:
                    # Extract the content
                    content = body["choices"][0]["message"]["content"]
                    usage = body.get("usage", {})
                    
                    processed_results[custom_id] = {
                        "success": True,
                        "content": content,
                        "usage": usage,
                        "custom_id": custom_id
                    }
                    
                    # Track token usage
                    total_prompt_tokens += usage.get("prompt_tokens", 0)
                    total_completion_tokens += usage.get("completion_tokens", 0)
                    successful_results += 1
                    
                else:
                    # Response structure issue
                    processed_results[custom_id] = {
                        "success": False,
                        "error": "Invalid response structure",
                        "custom_id": custom_id
                    }
                    failed_results += 1
            
            elif "error" in result:
                # Failed result
                error_info = result["error"]
                processed_results[custom_id] = {
                    "success": False,
                    "error": error_info,
                    "custom_id": custom_id
                }
                failed_results += 1
            
            else:
                # Unknown result format
                processed_results[custom_id] = {
                    "success": False,
                    "error": "Unknown result format",
                    "custom_id": custom_id
                }
                failed_results += 1
        
        # Print summary
        print(f"📊 Batch Processing Summary:")
        print(f"   Successful: {successful_results}")
        print(f"   Failed: {failed_results}")
        print(f"   Total prompt tokens: {total_prompt_tokens:,}")
        print(f"   Total completion tokens: {total_completion_tokens:,}")
        
        return {
            "results": processed_results,
            "summary": {
                "successful": successful_results,
                "failed": failed_results,
                "total_prompt_tokens": total_prompt_tokens,
                "total_completion_tokens": total_completion_tokens
            }
        }