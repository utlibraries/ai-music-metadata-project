"""
OpenAI Batch Processing Module for AI Music Metadata Project

This module provides batch processing capabilities for OpenAI API calls,
offering significant cost savings (50% discount) and higher rate limits
for large-scale LP metadata processing.

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
from lp_workflow_config import get_model_config, get_token_limit_param, get_temperature_param

def _get_batch_threshold(step_name: str) -> int:
    cfg = get_model_config(step_name)
    return int(cfg.get("batch_threshold", 11))

def _get_step_model(step_name: str) -> str:
    cfg = get_model_config(step_name)
    return cfg.get("model", "gpt-4o-mini-2024-07-18")   

class BatchProcessor:
    """
    Handles OpenAI Batch API operations with robust error handling and monitoring.

    Features:
    - Automatic batch submission and monitoring
    - Cost tracking and logging
    - Error recovery and retry logic with exponential backoff
    - Progress monitoring with status updates
    - Integration with existing token logging system
    - Configurable chunk sizes for large file handling
    - Extended timeouts for large file uploads
    - Persistent batch ID tracking for recovery after interruptions
    """

    def __init__(self, api_key: Optional[str] = None, default_step: str = "step1", persistence_dir: Optional[str] = None):
        """
        Initialize batch processor with OpenAI client.

        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            default_step: Default workflow step name
            persistence_dir: Directory to store batch state (defaults to ~/.ai-music-batch-state)
        """
        # Initialize with extended timeouts for large file uploads
        self.client = OpenAI(
            api_key=api_key or os.getenv('OPENAI_API_KEY'),
            timeout=3600.0,  # 1 hour timeout for large file uploads
            max_retries=0  # Handle retries manually with custom logic
        )
        self.batch_jobs = {}
        self.default_step = default_step

        # Set up persistence directory
        if persistence_dir is None:
            persistence_dir = os.path.expanduser("~/.ai-music-batch-state")
        self.persistence_dir = persistence_dir
        os.makedirs(self.persistence_dir, exist_ok=True)

        self.state_file = os.path.join(self.persistence_dir, "batch_state_lp.json")
        self._load_state()

    def _load_state(self):
        """Load batch state from disk if it exists."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    # Convert ISO format strings back to datetime objects
                    for batch_id, info in state.items():
                        if "created_at" in info and isinstance(info["created_at"], str):
                            info["created_at"] = datetime.fromisoformat(info["created_at"])
                        self.batch_jobs[batch_id] = info
                    if self.batch_jobs:
                        print(f"Loaded {len(self.batch_jobs)} existing batch job(s) from state file")
            except Exception as e:
                print(f"Warning: Failed to load batch state: {e}")

    def _save_state(self):
        """Save current batch state to disk."""
        try:
            # Convert datetime objects to ISO format strings for JSON serialization
            serializable_state = {}
            for batch_id, info in self.batch_jobs.items():
                serializable_info = info.copy()
                if "created_at" in serializable_info and isinstance(serializable_info["created_at"], datetime):
                    serializable_info["created_at"] = serializable_info["created_at"].isoformat()
                serializable_state[batch_id] = serializable_info

            with open(self.state_file, 'w') as f:
                json.dump(serializable_state, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save batch state: {e}")

    def list_active_batches(self) -> List[Dict[str, Any]]:
        """
        List all active batches from state file.

        Returns:
            List of batch information dictionaries
        """
        active_batches = []
        for batch_id, info in self.batch_jobs.items():
            try:
                status_info = self.check_batch_status(batch_id)
                if "error" not in status_info:
                    batch_info = {
                        "batch_id": batch_id,
                        "status": status_info.get("status"),
                        "description": info.get("description"),
                        "request_count": info.get("request_count"),
                        "created_at": info.get("created_at")
                    }
                    active_batches.append(batch_info)
            except Exception as e:
                print(f"Warning: Could not check status for batch {batch_id}: {e}")
        return active_batches

    def resume_batch(self, batch_id: str, max_wait_hours: int = 24, check_interval_minutes: int = 5) -> Optional[List[Dict[str, Any]]]:
        """
        Resume waiting for a batch that was interrupted.

        Args:
            batch_id: ID of the batch to resume
            max_wait_hours: Maximum hours to wait
            check_interval_minutes: Minutes between status checks

        Returns:
            Batch results or None if failed
        """
        print(f"Resuming batch {batch_id}...")

        # Check if batch exists in state
        if batch_id not in self.batch_jobs:
            # Try to get info from OpenAI directly
            status_info = self.check_batch_status(batch_id)
            if "error" in status_info:
                print(f"Batch {batch_id} not found in state or OpenAI")
                return None

            # Add to state
            self.batch_jobs[batch_id] = {
                "created_at": datetime.now(),
                "description": "Resumed batch",
                "request_count": status_info.get("request_counts", {}).get("total", 0)
            }
            self._save_state()

        return self.wait_for_completion(batch_id, max_wait_hours, check_interval_minutes)

    def cleanup_completed_batches(self):
        """Remove completed/failed/expired batches from state file."""
        batches_to_remove = []

        for batch_id in self.batch_jobs.keys():
            status_info = self.check_batch_status(batch_id)
            if "error" not in status_info:
                status = status_info.get("status")
                if status in ["completed", "failed", "expired", "cancelled"]:
                    batches_to_remove.append(batch_id)

        for batch_id in batches_to_remove:
            del self.batch_jobs[batch_id]

        if batches_to_remove:
            self._save_state()
            print(f"Cleaned up {len(batches_to_remove)} completed batch(es) from state")

        
    def should_use_batch(self, num_requests: int, force_batch: bool = False, step_name: Optional[str] = None) -> bool:
        if force_batch:
            return True
        use_batch_env = os.getenv('USE_BATCH_PROCESSING', 'auto').lower()
        if use_batch_env == 'true':
            return True
        if use_batch_env == 'false':
            return False
        step = step_name or self.default_step
        threshold = _get_batch_threshold(step)
        return num_requests > threshold

    
    def create_batch_requests(self, requests_data, custom_id_prefix: str = "req", step_name: Optional[str] = None):
        batch_requests = []
        step = step_name or self.default_step
        step_cfg = get_model_config(step)
        default_model = step_cfg.get("model", "gpt-4o-mini-2024-07-18")
        default_max_tokens = step_cfg.get("max_tokens", 2000)
        default_temperature = step_cfg.get("temperature", 0)

        for i, req_data in enumerate(requests_data):
            model_name = req_data.get("model", default_model)
            max_tokens_value = req_data.get("max_tokens", default_max_tokens)
            temperature_value = req_data.get("temperature", default_temperature)

            # Build the body with model-appropriate token and temperature parameters
            body = {
                "model": model_name,
                "messages": req_data["messages"],
                **get_token_limit_param(model_name, max_tokens_value),
                **get_temperature_param(model_name, temperature_value)
            }
            if "response_format" in req_data:
                body["response_format"] = req_data["response_format"]

            batch_requests.append({
                "custom_id": f"{custom_id_prefix}_{i}_{uuid.uuid4().hex[:8]}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body
            })
        return batch_requests

    
    def estimate_batch_cost(
        self,
        batch_requests: List[Dict[str, Any]],
        model_name: Optional[str] = None,
        step_name: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Estimate the cost of processing a batch of requests.

        Args:
            batch_requests: List of request dictionaries. Can be either:
                            - pre-batch shape: {"model","messages","max_tokens"...}
                            - batch shape: {"custom_id","method","url","body": {...}}
            model_name: Explicit model to use for pricing (optional)
            step_name: Step name whose model to use if model_name not provided (optional)

        Returns:
            Dictionary with cost estimates and savings information
        """
        total_estimated_prompt_tokens = 0
        total_estimated_completion_tokens = 0

        for request in batch_requests:
            # Support both shapes
            messages = request.get("messages")
            if messages is None:
                messages = request.get("body", {}).get("messages", [])

            # ----- prompt token estimate -----
            prompt_text = ""
            image_count = 0
            for message in (messages or []):
                content = message.get("content", "")
                if isinstance(content, str):
                    prompt_text += content
                elif isinstance(content, list):
                    for item in content:
                        if item.get("type") == "text":
                            prompt_text += item.get("text", "")
                        elif item.get("type") == "image_url":
                            image_count += 1

            estimated_prompt_tokens = len(prompt_text) // 4  # ~4 chars/token
            estimated_prompt_tokens += image_count * 1000    # rough visual token cost
            estimated_prompt_tokens += 100                   # system/formatting headroom
            total_estimated_prompt_tokens += estimated_prompt_tokens

            # ----- completion token estimate -----
            max_tokens = request.get("max_tokens")
            if max_tokens is None:
                max_tokens = request.get("body", {}).get("max_tokens", 2000)
            total_estimated_completion_tokens += int(max_tokens * 0.6)

        print(" Token Estimation:")
        print(f"   Estimated prompt tokens: {total_estimated_prompt_tokens:,}")
        print(f"   Estimated completion tokens: {total_estimated_completion_tokens:,}")
        print(f"   Total estimated tokens: {total_estimated_prompt_tokens + total_estimated_completion_tokens:,}")

        # Use provided model, or derive from step (fallback to default step)
        model_for_pricing = model_name or _get_step_model(step_name or self.default_step)

        return estimate_cost(
            model_name=model_for_pricing,
            estimated_prompt_tokens=total_estimated_prompt_tokens,
            estimated_completion_tokens=total_estimated_completion_tokens,
            is_batch=True
        )

    def _upload_file_with_retry(self, file_path: str, max_retries: int = 5) -> Any:
        """
        Upload a file to OpenAI with retry logic for timeouts and server errors.
        
        Args:
            file_path: Path to the file to upload
            max_retries: Maximum number of retry attempts
            
        Returns:
            Uploaded file object
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with open(file_path, 'rb') as f:
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    
                    if attempt > 0:
                        # Exponential backoff with longer waits: 10s, 20s, 40s, 80s, 160s
                        wait_time = 10 * (2 ** (attempt - 1))
                        print(f"   Retry attempt {attempt}/{max_retries} after {wait_time}s wait...")
                        time.sleep(wait_time)
                    
                    print(f"   Uploading {file_size_mb:.1f} MB file... (attempt {attempt + 1}/{max_retries})")
                    batch_input_file = self.client.files.create(
                        file=f,
                        purpose="batch"
                    )
                    print(f"   Upload successful! File ID: {batch_input_file.id}")
                    return batch_input_file
                    
            except Exception as e:
                error_str = str(e)
                last_error = e

                # Log the actual error for debugging
                print(f"   Error: {error_str[:200]}")  # First 200 chars of error

                # Check if it's a retryable error (connection errors, timeouts, server errors, rate limits)
                error_lower = error_str.lower()
                is_connection_error = "connection" in error_lower or "remoteprotocolerror" in error_lower or "apiconnectionerror" in error_lower
                is_timeout = "504" in error_str or "time" in error_lower or "timeout" in error_lower
                is_server_error = any(code in error_str for code in ["500", "502", "503"])
                is_rate_limit = "rate" in error_lower or "429" in error_str

                # Connection errors, timeouts, server errors, and rate limits are all retryable
                if is_connection_error or is_timeout or is_server_error or is_rate_limit:
                    if attempt < max_retries - 1:
                        wait_time = 10 * (2 ** attempt)
                        error_type = ("connection error" if is_connection_error else
                                     "timeout" if is_timeout else
                                     "rate limit" if is_rate_limit else
                                     "server error")
                        print(f"   Upload failed ({error_type}), will retry in {wait_time}s...")
                        continue
                    else:
                        print(f"   Upload failed after {max_retries} attempts: {error_str}")
                        raise
                else:
                    # Non-retryable error
                    print(f"   Upload failed with non-retryable error: {error_str}")
                    raise
        
        # If we got here, all retries failed
        raise last_error

    
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
            print(f"Uploading batch file with {len(batch_requests)} requests...")
            
            # Upload the batch file with retry logic
            batch_input_file = self._upload_file_with_retry(temp_file_path)
            
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

            # Persist to disk immediately
            self._save_state()

            print(f"Batch job submitted successfully!")
            print(f"   Batch ID: {batch_job.id}")
            print(f"   Requests: {len(batch_requests)}")
            print(f"   Status: {batch_job.status}")
            print(f"   Batch state saved to: {self.state_file}")

            return batch_job.id
            
        except Exception as e:
            print(f"Failed to submit batch job: {str(e)}")
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            raise
    
    def submit_adaptive_batch(self, batch_requests: List[Dict[str, Any]], 
                            custom_id_mapping: Dict[str, Any],
                            description: str = "",
                            max_file_size_mb: int = 50) -> List[Dict[str, Any]]:
        """
        Submit batch requests with adaptive splitting based on file size.
        Creates the full batch file, then splits if needed, maintaining order.
        
        Args:
            batch_requests: List of formatted batch requests
            custom_id_mapping: Mapping of custom IDs to original data
            description: Optional description for the batch job
            max_file_size_mb: Maximum file size in MB before splitting (default: 50 MB)
            
        Returns:
            Combined results from all batches
        """
        print(f"Creating batch file for {len(batch_requests)} requests...")
        
        # Create full batch file first
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for request in batch_requests:
                f.write(json.dumps(request) + '\n')
            full_batch_path = f.name
        
        # Check file size
        file_size = os.path.getsize(full_batch_path)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"Full batch file size: {file_size_mb:.1f} MB")
        
        try:
            if file_size_mb <= max_file_size_mb:
                # Single batch processing
                print(f"File size within {max_file_size_mb} MB limit, processing as single batch")
                return self._process_single_batch_file(full_batch_path, description)
            else:
                # Split into multiple batches
                print(f"File exceeds {max_file_size_mb} MB limit, splitting into chunks...")
                return self._process_split_batches(full_batch_path, batch_requests, description, max_file_size_mb)
        
        finally:
            # Clean up the full batch file
            if os.path.exists(full_batch_path):
                os.unlink(full_batch_path)

    def _process_single_batch_file(self, batch_file_path: str, description: str) -> List[Dict[str, Any]]:
        """Process a single batch file."""
        # Use retry logic for upload
        batch_input_file = self._upload_file_with_retry(batch_file_path)

        batch_job = self.client.batches.create(
            input_file_id=batch_input_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": description}
        )

        # Store batch job info and persist
        self.batch_jobs[batch_job.id] = {
            "created_at": datetime.now(),
            "description": description,
            "input_file_id": batch_input_file.id
        }
        self._save_state()

        print(f"Batch job submitted: {batch_job.id}")
        print(f"Batch state saved to: {self.state_file}")
        return self.wait_for_completion(batch_job.id)

    def _process_split_batches(self, full_batch_path: str, batch_requests: List[Dict[str, Any]], 
                            description: str, max_file_size_mb: int) -> List[Dict[str, Any]]:
        """
        Split batch file and process chunks sequentially to avoid overwhelming API.
        """
        
        # Calculate optimal chunk size based on file size
        file_size_mb = os.path.getsize(full_batch_path) / (1024 * 1024)
        estimated_chunks = int(file_size_mb / max_file_size_mb) + 1
        chunk_size = len(batch_requests) // estimated_chunks + 1
        
        print(f"Splitting into approximately {estimated_chunks} chunks of ~{chunk_size} requests each")
        print(f"Note: Chunks will be submitted sequentially to avoid API overload")
        
        chunk_files = []
        batch_ids = []
        
        try:
            # Create all chunk files first
            for chunk_idx in range(0, len(batch_requests), chunk_size):
                chunk_requests = batch_requests[chunk_idx:chunk_idx + chunk_size]
                
                # Create chunk file with properly indexed requests
                chunk_num = chunk_idx // chunk_size
                with tempfile.NamedTemporaryFile(mode='w', suffix=f'_chunk_{chunk_num}.jsonl', delete=False) as f:
                    for request in chunk_requests:
                        f.write(json.dumps(request) + '\n')
                    chunk_file_path = f.name
                
                chunk_files.append(chunk_file_path)
                chunk_size_mb = os.path.getsize(chunk_file_path) / (1024 * 1024)
                chunk_num = chunk_idx // chunk_size + 1
                total_chunks = (len(batch_requests) + chunk_size - 1) // chunk_size
                
                print(f"Chunk {chunk_num}/{total_chunks}: {len(chunk_requests)} requests, {chunk_size_mb:.1f} MB")
            
            # Submit all batches in parallel for faster processing
            print(f"\nSubmitting all {len(chunk_files)} batches in parallel...")
            for i, chunk_file_path in enumerate(chunk_files):
                chunk_num = i + 1
                chunk_description = f"{description} - Chunk {chunk_num}/{len(chunk_files)}"
                
                print(f"Submitting chunk {chunk_num}/{len(chunk_files)}...")
                
                # Upload with retry logic
                batch_input_file = self._upload_file_with_retry(chunk_file_path)
                
                batch_job = self.client.batches.create(
                    input_file_id=batch_input_file.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                    metadata={"description": chunk_description}
                )

                # Store batch job info
                self.batch_jobs[batch_job.id] = {
                    "created_at": datetime.now(),
                    "description": chunk_description,
                    "input_file_id": batch_input_file.id,
                    "chunk_num": chunk_num,
                    "total_chunks": len(chunk_files)
                }

                batch_ids.append(batch_job.id)
                print(f"   Submitted: {batch_job.id}")

            # Save state after all chunks are submitted
            self._save_state()
            print(f"\nAll {len(batch_ids)} chunks submitted successfully!")
            print(f"Batch state saved to: {self.state_file}")
            
            # Wait for all batches to complete
            print(f"\nWaiting for all {len(batch_ids)} batches to complete...")
            all_results = []
            
            # Track completion status
            completed_batches = {}
            
            while len(completed_batches) < len(batch_ids):
                for i, batch_id in enumerate(batch_ids):
                    if batch_id not in completed_batches:
                        status_info = self.check_batch_status(batch_id)
                        
                        if "error" in status_info:
                            print(f"Error checking chunk {i+1} status: {status_info['error']}")
                            completed_batches[batch_id] = None
                            continue
                        
                        status = status_info["status"]
                        
                        if status == "completed":
                            print(f"Chunk {i+1} completed!")
                            results = self._retrieve_batch_results(batch_id, status_info)
                            completed_batches[batch_id] = results
                            all_results.extend(results or [])
                            
                        elif status in ["failed", "expired", "cancelled"]:
                            print(f"Chunk {i+1} {status}!")
                            completed_batches[batch_id] = None
                
                # If not all completed, wait before next check
                if len(completed_batches) < len(batch_ids):
                    completed_count = len(completed_batches)
                    total_count = len(batch_ids)
                    print(f"Progress: {completed_count}/{total_count} chunks completed. Checking again in 30s...")
                    time.sleep(30)
            
            print(f"\nAll chunks completed! Total results: {len(all_results)}")
            return all_results
            
        finally:
            # Clean up all chunk files
            for chunk_file in chunk_files:
                if os.path.exists(chunk_file):
                    os.unlink(chunk_file)

    def check_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """
        Check the status of a batch job.
        
        Args:
            batch_id: ID of the batch job
            
        Returns:
            Dictionary with batch status information
        """
        try:
            batch = self.client.batches.retrieve(batch_id)
            
            return {
                "batch_id": batch_id,
                "status": batch.status,
                "request_counts": batch.request_counts,
                "created_at": batch.created_at,
                "completed_at": batch.completed_at,
                "expires_at": batch.expires_at,
                "output_file_id": batch.output_file_id,
                "error_file_id": batch.error_file_id
            }
            
        except Exception as e:
            return {
                "batch_id": batch_id,
                "error": str(e)
            }
    
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
        
        print(f"Waiting for batch completion (ID: {batch_id})")
        print(f"   Max wait time: {max_wait_hours} hours")
        print(f"   Check interval: {check_interval_minutes} minutes")
        
        last_check = datetime.now()
        
        while datetime.now() - start_time < max_wait_time:
            # Check status
            status_info = self.check_batch_status(batch_id)
            
            if "error" in status_info:
                print(f"Error checking batch status: {status_info['error']}")
                return None
            
            status = status_info["status"]
            request_counts = status_info.get("request_counts", {})
            
            # Print progress update
            if datetime.now() - last_check >= check_interval:
                print(f"Batch Status: {status}")
                if request_counts:
                    total = getattr(request_counts, "total", 0)
                    completed = getattr(request_counts, "completed", 0)
                    failed = getattr(request_counts, "failed", 0)
                    print(f"   Progress: {completed}/{total} completed, {failed} failed")
                last_check = datetime.now()
            
            # Check if completed
            if status == "completed":
                print(f"Batch completed successfully!")
                return self._retrieve_batch_results(batch_id, status_info)
            
            elif status == "failed":
                print(f"Batch failed!")
                self._handle_batch_errors(batch_id, status_info)
                return None
            
            elif status in ["expired", "cancelled"]:
                print(f"Batch {status}!")
                return None
            
            # Wait before next check
            time.sleep(60)  # Check every minute, but only print updates per interval
        
        print(f"Timeout waiting for batch completion after {max_wait_hours} hours")
        return None
    
    def _retrieve_batch_results(self, batch_id: str, 
                              status_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve and parse batch results from completed job."""
        try:
            output_file_id = status_info.get("output_file_id")
            if not output_file_id:
                print(f"No output file ID found for batch {batch_id}")
                return []
            
            print(f"Downloading batch results...")
            
            # Download the results file
            result_content = self.client.files.content(output_file_id)
            
            # Parse JSONL results
            results = []
            for line in result_content.text.strip().split('\n'):
                if line.strip():
                    result = json.loads(line)
                    results.append(result)
            
            print(f"Retrieved {len(results)} batch results")

            # Clean up temporary file if it exists
            if batch_id in self.batch_jobs:
                temp_file_path = self.batch_jobs[batch_id].get("temp_file_path")
                if temp_file_path and os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

                # Remove completed batch from state
                del self.batch_jobs[batch_id]
                self._save_state()
                print(f"Batch {batch_id} removed from state (completed)")

            return results
            
        except Exception as e:
            print(f"Failed to retrieve batch results: {str(e)}")
            return []
    
    def _handle_batch_errors(self, batch_id: str, status_info: Dict[str, Any]):
        """Handle and log batch errors."""
        try:
            error_file_id = status_info.get("error_file_id")
            if error_file_id:
                error_content = self.client.files.content(error_file_id)
                print(f"Batch Error Details:")
                print(error_content.text)
            else:
                print(f"Batch failed but no error file available")
                
        except Exception as e:
            print(f"Failed to retrieve error details: {str(e)}")
    
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
        print(f"Batch Processing Summary:")
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