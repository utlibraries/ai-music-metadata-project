#!/usr/bin/env python3
"""
AI Music CD Processing Workflow runner script.
Executes all 6 steps of the CD processing workflow in sequence.
Don't forget to point to the correct images folder in Script 1!
"""

import subprocess
import sys
import time
import os
from datetime import datetime

def run_script(script_name, step_number, step_description):
    """Run a Python script and handle any errors."""
    print(f"\n{'='*60}")
    print(f"STEP {step_number}: {step_description}")
    print(f"Running: {script_name}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    # Get the directory where this runner script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, script_name)
    
    # Check if the script exists
    if not os.path.exists(script_path):
        print(f"STEP {step_number} FAILED")
        print(f"Error: Could not find script '{script_name}' in directory '{script_dir}'")
        print(f"Looking for: {script_path}")
        print("Make sure all script files are in the same directory as this runner.")
        return False
    
    try:
        print(f"\n🔄 REAL-TIME OUTPUT:")
        print("-" * 40)
        
        # Use a much simpler approach - just run with direct inheritance
        result = subprocess.run([
            sys.executable, '-u', script_path
        ], 
        env={**os.environ, 'PYTHONUNBUFFERED': '1'},
        text=True)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            print(f"\nSTEP {step_number} COMPLETED SUCCESSFULLY")
            print(f"Duration: {duration:.2f} seconds")
            return True
        else:
            print(f"\nSTEP {step_number} FAILED")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Error code: {result.returncode}")
            return False
        
    except FileNotFoundError:
        print(f"\nSTEP {step_number} FAILED")
        print(f"Error: Could not find script '{script_name}'")
        print("Make sure all script files are in the same directory as this runner.")
        return False
    
    except Exception as e:
        print(f"\n STEP {step_number} FAILED")
        print(f"Unexpected error: {str(e)}")
        return False

def check_environment():
    """Check if required environment variables are set."""
    required_vars = ['OPENAI_API_KEY', 'OCLC_CLIENT_ID', 'OCLC_SECRET']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"ENVIRONMENT CHECK FAILED")
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print(f"Please set these environment variables before running the workflow.")
        return False
    
    print(f"ENVIRONMENT CHECK PASSED")
    print(f"All required environment variables are set.")
    return True

def main():
    """Main function to run the entire CD processing workflow."""
    print("AI MUSIC CD PROCESSING WORKFLOW")
    print("=" * 60)
    print(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check environment variables
    if not check_environment():
        print(f"\nPlease fix environment issues and try again.")
        return
    
    # Define the workflow steps
    steps = [
        ("ai-music-step-1-cd.py", 1, "Extract metadata from CD images using AI"),
        ("ai-music-step-1.5-cd.py", 1.5, "Clean and normalize extracted metadata"),
        ("ai-music-step-2-cd.py", 2, "Search OCLC database for matching records"),
        ("ai-music-step-3-cd.py", 3, "Analyze OCLC matches using AI"),
        ("ai-music-step-4-cd.py", 4, "Verify track listings and publication years"),
        ("ai-music-step-5-cd.py", 5, "Create final sorted results and batch files")
    ]
    
    # Track overall progress
    workflow_start_time = time.time()
    successful_steps = 0
    total_steps = len(steps)
    
    # Run each step
    for script_name, step_number, description in steps:
        print(f"\nSTARTING STEP {step_number}")
        print(f"Progress: {successful_steps}/{total_steps} steps completed")
        
        success = run_script(script_name, step_number, description)
        
        if success:
            successful_steps += 1
            print(f"\nStep {step_number} completed successfully!")
            print(f"Overall progress: {successful_steps}/{total_steps} steps completed")
        else:
            print(f"\nPROCESSING STOPPED")
            print(f"Step {step_number} failed. Cannot continue to next step.")
            break
        
        # Brief pause between steps
        if step_number < total_steps:
            print(f"\nPausing 2 seconds before next step...")
            time.sleep(2)
    
    # Final summary
    workflow_end_time = time.time()
    total_duration = workflow_end_time - workflow_start_time
    
    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    print(f"Steps completed: {successful_steps}/{total_steps}")
    
    if successful_steps == total_steps:
        print(f"PROCESSING COMPLETED SUCCESSFULLY!")
        print(f"All CD processing steps finished. Check the results folder for output files.")
    else:
        print(f"PROCESSING INCOMPLETE")
        print(f"Only {successful_steps} out of {total_steps} steps completed successfully.")
    
    print(f"\nProcessing finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()