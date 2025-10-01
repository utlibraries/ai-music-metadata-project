"""
AI Music LP Processing Workflow runner script.
Executes all 6 steps of the LP processing workflow in sequence.
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
        print(f"\n REAL-TIME OUTPUT:")
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

def validate_image_files():
    """Run file validation and handle user confirmation for issues."""
    print(f"\n{'='*60}")
    print(f"PRE-PROCESSING: Validating image file formats")
    print(f"{'='*60}")
    
    # Get the directory where this runner script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    validation_script = os.path.join(script_dir, "ai-music-step-.5-lp.py")
    
    if not os.path.exists(validation_script):
        print(f"Warning: Could not find validation script 'ai-music-step-.5-lp.py'")
        print(f"Skipping file validation...")
        return True
    
    max_attempts = 3
    attempt = 1
    
    while attempt <= max_attempts:
        print(f"\nValidation attempt {attempt}/{max_attempts}")
        print("-" * 40)
        
        try:
            # Run the validation script and capture output
            result = subprocess.run([
                sys.executable, '-u', validation_script
            ], 
            env={**os.environ, 'PYTHONUNBUFFERED': '1'},
            text=True,
            capture_output=True)
            
            # Print the output
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print("Errors:", result.stderr)
            
            # Check if validation passed (return code 0 means no issues)
            if result.returncode == 0:
                print(f"\nFILE VALIDATION PASSED")
                print(f"All image files are properly formatted.")
                return True
            else:
                print(f"\nFILE VALIDATION FAILED")
                print(f"Issues found with image file formatting.")
                
                if attempt < max_attempts:
                    print(f"\nPlease fix the issues listed above, then press Enter to re-validate...")
                    print(f"Or type 'skip' to continue anyway (not recommended):")
                    
                    user_input = input().strip().lower()
                    if user_input == 'skip':
                        print(f"Skipping validation - proceeding with potentially invalid files...")
                        return True
                    
                    attempt += 1
                else:
                    print(f"\nValidation failed after {max_attempts} attempts.")
                    print(f"Please fix the file formatting issues before running the workflow.")
                    return False
                    
        except Exception as e:
            print(f"Error running validation: {str(e)}")
            return False
    
    return False

def main():
    """Main function to run the entire LP processing workflow."""
    print("AI MUSIC LP PROCESSING WORKFLOW") 
    print("=" * 60)
    print(f"Processing started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check environment variables
    if not check_environment():
        print(f"\nPlease fix environment issues and try again.")
        return
    
    # Validate image files before starting processing
    if not validate_image_files():
        print(f"\nFile validation failed. Please fix issues and try again.")
        return
    
    # Ask about HTML generation upfront
    print(f"\n{'='*60}")
    print(f"HTML REVIEW INTERFACE OPTION")
    print(f"{'='*60}")
    print(f"\nStep 6 creates an interactive HTML review interface that copies all images in this run to the results folder.")
    print(f"The entire results folder must be downloaded and opened locally on your computer (unzipped) in order to view the HTML.")
    print(f"The HTML website can then be opened in a web browser by double clicking on index.html.")
    print(f"\nBenefits: 1. Easy review of AI-suggested OCLC matches alongside full size images of LPs.")
    print(f"          2. Records sortable by confidence.")
    print(f"          3. Cataloger decisions may then be exported to CSV.")
    print(f"\nImportant: The HTML runs entirely on your local machine with no external connections.")
    print(f"           Decisions are stored in your browser's local storage only.")
    print(f"           You must export decisions to CSV and manually save the file to preserve your work.")
    print(f"\nNote: Not recommended for batches over 500 records due to the size of the generated folder.")
    print(f"          For the same reason, we recommend using JPEG format for images when intending to generate HTML.")
    print(f"\nGenerate HTML review interface? (y/n): ", end='')
    
    run_html_step = input().strip().lower() == 'y'
    
    if run_html_step:
        print(f"HTML review will be generated after Step 5.")
    else:
        print(f"Skipping HTML generation. Only spreadsheet/text outputs will be created.")
    
    # Define the workflow steps
    steps = [
        ("ai-music-step-1-lp.py", 1, "Extract metadata from LP images using AI"),
        ("ai-music-step-1.5-lp.py", 1.5, "Clean and normalize extracted metadata"),
        ("ai-music-step-2-lp.py", 2, "Search OCLC database for matching records"),
        ("ai-music-step-3-lp.py", 3, "Analyze OCLC matches using AI"),
        ("ai-music-step-4-lp.py", 4, "Verify track listings and publication years"),
        ("ai-music-step-5-lp.py", 5, "Create final sorted results and batch files")
    ]
    
    # Add Step 6 if user chose it
    if run_html_step:
        steps.append(("ai-music-step-6-lp.py", 6, "Create interactive HTML review interface"))
    
    # Track overall progress
    workflow_start_time = time.time()
    successful_steps = 0
    
    # Run each step
    for script_name, step_number, description in steps:
        print(f"\nSTARTING STEP {step_number}")
        print(f"Progress: {successful_steps}/{len(steps)} steps completed")
        
        success = run_script(script_name, step_number, description)
        
        if success:
            successful_steps += 1
            print(f"\nStep {step_number} completed successfully!")
            print(f"Overall progress: {successful_steps}/{len(steps)} steps completed")
        else:
            print(f"\nPROCESSING STOPPED")
            print(f"Step {step_number} failed. Cannot continue to next step.")
            break
        
        # Brief pause between steps
        if step_number < len(steps):
            print(f"\nPausing 2 seconds before next step...")
            time.sleep(2)
    
    # Final summary
    workflow_end_time = time.time()
    total_duration = workflow_end_time - workflow_start_time
    
    print(f"\n{'='*60}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    print(f"Steps completed: {successful_steps}/{len(steps)} steps")

    if successful_steps == len(steps):
        print(f"PROCESSING COMPLETED SUCCESSFULLY!")
        print(f"All LP processing steps finished. Check the results folder for output files.")
    else:
        print(f"PROCESSING INCOMPLETE")
        print(f"Only {successful_steps} out of {len(steps)} steps completed successfully.")
    
    print(f"\nProcessing finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
if __name__ == "__main__":
    main()