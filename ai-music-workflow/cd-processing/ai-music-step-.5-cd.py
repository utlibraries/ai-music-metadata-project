#!/usr/bin/env python3
"""
Check image filenames to ensure they follow pattern: {N}digits + letter + extension
Example: 059173017359115a.png, 059173017359115b.jpg, etc.
Automatically removes spaces from filenames before validation.
User will be prompted to fix any issues before proceeding.
"""

import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from cd_workflow_config import get_file_path_config

# Configuration - easily changeable
DIGITS_COUNT = 15  # Change this to 10, 12, etc. as needed
VALID_EXTENSIONS = {'.png', '.jpg', '.jpeg'}
LETTERS = 'abcdefghijklmnopqrstuvwxyz'

def remove_spaces_from_filenames(directory_path):
    """Remove spaces from all image filenames and return list of modified files."""
    directory = Path(directory_path)
    modified_files = []
    
    # Get all files (not just valid extensions, in case there are space issues)
    all_files = [f for f in directory.iterdir() if f.is_file()]
    
    for file_path in all_files:
        filename = file_path.name
        
        # Check if filename contains spaces
        if ' ' in filename:
            # Create new filename without spaces
            new_filename = filename.replace(' ', '')
            new_file_path = file_path.parent / new_filename
            
            try:
                # Rename the file
                file_path.rename(new_file_path)
                modified_files.append((filename, new_filename))
            except Exception as e:
                print(f"Error renaming {filename}: {e}")
    
    return modified_files

def is_valid_format(filename):
    """Check if filename matches the expected format: {DIGITS_COUNT}digits + letter + extension."""
    name_without_ext = os.path.splitext(filename)[0]
    extension = os.path.splitext(filename)[1].lower()
    
    # Check if extension is valid
    if extension not in VALID_EXTENSIONS:
        return False
    
    # Check if name matches pattern: exact number of digits + single letter
    pattern = f'^\\d{{{DIGITS_COUNT}}}[a-z]$'
    return bool(re.match(pattern, name_without_ext))

def create_validation_log(results_folder_path, valid_files, invalid_files):
    """Create a log file with validation results."""
    log_file_path = os.path.join(results_folder_path, "logs", "file_validation_log.txt")
    
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    with open(log_file_path, "w") as log_file:
        log_file.write(f"File Validation Log\n")
        log_file.write(f"Created at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write("="*60 + "\n\n")
        
        log_file.write(f"SUMMARY:\n")
        log_file.write(f"Valid files: {len(valid_files)}\n")
        log_file.write(f"Invalid files: {len(invalid_files)}\n")
        
        if valid_files:
            log_file.write(f"VALID FILES ({len(valid_files)}):\n")
            for filename in sorted(valid_files):
                log_file.write(f"  {filename}\n")
            log_file.write("\n")
        
        if invalid_files:
            log_file.write(f"INVALID FILES ({len(invalid_files)}):\n")
            for filename in sorted(invalid_files):
                log_file.write(f"  {filename}\n")
            log_file.write("\n")
    
    return log_file_path

def main(): 
    file_paths = get_file_path_config()
    directory_path = file_paths["images_folder"]
    
    print(f"Checking files in: {os.path.abspath(directory_path)}")
    
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"Error: Directory {directory_path} does not exist")
        return False
    
    # First, remove spaces from filenames
    modified_files = remove_spaces_from_filenames(directory_path)
    
    if modified_files:
        print(f"\nREMOVED SPACES FROM {len(modified_files)} FILE(S):")
        for old_name, new_name in modified_files:
            print(f"  '{old_name}' → '{new_name}'")
        print()
    
    # Get all image files (after space removal)
    image_files = []
    for ext in VALID_EXTENSIONS:
        image_files.extend(directory.glob(f'*{ext}'))
        image_files.extend(directory.glob(f'*{ext.upper()}'))
    
    if not image_files:
        print("No image files found in directory")
        return False
    
    print(f"Found {len(image_files)} image files")
    print(f"Expected format: {DIGITS_COUNT} digits + letter + extension (e.g., {'0' * DIGITS_COUNT}a.png)")
    print("=" * 70)
    
    # Separate valid and invalid files
    valid_files = []
    invalid_files = []
    
    for file_path in image_files:
        if is_valid_format(file_path.name):
            valid_files.append(file_path.name)
        else:
            invalid_files.append(file_path.name)
    
    # Show results
    print(f"\nVALID FILES ({len(valid_files)}):")
    if valid_files:
        valid_files.sort()
        for filename in valid_files:
            print(f"  {filename}")
    else:
        print("  None")
    
    print(f"\nINVALID FILES ({len(invalid_files)}):")
    has_issues = len(invalid_files) > 0
    if invalid_files:
        invalid_files.sort()
        for filename in invalid_files:
            print(f"  {filename}")
        
        print("\n" + "=" * 70)
        print("BEFORE STARTING THE WORKFLOW:")
        print(f"   Please normalize the {len(invalid_files)} invalid filename(s) above")
        print(f"   Expected pattern: [barcode]{LETTERS[0]}.png (or .jpg/.jpeg)")
        print(f"   Examples: {'059173017359115' if DIGITS_COUNT == 15 else '0' * DIGITS_COUNT}a.png, {'059173017359115' if DIGITS_COUNT == 15 else '0' * DIGITS_COUNT}b.jpg")
        print("   - Use exactly {} digits for the barcode".format(DIGITS_COUNT))
        print("   - Add letter suffix (a, b, c) for multiple files with same barcode")
        print("   - Use lowercase file extensions (.png, .jpg, .jpeg)")
        print("=" * 70)
    else:
        print("  None - All files are properly formatted!")
    
    return not has_issues

if __name__ == "__main__":
    success = main()
    # Exit with code 1 if there were issues, 0 if all files are valid
    sys.exit(0 if success else 1)