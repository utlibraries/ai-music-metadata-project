#!/usr/bin/env python3
"""
Enhanced barcode extraction with OCR text detection and automatic renaming.
Looks for "UNIVERSITY OF TEXAS AT AUSTIN - UNIV LIBS" text, extracts barcode,
reads the barcode number, and renames the file.

Install dependencies:
    pip install pytesseract --break-system-packages
    
    On macOS: brew install tesseract
    On Ubuntu: sudo apt-get install tesseract-ocr
"""

import cv2
import numpy as np
from pathlib import Path
import argparse
import re
import shutil

try:
    import pytesseract
    TESSERACT_AVAILABLE = True
    print(">>> pytesseract available")
except ImportError:
    TESSERACT_AVAILABLE = False
    print(">>> pytesseract not available - install with: pip install pytesseract --break-system-packages")

print(">>> extract_and_rename_barcodes.py file loaded")


def find_ut_library_text(image, debug=False):
    """
    Use OCR to find "UNIVERSITY OF TEXAS AT AUSTIN" text.
    Scales up image for better text detection.
    Returns bounding boxes of matching regions.
    """
    if not TESSERACT_AVAILABLE:
        return []
    
    scale = 2.0
    width = int(image.shape[1] * scale)
    height = int(image.shape[0] * scale)
    scaled_image = cv2.resize(image, (width, height), interpolation=cv2.INTER_CUBIC)
    
    gray = cv2.cvtColor(scaled_image, cv2.COLOR_BGR2GRAY)
    
    candidates = []
    
    preprocessed_images = [
        ('gray', gray),
        ('binary_otsu', cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]),
    ]
    
    psm_modes = [
        ('psm_11', '--oem 3 --psm 11'),
        ('psm_6', '--oem 3 --psm 6'),
        ('psm_3', '--oem 3 --psm 3'),
    ]
    
    for method_name, img_preprocessed in preprocessed_images:
        for psm_name, config in psm_modes:
            try:
                ocr_data = pytesseract.image_to_data(img_preprocessed, config=config,
                                                     output_type=pytesseract.Output.DICT)
                
                keywords = ['UNIVERSITY', 'TEXAS', 'AUSTIN', 'UNIV', 'LIBS']
                
                for i in range(len(ocr_data['text'])):
                    text = str(ocr_data['text'][i])
                    text_upper = text.upper().strip()
                    conf = int(ocr_data['conf'][i]) if str(ocr_data['conf'][i]) != '-1' else 0
                    
                    if any(keyword in text_upper for keyword in keywords) and conf > 20:
                        x, y, w, h = (ocr_data['left'][i], ocr_data['top'][i], 
                                     ocr_data['width'][i], ocr_data['height'][i])
                        
                        if w < 30 or h < 8:
                            continue
                        
                        x_orig = int(x / scale)
                        y_orig = int(y / scale)
                        w_orig = int(w / scale)
                        h_orig = int(h / scale)
                        
                        expand_down = h_orig * 5
                        expand_side = int(w_orig * 1.5)
                        expand_up = int(h_orig * 1.5)
                        
                        x1 = max(0, x_orig - expand_side)
                        y1 = max(0, y_orig - expand_up)
                        x2 = min(image.shape[1], x_orig + w_orig + expand_side)
                        y2 = min(image.shape[0], y_orig + expand_down)
                        
                        if y2 > y1 and x2 > x1:
                            region = image[y1:y2, x1:x2]
                            
                            if region.shape[0] > 60 and region.shape[1] > 120:
                                gray_region = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY)
                                white_ratio = np.sum(gray_region > 170) / gray_region.size
                                
                                if white_ratio > 0.20:
                                    candidates.append({
                                        'bbox': (x1, y1, x2-x1, y2-y1),
                                        'text': text,
                                        'confidence': conf,
                                        'score': 650 + conf,
                                        'method': f'ocr_{method_name}_{psm_name}'
                                    })
                                    
                                    if debug:
                                        print(f"      [{method_name}/{psm_name}] Found '{text}' (conf={conf})")
            
            except Exception as e:
                if debug and 'gray' in method_name and 'psm_11' in psm_name:
                    print(f"      OCR error: {e}")
    
    unique = []
    for candidate in candidates:
        x, y, w, h = candidate['bbox']
        is_duplicate = False
        
        for existing in unique:
            ex, ey, ew, eh = existing['bbox']
            x_overlap = max(0, min(x + w, ex + ew) - max(x, ex))
            y_overlap = max(0, min(y + h, ey + eh) - max(y, ey))
            intersection = x_overlap * y_overlap
            union = w * h + ew * eh - intersection
            iou = intersection / union if union > 0 else 0
            
            if iou > 0.5:
                if candidate['score'] > existing['score']:
                    unique.remove(existing)
                else:
                    is_duplicate = True
                break
        
        if not is_duplicate:
            unique.append(candidate)
    
    unique.sort(key=lambda c: c['score'], reverse=True)
    
    return unique


def detect_white_rectangular_regions(image):
    """
    Detect white rectangular regions.
    """
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    all_contours = []
    for thresh_val in [180, 200, 220]:
        _, thresh = cv2.threshold(gray, thresh_val, 255, cv2.THRESH_BINARY)
        
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (7, 7))
        thresh = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
        thresh = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
        
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        all_contours.extend(contours)
    
    return all_contours


def detect_line_patterns(image):
    """
    Detect vertical line patterns.
    """
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    gradX = cv2.Sobel(gray, ddepth=cv2.CV_32F, dx=1, dy=0, ksize=-1)
    gradY = cv2.Sobel(gray, ddepth=cv2.CV_32F, dx=0, dy=1, ksize=-1)
    
    gradient = cv2.subtract(gradX, gradY)
    gradient = cv2.convertScaleAbs(gradient)
    
    blurred = cv2.blur(gradient, (9, 9))
    _, thresh = cv2.threshold(blurred, 225, 255, cv2.THRESH_BINARY)
    
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (21, 7))
    closed = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
    closed = cv2.erode(closed, None, iterations=4)
    closed = cv2.dilate(closed, None, iterations=4)
    
    contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    return contours


def score_region(contour, image):
    """
    Score a region based on barcode label characteristics.
    """
    img_height, img_width = image.shape[:2]
    area = cv2.contourArea(contour)
    x, y, w, h = cv2.boundingRect(contour)
    
    if w < 80 or h < 40 or area < 3000:
        return None
    if w > img_width * 0.8 or h > img_height * 0.8:
        return None
    
    aspect_ratio = w / h if h > 0 else 0
    if aspect_ratio < 0.5 or aspect_ratio > 6.0:
        return None
    
    x1, y1 = max(0, x), max(0, y)
    x2, y2 = min(img_width, x + w), min(img_height, y + h)
    region = image[y1:y2, x1:x2]
    
    score = 0
    
    gray = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY)
    white_ratio = np.sum(gray > 180) / gray.size
    if white_ratio > 0.4:
        score += 80
    else:
        return None
    
    relative_area = area / (img_width * img_height)
    if 0.008 < relative_area < 0.10:
        score += 80
    elif 0.003 < relative_area < 0.15:
        score += 30
    elif relative_area > 0.20:
        score -= 80
    
    if 1.3 < aspect_ratio < 3.2:
        score += 80
    elif 1.0 < aspect_ratio < 4.0:
        score += 40
    
    if score < 100:
        return None
    
    return {
        'bbox': (x, y, w, h),
        'score': score,
        'method': 'cv'
    }


def detect_barcode_with_ocr(image_path, debug=False):
    """
    Multi-method detection: OCR + computer vision.
    Priority: OCR > CV
    """
    img = cv2.imread(str(image_path))
    if img is None:
        print(f"Error: Could not read image {image_path}")
        return None
    
    img_height, img_width = img.shape[:2]
    all_candidates = []
    
    if TESSERACT_AVAILABLE:
        print(f"  Using OCR to find 'UNIVERSITY OF TEXAS AT AUSTIN - UNIV LIBS'...")
        ocr_results = find_ut_library_text(img, debug)
        if ocr_results:
            all_candidates.extend(ocr_results)
            print(f"    Found {len(ocr_results)} match(es) via OCR")
    else:
        print(f"  OCR not available (install: pip install pytesseract --break-system-packages)")
    
    if not all_candidates:
        print(f"  Using computer vision methods...")
        
        contours = detect_white_rectangular_regions(img)
        for contour in contours:
            result = score_region(contour, img)
            if result:
                all_candidates.append(result)
        
        contours = detect_line_patterns(img)
        for contour in contours:
            result = score_region(contour, img)
            if result:
                all_candidates.append(result)
        
        print(f"    Found {len(all_candidates)} candidates")
    
    if not all_candidates:
        print(f"  No barcode detected")
        return None
    
    unique = []
    for candidate in sorted(all_candidates, key=lambda c: c['score'], reverse=True):
        x, y, w, h = candidate['bbox']
        
        is_duplicate = False
        for existing in unique:
            ex, ey, ew, eh = existing['bbox']
            
            x_overlap = max(0, min(x + w, ex + ew) - max(x, ex))
            y_overlap = max(0, min(y + h, ey + eh) - max(y, ey))
            intersection = x_overlap * y_overlap
            union = w * h + ew * eh - intersection
            iou = intersection / union if union > 0 else 0
            
            if iou > 0.5:
                is_duplicate = True
                break
        
        if not is_duplicate:
            unique.append(candidate)
    
    best = unique[0]
    print(f"  Best: {best['method']}, score={best['score']:.1f}")
    
    x, y, w, h = best['bbox']
    
    padding = 300
    x1 = max(0, x - padding)
    y1 = max(0, y - padding)
    x2 = min(img_width, x + w + padding)
    y2 = min(img_height, y + h + padding)
    
    barcode_crop = img[y1:y2, x1:x2]
    
    if debug:
        debug_img = img.copy()
        cv2.rectangle(debug_img, (x1, y1), (x2, y2), (0, 255, 0), 3)
        method_text = f"{best['method']}: {best['score']:.0f}"
        cv2.putText(debug_img, method_text, (x1, y1-10), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
        cv2.imshow('Detected', cv2.resize(debug_img, (800, 800)))
        cv2.imshow('Extracted', barcode_crop)
        cv2.waitKey(0)
        cv2.destroyAllWindows()
    
    return barcode_crop


def enlarge_barcode(barcode_img, scale_factor=3):
    """Enlarge barcode image."""
    if barcode_img is None:
        return None
    
    height, width = barcode_img.shape[:2]
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)
    
    enlarged = cv2.resize(barcode_img, (new_width, new_height), 
                         interpolation=cv2.INTER_CUBIC)
    return enlarged


def read_barcode_number(barcode_img, debug=False):
    """
    Read the barcode number from the extracted barcode image using OCR.
    
    Returns the 15-digit barcode number (with 05917 prefix) or None if not found.
    
    Logic:
    - Look for 15-digit number starting with 05917 (use as-is)
    - Look for 10-digit number (prepend 05917)
    - If both found, verify last 10 digits of 15-digit match the 10-digit
    - Handles numbers with spaces (e.g., "0 5917 3041374312")
    - Handles OCR misreads (e.g., "9" instead of "0" at start)
    """
    if not TESSERACT_AVAILABLE:
        print("    OCR not available - cannot read barcode number")
        return None
    
    if barcode_img is None:
        return None
    
    gray = cv2.cvtColor(barcode_img, cv2.COLOR_BGR2GRAY)
    
    sharpen_kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
    sharpened = cv2.filter2D(gray, -1, sharpen_kernel)
    
    preprocessed_images = [
        ('original', gray),
        ('sharpen', sharpened),
        ('binary', cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]),
        ('adaptive', cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                          cv2.THRESH_BINARY, 11, 2)),
    ]
    
    all_numbers_15 = []
    all_numbers_10 = []
    all_raw_text = []
    
    configs = [
        '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789 ',
        '--oem 3 --psm 11 -c tessedit_char_whitelist=0123456789 ',
        '--oem 3 --psm 4 -c tessedit_char_whitelist=0123456789 ',
        '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 11 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 6',
    ]
    
    for method_name, img_preprocessed in preprocessed_images:
        for config in configs:
            try:
                text = pytesseract.image_to_string(img_preprocessed, config=config)
                all_raw_text.append(text)
                
                cleaned = re.sub(r'\D', '', text)
                
                if debug:
                    print(f"      OCR [{method_name}]: '{text.strip()}' -> '{cleaned}'")
                
                matches_15 = re.findall(r'05917\d{10}', cleaned)
                all_numbers_15.extend(matches_15)
                
                misread_matches = re.findall(r'95917\d{10}', cleaned)
                for match in misread_matches:
                    corrected = '0' + match[1:]
                    all_numbers_15.append(corrected)
                    if debug:
                        print(f"      Corrected misread: {match} -> {corrected}")
                
                matches_10 = re.findall(r'(?<!\d)\d{10}(?!\d)', cleaned)
                all_numbers_10.extend(matches_10)
                
            except Exception as e:
                if debug:
                    print(f"      OCR error [{method_name}]: {e}")
    
    for text in all_raw_text:
        spaced_patterns = [
            r'0[\s\.\,]*5917[\s\.\,]*(\d[\s\d]{9,})',
            r'5917[\s\.\,]*(\d[\s\d]{9,})',
        ]
        
        for pattern in spaced_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                digits_only = re.sub(r'\D', '', '05917' + match)
                if len(digits_only) == 15 and digits_only.startswith('05917'):
                    all_numbers_15.append(digits_only)
                    if debug:
                        print(f"      Found spaced pattern: 05917{match} -> {digits_only}")
    
    numbers_15 = list(set(all_numbers_15))
    numbers_10 = list(set(all_numbers_10))
    
    if debug:
        print(f"    Found 15-digit numbers: {numbers_15}")
        print(f"    Found 10-digit numbers: {numbers_10}")
    
    if numbers_15 and numbers_10:
        for num_15 in numbers_15:
            for num_10 in numbers_10:
                if num_15[-10:] == num_10:
                    print(f"    Verified match: {num_15} (last 10 digits match {num_10})")
                    return num_15
        
        print(f"    Warning: 15-digit and 10-digit numbers don't match")
        print(f"    Using 15-digit number: {numbers_15[0]}")
        return numbers_15[0]
    
    elif numbers_15:
        print(f"    Found 15-digit number: {numbers_15[0]}")
        return numbers_15[0]
    
    elif numbers_10:
        barcode_number = '05917' + numbers_10[0]
        print(f"    Found 10-digit number: {numbers_10[0]}")
        print(f"    Prepended 05917: {barcode_number}")
        return barcode_number
    
    else:
        print(f"    No barcode number found")
        return None


def process_folder(input_folder, output_folder=None, scale_factor=3, debug=False):
    """Process folder of LP images."""
    input_path = Path(input_folder)
    
    if output_folder is None:
        output_path = input_path.parent / 'renamed_images'
    else:
        output_path = Path(output_folder)
    
    output_path.mkdir(parents=True, exist_ok=True)
    
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif'}
    image_files = sorted([
        f for f in input_path.iterdir() 
        if f.suffix.lower() in image_extensions
    ])
    
    if not image_files:
        print(f"No images found in {input_folder}")
        return
    
    print(f"\n{'='*60}")
    print(f"Barcode Extraction with OCR + Automatic Renaming")
    print(f"{'='*60}")
    print(f"Found {len(image_files)} images")
    print(f"Processing {len(image_files) // 2} pairs...")
    
    methods = []
    if TESSERACT_AVAILABLE:
        methods.append("OCR")
    methods.append("Computer Vision")
    print(f"Available methods: {', '.join(methods)}")
    
    if not TESSERACT_AVAILABLE:
        print("  Warning: Install pytesseract for barcode number reading: pip install pytesseract --break-system-packages")
    
    processed = 0
    failed = 0
    
    for idx, image_file in enumerate(image_files):
        if idx % 2 == 1:
            continue
        
        print(f"\n{'='*60}")
        print(f"[{idx//2 + 1}] {image_file.name}")
        
        barcode = detect_barcode_with_ocr(image_file, debug=debug)
        
        if barcode is None:
            print(f"  Failed to extract barcode")
            failed += 1
            continue
        
        enlarged_barcode = enlarge_barcode(barcode, scale_factor)
        
        print(f"  Reading barcode number...")
        barcode_number = read_barcode_number(enlarged_barcode, debug=debug)
        
        if not barcode_number:
            print(f"  Warning: Could not read barcode number, skipping pair")
            failed += 1
            continue
        
        front_file = image_file
        back_file = image_files[idx + 1] if idx + 1 < len(image_files) else None
        
        front_ext = front_file.suffix
        front_output = output_path / f"{barcode_number}a{front_ext}"
        
        shutil.copy2(front_file, front_output)
        print(f"  Renamed front: {front_output.name}")
        
        if back_file:
            back_ext = back_file.suffix
            back_output = output_path / f"{barcode_number}b{back_ext}"
            shutil.copy2(back_file, back_output)
            print(f"  Renamed back: {back_output.name}")
        
        processed += 1
    
    print(f"\n{'='*60}")
    print(f"COMPLETE!")
    print(f"{'='*60}")
    print(f"Successfully processed: {processed} pairs")
    print(f"Failed: {failed} pairs")
    print(f"Output folder: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract barcodes using OCR, read numbers, and rename files'
    )
    parser.add_argument('input_folder', help='Folder with LP images')
    parser.add_argument('-o', '--output', default=None, help='Output folder')
    parser.add_argument('-s', '--scale', type=float, default=3.0, help='Scale factor')
    parser.add_argument('-d', '--debug', action='store_true', help='Show visualizations')
    
    args = parser.parse_args()
    process_folder(args.input_folder, args.output, args.scale, args.debug)


if __name__ == '__main__':
    main()