#!/usr/bin/env python3
"""
Barcode extraction with OCR text detection and automatic renaming. - sometimes incorrect unfortunately

Detection strategy:
1. First, try to find "UNIVERSITY OF TEXAS" text using OCR
2. If not found, look for 10 or 15 digit barcode numbers using OCR
3. Use the located text to crop the barcode region

Features:
- Tiered OCR: fast methods first, exhaustive only if needed
- Early exit when confident match found
- Optional parallel processing for batches

Install dependencies:
    pip install pytesseract
"""

import cv2
import numpy as np
from pathlib import Path
import argparse
import re
import shutil
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

try:
    import pytesseract
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    print(">>> pytesseract not available - install with: pip install pytesseract --break-system-packages")


def find_ut_library_text(image, debug=False):
    """
    Use OCR to find "UNIVERSITY OF TEXAS AT AUSTIN" text.
    More robust: multiple preprocessing and PSM modes with early exit.
    """
    if not TESSERACT_AVAILABLE:
        return []
    
    scale = 2.0
    width = int(image.shape[1] * scale)
    height = int(image.shape[0] * scale)
    scaled_image = cv2.resize(image, (width, height), interpolation=cv2.INTER_CUBIC)
    
    gray = cv2.cvtColor(scaled_image, cv2.COLOR_BGR2GRAY)
    
    candidates = []
    
    # Multiple preprocessing options for robustness
    _, binary_otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    preprocessed_images = [
        ('gray', gray),
        ('binary_otsu', binary_otsu),
    ]
    
    # Multiple PSM modes - try most effective first
    psm_modes = [
        ('psm_11', '--oem 3 --psm 11'),  # Sparse text
        ('psm_6', '--oem 3 --psm 6'),    # Block of text
        ('psm_3', '--oem 3 --psm 3'),    # Auto
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
                                    
                                    # Early exit if we found a high-confidence match
                                    if conf >= 90:
                                        if debug:
                                            print(f"      Early exit: high-confidence match '{text}' (conf={conf})")
                                        # Return immediately with this candidate
                                        return [candidates[-1]]
            
            except Exception as e:
                if debug:
                    print(f"      OCR error: {e}")
    
    # Deduplicate
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
    """Detect white rectangular regions with multiple threshold levels."""
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    all_contours = []
    # More threshold values for robustness
    for thresh_val in [170, 190, 210, 230]:
        _, thresh = cv2.threshold(gray, thresh_val, 255, cv2.THRESH_BINARY)
        
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (7, 7))
        thresh = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
        thresh = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
        
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        all_contours.extend(contours)
    
    # Also try adaptive threshold for variable lighting
    adaptive = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                     cv2.THRESH_BINARY, 21, 5)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (7, 7))
    adaptive = cv2.morphologyEx(adaptive, cv2.MORPH_CLOSE, kernel)
    contours, _ = cv2.findContours(adaptive, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    all_contours.extend(contours)
    
    return all_contours


def check_region_has_barcode(region):
    """
    Check if a region likely contains a barcode by looking for vertical line patterns.
    Returns a confidence score 0-100.
    """
    if region is None or region.size == 0:
        return 0
    
    gray = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY) if len(region.shape) == 3 else region
    
    # Look for vertical edges (barcode lines)
    sobelx = cv2.Sobel(gray, cv2.CV_64F, 1, 0, ksize=3)
    sobely = cv2.Sobel(gray, cv2.CV_64F, 0, 1, ksize=3)
    
    # Barcodes have strong vertical edges, weak horizontal
    vert_energy = np.mean(np.abs(sobelx))
    horiz_energy = np.mean(np.abs(sobely))
    
    if horiz_energy > 0:
        edge_ratio = vert_energy / horiz_energy
    else:
        edge_ratio = vert_energy
    
    # Barcodes typically have edge_ratio > 1.5
    if edge_ratio > 2.0:
        return 100
    elif edge_ratio > 1.5:
        return 70
    elif edge_ratio > 1.2:
        return 40
    return 10



def score_region(contour, image):
    """Score a region based on barcode label characteristics."""
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
    
    # POSITION BONUS: Strongly prefer regions in the upper portion of the image
    # UT library barcodes are typically at the top
    vertical_position = y / img_height
    if vertical_position < 0.25:
        score += 200  # Strong bonus for top quarter
    elif vertical_position < 0.4:
        score += 100  # Good bonus for upper portion
    elif vertical_position > 0.7:
        score -= 200  # Strong penalty for bottom portion (call number labels)
    elif vertical_position > 0.5:
        score -= 100  # Penalty for lower half
    
    # BARCODE PATTERN BONUS: Check if region has vertical line patterns
    barcode_confidence = check_region_has_barcode(region)
    score += barcode_confidence
    
    # WIDTH BONUS: Barcode labels are typically wider
    if w > img_width * 0.3:
        score += 50
    
    if score < 100:
        return None
    
    return {
        'bbox': (x, y, w, h),
        'score': score,
        'method': 'cv'
    }


def find_barcode_number_text(image, debug=False):
    """
    Use OCR to find barcode numbers (10 or 15 digits) directly in the image.
    Fallback when University of Texas text is not found.
    """
    if not TESSERACT_AVAILABLE:
        return []

    scale = 2.0
    width = int(image.shape[1] * scale)
    height = int(image.shape[0] * scale)
    scaled_image = cv2.resize(image, (width, height), interpolation=cv2.INTER_CUBIC)

    gray = cv2.cvtColor(scaled_image, cv2.COLOR_BGR2GRAY)

    candidates = []

    # Multiple preprocessing options
    _, binary_otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    preprocessed_images = [
        ('gray', gray),
        ('binary_otsu', binary_otsu),
    ]

    # PSM modes for text detection
    psm_modes = [
        ('psm_11', '--oem 3 --psm 11'),  # Sparse text
        ('psm_6', '--oem 3 --psm 6'),    # Block of text
    ]

    for method_name, img_preprocessed in preprocessed_images:
        for psm_name, config in psm_modes:
            try:
                ocr_data = pytesseract.image_to_data(img_preprocessed, config=config,
                                                     output_type=pytesseract.Output.DICT)

                for i in range(len(ocr_data['text'])):
                    text = str(ocr_data['text'][i])
                    conf = int(ocr_data['conf'][i]) if str(ocr_data['conf'][i]) != '-1' else 0

                    # Look for 10 or 15 digit numbers
                    cleaned = re.sub(r'\D', '', text)
                    if len(cleaned) in [10, 15] and conf > 30:
                        x, y, w, h = (ocr_data['left'][i], ocr_data['top'][i],
                                     ocr_data['width'][i], ocr_data['height'][i])

                        if w < 20 or h < 8:
                            continue

                        x_orig = int(x / scale)
                        y_orig = int(y / scale)
                        w_orig = int(w / scale)
                        h_orig = int(h / scale)

                        # Expand region to capture full barcode label
                        expand_down = h_orig * 5
                        expand_side = int(w_orig * 1.5)
                        expand_up = int(h_orig * 1.5)

                        x1 = max(0, x_orig - expand_side)
                        y1 = max(0, y_orig - expand_up)
                        x2 = min(image.shape[1], x_orig + w_orig + expand_side)
                        y2 = min(image.shape[0], y_orig + expand_down)

                        if y2 > y1 and x2 > x1:
                            region = image[y1:y2, x1:x2]

                            if region.shape[0] > 40 and region.shape[1] > 80:
                                candidates.append({
                                    'bbox': (x1, y1, x2-x1, y2-y1),
                                    'text': cleaned,
                                    'confidence': conf,
                                    'score': 500 + conf,
                                    'method': f'ocr_barcode_{method_name}_{psm_name}'
                                })

                                if debug:
                                    print(f"      Found barcode number candidate: {cleaned} (conf={conf})")

                                # Early exit if high confidence
                                if conf >= 80 and len(cleaned) == 15:
                                    return [candidates[-1]]

            except Exception as e:
                if debug:
                    print(f"      Barcode OCR error: {e}")

    # Deduplicate
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


def detect_barcode_with_ocr(image_path, debug=False):
    """
    Multi-method detection: OCR for UT text, then OCR for barcode numbers.
    """
    img = cv2.imread(str(image_path))
    if img is None:
        return None

    img_height, img_width = img.shape[:2]
    all_candidates = []

    if TESSERACT_AVAILABLE:
        # First try: Look for University of Texas text
        ocr_results = find_ut_library_text(img, debug)
        if ocr_results:
            if debug:
                print(f"    Found UT library text")
            all_candidates.extend(ocr_results)
        else:
            # Fallback: Look for barcode numbers directly
            if debug:
                print(f"    UT text not found, looking for barcode numbers...")
            barcode_results = find_barcode_number_text(img, debug)
            if barcode_results:
                if debug:
                    print(f"    Found {len(barcode_results)} barcode number candidates")
                all_candidates.extend(barcode_results)

    if not all_candidates:
        return None
    
    # Deduplicate
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
    
    x, y, w, h = best['bbox']
    padding = 200
    x1 = max(0, x - padding)
    y1 = max(0, y - padding)
    x2 = min(img_width, x + w + padding)
    y2 = min(img_height, y + h + padding)
    
    barcode_crop = img[y1:y2, x1:x2]
    return barcode_crop


def enhance_barcode_image(barcode_img, scale_factor=2):
    """
    OPTIMIZED: Reduced to essential enhancements only.
    """
    if barcode_img is None:
        return None
    
    height, width = barcode_img.shape[:2]
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)
    
    # Basic enlargement
    enlarged = cv2.resize(barcode_img, (new_width, new_height), interpolation=cv2.INTER_LANCZOS4)
    
    # Denoised
    denoised = cv2.fastNlMeansDenoisingColored(barcode_img, None, 6, 6, 7, 21)
    enlarged_denoised = cv2.resize(denoised, (new_width, new_height), interpolation=cv2.INTER_LANCZOS4)
    
    gray = cv2.cvtColor(enlarged_denoised, cv2.COLOR_BGR2GRAY)
    
    # Sharpened
    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]])
    sharpened = cv2.filter2D(gray, -1, kernel)
    
    # Binary
    _, binary = cv2.threshold(sharpened, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    return {
        'gray': gray,
        'sharpened': sharpened,
        'binary': binary,
        'for_display': cv2.cvtColor(sharpened, cv2.COLOR_GRAY2BGR),
    }


def extract_barcode_number(text):
    """Extract and validate barcode number from OCR text."""
    cleaned = re.sub(r'\D', '', text)
    
    # Look for 15-digit patterns starting with 05917
    matches = re.findall(r'05917\d{10}', cleaned)
    if matches:
        return matches
    
    # Try common misreads
    misread_fixes = [
        (r'95917\d{10}', lambda m: '0' + m[1:]),
        (r'09517\d{10}', lambda m: '05917' + m[5:]),
    ]
    
    results = []
    for pattern, fixer in misread_fixes:
        for match in re.findall(pattern, cleaned):
            fixed = fixer(match)
            if len(fixed) == 15:
                results.append(fixed)
    
    return results


def read_barcode_fast(enhanced_dict, debug=False):
    """
    TIER 1: Fast OCR - try minimal configs first.
    Returns barcode number or None.
    """
    if not TESSERACT_AVAILABLE or enhanced_dict is None:
        return None
    
    # Most effective configs only
    fast_configs = [
        '--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789',
    ]
    
    # Most effective preprocessed images
    images_to_try = [
        ('sharpened', enhanced_dict['sharpened']),
        ('binary', enhanced_dict['binary']),
    ]
    
    all_numbers = []
    
    for img_name, img in images_to_try:
        for config in fast_configs:
            try:
                text = pytesseract.image_to_string(img, config=config)
                numbers = extract_barcode_number(text)
                all_numbers.extend(numbers)
                
                if debug and numbers:
                    print(f"      FAST [{img_name}]: found {numbers}")
                
                # Early exit: if we found 2+ matching numbers, we're confident
                counter = Counter(all_numbers)
                if counter and counter.most_common(1)[0][1] >= 2:
                    result = counter.most_common(1)[0][0]
                    if debug:
                        print(f"      FAST: Confident match {result}")
                    return result
                    
            except Exception as e:
                if debug:
                    print(f"      FAST error [{img_name}]: {e}")
    
    # Return most common if any found
    if all_numbers:
        counter = Counter(all_numbers)
        return counter.most_common(1)[0][0]
    
    return None


def read_barcode_medium(enhanced_dict, debug=False):
    """
    TIER 2: Medium OCR - more configs and preprocessing.
    """
    if not TESSERACT_AVAILABLE or enhanced_dict is None:
        return None
    
    configs = [
        '--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 13 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 7',  # Without whitelist
    ]
    
    gray = enhanced_dict['gray']
    sharpened = enhanced_dict['sharpened']
    binary = enhanced_dict['binary']
    
    # Additional preprocessing
    _, binary_150 = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
    adaptive = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                     cv2.THRESH_BINARY, 11, 2)
    inverted = cv2.bitwise_not(binary)
    
    images_to_try = [
        ('sharpened', sharpened),
        ('binary', binary),
        ('binary_150', binary_150),
        ('adaptive', adaptive),
        ('inverted', inverted),
        ('gray', gray),
    ]
    
    all_numbers = []
    
    for img_name, img in images_to_try:
        for config in configs:
            try:
                text = pytesseract.image_to_string(img, config=config)
                numbers = extract_barcode_number(text)
                all_numbers.extend(numbers)
                
                # Early exit with confidence
                counter = Counter(all_numbers)
                if counter and counter.most_common(1)[0][1] >= 3:
                    result = counter.most_common(1)[0][0]
                    if debug:
                        print(f"      MEDIUM: Confident match {result}")
                    return result
                    
            except Exception:
                pass
    
    if all_numbers:
        counter = Counter(all_numbers)
        return counter.most_common(1)[0][0]
    
    return None


def read_barcode_exhaustive(enhanced_dict, debug=False):
    """
    TIER 3: Exhaustive OCR - last resort, slower.
    """
    if not TESSERACT_AVAILABLE or enhanced_dict is None:
        return None
    
    configs = [
        '--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 8 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 13 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 11 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 4 -c tessedit_char_whitelist=0123456789',
        '--oem 3 --psm 7',
        '--oem 3 --psm 6',
    ]
    
    gray = enhanced_dict['gray']
    sharpened = enhanced_dict['sharpened']
    binary = enhanced_dict['binary']
    
    # Many preprocessing variations
    _, binary_150 = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
    _, binary_180 = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY)
    adaptive = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                     cv2.THRESH_BINARY, 11, 2)
    adaptive_mean = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, 
                                          cv2.THRESH_BINARY, 11, 2)
    inverted = cv2.bitwise_not(binary)
    inverted_adaptive = cv2.bitwise_not(adaptive)
    
    kernel = np.ones((2, 2), np.uint8)
    dilated = cv2.dilate(binary, kernel, iterations=1)
    eroded = cv2.erode(binary, kernel, iterations=1)
    
    high_contrast = cv2.convertScaleAbs(gray, alpha=1.5, beta=0)
    
    images_to_try = [
        ('sharpened', sharpened),
        ('binary', binary),
        ('binary_150', binary_150),
        ('binary_180', binary_180),
        ('adaptive', adaptive),
        ('adaptive_mean', adaptive_mean),
        ('inverted', inverted),
        ('inverted_adaptive', inverted_adaptive),
        ('dilated', dilated),
        ('eroded', eroded),
        ('high_contrast', high_contrast),
        ('gray', gray),
    ]
    
    all_numbers_15 = []
    all_numbers_10 = []
    
    for img_name, img in images_to_try:
        for config in configs:
            try:
                text = pytesseract.image_to_string(img, config=config)
                cleaned = re.sub(r'\D', '', text)
                
                # 15-digit
                matches_15 = re.findall(r'05917\d{10}', cleaned)
                all_numbers_15.extend(matches_15)
                
                # 10-digit fallback
                matches_10 = re.findall(r'(?<!\d)\d{10}(?!\d)', cleaned)
                all_numbers_10.extend(matches_10)
                
                # Early exit
                counter = Counter(all_numbers_15)
                if counter and counter.most_common(1)[0][1] >= 3:
                    return counter.most_common(1)[0][0]
                    
            except Exception:
                pass
    
    if all_numbers_15:
        counter = Counter(all_numbers_15)
        return counter.most_common(1)[0][0]
    
    if all_numbers_10:
        counter = Counter(all_numbers_10)
        return '05917' + counter.most_common(1)[0][0]
    
    return None


def read_barcode_number_tiered(enhanced_dict, debug=False):
    """
    OPTIMIZED: Tiered approach - fast first, then medium, then exhaustive.
    """
    if enhanced_dict is None:
        return None
    
    # Tier 1: Fast (2-4 OCR calls)
    if debug:
        print(f"    Tier 1: Fast OCR...")
    result = read_barcode_fast(enhanced_dict, debug)
    if result:
        if debug:
            print(f"    ✓ Found in Tier 1: {result}")
        return result
    
    # Tier 2: Medium (~30 OCR calls)
    if debug:
        print(f"    Tier 2: Medium OCR...")
    result = read_barcode_medium(enhanced_dict, debug)
    if result:
        if debug:
            print(f"    ✓ Found in Tier 2: {result}")
        return result
    
    # Tier 3: Exhaustive (~96 OCR calls)
    if debug:
        print(f"    Tier 3: Exhaustive OCR...")
    result = read_barcode_exhaustive(enhanced_dict, debug)
    if result:
        if debug:
            print(f"    ✓ Found in Tier 3: {result}")
        return result
    
    return None


def process_single_image(args):
    """Process a single image pair. Used for parallel processing."""
    image_file, next_file, input_path, barcode_crops_path, failed_path, scale_factor, debug = args
    
    try:
        barcode = detect_barcode_with_ocr(image_file, debug=debug)
        
        if barcode is None:
            return ('failed', image_file, next_file, None, "Could not detect barcode region")
        
        enhanced_dict = enhance_barcode_image(barcode, scale_factor)
        
        if enhanced_dict is None:
            return ('failed', image_file, next_file, None, "Could not enhance barcode")
        
        barcode_number = read_barcode_number_tiered(enhanced_dict, debug=debug)
        
        if not barcode_number:
            return ('failed', image_file, next_file, None, "Could not read barcode number")
        
        return ('success', image_file, next_file, barcode_number, enhanced_dict['for_display'])
        
    except Exception as e:
        return ('failed', image_file, next_file, None, str(e))


def process_folder(input_folder, output_folder=None, scale_factor=2, debug=False, parallel=False):
    """
    Process folder of LP images.
    OPTIMIZED: Optional parallel processing.
    """
    input_path = Path(input_folder)
    
    barcode_crops_path = input_path / 'extracted_barcodes'
    barcode_crops_path.mkdir(parents=True, exist_ok=True)
    
    failed_path = input_path / 'unprocessed'
    failed_path.mkdir(parents=True, exist_ok=True)
    
    image_extensions = {'.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.tif'}
    image_files = sorted([
        f for f in input_path.iterdir() 
        if f.suffix.lower() in image_extensions and f.parent == input_path
    ])
    
    if not image_files:
        print(f"No images found in {input_folder}")
        return
    
    print(f"\n{'='*60}")
    print(f"OPTIMIZED Barcode Extraction")
    print(f"{'='*60}")
    print(f"Found {len(image_files)} images ({len(image_files) // 2} pairs)")
    
    if not TESSERACT_AVAILABLE:
        print("ERROR: pytesseract is required but not available")
        return
    
    processed = 0
    failed = 0
    
    # Build list of image pairs
    pairs = []
    for idx in range(0, len(image_files), 2):
        front_file = image_files[idx]
        back_file = image_files[idx + 1] if idx + 1 < len(image_files) else None
        pairs.append((front_file, back_file))
    
    if parallel and len(pairs) > 1:
        print(f"Using parallel processing ({multiprocessing.cpu_count()} workers)")
        
        # Prepare args for parallel processing
        args_list = [
            (front, back, input_path, barcode_crops_path, failed_path, scale_factor, debug)
            for front, back in pairs
        ]
        
        with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = {executor.submit(process_single_image, args): args for args in args_list}
            
            for future in as_completed(futures):
                result = future.result()
                status, front_file, back_file, barcode_number, extra = result
                
                print(f"\n[{processed + failed + 1}/{len(pairs)}] {front_file.name}")
                
                if status == 'failed':
                    print(f"  ✗ {extra}")
                    shutil.move(str(front_file), str(failed_path / front_file.name))
                    if back_file:
                        shutil.move(str(back_file), str(failed_path / back_file.name))
                    failed += 1
                else:
                    print(f"  ✓ {barcode_number}")
                    
                    # Save crop
                    crop_filename = barcode_crops_path / f"{barcode_number}.png"
                    cv2.imwrite(str(crop_filename), extra)
                    
                    # Rename files
                    front_ext = front_file.suffix
                    front_new = input_path / f"{barcode_number}a{front_ext}"
                    front_file.rename(front_new)
                    
                    if back_file:
                        back_ext = back_file.suffix
                        back_new = input_path / f"{barcode_number}b{back_ext}"
                        back_file.rename(back_new)
                    
                    processed += 1
    else:
        # Sequential processing
        for idx, (front_file, back_file) in enumerate(pairs):
            print(f"\n[{idx + 1}/{len(pairs)}] {front_file.name}")
            
            barcode = detect_barcode_with_ocr(front_file, debug=debug)
            
            if barcode is None:
                print(f"  ✗ Could not detect barcode region")
                shutil.move(str(front_file), str(failed_path / front_file.name))
                if back_file:
                    shutil.move(str(back_file), str(failed_path / back_file.name))
                failed += 1
                continue
            
            enhanced_dict = enhance_barcode_image(barcode, scale_factor)
            
            if enhanced_dict is None:
                print(f"  ✗ Could not enhance barcode")
                shutil.move(str(front_file), str(failed_path / front_file.name))
                if back_file:
                    shutil.move(str(back_file), str(failed_path / back_file.name))
                failed += 1
                continue
            
            barcode_number = read_barcode_number_tiered(enhanced_dict, debug=debug)
            
            if not barcode_number:
                print(f"  ✗ Could not read barcode number")
                shutil.move(str(front_file), str(failed_path / front_file.name))
                if back_file:
                    shutil.move(str(back_file), str(failed_path / back_file.name))
                failed += 1
                continue
            
            print(f"  ✓ {barcode_number}")
            
            # Save crop
            crop_filename = barcode_crops_path / f"{barcode_number}.png"
            cv2.imwrite(str(crop_filename), enhanced_dict['for_display'])
            
            # Rename files
            front_ext = front_file.suffix
            front_new = input_path / f"{barcode_number}a{front_ext}"
            front_file.rename(front_new)
            
            if back_file:
                back_ext = back_file.suffix
                back_new = input_path / f"{barcode_number}b{back_ext}"
                back_file.rename(back_new)
            
            processed += 1
    
    print(f"\n{'='*60}")
    print(f"COMPLETE: {processed} succeeded, {failed} failed")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description='OPTIMIZED barcode extraction with tiered OCR'
    )
    parser.add_argument('input_folder', help='Folder with LP images')
    parser.add_argument('-s', '--scale', type=float, default=2.0, help='Scale factor (default: 2.0)')
    parser.add_argument('-d', '--debug', action='store_true', help='Show debug info')
    parser.add_argument('-p', '--parallel', action='store_true', help='Use parallel processing')
    
    args = parser.parse_args()
    
    process_folder(args.input_folder, None, args.scale, args.debug, args.parallel)


if __name__ == '__main__':
    main()