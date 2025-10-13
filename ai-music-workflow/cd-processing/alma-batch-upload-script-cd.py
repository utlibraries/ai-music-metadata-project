"""
OCLC to Alma Import (CD workflow)

INPUT (text file):
  Format: oclcNumber|barcode|title
  - title is optional
  - default delimiter is "|"; override with --delimiter ','

WHAT IT DOES:
  • Fetches a WorldCat record (Discovery v2), builds MARCXML, and imports a new bib to Alma
  • Creates/uses a holding at your configured library/location
  • Creates one item per line (material type forced to "CD")
  • Skips titles that already exist in your Alma instance (by OCLC number)
  • Writes a CSV of created IDs (MMS, holding, item) next to the input file

DEFAULTS / SAFETY:
  • Runs against **Alma SANDBOX** by default (uses ALMA_SANDBOX_API_KEY)
  • This can be used for production.  If you have permissions and decide to switch to PRODUCTION, set ALMA_API_KEY instead of the Sandbox key and **consider whether or not to comment out** the unsuppress step in `process_file()`

REQUIRED ENVIRONMENT VARIABLES:
  ALMA_SANDBOX_API_KEY  
  OCLC_CLIENT_ID
  OCLC_SECRET
  ALMA_LIBRARY_CODE
  ALMA_LOCATION_CODE
  ALMA_CD_ITEM_POLICY
  ALMA_CATALOGING_INSTITUTION
  
OPTIONAL ENVIRONMENT VARIABLES:
  ALMA_REGION=api-na (default) 
  INTERNAL_NOTE_2="AI-assisted cataloging"

USAGE:
  python path/to/alma-batch-upload-script-cd.py --auto
    (this will find the latest batch-upload-alma-cd-<timestamp>.txt file from workflow output)
  python path/to/alma-batch-upload-script-cd.py path/to/other/input/file.txt
  python path/to/alma-batch-upload-script-cd.py --auto --delimiter ','
"""

import os
import requests
import xml.etree.ElementTree as ET
import csv
import time
from datetime import datetime

from shared_utilities import find_latest_results_folder
from cd_workflow_config import get_file_path_config

# ====== CONFIGURATION ======
# Load from environment variables with validation
def get_required_env(var_name):
    """Get required environment variable or raise clear error."""
    value = os.environ.get(var_name)
    if not value:
        raise SystemExit(f"Error: {var_name} environment variable is required but not set")
    return value

# Load required external system environment variables
alma_api_key = get_required_env("ALMA_SANDBOX_API_KEY") 
client_id = get_required_env("OCLC_CLIENT_ID")
client_secret = get_required_env("OCLC_SECRET")

# Load required institutional/workflow values
LIBRARY_CODE = get_required_env("ALMA_LIBRARY_CODE")         
LOCATION_CODE = get_required_env("ALMA_LOCATION_CODE")
ITEM_POLICY_CODE = get_required_env("ALMA_CD_ITEM_POLICY")
CATALOGING_INSTITUTION = get_required_env("ALMA_CATALOGING_INSTITUTION")

ALMA_REGION = os.environ.get("ALMA_REGION", "api-na") # Default to North America
INTERNAL_NOTE_2 = os.environ.get("ALMA_INTERNAL_NOTE_2", "AI-assisted cataloging")

def validate_input_file(file_path):
    """Validate input file exists and is readable."""
    if not os.path.isfile(file_path):
        raise SystemExit(f"Error: Input file not found: {file_path}")
    if not os.access(file_path, os.R_OK):
        raise SystemExit(f"Error: Input file not readable: {file_path}")
    return file_path

def find_latest_batch_upload_file(base_path=None):
    """Find the most recent batch upload file from workflow output."""
    # Get the configured paths
    file_paths = get_file_path_config()
    
    # Find the latest results folder using the shared utility
    results_folder = find_latest_results_folder(file_paths["results_prefix"])
    
    if not results_folder:
        print(f"No results folder found matching pattern: {file_paths['results_prefix']}*")
        return None
    
    print(f"Found results folder: {results_folder}")
    
    # Look in deliverables subfolder
    deliverables_path = os.path.join(results_folder, 'deliverables')
    
    if not os.path.exists(deliverables_path):
        print(f"Deliverables folder not found at: {deliverables_path}")
        return None
    
    print(f"Checking deliverables folder: {deliverables_path}")
    
    # Find batch upload files
    batch_files = [f for f in os.listdir(deliverables_path) 
                   if f.startswith('batch-upload-alma-') and f.endswith('.txt')]
    
    if not batch_files:
        print(f"No batch-upload-alma-*.txt files found in: {deliverables_path}")
        print(f"Available files: {os.listdir(deliverables_path)}")
        return None
    
    # Return most recent batch file (sorted by name, which includes timestamp)
    latest_batch = max(batch_files)
    full_path = os.path.join(deliverables_path, latest_batch)
    return full_path

# API base URL
ALMA_BASE = f"https://{ALMA_REGION}.hosted.exlibrisgroup.com/almaws/v1"

# Headers for Alma API
HEADERS_XML = {
    "Authorization": f"apikey {alma_api_key}",
    "Accept": "application/xml",
    "Content-Type": "application/xml"
}

# ====== OCLC FUNCTIONS ======

def get_access_token(client_id, client_secret):
    """Get access token using wcapi scope"""
    token_url = "https://oauth.oclc.org/token"
    data = {
        "grant_type": "client_credentials",
        "scope": "wcapi"  
    }
    response = requests.post(token_url, data=data, auth=(client_id, client_secret))
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Failed to get access token: {response.text}")


def get_marcxml_from_oclc(oclc_number, access_token):
    """
    Fetch MARCXML from OCLC using Search API v2
    """
    oclc_num = oclc_number.replace("(OCoLC)", "").strip()
    
    # Use Search API v2 endpoint
    url = "https://americas.discovery.api.oclc.org/worldcat/search/v2/bibs"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"  # Search API returns JSON
    }
    
    # Search for the specific OCLC number
    params = {
        "q": f"no:{oclc_num}",
        "limit": 1,
        "itemType": "music",
        "itemSubType": "music-cd"
    }
    
    print(f"         Searching OCLC Search API v2 for #{oclc_num}...")
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    data = r.json()
    record = data['bibRecords'][0]
    # Build a richer MARCXML straight from the Discovery bibRecord we already have
    print("         → Building MARCXML from Discovery record...")
    marcxml = build_marcxml_from_discovery_record(record)
    
    # Return MARCXML
    return marcxml, record

def convert_oclc_json_to_marcxml(oclc_json_data):
    """
    Convert OCLC JSON response to MARCXML format for Alma import
    """
    # Check if this has detailed MARC fields
    if 'briefRecords' in oclc_json_data and len(oclc_json_data['briefRecords']) > 0:
        record = oclc_json_data['briefRecords'][0]
    elif 'bibRecords' in oclc_json_data and len(oclc_json_data['bibRecords']) > 0:
        record = oclc_json_data['bibRecords'][0]
    else:
        raise ValueError("Could not find record data in OCLC response")
    
    # Build MARCXML structure
    marc_ns = "http://www.loc.gov/MARC21/slim"
    root = ET.Element('{%s}record' % marc_ns)
    root.set('xmlns', marc_ns)
    
    # Leader
    leader = ET.SubElement(root, 'leader')
    leader.text = '00000njm a2200000Ii 4500'
    
    # 001 - Control Number (OCLC number)
    if 'identifier' in record and 'oclcNumber' in record['identifier']:
        field001 = ET.SubElement(root, 'controlfield')
        field001.set('tag', '001')
        field001.text = record['identifier']['oclcNumber']
    
    # 003 - Control Number Identifier
    field003 = ET.SubElement(root, 'controlfield')
    field003.set('tag', '003')
    field003.text = 'OCoLC'
    
    # 007 - Physical Description Fixed Field (for CD)
    field007 = ET.SubElement(root, 'controlfield')
    field007.set('tag', '007')
    field007.text = 'sd fsngnnmmned'  # Standard for audio CD
    
    # 008 - Fixed-Length Data Elements
    field008 = ET.SubElement(root, 'controlfield')
    field008.set('tag', '008')
    pub_date = record.get('date', {}).get('publicationDate', '    ')[:4]
    field008.text = f"      s{pub_date}    xxu|||  ||||||  ||  eng d"
    
    # 020 - ISBN (if present)
    if 'identifier' in record and 'otherStandardIdentifiers' in record['identifier']:
        for ident in record['identifier']['otherStandardIdentifiers']:
            if isinstance(ident, dict) and ident.get('type') == 'ISBN':
                field020 = ET.SubElement(root, 'datafield')
                field020.set('tag', '020')
                field020.set('ind1', ' ')
                field020.set('ind2', ' ')
                subfield_a = ET.SubElement(field020, 'subfield')
                subfield_a.set('code', 'a')
                subfield_a.text = ident.get('id', '')
    
    # 024 - UPC
    if 'identifier' in record and 'otherStandardIdentifiers' in record['identifier']:
        for ident in record['identifier']['otherStandardIdentifiers']:
            if isinstance(ident, dict) and 'UPC' in ident.get('type', ''):
                field024 = ET.SubElement(root, 'datafield')
                field024.set('tag', '024')
                field024.set('ind1', '1')
                field024.set('ind2', ' ')
                subfield_a = ET.SubElement(field024, 'subfield')
                subfield_a.set('code', 'a')
                subfield_a.text = ident.get('id', '')
    
    # 035 - System Control Number (OCLC)
    if 'identifier' in record and 'oclcNumber' in record['identifier']:
        field035 = ET.SubElement(root, 'datafield')
        field035.set('tag', '035')
        field035.set('ind1', ' ')
        field035.set('ind2', ' ')
        subfield_a = ET.SubElement(field035, 'subfield')
        subfield_a.set('code', 'a')
        subfield_a.text = f"(OCoLC){record['identifier']['oclcNumber']}"
    
    # 100/110 - Main Entry (creator)
    if 'contributor' in record and 'creators' in record['contributor']:
        creators = record['contributor']['creators']
        if creators:
            creator = creators[0]
            if 'nonPersonName' in creator:
                # Corporate name
                field110 = ET.SubElement(root, 'datafield')
                field110.set('tag', '110')
                field110.set('ind1', '2')
                field110.set('ind2', ' ')
                subfield_a = ET.SubElement(field110, 'subfield')
                subfield_a.set('code', 'a')
                subfield_a.text = creator['nonPersonName'].get('text', '')
            elif 'firstName' in creator or 'secondName' in creator:
                # Personal name
                field100 = ET.SubElement(root, 'datafield')
                field100.set('tag', '100')
                field100.set('ind1', '1')
                field100.set('ind2', ' ')
                subfield_a = ET.SubElement(field100, 'subfield')
                subfield_a.set('code', 'a')
                first = creator.get('firstName', {}).get('text', '')
                second = creator.get('secondName', {}).get('text', '')
                subfield_a.text = f"{second}, {first}".strip(', ')
    
    # 245 - Title
    if 'title' in record:
        field245 = ET.SubElement(root, 'datafield')
        field245.set('tag', '245')
        field245.set('ind1', '0' if not ('contributor' in record and 'creators' in record['contributor']) else '1')
        field245.set('ind2', '0')
        
        if 'mainTitles' in record['title'] and record['title']['mainTitles']:
            subfield_a = ET.SubElement(field245, 'subfield')
            subfield_a.set('code', 'a')
            subfield_a.text = record['title']['mainTitles'][0].get('text', '')
        
        if 'subtitles' in record['title'] and record['title']['subtitles']:
            subfield_b = ET.SubElement(field245, 'subfield')
            subfield_b.set('code', 'b')
            subfield_b.text = record['title']['subtitles'][0].get('text', '')
    
    # 264 - Publication
    if 'publishers' in record and record['publishers']:
        pub = record['publishers'][0]
        field264 = ET.SubElement(root, 'datafield')
        field264.set('tag', '264')
        field264.set('ind1', ' ')
        field264.set('ind2', '1')
        
        if 'publicationPlace' in pub:
            subfield_a = ET.SubElement(field264, 'subfield')
            subfield_a.set('code', 'a')
            subfield_a.text = pub['publicationPlace']
        
        if 'publisherName' in pub:
            subfield_b = ET.SubElement(field264, 'subfield')
            subfield_b.set('code', 'b')
            subfield_b.text = pub['publisherName'].get('text', '')
        
        if 'publicationDate' in record.get('date', {}):
            subfield_c = ET.SubElement(field264, 'subfield')
            subfield_c.set('code', 'c')
            subfield_c.text = record['date']['publicationDate']
    
    # 300 - Physical Description
    if 'description' in record and 'physicalDescription' in record['description']:
        field300 = ET.SubElement(root, 'datafield')
        field300.set('tag', '300')
        field300.set('ind1', ' ')
        field300.set('ind2', ' ')
        subfield_a = ET.SubElement(field300, 'subfield')
        subfield_a.set('code', 'a')
        subfield_a.text = record['description']['physicalDescription']
    
    # 505 - Contents
    if 'description' in record and 'contents' in record['description']:
        for content in record['description']['contents']:
            if 'contentNote' in content:
                field505 = ET.SubElement(root, 'datafield')
                field505.set('tag', '505')
                field505.set('ind1', '0')
                field505.set('ind2', '0')
                subfield_a = ET.SubElement(field505, 'subfield')
                subfield_a.set('code', 'a')
                subfield_a.text = content['contentNote'].get('text', '')
    
    # Convert to string
    xml_string = ET.tostring(root, encoding='unicode')
    return xml_string


def convert_brief_record_to_marcxml(brief_record):
    """
    Fallback: Convert a brief OCLC record to minimal but VALID MARCXML for Alma
    """
    marc_ns = "http://www.loc.gov/MARC21/slim"
    root = ET.Element('{%s}record' % marc_ns)
    root.set('xmlns', marc_ns)
    
    # Leader - proper format for sound recording
    leader = ET.SubElement(root, 'leader')
    leader.text = '00000njm a2200000Ii 4500'
    
    # 001 - OCLC number
    if 'identifier' in brief_record and 'oclcNumber' in brief_record['identifier']:
        field001 = ET.SubElement(root, 'controlfield')
        field001.set('tag', '001')
        field001.text = brief_record['identifier']['oclcNumber']
    
    # 003 - Control number identifier (REQUIRED)
    field003 = ET.SubElement(root, 'controlfield')
    field003.set('tag', '003')
    field003.text = 'OCoLC'
    
    # 005 - Date and time of latest transaction (REQUIRED)
    field005 = ET.SubElement(root, 'controlfield')
    field005.set('tag', '005')
    field005.text = datetime.now().strftime('%Y%m%d%H%M%S.0')
    
    # 007 - Physical description for CD (REQUIRED for sound recordings)
    field007 = ET.SubElement(root, 'controlfield')
    field007.set('tag', '007')
    field007.text = 'sd fsngnnmmned'
    
    # 008 - Fixed-length data elements (REQUIRED)
    field008 = ET.SubElement(root, 'controlfield')
    field008.set('tag', '008')
    # Get publication date if available
    pub_date = '    '
    if 'date' in brief_record and 'publicationDate' in brief_record['date']:
        pub_date = brief_record['date']['publicationDate'][:4]
    current_date = datetime.now().strftime('%y%m%d')
    field008.text = f"{current_date}s{pub_date}    xxu           |  eng d"
    
    # 035 - System Control Number (RECOMMENDED)
    if 'identifier' in brief_record and 'oclcNumber' in brief_record['identifier']:
        field035 = ET.SubElement(root, 'datafield')
        field035.set('tag', '035')
        field035.set('ind1', ' ')
        field035.set('ind2', ' ')
        subfield_a = ET.SubElement(field035, 'subfield')
        subfield_a.set('code', 'a')
        subfield_a.text = f"(OCoLC){brief_record['identifier']['oclcNumber']}"
    
    # 040 - Cataloging source (REQUIRED)
    field040 = ET.SubElement(root, 'datafield')
    field040.set('tag', '040')
    field040.set('ind1', ' ')
    field040.set('ind2', ' ')
    subfield_a = ET.SubElement(field040, 'subfield')
    subfield_a.set('code', 'a')
    subfield_a.text = 'OCLC'
    subfield_c = ET.SubElement(field040, 'subfield')
    subfield_c.set('code', 'c')
    subfield_c.text = CATALOGING_INSTITUTION 
    
    # 245 - Title (REQUIRED)
    field245 = ET.SubElement(root, 'datafield')
    field245.set('tag', '245')
    field245.set('ind1', '0')
    field245.set('ind2', '0')
    subfield_a = ET.SubElement(field245, 'subfield')
    subfield_a.set('code', 'a')
    
    if isinstance(brief_record.get('title'), str):
        subfield_a.text = brief_record['title']
    elif 'title' in brief_record and 'mainTitles' in brief_record['title']:
        title_text = brief_record['title']['mainTitles'][0].get('text', 'Unknown Title')
        subfield_a.text = title_text if not title_text.endswith('.') else title_text
        if not title_text.endswith(('.', '?', '!')):
            subfield_a.text += '.'
    else:
        subfield_a.text = 'Unknown Title.'
    
    # 300 - Physical description (RECOMMENDED for CDs)
    field300 = ET.SubElement(root, 'datafield')
    field300.set('tag', '300')
    field300.set('ind1', ' ')
    field300.set('ind2', ' ')
    subfield_a = ET.SubElement(field300, 'subfield')
    subfield_a.set('code', 'a')
    subfield_a.text = '1 audio disc :'
    subfield_b = ET.SubElement(field300, 'subfield')
    subfield_b.set('code', 'b')
    subfield_b.text = 'digital ;'
    subfield_c = ET.SubElement(field300, 'subfield')
    subfield_c.set('code', 'c')
    subfield_c.text = '4 3/4 in.'
    
    xml_string = ET.tostring(root, encoding='unicode')
    return xml_string

def build_marcxml_from_discovery_record(rec: dict) -> str:
    """
    Build reasonably rich MARCXML from a WorldCat Discovery v2 bibRecord.
    Maps obvious, high-signal fields only (conservative, not full cataloging).
    """
    ns = "http://www.loc.gov/MARC21/slim"
    R = ET.Element(f"{{{ns}}}record"); R.set("xmlns", ns)

    def cf(tag, text):
        if text:
            el = ET.SubElement(R, "controlfield"); el.set("tag", tag); el.text = text

    def df(tag, ind1=" ", ind2=" "):
        el = ET.SubElement(R, "datafield"); el.set("tag", tag); el.set("ind1", ind1); el.set("ind2", ind2); return el

    def sf(df_el, code, text):
        if text is not None and str(text).strip() != "":
            s = ET.SubElement(df_el, "subfield"); s.set("code", code); s.text = str(text)

    # --- Leader / controlfields ---
    leader = ET.SubElement(R, "leader"); leader.text = "00000njm a2200000 i 4500"

    oclc_num = (rec.get("identifier") or {}).get("oclcNumber")
    cf("001", oclc_num or "")
    cf("003", "OCoLC")
    cf("005", datetime.now().strftime("%Y%m%d%H%M%S.0"))

    # 007: format-sensitive
    specific_fmt = (rec.get("format") or {}).get("specificFormat", "")
    if str(specific_fmt).upper() == "LP":
        cf("007", "sd fungnnmmned")  # analog disc
    else:
        cf("007", "sd fsngnnmmned")  # digital disc (CD-like)

    # 008
    pub_year = (rec.get("date") or {}).get("publicationDate", "")
    pub_year4 = pub_year[:4] if pub_year else "    "
    today_6 = datetime.now().strftime("%y%m%d")
    lang = (rec.get("language") or {}).get("itemLanguage", "eng")
    cf("008", f"{today_6}s{pub_year4}    xxu                 {lang} d")

    # 035 (OCLC)
    if oclc_num:
        f035 = df("035"); sf(f035, "a", f"(OCoLC){oclc_num}")

    # 040 (cataloging source)
    f040 = df("040"); sf(f040, "a", "OCLC"); sf(f040, "c", CATALOGING_INSTITUTION)

    # 041 language
    if lang:
        f041 = df("041"); sf(f041, "a", lang)

    # 1xx/7xx creators (simple personal-name mapping)
    creators = ((rec.get("contributor") or {}).get("creators")) or []
    for i, cr in enumerate(creators):
        last = (cr.get("secondName") or {}).get("text", "")
        first = (cr.get("firstName") or {}).get("text", "")
        name_txt = ", ".join([v for v in [last, first] if v]).strip(", ")
        if not name_txt:
            continue
        rel = None
        rels = cr.get("relators") or []
        if rels:
            rel = (rels[0].get("alternateTerm") or rels[0].get("term"))
        if i == 0:
            f = df("100", "1", " "); sf(f, "a", name_txt); 
            if rel: sf(f, "e", rel)
        else:
            f = df("700", "1", " "); sf(f, "a", name_txt)
            if rel: sf(f, "e", rel)

    # 245 Title + subtitle
    title = rec.get("title") or {}
    main_titles = title.get("mainTitles") or []
    sub_titles = title.get("subtitles") or []
    has_1xx = len(creators) > 0
    f245 = df("245", "1" if has_1xx else "0", "0")
    sf(f245, "a", (main_titles[0].get("text") if main_titles else ""))
    if sub_titles:
        sf(f245, "b", sub_titles[0].get("text"))

    # Series → 490/830
    series = title.get("seriesTitles") or []
    if series:
        sname = series[0].get("seriesTitle")
        if sname:
            f490 = df("490", "1", " "); sf(f490, "a", sname)
            f830 = df("830", " ", "0"); sf(f830, "a", sname)

    # 264 Publication
    pubs = rec.get("publishers") or []
    if pubs:
        p = pubs[0]
        f264 = df("264", " ", "1")
        sf(f264, "a", p.get("publicationPlace"))
        pubname = ((p.get("publisherName") or {}).get("text"))
        sf(f264, "b", pubname)
        if pub_year4.strip():
            sf(f264, "c", pub_year4)

    # 300 Physical description
    phys = (rec.get("description") or {}).get("physicalDescription")
    if phys:
        f300 = df("300"); sf(f300, "a", phys)

    # 028 Publisher/music numbers via varFields and identifiers
    for vf in (rec.get("varFields") or []):
        if vf.get("marcTag") == "028":
            f028 = df("028", "0", " ")
            for s in vf.get("subfields", []):
                code = s.get("code"); content = s.get("content")
                if code in ("a", "b", "q"): sf(f028, code, content)

    other_ids = ((rec.get("identifier") or {}).get("otherStandardIdentifiers")) or []
    for oid in other_ids:
        id_type = (oid.get("type") or "").lower()
        id_val = oid.get("id")
        if id_val and any(t in id_type for t in ["music", "publisher", "catalog"]):
            f028 = df("028", "0", " "); sf(f028, "a", id_val); sf(f028, "b", id_type)

    # 024 UPC
    for oid in other_ids:
        id_type = (oid.get("type") or "").upper()
        if "UPC" in id_type and oid.get("id"):
            f024 = df("024", "1", " "); sf(f024, "a", oid["id"])

    # 050/082 (light-touch)
    cls = rec.get("classification") or {}
    if cls.get("lc"):
        f050 = df("050"); sf(f050, "a", cls["lc"])
    if cls.get("dewey"):
        f082 = df("082"); sf(f082, "a", cls["dewey"])

    # 5xx notes
    note = rec.get("note") or {}
    for g in (note.get("generalNotes") or []):
        f500 = df("500"); sf(f500, "a", g.get("text"))
    perf = note.get("performerNotes") or []
    if perf:
        f511 = df("511"); sf(f511, "a", "; ".join(perf))
    if note.get("participantNote"):
        f511b = df("511"); sf(f511b, "a", note["participantNote"])

    # 505 contents
    for c in ((rec.get("description") or {}).get("contents") or []):
        cn = ((c.get("contentNote") or {}).get("text"))
        if cn:
            f505 = df("505", "0", " "); sf(f505, "a", cn)

    # 6xx/655 subjects
    for subj in (rec.get("subjects") or []):
        stype = (subj.get("subjectType") or "").lower()
        vocab = (subj.get("vocabulary") or "").lower()
        text = ((subj.get("subjectName") or {}).get("text")) or ""
        if not text:
            continue
        if stype == "topic":
            f650 = df("650", " ", "0"); sf(f650, "a", text)
        elif stype == "genreformterm":
            f655 = df("655", " ", "7"); sf(f655, "a", text)
            if vocab in ["lcgft", "fast", "rvmgf"]:
                sf(f655, "2", vocab)

    # RDA triplet
    f336 = df("336"); sf(f336, "a", "performed music"); sf(f336, "b", "prm"); sf(f336, "2", "rdacontent")
    f337 = df("337"); sf(f337, "a", "audio");          sf(f337, "b", "s");   sf(f337, "2", "rdamedia")
    f338 = df("338"); sf(f338, "a", "audio disc");     sf(f338, "b", "sd");  sf(f338, "2", "rdacarrier")

    return ET.tostring(R, encoding="unicode")


def detect_format_and_material_type(bib_record: dict):
    """
    Returns (specific_format, alma_item_material_value)
    """
    spec = ((bib_record.get("format") or {}).get("specificFormat") or "").upper()
    if spec == "LP":
        # Verify your Alma codes; 'VINYL' is a placeholder if that's your configured code.
        return ("LP", "VINYL")
    return (spec or "CD", "CD")


def check_if_oclc_exists_in_alma(oclc_number):
    """
    Search Alma to see if an OCLC number already exists.
    Returns MMS ID if found, None if not found.
    """
    url = f"{ALMA_BASE}/bibs"
    
    oclc_num = oclc_number.replace("(OCoLC)", "").strip()
    
    search_formats = [
        f"(OCoLC){oclc_num}",
        oclc_num
    ]
    
    for search_term in search_formats:
        params = {
            "other_system_id": search_term,
            "limit": "1"
        }
        
        try:
            r = requests.get(url, headers=HEADERS_XML, params=params, timeout=30)
            r.raise_for_status()
            
            root = ET.fromstring(r.text)
            total_records = root.find('total_record_count')
            
            if total_records is not None and int(total_records.text) > 0:
                bib = root.find('.//bib')
                if bib is not None:
                    mms_id = bib.find('mms_id')
                    if mms_id is not None:
                        return mms_id.text
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                continue
            raise
    
    return None


def import_to_alma(marcxml, normalization_rule=None):
    """Import MARCXML into Alma and return MMS ID."""
    url_bibs = f"{ALMA_BASE}/bibs"
    
    params = {}
    if normalization_rule:
        params["normalization"] = normalization_rule
    
    # Wrap the MARC record in <bib> tags
    bib_xml = f"<bib>{marcxml}</bib>"
    
    r = requests.post(url_bibs, headers=HEADERS_XML, params=params, data=bib_xml, timeout=120)
    r.raise_for_status()
    
    result = ET.fromstring(r.text)
    mms_id = result.find('mms_id')
    
    if mms_id is not None:
        return mms_id.text
    else:
        raise RuntimeError("No MMS ID returned from Alma")

# Important note: If you run this in PRODUCTION, you may want to comment out this unsuppressing step
# so new bibs stay suppressed for review before discovery.
def unsuppress_bib(mms_id: str):
    """Unsuppress bib for Discovery (Primo VE). Does NOT touch external search."""
    url = f"{ALMA_BASE}/bibs/{mms_id}"

    # GET existing bib
    r = requests.get(url, headers=HEADERS_XML, timeout=30)
    r.raise_for_status()
    bib_xml = ET.fromstring(r.text)

    # Ensure <suppress_from_publishing> exists, then set to false
    s = bib_xml.find('suppress_from_publishing')
    if s is None:
        s = ET.SubElement(bib_xml, 'suppress_from_publishing')
    s.text = 'false'

    # PUT back (update in Alma)
    r2 = requests.put(
        url,
        headers=HEADERS_XML,
        data=ET.tostring(bib_xml, encoding='unicode'),
        timeout=30
    )
    r2.raise_for_status()

def get_holdings(mms_id, location_code):
    """Check if holding exists for the specified location."""
    url_holdings = f"{ALMA_BASE}/bibs/{mms_id}/holdings"
    
    r = requests.get(url_holdings, headers=HEADERS_XML, timeout=30)
    r.raise_for_status()
    
    root = ET.fromstring(r.text)
    
    for holding in root.findall('holding'):
        loc = holding.find('location')
        if loc is not None and loc.text == location_code:
            hid = holding.find('holding_id')
            if hid is not None:
                return hid.text
    
    return None

def create_holding(mms_id, library_code, location_code):
    """Create a minimal, neutral holding record."""
    url_holdings = f"{ALMA_BASE}/bibs/{mms_id}/holdings"
    today6 = datetime.now().strftime('%y%m%d')

    # Minimal leader + neutral 008 (date-stamped) + 852(b,c)
    data = f'''<holding>
  <record>
    <leader>00000nx a2200000   4500</leader>
    <controlfield tag="008">{today6}{" " * 32}</controlfield>
    <datafield ind1=" " ind2=" " tag="852">
      <subfield code="b">{library_code}</subfield>
      <subfield code="c">{location_code}</subfield>
    </datafield>
  </record>
</holding>'''

    r = requests.post(url_holdings, headers=HEADERS_XML, data=data, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    hid = root.findtext('holding_id')
    if not hid:
        raise RuntimeError("Failed to create holding")
    return hid

def create_item(mms_id, holding_id, barcode, item_policy_code, material_value="CD"):
    """Create a physical item with a minimal payload. Material type is hard-coded to CD for this workflow."""
    url_items = f"{ALMA_BASE}/bibs/{mms_id}/holdings/{holding_id}/items"
    today = datetime.now().strftime("%Y-%m-%d") + "Z"

    # Force CD for this workflow; ignore any callers' variation
    material_value = "CD"

    data = f'''<item>
  <holding_data>
    <holding_id>{holding_id}</holding_id>
    <in_temp_location>false</in_temp_location>
  </holding_data>
  <item_data>
    <barcode>{barcode}</barcode>
    <physical_material_type>{material_value}</physical_material_type>
    <policy><value>{item_policy_code}</value></policy>
    <arrival_date>{today}</arrival_date>
    <internal_note_2>{INTERNAL_NOTE_2}</internal_note_2>
    <process_type>PHYSICAL_PROCESSING</process_type>
  </item_data>
</item>'''

    r = requests.post(url_items, headers=HEADERS_XML, data=data, timeout=30)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    item_pid = root.find('.//pid')
    if item_pid is not None:
        return item_pid.text
    else:
        raise RuntimeError("Item creation failed - no PID returned")

'''
def print_physical_material_type_codes():
    url = f"{ALMA_BASE}/conf/code-tables/PhysicalMaterialType"
    r = requests.get(url, headers=HEADERS_XML, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    print("PhysicalMaterialType codes:")
    for row in root.findall('.//row'):
        val = row.findtext('code')
        desc = row.findtext('description')
        print(f"  {val:10s}  - {desc}")
'''

# ====== MAIN PROCESSING ======

def process_file(input_file, delimiter='|'):
    """Process input file with format: oclcNumber|barcode or oclcNumber|barcode|title"""
    if not all([alma_api_key, client_id, client_secret]):
        raise SystemExit("Error: Missing required environment variables")
    
    print("="*60)
    print("OCLC to Alma Import Script - CD Workflow")
    print("="*60)
    print(f"Library: {LIBRARY_CODE}")
    print(f"Location: {LOCATION_CODE}")
    print(f"Item Policy: {ITEM_POLICY_CODE}")
    print(f"Input File: {input_file}")
    print("="*60)
    
    print("\nAuthenticating with OCLC...")
    try:
        oclc_token = get_access_token(client_id, client_secret)
        print("OCLC authentication successful")
    except Exception as e:
        raise SystemExit(f"Failed to authenticate with OCLC: {e}")
    
    results = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    total = len(lines)
    print(f"\nProcessing {total} records...\n")
    
    for idx, line in enumerate(lines, 1):
        if delimiter not in line:
            print(f"[{idx}/{total}] Skipping invalid line: {line}")
            continue
        
        parts = line.split(delimiter)
        if len(parts) < 2:
            print(f"[{idx}/{total}] Skipping - need at least oclcNumber{delimiter}barcode: {line}")
            continue
        
        oclc_num = parts[0].strip()
        barcode = parts[1].strip()
        title = parts[2].strip() if len(parts) > 2 else ""  # Title is optional
        
        print(f"[{idx}/{total}] Processing OCLC #{oclc_num} | Barcode: {barcode}")
        if title:
            print(f"         Title: {title[:60]}...")
        
        result = {
            'oclc': oclc_num,
            'barcode': barcode,
            'title': title
        }
        
        try:
            # Step 1: Check if record already exists
            print(f"         Checking if OCLC #{oclc_num} exists in Alma...")
            existing_mms_id = check_if_oclc_exists_in_alma(oclc_num)
            
            if existing_mms_id:
                print(f"         ALREADY EXISTS in Alma (MMS ID: {existing_mms_id})")
                print(f"         Item will NOT be processed")
                print(f"         Check physical item or add to giveaway pile\n")
                result['mms_id'] = existing_mms_id
                result['status'] = 'already_exists'
                result['action'] = 'Verify physical item or discard'
            else:
                print(f"         Record not found, fetching from OCLC...")
                marcxml, discovery_rec = get_marcxml_from_oclc(oclc_num, oclc_token)

                # Hard-code CD for this workflow
                result['format'] = "CD"
                result['material_type'] = "CD"
                mat_value = "CD"


                print(f"         Importing to Alma...")
                mms_id = import_to_alma(marcxml)
                result['mms_id'] = mms_id
                print(f"         MMS ID created: {mms_id}")

                # NEW: immediately unsuppress so it's not hidden in Primo VE
                print(f"         Unsuppressing bib in Alma...")
                unsuppress_bib(mms_id)
                print(f"         Bib is visible (Suppress from Discovery = OFF)")
                
                # Step 3: Check/create holding
                print(f"         Checking holdings...")
                holding_id = get_holdings(mms_id, LOCATION_CODE)

                if holding_id:
                    print(f"         Found existing holding: {holding_id}")
                else:
                    print(f"         Creating holding...")
                    holding_id = create_holding(mms_id, LIBRARY_CODE, LOCATION_CODE)
                    print(f"         Created holding: {holding_id}")

                result['holding_id'] = holding_id

                # Step 4: Create item
                print(f"         Creating item (material: {mat_value})...")
                item_pid = create_item(mms_id, holding_id, barcode, ITEM_POLICY_CODE, material_value=mat_value)
                result['item_pid'] = item_pid
                print(f"         Item created: {barcode}")

                result['status'] = 'success'
                print(f"         SUCCESS (new record imported)\n")

                
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
            result['status'] = 'error'
            result['error'] = error_msg
            print(f"         ERROR: {error_msg}\n")
            
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            print(f"         ERROR: {e}\n")
        
        results.append(result)
        time.sleep(2.0) # To avoid hitting rate limits
    
    return results

def write_id_table(results, input_file_path):
    """Write created record IDs to CSV with consistent naming."""
    # Console table
    print("\n" + "-"*60)
    print("CREATED RECORD IDS")
    print("-"*60)
    print("MMS ID | Holding ID | Item ID")
    for r in results:
        if r.get('status') == 'success':
            print(f"{r.get('mms_id','')} | {r.get('holding_id','')} | {r.get('item_pid','')}")

    # Determine output directory from input file
    input_dir = os.path.dirname(input_file_path)
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    csv_filename = f"alma-import-ids-{timestamp}.csv"
    csv_path = os.path.join(input_dir, csv_filename)
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["MMS ID", "Holding ID", "Item ID", "OCLC", "Barcode", "Title", "Format", "Material Type"])
        for r in results:
            if r.get('status') == 'success':
                w.writerow([
                    r.get('mms_id',''),
                    r.get('holding_id',''),
                    r.get('item_pid',''),
                    r.get('oclc',''),
                    r.get('barcode',''),
                    r.get('title',''),
                    r.get('format',''),
                    r.get('material_type',''),
                ])
    print(f"\nCreated record IDs written to: {csv_path}")
    return csv_path


def print_summary(results, input_file_path):
    """Print a console summary and write the CSV ID table."""
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)

    success = [r for r in results if r['status'] == 'success']
    errors = [r for r in results if r['status'] == 'error']
    already_exists = [r for r in results if r['status'] == 'already_exists']

    print(f"Total processed: {len(results)}")
    print(f"Successfully imported: {len(success)}")
    print(f"Already exist in Alma: {len(already_exists)}")
    print(f"Failed: {len(errors)}")

    if already_exists:
        print("\n" + "-"*60)
        print("RECORDS ALREADY IN ALMA:")
        print("-"*60)
        for r in already_exists:
            print(f"\nOCLC: {r['oclc']}")
            print(f"Barcode: {r['barcode']}")
            if r.get('title'):
                print(f"Title: {r['title'][:60]}")
            print(f"MMS ID: {r['mms_id']}")

    if errors:
        print("\n" + "-"*60)
        print("FAILED RECORDS:")
        print("-"*60)
        for r in errors:
            print(f"\nOCLC: {r['oclc']}")
            print(f"Barcode: {r['barcode']}")
            if r.get('title'):
                print(f"Title: {r['title'][:60]}")
            print(f"Error: {r.get('error', 'Unknown error')}")

    # Write the CSV of created IDs
    csv_path = write_id_table(results, input_file_path)
    print(f"\nID table written to CSV: {csv_path}")
    print("="*60)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Import OCLC records to Alma with holdings and items'
    )
   
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Path to pipe-delimited input file (oclcNumber|barcode|title)'
    )
    
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Automatically find latest batch upload file'
    )
    parser.add_argument(
        '--delimiter', 
        default='|',
        help='Field delimiter (default: |)'
    )
    
    args = parser.parse_args()
    
    # Determine input file
    input_file = None
    if args.auto:
        print("Searching for latest batch upload file...")
        input_file = find_latest_batch_upload_file()
        if not input_file:
            raise SystemExit("Error: No batch upload file found. Run workflow first.")
        print(f"Found: {input_file}")
    elif args.input_file:
        input_file = validate_input_file(args.input_file)
    else:
        parser.print_help()
        raise SystemExit("\nError: Provide input file path or use --auto flag")
    
    try:
        results = process_file(input_file, delimiter=args.delimiter)
        print_summary(results, input_file)
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        raise