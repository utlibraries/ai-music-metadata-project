"""
OCLC to Alma Import (LP workflow)

INPUT (text file):
  Format: oclcNumber|barcode|title
  - title is optional
  - default delimiter is "|"; override with --delimiter ','

WHAT IT DOES:
  • Fetches a WorldCat record (Discovery v2), builds MARCXML, and imports a new bib to Alma
  • Creates/uses a holding at your configured library/location
  • Creates one item per line (material type forced to "LP")
  • Skips titles that already exist in your Alma instance (by OCLC number)
  • Writes a CSV of created IDs (MMS, holding, item) next to the input file

USAGE:
  python path/to/alma-batch-upload-script-lp.py path/to/input.txt [--delimiter '|'] [--yes] [--restrict-dir /expected/deliverables] [--report]

SAFETY:
  • File path is REQUIRED (no auto-detect).
  • Standard run performs imports after a confirmation prompt.
  • Use --yes to skip the prompt (non-interactive runs).
  • Optional: --report prints a summary in the terminal and exits (no other action taken).
  • Optional: --restrict-dir limits inputs to a known directory tree.


REQUIRED ENVIRONMENT VARIABLES:
  ALMA_SANDBOX_API_KEY
  OCLC_CLIENT_ID
  OCLC_SECRET
  ALMA_LIBRARY_CODE
  ALMA_LOCATION_CODE
  ALMA_LP_ITEM_POLICY
  ALMA_CATALOGING_INSTITUTION
  
OPTIONAL ENVIRONMENT VARIABLES:
  ALMA_REGION=api-na (default) 
  ALMA_INTERNAL_NOTE_2="AI-assisted cataloging"


"""

import os
import requests
import xml.etree.ElementTree as ET
import csv
import time
from datetime import datetime

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
ITEM_POLICY_CODE = get_required_env("ALMA_LP_ITEM_POLICY")
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

def ensure_under_dir(file_path: str, allowed_root: str):
    p = os.path.realpath(file_path)
    root = os.path.realpath(allowed_root)
    if not p.startswith(root + os.sep) and p != root:
        raise SystemExit(f"Error: {p} is outside allowed path: {root}")


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


def get_marcxml_from_oclc(oclc_number: str, access_token: str):
    """
    Fetch a WorldCat Discovery v2 record and return MARCXML + the source record.
    Prefers full bibRecords; falls back to briefRecords; raises if nothing usable.

    Returns: (marcxml: str, source_record: dict)
    """
    base_url = "https://americas.discovery.api.oclc.org/worldcat/search/v2/bibs"
    oclc_num = oclc_number.replace("(OCoLC)", "").strip()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    # We try progressively broader queries:
    query_attempts = [
        # Most specific: music LPs
        {"q": f"no:{oclc_num}", "limit": 1, "itemType": "music", "itemSubType": "music-lp"},
        # Next: any music
        {"q": f"no:{oclc_num}", "limit": 1, "itemType": "music"},
        # Broadest: no item filters
        {"q": f"no:{oclc_num}", "limit": 1},
    ]

    last_error_text = None
    data = None

    for params in query_attempts:
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=60)
            # If access token is bad/expired, caller should fetch a new one and retry the whole record
            if r.status_code in (401, 403):
                raise RuntimeError(f"OCLC auth error ({r.status_code}): {r.text}")

            r.raise_for_status()
            data = r.json()  # may raise if not JSON
        except Exception as e:
            last_error_text = str(e)
            continue

        # If we got a response, check for usable records
        if data:
            # Preferred path: full bibRecords
            bibs = (data.get("bibRecords") or [])
            if bibs:
                rec = bibs[0]
                try:
                    marcxml = build_marcxml_from_discovery_record(rec)
                    return marcxml, rec
                except Exception:
                    # fall through to try brief or next attempt
                    pass

            # Fallback path: briefRecords
            briefs = (data.get("briefRecords") or [])
            if briefs:
                brief = briefs[0]
                # If you renamed your fallback, call build_minimal_marcxml_fallback(brief)
                marcxml = build_minimal_marcxml_fallback(brief)
                return marcxml, brief

        # No usable record from this attempt; try the next params
        last_error_text = "No bibRecords or briefRecords in response."

    # If we exhausted attempts, error out with the most informative thing we have
    raise ValueError(
        f"No usable OCLC record found for {oclc_num}. "
        f"Last error/response: {last_error_text or 'none'}"
    )


def build_minimal_marcxml_fallback(brief_record):
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
    
    # 007 - Physical description for LP (REQUIRED for sound recordings)
    field007 = ET.SubElement(root, 'controlfield')
    field007.set('tag', '007')
    field007.text = 'sd fungnnmmned'  
    
    # 008 - Fixed-length data elements (REQUIRED)
    field008 = ET.SubElement(root, 'controlfield')
    field008.set('tag', '008')
    # Get publication date if available
    pub_date = '    '
    if 'date' in brief_record and 'publicationDate' in brief_record['date']:
        pub_date = brief_record['date']['publicationDate'][:4]
    current_date = datetime.now().strftime('%y%m%d')
    field008.text = f"{current_date}s{pub_date}    xxu           |  eng d"
    
    # 035 - System Control Number 
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
    
    # 300 - Physical description
    field300 = ET.SubElement(root, 'datafield')
    field300.set('tag', '300')
    field300.set('ind1', ' ')
    field300.set('ind2', ' ')
    subfield_a = ET.SubElement(field300, 'subfield')
    subfield_a.set('code', 'a')
    subfield_a.text = '1 audio disc :'  
    subfield_b = ET.SubElement(field300, 'subfield')
    subfield_b.set('code', 'b')
    subfield_b.text = 'analog ;'  
    subfield_c = ET.SubElement(field300, 'subfield')
    subfield_c.set('code', 'c')
    subfield_c.text = '12 in.'  
    
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

    # 050/082 (minimal)
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
            r = requests.get(url, headers=HEADERS_XML, params=params, timeout=60)
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

# This unsuppresses new bibs when called (makes them visible in Primo VE). Leave it in for immediate discovery.
def unsuppress_bib(mms_id: str):
    """Unsuppress bib for Discovery (Primo VE). Does NOT touch external search."""
    url = f"{ALMA_BASE}/bibs/{mms_id}"

    # GET existing bib
    r = requests.get(url, headers=HEADERS_XML, timeout=60)
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
        timeout=60
    )
    r2.raise_for_status()

def get_holdings(mms_id, location_code):
    """Check if holding exists for the specified location."""
    url_holdings = f"{ALMA_BASE}/bibs/{mms_id}/holdings"
    
    r = requests.get(url_holdings, headers=HEADERS_XML, timeout=60)
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
    """Create a minimal holding record."""
    url_holdings = f"{ALMA_BASE}/bibs/{mms_id}/holdings"
    today6 = datetime.now().strftime('%y%m%d')

    data = f'''<holding>
  <record>
    <leader>00000nx a2200000   4500</leader>
    <!-- Neutral 008: date stamp + unknowns; makes no acquisition/“gift” claims -->
    <controlfield tag="008">{today6}uu{" " * 25}{today6}</controlfield>
    <datafield ind1="0" ind2=" " tag="852">
      <subfield code="b">{library_code}</subfield>
      <subfield code="c">{location_code}</subfield>
    </datafield>
  </record>
</holding>'''

    r = requests.post(url_holdings, headers=HEADERS_XML, data=data, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    hid = root.findtext('holding_id')
    if not hid:
        raise RuntimeError("Failed to create holding")
    return hid

def create_item(mms_id, holding_id, barcode, item_policy_code, material_value="LP"):
    url_items = f"{ALMA_BASE}/bibs/{mms_id}/holdings/{holding_id}/items"
    today = datetime.now().strftime("%Y-%m-%d") + "Z"
    material_value = (material_value or "LP").strip()

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

    r = requests.post(url_items, headers=HEADERS_XML, data=data, timeout=60)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    item_pid = root.find('.//pid')

    if item_pid is not None:
        return item_pid.text
    else:
        raise RuntimeError(f"Item creation failed - no PID returned")
    
def report_summary(input_file, delimiter):
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    total = len(lines)
    sample = lines[:5]
    print("\n--- CONSOLE REPORT ---")
    print()
    print(f"Input file: {input_file}")
    print()
    print(f"Delimiter:  {delimiter!r}")
    print(f"Total lines: {total}")
    if sample:
        print("Snippet of input file (up to 5 records):")
        print ()
        for s in sample:
            print(f"  {s}")
            print()
    out_dir = os.path.dirname(input_file)
    ts = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    print()
    print("No actions taken.")
    print()
    print(f"Planned path to output CSV: {os.path.join(out_dir, f'alma-import-ids-{ts}.csv')}")
    print()
    return total

def _classify_oclc_source(rec: dict) -> str:
    """
    Label the type of OCLC record used.
    """
    if not isinstance(rec, dict):
        return "Unknown OCLC record type"
    if rec.get("varFields") or rec.get("publishers") or rec.get("contributor"):
        return "OCLC full bib record"
    return "OCLC brief fallback record"

'''
# Uncomment if you need to look up valid PhysicalMaterialType codes
def print_physical_material_type_codes():
    url = f"{ALMA_BASE}/conf/code-tables/PhysicalMaterialType"
    r = requests.get(url, headers=HEADERS_XML, timeout=60)
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
    print("OCLC to Alma Import Script - LP Workflow")
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
        
        parts = line.split(delimiter, 2)
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
        
        max_retries = 3
        for attempt in range(max_retries):
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
                    result['oclc_source'] = _classify_oclc_source(discovery_rec)

                    # Hard-code LP for this workflow
                    result['format'] = "LP"
                    result['material_type'] = "LP"
                    mat_value = "LP"

                    print(f"         Importing to Alma...")
                    mms_id = import_to_alma(marcxml)
                    result['mms_id'] = mms_id
                    print(f"         MMS ID created: {mms_id}")

                    # Immediately unsuppress so it's not hidden in Primo VE - comment out if unwanted
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
                    item_pid = create_item(
                        mms_id,
                        holding_id,
                        barcode,
                        ITEM_POLICY_CODE,
                        material_value=mat_value,
                    )

                    result['item_pid'] = item_pid
                    print(f"         Item created: {barcode}")

                    result['status'] = 'success'
                    print(f"         SUCCESS (new record imported)\n")
                    print(f"         Source used: {result.get('oclc_source','Unknown')}\n")

                break  # Success or already_exists — no retry needed

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    wait_time = 30 * (2 ** attempt)
                    print(f"         Timeout/connection error (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"         Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    result['status'] = 'error'
                    result['error'] = str(e)
                    print(f"         ERROR after {max_retries} attempts: {e}\n")

            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
                result['status'] = 'error'
                result['error'] = error_msg
                print(f"         ERROR: {error_msg}\n")
                break

            except Exception as e:
                result['status'] = 'error'
                result['error'] = str(e)
                print(f"         ERROR: {e}\n")
                break
        
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
        w.writerow(["MMS ID", "Holding ID", "Item ID", "OCLC", "Barcode", "Title", "Format", "Material Type", "OCLC Source"])
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
                    r.get('oclc_source',''),
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

    # Write CSV of created IDs
    csv_path = write_id_table(results, input_file_path)
    print(f"\nID table written to CSV: {csv_path}")
    print("="*60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Import OCLC records to Alma with holdings and items (LP workflow)'
    )
    
    parser.add_argument(
        'input_file',
        help='Path to delimited input file (oclcNumber|barcode|title)'
    )
    parser.add_argument(
        '--delimiter',
        default='|',
        help='Field delimiter (default: |)'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Do not prompt for confirmation.'
    )
    parser.add_argument(
        '--report',
        action='store_true',
        help='Report only; no writes.'
    )
    parser.add_argument(
        '--restrict-dir',
        default=None,
        help='Only allow input files under this directory.'
    )

    args = parser.parse_args()

    # Mandatory file, validated
    input_file = validate_input_file(args.input_file)

    # Optional path restriction
    if args.restrict_dir:
        ensure_under_dir(input_file, args.restrict_dir)

    # Short report header
    count = report_summary(input_file, args.delimiter)

    # If just reporting, exit cleanly
    if args.report:
        raise SystemExit(0)

    # Require explicit confirmation unless --yes
    if not args.yes:
        resp = input(f"Proceed to import {count} line(s) into Alma? Type 'yes' to continue: ").strip().lower()
        if resp != 'yes':
            raise SystemExit("Aborted by user.")

    # Execute
    try:
        results = process_file(input_file, delimiter=args.delimiter)
        print_summary(results, input_file)
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        raise
