"""
OCLC API Testing and Query Module

Interactive testing tool for querying OCLC WorldCat API with automatic
holdings information enrichment. Primarily used for development and
debugging of music catalog searches with IXA holdings verification.
"""

import os
import requests
import json

def get_access_token(client_id, client_secret):
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

def get_holdings_info(oclc_number, access_token):
    """
    Query the OCLC API for holdings information for a specific OCLC number.
    
    Parameters:
    oclc_number (str): The OCLC number to query
    access_token (str): The access token for the OCLC API
    
    Returns:
    tuple: (is_held_by_IXA, total_holding_count, holding_symbols)
    """
    base_url = "https://americas.discovery.api.oclc.org/worldcat/search/v2"
    endpoint = f"{base_url}/bibs-holdings"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    params = {
        "oclcNumber": oclc_number,
        "limit": 50  # Adjust as needed to get more holdings
    }
    
    try:
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Initialize variables
        is_held_by_IXA = False
        total_holding_count = 0
        holding_symbols = []
        
        # Extract the holdings information
        if "briefRecords" in data and len(data["briefRecords"]) > 0:
            record = data["briefRecords"][0]
            
            if "institutionHolding" in record:
                holdings = record["institutionHolding"]
                total_holding_count = holdings.get("totalHoldingCount", 0)
                
                if "briefHoldings" in holdings:
                    for holding in holdings["briefHoldings"]:
                        symbol = holding.get("oclcSymbol", "")
                        holding_symbols.append(symbol)
                        
                        if symbol == "IXA":
                            is_held_by_IXA = True
        
        return is_held_by_IXA, total_holding_count, holding_symbols
    
    except requests.RequestException as e:
        print(f"Error getting holdings for OCLC number {oclc_number}: {str(e)}")
        return False, 0, []

def query_oclc_api(query, limit=5):
    client_id = os.environ.get("OCLC_CLIENT_ID")
    client_secret = os.environ.get("OCLC_SECRET")

    if not client_id or not client_secret:
        return "Error: OCLC_CLIENT_ID and OCLC_SECRET must be set in environment variables."

    try:
        access_token = get_access_token(client_id, client_secret)
    except Exception as e:
        return f"Error getting access token: {str(e)}"

    base_url = "https://americas.discovery.api.oclc.org/worldcat/search/v2/bibs"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    params = {
        "q": query,
        "limit": limit,
        "offset": 1,
        "itemType": "music",
        "inCatalogLanguage": "eng",
        "itemSubType": "music-lp"
    }

    try:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        # Add holdings information to each record
        if "bibRecords" in result:
            for record in result["bibRecords"]:
                if "identifier" in record and "oclcNumber" in record["identifier"]:
                    oclc_number = record["identifier"]["oclcNumber"]
                    
                    # Get holdings information
                    is_held_by_IXA, total_holding_count, holding_symbols = get_holdings_info(oclc_number, access_token)
                    
                    # Add holdings information to the record
                    record["holdingsInfo"] = {
                        "heldByIXA": is_held_by_IXA,
                        "totalHoldingCount": total_holding_count,
                        "holdingSymbols": holding_symbols
                    }
        
        return result
    except requests.RequestException as e:
        error_message = str(e)
        if hasattr(e, 'response') and e.response is not None:
            error_message += f"\nFull response content: {e.response.text}"
        return f"Error querying OCLC API: {error_message}"

def format_record(record):
    """Format a single record for display."""
    output = []
    if "identifier" in record and "oclcNumber" in record["identifier"]:
        output.append(f"OCLC Number: {record['identifier']['oclcNumber']}")
    
    if "title" in record and "mainTitles" in record["title"] and record["title"]["mainTitles"]:
        output.append(f"Title: {record['title']['mainTitles'][0].get('text', 'N/A')}")
    
    if "contributor" in record and "creators" in record["contributor"] and record["contributor"]["creators"]:
        creators = []
        for creator in record["contributor"]["creators"]:
            if "nonPersonName" in creator and "text" in creator["nonPersonName"]:
                creators.append(creator["nonPersonName"]["text"])
            elif "firstName" in creator and "secondName" in creator:
                first_name = creator.get("firstName", {}).get("text", "")
                second_name = creator.get("secondName", {}).get("text", "")
                creators.append(f"{first_name} {second_name}".strip())
        if creators:
            output.append(f"Creator(s): {', '.join(creators)}")
    
    # Add holdings information
    if "holdingsInfo" in record:
        holdings_info = record["holdingsInfo"]
        output.append(f"Held by IXA: {'Yes' if holdings_info['heldByIXA'] else 'No'}")
        output.append(f"Total institutions holding: {holdings_info['totalHoldingCount']}")
        output.append(f"Institution symbols: {', '.join(holdings_info['holdingSymbols'])}")
    
    return "\n".join(output)

def main():
    print("OCLC API Query Testing")
    print("Enter your queries one at a time. Type 'exit' to quit.")
    
    while True:
        query = input("Enter your query: ").strip()
        if query.lower() == 'exit':
            print("Exiting program.")
            break
        if not query:
            print("Query cannot be empty. Please try again.")
            continue

        print("\nQuerying OCLC API...")
        result = query_oclc_api(query)
        
        if isinstance(result, dict):
            if "numberOfRecords" in result and result["numberOfRecords"] > 0:
                print(f"\nFound {result['numberOfRecords']} records. Showing first {len(result.get('bibRecords', []))}:")
                for i, record in enumerate(result.get("bibRecords", []), 1):
                    print(f"\n--- Record {i} ---")
                    print(format_record(record))
                    print("-------------------")
            else:
                print("No records found.")
                
            # Provide option to see the full JSON
            show_full = input("\nShow full JSON response? (y/n): ").strip().lower()
            if show_full == 'y':
                print(json.dumps(result, indent=2))
        else:
            print(result)

if __name__ == "__main__":
    main()