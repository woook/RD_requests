"""
Reads data from East GLH RD TD spreadsheet and processes columns and rows to
match data from PostgreSQL database (td_sql.csv).
"""
import argparse
import pandas as pd
import re
import requests

# Define the file paths and columns
COLS = [
    'Clinical indication ID', 'Test ID', 'Clinical Indication', 'Target/Genes'
]
TEST_IDS = pd.read_csv("td_sql.csv")["test-id"].tolist()

def extract_panel_id(value):
    """
    Extracts the panel ID from the "Target/Genes" column if present.

    Args:
        value (str): A string containing a potential panel_id in parentheses.

    Returns:
        str|None: Extracted panel_id if found, otherwise None.
    """
    match = re.search(r'\((\d+)\)', value)
    if match:
        return match.group(1)
    return None

def get_panel_info(panel_id):
    """
    Fetches the panel name and latest sign-off version of a given panel_id from
    the PanelApp API.

    Args:
        panel_id (str): The PanelApp ID.

    Returns:
        tuple: A tuple containing (panel_name, panel_version), or
        (None, None) if the panel_id is not found.
    """
    if panel_id is None:
        return None, None

    url = (
        f"https://panelapp.genomicsengland.co.uk/api/v1/panels/signedoff/"
        f"?panel_id={panel_id}"
    )

    response = requests.get(url, headers={'accept': 'application/json'})

    if response.status_code == 200:
        results = response.json().get("results")
        if results:
            latest_result = results[0]
            panel_name = latest_result["name"]
            panel_version = latest_result["version"]
 
            return panel_name, panel_version

    print(f"Request failed for {panel_id}. Status code:{response.status_code}")
    return None, None

def parse_spreadsheet(file):
    """
    Parses the spreadsheet and formats the data for comparison with td_sql.csv.

    Args:
        file (str): Path to the spreadsheet file to parse.
    """
    df = pd.read_excel(file)
    
    # Select relevant columns and filter rows to match "td_sql.csv"
    df = df[COLS]
    df = df[df["Test ID"].isin(TEST_IDS)]

    # Rename cols to match db schema
    df.columns = [
        "clinical-indication-id",
        "test-id",
        "clinical-indication",
        "Target/Genes"
    ]

    df["panel-id"] = df["Target/Genes"].apply(extract_panel_id)

    # Fetch panel_name and panel_version from the API based on panel_id
    df[["panel-name", "panel-version"]] = df["panel-id"].apply(
        lambda panel_id: pd.Series(get_panel_info(panel_id))
    )

    # Determine the panel type based on whether the panel_id exists
    df["panel-type"] = df["panel-id"].apply(
        lambda x: "PanelApp" if x else "EastGLH"
    )

    # Drop the original "Target/Genes" column
    df.drop(columns=["Target/Genes"], inplace=True)

    df.sort_values("test-id", inplace=True)
    df.to_csv("internal_east_glh_td.csv", index=False)
    
def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Process the East GLH RD TD spreadsheet and format it for comparison.'
    )
    parser.add_argument(
        '--internal_td_spreadsheet', '-i', 
        type=str, required=True,
        help='Path to the Internal East GLH Rare Disease Test Directory spreadsheet (.xlsx)'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    parse_spreadsheet(args.internal_td_spreadsheet)

    print("DONE")
