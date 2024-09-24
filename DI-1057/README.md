## Database specification for Test Directory

### Original request
Validate the version of TD database hosted on AWS RDS service against the Internal East-GLH Rare and Inherited Disease Test Directory v7 spreadsheet

Add genes data into TD database

### Scripts
1. `query_db.py`: 
Fetches panel and gene data from the Test Directory database and saves it to a CSV file.

2. `parse_east_glh_td_spreadsheet.py`:
Parses the East-GLH Test Directory spreadsheet, retrieves panel info from the PanelApp API, and formats the data for comparison.

3. `compare_dfs.py`:
Compares the data from the spreadsheet and database, identifying mismatches

### How to Run
1. Set up credentials: Ensure your database credentials are set in environment variables.

2. Install dependencies:
`pip install -r requirements.txt`

3. Run scripts in following order:
- `python query_db.py`
- `python parse_east_glh_td_spreadsheet.py`
- `python compare_dfs.py`

### Output
Prints diff between the two data sources to stdout and writes a summary to a spreadsheet (`td_diff.xlsx`).
