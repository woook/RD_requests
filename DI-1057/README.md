## Database specification for Test Directory

### Original request
Validate the version of TD database hosted on AWS RDS service against the Internal East-GLH Rare and Inherited Disease Test Directory v7 spreadsheet

Add genes data into TD database

### Scripts
1. `query_db.py`: 
Fetches panel and gene data from the Test Directory database and saves it to a CSV file.

2. `parse_east_glh_td_spreadsheet.py`:
Parses the East-GLH Test Directory spreadsheet, retrieves panel info from the PanelApp API, and formats the data for comparison.
*Requires the spreadsheet file as input (-i or --internal_td_spreadsheet).*

3. `compare_dfs.py`:
Compares the data from the spreadsheet and database, identifying mismatches

4. `add_genes.py`:
Populates the table, `east-genes`.

5. `validate_east_genes_table.py`:
Verifies that `east-genes` table has been populated

6. `generate_genepanels.py`:
Generates a new genepanels file by querying all panels and genes in new ngtd database

7. `compare_genepanels.py`:
Compares the new genepanels with the old (prod) one and summarises any diff to a spreadsheet

8. `check_gene_to_transcript.py`:
Check that all genes in new genepanels are mapped to a clinical transcript in prod g2t file.

### How to Run
1. Set up credentials: Ensure your database credentials are set in environment variables.

2. Install dependencies:
`pip install -r requirements.txt`

3. Run scripts in following order to validate panels in ngtd database:
- `python query_db.py`
- `python parse_east_glh_td_spreadsheet.py -i path/to/spreadsheet`
- `python compare_dfs.py`

### Output
Prints diff between the two data sources to stdout and writes a summary to a spreadsheet (`td_diff.xlsx`).
