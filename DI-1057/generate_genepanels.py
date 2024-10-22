"""
Retrieves panel and gene data from the test_directory database and saves
the results into a TSV file '{yymmdd}_genepanels.tsv'.

The TSV file will have the following columns:
- TestID_ClinicalIndication_P
- PanelName_Version
- HGNCID
- PanelID
"""

import psycopg2
import csv
import argparse
from datetime import datetime
import dxpy
from query_db import DB_CONFIG


def fetch_genepanel_data() -> list:
    """
    Fetches gene-panels from the ngtd database.

    Returns:
        list: A list of tuples, where each tuple contains the following:
        (TestID_ClinicalIndication_P, PanelName_Version, HGNCID, PanelID)
    """
    query = """
    SELECT 
        CONCAT(t."test-id", '_', t."clinical-indication", '_P') AS test_info,
        CONCAT(p."panel-name", '_', p."panel-version") AS panel_info,
        g."hgnc-id",
        p."panel-id"
    FROM 
        testdirectory."east-genes" g
    JOIN 
        testdirectory."east-panels" p
        ON g."east-panel-id" = p."id"
    JOIN 
        testdirectory."east-tests" t
        ON p."east-tests-id" = t."id"
    ORDER BY 
        t."test-id"
    """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
    except Exception as e:
        print(f"Error querying database: {e}")
        return []


def upload_genepanels(data: list, project_id: str) -> None:
    """
    Saves gene panel data and uploads to DNAnexus.

    Args:
        data (list): List of tuples representing the gene-panels.
        project_id (str): DNAnexus project to upload file to
    """
    date_str = datetime.now().strftime("%y%m%d")
    file_name = f"{date_str}_genepanels.tsv"

    with open(file_name, mode="w", newline="", encoding="utf-8") as tsvfile:
        writer = csv.writer(tsvfile, delimiter="\t")

        for row in data:
            writer.writerow(row)

    # uploaod to DNAnexus
    res = dxpy.upload_local_file(
        filename=file_name,
        project=project_id,
    )
    if res.id:
        print(f"Successfully uploaded genepanels. File-id: {res.id}")
    else:
        print("Error uploading file")


def main():
    """Entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fetch and upload gene panel data."
    )
    parser.add_argument(
        "--project_id", required=True,
        help="DNAnexus project ID to upload the file to."
    )
    args = parser.parse_args()

    # generate and upload genepanels
    genepanels = fetch_genepanel_data()

    if genepanels:
        upload_genepanels(genepanels, args.project_id)
    else:
        print("No file to upload.")


if __name__ == "__main__":
    main()
