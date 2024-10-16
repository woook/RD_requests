"""
Retrieves panel IDs from the 'east-panels' table,
fetches high-confidence genes assocatiared with each panel from PanelApp API,
and inserts them into the 'east-genes' table using psycopg2.
"""

import requests
import psycopg2
from query_db import DB_CONFIG


def get_high_confidence_genes(panel_id: int, version: str) -> list:
    """Fetch high-confidence genes from the PanelApp API

    Args:
        panel_id (int): PanelApp's ID of panel
        version (str): Latest Signoff Version of the panel.

    Returns:
        list[str]: List of hgnc ids
    """

    url = (
        f"https://panelapp.genomicsengland.co.uk/api/v1/panels/{panel_id}/"
        f"?version={version}"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        panel_data = response.json()

        high_confidence_genes = [
            gene["gene_data"]["hgnc_id"]
            for gene in panel_data["genes"]
            if gene["confidence_level"] == "3"
        ]
        return high_confidence_genes
    except Exception as e:
        print(f"Error fetching data for panel {panel_id}: {e}")
        return []


def insert_genes_into_db(east_panel_id, hgnc_ids, cursor) -> None:
    """Insert high-confidence genes for a panel into the database.

    Args:
        east_panel_id (int): primary key of panel in "east-panels" table
        hgnc_ids (List[str]): list of hgnc ids
        cursor (pyscopg2.Cursor): A database cursor object used to execute
        SQL queries.
    """
    
    for hgnc_id in hgnc_ids:
        try:
            cursor.execute(
                """
                INSERT INTO "testdirectory"."east-genes" ("east-panel-id", "hgnc-id")
                VALUES (%s, %s)
                """,
                (east_panel_id, hgnc_id)
            )
        except Exception as e:
            print(f"Error inserting panel {east_panel_id}, gene {hgnc_id}: {e}")


def main():
    """Entry point
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                print("Connected to the database successfully.")

                cursor.execute("""
                    SELECT "id", "panel-id", "panel-version"
                    FROM testdirectory."east-panels"
                    WHERE "panel-type-id" = 1
                """)

                panel_data = cursor.fetchall()

                # fetchall() returns a list of tuples,
                for east_panel_id, panel_id, version in panel_data:
                    print(f"Processing panel {panel_id}...")

                    hgnc_ids = get_high_confidence_genes(panel_id, version)

                    if hgnc_ids:
                        insert_genes_into_db(east_panel_id, hgnc_ids, cursor)

                conn.commit()

    except Exception as e:
        print(f"Error connecting to database: {e}")


if __name__ == "__main__":
    main()
