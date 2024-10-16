"""
Verifies that the 'east-genes' table has been populated and sense checks:
- Count of unique east-panel-id should be 38.
- Each panel has at least one gene.
- No duplicate (east-panel-id, hgnc-id) pairs.
"""

import psycopg2
from query_db import DB_CONFIG


def perform_sense_checks():
    """Performs sense checks on the 'east-genes' table."""

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            print("Connected to the database successfully.\n")

            # Check total number of rows in east-genes table
            cursor.execute('SELECT COUNT(*) FROM testdirectory."east-genes"')
            total_rows = cursor.fetchone()[0]
            print(f"Total rows in 'east-genes' table: {total_rows}")

            # Check number of unique east-panel-id
            cursor.execute(
                """
                SELECT COUNT(DISTINCT "east-panel-id")
                FROM testdirectory."east-genes"
            """
            )
            unique_panel_count = cursor.fetchone()[0]
            print(f"Number of unique east-panel-id: {unique_panel_count}")

            # Check if all panels have at least one gene (should be 40)
            if unique_panel_count == 40:
                print("All 40 Panels have been populated with genes.")
            else:
                print(
                    f"Warning: Only {unique_panel_count} panels have been "
                    "populated with genes (expected 40)."
                )

            # Check for any east-panel-id without genes
            cursor.execute(
                """
                SELECT p."id", p."panel-id"
                FROM testdirectory."east-panels" p
                LEFT JOIN testdirectory."east-genes" g
                ON p."id" = g."east-panel-id"
                WHERE g."east-panel-id" IS NULL
            """
            )
            panels_without_genes = cursor.fetchall()
            if panels_without_genes:
                print(
                    f"Warning: The following panels have no associated genes: "
                    f"{panels_without_genes}"
                )
            else:
                print("All Panels have associated genes.")

            # Check for duplicate (east-panel-id, hgnc-id) pairs
            cursor.execute(
                """
                SELECT "east-panel-id", "hgnc-id", COUNT(*)
                FROM testdirectory."east-genes"
                GROUP BY "east-panel-id", "hgnc-id"
                HAVING COUNT(*) > 1
            """
            )
            duplicates = cursor.fetchall()
            if duplicates:
                print(
                    f"Warning: Duplicate (east-panel-id, hgnc-id) pairs found:"
                    f" {duplicates}"
                )
            else:
                print("No duplicate (east-panel-id, hgnc-id) pairs found.")

            # Check for any rows with NULL hgnc-id or east-panel-id
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM testdirectory."east-genes"
                WHERE "hgnc-id" IS NULL OR "east-panel-id" IS NULL
            """
            )
            null_values_count = cursor.fetchone()[0]
            if null_values_count > 0:
                print(
                    f"Warning: {null_values_count} rows have NULL values for"
                    f"either 'hgnc-id' or 'east-panel-id'."
                )
            else:
                print("No NULL values for 'hgnc-id' or 'east-panel-id'.")


if __name__ == "__main__":
    perform_sense_checks()
