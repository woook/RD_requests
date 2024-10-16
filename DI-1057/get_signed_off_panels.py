import psycopg2
import requests
import pandas as pd
from query_db import DB_CONFIG


def fetch_latest_signoff(panel_id):
    """
    Fetch the latest signed-off version for a given panel ID from the API.

    Args:
        panel_id (int): The panel ID to query.

    Returns:
        tuple: A tuple containing the name, version, and signed_off date.
    """
    url = (
        f"https://panelapp.genomicsengland.co.uk/api/v1/panels/signedoff/"
        f"?panel_id={panel_id}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["results"]:
            latest_result = data["results"][0]
            return (
                latest_result["name"],
                latest_result["version"],
                latest_result["signed_off"],
            )
    else:
        print(
            f"Error fetching {panel_id}, status code: {response.status_code}"
        )
    return None, None, None


def main():
    # Connect to the database and fetch panel IDs
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            print("Connected to the database successfully.")
            cursor.execute(
                """
                SELECT "panel-id", "panel-version"
                FROM testdirectory."east-panels"
                WHERE "panel-type-id" = 1
            """
            )
            panel_data = cursor.fetchall()

            # Fetch latest signoff data for each panel ID
            for panel_id, current_version in panel_data:
                _, latest_version, signed_off = fetch_latest_signoff(panel_id)

                if latest_version and latest_version != current_version:
                    # Update the panel-version in the database
                    update_query = f"""
                    UPDATE testdirectory."east-panels"
                    SET "panel-version" = '{latest_version}'
                    WHERE "panel-id" = '{panel_id}'
                    """
                    cursor.execute(update_query)
                    print(f"Updated panel {panel_id} to v_{latest_version}")    

            # Commit the changes to the database
            conn.commit()
            print("Database has been updated with the latest panel versions.")


if __name__ == "__main__":
    main()
