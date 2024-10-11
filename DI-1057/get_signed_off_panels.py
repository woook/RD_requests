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
    url = f"https://panelapp.genomicsengland.co.uk/api/v1/panels/signedoff/?panel_id={panel_id}"
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
                SELECT "id", "panel-id" 
                FROM testdirectory."east-panels"
                WHERE "panel-type-id" = 1
            """
            )
            panel_ids = cursor.fetchall()

    results = []

    # Fetch latest signoff data for each panel ID
    for _, panel_id in panel_ids:
        name, version, signed_off = fetch_latest_signoff(panel_id)
        if name and version and signed_off:
            results.append(
                {
                    "PanelID": panel_id,
                    "Name": name,
                    "Version": version,
                    "SignedOffDate": signed_off,
                }
            )

    # Convert the list of results to a DataFrame
    df = pd.DataFrame(results)

    # Save the DataFrame to a TSV file
    df.to_csv("latest_signoff_panels.tsv", sep="\t", index=False)
    print("Data has been saved to latest_signoff_panels.tsv")


if __name__ == "__main__":
    main()
