"""queries RD panels in db with credentials saved in env
and reads query result into a pandas df 
"""
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host" : os.getenv("DB_ENDPOINT"),
    "port" : os.getenv("DB_PORT"),
    "user" : os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "dbname": os.getenv("DB_NAME"),
}

QUERY = """
    SELECT "clinical-indication-id", "test-id", "clinical-indication",
    "panel-name", "panel-version", "panel-id", "panel-type"
    FROM "testdirectory"."east-tests"
    INNER JOIN "testdirectory"."east-panels" ON "east-tests"."id" = "east-panels"."east-tests-id"
    INNER JOIN "testdirectory"."panel-type" ON "east-panels"."panel-type-id" = "panel-type"."id"
"""

def read_query():
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql_query(QUERY, conn)
        df.sort_values("test-id", inplace=True)
        df.to_csv("td_sql.csv", index=False)
        return

    
if __name__ == "__main__":
    read_query()
    print("DONE")