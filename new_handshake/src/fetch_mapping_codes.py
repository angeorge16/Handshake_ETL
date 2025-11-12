import pandas as pd
import pyodbc
from datetime import datetime
import os
from src.db_connection import get_connection
from src.utils import save_to_csv

def extract_mapping_codes():
    """
    Executes stored procedure dbo.get_new_mapping_codes and saves the output to CSV.
    """

    print("Extracting mapping codes...")

    # Step 1: Connect to SQL Server
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Step 2: Execute the stored procedure
        query = "EXEC dbo.get_new_mapping_codes"
        print("Running stored procedure dbo.get_new_mapping_codes ...")

        # Step 3: Capture result set directly into a pandas DataFrame
        df = pd.read_sql(query, conn)
        print(f"Retrieved {len(df)} rows from stored procedure.")

        # Step 4: Save to CSV (under output/mapping_codes/)
        save_to_csv(df, category="mapping_codes")

    except Exception as e:
        print("Error during ETL:", e)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
