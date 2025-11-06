import pandas as pd
import pyodbc
from datetime import datetime
import os
from src.db_connection import get_connection, get_config
from src.utils import save_to_csv

def extract_current_students(load_type='delta'):
    """
    Executes stored procedure dbo.sp_get_delta_from_source and saves output to CSV.
    
    Parameters
    ----------
    load_type : str, optional
        'delta' or 'full'. Defaults to 'delta'.
    """
    print(f"Extracting current students ({load_type.upper()} load)...")

    # Connect to SQL Server
    conn = get_connection()
    cursor = conn.cursor()

    # Run stored procedure
    try:
        if load_type.lower() in ['delta', 'full']:
            query = f"EXEC dbo.sp_get_delta_from_source '{load_type}'"
        else:
            raise ValueError("load_type must be 'delta' or 'full'")
        
        # Fetch data into a pandas DataFrame
        df = pd.read_sql(query, conn)
        print(f"Retrieved {len(df)} rows.")

        # Save to CSV
        cfg = get_config()
        base_folder = "output/current_students"
        os.makedirs(base_folder, exist_ok=True)

        # Timestamp
        timestamp = datetime.now().strftime("%b%d%Y-%H%M%S")
        load_type_lower = load_type.lower()
        filename = f"current_students_{load_type_lower}_{timestamp}.csv"

        output_path = os.path.join(base_folder, filename)

        # Save to CSV
        save_to_csv(df, output_path)

    except Exception as e:
        print("Error during ETL:", e)
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
