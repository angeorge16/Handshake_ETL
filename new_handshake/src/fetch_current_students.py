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

    load_type_lower = load_type.lower()
    if load_type_lower not in ['delta', 'full']:
        raise ValueError("load_type must be 'delta' or 'full'")

    # Connect to SQL Server
    conn = get_connection()
    cursor = conn.cursor()

   
    try:
         # Step 1: Execute stored procedure
        cursor.execute(f"EXEC dbo.sp_get_delta_from_source '{load_type_lower}'")
        print(f"Stored procedure executed successfully ({load_type_lower})")

        # Step 2: Drain all remaining result sets (VERY important!)
        while cursor.nextset():
            pass

        cols = [
        "email_address", "username", "auth_identifier", "card_id", "first_name", "last_name",
        "middle_name", "preferred_name", "school_year_name",
        "primary_education_education_level_name", "primary_education_cumulative_gpa",
        "primary_education_department_gpa", "primary_education_primary_major_name",
        "primary_education_major_names", "primary_education_minor_names",
        "primary_education_college_name", "primary_education_start_date",
        "primary_education_end_date", "primary_education_currently_attending",
        "campus_name", "opt_cpt_eligible", "ethnicity", "gender", "disabled",
        "work_study_eligible", "system_label_names", "mobile_number",
        "assigned_to_email_address", "hometown_location_name", "athlete",
        "first_generation", "veteran", "eu_gdpr_subject"
        ]

        # Fetch results from appropriate table
        if load_type_lower == 'delta':
            #select_query = "SELECT * FROM dbo.HANDSHK_CURRENT_STUDENTS_DELTA"  # replace with actual table
            select_query = f"SELECT {', '.join(f'[{c}]' for c in cols)} FROM dbo.HANDSHK_CURRENT_STUDENTS_DELTA"

        else:
            #select_query = "SELECT * FROM dbo.HANDSHK_CURRENT_STUDENTS_PREV_RUN"   # replace with actual table
            select_query = f"SELECT {', '.join(f'[{c}]' for c in cols)} FROM dbo.HANDSHK_CURRENT_STUDENTS_PREV_RUN"


        df = pd.read_sql(select_query, conn)
        print(f"Retrieved {len(df)} rows.")

        # Save to CSV
        save_to_csv(df, category="current_students", extra_info=load_type_lower)

    except Exception as e:
        print("Error during ETL:", e)
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
