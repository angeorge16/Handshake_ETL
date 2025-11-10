import pandas as pd
import pyodbc
from datetime import datetime
import os
from src.db_connection import get_connection, get_config
from src.utils import save_to_csv

def extract_alumni():
    """
    Executes stored procedure dbo.sp_run_handshk_alumni_load and saves output to CSV.
    """

    print(f"Extracting alumni students...")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Step 1: Execute stored procedure
        cursor.execute("EXEC dbo.sp_run_handshk_alumni_load")
        print("Stored procedure executed successfully.")

        # Step 2: Drain all remaining result sets
        while cursor.nextset():
            pass

        # Step 3: Get current term code from DATAMART
        cursor.execute("SELECT TOP 1 TERM_CD FROM [DATAMART].[DBO].[DM_PREV_TERM]")
        prev_term_cd = cursor.fetchone()[0]

        # Step 4: Select alumni data
        select_query = """
        SELECT 
        [email_address],
        [username],
        [auth_identifier],
        [card_id],
        [first_name],
        [last_name],
        [middle_name],
        [preferred_name],
        [school_year_name],
        [primary_education:education_level_name],
        [primary_education:cumulative_gpa],
        [primary_education:department_gpa],
        [primary_education:primary_major_name],
        [primary_education:major_names],
        [primary_education:minor_names],
        [primary_education:college_name],
        [primary_education:start_date],
        [primary_education:end_date],
        [primary_education:currently_attending],
        [campus_name],
        [opt_cpt_eligible],
        [ethnicity],
        [gender],
        [disabled],
        [work_study_eligible],
        [system_label_names],
        [mobile_number],
        [assigned_to_email_address],
        [hometown_location_attributes:name],
        [athlete],
        [first_generation],
        [veteran],
        [eu_gdpr_subject]
        FROM dbo.handshk_alumni_master
        WHERE term_cd =  220255
        """
        df = pd.read_sql(select_query, conn)
        print(f"Retrieved {len(df)} rows.")

        # Step 5: Save to CSV
        save_to_csv(df, category="alumni", extra_info=prev_term_cd)

    except Exception as e:
        print("Error during ETL:", e)

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")
