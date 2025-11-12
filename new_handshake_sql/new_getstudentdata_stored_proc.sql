USE [DATAMART];
GO

IF OBJECT_ID('dbo.sp_get_delta_from_source', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_delta_from_source;
GO

CREATE OR ALTER PROCEDURE dbo.sp_get_delta_from_source
    @mode VARCHAR(10) = 'delta'  -- options: 'delta' or 'full'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @today DATE = CAST(GETDATE() AS DATE);
    DECLARE @last_run_date DATE;

    -- get last execution date
    SELECT TOP 1 @last_run_date = last_execution_date
    FROM [dbo].[HANDSHK_CURRENT_STUDENTS_RUN_LOG]
    WHERE run_type IN ('full','delta')
    ORDER BY run_id DESC;

    -- CASE 1: If user asked only for delta OR we already ran today, return delta only
    IF @last_run_date = @today
    BEGIN
	    IF @mode = 'delta'
		    RETURN;
		ELSE IF @mode = 'full'
		    RETURN;
    END

    -- 1) Create temp table for today's run
    IF OBJECT_ID('tempdb..#current_run') IS NOT NULL
        DROP TABLE #current_run;

    WITH unioned_data AS (
        -- Your UNION query for current and future students (unchanged)
        SELECT 
            netf.NETID_PRINCIPAL + '@uic.edu' AS email_address,
            sf.UIN AS username,
            netf.NETID_PRINCIPAL + '@uic.edu' AS auth_identifier,
            '' AS card_id,
            sf.PERS_FNAME AS first_name,
            sf.PERS_LNAME AS last_name,
            sf.PERS_MNAME AS middle_name,
            sf.PERS_PREFERRED_FNAME AS preferred_name,
            CASE
                WHEN edu.Handshake_Name = 'Doctorate' THEN 'Doctorate'
                WHEN schl.Handshake_Name IS NOT NULL THEN schl.Handshake_Name
                WHEN edu.Handshake_Name IS NOT NULL THEN edu.Handshake_Name
                ELSE ''
            END AS school_year_name,
            CASE 
                WHEN edu.Handshake_Name = 'NONE' OR edu.Handshake_Name IS NULL THEN ''
                ELSE edu.Handshake_Name
            END AS primary_education_education_level_name,
            ISNULL(CONVERT(DECIMAL(10,2), gpa.LEVEL_GPA), 0) AS primary_education_cumulative_gpa,
            0.0 AS primary_education_department_gpa,
            termc.STUDENT_CURR_1_MAJOR_NAME AS primary_education_primary_major_name,
            handshakeDB.dbo.fn_ConcatMajors_v2(termc.STUDENT_CURR_1_MAJOR_NAME, termc.STUDENT_CURR_1_MAJOR_2_NAME, termc.STUDENT_CURR_2_MAJOR_1_NAME, termc.STUDENT_CURR_2_MAJOR_2_NAME) AS primary_education_major_names,
            handshakeDB.dbo.fn_ConcatMajors_v2(termc.STUDENT_CURR_1_MINOR_1_NAME, termc.STUDENT_CURR_1_MINOR_2_NAME, termc.STUDENT_CURR_2_MINOR_1_NAME, termc.STUDENT_CURR_2_MINOR_2_NAME) AS primary_education_minor_names,
            termc.ACAD_COLL_NAME AS primary_education_college_name,
            '' AS primary_education_start_date,
            '' AS primary_education_end_date,
            '' AS primary_education_currently_attending,
            '' AS campus_name,
            '' AS opt_cpt_eligible,
            CASE 
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'Y' THEN 'Multi-Race'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND HISPANIC_ETH_IND = 'Y' THEN 'Hispanic'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND AIAN_RACE_IND = 'Y' THEN 'AIAN'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND ASIAN_RACE_IND = 'Y' THEN 'Asian'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND BLACK_RACE_IND = 'Y' THEN 'Black/African American'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND NHPI_RACE_IND = 'Y' THEN 'NHPI'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND WHITE_RACE_IND = 'Y' THEN 'White'
                ELSE RACE_ETH_RPT_DESC
            END AS ethnicity,
            sf.SEX_DESC AS gender,
            '' AS disabled,
            '' AS work_study_eligible,
            '' AS system_label_names,
            '' AS mobile_number,
            '' AS assigned_to_email_address,
            '' AS hometown_location_name,
            '' AS athlete,
            '' AS first_generation,
            '' AS veteran,
            '' AS eu_gdpr_subject
        FROM maxientDB.dbo.PERS_HIST_CURRENT_TERMCD sf
        JOIN maxientDB.dbo.CURRENT_NETID netf ON sf.EDW_PERS_ID = netf.EDW_PERS_ID
        INNER JOIN DATAMART.dbo.HS_CUR_STUDENTS termc ON sf.EDW_PERS_ID = termc.EDW_PERS_ID
        LEFT JOIN DATAMART.dbo.CUR_LEVEL_GPA gpa ON sf.EDW_PERS_ID = gpa.EDW_PERS_ID AND gpa.GPA_LEVEL_CD = termc.STUDENT_LEVEL_CD
        LEFT JOIN handshakeDB.dbo.Hndshk_Edu_Lvl_Map edu ON edu.DEG_CD = termc.STUDENT_CURR_1_DEG_CD
        LEFT JOIN handshakeDB.dbo.Hndshk_Schl_Yr_Map schl ON schl.Calc_Schl_Yr = termc.CALC_CLS_DESC
        WHERE netf.NETID_DOMAIN = 'uic.edu'
          AND sf.PERS_CONFIDENTIALITY_IND = 'N'
          AND schl.Handshake_Name != 'Non-Degree Seeking'
          AND edu.Handshake_Name != 'Non-Degree Seeking'
          AND termc.STUDENT_LEVEL_CD NOT IN ('NC','ND')

        UNION

        SELECT 
            netf.NETID_PRINCIPAL + '@uic.edu' AS email_address,
            sf.UIN AS username,
            netf.NETID_PRINCIPAL + '@uic.edu' AS auth_identifier,
            '' AS card_id,
            sf.PERS_FNAME AS first_name,
            sf.PERS_LNAME AS last_name,
            sf.PERS_MNAME AS middle_name,
            sf.PERS_PREFERRED_FNAME AS preferred_name,
            CASE
                WHEN edu.Handshake_Name = 'Doctorate' THEN 'Doctorate'
                WHEN schl.Handshake_Name IS NOT NULL THEN schl.Handshake_Name
                WHEN edu.Handshake_Name IS NOT NULL THEN edu.Handshake_Name
                ELSE ''
            END AS school_year_name,
            CASE 
                WHEN edu.Handshake_Name = 'NONE' OR edu.Handshake_Name IS NULL THEN ''
                ELSE edu.Handshake_Name
            END AS primary_education_education_level_name,
            ISNULL(CONVERT(DECIMAL(10,2), gpa.LEVEL_GPA), 0) AS primary_education_cumulative_gpa,
            0.0 AS primary_education_department_gpa,
            termf.STUDENT_CURR_1_MAJOR_NAME AS primary_education_primary_major_name,
            handshakeDB.dbo.fn_ConcatMajors_v2(termf.STUDENT_CURR_1_MAJOR_NAME, termf.STUDENT_CURR_1_MAJOR_2_NAME, termf.STUDENT_CURR_2_MAJOR_1_NAME, termf.STUDENT_CURR_2_MAJOR_2_NAME) AS primary_education_major_names,
            handshakeDB.dbo.fn_ConcatMajors_v2(termf.STUDENT_CURR_1_MINOR_1_NAME, termf.STUDENT_CURR_1_MINOR_2_NAME, termf.STUDENT_CURR_2_MINOR_1_NAME, termf.STUDENT_CURR_2_MINOR_2_NAME) AS primary_education_minor_names,
            termf.ACAD_COLL_NAME AS primary_education_college_name,
            '' AS primary_education_start_date,
            '' AS primary_education_end_date,
            '' AS primary_education_currently_attending,
            '' AS campus_name,
            '' AS opt_cpt_eligible,
            CASE 
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'Y' THEN 'Multi-Race'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND HISPANIC_ETH_IND = 'Y' THEN 'Hispanic'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND AIAN_RACE_IND = 'Y' THEN 'AIAN'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND ASIAN_RACE_IND = 'Y' THEN 'Asian'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND BLACK_RACE_IND = 'Y' THEN 'Black/African American'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND NHPI_RACE_IND = 'Y' THEN 'NHPI'
                WHEN RACE_ETH_RPT_DESC = 'International' AND MULT_RACE_IND = 'N' AND WHITE_RACE_IND = 'Y' THEN 'White'
                ELSE RACE_ETH_RPT_DESC
            END AS ethnicity,
            sf.SEX_DESC AS gender,
            '' AS disabled,
            '' AS work_study_eligible,
            '' AS system_label_names,
            '' AS mobile_number,
            '' AS assigned_to_email_address,
            '' AS hometown_location_name,
            '' AS athlete,
            '' AS first_generation,
            '' AS veteran,
            '' AS eu_gdpr_subject
        FROM maxientDB.dbo.PERS_HIST_CURRENT_TERMCD sf
        JOIN maxientDB.dbo.CURRENT_NETID netf ON sf.EDW_PERS_ID = netf.EDW_PERS_ID
        INNER JOIN DATAMART.dbo.HS_FUT_STUDENTS termf ON sf.EDW_PERS_ID = termf.EDW_PERS_ID
        LEFT JOIN DATAMART.dbo.FUT_LEVEL_GPA gpa ON sf.EDW_PERS_ID = gpa.EDW_PERS_ID AND gpa.GPA_LEVEL_CD = termf.STUDENT_LEVEL_CD
        LEFT JOIN handshakeDB.dbo.Hndshk_Edu_Lvl_Map edu ON edu.DEG_CD = termf.STUDENT_CURR_1_DEG_CD
        LEFT JOIN handshakeDB.dbo.Hndshk_Schl_Yr_Map schl ON schl.Calc_Schl_Yr = termf.CALC_CLS_DESC
        WHERE netf.NETID_DOMAIN = 'uic.edu'
          AND sf.PERS_CONFIDENTIALITY_IND = 'N'
          AND schl.Handshake_Name != 'Non-Degree Seeking'
          AND edu.Handshake_Name != 'Non-Degree Seeking'
          AND termf.STUDENT_LEVEL_CD NOT IN ('NC','ND')
    )
    SELECT *,
           CONVERT(VARCHAR(32), HASHBYTES('MD5', 
               COALESCE(email_address,'') + '|' +
               COALESCE(auth_identifier,'') + '|' +
               COALESCE(card_id,'') + '|' +
               COALESCE(first_name,'') + '|' +
               COALESCE(last_name,'') + '|' +
               COALESCE(middle_name,'') + '|' +
               COALESCE(preferred_name,'') + '|' +
               COALESCE(school_year_name,'') + '|' +
               COALESCE(primary_education_education_level_name,'') + '|' +
               CONVERT(VARCHAR, primary_education_cumulative_gpa) + '|' +
               CONVERT(VARCHAR, primary_education_department_gpa) + '|' +
               COALESCE(primary_education_primary_major_name,'') + '|' +
               COALESCE(primary_education_major_names,'') + '|' +
               COALESCE(primary_education_minor_names,'') + '|' +
               COALESCE(primary_education_college_name,'') + '|' +
               COALESCE(primary_education_start_date,'') + '|' +
               COALESCE(primary_education_end_date,'') + '|' +
               COALESCE(primary_education_currently_attending,'') + '|' +
               COALESCE(campus_name,'') + '|' +
               COALESCE(opt_cpt_eligible,'') + '|' +
               COALESCE(ethnicity,'') + '|' +
               COALESCE(gender,'') + '|' +
               COALESCE(disabled,'') + '|' +
               COALESCE(work_study_eligible,'') + '|' +
               COALESCE(system_label_names,'') + '|' +
               COALESCE(mobile_number,'') + '|' +
               COALESCE(assigned_to_email_address,'') + '|' +
               COALESCE(hometown_location_name,'') + '|' +
               COALESCE(athlete,'') + '|' +
               COALESCE(first_generation,'') + '|' +
               COALESCE(veteran,'') + '|' +
               COALESCE(eu_gdpr_subject,'')
           ), 2) AS row_hash
    INTO #current_run
    FROM unioned_data;

    -- 2) Insert new/updated rows
    TRUNCATE TABLE [dbo].[HANDSHK_CURRENT_STUDENTS_DELTA];

	INSERT INTO [dbo].[HANDSHK_CURRENT_STUDENTS_DELTA]
	SELECT 
    t.email_address,
	t.username,
    t.auth_identifier,
    t.card_id,
    t.first_name,
    t.last_name,
    t.middle_name,
    t.preferred_name,
    t.school_year_name,
    t.primary_education_education_level_name,
    t.primary_education_cumulative_gpa,
    t.primary_education_department_gpa,
    t.primary_education_primary_major_name,
    t.primary_education_major_names,
    t.primary_education_minor_names,
    t.primary_education_college_name,
    t.primary_education_start_date,
    t.primary_education_end_date,
    t.primary_education_currently_attending,
    t.campus_name,
    t.opt_cpt_eligible,
    t.ethnicity,
    t.gender,
    t.disabled,
    t.work_study_eligible,
    t.system_label_names,
    t.mobile_number,
    t.assigned_to_email_address,
    t.hometown_location_name,
    t.athlete,
    t.first_generation,
    t.veteran,
    t.eu_gdpr_subject
	FROM #current_run t
	LEFT JOIN [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN] y
		ON t.username = y.username
	WHERE y.username IS NULL
	   OR t.row_hash <> y.row_hash;

    DECLARE @newly_added_rows INT = (SELECT COUNT(*) FROM [dbo].[HANDSHK_CURRENT_STUDENTS_DELTA]);
    DECLARE @total_rows INT = (SELECT COUNT(*) FROM [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN]);

	-- 3) Backup and refresh previous run
    TRUNCATE TABLE [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN_BACKUP];
    INSERT INTO [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN_BACKUP]
    SELECT * FROM [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN];

    TRUNCATE TABLE [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN];
    INSERT INTO [dbo].[HANDSHK_CURRENT_STUDENTS_PREV_RUN]
    SELECT * FROM #current_run;

	-- 4) Log this run (with counts)
    INSERT INTO [dbo].[HANDSHK_CURRENT_STUDENTS_RUN_LOG] (last_execution_date, run_type, newly_added_rows, total_rows)
    VALUES (@today, @mode, @newly_added_rows, @total_rows);

END;
GO

exec dbo.sp_get_delta_from_source @mode = 'delta';