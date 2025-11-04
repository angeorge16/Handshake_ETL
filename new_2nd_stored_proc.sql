USE DATAMART;
GO

IF OBJECT_ID('DBO.SP_LOAD_HANDSHK_ALUMNI_MASTER') IS NOT NULL
    DROP PROCEDURE DBO.SP_LOAD_HANDSHK_ALUMNI_MASTER;
GO

CREATE OR ALTER PROCEDURE DBO.SP_LOAD_HANDSHK_ALUMNI_MASTER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PREV_TERM_CD VARCHAR(6);
    DECLARE @CURR_TERM_CD VARCHAR(6);
    DECLARE @RowCount INT = 0;
    DECLARE @Message NVARCHAR(1000) = '';

    SELECT TOP 1 @CURR_TERM_CD = TERM_CD FROM DATAMART.DBO.DM_CUR_TERM;
    SELECT TOP 1 @PREV_TERM_CD = TERM_CD FROM DATAMART.DBO.DM_PREV_TERM;

    PRINT '--------------------------------------------------';
    PRINT 'Loading Alumni Master Table...';
    PRINT 'Current Term:  ' + ISNULL(@CURR_TERM_CD, 'N/A');
    PRINT 'Previous Term: ' + ISNULL(@PREV_TERM_CD, 'N/A');
    PRINT '--------------------------------------------------';

    BEGIN TRY
        ----------------------------------------------------------------------
        -- MAIN LOAD QUERY
        ----------------------------------------------------------------------
        INSERT INTO DATAMART.DBO.HANDSHK_ALUMNI_MASTER (
            TERM_CD, EDW_PERS_ID, email_address, username, auth_identifier, 
            card_id, first_name, last_name, middle_name, preferred_name, 
            school_year_name, [primary_education:education_level_name], 
            [primary_education:cumulative_gpa], [primary_education:department_gpa],
            [primary_education:primary_major_name], [primary_education:major_names], 
            [primary_education:minor_names], [primary_education:college_name],
            [primary_education:start_date], [primary_education:end_date],
            [primary_education:currently_attending], campus_name, opt_cpt_eligible, 
            ethnicity, gender, disabled, work_study_eligible, system_label_names, 
            mobile_number, assigned_to_email_address, [hometown_location_attributes:name],
            athlete, first_generation, veteran, eu_gdpr_subject
        )
        SELECT DISTINCT
            @PREV_TERM_CD AS TERM_CD,
            ALUMNI.EDW_PERS_ID,
            CONCAT(netf.NETID_PRINCIPAL,'@uic.edu') AS email_address,
            UIN.UIN AS username,
            CONCAT(netf.NETID_PRINCIPAL,'@uic.edu') AS auth_identifier,
            '' AS card_id,
            UIN.PERS_FNAME AS first_name,
            UIN.PERS_LNAME AS last_name,
            UIN.PERS_MNAME AS middle_name,
            UIN.PERS_PREFERRED_FNAME AS preferred_name,
            'Alumni' AS school_year_name,
            CASE WHEN edu.Handshake_Name = 'NONE' THEN '' ELSE edu.Handshake_Name END AS [primary_education:education_level_name],
            GPA.LEVEL_GPA AS [primary_education:cumulative_gpa],
            0.0 AS [primary_education:department_gpa],
            termf.STUDENT_CURR_1_MAJOR_NAME AS [primary_education:primary_major_name],
            handshakeDB.dbo.fn_ConcatMajors_v2(termf.STUDENT_CURR_1_MAJOR_NAME, termf.STUDENT_CURR_1_MAJOR_2_NAME, termf.STUDENT_CURR_2_MAJOR_1_NAME, termf.STUDENT_CURR_2_MAJOR_2_NAME) AS [primary_education:major_names],
            handshakeDB.dbo.fn_ConcatMajors_v2(termf.STUDENT_CURR_1_MINOR_1_NAME, termf.STUDENT_CURR_1_MINOR_2_NAME, termf.STUDENT_CURR_2_MINOR_1_NAME, termf.STUDENT_CURR_2_MINOR_2_NAME) AS [primary_education:minor_names],
            termf.ACAD_COLL_NAME AS [primary_education:college_name],
            '' AS [primary_education:start_date],
            '' AS [primary_education:end_date],
            '' AS [primary_education:currently_attending],
            '' AS campus_name,
            '' AS opt_cpt_eligible,
            CASE 
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'Y' THEN 'Multi-Race'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND HISPANIC_ETH_IND = 'Y' THEN 'Hispanic'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND AIAN_RACE_IND = 'Y' THEN 'AIAN'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND ASIAN_RACE_IND = 'Y' THEN 'Asian'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND BLACK_RACE_IND = 'Y' THEN 'Black/African American'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND NHPI_RACE_IND = 'Y' THEN 'NHPI'
                WHEN UIN.RACE_ETH_RPT_DESC = 'International' AND UIN.MULT_RACE_IND = 'N' AND WHITE_RACE_IND = 'Y' THEN 'White'
                ELSE UIN.RACE_ETH_RPT_DESC
            END AS ethnicity,
            UIN.SEX_DESC AS gender,
            '' AS disabled,
            '' AS work_study_eligible,
            '' AS system_label_names,
            '' AS mobile_number,
            '' AS assigned_to_email_address,
            '' AS [hometown_location_attributes:name],
            '' AS athlete,
            '' AS first_generation,
            '' AS veteran,
            '' AS eu_gdpr_subject
        FROM [DSPROD1]..[EDW].[V_PERS_HIST_PRR_FULL_LTD] UIN
        INNER JOIN [DATAMART].[DBO].[HANDSHK_ALUM1] ALUMNI
            ON ALUMNI.EDW_PERS_ID = UIN.EDW_PERS_ID
        LEFT JOIN [DATAMART].[DBO].[HANDSHK_ALUM_NETID1] netf
            ON netf.EDW_PERS_ID = UIN.EDW_PERS_ID
        LEFT JOIN [DATAMART].[DBO].[HANDSHK_ALUM_REG_TST1] termf
            ON termf.EDW_PERS_ID = ALUMNI.EDW_PERS_ID
            AND termf.STUDENT_LEVEL_CD = ALUMNI.STUDENT_LEVEL_CD
        LEFT JOIN [DATAMART].[DBO].[HANDSHK_ALUM_GPA1] GPA
            ON GPA.EDW_PERS_ID = UIN.EDW_PERS_ID
            AND GPA.LEVEL_CD = ALUMNI.STUDENT_LEVEL_CD
        JOIN [handshakeDB].[dbo].[Hndshk_Edu_Lvl_Map] edu
            ON edu.DEG_CD = termf.STUDENT_CURR_1_DEG_CD
        WHERE UIN.PERS_CUR_INFO_IND = 'Y'
          AND ALUMNI.EDW_PERS_ID NOT IN (
                SELECT EDW_PERS_ID FROM [DATAMART].[DBO].[HANDSHK_ALUM_CUR] );

        ----------------------------------------------------------------------
        -- LOG SUCCESS
        ----------------------------------------------------------------------
        SET @RowCount = @@ROWCOUNT;
        SET @Message = 'Alumni master load completed successfully.';

        INSERT INTO DATAMART.DBO.HANDSHK_ALUMNI_LOAD_LOG (
            PREV_TERM_CD, CURR_TERM_CD, ROW_COUNT, STATUS, MESSAGE
        )
        VALUES (@PREV_TERM_CD, @CURR_TERM_CD, @RowCount, 'SUCCESS', @Message);

        PRINT '--------------------------------------------------';
        PRINT 'Alumni master table loaded successfully for term: ' + @PREV_TERM_CD;
        PRINT 'Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT '--------------------------------------------------';

    END TRY
    BEGIN CATCH
        SET @Message = ERROR_MESSAGE();

        INSERT INTO DATAMART.DBO.HANDSHK_ALUMNI_LOAD_LOG (
            PREV_TERM_CD, CURR_TERM_CD, ROW_COUNT, STATUS, MESSAGE
        )
        VALUES (@PREV_TERM_CD, @CURR_TERM_CD, 0, 'FAILED', @Message);

        PRINT '--------------------------------------------------';
        PRINT 'Error during alumni master load: ' + @Message;
        PRINT '--------------------------------------------------';
    END CATCH
END;
GO
