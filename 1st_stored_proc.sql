USE [DATAMART];
GO

IF OBJECT_ID('[DBO].[SP_PROCESS_HANDSHK_ALUMNI]') IS NOT NULL
    DROP PROCEDURE [DBO].[SP_PROCESS_HANDSHK_ALUMNI];
GO

CREATE OR ALTER PROCEDURE [DBO].[SP_PROCESS_HANDSHK_ALUMNI]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CURR_TERM_CD VARCHAR(6);
    DECLARE @PREV_TERM_CD VARCHAR(6);

    SELECT TOP 1 @CURR_TERM_CD = [TERM_CD]
    FROM [DATAMART].[DBO].[DM_CUR_TERM];

    SELECT TOP 1 @PREV_TERM_CD = [TERM_CD]
    FROM [DATAMART].[DBO].[DM_PREV_TERM];

    PRINT '--------------------------------------------------';
    PRINT 'Processing HANDSHAKE Alumni for Previous Term...';
    PRINT 'Current Term: ' + ISNULL(@CURR_TERM_CD, 'N/A');
    PRINT 'Previous Term: ' + ISNULL(@PREV_TERM_CD, 'N/A');
    PRINT '--------------------------------------------------';

    ----------------------------------------------------------------------
    -- [1] Currently Registered Students (for filtering out)
    ----------------------------------------------------------------------
    IF OBJECT_ID('[DATAMART].[DBO].[HANDSHK_ALUM_CUR]') IS NOT NULL
        DROP TABLE [DATAMART].[DBO].[HANDSHK_ALUM_CUR];

    SELECT DISTINCT [Z].[EDW_PERS_ID]
    INTO [DATAMART].[DBO].[HANDSHK_ALUM_CUR]
    FROM [DATAMART].[DBO].[DMG_CENSUS_DATA] AS [Z]
    WHERE [Z].[TERM_CD] = @CURR_TERM_CD;

    ----------------------------------------------------------------------
    -- [2] GPA
    ----------------------------------------------------------------------
    IF OBJECT_ID('[DATAMART].[DBO].[HANDSHK_ALUM_GPA]') IS NOT NULL
        DROP TABLE [DATAMART].[DBO].[HANDSHK_ALUM_GPA];

    SELECT DISTINCT 
        [D].[EDW_PERS_ID], 
        [D].[GPA_LEVEL_CD] AS [LEVEL_CD],
        MAX(CONVERT(DECIMAL(10,2), [D].[LEVEL_GPA])) AS [LEVEL_GPA]
    INTO [DATAMART].[DBO].[HANDSHK_ALUM_GPA]
    FROM [DSPROD1]..[EDW].[T_STUDENT_AH_LEVEL_GPA_HIST] AS [D]
    WHERE [D].[STUDENT_LEVEL_GPA_CUR_INFO_IND] = 'Y'
      AND [D].[LEVEL_GPA_TYPE_IND] = 'O'
      AND [D].[DEPT_CD] != '2NON'
      AND [D].[VPDI_CD] = '2UIC'
    GROUP BY [D].[EDW_PERS_ID], [D].[GPA_LEVEL_CD];

    ----------------------------------------------------------------------
    -- [3] REGISTRATION DATA (Previous Term)
    ----------------------------------------------------------------------
    IF OBJECT_ID('[DATAMART].[DBO].[HANDSHK_ALUM_REGISTRATION]') IS NOT NULL
        DROP TABLE [DATAMART].[DBO].[HANDSHK_ALUM_REGISTRATION];

    SELECT  
        [T].[EDW_PERS_ID],
        [T].[TERM_CD],
        [T].[STUDENT_CURR_1_MAJOR_NAME],
        [T].[STUDENT_CURR_1_MAJOR_2_NAME],
        [T].[STUDENT_CURR_2_MAJOR_1_NAME],
        [T].[STUDENT_CURR_2_MAJOR_2_NAME],
        [T].[STUDENT_CURR_1_MINOR_1_NAME], 
        [T].[STUDENT_CURR_1_MINOR_2_NAME],
        [T].[STUDENT_CURR_2_MINOR_1_NAME], 
        [T].[STUDENT_CURR_2_MINOR_2_NAME],
        [T].[ACAD_COLL_NAME],
        [T].[STUDENT_CURR_1_DEG_CD], 
        [T].[STUDENT_LEVEL_CD]
    INTO [DATAMART].[DBO].[HANDSHK_ALUM_REGISTRATION]
    FROM [DSPROD1]..[EDW].[T_STUDENT_TERM] AS [T]
    WHERE [T].[TERM_CD] = @PREV_TERM_CD;

    ----------------------------------------------------------------------
    -- [4] NETID
    ----------------------------------------------------------------------
    IF OBJECT_ID('[DATAMART].[DBO].[HANDSHK_ALUM_NETID]') IS NOT NULL
        DROP TABLE [DATAMART].[DBO].[HANDSHK_ALUM_NETID];

    SELECT DISTINCT 
        [T].[EDW_PERS_ID],
        [T].[NETID_PRINCIPAL]
    INTO [DATAMART].[DBO].[HANDSHK_ALUM_NETID]
    FROM [DSPROD1]..[EDW].[T_NETID] AS [T]
    WHERE [T].[ENTRP_ID_IND] = 'N'
      AND [T].[NETID_DOMAIN] = 'uic.edu';

    ----------------------------------------------------------------------
    -- [5] ALUMNI (Graduated in Previous Term)
    ----------------------------------------------------------------------
    IF OBJECT_ID('[DATAMART].[DBO].[HANDSHK_ALUM]') IS NOT NULL
        DROP TABLE [DATAMART].[DBO].[HANDSHK_ALUM];

    SELECT DISTINCT 
        [AL].[EDW_PERS_ID],
        [AL].[GRAD_TERM_CD] AS [TERM_CD],
        [AL].[STUDENT_LEVEL_CD]
    INTO [DATAMART].[DBO].[HANDSHK_ALUM]
    FROM [DSPROD1]..[EDW].[T_STUDENT_AH_DEG] AS [AL]
    WHERE [AL].[DEG_STATUS_CD] = 'AW'
      AND [AL].[GRAD_TERM_CD] = @PREV_TERM_CD;

    PRINT '--------------------------------------------------';
    PRINT 'Step 1 Completed — Staging source data ready for load.';
    PRINT '--------------------------------------------------';
END;
GO
