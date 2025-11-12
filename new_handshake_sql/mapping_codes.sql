USE [DATAMART];
GO

-- Drop existing procedure if it exists
IF OBJECT_ID('dbo.get_new_mapping_codes', 'P') IS NOT NULL
    DROP PROCEDURE dbo.get_new_mapping_codes;
GO

-- Create the procedure
CREATE PROCEDURE dbo.get_new_mapping_codes
AS
BEGIN
    SET NOCOUNT ON;

    WITH DEGS AS (
        SELECT DISTINCT 
            termf.STUDENT_CURR_1_DEG_CD AS [STUDENT_DEGREE_CD], 
            termf.STUDENT_CURR_1_DEG_NAME AS [STUDENT_DEGREE_NAME], 
            termf.CALC_CLS_DESC, 
            termf.STUDENT_LEVEL_CD
        FROM maxientDB.dbo.CURRENT_TERM termf
        
        UNION
        
        SELECT DISTINCT 
            termf.STUDENT_CURR_1_DEG_CD, 
            termf.STUDENT_CURR_1_DEG_NAME, 
            termf.CALC_CLS_DESC, 
            termf.STUDENT_LEVEL_CD
        FROM maxientDB.dbo.CURRENT_TERM_F termf
    )
    SELECT *
    FROM DEGS
    WHERE [STUDENT_DEGREE_CD] NOT IN (
        SELECT DEG_CD 
        FROM handshakedb.dbo.Hndshk_Edu_Lvl_Map
    )
    ORDER BY [STUDENT_DEGREE_NAME];
END;
GO



EXEC dbo.get_new_mapping_codes