USE [DATAMART];
GO

IF OBJECT_ID('[DBO].[SP_RUN_HANDSHK_ALUMNI_LOAD]') IS NOT NULL
    DROP PROCEDURE [DBO].[SP_RUN_HANDSHK_ALUMNI_LOAD];
GO

CREATE OR ALTER PROCEDURE [DBO].[SP_RUN_HANDSHK_ALUMNI_LOAD]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CURR_TERM_CD VARCHAR(6);
    DECLARE @PREV_TERM_CD VARCHAR(6);
    DECLARE @AlreadyProcessed BIT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowCount INT = 0;
    DECLARE @Message NVARCHAR(1000) = N'';

    -- Get current and previous term
    SELECT TOP 1 @CURR_TERM_CD = [TERM_CD] 
    FROM [DATAMART].[DBO].[DM_CUR_TERM];

    SELECT TOP 1 @PREV_TERM_CD = [TERM_CD] 
    FROM [DATAMART].[DBO].[DM_PREV_TERM];

    PRINT '--------------------------------------------------';
    PRINT 'HANDSHAKE Alumni Processing Controller';
    PRINT 'Current Term:  ' + ISNULL(@CURR_TERM_CD, 'N/A');
    PRINT 'Previous Term: ' + ISNULL(@PREV_TERM_CD, 'N/A');
    PRINT '--------------------------------------------------';

    ----------------------------------------------------------------------
    -- 1. Check if previous term is already processed
    ----------------------------------------------------------------------
    IF EXISTS (
        SELECT 1 
        FROM [DATAMART].[DBO].[HANDSHK_ALUMNI_LOAD_LOG]
        WHERE [PREV_TERM_CD] = @PREV_TERM_CD
    )
    BEGIN
        SET @AlreadyProcessed = 1;
        PRINT 'Data for term ' + @PREV_TERM_CD + ' already exists in HANDSHK_ALUMNI_LOAD_LOG.';
        PRINT 'Skipping data load...';
    END
    ELSE IF EXISTS (
        SELECT 1 
        FROM [DATAMART].[DBO].[HANDSHK_ALUMNI_MASTER]
        WHERE [TERM_CD] = @PREV_TERM_CD
    )
    BEGIN
        SET @AlreadyProcessed = 1;
        PRINT 'Data for term ' + @PREV_TERM_CD + ' already exists in HANDSHK_ALUMNI_MASTER.';
        PRINT 'Skipping data load...';
    END

    ----------------------------------------------------------------------
    -- 2. Run data process and load only if not processed yet
    ----------------------------------------------------------------------
    IF @AlreadyProcessed = 0
    BEGIN
        PRINT 'Starting HANDSHAKE alumni data process for term ' + @PREV_TERM_CD + '...';

        BEGIN TRY
            -- Step 1: Process source data
            EXEC [DATAMART].[DBO].[SP_PROCESS_HANDSHK_ALUMNI];

            -- Step 2: Load to master table
            EXEC [DATAMART].[DBO].[SP_LOAD_HANDSHK_ALUMNI_MASTER];

            -- Get number of rows inserted into master table for this term
            SELECT @RowCount = COUNT(*) 
            FROM [DATAMART].[DBO].[HANDSHK_ALUMNI_MASTER]
            WHERE [TERM_CD] = @PREV_TERM_CD;

            SET @Message = N'Processing completed successfully.';

            -- Log success
            INSERT INTO [DATAMART].[DBO].[HANDSHK_ALUMNI_LOAD_LOG]
            (
                [PREV_TERM_CD],[CURR_TERM_CD],
                [EXECUTION_DATE],[ROW_COUNT],
                [STATUS],[MESSAGE]
            )
            VALUES
            (
                @PREV_TERM_CD,
                @CURR_TERM_CD,
                GETDATE(),
                @RowCount,
                'SUCCESS',
                @Message
            );

            PRINT 'Processing completed successfully for term ' + @PREV_TERM_CD + '.';
            PRINT 'Rows inserted: ' + CAST(@RowCount AS VARCHAR(10));
        END TRY
        BEGIN CATCH
            SET @Message = ERROR_MESSAGE();

            -- Log failure
            INSERT INTO [DATAMART].[DBO].[HANDSHK_ALUMNI_LOAD_LOG]
            (
                [PREV_TERM_CD],
                [CURR_TERM_CD],
                [EXECUTION_DATE],
                [ROW_COUNT],
                [STATUS],
                [MESSAGE]
            )
            VALUES
            (
                @PREV_TERM_CD,
                @CURR_TERM_CD,
                GETDATE(),
                0,
                'FAILED',
                @Message
            );

            PRINT 'Error during alumni master load: ' + @Message;
        END CATCH
    END
    ELSE
    BEGIN
        PRINT 'No action taken — data already processed for ' + @PREV_TERM_CD + '.';
    END
END;
GO
