/*==========================================================
01_ETLLog_Procedures
============================================================*/

CREATE OR ALTER PROCEDURE ETLLog.usp_RunHeaderStart
    @JobName NVARCHAR(MAX),
    @SourceFileName NVARCHAR(MAX),
    @RunID BIGINT OUTPUT

AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ETLLog.RunHeader (
        Job_Name,
        Source_File_Name,
        Run_Status,
        Start_Time
    )
    VALUES (
        @JobName,
        @SourceFileName,
        'STARTED',
        SYSUTCDATETIME()
    );

    SET @RunID = SCOPE_IDENTITY();
END;


/*==========================================================
Finalizing ETL Run
============================================================*/

CREATE OR ALTER PROCEDURE ETLLog.usp_FinalizeRun
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FinalStatus SYSNAME;

    IF EXISTS (
        SELECT 1
        FROM ETLLog.RunStep
        WHERE Run_ID = @RunID
          AND Step_Status = 'FAILED'
    )
    BEGIN
        SET @FinalStatus = 'FAILED';
    END
    ELSE
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM ETLLog.RunStep
            WHERE Run_ID = @RunID
              AND Rows_Rejected > 0
        )
        BEGIN
            SET @FinalStatus = 'PARTIAL';
        END
        ELSE
        BEGIN
            SET @FinalStatus = 'SUCCESS';
        END
    END

    UPDATE ETLLog.RunHeader
    SET
        Run_Status = @FinalStatus,
        End_Time = SYSUTCDATETIME()
    WHERE Run_ID = @RunID;
END;
GO



