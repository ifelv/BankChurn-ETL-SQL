/*==========================================================
03_Dim_Reference_Loads
============================================================*/

/*==========================================================
Loading Gender into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Person_Gender
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @Inserted INT = 0;
    DECLARE @Rejected INT = 0;

    INSERT INTO ETLLog.RunStep (
        Run_ID,
        Step_Name,
        Target_Table,
        Step_Status,
        Start_Time
    )
    VALUES (
        @RunID,
        'Load Gender Reference',
        'Person.Gender',
        'STARTED',
        SYSUTCDATETIME()
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY
        INSERT INTO ETLLog.RowReject (
            RunStep_ID,
            Target_Table,
            Reject_Reason,
            Reject_Payload,
            Reject_Date
        )
        SELECT DISTINCT
            @RunStepID,
            'Person.Gender',
            'Invalid Gender_Code',
            s.Gender_Code,
            SYSUTCDATETIME()
        FROM Staging.Staging_Customer s
        WHERE
            s.Gender_Code IS NOT NULL
            AND s.Gender_Code NOT IN ('M', 'F');

        SET @Rejected += @@ROWCOUNT;

        BEGIN TRAN;

            INSERT INTO Person.Gender (
                Gender_Code,
                Gender_Desc,
                Load_Date
            )
            SELECT
                v.Gender_Code,
                CASE v.Gender_Code
                    WHEN 'M' THEN 'Male'
                    WHEN 'F' THEN 'Female'
                END AS Gender_Desc,
                v.Load_Date
            FROM (
                SELECT DISTINCT Gender_Code, Load_Date
                FROM Staging.Staging_Customer
                WHERE Gender_Code IN ('M', 'F')
            ) v
            WHERE NOT EXISTS (
                SELECT 1
                FROM Person.Gender g
                WHERE g.Gender_Code = v.Gender_Code
            );

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Inserted = @Inserted,
            Rows_Rejected = @Rejected,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;

        UPDATE ETLLog.RunStep
        SET
            Step_Status = 'FAILED',
            ErrorMessage = ERROR_MESSAGE(),
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        THROW;
    END CATCH;
END;
GO

/*==========================================================
Loading EducationLevel into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Person_EducationLevel
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @Inserted INT = 0;

    INSERT INTO ETLLog.RunStep (
        Run_ID,
        Step_Name,
        Target_Table,
        Step_Status
    )
    VALUES (
        @RunID,
        'Load EducationLevel Reference',
        'Person.EducationLevel',
        'STARTED'
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY

        BEGIN TRAN;

            INSERT INTO Person.EducationLevel (Education_Level, Load_Date)
            SELECT DISTINCT
                s.Education_Level,
                s.Load_Date
            FROM Staging.Staging_Customer s
            WHERE s.Education_Level IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM Person.EducationLevel e
                        WHERE e.Education_Level = s.Education_Level
                    );

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Inserted = @Inserted,
            Rows_Rejected = 0,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;

        UPDATE ETLLog.RunStep
        SET
            Step_Status = 'FAILED',
            ErrorMessage = ERROR_MESSAGE(),
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        THROW;
    END CATCH;
END;
GO

/*==========================================================
Loading MaritalStatus into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Person_MaritalStatus
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @Inserted INT = 0;

    INSERT INTO ETLLog.RunStep (
        Run_ID,
        Step_Name,
        Target_Table,
        Step_Status
    )
    VALUES (
        @RunID,
        'Load MaritalStatus Reference',
        'Person.MaritalStatus',
        'STARTED'
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

            INSERT INTO Person.MaritalStatus (Marital_Status, Load_Date)
            SELECT DISTINCT
                s.Marital_Status,
                s.Load_Date
            FROM Staging.Staging_Customer s
            WHERE s.Marital_Status IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM Person.MaritalStatus m
                        WHERE m.Marital_Status = s.Marital_Status
                    );

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Inserted = @Inserted,
            Rows_Rejected = 0,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;

        UPDATE ETLLog.RunStep
        SET
            Step_Status = 'FAILED',
            ErrorMessage = ERROR_MESSAGE(),
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        THROW;
    END CATCH;
END;
GO

/*==========================================================
Loading CardCategory into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Bank_CardCategory
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @Inserted INT = 0;

    INSERT INTO ETLLog.RunStep (
        Run_ID,
        Step_Name,
        Target_Table,
        Step_Status
    )
    VALUES (
        @RunID,
        'Load CardCategory Reference',
        'Bank.CardCategory',
        'STARTED'
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRAN;

            INSERT INTO Bank.CardCategory (Card_Type, Load_Date)
            SELECT DISTINCT
                s.Card_Type,
                s.Load_Date
            FROM Staging.Staging_Customer s
            WHERE
                s.Card_Type IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM Bank.CardCategory c
                    WHERE c.Card_Type = s.Card_Type
                );

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Inserted = @Inserted,
            Rows_Rejected = 0,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK;

        UPDATE ETLLog.RunStep
        SET
            Step_Status = 'FAILED',
            ErrorMessage = ERROR_MESSAGE(),
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        THROW;
    END CATCH;
END;
GO

