/*==========================================================
02_Staging_Load
============================================================*/
CREATE OR ALTER PROCEDURE Staging.usp_LoadCustomer
    @RunID BIGINT,
    @StepName NVARCHAR(MAX),
    @SourceFileName NVARCHAR(MAX),
    @FilePath NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @RowsInserted INT;
    DECLARE @DynamicSQL NVARCHAR(MAX);

    INSERT INTO ETLLog.RunStep (Run_ID, Step_Name, Target_Table, Start_Time, Step_Status)
    VALUES (@RunID, @StepName, 'Staging.Staging_Customer', SYSUTCDATETIME(), 'STARTED');

    SET @RunStepID = SCOPE_IDENTITY(); 

    BEGIN TRY

        TRUNCATE TABLE Staging.Staging_Customer;

        BEGIN TRAN;

        -- REFACTOR: Removed @json variable. Used direct CROSS APPLY for efficiency.
        SET @DynamicSQL = N'
        INSERT INTO Staging.Staging_Customer (
            Run_ID, CLIENTNUM, Attrition_Flag, Customer_Age, Gender_Code, 
            Dependent_count, Education_Level, Marital_Status, Income_Category, 
            Card_Type, Months_on_book, Total_Relationship_Count, Months_Inactive_12_mon, 
            Contacts_Count_12_mon, Credit_Limit, Total_Revolving_Bal, Avg_Open_To_Buy, 
            Total_Amt_Chng_Q4_Q1, Total_Trans_Amt, Total_Trans_Ct, Total_Ct_Chng_Q4_Q1, 
            Avg_Utilization_Ratio, NaiveBayesScore_1, NaiveBayesScore_2, 
            Source_File_Name, Load_Date
        )
        SELECT 
            @RunID, CLIENTNUM, Attrition_Flag, Customer_Age, Gender_Code, 
            Dependent_count, Education_Level, Marital_Status, Income_Category, 
            Card_Type, Months_on_book, Total_Relationship_Count, Months_Inactive_12_mon, 
            Contacts_Count_12_mon, Credit_Limit, Total_Revolving_Bal, Avg_Open_To_Buy, 
            Total_Amt_Chng_Q4_Q1, Total_Trans_Amt, Total_Trans_Ct, Total_Ct_Chng_Q4_Q1, 
            Avg_Utilization_Ratio, NaiveBayesScore_1, NaiveBayesScore_2, 
            @SourceFileName, SYSUTCDATETIME()
        FROM OPENROWSET (BULK ''' + @FilePath + ''', SINGLE_CLOB) AS J
        CROSS APPLY OPENJSON(BulkColumn) -- <--- This reads directly from the file content
        WITH(
            CLIENTNUM                  nvarchar(max),
            Attrition_Flag             nvarchar(max),
            Customer_Age               nvarchar(max),
            Gender_Code                nvarchar(max),
            Dependent_count            nvarchar(max),
            Education_Level            nvarchar(max),
            Marital_Status             nvarchar(max),
            Income_Category            nvarchar(max),
            Card_Type                  nvarchar(max),
            Months_on_book             nvarchar(max),
            Total_Relationship_Count   nvarchar(max),
            Months_Inactive_12_mon     nvarchar(max),
            Contacts_Count_12_mon      nvarchar(max),
            Credit_Limit               nvarchar(max),
            Total_Revolving_Bal        nvarchar(max),
            Avg_Open_To_Buy            nvarchar(max),
            Total_Amt_Chng_Q4_Q1       nvarchar(max),
            Total_Trans_Amt            nvarchar(max),
            Total_Trans_Ct             nvarchar(max),
            Total_Ct_Chng_Q4_Q1        nvarchar(max),
            Avg_Utilization_Ratio      nvarchar(max),
            NaiveBayesScore_1          nvarchar(max),
            NaiveBayesScore_2          nvarchar(max)
        );';

        EXEC sp_executesql @DynamicSQL, 
            N'@RunID BIGINT, @SourceFileName NVARCHAR(MAX)', 
            @RunID, @SourceFileName;
        
        SET @RowsInserted = @@ROWCOUNT;

        COMMIT;
        
        UPDATE ETLLog.RunStep
        SET Rows_Inserted = @RowsInserted, End_Time = SYSUTCDATETIME(), Step_Status = 'SUCCESS'
        WHERE RunStep_ID = @RunStepID;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        UPDATE ETLLog.RunStep SET Step_Status = 'FAILED', ErrorMessage = ERROR_MESSAGE(), End_Time = SYSUTCDATETIME() WHERE RunStep_ID = @RunStepID;
        THROW;
    END CATCH;
END;

