/*==========================================================
99_Orchestration_Master
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_RunBankChurnersETL
AS
BEGIN
    DECLARE @RunID BIGINT;
    DECLARE @JobName NVARCHAR(MAX) = N'BankChurners_ETL';
    DECLARE @SourceFileName NVARCHAR(MAX) = N'BankChurners_json_20251218.json';
    DECLARE @FilePath NVARCHAR(MAX) = N'C:\Users\Public\Git SandBox\BankChurnersDec_SQL_Files_Git\docs\BankChurners_json_20251218.json';

    EXEC ETLLog.usp_RunHeaderStart
        @JobName = @JobName,
        @SourceFileName = @SourceFileName,
        @RunID = @RunID OUTPUT;

    EXEC Staging.usp_LoadCustomer
        @RunID = @RunID,
        @StepName = N'Load Staging Customer',
        @SourceFileName = @SourceFileName,
        @FilePath = @FilePath;

    EXEC ETL.usp_Load_Person_Gender
        @RunID = @RunID;

    EXEC ETL.usp_Load_Person_EducationLevel
        @RunID = @RunID;

    EXEC ETL.usp_Load_Person_MaritalStatus
        @RunID = @RunID;

    EXEC ETL.usp_Load_Person_Customer
        @RunID = @RunID;

    EXEC ETL.usp_Load_Bank_CardCategory
        @RunID = @RunID;

    EXEC ETL.usp_Load_Bank_BankCard
        @RunID = @RunID;

    EXEC ETL.usp_Load_Bank_AccountActivity
        @RunID = @RunID;

    EXEC ETL.usp_Load_Bank_NaiveBayesScore
        @RunID = @RunID;

    EXEC ETLLog.usp_FinalizeRun
        @RunID = @RunID;
END;

