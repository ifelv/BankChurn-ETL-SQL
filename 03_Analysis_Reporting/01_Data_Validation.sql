/*==========================================================
01_Data_Validation
============================================================*/

SELECT * FROM ETLLog.RunHeader;
SELECT * FROM ETLLog.RunStep;
SELECT * FROM ETLLog.RowReject;
SELECT * FROM Staging.Staging_Customer;
SELECT * FROM Person.Gender;
SELECT * FROM Person.EducationLevel;
SELECT * FROM Person.MaritalStatus;
SELECT * FROM Bank.CardCategory;
SELECT * FROM Person.Customer;
SELECT * FROM Person.NaiveBayesScore;
SELECT * FROM Bank.BankCard;
SELECT * FROM Bank.AccountActivity;

/*==========================================================
Duplicate Check for Staging Data
============================================================*/

WITH CTE_DuplicateCheck AS (
    SELECT 
        Source_Row_ID,
        CLIENTNUM,
        Load_Date,
        ROW_NUMBER() OVER (
            PARTITION BY CLIENTNUM 
            ORDER BY Source_Row_ID DESC
        ) AS rn
    FROM Staging.Staging_Customer
)
SELECT * FROM CTE_DuplicateCheck WHERE rn > 1;

