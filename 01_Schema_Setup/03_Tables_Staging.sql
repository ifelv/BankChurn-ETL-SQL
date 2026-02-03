/*==========================================================
2) ETL Staging table
============================================================*/
CREATE TABLE Staging.Staging_Customer (
    Source_Row_ID              BIGINT IDENTITY(1,1) PRIMARY KEY,
    Run_ID                     BIGINT NOT NULL,
    CLIENTNUM                  NVARCHAR(MAX),
    Attrition_Flag             NVARCHAR(MAX),
    Customer_Age               NVARCHAR(MAX),
    Gender_Code                NVARCHAR(MAX),
    Dependent_count            NVARCHAR(MAX),
    Education_Level            NVARCHAR(MAX),
    Marital_Status             NVARCHAR(MAX),
    Income_Category            NVARCHAR(MAX),
    Card_Type                  NVARCHAR(MAX),
    Months_on_book             NVARCHAR(MAX),
    Total_Relationship_Count   NVARCHAR(MAX),
    Months_Inactive_12_mon     NVARCHAR(MAX),
    Contacts_Count_12_mon      NVARCHAR(MAX),
    Credit_Limit               NVARCHAR(MAX),
    Total_Revolving_Bal        NVARCHAR(MAX),
    Avg_Open_To_Buy            NVARCHAR(MAX),
    Total_Amt_Chng_Q4_Q1       NVARCHAR(MAX),
    Total_Trans_Amt            NVARCHAR(MAX),
    Total_Trans_Ct             NVARCHAR(MAX),
    Total_Ct_Chng_Q4_Q1        NVARCHAR(MAX),
    Avg_Utilization_Ratio      NVARCHAR(MAX),
    NaiveBayesScore_1          NVARCHAR(MAX),
    NaiveBayesScore_2          NVARCHAR(MAX),
    Source_File_Name           NVARCHAR(MAX) NULL,
    Load_Date                  DATETIME DEFAULT SYSUTCDATETIME()
);
