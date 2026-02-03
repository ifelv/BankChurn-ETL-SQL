/*==========================================================
04_Fact_Loads
============================================================*/

/*==========================================================
Loading Customers into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Person_Customer
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @RowsRead INT = 0;
    DECLARE @Inserted INT = 0;
    DECLARE @Updated INT = 0;
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
        'Load Customer', 
        'Person.Customer', 
        'STARTED', 
        SYSUTCDATETIME()
        );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY

        SELECT @RowsRead = COUNT(*)
        FROM Staging.Staging_Customer;

        INSERT INTO ETLLog.RowReject (
            RunStep_ID,
            Source_Row_ID,
            Target_Table,
            Reject_Reason,
            Reject_Payload
        )
        SELECT
            @RunStepID,
            s.Source_Row_ID,
            'Person.Customer',
            'Invalid basic customer attributes',
            CONCAT_WS('|',
                CLIENTNUM,
                Customer_Age,
                Dependent_count,
                Gender_Code,
                Education_Level,
                Marital_Status,
                Income_Category
            )
        FROM Staging.Staging_Customer s
        WHERE (
            TRY_CAST(CLIENTNUM AS INT) IS NULL
            OR TRY_CAST(Customer_Age AS INT) IS NULL
            OR TRY_CAST(Customer_Age AS INT) < 18
            OR TRY_CAST(Dependent_count AS int) IS NULL
            OR Income_Category IS NULL
            )
            OR NOT EXISTS (SELECT 1 FROM Person.Gender g WHERE g.Gender_Code = s.Gender_Code)
            OR NOT EXISTS (SELECT 1 FROM Person.EducationLevel e WHERE e.Education_Level = s.Education_Level)
            OR NOT EXISTS (SELECT 1 FROM Person.MaritalStatus m WHERE m.Marital_Status = s.Marital_Status);

        SET @Rejected += @@ROWCOUNT;

        SELECT
            s.Source_Row_ID,
            TRY_CAST(s.CLIENTNUM AS INT)        AS CLIENTNUM,
            s.Attrition_Flag,
            TRY_CAST(s.Customer_Age AS INT)     AS Customer_Age,
            TRY_CAST(s.Dependent_count AS INT)  AS Dependent_count,
            s.Income_Category,
            g.Gender_ID,
            e.EducationLevel_ID,
            m.MaritalStatus_ID,
            s.Load_Date,
            @RunID AS Run_ID
        INTO #ResolvedCustomer
        FROM Staging.Staging_Customer s
        LEFT JOIN Person.Gender g
            ON g.Gender_Code = s.Gender_Code
        LEFT JOIN Person.EducationLevel e
            ON e.Education_Level = s.Education_Level
        LEFT JOIN Person.MaritalStatus m
            ON m.Marital_Status = s.Marital_Status
        WHERE 
            NOT EXISTS (
                SELECT 1
                FROM ETLLog.RowReject rr
                WHERE rr.RunStep_ID = @RunStepID
                  AND rr.Source_Row_ID = s.Source_Row_ID
          );

        BEGIN TRAN;

            MERGE Person.Customer AS Target
            USING #ResolvedCustomer AS Source
            ON (Target.CLIENTNUM = Source.CLIENTNUM)
            WHEN MATCHED AND (
                 Target.Attrition_Flag <> Source.Attrition_Flag OR
                 Target.Customer_Age <> Source.Customer_Age OR
                 Target.Income_Category <> Source.Income_Category OR
                 Target.MaritalStatus_ID <> Source.MaritalStatus_ID OR
                 Target.Dependent_count <> Source.Dependent_count
            ) THEN 
                UPDATE SET 
                    Target.Attrition_Flag = Source.Attrition_Flag,
                    Target.Customer_Age = Source.Customer_Age,
                    Target.Dependent_count = Source.Dependent_count,
                    Target.Income_Category = Source.Income_Category,
                    Target.MaritalStatus_ID = Source.MaritalStatus_ID,
                    Target.Load_Date = Source.Load_Date,
                    Target.Run_ID = Source.Run_ID
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    CLIENTNUM, Attrition_Flag, Customer_Age, Dependent_count, 
                    Income_Category, Gender_ID, EducationLevel_ID, MaritalStatus_ID, 
                    Load_Date, Run_ID
                )
                VALUES (
                    Source.CLIENTNUM, Source.Attrition_Flag, Source.Customer_Age, Source.Dependent_count, 
                    Source.Income_Category, Source.Gender_ID, Source.EducationLevel_ID, Source.MaritalStatus_ID, 
                    Source.Load_Date, Source.Run_ID
                );

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Read = @RowsRead,
            Rows_Inserted = @Inserted,
            Rows_Rejected = @Rejected,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        DROP TABLE IF EXISTS #ResolvedCustomer;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

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
Loading BankCard into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Bank_BankCard
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @RowsRead INT = 0;
    DECLARE @Inserted INT = 0;
    DECLARE @Updated INT = 0;
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
        'Load BankCard', 
        'Bank.BankCard', 
        'STARTED', 
        SYSUTCDATETIME()
        );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY

        SELECT @RowsRead = COUNT(*)
        FROM Staging.Staging_Customer;

        INSERT INTO ETLLog.RowReject (
            RunStep_ID,
            Source_Row_ID,
            Target_Table,
            Reject_Reason,
            Reject_Payload,
            Reject_Date
        )
        SELECT
            @RunStepID,
            s.Source_Row_ID,
            'Bank.BankCard',
            'Invalid basic BankCard attributes or Missing Card Category',
            CONCAT_WS('|',
                s.CLIENTNUM,
                s.Credit_Limit,
                s.Avg_Open_To_Buy,
                s.Avg_Utilization_Ratio,
                s.Card_Type
            ),
            SYSUTCDATETIME()
        FROM Staging.Staging_Customer s
        WHERE (
             TRY_CAST(CLIENTNUM AS INT) IS NULL
             OR TRY_CAST(Credit_Limit AS DECIMAL(7,2)) IS NULL
             OR TRY_CAST(Avg_Open_To_Buy AS DECIMAL(7,2)) IS NULL
             OR TRY_CAST(Avg_Utilization_Ratio AS DECIMAL(4,3)) IS NULL
             )
             OR NOT EXISTS (SELECT 1 FROM Bank.CardCategory c WHERE c.Card_Type = s.Card_Type);

        SET @Rejected += @@ROWCOUNT;

        SELECT
            s.Source_Row_ID,
            TRY_CAST(s.CLIENTNUM AS INT) AS CLIENTNUM,
            TRY_CAST(s.Credit_Limit AS DECIMAL(7,2)) AS Credit_Limit,
            TRY_CAST(s.Avg_Open_To_Buy AS DECIMAL(7,2)) AS Avg_Open_To_Buy,
            TRY_CAST(s.Avg_Utilization_Ratio AS DECIMAL(4,3)) AS Avg_Utilization_Ratio,
            c.CardCategory_ID,
            s.Load_Date,
            @RunID AS Run_ID
        INTO #ResolvedBankCard
        FROM Staging.Staging_Customer s
        LEFT JOIN Bank.CardCategory c
            ON c.Card_Type = s.Card_Type
        WHERE 
            NOT EXISTS (
                SELECT 1
                FROM ETLLog.RowReject rr
                WHERE rr.RunStep_ID = @RunStepID
                  AND rr.Source_Row_ID = s.Source_Row_ID
          );

        BEGIN TRAN;

            MERGE Bank.BankCard AS Target
            USING #ResolvedBankCard AS Source
            ON (Target.CLIENTNUM = Source.CLIENTNUM AND Target.CardCategory_ID = Source.CardCategory_ID)

            WHEN MATCHED AND (
                Target.Credit_Limit <> Source.Credit_Limit OR
                Target.Avg_Open_To_Buy <> Source.Avg_Open_To_Buy OR
                Target.Avg_Utilization_Ratio <> Source.Avg_Utilization_Ratio
            ) THEN
                UPDATE SET
                    Target.Credit_Limit = Source.Credit_Limit,
                    Target.Avg_Open_To_Buy = Source.Avg_Open_To_Buy,
                    Target.Avg_Utilization_Ratio = Source.Avg_Utilization_Ratio,
                    Target.Load_Date = Source.Load_Date,
                    Target.Run_ID = Source.Run_ID

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (CLIENTNUM, Credit_Limit, Avg_Open_To_Buy, Avg_Utilization_Ratio, CardCategory_ID, Load_Date, Run_ID)
                VALUES (Source.CLIENTNUM, Source.Credit_Limit, Source.Avg_Open_To_Buy, Source.Avg_Utilization_Ratio, Source.CardCategory_ID, Source.Load_Date, Source.Run_ID);

            SET @Inserted = @@ROWCOUNT;            

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Read = @RowsRead,
            Rows_Inserted = @Inserted,
            Rows_Rejected = @Rejected,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        DROP TABLE IF EXISTS #ResolvedBankCard;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

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
Loading AccountActivity into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Bank_AccountActivity
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @RowsRead INT = 0;
    DECLARE @Inserted INT = 0;
    DECLARE @Updated INT = 0;
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
        'Load AccountActivity',
        'Bank.AccountActivity',
        'STARTED',
        SYSUTCDATETIME()
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY

        SELECT @RowsRead = COUNT(*)
        FROM Staging.Staging_Customer;

        INSERT INTO ETLLog.RowReject (
            RunStep_ID,
            Source_Row_ID,
            Target_Table,
            Reject_Reason,
            Reject_Payload,
            Reject_Date
        )
        SELECT
            @RunStepID,
            s.Source_Row_ID,
            'Bank.AccountActivity',
            'Invalid basic AccountActivity attributes',
            CONCAT_WS('|',
                s.CLIENTNUM,
                s.Months_on_book,
                s.Total_Relationship_Count,
                s.Months_Inactive_12_mon,
                s.Contacts_Count_12_mon,
                s.Total_Revolving_Bal,
                s.Total_Trans_Amt,
                s.Total_Amt_Chng_Q4_Q1,
                s.Total_Trans_Ct,
                s.Total_Ct_Chng_Q4_Q1
            ),
            SYSUTCDATETIME()
        FROM Staging.Staging_Customer s
        LEFT JOIN Bank.CardCategory c ON c.Card_Type = s.Card_Type
        WHERE (
            TRY_CAST(s.CLIENTNUM AS INT) IS NULL
            OR TRY_CAST(s.Months_on_book AS INT) IS NULL
            OR TRY_CAST(s.Total_Relationship_Count AS INT) IS NULL
            OR TRY_CAST(s.Months_Inactive_12_mon AS INT) IS NULL
            OR TRY_CAST(s.Contacts_Count_12_mon AS INT) IS NULL
            OR TRY_CAST(s.Total_Revolving_Bal AS INT) IS NULL
            OR TRY_CAST(s.Total_Trans_Amt AS DECIMAL(7,2)) IS NULL
            OR TRY_CAST(s.Total_Amt_Chng_Q4_Q1 AS DECIMAL(4,3)) IS NULL
            OR TRY_CAST(s.Total_Trans_Ct AS INT) IS NULL
            OR TRY_CAST(s.Total_Ct_Chng_Q4_Q1 AS DECIMAL(4,3)) IS NULL
            )
            OR NOT EXISTS (SELECT 1 FROM Bank.BankCard b WHERE b.CLIENTNUM = TRY_CAST(s.CLIENTNUM AS INT) AND b.CardCategory_ID = c.CardCategory_ID);

        SET @Rejected += @@ROWCOUNT;

        SELECT
            s.Source_Row_ID,
            b.Card_ID,
            TRY_CAST(s.Months_on_book AS INT) AS Months_on_book,
            TRY_CAST(s.Total_Relationship_Count AS INT) AS Total_Relationship_Count,
            TRY_CAST(s.Months_Inactive_12_mon AS INT) AS Months_Inactive_12_mon,
            TRY_CAST(s.Contacts_Count_12_mon AS INT) AS Contacts_Count_12_mon,
            TRY_CAST(s.Total_Revolving_Bal AS INT) AS Total_Revolving_Bal,
            TRY_CAST(s.Total_Trans_Amt AS DECIMAL(7,2)) AS Total_Trans_Amt,
            TRY_CAST(s.Total_Amt_Chng_Q4_Q1 AS DECIMAL(4,3)) AS Total_Amt_Chng_Q4_Q1,
            TRY_CAST(s.Total_Trans_Ct AS INT) AS Total_Trans_Ct,
            TRY_CAST(s.Total_Ct_Chng_Q4_Q1 AS DECIMAL(4,3)) AS Total_Ct_Chng_Q4_Q1,
            s.Load_Date,
            @RunID AS Run_ID
        INTO #ResolvedAccountActivity
        FROM Staging.Staging_Customer s
        LEFT JOIN Bank.CardCategory c ON c.Card_Type = s.Card_Type
        INNER JOIN Bank.BankCard b
            ON b.CLIENTNUM = TRY_CAST(s.CLIENTNUM AS INT)
            AND b.CardCategory_ID = c.CardCategory_ID
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM ETLLog.RowReject rr
                WHERE rr.RunStep_ID = @RunStepID
                  AND rr.Source_Row_ID = s.Source_Row_ID
          );

        BEGIN TRAN;

            MERGE Bank.AccountActivity AS Target
            USING #ResolvedAccountActivity AS Source
            ON (Target.Card_ID = Source.Card_ID)

            WHEN MATCHED AND (
                Target.Months_on_book <> Source.Months_on_book OR
                Target.Total_Relationship_Count <> Source.Total_Relationship_Count OR
                Target.Months_Inactive_12_mon <> Source.Months_Inactive_12_mon OR
                Target.Contacts_Count_12_mon <> Source.Contacts_Count_12_mon OR
                Target.Total_Revolving_Bal <> Source.Total_Revolving_Bal OR
                Target.Total_Trans_Amt <> Source.Total_Trans_Amt OR
                Target.Total_Amt_Chng_Q4_Q1 <> Source.Total_Amt_Chng_Q4_Q1 OR
                Target.Total_Trans_Ct <> Source.Total_Trans_Ct OR
                Target.Total_Ct_Chng_Q4_Q1 <> Source.Total_Ct_Chng_Q4_Q1
            ) THEN
                UPDATE SET
                    Target.Months_on_book = Source.Months_on_book,
                    Target.Total_Relationship_Count = Source.Total_Relationship_Count,
                    Target.Months_Inactive_12_mon = Source.Months_Inactive_12_mon,
                    Target.Contacts_Count_12_mon = Source.Contacts_Count_12_mon,
                    Target.Total_Revolving_Bal = Source.Total_Revolving_Bal,
                    Target.Total_Trans_Amt = Source.Total_Trans_Amt,
                    Target.Total_Amt_Chng_Q4_Q1 = Source.Total_Amt_Chng_Q4_Q1,
                    Target.Total_Trans_Ct = Source.Total_Trans_Ct,
                    Target.Total_Ct_Chng_Q4_Q1 = Source.Total_Ct_Chng_Q4_Q1,
                    Target.Load_Date = Source.Load_Date,
                    Target.Run_ID = Source.Run_ID

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (Card_ID, Months_on_book, Total_Relationship_Count, Months_Inactive_12_mon, Contacts_Count_12_mon, Total_Revolving_Bal, Total_Trans_Amt, Total_Amt_Chng_Q4_Q1, Total_Trans_Ct, Total_Ct_Chng_Q4_Q1, Load_Date, Run_ID)
                VALUES (Source.Card_ID, Source.Months_on_book, Source.Total_Relationship_Count, Source.Months_Inactive_12_mon, Source.Contacts_Count_12_mon, Source.Total_Revolving_Bal, Source.Total_Trans_Amt, Source.Total_Amt_Chng_Q4_Q1, Source.Total_Trans_Ct, Source.Total_Ct_Chng_Q4_Q1, Source.Load_Date, Source.Run_ID);

            SET @Inserted = @@ROWCOUNT;
            
        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Read = @RowsRead,
            Rows_Inserted = @Inserted,
            Rows_Rejected = @Rejected,
            Step_Status = 'SUCCESS',
            End_Time = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;

        DROP TABLE IF EXISTS #ResolvedAccountActivity;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

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
Loading NaiveBayesScore into Production
============================================================*/

CREATE OR ALTER PROCEDURE ETL.usp_Load_Bank_NaiveBayesScore
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStepID BIGINT;
    DECLARE @RowsRead INT = 0;
    DECLARE @Inserted INT = 0;
    DECLARE @Updated INT = 0;
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
        'Load NaiveBayesScore',
        'Person.NaiveBayesScore',
        'STARTED',
        SYSUTCDATETIME()
    );

    SET @RunStepID = SCOPE_IDENTITY();

    BEGIN TRY

        SELECT @RowsRead = COUNT(*)
        FROM Staging.Staging_Customer;

        INSERT INTO ETLLog.RowReject (
            RunStep_ID,
            Source_Row_ID,
            Target_Table,
            Reject_Reason,
            Reject_Payload,
            Reject_Date
        )
        SELECT
            @RunStepID,
            s.Source_Row_ID,
            'Person.NaiveBayesScore',
            'Invalid Scientific Notation or Range',
            CONCAT_WS('|',
                s.CLIENTNUM,
                s.NaiveBayesScore_1,
                s.NaiveBayesScore_2
            ),
            SYSUTCDATETIME()
        FROM Staging.Staging_Customer s
        WHERE 
             NOT EXISTS (SELECT 1 FROM Person.NaiveBayesScore nbs WHERE nbs.CLIENTNUM = TRY_CAST(s.CLIENTNUM AS INT))
             AND (
                TRY_CAST(s.CLIENTNUM AS INT) IS NULL
                OR TRY_CAST(s.NaiveBayesScore_1 AS FLOAT) IS NULL
                OR TRY_CAST(s.NaiveBayesScore_2 AS FLOAT) IS NULL
                OR ABS(TRY_CAST(s.NaiveBayesScore_1 AS FLOAT)) >= 10.0
                OR ABS(TRY_CAST(s.NaiveBayesScore_2 AS FLOAT)) >= 10.0
             );

        SET @Rejected += @@ROWCOUNT;

        SELECT
            s.Source_Row_ID,
            TRY_CAST(s.CLIENTNUM AS INT) AS CLIENTNUM,
            CAST(TRY_CAST(s.NaiveBayesScore_1 AS FLOAT) AS DECIMAL(11,10)) AS NaiveBayesScore_1,
            CAST(TRY_CAST(s.NaiveBayesScore_2 AS FLOAT) AS DECIMAL(11,10)) AS NaiveBayesScore_2,
            s.Load_Date,
            @RunID AS Run_ID
        INTO #ResolvedNaiveBayesScore
        FROM Staging.Staging_Customer s
        WHERE 
             NOT EXISTS (
                SELECT 1 
                FROM ETLLog.RowReject rr 
                WHERE rr.RunStep_ID = @RunStepID 
                  AND rr.Source_Row_ID = s.Source_Row_ID
             );

        BEGIN TRAN;

            MERGE Person.NaiveBayesScore AS Target
            USING #ResolvedNaiveBayesScore AS Source
            ON (Target.CLIENTNUM = Source.CLIENTNUM)

            WHEN MATCHED AND (
                Target.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 <> Source.NaiveBayesScore_1 OR
                Target.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_2 <> Source.NaiveBayesScore_2
            ) THEN
                UPDATE SET
                    Target.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 = Source.NaiveBayesScore_1,
                    Target.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_2 = Source.NaiveBayesScore_2,
                    Target.Load_Date = Source.Load_Date,
                    Target.Run_ID = Source.Run_ID

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (CLIENTNUM, Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1, Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_2, Load_Date, Run_ID)
                VALUES (Source.CLIENTNUM, Source.NaiveBayesScore_1, Source.NaiveBayesScore_2, Source.Load_Date, Source.Run_ID);

            SET @Inserted = @@ROWCOUNT;

        COMMIT;

        UPDATE ETLLog.RunStep
        SET
            Rows_Read     = @RowsRead,
            Rows_Inserted = @Inserted,
            Rows_Rejected = @Rejected,
            Step_Status   = 'SUCCESS',
            End_Time      = SYSUTCDATETIME()
        WHERE RunStep_ID = @RunStepID;
        
        DROP TABLE IF EXISTS #ResolvedNaiveBayesScore;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

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

