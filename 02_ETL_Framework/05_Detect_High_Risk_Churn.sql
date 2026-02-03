/*==========================================================
05_Detect_High_Risk_Churn
============================================================*/

CREATE OR ALTER PROCEDURE Operations.usp_GenerateChurnAlerts
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @NewAlerts INT = 0;

    INSERT INTO Operations.ChurnAlert (
        CLIENTNUM,
        Alert_Date,
        Alert_Type,
        Risk_Score,
        Contacts_Count,
        Months_Inactive,
        Is_Resolved
    )
    SELECT 
        nbs.CLIENTNUM,
        SYSUTCDATETIME(),              
        'High Risk: Disengaged & Complaining', 
        nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1, 
        aa.Contacts_Count_12_mon,
        aa.Months_Inactive_12_mon,
        0                                   
    FROM 
        Person.NaiveBayesScore nbs  
        INNER JOIN Person.Customer c 
            ON nbs.CLIENTNUM = c.CLIENTNUM
        INNER JOIN Bank.BankCard bc 
            ON c.CLIENTNUM = bc.CLIENTNUM
        INNER JOIN Bank.AccountActivity aa 
            ON bc.Card_ID = aa.Card_ID
    WHERE 
        c.Attrition_Flag = 'Existing Customer'
        AND aa.Contacts_Count_12_mon >= 3
        AND aa.Months_Inactive_12_mon >= 3
        AND aa.Total_Trans_Ct <= 44
        AND bc.Avg_Utilization_Ratio <= 0.17
        AND NOT EXISTS (
            SELECT 1 
            FROM Operations.ChurnAlert ca 
            WHERE ca.CLIENTNUM = nbs.CLIENTNUM 
              AND ca.Is_Resolved = 0
        );

    SET @NewAlerts = @@ROWCOUNT;

    IF @NewAlerts > 0
    BEGIN
        DECLARE @Msg NVARCHAR(MAX);
        SET @Msg = 'WARNING: ' + CAST(@NewAlerts AS NVARCHAR(10)) + ' High-risk alerts generated in Operations.ChurnAlert.';
        
        INSERT INTO ETLLog.AppEvent (Event_Source, Message)
        VALUES ('Operations.usp_GenerateChurnAlerts', @Msg);
    END
    ELSE
    BEGIN
         INSERT INTO ETLLog.AppEvent (Event_Source, Message)
         VALUES ('Operations.usp_GenerateChurnAlerts', 'No new high-risk customers identified in this run.');
    END
END;
GO
