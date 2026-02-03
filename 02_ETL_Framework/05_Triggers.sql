/*==========================================================
05_Triggers
============================================================*/

CREATE OR ALTER TRIGGER Person.trg_DetectHighRiskChurn
ON Person.NaiveBayesScore
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

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
        i.CLIENTNUM,
        SYSUTCDATETIME(),            
        'High Risk: Disengaged & Complaining', 
        i.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1, 
        aa.Contacts_Count_12_mon,
        aa.Months_Inactive_12_mon,
        0                            
    FROM 
        inserted i  
        INNER JOIN Person.Customer c 
            ON i.CLIENTNUM = c.CLIENTNUM
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
            WHERE ca.CLIENTNUM = i.CLIENTNUM 
              AND ca.Is_Resolved = 0
        );

    IF @@ROWCOUNT > 0
    BEGIN
        DECLARE @Msg NVARCHAR(MAX);
        SET @Msg = 'WARNING: ' + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' High-risk alerts generated in Operations.ChurnAlert.';
        
        INSERT INTO ETLLog.AppEvent (Event_Source, Message)
        VALUES ('Trigger: DetectHighRisk', @Msg);
    END
END;
GO
