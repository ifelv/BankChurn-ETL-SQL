/*==========================================================
04_Risk_Segmentation
============================================================*/

/*==========================================================
Loyalty Score Logic and Loyalty Ranking
============================================================*/

CREATE OR ALTER FUNCTION Bank.fn_CalculateLoyaltyScore 
(
    @MonthsOnBook INT,
    @TotalRelationshipCount INT
)
RETURNS DECIMAL(10,2)
AS
BEGIN
    DECLARE @Score DECIMAL(10,2);

    SET @Score = (@MonthsOnBook * 0.5) + (@TotalRelationshipCount * 10.0);

    RETURN @Score;
END;
GO

SELECT TOP 20
    c.CLIENTNUM,
    aa.Months_on_book,
    aa.Total_Relationship_Count,
    Bank.fn_CalculateLoyaltyScore(aa.Months_on_book, aa.Total_Relationship_Count) AS Loyalty_Score,
    c.Attrition_Flag
FROM Person.Customer c
INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
ORDER BY Loyalty_Score DESC;

/*==========================================================
Risk Analysis - Inactive Premium Cards
============================================================*/
WITH InactivePremiumCards AS (
    SELECT c.CLIENTNUM, c.Attrition_Flag
    FROM Person.Customer c
    INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
    INNER JOIN Bank.CardCategory cc ON bc.CardCategory_ID = cc.CardCategory_ID
    WHERE cc.Card_Type IN ('Gold', 'Platinum')

    INTERSECT 

    SELECT c.CLIENTNUM, c.Attrition_Flag
    FROM Person.Customer c
    INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
    INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
    WHERE aa.Total_Trans_Ct < 44
    )
SELECT
    ipc.CLIENTNUM,
    aa.Total_Trans_Amt,
    aa.Total_Trans_Ct,
    ipc.Attrition_Flag
FROM InactivePremiumCards ipc
INNER JOIN Bank.BankCard bc ON ipc.CLIENTNUM = bc.CLIENTNUM
INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
ORDER BY aa.Total_Trans_Amt DESC, aa.Total_Trans_Ct DESC;

/*==========================================================
Statistical Segmentation
============================================================*/

SELECT 
    c.CLIENTNUM,
    bc.Credit_Limit,
    NTILE(4) OVER (ORDER BY bc.Credit_Limit DESC) AS Quartile_Group,
    CASE NTILE(4) OVER (ORDER BY bc.Credit_Limit DESC)
        WHEN 1 THEN 'Tier 1 - Platinum VIP'   -- Top 25%
        WHEN 2 THEN 'Tier 2 - Gold High'      -- Next 25%
        WHEN 3 THEN 'Tier 3 - Silver Mid'     -- Next 25%
        WHEN 4 THEN 'Tier 4 - Blue Entry'     -- Bottom 25%
    END AS Credit_Segment_Label,
    c.Attrition_Flag
INTO #StatisticalSegmentation
FROM Person.Customer c
INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM;

SELECT
    CLIENTNUM,
    Credit_Limit,
    Quartile_Group,
    Credit_Segment_Label,
    Attrition_Flag,
    COUNT(*) OVER (PARTITION BY Credit_Segment_Label) AS Flagged
FROM #StatisticalSegmentation
WHERE Attrition_Flag = 'Attrited Customer'

/*==========================================================
Attritions Score Comparison
============================================================*/

SELECT 
    c.Attrition_Flag,
    AVG(CAST(nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 AS FLOAT)) as Avg_Churn_Probability,
    AVG(CAST(nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_2 AS FLOAT)) as Avg_Remain_Probability
FROM Person.Customer c
JOIN Person.NaiveBayesScore nbs ON c.CLIENTNUM = nbs.CLIENTNUM
GROUP BY c.Attrition_Flag;

/*==========================================================
Profiling Attritions
============================================================*/

SELECT 
    CASE 
        WHEN nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 > 0.99
        THEN 'High Risk (Likely Churn)' 
        ELSE 'Low Risk (Safe)' 
    END AS Risk_Segment,
    COUNT(c.CLIENTNUM) as Total_Customers,
    AVG(aa.Contacts_Count_12_mon) as Avg_Contacts,
    AVG(aa.Months_Inactive_12_mon) as Avg_Inactive_Months,
    AVG(aa.Total_Trans_Ct) as Avg_Transaction_Count,
    AVG(bc.Avg_Utilization_Ratio) as Avg_Credit_Utilization
FROM Person.Customer c
JOIN Person.NaiveBayesScore nbs ON c.CLIENTNUM = nbs.CLIENTNUM
JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
GROUP BY 
    CASE 
        WHEN nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 > 0.99
        THEN 'High Risk (Likely Churn)' 
        ELSE 'Low Risk (Safe)' 
    END;

SELECT
    Attrition_Flag,
    COUNT(CLIENTNUM) as Total_Customers
FROM Person.Customer
GROUP BY Attrition_Flag;

/*==========================================================
Customers in Danger Zone (High Contacts + High Inactivity + Low Transaction Count + Low Credit Utilization
============================================================*/

CREATE OR ALTER PROCEDURE Bank.GetHighRiskInterventionList
    @ContactThreshold INT = 3,     
    @InactiveThreshold INT = 3,     
    @TransactionCount INT = 44,
    @CreditUtilization DECIMAL(4,3) = 0.17
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        c.CLIENTNUM,
        c.Attrition_Flag,
        aa.Contacts_Count_12_mon AS [Recent_Contacts],
        aa.Months_Inactive_12_mon AS [Months_Inactive],
        aa.Total_Trans_Ct AS [Transaction_Count],
        bc.Credit_Limit,
        bc.Avg_Utilization_Ratio,
        (aa.Contacts_Count_12_mon + aa.Months_Inactive_12_mon) AS Risk_Intensity,
        nbs.Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 AS [Model_Churn_Prob]
    FROM 
        Person.Customer c
        INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
        INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
        LEFT JOIN Person.NaiveBayesScore nbs ON c.CLIENTNUM = nbs.CLIENTNUM
    WHERE 
        c.Attrition_Flag = 'Existing Customer'  
        AND aa.Contacts_Count_12_mon >= @ContactThreshold
        AND aa.Months_Inactive_12_mon >= @InactiveThreshold
        AND aa.Total_Trans_Ct <= @TransactionCount
        AND bc.Avg_Utilization_Ratio <= @CreditUtilization
    ORDER BY 
        Risk_Intensity DESC, 
        [Model_Churn_Prob] DESC;
END;
GO

EXEC Bank.GetHighRiskInterventionList;

