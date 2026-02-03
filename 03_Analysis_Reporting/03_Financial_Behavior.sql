/*==========================================================
03_Financial_Behavior
============================================================*/

/*==========================================================
Comparative Analysis: Churned vs. Existing Customers
============================================================*/

SELECT 
    c.Attrition_Flag,
    COUNT(c.CLIENTNUM) AS Customer_Count,
    FORMAT(AVG(aa.Total_Trans_Amt), 'C', 'en-US') AS Avg_Trans_Amount,
    CAST(AVG(aa.Total_Trans_Ct) AS DECIMAL(10,1)) AS Avg_Trans_Count,
    CAST(AVG(bc.Avg_Utilization_Ratio) * 100 AS DECIMAL(5,2)) AS Avg_Utilization_Pct,
    CAST(AVG(aa.Months_on_book) AS DECIMAL(5,1)) AS Avg_Months_Tenure
FROM Person.Customer c
INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
GROUP BY c.Attrition_Flag;

/*==========================================================
Finding High-Value Customers
============================================================*/

SELECT TOP 100
    c.CLIENTNUM,
    c.Income_Category,
    bc.Credit_Limit,
    aa.Total_Trans_Amt,
    aa.Total_Trans_Ct,
    c.Attrition_Flag
FROM Person.Customer c
INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
WHERE 
    aa.Total_Trans_Amt > (
        SELECT AVG(Total_Trans_Amt) 
        FROM Bank.AccountActivity
    )
ORDER BY aa.Total_Trans_Amt DESC;

/*==========================================================
Finding VIP Targets - High Value, High Frequency
============================================================*/

WITH VIPTargets AS (
    SELECT 
        c.CLIENTNUM, 
        aa.Total_Trans_Amt,
        aa.Total_Trans_Ct,
        c.Attrition_Flag
    FROM Person.Customer c
    INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
    INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
    WHERE aa.Total_Trans_Amt > 10000

    UNION 

    SELECT 
        c.CLIENTNUM, 
        aa.Total_Trans_Amt,
        aa.Total_Trans_Ct,
        c.Attrition_Flag
    FROM Person.Customer c
    INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
    INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
    WHERE aa.Total_Trans_Ct > 100
    )
SELECT 
    CLIENTNUM, 
    Total_Trans_Amt,
    Total_Trans_Ct,
    Attrition_Flag
FROM VIPTargets
WHERE Attrition_Flag = 'Attrited Customer';

/*==========================================================
Ranking Customers per Card Category
============================================================*/

WITH RankedCustomers AS (
    SELECT 
        cc.Card_Type,
        c.CLIENTNUM,
        aa.Total_Trans_Amt,
        DENSE_RANK() OVER (
            PARTITION BY cc.Card_Type       
            ORDER BY aa.Total_Trans_Amt DESC 
        ) AS Spending_Rank,
        c.Attrition_Flag
    FROM Person.Customer c
    INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
    INNER JOIN Bank.CardCategory cc ON bc.CardCategory_ID = cc.CardCategory_ID
    INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
)
SELECT * FROM RankedCustomers
WHERE Spending_Rank <= 10;

