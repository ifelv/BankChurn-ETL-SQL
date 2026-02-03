/*==========================================================
02_Demographics_Analysis
============================================================*/

/*==========================================================
Income vs. Education
============================================================*/

SELECT 
    Education_Level,
    ISNULL([Less than $40K], 0)  AS [Income < 40K],
    ISNULL([$40K - $60K], 0)     AS [Income 40K-60K],
    ISNULL([$60K - $80K], 0)     AS [Income 60K-80K],
    ISNULL([$80K - $120K], 0)    AS [Income 80K-120K],
    ISNULL([$120K +], 0)         AS [Income > 120K],
    ISNULL([Unknown], 0)         AS [Income Unknown],
    (ISNULL([Less than $40K], 0) + ISNULL([$40K - $60K], 0) + 
     ISNULL([$60K - $80K], 0) + ISNULL([$80K - $120K], 0) + 
     ISNULL([$120K +], 0) + ISNULL([Unknown], 0)) AS [TOTAL PER EDUCATION]
FROM 
(
    SELECT 
        Education_Level, 
        Income_Category, 
        CLIENTNUM 
    FROM Person.v_CustomerDemographics
) AS SourceTable
PIVOT 
(
    COUNT(CLIENTNUM) 
    FOR Income_Category IN (
        [Less than $40K], 
        [$40K - $60K], 
        [$60K - $80K], 
        [$80K - $120K], 
        [$120K +], 
        [Unknown]
    )
) AS PivotTable
ORDER BY [TOTAL PER EDUCATION] DESC;


/*==========================================================
Age and Risc Segmentation
============================================================*/

SELECT 
    CASE 
        WHEN Customer_Age < 30 THEN 'Young (<30)'
        WHEN Customer_Age BETWEEN 30 AND 45 THEN 'Adults (30-45)'
        WHEN Customer_Age BETWEEN 46 AND 60 THEN 'Mature (46-60)'
        ELSE 'Seniors (>60)'
    END AS Age_Segment,
    COUNT(CLIENTNUM) AS Total_Customers,   
    SUM(Is_Churned) AS Churned_Customers, 
    CAST(AVG(CAST(Is_Churned AS DECIMAL(10,4))) * 100 AS DECIMAL(5,2)) AS Churn_Rate_Percent
FROM Person.v_CustomerDemographics
GROUP BY 
    CASE 
        WHEN Customer_Age < 30 THEN 'Young (<30)'
        WHEN Customer_Age BETWEEN 30 AND 45 THEN 'Adults (30-45)'
        WHEN Customer_Age BETWEEN 46 AND 60 THEN 'Mature (46-60)'
        ELSE 'Seniors (>60)'
    END
ORDER BY Churn_Rate_Percent DESC;

/*==========================================================
Loyal Customer Profiling
============================================================*/

SELECT 
    Income_Category,
    COUNT(CLIENTNUM) AS Nr_Clienti,
    AVG(Customer_Age) AS Media_Varsta_Grup,
    (SELECT AVG(Customer_Age) FROM Person.Customer) AS Media_Varsta_Globala
FROM Person.v_CustomerDemographics
WHERE 
    Gender_Desc = 'Female' 
    AND Marital_Status = 'Married'
    AND Education_Level IN ('Graduate', 'Post-Graduate', 'Doctorate') 
    AND Attrition_Flag = 'Existing Customer'
GROUP BY Income_Category
HAVING COUNT(CLIENTNUM) > 10 
ORDER BY Nr_Clienti DESC;

