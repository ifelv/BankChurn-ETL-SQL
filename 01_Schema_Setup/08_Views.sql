/*==========================================================
08_Views
============================================================*/

CREATE OR ALTER VIEW Person.v_CustomerDemographics AS
SELECT 
    c.CLIENTNUM,
    c.Attrition_Flag,
    c.Customer_Age,
    c.Income_Category,
    c.Dependent_count,
    g.Gender_Desc,
    e.Education_Level,
    m.Marital_Status,
    CASE 
        WHEN c.Attrition_Flag = 'Attrited Customer' THEN 1 
        ELSE 0 
    END AS Is_Churned
FROM Person.Customer c
INNER JOIN Person.Gender g ON c.Gender_ID = g.Gender_ID
INNER JOIN Person.EducationLevel e ON c.EducationLevel_ID = e.EducationLevel_ID
INNER JOIN Person.MaritalStatus m ON c.MaritalStatus_ID = m.MaritalStatus_ID;
GO

