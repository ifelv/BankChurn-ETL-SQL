/*==========================================================
07_Indexes
============================================================*/

CREATE NONCLUSTERED INDEX IX_Gender_Code 
ON Person.Gender(Gender_Code);

CREATE NONCLUSTERED INDEX IX_Gender_Desc
ON Person.Gender(Gender_Desc);

CREATE NONCLUSTERED INDEX IX_Education_Level 
ON Person.EducationLevel(Education_Level);

CREATE NONCLUSTERED INDEX IX_Marital_Status 
ON Person.MaritalStatus(Marital_Status);

CREATE NONCLUSTERED INDEX IX_CardCategory_Type 
ON Bank.CardCategory(Card_Type);

CREATE NONCLUSTERED INDEX IX_Customer_Load 
ON Person.Customer(CLIENTNUM) INCLUDE (Attrition_Flag);

CREATE NONCLUSTERED INDEX IX_ChurnAlert_Open 
ON Operations.ChurnAlert(Is_Resolved) 
INCLUDE (CLIENTNUM, Alert_Date)
WHERE Is_Resolved = 0;

