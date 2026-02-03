/*==========================================================
99_Reporting
============================================================*/

/*==========================================================
Reporting Pagination
============================================================*/

DECLARE @PageNumber INT = 3;
DECLARE @RowsPerPage INT = 20;

SELECT 
    c.CLIENTNUM,
    c.Customer_Age,
    c.Income_Category,
    aa.Total_Trans_Amt
FROM Person.Customer c
INNER JOIN Bank.BankCard bc ON c.CLIENTNUM = bc.CLIENTNUM
INNER JOIN Bank.AccountActivity aa ON bc.Card_ID = aa.Card_ID
ORDER BY aa.Total_Trans_Amt DESC
OFFSET (@PageNumber - 1) * @RowsPerPage ROWS
FETCH NEXT @RowsPerPage ROWS ONLY;
