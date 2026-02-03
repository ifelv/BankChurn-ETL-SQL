/*==========================================================
05_Tables_Core_Bank
============================================================*/

CREATE TABLE Bank.CardCategory (
    CardCategory_ID INT IDENTITY(1,1) PRIMARY KEY,   -- Product Variable - Type of Card (Blue, Silver, Gold, Platinum)
    Card_Type VARCHAR(20) UNIQUE, -- Blue, Silver, Gold, Platinum
    Load_Date DATETIME
);

CREATE TABLE Bank.BankCard (
    Card_ID INT IDENTITY(1,1) PRIMARY KEY, 
    CLIENTNUM INT,
    Credit_Limit DECIMAL(7,2), -- Credit Limit on the Credit Card
    Avg_Open_To_Buy DECIMAL(7,2), -- Open to Buy Credit Line (Average of last 12 months)
    Avg_Utilization_Ratio DECIMAL(4,3), -- Average Card Utilization Ratio
    CardCategory_ID INT FOREIGN KEY REFERENCES Bank.CardCategory(CardCategory_ID),
    FOREIGN KEY (CLIENTNUM) REFERENCES Person.Customer(CLIENTNUM),
    Load_Date DATETIME,
    Run_ID BIGINT NOT NULL
);

CREATE TABLE Bank.AccountActivity (
    Activity_ID INT IDENTITY(1,1) PRIMARY KEY, 
    Card_ID INT,
    Months_on_book INT, -- Period of relationship with bank
    Total_Relationship_Count INT, -- Total no. of products held by the customer
    Months_Inactive_12_mon INT, -- No. of months inactive in the last 12 months
    Contacts_Count_12_mon INT, -- No. of Contacts in the last 12 months
    Total_Revolving_Bal INT, -- Total Revolving Balance on the Credit Card
    Total_Trans_Amt DECIMAL(7,2), -- Total Transaction Amount (Last 12 months)
    Total_Amt_Chng_Q4_Q1 DECIMAL(4,3), -- Change in Transaction Amount (Q4 over Q1) 
    Total_Trans_Ct INT, -- Total Transaction Count (Last 12 months)
    Total_Ct_Chng_Q4_Q1 DECIMAL(4,3), -- Change in Transaction Count (Q4 over Q1) 
    FOREIGN KEY (Card_ID) REFERENCES Bank.BankCard(Card_ID),
    Load_Date DATETIME,
    Run_ID BIGINT NOT NULL
);

