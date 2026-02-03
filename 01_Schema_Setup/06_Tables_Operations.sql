/*==========================================================
06_Tables_Operations
============================================================*/

CREATE TABLE Operations.ChurnAlert (
    Alert_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    CLIENTNUM INT NOT NULL FOREIGN KEY REFERENCES Person.Customer(CLIENTNUM),
    Alert_Date DATETIME DEFAULT SYSUTCDATETIME(),
    Alert_Type VARCHAR(50),  -- e.g., 'High Risk Activity', 'Model Prediction'
    Risk_Score DECIMAL(11,10), -- The Naive Bayes score at time of alert
    Contacts_Count INT, -- Snapshot of data at time of alert
    Months_Inactive INT, -- Snapshot of data at time of alert
    Is_Resolved BIT DEFAULT 0, -- 0 = Open, 1 = Handled by agent
    Resolved_Date DATETIME NULL,
    Resolved_Notes NVARCHAR(MAX) NULL
);

