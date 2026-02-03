/*==========================================================
1) ETL Log tables
============================================================*/
CREATE TABLE ETLLog.RunHeader (
    Run_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    Job_Name SYSNAME NOT NULL,
    Source_File_Name NVARCHAR(MAX),
    Start_Time DATETIME NOT NULL DEFAULT SYSUTCDATETIME(),
    End_Time DATETIME,
    Run_Status NVARCHAR(10),        -- (STARTED / SUCCESS / FAILED / PARTIAL)
    ErrorMessage NVARCHAR(MAX)
);

CREATE TABLE ETLLog.RunStep (
    RunStep_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    Run_ID BIGINT FOREIGN KEY REFERENCES ETLLog.RunHeader(Run_ID) NOT NULL,
    Step_Name NVARCHAR(MAX),
    Target_Table NVARCHAR(MAX),
    Start_Time DATETIME NOT NULL DEFAULT SYSUTCDATETIME(),
    End_Time DATETIME,
    Rows_Read INT NOT NULL DEFAULT 0,
    Rows_Inserted INT NOT NULL DEFAULT 0,
    Rows_Updated INT NOT NULL DEFAULT 0,
    Rows_Rejected INT NOT NULL DEFAULT 0,
    Step_Status NVARCHAR(10),   -- STARTED, SUCCESS, FAILED
    ErrorMessage NVARCHAR(MAX)
);

CREATE TABLE ETLLog.RowReject (
    Reject_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    RunStep_ID BIGINT FOREIGN KEY REFERENCES ETLLog.RunStep(RunStep_ID),
    Source_Row_ID BIGINT,
    Target_Table NVARCHAR(MAX) NOT NULL,
    Reject_Reason NVARCHAR(MAX) NOT NULL,
    Reject_Payload NVARCHAR(MAX) NULL,
    Reject_Date DATETIME DEFAULT SYSUTCDATETIME()
);

CREATE TABLE ETLLog.AppEvent (
    Event_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    Event_Time DATETIME DEFAULT SYSUTCDATETIME(),
    Event_Source NVARCHAR(100), 
    Message NVARCHAR(MAX)
);
