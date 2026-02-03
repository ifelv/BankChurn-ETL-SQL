/*==========================================================
04_Tables_Core_Person
============================================================*/


CREATE TABLE Person.Gender (
    Gender_ID INT IDENTITY(1,1) PRIMARY KEY,  -- Demographic variable - M=Male, F=Female
    Gender_Code VARCHAR(1) UNIQUE, -- 'M' or 'F'
    Gender_Desc VARCHAR(10),        -- Male, Female
    Load_Date DATETIME
);

CREATE TABLE Person.EducationLevel (
    EducationLevel_ID INT IDENTITY(1,1) PRIMARY KEY,   -- Demographic variable - Educational Qualification of the account holder (example: high school, college graduate, etc.)
    Education_Level VARCHAR(15) UNIQUE, -- High School, Graduate, etc.
    Load_Date DATETIME
);

CREATE TABLE Person.MaritalStatus (
    MaritalStatus_ID INT IDENTITY(1,1) PRIMARY KEY,   -- Demographic variable - Married, Single, Divorced, Unknown
    Marital_Status VARCHAR(20) UNIQUE, -- Married, Single, Divorced, Unknown
    Load_Date DATETIME
);

CREATE TABLE Person.Customer (
    CLIENTNUM INT PRIMARY KEY, -- Client number. Unique identifier for the customer holding the account
    Attrition_Flag VARCHAR(20), -- Internal event (customer activity) variable - if the account is closed then 'Attrited Customer' else 'Existing Customer'
    Customer_Age INT, -- Demographic variable - Customer's Age in Years
    Dependent_count INT, -- Demographic variable - Number of dependents
    Income_Category VARCHAR(50), -- Demographic variable - Annual Income Category of the account holder (< $40K, $40K - 60K, $60K - $80K, $80K-$120K, > $120K, Unknown)
    Gender_ID INT FOREIGN KEY REFERENCES Person.Gender(Gender_ID),
    EducationLevel_ID INT FOREIGN KEY REFERENCES Person.EducationLevel(EducationLevel_ID),
    MaritalStatus_ID INT FOREIGN KEY REFERENCES Person.MaritalStatus(MaritalStatus_ID),
    Load_Date DATETIME,
    Run_ID BIGINT NOT NULL
);

CREATE TABLE Person.NaiveBayesScore (                 
    Score_ID INT IDENTITY(1,1) PRIMARY KEY,
    CLIENTNUM INT,
    Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_1 DECIMAL(11,10),
    Naive_Bayes_Clas_Attr_F_Crd_Cat_Cntacts_Cnt_12_m_Dep_cnt_Edu_L_Ms_Inact_12_mon_2 DECIMAL(11,10),
    FOREIGN KEY (CLIENTNUM) REFERENCES Person.Customer(CLIENTNUM),
    Load_Date DATETIME,
    Run_ID BIGINT NOT NULL
);
