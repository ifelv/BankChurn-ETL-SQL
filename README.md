# Bank Customer Churn: End-to-End ETL & Analysis Project

![SQL Server](https://img.shields.io/badge/SQL%20Server-Production-CC2927?style=flat&logo=microsoft-sql-server)
![Python](https://img.shields.io/badge/Python-Data%20Prep-3776AB?style=flat&logo=python)
![ETL](https://img.shields.io/badge/ETL-Pipeline-green)

## 📌 Project Overview
This project implements a robust **End-to-End ETL (Extract, Transform, Load) Framework** for a banking institution. It processes customer data, standardizes it into a normalized relational schema, and performs advanced risk analysis to identify potential customer churn.

Unlike simple query collections, this project simulates a production-grade environment featuring **transaction handling, error logging, audit trails, and role-based security**.

## 📂 Data Source
The dataset used is the **Bank Churners** dataset from Kaggle.
* **Source:** [Kaggle - Credit Card Customers](https://www.kaggle.com/sakshigoyal7/credit-card-customers)
* **Note:** To maintain a lightweight repository, the raw `.csv` and processed `.json` files are **excluded** via `.gitignore`.

**Setup Instructions:**
1. Download the dataset from the link above.
2. Place the `.csv` file in the `04_Source_Scripts/` folder.
3. Run the Python script to convert the CSV to JSON format.

## 🏗️ Project Structure
The repository is organized into the logical flow of the engineering pipeline:

| Folder | Description |
| :--- | :--- |
| **`01_Schema_Setup`** | Database creation, Schema definitions (`ETLLog`, `Staging`, `ETL`, `Person`, `Bank`, `Operations`), Table creation with constraints, and Security Roles. |
| **`02_ETL_Framework`** | Stored Procedures for data loading, the Master Orchestration script, and the custom ETL Logging architecture. |
| **`03_Analysis_Reporting`** | Views for demographics, financial behavior analysis, high-risk churn segmentation, and reporting queries. |
| **`04_Source_Scripts`** | Python script (`converter.py`) for pre-processing raw CSV data into JSON format for SQL ingestion. |

## ⚙️ Key Technical Features

### 1. Robust ETL Architecture
* **Custom Logging Schema:** The `ETLLog` schema tracks every execution (`RunHeader`) and every step (`RunStep`), capturing row counts (Read/Inserted/Rejected) and execution times.
* **Error Handling:** Full use of `TRY...CATCH` blocks with transaction management (`BEGIN TRAN`, `COMMIT`, `ROLLBACK`) to ensure data atomicity.
* **Data Quality Logic:** "Bad data" isn't just discarded; it is captured in a `RowReject` table with specific reasons (e.g., "Invalid Gender Code", "Scientific Notation Error") for auditing.

### 2. Advanced SQL Implementation
* **Merge Statements:** Efficiently handles `UPSERT` operations (Update existing records, Insert new ones).
* **Business Logic** | Stored Procedures to detect high-risk churners and log operational alerts.
* **Window Functions:** Utilized for pagination, quartiles (NTILE), and ranking customers by value.

### 3. Security & Governance
* **Role-Based Access Control (RBAC):** Implementation of specific roles (e.g., `JuniorAnalystRole`) with granular permissions (SELECT only, DENY DELETE).

## 🚀 How to Run This Project

### Prerequisites
* Microsoft SQL Server (Developer or Enterprise Edition)
* Python 3.x 

### Execution Order
1. **Prepare Data:**
   Run the Python script in folder `04_Source_Scripts/` to generate the JSON file. It defaults to looking for `data.csv`, or you can specify filenames:
   ```bash
   python converter.py --input "BankChurners.csv" --output "BankChurners_json_20251218.json"
   ```
   
   > ⚠️ **Critical Step:** Before running the SQL, open `02_ETL_Framework/usp_RunBankChurnersETL.sql` and update the `@FilePath` variable to point to the location of the JSON file on **your** local computer.

2. **Build Database:**
   Execute scripts in `01_Schema_Setup` to create the database, schemas, and tables.

3. **Run ETL:**
   Execute the scripts in `02_ETL_Framework` to create the stored procedures.
   * Finally, run the Master Procedure: `EXEC ETL.usp_RunBankChurnersETL;`

4. **Automation (Optional):**
   To simulate a production environment, you can schedule the Master Procedure using **SQL Server Agent** (requires Developer or Enterprise Edition).

    **Steps to Schedule:**
    1. Open SSMS and expand **SQL Server Agent**.
    2. Right-click **Jobs** > **New Job**.
    3. **General**: Name the job `Daily_BankChurn_ETL`.
    4. **Steps**: Add a new step type **Transact-SQL script (T-SQL)**.
        * Command: `EXEC ETL.usp_RunBankChurnersETL;`
        * Database: `BankChurnersDec`
    5. **Schedules**: Create a new schedule (e.g., Daily at 3:00 AM).

5. **Analyze:**
   Run the scripts in `03_Analysis_Reporting` to generate insights and view the dashboard metrics.

---
*Author: [ifelv]*