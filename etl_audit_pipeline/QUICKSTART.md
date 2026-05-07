# Quick Start Guide - ETL Audit Pipeline

Get up and running with the ETL Audit Pipeline in 15 minutes.

## 🚀 Prerequisites

- Azure Databricks workspace
- Azure Data Factory instance
- Database access (SQL Server, Synapse, or Databricks SQL)
- Appropriate permissions to create tables and run notebooks

## 📝 Step-by-Step Setup

### Step 1: Create Database Tables (5 minutes)

```sql
-- Connect to your database and run these scripts in order:

-- 1. Create ETL_AUDT_REF table
-- Copy and execute: sql/01_create_etl_audt_ref_table.sql

-- 2. Create ETL_AUDT_FACT table
-- Copy and execute: sql/02_create_etl_audt_fact_table.sql

-- 3. Verify tables were created
SHOW TABLES LIKE 'ETL_AUDT%';
```

### Step 2: Configure Reference Data (2 minutes)

```sql
-- Add your job configuration to ETL_AUDT_REF
INSERT INTO ETL_AUDT_REF (
    job_name, 
    src_table1, 
    target_table, 
    chg_final, 
    desc_text
) VALUES (
    'MY_FIRST_JOB',           -- Your job name
    'staging.my_source',       -- Your source table
    'dwh.my_target',          -- Your target table
    'FINAL',                  -- 'CHG' or 'FINAL'
    'My first ETL job'        -- Description
);

-- Verify insertion
SELECT * FROM ETL_AUDT_REF WHERE job_name = 'MY_FIRST_JOB';
```

### Step 3: Upload Notebooks to Databricks (5 minutes)

1. **Open Databricks Workspace**
   - Navigate to your Databricks workspace
   - Go to Workspace → Users → [Your User]

2. **Create Folder Structure**
   ```
   /Workspace/Notebooks/ETL_Audit/
   ```

3. **Import Notebooks**
   - Click "Import" in the folder
   - Upload these files:
     - `notebooks/Notebook1_Lookup_Validation.py`
     - `notebooks/Notebook2_Processing_Loop.py`
     - `notebooks/Final_Notebook_Cleanup.py`
     - `notebooks/CHG_Table_Cleanup.py`

4. **Verify Upload**
   - Open each notebook
   - Check that all cells are visible
   - No import needed, just upload

### Step 4: Test Individual Notebooks (3 minutes)

#### Test Notebook1 - Validation

```python
# In Databricks, run Notebook1 with these parameters:
{
    "job_name": "MY_FIRST_JOB",
    "src_table1": "staging.my_source",
    "database_name": "default"
}
```

Expected output: Records inserted into ETL_AUDT_FACT

#### Verify in Database

```sql
-- Check audit records were created
SELECT * FROM ETL_AUDT_FACT 
WHERE processed_flag = 'N'
ORDER BY create_ts DESC
LIMIT 10;
```

### Step 5: Setup Azure Data Factory (Optional)

If using ADF for orchestration:

1. **Create Databricks Linked Service**
   - Name: `AzureDatabricks_LinkedService`
   - Configure cluster and authentication

2. **Import Pipeline**
   - Import `adf/pipeline_etl_audit_orchestration.json`
   - Update notebook paths to match your workspace

3. **Configure Parameters**
   ```json
   {
     "job_name": "MY_FIRST_JOB",
     "src_table1": "staging.my_source",
     "database_name": "default",
     "target_table": "dwh.my_target",
     "chg_final": "FINAL"
   }
   ```

## 🎯 First Run

### Option A: Manual Execution (Recommended for Testing)

```python
# Run in Databricks notebook or Python script

# Step 1: Validation
result1 = dbutils.notebook.run(
    "/Workspace/Notebooks/ETL_Audit/Notebook1_Lookup_Validation",
    timeout_seconds=600,
    arguments={
        "job_name": "MY_FIRST_JOB",
        "src_table1": "staging.my_source",
        "database_name": "default"
    }
)
print(f"Notebook1 Result: {result1}")

# Step 2: Processing (if you have processing logic)
result2 = dbutils.notebook.run(
    "/Workspace/Notebooks/ETL_Audit/Notebook2_Processing_Loop",
    timeout_seconds=3600,
    arguments={
        "database_name": "default",
        "max_parallel_batches": "5"
    }
)
print(f"Notebook2 Result: {result2}")

# Step 3: Cleanup
result3 = dbutils.notebook.run(
    "/Workspace/Notebooks/ETL_Audit/Final_Notebook_Cleanup",
    timeout_seconds=600,
    arguments={
        "database_name": "default",
        "target_table": "dwh.my_target"
    }
)
print(f"Final Notebook Result: {result3}")
```

### Option B: ADF Pipeline Execution

1. Open Azure Data Factory
2. Navigate to your pipeline
3. Click "Debug" or "Trigger Now"
4. Enter parameters
5. Monitor execution

## 🔍 Verify Success

### Check Audit Records

```sql
-- View all audit records
SELECT 
    edf_job_run_item_id,
    processed_flag,
    run_status,
    job_start_time,
    job_end_time,
    source_count,
    target_count,
    error_count
FROM ETL_AUDT_FACT
ORDER BY create_ts DESC
LIMIT 20;

-- Check processing summary
SELECT 
    processed_flag,
    run_status,
    COUNT(*) as count,
    SUM(source_count) as total_source,
    SUM(target_count) as total_target
FROM ETL_AUDT_FACT
GROUP BY processed_flag, run_status;
```

### Expected Results

✅ **Success Indicators:**
- Records in ETL_AUDT_FACT with `processed_flag = 'Y'`
- `run_status = 'SUCCESS'`
- `source_count` and `target_count` populated
- `error_count = 0`

❌ **Failure Indicators:**
- `run_status = 'FAILED'`
- `error_count > 0`
- `error_message` populated

## 🐛 Common Issues & Solutions

### Issue 1: "Job configuration not found"

**Cause:** Missing entry in ETL_AUDT_REF

**Solution:**
```sql
INSERT INTO ETL_AUDT_REF (job_name, src_table1, target_table, chg_final)
VALUES ('YOUR_JOB', 'source_table', 'target_table', 'FINAL');
```

### Issue 2: "Table does not exist"

**Cause:** Source or target table not found

**Solution:**
```sql
-- Verify table exists
SHOW TABLES LIKE 'your_table_name';

-- Create table if needed
CREATE TABLE IF NOT EXISTS your_table_name (...);
```

### Issue 3: "No records to process"

**Cause:** All records already processed

**Solution:**
```sql
-- Check processed status
SELECT processed_flag, COUNT(*) 
FROM ETL_AUDT_FACT 
GROUP BY processed_flag;

-- Reset if needed (for testing only)
UPDATE ETL_AUDT_FACT 
SET processed_flag = 'N' 
WHERE processed_flag = 'Y';
```

### Issue 4: Notebook import errors

**Cause:** PySpark/dbutils not recognized in IDE

**Solution:** These are Databricks-specific libraries. Errors in VS Code are expected. The notebooks will run correctly in Databricks.

## 📊 Monitoring Dashboard Queries

### Real-time Processing Status

```sql
-- Current processing status
SELECT 
    CASE 
        WHEN processed_flag = 'N' THEN 'In Progress'
        WHEN processed_flag = 'Y' AND run_status = 'SUCCESS' THEN 'Completed'
        WHEN processed_flag = 'Y' AND run_status = 'FAILED' THEN 'Failed'
        ELSE 'Unknown'
    END as status,
    COUNT(*) as count,
    MIN(create_ts) as oldest,
    MAX(update_ts) as newest
FROM ETL_AUDT_FACT
GROUP BY processed_flag, run_status;
```

### Performance Metrics

```sql
-- Average processing time
SELECT 
    AVG(TIMESTAMPDIFF(SECOND, job_start_time, job_end_time)) as avg_seconds,
    MIN(TIMESTAMPDIFF(SECOND, job_start_time, job_end_time)) as min_seconds,
    MAX(TIMESTAMPDIFF(SECOND, job_start_time, job_end_time)) as max_seconds
FROM ETL_AUDT_FACT
WHERE job_start_time IS NOT NULL 
AND job_end_time IS NOT NULL;
```

### Error Analysis

```sql
-- Top errors
SELECT 
    error_message,
    COUNT(*) as occurrence_count,
    MAX(update_ts) as last_occurrence
FROM ETL_AUDT_FACT
WHERE error_count > 0
GROUP BY error_message
ORDER BY occurrence_count DESC
LIMIT 10;
```

## 🎓 Next Steps

1. **Customize Processing Logic**
   - Modify Notebook2 to include your specific ETL logic
   - Add data transformations
   - Implement business rules

2. **Add More Jobs**
   - Insert additional job configurations in ETL_AUDT_REF
   - Create job-specific processing notebooks
   - Configure ADF pipelines for each job

3. **Setup Monitoring**
   - Create dashboards in Power BI or Tableau
   - Setup email alerts for failures
   - Configure Azure Monitor alerts

4. **Optimize Performance**
   - Tune batch sizes
   - Configure parallel processing
   - Optimize Spark cluster settings

## 📚 Additional Resources

- **Full Documentation:** See `README.md`
- **Architecture Diagram:** See diagram in task description
- **SQL Scripts:** `sql/` folder
- **Notebooks:** `notebooks/` folder
- **ADF Pipeline:** `adf/` folder

## 💡 Tips

1. **Start Small:** Test with a small dataset first
2. **Monitor Closely:** Watch the first few runs carefully
3. **Use Batch Processing:** Process data in manageable batches
4. **Keep Audit History:** Don't delete old audit records immediately
5. **Document Changes:** Keep track of configuration changes

## 🆘 Getting Help

If you encounter issues:

1. Check the troubleshooting section in README.md
2. Review Databricks job logs
3. Query ETL_AUDT_FACT for error messages
4. Contact the Data Engineering team

---

**Ready to go?** Start with Step 1 and you'll be running in 15 minutes! 🚀