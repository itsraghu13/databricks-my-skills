# ETL Audit Pipeline System

A comprehensive ETL audit and orchestration system for managing batch data processing with change table handling and audit logging.

## 📋 Overview

This system implements an ETL audit pipeline that:
- Tracks job execution through reference and audit tables
- Handles both CHG (Change) and FINAL table types with appropriate write modes
- Provides batch processing with loop orchestration
- Maintains comprehensive audit logs
- Automatically cleans up CHG tables after successful processing

## 🏗️ Architecture

```
┌─────────────────────┐
│  ETL_AUDT_REF(T)   │  ← Reference/Control Table
│  Job metadata       │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│   Notebook1         │  ← Lookup & Validation
│   - Check config    │
│   - Validate data   │
│   - Create audit    │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ ETL_AUDT_FACT(T)   │  ← Audit Log Table
│ Job execution data  │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ ADF Pipeline/       │  ← Orchestration Loop
│ Notebook2 (Loop)    │
│ - Process batches   │
│ - CHG: overwrite    │
│ - FINAL: append     │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ Final Notebook      │  ← Cleanup & Finalization
│ - Update flags      │
│ - Set timestamps    │
│ - Clean CHG tables  │
└─────────────────────┘
```

## 📁 Project Structure

```
etl_audit_pipeline/
├── sql/
│   ├── 01_create_etl_audt_ref_table.sql      # Reference table DDL
│   └── 02_create_etl_audt_fact_table.sql     # Audit fact table DDL
├── notebooks/
│   ├── Notebook1_Lookup_Validation.py        # Initial validation
│   ├── Notebook2_Processing_Loop.py          # Batch processing loop
│   └── Final_Notebook_Cleanup.py             # Cleanup and finalization
├── adf/
│   └── pipeline_etl_audit_orchestration.json # ADF pipeline definition
├── docs/
│   └── architecture_diagram.png              # System architecture
└── README.md                                  # This file
```

## 🚀 Setup Instructions

### 1. Database Setup

Execute the SQL scripts in order:

```sql
-- Step 1: Create reference table
source sql/01_create_etl_audt_ref_table.sql

-- Step 2: Create audit fact table
source sql/02_create_etl_audt_fact_table.sql
```

### 2. Databricks Notebook Setup

Upload notebooks to your Databricks workspace:

1. Navigate to Databricks Workspace
2. Create folder: `/Workspace/Notebooks/ETL_Audit/`
3. Import notebooks:
   - `Notebook1_Lookup_Validation.py`
   - `Notebook2_Processing_Loop.py`
   - `Final_Notebook_Cleanup.py`

### 3. Azure Data Factory Setup

1. Create Databricks Linked Service:
   - Name: `AzureDatabricks_LinkedService`
   - Configure authentication and cluster settings

2. Import pipeline:
   - Import `adf/pipeline_etl_audit_orchestration.json`
   - Update notebook paths if needed
   - Configure parameters

## 📊 Table Schemas

### ETL_AUDT_REF (Reference Table)

| Column | Type | Description |
|--------|------|-------------|
| job_name | VARCHAR(255) | Job identifier (PK) |
| src_table1 | VARCHAR(255) | Source table name |
| target_table | VARCHAR(255) | Target table name (PK) |
| chg_final | VARCHAR(50) | Table type: CHG or FINAL |
| desc_text | VARCHAR(500) | Job description |
| create_ts | TIMESTAMP | Record creation timestamp |
| update_ts | TIMESTAMP | Last update timestamp |
| active_flag | CHAR(1) | Active status (Y/N) |

### ETL_AUDT_FACT (Audit Log Table)

| Column | Type | Description |
|--------|------|-------------|
| audit_id | BIGINT | Auto-increment primary key |
| edf_job_run_item_id | VARCHAR(255) | Job run item identifier |
| processed_flag | CHAR(1) | Processing status (Y/N) |
| run_status | VARCHAR(50) | Execution status |
| job_start_time | TIMESTAMP | Job start time |
| job_end_time | TIMESTAMP | Job end time |
| source_count | BIGINT | Source record count |
| target_count | BIGINT | Target record count |
| error_count | BIGINT | Error count |
| error_message | VARCHAR(4000) | Error details |
| batch_no | INT | Batch number |
| notebook_name | VARCHAR(255) | Processing notebook |
| create_ts | TIMESTAMP | Record creation |
| update_ts | TIMESTAMP | Last update |

## 🔄 Workflow

### Step 1: Notebook1 - Lookup & Validation

**Purpose:** Validate job configuration and prepare audit records

**Process:**
1. Lookup `ETL_AUDT_REF` for job configuration
2. Fail if configuration not found
3. Check `ETL_AUDT_FACT` for unprocessed items (Processed_Flag='N')
4. Select distinct `job_run_item_id` from source table
5. Insert new audit records with Processed_Flag='N'

**Parameters:**
- `job_name`: Job identifier
- `src_table1`: Source table name
- `database_name`: Database name

### Step 2: ADF Pipeline / Notebook2 - Processing Loop

**Purpose:** Orchestrate batch processing with appropriate write modes

**Process:**
1. Select batches from `ETL_AUDT_FACT` where Processed_Flag='N'
2. Loop through each batch
3. **If CHG table:** Use `overwrite` mode to replace data
4. **If FINAL table:** Use `append` mode to accumulate data
5. Pass batch_no to processing notebooks
6. Track execution status

**Parameters:**
- `database_name`: Database name
- `max_parallel_batches`: Parallel execution limit
- `notebook_path`: Path to processing notebook

### Step 3: Final Notebook - Cleanup

**Purpose:** Finalize processing and clean up

**Process:**
1. Update `ETL_AUDT_FACT` records:
   - Set Processed_Flag='Y'
   - Set job_end_time
   - Update run_status
2. Validate target table data
3. **If CHG table:** Clean/Delete CHG table for next run
4. Generate summary report

**Parameters:**
- `database_name`: Database name
- `batch_no`: Specific batch (optional)
- `target_table`: Target table name

## 🎯 Key Features

### 1. CHG vs FINAL Table Handling

**CHG Tables (Change Tables):**
- Write mode: `overwrite`
- Purpose: Temporary staging for incremental changes
- Cleanup: Deleted after successful processing
- Use case: CDC (Change Data Capture) scenarios

**FINAL Tables:**
- Write mode: `append`
- Purpose: Accumulate all processed data
- Cleanup: No deletion, data persists
- Use case: Historical data warehouse tables

### 2. Audit Trail

Complete audit trail maintained in `ETL_AUDT_FACT`:
- Job execution times
- Record counts (source/target)
- Error tracking
- Processing status
- Batch information

### 3. Error Handling

- Automatic retry logic in ADF pipeline
- Error count tracking
- Detailed error messages
- Failed batch identification

### 4. Batch Processing

- Configurable batch sizes
- Parallel processing support
- Batch-level status tracking
- Resume capability for failed batches

## 📝 Usage Examples

### Example 1: Run Complete Pipeline via ADF

```json
{
  "job_name": "DAILY_SALES_ETL",
  "src_table1": "staging.sales_data",
  "database_name": "production",
  "target_table": "dwh.sales_fact",
  "chg_final": "FINAL"
}
```

### Example 2: Manual Notebook Execution

```python
# Notebook1 - Validation
dbutils.notebook.run(
    "/Workspace/Notebooks/Notebook1_Lookup_Validation",
    timeout_seconds=600,
    arguments={
        "job_name": "DAILY_SALES_ETL",
        "src_table1": "staging.sales_data",
        "database_name": "production"
    }
)

# Notebook2 - Processing
dbutils.notebook.run(
    "/Workspace/Notebooks/Notebook2_Processing_Loop",
    timeout_seconds=3600,
    arguments={
        "database_name": "production",
        "max_parallel_batches": "5"
    }
)

# Final Notebook - Cleanup
dbutils.notebook.run(
    "/Workspace/Notebooks/Final_Notebook_Cleanup",
    timeout_seconds=600,
    arguments={
        "database_name": "production",
        "target_table": "dwh.sales_fact"
    }
)
```

### Example 3: Query Audit Status

```sql
-- Check processing status
SELECT 
    batch_no,
    COUNT(*) as total_items,
    SUM(CASE WHEN processed_flag = 'Y' THEN 1 ELSE 0 END) as processed,
    SUM(CASE WHEN processed_flag = 'N' THEN 1 ELSE 0 END) as pending,
    MAX(update_ts) as last_update
FROM ETL_AUDT_FACT
GROUP BY batch_no
ORDER BY batch_no;

-- Check failed jobs
SELECT 
    edf_job_run_item_id,
    run_status,
    error_count,
    error_message,
    job_start_time,
    job_end_time
FROM ETL_AUDT_FACT
WHERE run_status = 'FAILED'
ORDER BY job_start_time DESC;
```

## 🔧 Configuration

### ADF Pipeline Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| job_name | Yes | - | Job identifier |
| src_table1 | Yes | - | Source table |
| database_name | No | default | Database name |
| target_table | Yes | - | Target table |
| chg_final | No | FINAL | Table type (CHG/FINAL) |

### Notebook Parameters

All notebooks support dynamic parameters via widgets for flexible execution.

## 🐛 Troubleshooting

### Issue: Job not found in ETL_AUDT_REF

**Solution:** Insert job configuration:
```sql
INSERT INTO ETL_AUDT_REF (job_name, src_table1, target_table, chg_final)
VALUES ('YOUR_JOB', 'source_table', 'target_table', 'FINAL');
```

### Issue: No unprocessed records

**Solution:** Check if all records are already processed:
```sql
SELECT processed_flag, COUNT(*) 
FROM ETL_AUDT_FACT 
GROUP BY processed_flag;
```

### Issue: CHG table not cleaned up

**Solution:** Manually run cleanup:
```sql
DELETE FROM your_chg_table WHERE processed_flag = 'Y';
```

## 📈 Monitoring

Monitor pipeline health using:

1. **ADF Monitoring Dashboard**
   - Pipeline runs
   - Activity duration
   - Failure rates

2. **Audit Queries**
   - Processing status
   - Error trends
   - Performance metrics

3. **Databricks Job Logs**
   - Detailed execution logs
   - Spark metrics
   - Resource utilization

## 🤝 Contributing

To extend this system:

1. Add new validation rules in Notebook1
2. Implement custom processing logic in Notebook2
3. Add cleanup procedures in Final Notebook
4. Update ADF pipeline for new activities

## 📄 License

Internal use only - Company proprietary system

## 👥 Support

For issues or questions:
- Create ticket in JIRA
- Contact Data Engineering team
- Email: data-engineering@company.com

---

**Version:** 1.0.0  
**Last Updated:** 2026-05-07  
**Maintained by:** Data Engineering Team