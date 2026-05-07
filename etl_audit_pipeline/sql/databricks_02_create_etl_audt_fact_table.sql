-- =====================================================
-- ETL_AUDT_FACT Table - Databricks Unity Catalog
-- =====================================================
-- Catalog: dev_edf_silver
-- Schema: dps_stage
-- Table: ETL_AUDT_FACT
-- =====================================================

USE CATALOG dev_edf_silver;
USE SCHEMA dps_stage;

-- Drop table if exists (for clean setup)
-- DROP TABLE IF EXISTS dev_edf_silver.dps_stage.ETL_AUDT_FACT;

-- Create ETL_AUDT_FACT table
CREATE TABLE IF NOT EXISTS dev_edf_silver.dps_stage.ETL_AUDT_FACT (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Auto-increment primary key',
    edf_job_run_item_id STRING NOT NULL COMMENT 'Job run item identifier',
    processed_flag STRING DEFAULT 'N' COMMENT 'Processing status (Y/N)',
    run_status STRING COMMENT 'Job execution status (SUCCESS, FAILED, RUNNING, etc.)',
    job_start_time TIMESTAMP COMMENT 'Job start timestamp',
    job_end_time TIMESTAMP COMMENT 'Job end timestamp',
    source_count BIGINT COMMENT 'Source record count',
    target_count BIGINT COMMENT 'Target record count',
    error_count BIGINT DEFAULT 0 COMMENT 'Error count',
    error_message STRING COMMENT 'Error details',
    batch_no INT COMMENT 'Batch number',
    notebook_name STRING COMMENT 'Processing notebook name',
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Last update timestamp',
    CONSTRAINT pk_etl_audt_fact PRIMARY KEY (audit_id),
    CONSTRAINT chk_processed_flag CHECK (processed_flag IN ('Y', 'N'))
)
USING DELTA
PARTITIONED BY (DATE(create_ts))
COMMENT 'Audit fact table storing job execution details and processing status'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration' = 'interval 30 days'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_job_run 
ON dev_edf_silver.dps_stage.ETL_AUDT_FACT (edf_job_run_item_id);

CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_processed 
ON dev_edf_silver.dps_stage.ETL_AUDT_FACT (processed_flag);

CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_batch 
ON dev_edf_silver.dps_stage.ETL_AUDT_FACT (batch_no);

CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_status 
ON dev_edf_silver.dps_stage.ETL_AUDT_FACT (run_status);

CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_create_ts 
ON dev_edf_silver.dps_stage.ETL_AUDT_FACT (create_ts);

-- Add column comments
COMMENT ON COLUMN dev_edf_silver.dps_stage.ETL_AUDT_FACT.audit_id IS 'Unique audit record identifier';
COMMENT ON COLUMN dev_edf_silver.dps_stage.ETL_AUDT_FACT.edf_job_run_item_id IS 'External job run item identifier';
COMMENT ON COLUMN dev_edf_silver.dps_stage.ETL_AUDT_FACT.processed_flag IS 'Y = Processed, N = Not Processed';
COMMENT ON COLUMN dev_edf_silver.dps_stage.ETL_AUDT_FACT.run_status IS 'Job execution status (SUCCESS, FAILED, RUNNING, COMPLETED_WITH_ERRORS)';

-- Insert sample data for testing
INSERT INTO dev_edf_silver.dps_stage.ETL_AUDT_FACT 
(edf_job_run_item_id, processed_flag, run_status, batch_no, notebook_name) 
VALUES
('RUN_001_ITEM_001', 'N', NULL, 1, NULL),
('RUN_001_ITEM_002', 'N', NULL, 1, NULL),
('RUN_001_ITEM_003', 'N', NULL, 2, NULL);

-- Verify table creation
SELECT 
    'ETL_AUDT_FACT' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN processed_flag = 'Y' THEN 1 ELSE 0 END) as processed_count,
    SUM(CASE WHEN processed_flag = 'N' THEN 1 ELSE 0 END) as pending_count,
    MAX(create_ts) as latest_record
FROM dev_edf_silver.dps_stage.ETL_AUDT_FACT;

-- Show table properties
DESCRIBE EXTENDED dev_edf_silver.dps_stage.ETL_AUDT_FACT;

-- Show table history (Delta Lake feature)
DESCRIBE HISTORY dev_edf_silver.dps_stage.ETL_AUDT_FACT;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE dev_edf_silver.dps_stage.ETL_AUDT_FACT TO `data_engineers`;
-- GRANT SELECT ON TABLE dev_edf_silver.dps_stage.ETL_AUDT_FACT TO `data_analysts`;

-- Create view for monitoring (optional)
CREATE OR REPLACE VIEW dev_edf_silver.dps_stage.v_etl_audit_summary AS
SELECT 
    DATE(create_ts) as audit_date,
    processed_flag,
    run_status,
    COUNT(*) as record_count,
    SUM(source_count) as total_source_records,
    SUM(target_count) as total_target_records,
    SUM(error_count) as total_errors,
    MIN(job_start_time) as earliest_start,
    MAX(job_end_time) as latest_end
FROM dev_edf_silver.dps_stage.ETL_AUDT_FACT
GROUP BY DATE(create_ts), processed_flag, run_status
ORDER BY audit_date DESC, processed_flag, run_status;

-- Test the view
SELECT * FROM dev_edf_silver.dps_stage.v_etl_audit_summary;

-- Made with Bob
