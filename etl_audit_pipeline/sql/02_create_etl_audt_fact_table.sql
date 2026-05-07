-- =====================================================
-- ETL_AUDT_FACT Table - Audit Log/Fact Table
-- =====================================================
-- This table stores audit information for each job run
-- Tracks job execution details, run items, processing status, etc.

CREATE TABLE IF NOT EXISTS ETL_AUDT_FACT (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    edf_job_run_item_id VARCHAR(255) NOT NULL,
    processed_flag CHAR(1) DEFAULT 'N',
    run_status VARCHAR(50),
    job_start_time TIMESTAMP,
    job_end_time TIMESTAMP,
    source_count BIGINT,
    target_count BIGINT,
    error_count BIGINT DEFAULT 0,
    error_message VARCHAR(4000),
    batch_no INT,
    notebook_name VARCHAR(255),
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_processed_flag CHECK (processed_flag IN ('Y', 'N'))
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_job_run ON ETL_AUDT_FACT(edf_job_run_item_id);
CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_processed ON ETL_AUDT_FACT(processed_flag);
CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_batch ON ETL_AUDT_FACT(batch_no);
CREATE INDEX IF NOT EXISTS idx_etl_audt_fact_status ON ETL_AUDT_FACT(run_status);

COMMENT ON TABLE ETL_AUDT_FACT IS 'Audit fact table storing job execution details and processing status';
COMMENT ON COLUMN ETL_AUDT_FACT.edf_job_run_item_id IS 'Unique identifier for each job run item';
COMMENT ON COLUMN ETL_AUDT_FACT.processed_flag IS 'Y = Processed, N = Not Processed';
COMMENT ON COLUMN ETL_AUDT_FACT.run_status IS 'Job execution status (SUCCESS, FAILED, RUNNING, etc.)';

-- Made with Bob
