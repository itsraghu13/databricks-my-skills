-- =====================================================
-- ETL_AUDT_REF Table - Reference/Control Table
-- =====================================================
-- This table stores metadata about jobs and their target tables
-- Used to track which jobs should write to which tables

CREATE TABLE IF NOT EXISTS ETL_AUDT_REF (
    job_name VARCHAR(255) NOT NULL,
    src_table1 VARCHAR(255),
    target_table VARCHAR(255) NOT NULL,
    chg_final VARCHAR(50),
    desc_text VARCHAR(500),
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active_flag CHAR(1) DEFAULT 'Y',
    PRIMARY KEY (job_name, target_table)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_etl_audt_ref_job ON ETL_AUDT_REF(job_name);
CREATE INDEX IF NOT EXISTS idx_etl_audt_ref_target ON ETL_AUDT_REF(target_table);

-- Sample data insert
INSERT INTO ETL_AUDT_REF (job_name, src_table1, target_table, chg_final, desc_text) VALUES
('JOB_001', 'SOURCE_TABLE_A', 'TARGET_TABLE_X', 'CHG', 'Initial load job for Table X'),
('JOB_002', 'SOURCE_TABLE_B', 'TARGET_TABLE_Y', 'FINAL', 'Final processing job for Table Y'),
('JOB_003', 'SOURCE_TABLE_C', 'TARGET_TABLE_Z', 'CHG', 'Change data capture for Table Z');

COMMENT ON TABLE ETL_AUDT_REF IS 'Reference table for ETL job metadata and target table mappings';

-- Made with Bob
