-- =====================================================
-- ETL_AUDT_REF Table - Databricks Unity Catalog
-- =====================================================
-- Catalog: dev_edf_silver
-- Schema: dps_stage
-- Table: ETL_AUDT_REF
-- =====================================================

USE CATALOG dev_edf_silver;
USE SCHEMA dps_stage;

-- Drop table if exists (for clean setup)
-- DROP TABLE IF EXISTS dev_edf_silver.dps_stage.ETL_AUDT_REF;

-- Create ETL_AUDT_REF table
CREATE TABLE IF NOT EXISTS dev_edf_silver.dps_stage.ETL_AUDT_REF (
    job_name STRING NOT NULL COMMENT 'Job identifier',
    src_table1 STRING COMMENT 'Source table name',
    target_table STRING NOT NULL COMMENT 'Target table name',
    chg_final STRING COMMENT 'Table type: CHG or FINAL',
    desc_text STRING COMMENT 'Job description',
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp',
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Last update timestamp',
    active_flag STRING DEFAULT 'Y' COMMENT 'Active status (Y/N)',
    CONSTRAINT pk_etl_audt_ref PRIMARY KEY (job_name, target_table)
)
USING DELTA
COMMENT 'Reference table for ETL job metadata and target table mappings'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_etl_audt_ref_job 
ON dev_edf_silver.dps_stage.ETL_AUDT_REF (job_name);

CREATE INDEX IF NOT EXISTS idx_etl_audt_ref_target 
ON dev_edf_silver.dps_stage.ETL_AUDT_REF (target_table);

CREATE INDEX IF NOT EXISTS idx_etl_audt_ref_active 
ON dev_edf_silver.dps_stage.ETL_AUDT_REF (active_flag);

-- Insert sample data
INSERT INTO dev_edf_silver.dps_stage.ETL_AUDT_REF 
(job_name, src_table1, target_table, chg_final, desc_text) 
VALUES
('JOB_SALES_ETL', 'dev_edf_bronze.staging.sales_data', 'dev_edf_silver.dps_stage.sales_fact', 'FINAL', 'Daily sales ETL job'),
('JOB_CUSTOMER_CDC', 'dev_edf_bronze.staging.customer_changes', 'dev_edf_silver.dps_stage.customer_chg', 'CHG', 'Customer CDC processing'),
('JOB_INVENTORY_ETL', 'dev_edf_bronze.staging.inventory_data', 'dev_edf_silver.dps_stage.inventory_fact', 'FINAL', 'Inventory ETL job');

-- Verify table creation
SELECT 
    'ETL_AUDT_REF' as table_name,
    COUNT(*) as record_count,
    MAX(create_ts) as latest_record
FROM dev_edf_silver.dps_stage.ETL_AUDT_REF;

-- Show table properties
DESCRIBE EXTENDED dev_edf_silver.dps_stage.ETL_AUDT_REF;

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON TABLE dev_edf_silver.dps_stage.ETL_AUDT_REF TO `data_engineers`;
-- GRANT SELECT ON TABLE dev_edf_silver.dps_stage.ETL_AUDT_REF TO `data_analysts`;

-- Made with Bob
