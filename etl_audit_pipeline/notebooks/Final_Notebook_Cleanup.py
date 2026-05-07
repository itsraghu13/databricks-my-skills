# Databricks notebook source
# MAGIC %md
# MAGIC # Final Notebook - Cleanup and Finalization
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - After other main loops have updated target list in ETL_AUDT_FACT
# MAGIC - Make Processed_Flag='Y'
# MAGIC - Update End_Timestamp_Dttm
# MAGIC - Update StatusCompleteFailure etc
# MAGIC - Complete table

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, when
from datetime import datetime

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("database_name", "default", "Database Name")
dbutils.widgets.text("batch_no", "", "Batch Number (optional - leave empty for all)")
dbutils.widgets.text("target_table", "", "Target Table Name")

database_name = dbutils.widgets.get("database_name")
batch_no = dbutils.widgets.get("batch_no")
target_table = dbutils.widgets.get("target_table")

print(f"Database: {database_name}")
print(f"Batch Number: {batch_no if batch_no else 'ALL'}")
print(f"Target Table: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get Records to Update

# COMMAND ----------

def get_records_to_update(batch_no=None):
    """
    Get records from ETL_AUDT_FACT that need to be updated
    Filter by batch_no if provided
    """
    try:
        base_query = f"""
            SELECT 
                audit_id,
                edf_job_run_item_id,
                processed_flag,
                run_status,
                job_start_time,
                job_end_time,
                source_count,
                target_count,
                error_count,
                batch_no,
                notebook_name
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'N'
        """
        
        if batch_no:
            query = base_query + f" AND batch_no = {batch_no}"
        else:
            query = base_query
        
        records_df = spark.sql(query)
        count = records_df.count()
        
        print(f"Found {count} record(s) to update")
        
        if count > 0:
            records_df.show(10, truncate=False)
        
        return records_df, count
        
    except Exception as e:
        print(f"ERROR in get_records_to_update: {str(e)}")
        raise

# Get records to update
records_df, record_count = get_records_to_update(batch_no if batch_no else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Target Table Data

# COMMAND ----------

def validate_target_table_data(target_table, job_run_item_ids):
    """
    Validate that data was successfully loaded into target table
    Returns count of records in target table for given job_run_item_ids
    """
    try:
        if not target_table:
            print("WARNING: No target table specified, skipping validation")
            return None
        
        # Create list of IDs for SQL IN clause
        id_list = "','".join(job_run_item_ids)
        
        query = f"""
            SELECT 
                COUNT(*) as target_count
            FROM {database_name}.{target_table}
            WHERE edf_job_run_item_id IN ('{id_list}')
        """
        
        result = spark.sql(query)
        target_count = result.collect()[0].target_count
        
        print(f"Target table '{target_table}' contains {target_count} records for processed items")
        
        return target_count
        
    except Exception as e:
        print(f"ERROR in validate_target_table_data: {str(e)}")
        return None

# Validate if target table specified
if record_count > 0 and target_table:
    job_run_ids = [row.edf_job_run_item_id for row in records_df.collect()]
    target_count = validate_target_table_data(target_table, job_run_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Update ETL_AUDT_FACT Records

# COMMAND ----------

def update_etl_audt_fact(batch_no=None):
    """
    Update ETL_AUDT_FACT records:
    - Set processed_flag = 'Y'
    - Set job_end_time = current_timestamp
    - Update run_status based on error_count
    - Update update_ts = current_timestamp
    """
    try:
        # Build WHERE clause
        where_clause = "processed_flag = 'N'"
        if batch_no:
            where_clause += f" AND batch_no = {batch_no}"
        
        # Update query
        update_query = f"""
            UPDATE {database_name}.ETL_AUDT_FACT
            SET 
                processed_flag = 'Y',
                job_end_time = CURRENT_TIMESTAMP(),
                run_status = CASE 
                    WHEN error_count > 0 THEN 'COMPLETED_WITH_ERRORS'
                    WHEN run_status = 'FAILED' THEN 'FAILED'
                    ELSE 'SUCCESS'
                END,
                update_ts = CURRENT_TIMESTAMP()
            WHERE {where_clause}
        """
        
        # Execute update
        spark.sql(update_query)
        
        # Get count of updated records
        count_query = f"""
            SELECT COUNT(*) as updated_count
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'Y'
            AND update_ts >= CURRENT_TIMESTAMP() - INTERVAL 1 MINUTE
        """
        
        result = spark.sql(count_query)
        updated_count = result.collect()[0].updated_count
        
        print(f"SUCCESS: Updated {updated_count} record(s) in ETL_AUDT_FACT")
        
        return updated_count
        
    except Exception as e:
        print(f"ERROR in update_etl_audt_fact: {str(e)}")
        raise

# Update records
if record_count > 0:
    updated_count = update_etl_audt_fact(batch_no if batch_no else None)
else:
    print("No records to update")
    updated_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Updates

# COMMAND ----------

def verify_updates(batch_no=None):
    """
    Verify that updates were successful
    Show summary of processed records
    """
    try:
        base_query = f"""
            SELECT 
                run_status,
                COUNT(*) as count,
                SUM(source_count) as total_source_records,
                SUM(target_count) as total_target_records,
                SUM(error_count) as total_errors
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'Y'
            AND update_ts >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
        """
        
        if batch_no:
            query = base_query + f" AND batch_no = {batch_no} GROUP BY run_status"
        else:
            query = base_query + " GROUP BY run_status"
        
        summary_df = spark.sql(query)
        
        print("\nProcessing Summary:")
        print("="*60)
        summary_df.show(truncate=False)
        
        return summary_df
        
    except Exception as e:
        print(f"ERROR in verify_updates: {str(e)}")
        return None

# Verify updates
if updated_count > 0:
    summary_df = verify_updates(batch_no if batch_no else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Clean Up Old Records (Optional)

# COMMAND ----------

def cleanup_old_records(retention_days=30):
    """
    Optional: Clean up old processed records
    Delete records older than retention_days
    """
    try:
        delete_query = f"""
            DELETE FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'Y'
            AND update_ts < CURRENT_TIMESTAMP() - INTERVAL {retention_days} DAYS
        """
        
        # Get count before deletion
        count_query = f"""
            SELECT COUNT(*) as old_count
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'Y'
            AND update_ts < CURRENT_TIMESTAMP() - INTERVAL {retention_days} DAYS
        """
        
        result = spark.sql(count_query)
        old_count = result.collect()[0].old_count
        
        if old_count > 0:
            spark.sql(delete_query)
            print(f"Cleaned up {old_count} old record(s) (older than {retention_days} days)")
        else:
            print(f"No old records to clean up (retention: {retention_days} days)")
        
        return old_count
        
    except Exception as e:
        print(f"ERROR in cleanup_old_records: {str(e)}")
        return 0

# Uncomment to enable cleanup
# cleanup_count = cleanup_old_records(retention_days=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("\n" + "="*60)
print("FINAL NOTEBOOK EXECUTION SUMMARY")
print("="*60)
print(f"Database: {database_name}")
print(f"Batch Number: {batch_no if batch_no else 'ALL'}")
print(f"Target Table: {target_table if target_table else 'N/A'}")
print(f"Records Found: {record_count}")
print(f"Records Updated: {updated_count}")
print("="*60)

# Exit with success message
if updated_count > 0:
    dbutils.notebook.exit(f"SUCCESS: Updated {updated_count} records to Processed_Flag='Y'")
else:
    dbutils.notebook.exit("SUCCESS: No records to update")

# Made with Bob
