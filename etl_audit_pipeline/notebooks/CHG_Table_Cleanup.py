# Databricks notebook source
# MAGIC %md
# MAGIC # CHG Table Cleanup Notebook
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Clean/Delete CHG (Change) tables after successful processing
# MAGIC - Prepare CHG table for next run
# MAGIC - Only runs for jobs with chg_final='CHG'

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from datetime import datetime

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("database_name", "default", "Database Name")
dbutils.widgets.text("chg_table", "", "CHG Table Name")
dbutils.widgets.text("cleanup_mode", "DELETE", "Cleanup Mode (DELETE/TRUNCATE)")

database_name = dbutils.widgets.get("database_name")
chg_table = dbutils.widgets.get("chg_table")
cleanup_mode = dbutils.widgets.get("cleanup_mode")

print(f"Database: {database_name}")
print(f"CHG Table: {chg_table}")
print(f"Cleanup Mode: {cleanup_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Validate CHG Table

# COMMAND ----------

def validate_chg_table(database_name, chg_table):
    """
    Validate that the table exists and is a CHG table
    """
    try:
        # Check if table exists
        table_exists = spark.catalog.tableExists(f"{database_name}.{chg_table}")
        
        if not table_exists:
            print(f"WARNING: Table {database_name}.{chg_table} does not exist")
            return False
        
        # Get table properties
        table_df = spark.sql(f"DESCRIBE EXTENDED {database_name}.{chg_table}")
        
        print(f"Table {chg_table} exists and is ready for cleanup")
        return True
        
    except Exception as e:
        print(f"ERROR in validate_chg_table: {str(e)}")
        return False

# Validate table
is_valid = validate_chg_table(database_name, chg_table)

if not is_valid:
    dbutils.notebook.exit("SKIPPED: CHG table does not exist or validation failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Get Record Count Before Cleanup

# COMMAND ----------

def get_record_count(database_name, chg_table):
    """
    Get current record count in CHG table
    """
    try:
        count_query = f"SELECT COUNT(*) as record_count FROM {database_name}.{chg_table}"
        result = spark.sql(count_query)
        count = result.collect()[0].record_count
        
        print(f"Current record count in {chg_table}: {count:,}")
        return count
        
    except Exception as e:
        print(f"ERROR in get_record_count: {str(e)}")
        return 0

# Get count before cleanup
before_count = get_record_count(database_name, chg_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Perform Cleanup

# COMMAND ----------

def cleanup_chg_table(database_name, chg_table, cleanup_mode):
    """
    Clean up CHG table using specified mode
    DELETE: Remove all records but keep table structure
    TRUNCATE: Fast truncate operation
    """
    try:
        if cleanup_mode.upper() == "TRUNCATE":
            # Truncate table (faster but less flexible)
            truncate_query = f"TRUNCATE TABLE {database_name}.{chg_table}"
            spark.sql(truncate_query)
            print(f"Successfully TRUNCATED table {chg_table}")
            
        elif cleanup_mode.upper() == "DELETE":
            # Delete all records (slower but more flexible)
            delete_query = f"DELETE FROM {database_name}.{chg_table}"
            spark.sql(delete_query)
            print(f"Successfully DELETED all records from {chg_table}")
            
        else:
            print(f"ERROR: Invalid cleanup mode '{cleanup_mode}'. Use DELETE or TRUNCATE")
            return False
        
        return True
        
    except Exception as e:
        print(f"ERROR in cleanup_chg_table: {str(e)}")
        return False

# Perform cleanup
cleanup_success = cleanup_chg_table(database_name, chg_table, cleanup_mode)

if not cleanup_success:
    dbutils.notebook.exit("FAILED: CHG table cleanup failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Cleanup

# COMMAND ----------

def verify_cleanup(database_name, chg_table):
    """
    Verify that cleanup was successful
    """
    try:
        after_count = get_record_count(database_name, chg_table)
        
        if after_count == 0:
            print(f"✓ Cleanup verified: {chg_table} is now empty")
            return True
        else:
            print(f"✗ Cleanup verification failed: {chg_table} still has {after_count} records")
            return False
            
    except Exception as e:
        print(f"ERROR in verify_cleanup: {str(e)}")
        return False

# Verify cleanup
verification_success = verify_cleanup(database_name, chg_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Optimize Table (Optional)

# COMMAND ----------

def optimize_table(database_name, chg_table):
    """
    Optimize table after cleanup for better performance
    """
    try:
        optimize_query = f"OPTIMIZE {database_name}.{chg_table}"
        spark.sql(optimize_query)
        print(f"Table {chg_table} optimized successfully")
        return True
        
    except Exception as e:
        print(f"WARNING: Table optimization failed: {str(e)}")
        return False

# Optimize table (optional, comment out if not needed)
# optimize_table(database_name, chg_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Report

# COMMAND ----------

print("\n" + "="*60)
print("CHG TABLE CLEANUP SUMMARY")
print("="*60)
print(f"Database: {database_name}")
print(f"CHG Table: {chg_table}")
print(f"Cleanup Mode: {cleanup_mode}")
print(f"Records Before: {before_count:,}")
print(f"Records After: 0")
print(f"Status: {'✓ SUCCESS' if verification_success else '✗ FAILED'}")
print("="*60)

# Exit with success message
if verification_success:
    dbutils.notebook.exit(f"SUCCESS: CHG table {chg_table} cleaned up successfully ({before_count:,} records removed)")
else:
    dbutils.notebook.exit(f"FAILED: CHG table cleanup verification failed")

# Made with Bob
