# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook2 - ADF Pipeline / Processing Loop
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Select batch_no, count from ETL_AUDT_FACT
# MAGIC - Loop all processing Notebooks
# MAGIC - Pass Batch_No and other parameters to starting Notebooks

# COMMAND ----------

from pyspark.sql.functions import col, count, lit
from datetime import datetime
import json

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("database_name", "default", "Database Name")
dbutils.widgets.text("max_parallel_batches", "5", "Max Parallel Batches")
dbutils.widgets.text("notebook_path", "/Workspace/Notebooks/Processing", "Processing Notebook Path")

database_name = dbutils.widgets.get("database_name")
max_parallel_batches = int(dbutils.widgets.get("max_parallel_batches"))
notebook_path = dbutils.widgets.get("notebook_path")

print(f"Database: {database_name}")
print(f"Max Parallel Batches: {max_parallel_batches}")
print(f"Notebook Path: {notebook_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get Unprocessed Batches from ETL_AUDT_FACT

# COMMAND ----------

def get_unprocessed_batches():
    """
    Select batch_no and count from ETL_AUDT_FACT where Processed_Flag='N'
    Group by batch_no to get count of items per batch
    """
    try:
        query = f"""
            SELECT 
                batch_no,
                COUNT(*) as item_count,
                MIN(edf_job_run_item_id) as first_item_id,
                MAX(edf_job_run_item_id) as last_item_id
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'N'
            AND batch_no IS NOT NULL
            GROUP BY batch_no
            ORDER BY batch_no
        """
        
        batch_df = spark.sql(query)
        batch_count = batch_df.count()
        
        print(f"Found {batch_count} unprocessed batch(es)")
        
        if batch_count > 0:
            batch_df.show(truncate=False)
        
        return batch_df
        
    except Exception as e:
        print(f"ERROR in get_unprocessed_batches: {str(e)}")
        raise

# Get unprocessed batches
batches_df = get_unprocessed_batches()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Process Each Batch

# COMMAND ----------

def process_batch(batch_no, item_count):
    """
    Process a single batch by calling the processing notebook
    """
    try:
        print(f"\n{'='*60}")
        print(f"Processing Batch {batch_no} with {item_count} items")
        print(f"{'='*60}")
        
        # Parameters to pass to processing notebook
        params = {
            "batch_no": str(batch_no),
            "database_name": database_name,
            "item_count": str(item_count)
        }
        
        # Call the processing notebook
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=3600,  # 1 hour timeout
            arguments=params
        )
        
        print(f"Batch {batch_no} completed with result: {result}")
        return True, result
        
    except Exception as e:
        error_msg = f"ERROR processing batch {batch_no}: {str(e)}"
        print(error_msg)
        return False, error_msg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Loop Through All Batches

# COMMAND ----------

def process_all_batches(batches_df):
    """
    Loop through all unprocessed batches and process them
    """
    results = []
    
    if batches_df.count() == 0:
        print("No unprocessed batches found")
        return results
    
    # Collect batch information
    batches = batches_df.collect()
    
    print(f"\nStarting to process {len(batches)} batch(es)")
    print(f"{'='*60}\n")
    
    for row in batches:
        batch_no = row.batch_no
        item_count = row.item_count
        
        success, result = process_batch(batch_no, item_count)
        
        results.append({
            "batch_no": batch_no,
            "item_count": item_count,
            "success": success,
            "result": result,
            "timestamp": datetime.now().isoformat()
        })
    
    return results

# Process all batches
processing_results = process_all_batches(batches_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Summary Report

# COMMAND ----------

def generate_summary_report(results):
    """
    Generate a summary report of all batch processing
    """
    print("\n" + "="*60)
    print("BATCH PROCESSING SUMMARY")
    print("="*60)
    
    total_batches = len(results)
    successful_batches = sum(1 for r in results if r["success"])
    failed_batches = total_batches - successful_batches
    total_items = sum(r["item_count"] for r in results)
    
    print(f"Total Batches Processed: {total_batches}")
    print(f"Successful Batches: {successful_batches}")
    print(f"Failed Batches: {failed_batches}")
    print(f"Total Items: {total_items}")
    print("="*60)
    
    if failed_batches > 0:
        print("\nFailed Batches:")
        for r in results:
            if not r["success"]:
                print(f"  - Batch {r['batch_no']}: {r['result']}")
    
    print("\nDetailed Results:")
    for r in results:
        status = "✓ SUCCESS" if r["success"] else "✗ FAILED"
        print(f"  Batch {r['batch_no']}: {status} ({r['item_count']} items)")
    
    print("="*60)
    
    return {
        "total_batches": total_batches,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "total_items": total_items
    }

# Generate summary
summary = generate_summary_report(processing_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exit with Results

# COMMAND ----------

# Exit with summary
exit_message = f"Processed {summary['total_batches']} batches: {summary['successful_batches']} successful, {summary['failed_batches']} failed"

if summary['failed_batches'] > 0:
    dbutils.notebook.exit(f"PARTIAL_SUCCESS: {exit_message}")
else:
    dbutils.notebook.exit(f"SUCCESS: {exit_message}")

# Made with Bob
