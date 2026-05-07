# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook1 - ETL Audit Lookup and Validation
# MAGIC 
# MAGIC **Purpose:**
# MAGIC - Lookup ETL_AUDT_REF table to check if entry exists for src_table1 and job_name
# MAGIC - If not found, fail the job
# MAGIC - If found, select distinct job_run_item_id from src_table1 where job_run_item_id is from ETL_AUDT_FACT with Processed_Flag='N'
# MAGIC - If no records in ETL_AUDT_FACT, select distinct job_run_item_id from src_table1
# MAGIC - Make entries in ETL_AUDT_FACT with create_ts=null and Processed_Flag='N'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from datetime import datetime

# COMMAND ----------

# Create widgets for parameters
dbutils.widgets.text("job_name", "", "Job Name")
dbutils.widgets.text("src_table1", "", "Source Table 1")
dbutils.widgets.text("catalog_name", "dev_edf_silver", "Catalog Name")
dbutils.widgets.text("schema_name", "dps_stage", "Schema Name")

job_name = dbutils.widgets.get("job_name")
src_table1 = dbutils.widgets.get("src_table1")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Job Name: {job_name}")
print(f"Source Table: {src_table1}")
print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Lookup ETL_AUDT_REF Table

# COMMAND ----------

def lookup_etl_audt_ref(job_name, src_table1):
    """Lookup ETL_AUDT_REF table to check if entry exists"""
    try:
        query = f"""
            SELECT job_name, src_table1, target_table, chg_final, desc_text
            FROM {database_name}.ETL_AUDT_REF
            WHERE job_name = '{job_name}'
            AND src_table1 = '{src_table1}'
            AND active_flag = 'Y'
        """
        ref_df = spark.sql(query)
        if ref_df.count() == 0:
            print(f"ERROR: No entry found in ETL_AUDT_REF")
            return None
        print(f"SUCCESS: Found matching record(s) in ETL_AUDT_REF")
        ref_df.show(truncate=False)
        return ref_df
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return None

ref_data = lookup_etl_audt_ref(job_name, src_table1)
if ref_data is None:
    dbutils.notebook.exit("FAILED: Job configuration not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check ETL_AUDT_FACT for Unprocessed Records

# COMMAND ----------

def get_unprocessed_job_run_items():
    """Check for unprocessed records in ETL_AUDT_FACT"""
    try:
        query = f"""
            SELECT DISTINCT edf_job_run_item_id
            FROM {database_name}.ETL_AUDT_FACT
            WHERE processed_flag = 'N'
        """
        unprocessed_df = spark.sql(query)
        count = unprocessed_df.count()
        print(f"Found {count} unprocessed job_run_item_id(s)")
        if count > 0:
            unprocessed_df.show(10, truncate=False)
        return unprocessed_df, count
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return None, 0

unprocessed_items, unprocessed_count = get_unprocessed_job_run_items()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Select job_run_item_id from Source Table

# COMMAND ----------

def get_job_run_items_from_source(src_table1, unprocessed_items, unprocessed_count):
    """Select distinct job_run_item_id from source table"""
    try:
        if unprocessed_count > 0:
            unprocessed_ids = [row.edf_job_run_item_id for row in unprocessed_items.collect()]
            source_df = spark.table(f"{database_name}.{src_table1}")
            filtered_df = source_df.filter(col("edf_job_run_item_id").isin(unprocessed_ids)).select("edf_job_run_item_id").distinct()
            print(f"Filtered by {len(unprocessed_ids)} unprocessed IDs")
        else:
            source_df = spark.table(f"{database_name}.{src_table1}")
            filtered_df = source_df.select("edf_job_run_item_id").distinct()
            print("Getting all distinct job_run_item_id(s)")
        count = filtered_df.count()
        print(f"Found {count} distinct job_run_item_id(s)")
        if count > 0:
            filtered_df.show(10, truncate=False)
        return filtered_df
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise

job_run_items_df = get_job_run_items_from_source(src_table1, unprocessed_items, unprocessed_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Insert into ETL_AUDT_FACT

# COMMAND ----------

def insert_into_etl_audt_fact(job_run_items_df):
    """Insert new records into ETL_AUDT_FACT with Processed_Flag='N'"""
    try:
        insert_df = job_run_items_df.withColumn("processed_flag", lit("N")).withColumn("run_status", lit(None).cast("string")).withColumn("job_start_time", lit(None).cast("timestamp")).withColumn("job_end_time", lit(None).cast("timestamp")).withColumn("source_count", lit(None).cast("long")).withColumn("target_count", lit(None).cast("long")).withColumn("error_count", lit(0)).withColumn("error_message", lit(None).cast("string")).withColumn("batch_no", lit(None).cast("int")).withColumn("notebook_name", lit(None).cast("string")).withColumn("create_ts", lit(None).cast("timestamp")).withColumn("update_ts", current_timestamp())
        insert_count = insert_df.count()
        if insert_count > 0:
            insert_df.write.mode("append").saveAsTable(f"{database_name}.ETL_AUDT_FACT")
            print(f"SUCCESS: Inserted {insert_count} records into ETL_AUDT_FACT")
        else:
            print("No new records to insert")
        return insert_count
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise

inserted_count = insert_into_etl_audt_fact(job_run_items_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*60)
print("NOTEBOOK1 EXECUTION SUMMARY")
print("="*60)
print(f"Job Name: {job_name}")
print(f"Source Table: {src_table1}")
print(f"Unprocessed Items Found: {unprocessed_count}")
print(f"New Records Inserted: {inserted_count}")
print("="*60)

dbutils.notebook.exit(f"SUCCESS: Processed {inserted_count} job_run_item_ids")

# Made with Bob
