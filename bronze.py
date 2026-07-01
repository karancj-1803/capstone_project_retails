# Databricks notebook source
# MAGIC %md
# MAGIC #Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG  IF NOT EXISTS retail;
# MAGIC CREATE DATABASE IF NOT EXISTS retail.bronze;
# MAGIC CREATE DATABASE IF NOT EXISTS retail.silver;
# MAGIC CREATE DATABASE IF NOT EXISTS retail.gold;
# MAGIC CREATE DATABASE IF NOT EXISTS retail.audit;

# COMMAND ----------

# MAGIC %md
# MAGIC #Creation of Audit table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.audit.file_metadata (
# MAGIC     source_name STRING,
# MAGIC     file_name STRING,
# MAGIC     file_path STRING,
# MAGIC     first_seen_timestamp TIMESTAMP,
# MAGIC     last_processed_timestamp TIMESTAMP,
# MAGIC     processing_status STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retail.audit.pipeline_run_log (
# MAGIC     run_id STRING,
# MAGIC     source_name STRING,
# MAGIC     step_name STRING,
# MAGIC     status STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     rows_processed BIGINT,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from proj_utils import load_config, audit_file_metadata, pipeline_run_log

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Config File

# COMMAND ----------

# MAGIC %skip
# MAGIC import json
# MAGIC
# MAGIC def load_config():
# MAGIC     CONFIG_PATH = '/Volumes/retail/landing/datasets/configs/config_initial.json'
# MAGIC     with open(CONFIG_PATH, 'r') as f:
# MAGIC         reader = json.load(f)
# MAGIC         config = reader['sources']
# MAGIC         return config

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.audit.rejected_records

# COMMAND ----------

import json

def load_config_adf():
    try:
        dbutils.widgets.get('source_config_array')
    except:
        dbutils.widgets.text('source_config_array', json.dumps(load_config()))

    config_param = dbutils.widgets.get('source_config_array')
    config = json.loads(config_param)
    
    for source in config:
        if not source['is_active'] :
            continue
        else:
            print(f"[{source['source_id']}] {source['source_name']:15s} | type={source['source_type']:5s} | pattern={source['load_pattern']:15s} | pk={source['primary_key']}")
    return config


# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze Layer

# COMMAND ----------

def detect_json_format(folder_path):

    file_path = dbutils.fs.ls(folder_path)[0].path
    first_chunk = dbutils.fs.head(file_path, 1024).strip()
    
    if first_chunk.startswith('[') or first_chunk.startswith('{'):
        return {"multiLine": "true"}
    
    else:
        return {"multiLine": "false"}


# COMMAND ----------

# MAGIC %skip
# MAGIC import datetime
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
# MAGIC
# MAGIC def audit_file_metadata(source_name, files, first_seen_timestamp, status):
# MAGIC     print(f"Starting File metadata for: {source_name}")
# MAGIC
# MAGIC     if not files:
# MAGIC         print(f"No new files for {source_name}, skipping metadata update")
# MAGIC         return
# MAGIC
# MAGIC     data = []
# MAGIC     for file in files:
# MAGIC         data.append(
# MAGIC             (
# MAGIC                 source_name,
# MAGIC                 file.split('/')[-1],
# MAGIC                 file,
# MAGIC                 first_seen_timestamp,
# MAGIC                 datetime.datetime.now(),
# MAGIC                 status,
# MAGIC             )
# MAGIC         )
# MAGIC
# MAGIC     schema = StructType([
# MAGIC         StructField("source_name", StringType(), True),
# MAGIC         StructField("file_name", StringType(), True),
# MAGIC         StructField("file_path", StringType(), True),
# MAGIC         StructField("first_seen_timestamp", TimestampType(), True),
# MAGIC         StructField("last_processed_timestamp", TimestampType(), True),
# MAGIC         StructField("processing_status", StringType(), True)
# MAGIC     ])
# MAGIC
# MAGIC     audit_df = spark.createDataFrame(
# MAGIC         data,
# MAGIC         schema,
# MAGIC     )
# MAGIC     audit_df.createOrReplaceTempView(f"temp_audit_{source_name}")
# MAGIC     spark.sql(
# MAGIC         f"""
# MAGIC         MERGE INTO retail.audit.file_metadata AS t
# MAGIC         USING temp_audit_{source_name} AS s
# MAGIC         ON t.file_path = s.file_path
# MAGIC         WHEN MATCHED THEN
# MAGIC             UPDATE SET t.last_processed_timestamp = s.last_processed_timestamp, t.processing_status = s.processing_status
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC             INSERT (source_name, file_name, file_path, first_seen_timestamp, last_processed_timestamp, processing_status)
# MAGIC             VALUES (s.source_name, s.file_name, s.file_path, s.first_seen_timestamp, s.last_processed_timestamp , s.processing_status)
# MAGIC         """
# MAGIC     )
# MAGIC     print(f"Ending File metadata for: {source_name} with {status}")

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
# MAGIC
# MAGIC def pipeline_run_log(run_id, source, step_name, status, start_time, rows_processed = None, error_message = None):
# MAGIC
# MAGIC     print(f"Starting pipeline run for: {source['source_name']}")
# MAGIC
# MAGIC     data = [(run_id, source['source_name'], step_name, status, start_time, datetime.datetime.now(), rows_processed, error_message)]
# MAGIC     schema = StructType([
# MAGIC         StructField('run_id', StringType(), False),
# MAGIC         StructField('source_name', StringType(), False),
# MAGIC         StructField('step_name', StringType(), False),
# MAGIC         StructField('status', StringType(), False),
# MAGIC         StructField('start_time', TimestampType(), False),
# MAGIC         StructField('end_time', TimestampType(), False),
# MAGIC         StructField('rows_processed', LongType(), True),
# MAGIC         StructField('error_message', StringType(), True)
# MAGIC     ])
# MAGIC     df = spark.createDataFrame(data, schema)
# MAGIC     df.write.mode("append").saveAsTable("retail.audit.pipeline_run_log")
# MAGIC
# MAGIC     print(f"Ending pipeline run for: {source['source_name']} with {status}")
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

def ingest_to_bronze(source, run_id):
    cloud_options = {
        "cloudFiles.format": source["source_type"],
        "cloudFiles.schemaLocation": source["schema_location"],
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }
    if source["source_type"] == "csv":
        cloud_options.update({"header": "true"})
    elif source["source_type"] == "json":
        cloud_options.update(detect_json_format(source['landing_path']))

    print(f"Starting bronze ingestion for {source['source_name']}")

    df = (
        spark.readStream.format("cloudFiles")
        .options(**cloud_options)
        .load(source["landing_path"])
    )
    df_bronze = (
        df
        .withColumn("_AdfPipelineRunId", lit(run_id))
        .withColumn("_SourceFile", df["_metadata.file_path"])
        .withColumn("_IngestionTimestamp", current_timestamp())
        .withColumn("_SourceName", lit(source["source_name"]))
    )

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.write.format("delta").option('mergeSchema', True).mode("append").saveAsTable(source["bronze_table_path"])

        files_df = batch_df.select("_SourceFile").distinct() \
            .withColumnRenamed("_SourceFile", "file_path")
        files_df.write.format("delta").mode("append") \
            .saveAsTable("retail.audit._files_seen_staging")

    query = (
        df_bronze.writeStream
        .option("checkpointLocation", source["checkpoint_path"])
        .trigger(availableNow=True)
        .foreachBatch(process_batch)
        .start()
    )
    query.awaitTermination()

    try:
        files_seen_this_run = [
            row["file_path"] for row in
            spark.read.table("retail.audit._files_seen_staging").select("file_path").distinct().collect()
        ]
    except Exception:
        files_seen_this_run = []

    spark.sql("TRUNCATE TABLE retail.audit._files_seen_staging")

    print(f"Finished Bronze ingestion for: {source['source_name']}")
    return files_seen_this_run


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.audit.file_metadata

# COMMAND ----------

import datetime
def bronze_layer_with_audit_logging(source, run_id):
    start_time = datetime.datetime.now()
    try:
        try:
            rows_before = spark.read.table(f'retail.bronze.{source['source_name']}').count()
        except:
            rows_before = 0
    
        files = ingest_to_bronze(source, run_id)

        rows_in_bronze = spark.read.table(f'retail.bronze.{source['source_name']}').count()
        rows_added = rows_in_bronze - rows_before

        audit_file_metadata(source['source_name'], files, start_time , "Success")
        
        pipeline_run_log(run_id, source, 'bronze_loading', 'Success', start_time, rows_added)

        print(f"[{source['source_name']}] SUCCESS - {rows_added} new rows added (total: {rows_in_bronze})")

    except Exception as e:
        audit_file_metadata(source['source_name'], [], start_time , "Failed")
        pipeline_run_log(run_id, source, 'bronze_loading', 'Failed', start_time, error_message=str(e))
    
    print('='*50)
    print(f'{source["source_name"]} loading Completed')
    print('='*50)

# COMMAND ----------

import uuid
def bronze_layer():
    run_id = str(uuid.uuid4())
    sources = load_config()
    for source in sources:
        bronze_layer_with_audit_logging(source, run_id)
    print('='*50)
    print('Bronze Layer Completed')
    print('='*50)
    

# COMMAND ----------

bronze_layer()

# COMMAND ----------

bronze_layer()

# COMMAND ----------

bronze_layer()

# COMMAND ----------

bronze_layer()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.audit.pipeline_run_log