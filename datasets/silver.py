# Databricks notebook source
# MAGIC %md
# MAGIC #Silver Layer
# MAGIC

# COMMAND ----------

import importlib
import proj_utils
importlib.reload(proj_utils)
from proj_utils import pipeline_run_log, load_config, flatten_df, get_last_watermark, parse_messy_date, update_watermark

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rejected table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.audit.rejected_records (
# MAGIC     SourceName STRING,
# MAGIC     RunId STRING,
# MAGIC     RejectedTimestamp TIMESTAMP,
# MAGIC     PrimaryKeyValue STRING,
# MAGIC     RejectedReason STRING,
# MAGIC     RowData STRING 
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###Watermark Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.audit.watermark (
# MAGIC     SourceName STRING,
# MAGIC     WatermarkColumn STRING,
# MAGIC     LastWatermarkValue STRING,
# MAGIC     LastRunId STRING,
# MAGIC     LastProcessedTimestamp TIMESTAMP,
# MAGIC     ProcessingStatus STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load
# MAGIC

# COMMAND ----------

import json

def load_config_adf():
    widget_name = 'source_config_array'
    
    try:
        config_param = dbutils.widgets.get(widget_name)
    except:
        dbutils.widgets.text(widget_name, "")
        config_param = ""
    
    if not config_param or config_param.strip() == "":
        config = load_config()
    else:
        config = json.loads(config_param)
        
    for source in config:
        if not source.get('is_active', True): 
            continue
        print(f"[{source['source_id']}] {source['source_name']:15s} | ...")
    
    return config


# COMMAND ----------

# MAGIC %md
# MAGIC ###Cast Columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, trim

def cast_columns(df, column_spec):
    for spec in column_spec:
        name = spec['name']
        target_type = spec['target_type']
        if spec['target_type'] == 'string':
            df = df.withColumn(name, trim(col(name).cast(target_type)))
        elif spec['target_type'] == 'date':
            df = df.withColumn(name, parse_messy_date(name))
        else:
            df = df.withColumn(name, col(name).cast(target_type))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Business Transformation

# COMMAND ----------

from pyspark.sql.functions import *
def apply_transformations(df, column_spec):
    TRANSFORMATIONS = {
    "lower": lambda c: lower(c),
    "upper": lambda c: upper(c),
    "initcap": lambda c: initcap(c),
    "digits_only": lambda c: regexp_replace(c, "[^0-9]", ""),
    "remove_special_chars": lambda c: regexp_replace(c, "[^a-zA-Z0-9 ]", ""),
    "normalize_space": lambda c: regexp_replace(c, "\\s+", " ")
}
    for spec in column_spec:
        column_name = spec["name"]

        for transformation in spec.get("transformations", []):
            if transformation in TRANSFORMATIONS:
                df = df.withColumn(
                    column_name,
                    TRANSFORMATIONS[transformation](col(column_name))
                )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###DeDuplications
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number
def deduplicate(df, primary_key):
    keys = primary_key if isinstance(primary_key, list) else [primary_key]
    window_spec = Window.partitionBy(*keys).orderBy(desc('_BronzeIngestionTimestamp'))
    df_deduped = df.withColumn('rank', row_number().over(window_spec)).filter('rank == 1').drop('rank')
    return df_deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validations
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit, col, when, array, array_remove, concat_ws, filter, size

def apply_validation_rules(df, column_spec):
    
    reasons = []
    valid_cols_to_drop = []
    
    for spec in column_spec:
        col_name = spec['name']
        rule = spec['rule']

        if rule == 'not_null':
            reasons.append(when(col(col_name).isNull(), f'{col_name} is Null').otherwise(None))
        elif rule == 'positive':
            reasons.append(when((col(col_name).isNull()) | (col(col_name) <= 0) , f'{col_name} is not Positive').otherwise(None))
        elif rule == 'referential':
            ref_table = spec['ref_table']
            ref_column = spec['ref_column']
            ref_df = spark.read.table(ref_table)
            if "IsCurrent" in ref_df.columns:
                ref_df = ref_df.filter(col("IsCurrent") == True)
            valid_df = ref_df.select(col(ref_column).alias(f'_valid_{col_name}')).distinct()
            df = df.join(valid_df, df[col_name] == col(f'_valid_{col_name}'), "left")
            reasons.append(when(col(f'_valid_{col_name}').isNull(), f'{col_name} not found in {ref_table}').otherwise(None))
            valid_cols_to_drop.append(f'_valid_{col_name}')
        elif rule == 'none':
            pass
    
    df = (
        df.withColumn("_RejectReasons", filter(array(*reasons), lambda x: x.isNotNull()))
        .withColumn('_IsRejected', size(col('_RejectReasons')) > 0)
        .withColumn('RejectedReason', concat_ws(';' , col('_RejectReasons')))
        .drop('_RejectReasons', *valid_cols_to_drop)
        )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Splititng good and rejected Records
# MAGIC

# COMMAND ----------

def split_good_bad_df(df):
    good_df = df.filter(~col('_IsRejected'))
    bad_df = df.filter(col('_IsRejected'))
    return good_df, bad_df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add auditing columns

# COMMAND ----------

from datetime import datetime

def add_auditing_columns(df, run_id):
    df = (
        df.withColumnRenamed('_AdfPipelineRunId', '_BronzeAdfPipelineRunId')
        .withColumnRenamed('_IngestionTimestamp', '_BronzeIngestionTimestamp')
    )
    return ( 
        df.withColumn('_AdfPipelineRunId', lit(run_id))
        .withColumn('_ProcessedTimestamp', lit(datetime.now()).cast('timestamp'))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Prepare reject data

# COMMAND ----------

from pyspark.sql.functions import to_json, struct, lit, current_timestamp, col


def prepare_reject_data(df, source, run_id):
    pk = source["primary_key"]
    original_columns = [c for c in df.columns if not c.startswith("_")]
    df = (
        df.withColumn("RowData", to_json(struct(*original_columns)))
        .withColumn("RunId", lit(run_id))
        .withColumn("RejectedTimestamp", current_timestamp())
        .withColumn("PrimaryKeyValue", col(pk).cast("string"))
    )
    return df.select(
        col("_SourceName").alias("SourceName"),
        "RunId",
        "RejectedTimestamp",
        "PrimaryKeyValue",
        "RejectedReason",
        "RowData",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD Function

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp

def write_scd2(incoming_df, source):
    silver_table_path = source["silver_table_path"]
    pk = source["primary_key"]

    incoming = (
        incoming_df
        .withColumn("EffectiveStartDate", current_timestamp())
        .withColumn("EffectiveEndDate", lit(None).cast("timestamp"))
        .withColumn("IsCurrent", lit(True))
    )

    table_exists = spark.catalog.tableExists(silver_table_path)
    is_first_run = (not table_exists) or (spark.table(silver_table_path).count() == 0)

    if is_first_run:
        incoming.write.format("delta").mode("overwrite").saveAsTable(silver_table_path)
        return

    silver_delta = DeltaTable.forName(spark, silver_table_path)

    change_cols = [c for c in incoming.columns if c not in (pk, "EffectiveStartDate", "EffectiveEndDate", "IsCurrent") and not c.startswith("_")]
    change_condition = " OR ".join([f"tgt.{c} <> src.{c}" for c in change_cols])

    (silver_delta.alias("tgt")
        .merge(incoming.alias("src"), f"tgt.{pk} = src.{pk} AND tgt.IsCurrent = true")
        .whenMatchedUpdate(
            condition=change_condition,
            set={"IsCurrent": "false", "EffectiveEndDate": "current_timestamp()"}
        )
        .execute())

    (silver_delta.alias("tgt")
        .merge(incoming.alias("src"), f"tgt.{pk} = src.{pk} AND tgt.IsCurrent = true")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Silver Output

# COMMAND ----------

def write_silver_output(good_df, rejected_df, source, run_id):
    good_df_final = good_df.drop("_IsRejected", "RejectedReason")

    if source["load_pattern"] == "scd2":
        write_scd2(good_df_final, source)
    elif source["load_pattern"] == "incremental":
        if good_df_final.count() > 0:
            writer = good_df_final.write.format("delta").mode("append")
            if source.get("partition_column"):
                writer = writer.partitionBy(source["partition_column"])
            writer.saveAsTable(source["silver_table_path"])
        else:
            print(f"No new rows for {source['source_name']} this run — leaving existing Silver table unchanged")
    else:
        writer = good_df_final.write.format("delta").mode("overwrite")
        if source.get("partition_column"):
            writer = writer.partitionBy(source["partition_column"])
        writer.saveAsTable(source["silver_table_path"])

    if rejected_df.count() > 0:
        rejected_final = prepare_reject_data(rejected_df, source, run_id)
        rejected_final.write.mode("append").saveAsTable("retail.audit.rejected_records")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cleaning

# COMMAND ----------

def clean_in_silver(source, run_id):
    print(f"Starting silver cleaning for {source['source_name']}")
    bronze_df = spark.read.table(source['bronze_table_path'])

    if source['source_type'] == 'json':
        bronze_df = flatten_df(bronze_df)

    casted_df = cast_columns(bronze_df, source['columns'])

    transformed_df = apply_transformations(casted_df, source['columns'])

    if source.get("watermark_column"):
        wm_col = source["watermark_column"]
        last_wm = get_last_watermark(source)
        if last_wm:
            transformed_df = transformed_df.filter(col(wm_col) > lit(last_wm))
    
    validated_df = apply_validation_rules(transformed_df, source['columns'])
    
    good_df, bad_df = split_good_bad_df(validated_df)

    good_df = add_auditing_columns(good_df, run_id)
    rejected_df = add_auditing_columns(bad_df, run_id)

    good_df = deduplicate(good_df, source['primary_key'])

    write_silver_output(good_df, rejected_df, source, run_id)
    
    if source.get("watermark_column") and good_df.count() > 0:
        wm_col = source["watermark_column"]
        if wm_col in good_df.columns:
            new_max = good_df.agg({wm_col: "max"}).collect()[0][0]
            update_watermark(source["source_name"], wm_col, str(new_max), run_id)
        else:
            print(f"Warning: Watermark column '{wm_col}' not found in DataFrame. Available columns: {good_df.columns}")


    print(f"Ending silver cleaning for {source['source_name']}")
    return good_df.count(), rejected_df.count()

# COMMAND ----------

from datetime import datetime

def silver_layer_with_audit_logging(source, run_id):
    start_time = datetime.now()
    try:
        accepted_count, rejected_count = clean_in_silver(source, run_id)
        pipeline_run_log(run_id, source, "silver_cleaning", 'Success', start_time, rows_processed=accepted_count)
        print(f"[{source['source_name']}] SUCCESS - {accepted_count} accepted rows, {rejected_count} rejected rows")
        print('-' * 50)
        print(f'{source["source_name"]} silver processing completed')
        print('-' * 50)
    except Exception as e:
        pipeline_run_log(run_id, source, "silver_cleaning", 'Failed', start_time, error_message = str(e))
        print(f"[{source['source_name']}] FAILED - {str(e)}")
        print('-' * 50)
        print(f'{source["source_name"]} silver processing failed')
        print('-' * 50)
    
    

# COMMAND ----------

import uuid

def silver_layer():
    run_id = str(uuid.uuid4())
    sources = load_config_adf()
    sources_by_name = {s["source_name"]: s for s in sources if s["is_active"]}

    source_order = ["products", "customers", "orders", "exchange_rates"]

    for name in source_order:
        if name not in sources_by_name:
            print(f"Skipping {name} - not found or inactive in config")
            continue
        silver_layer_with_audit_logging(sources_by_name[name], run_id)

    print('=' * 50)
    print('Silver Layer Completed')
    print('=' * 50)


# COMMAND ----------

silver_layer()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from retail.bronze.exchange_rates
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail.audit.watermark;