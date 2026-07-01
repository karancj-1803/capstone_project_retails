import json
import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, ArrayType, DateType
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, explode, lit, coalesce, to_date, current_timestamp

spark = SparkSession.builder.getOrCreate()

def load_config():
    CONFIG_PATH = '/Volumes/retail/landing/datasets/configs/config_initial.json'
    with open(CONFIG_PATH, 'r') as f:
        reader = json.load(f)
        config = reader['sources']
        return config

def audit_file_metadata(source_name, files, first_seen_timestamp, status):
    print(f"Starting File metadata for: {source_name}")

    if not files:
        print(f"No new files for {source_name}, skipping metadata update")
        return

    data = []
    for file in files:
        data.append(
            (
                source_name,
                file.split('/')[-1],
                file,
                first_seen_timestamp,
                datetime.datetime.now(),
                status,
            )
        )

    schema = StructType([
        StructField("source_name", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("first_seen_timestamp", TimestampType(), True),
        StructField("last_processed_timestamp", TimestampType(), True),
        StructField("processing_status", StringType(), True)
    ])

    audit_df = spark.createDataFrame(
        data,
        schema,
    )
    audit_df.createOrReplaceTempView(f"temp_audit_{source_name}")
    spark.sql(
        f"""
        MERGE INTO retail.audit.file_metadata AS t
        USING temp_audit_{source_name} AS s
        ON t.file_path = s.file_path
        WHEN MATCHED THEN
            UPDATE SET t.last_processed_timestamp = s.last_processed_timestamp, t.processing_status = s.processing_status
        WHEN NOT MATCHED THEN
            INSERT (source_name, file_name, file_path, first_seen_timestamp, last_processed_timestamp, processing_status)
            VALUES (s.source_name, s.file_name, s.file_path, s.first_seen_timestamp, s.last_processed_timestamp , s.processing_status)
        """
    )
    print(f"Ending File metadata for: {source_name} with {status}")


def pipeline_run_log(run_id, source, step_name, status, start_time, rows_processed = None, error_message = None):

    print(f"Starting pipeline run for: {source['source_name']}")

    data = [(run_id, source['source_name'], step_name, status, start_time, datetime.datetime.now(), rows_processed, error_message)]
    schema = StructType([
        StructField('run_id', StringType(), False),
        StructField('source_name', StringType(), False),
        StructField('step_name', StringType(), False),
        StructField('status', StringType(), False),
        StructField('start_time', TimestampType(), False),
        StructField('end_time', TimestampType(), False),
        StructField('rows_processed', LongType(), True),
        StructField('error_message', StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    df.write.mode("append").saveAsTable("retail.audit.pipeline_run_log")

    print(f"Ending pipeline run for: {source['source_name']} with {status}")

def flatten_df(df):
    for c in df.schema.fields:
        if isinstance(c.dataType, StructType):
            cols = []
            for sc in c.dataType.fields:
                cols.append(
                    col(f"{c.name}.{sc.name}")
                    .alias(f"{c.name}_{sc.name}")
                )
            df = df.select("*", *cols).drop(c.name)
            return flatten_df(df)
        
        elif isinstance(c.dataType, ArrayType):
            df = df.withColumn(c.name, explode(col(c.name)))
            return flatten_df(df)
    return df

def get_last_watermark(source):
    watermark_df = spark.read.table('retail.audit.watermark')
    result = (
        watermark_df
        .filter(col('SourceName') == source['source_name'])
        .select('LastWatermarkValue')
        .collect()
    )
    if len(result) == 0:
        return None

    return result[0]["LastWatermarkValue"]

def parse_messy_date(col_name):
    formats = ["yyyy-MM-dd", "dd/MM/yyyy", "MMM dd yyyy", "dd-MMM-yyyy", "yyyyMMdd"]
    result = lit(None).cast(DateType())
    for fmt in reversed(formats):
        result = coalesce(to_date(col(col_name), fmt), result)
    return result


def update_watermark(source_name, watermark_column, new_value, run_id):
    spark.sql(f"""
        MERGE INTO retail.audit.watermark AS tgt
        USING (SELECT '{source_name}' AS SourceName, '{watermark_column}' AS WatermarkColumn,
                      '{new_value}' AS LastWatermarkValue, '{run_id}' AS LastRunId,
                      current_timestamp() AS LastProcessedTimestamp, 'Success' AS ProcessingStatus) AS src
        ON tgt.SourceName = src.SourceName
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)