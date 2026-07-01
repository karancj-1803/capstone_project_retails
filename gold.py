# Databricks notebook source
# MAGIC %md
# MAGIC #Gold Layer

# COMMAND ----------

from proj_utils import pipeline_run_log

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dim products table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.gold.dim_product (
# MAGIC     ProductSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     ProductID INT,
# MAGIC     ProductName STRING,
# MAGIC     Category STRING,
# MAGIC     SubCategory STRING,
# MAGIC     Brand STRING,
# MAGIC     CostPrice DECIMAL(10,2)
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dim Products

# COMMAND ----------

from delta.tables import DeltaTable

def build_dim_product():
    print("Building dim_product")
    incoming = spark.table("retail.silver.products").select(
        "ProductID", "ProductName", "Category", "SubCategory", "Brand", "CostPrice"
    )

    dim_table = DeltaTable.forName(spark, "retail.gold.dim_product")

    (dim_table.alias("tgt")
        .merge(incoming.alias("src"), "tgt.ProductID = src.ProductID")
        .whenMatchedUpdate(set={
            "ProductName": "src.ProductName", "Category": "src.Category", "SubCategory": "src.SubCategory",
            "Brand": "src.Brand", "CostPrice": "src.CostPrice"
        })
        .whenNotMatchedInsert(values={
            "ProductID": "src.ProductID", "ProductName": "src.ProductName", "Category": "src.Category",
            "SubCategory": "src.SubCategory", "Brand": "src.Brand", "CostPrice": "src.CostPrice"
        })
        .execute())

    row_count = spark.table("retail.gold.dim_product").count()
    print(f"dim_product: {row_count} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dim Customers table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail.gold.dim_customer (
# MAGIC     CustomerSK BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     CustomerID INT,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Email STRING,
# MAGIC     Phone STRING,
# MAGIC     City STRING,
# MAGIC     State STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dim Customers

# COMMAND ----------

from delta.tables import DeltaTable

def build_dim_customer():
    print("Building dim_customer")
    incoming = (
        spark.table("retail.silver.customers")
        .filter(col("IsCurrent") == True)
        .select("CustomerID", "FirstName", "LastName", "Email", "Phone", "City", "State")
    )

    dim_table = DeltaTable.forName(spark, "retail.gold.dim_customer")

    (dim_table.alias("tgt")
        .merge(incoming.alias("src"), "tgt.CustomerID = src.CustomerID")
        .whenMatchedUpdate(set={
            "FirstName": "src.FirstName", "LastName": "src.LastName", "Email": "src.Email",
            "Phone": "src.Phone", "City": "src.City", "State": "src.State"
        })
        .whenNotMatchedInsert(values={
            "CustomerID": "src.CustomerID", "FirstName": "src.FirstName", "LastName": "src.LastName",
            "Email": "src.Email", "Phone": "src.Phone", "City": "src.City", "State": "src.State"
        })
        .execute())

    row_count = spark.table("retail.gold.dim_customer").count()
    print(f"dim_customer: {row_count} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fact Sales

# COMMAND ----------

def build_fact_sales():
    print("Building fact_sales")
    orders = spark.table("retail.silver.orders")
    dim_customer = spark.table("retail.gold.dim_customer").select("CustomerSK", "CustomerID")
    dim_product = spark.table("retail.gold.dim_product").select("ProductSK", "ProductID")

    df = (
        orders
        .join(dim_customer, "CustomerID", "left")
        .join(dim_product, "ProductID", "left")
        .withColumn("SalesAmount", col("Quantity") * col("UnitPrice"))
        .select("OrderID", "CustomerSK", "ProductSK", "OrderDate", "Quantity", "UnitPrice", "SalesAmount", "StoreCode")
    )

    df.write.format("delta").mode("overwrite").saveAsTable("retail.gold.fact_sales")
    print(f"fact_sales: {df.count()} rows")
    return df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Exchange Rates

# COMMAND ----------

def build_gold_exchange_rates():
    print("Building gold.exchange_rates")
    df = spark.table("retail.silver.exchange_rates").select(
        "BaseCurrency", "TargetCurrency", "ExchangeRate", "RateDate"
    )
    df.write.format("delta").mode("overwrite").saveAsTable("retail.gold.exchange_rates")
    print(f"exchange_rates: {df.count()} rows")
    return df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run gold with logging

# COMMAND ----------

import uuid
from datetime import datetime

def run_gold_step(source, build_fn, run_id):
    start_time = datetime.now()
    try:
        row_count = build_fn()
        pipeline_run_log(run_id, {"source_name": source}, "gold_build", "Success", start_time, rows_processed=row_count)
        print(f"[{source}] SUCCESS - {row_count} rows")
    except Exception as e:
        pipeline_run_log(run_id, {"source_name": source}, "gold_build", "Failed", start_time, error_message=str(e))
        print(f"[{source}] FAILED - {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Layer

# COMMAND ----------

def gold_layer():
    run_id = str(uuid.uuid4())
    run_gold_step("dim_customer", build_dim_customer, run_id)
    run_gold_step("dim_product", build_dim_product, run_id)
    run_gold_step("fact_sales", build_fact_sales, run_id)
    run_gold_step("exchange_rates", build_gold_exchange_rates, run_id)
    print("=" * 50)
    print("Gold Layer Completed")
    print("=" * 50)

# COMMAND ----------

gold_layer()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail.gold.fact_sales