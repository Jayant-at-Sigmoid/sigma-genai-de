"""
Sigma DataTech Transaction Analytics Pipeline
Fixed & Hardened Version
"""

import shutil
import logging
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, input_file_name, lit, max, min, when, sum, count, coalesce, expr, broadcast, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)

def validate_schema(df, expected_columns):
    """Schema Validation: check if all expected columns exist in the DataFrame"""
    missing = [col_name for col_name in expected_columns if col_name not in df.columns]
    if missing:
        raise ValueError(f"Schema validation failed: Missing columns {missing}")
    return True

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    bronze_df = None
    try:
        bronze_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("transaction_amount", StringType(), True),
            StructField("transaction_timestamp", StringType(), True),
            StructField("transaction_status", StringType(), True)
        ])

        bronze_df = (spark.readStream
                    .format("csv")
                    .option("header", "true")
                    .schema(bronze_schema)
                    .load(input_path)
                    .withColumn("ingestion_timestamp", current_date())
                    .withColumn("source_file", input_file_name())
                    .withColumn("pipeline_run_id", lit(run_id))
                    .withColumn("load_date", col("transaction_timestamp").cast(DateType())))

        # Idempotency: Delete partition folder first before writing with mode("overwrite")
        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        # Batch load for local spark context / testing to support overwrite mode safely
        # (pyspark streaming requires a starting check point to overwrite)
        if bronze_df.isStreaming:
            (bronze_df.writeStream
            .format("parquet")
            .partitionBy("load_date")
            .outputMode("append")
            .option("path", output_path)
            .start()
            .awaitTermination())
        else:
            (bronze_df.write
            .format("parquet")
            .partitionBy("load_date")
            .mode("overwrite")
            .save(output_path))

        logging.info(f"[Stage: ingest_bronze] output_count: {bronze_df.count():,} rows")

    except Exception as e:
        row_count_str = f"{bronze_df.count():,}" if bronze_df is not None else "N/A"
        logging.error(f"[Stage: ingest_bronze] Error: {e}, Row count: {row_count_str}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    silver_df = None
    try:
        # Partition Pruning: filter by partition column load_date
        bronze_df = (spark.read.format("parquet")
                     .load(bronze_path)
                     .where(col("load_date") == run_date)
                     .cache())

        # Schema Validation
        validate_schema(bronze_df, ["transaction_id", "customer_id", "transaction_amount", "transaction_timestamp", "transaction_status", "ingestion_timestamp", "source_file", "pipeline_run_id", "load_date"])
        logging.info(f"[Stage: transform_silver] input_count: {bronze_df.count():,} rows")

        silver_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("transaction_amount", FloatType(), False),
            StructField("transaction_timestamp", DateType(), False),
            StructField("transaction_status", StringType(), False),
            StructField("ingestion_timestamp", DateType(), False),
            StructField("source_file", StringType(), False),
            StructField("pipeline_run_id", StringType(), False),
            StructField("load_date", DateType(), False)
        ])

        # Cast columns and clean NULLs / invalid amounts (Null Handling)
        silver_df = (bronze_df.select([col(c).cast(silver_schema[c].dataType()) for c in silver_schema.fieldNames()])
                    .filter(col("transaction_amount").cast(FloatType()) > 0)
                    .filter(col("transaction_id").isNotNull() & col("customer_id").isNotNull() & col("transaction_amount").isNotNull()))

        logging.info(f"[Stage: transform_silver] after_filter_count: {silver_df.count():,} rows")

        # Deduplication
        silver_df = (silver_df.withColumn("rank",
                                          when(col("transaction_id").isNotNull(),
                                               max("ingestion_timestamp").over(Window.partitionBy("transaction_id")))
                                          )
                     .filter(col("rank") == col("ingestion_timestamp"))
                     .drop("rank"))

        logging.info(f"[Stage: transform_silver] after_dedup_count: {silver_df.count():,} rows")

        merchants_df = (spark.read.format("parquet").load(merchants_path).hint("broadcast"))

        silver_df = (silver_df.join(merchants_df, silver_df.customer_id == merchants_df.customer_id, "left")
                     .withColumn("quality_flag", when(col("merchants.customer_id").isNotNull(), "CLEAN").otherwise("UNMATCHED")))

        # Idempotency: Delete partition folder first
        partition_path = f"{output_path}/transaction_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        (silver_df.write
        .format("parquet")
        .partitionBy("transaction_timestamp")
        .mode("overwrite")
        .save(output_path))

        logging.info(f"[Stage: transform_silver] output_count: {silver_df.count():,} rows")

    except Exception as e:
        row_count_str = f"{silver_df.count():,}" if silver_df is not None else "N/A"
        logging.error(f"[Stage: transform_silver] Error: {e}, Row count: {row_count_str}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    run_metadata = {
        "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
        "run_date": run_date,
        "run_id": os.getenv("RUN_ID", "run_id_123"),
        "run_status": "SUCCESS",
        "error_message": None,
        "started_at": datetime.now().isoformat(),
        "completed_at": None
    }

    try:
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path, f"{gold_output_dir}/customer_ltv")
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)

        run_metadata["completed_at"] = datetime.now().isoformat()

        # Metadata Output
        run_metadata_json_path = f"{gold_output_dir}/run_metadata_{run_date}.json"
        with open(run_metadata_json_path, "w") as f:
            json.dump(run_metadata, f)

    except Exception as e:
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        run_metadata["completed_at"] = datetime.now().isoformat()
        
        run_metadata_json_path = f"{gold_output_dir}/run_metadata_{run_date}.json"
        try:
            with open(run_metadata_json_path, "w") as f:
                json.dump(run_metadata, f)
        except Exception:
            pass

        logging.error(f"[Stage: run_gold] Error: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        silver_transactions = spark.read.parquet(silver_path).filter(col("transaction_timestamp") == run_date)
        
        # Schema Validation
        validate_schema(silver_transactions, ["merchant_id", "merchant_name", "category", "city", "transaction_timestamp", "transaction_amount", "transaction_status"])
        logging.info(f"[Stage: build_merchant_performance] input_count: {silver_transactions.count():,} rows")

        # Business Rules: SUM(transaction_amount) WHERE status='COMPLETED'
        merchant_performance = (silver_transactions
            .filter(col("transaction_status") == "COMPLETED")
            .groupBy("merchant_id", "merchant_name", "category", "city", "transaction_timestamp")
            .agg(
                sum("transaction_amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("transaction_status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            ))

        # Idempotency: Delete partition folder first
        partition_path = f"{output_path}/transaction_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        merchant_performance.write.partitionBy("transaction_timestamp").parquet(output_path)
        logging.info(f"[Stage: build_merchant_performance] output_count: {merchant_performance.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: build_merchant_performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        silver_transactions = spark.read.parquet(silver_path)
        
        # Schema Validation
        validate_schema(silver_transactions, ["customer_id", "transaction_amount", "transaction_status", "transaction_timestamp", "preferred_payment_method"])
        logging.info(f"[Stage: build_customer_ltv] input_count: {silver_transactions.count():,} rows")

        # Business Rules: SUM(transaction_amount) WHERE status='COMPLETED'
        customer_ltv = (silver_transactions
            .filter(col("transaction_status") == "COMPLETED")
            .groupBy("customer_id")
            .agg(
                sum("transaction_amount").alias("total_spent"),
                count("*").alias("total_txns"),
                max("transaction_timestamp").alias("first_txn_date"),
                min("transaction_timestamp").alias("last_txn_date"),
                coalesce(expr("mode(preferred_payment_method)"), lit(None)).alias("preferred_payment_method")
            ))

        # Idempotency: Delete path first
        shutil.rmtree(output_path, ignore_errors=True)
        customer_ltv.write.parquet(output_path)
        logging.info(f"[Stage: build_customer_ltv] output_count: {customer_ltv.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: build_customer_ltv] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        silver_transactions = spark.read.parquet(silver_path).filter(col("transaction_timestamp") == run_date)
        
        # Schema Validation
        validate_schema(silver_transactions, ["transaction_timestamp", "transaction_amount", "transaction_status", "customer_id", "merchant_id"])
        logging.info(f"[Stage: build_daily_summary] input_count: {silver_transactions.count():,} rows")

        daily_summary = (silver_transactions.groupBy("transaction_timestamp")
           .agg(
                sum("transaction_amount").alias("total_revenue"),
                count("*").alias("total_txns"),
                count(when(col("transaction_status") == "COMPLETED", 1)).alias("unique_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants"),
                (count(when(col("transaction_status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            ))

        # Idempotency: Delete partition folder first
        partition_path = f"{output_path}/transaction_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        daily_summary.write.partitionBy("transaction_timestamp").parquet(output_path)
        logging.info(f"[Stage: build_daily_summary] output_count: {daily_summary.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: build_daily_summary] Error: {e}")
        raise

def main():
    spark = SparkSession.builder.appName("Customer Segmentation Pipeline").getOrCreate()

    input_path = os.getenv("INPUT_PATH")
    bronze_output_path = os.getenv("BRONZE_OUTPUT_PATH")
    merchants_path = os.getenv("MERCHANTS_PATH")
    silver_output_path = os.getenv("SILVER_OUTPUT_PATH")
    gold_output_dir = os.getenv("GOLD_OUTPUT_PATH")
    run_date = os.getenv("RUN_DATE", "2023-10-01")
    run_id = os.getenv("RUN_ID", "run_id_123")

    # Verify that paths are provided
    if not input_path or not bronze_output_path or not merchants_path:
        raise ValueError("Paths must be provided via environment variables.")

    ingest_bronze(spark, input_path, bronze_output_path, run_date, run_id)
    transform_silver(spark, bronze_output_path, merchants_path, silver_output_path, run_date)
    run_gold(spark, silver_output_path, gold_output_dir, run_date)

if __name__ == "__main__":
    main()
