import shutil
import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, input_file_name, lit, max, when, sum, count, min, expr, coalesce, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)

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

        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        (bronze_df.writeStream
        .format("parquet")
        .partitionBy("load_date")
        .outputMode("append")
        .option("path", output_path)
        .start()
        .awaitTermination())

        logging.info(f"[Stage: Ingest Bronze] Output count: {bronze_df.count():,} rows")

    except Exception as e:
        row_count_str = f"{bronze_df.count():,}" if bronze_df is not None else "N/A"
        logging.error(f"[Stage: Ingest Bronze] Error: {e}, Row count: {row_count_str}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    silver_df = None
    try:
        bronze_df = (spark.read.format("parquet")
                     .load(bronze_path)
                    .where(col("load_date") == run_date)
                    .cache())

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

        silver_df = (bronze_df.select([col(c).cast(silver_schema[c].dataType()) for c in silver_schema.fieldNames()])
                    .filter(col("transaction_amount").cast(FloatType()) > 0)
                    .filter(col("transaction_id").isNotNull()))

        logging.info(f"[Stage: Transform Silver] After filter count: {silver_df.count():,} rows")

        silver_df = (silver_df.withColumn("rank",
                                          when(col("transaction_id").isNotNull(),
                                               max("ingestion_timestamp").over(Window.partitionBy("transaction_id")))
                                          )
                     .filter(col("rank") == col("ingestion_timestamp"))
                    .drop("rank"))

        logging.info(f"[Stage: Transform Silver] After dedup count: {silver_df.count():,} rows")

        merchants_df = (spark.read.format("parquet").load(merchants_path).hint("broadcast"))

        silver_df = (silver_df.join(merchants_df, silver_df.customer_id == merchants_df.customer_id, "left")
                     .withColumn("quality_flag", when(col("merchants.customer_id").isNotNull(), "CLEAN").otherwise("UNMATCHED")))

        partition_path = f"{output_path}/transaction_timestamp={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        (silver_df.write
        .format("parquet")
        .partitionBy("transaction_timestamp")
        .mode("overwrite")
        .save(output_path))

        logging.info(f"[Stage: Transform Silver] Output count: {silver_df.count():,} rows")

    except Exception as e:
        row_count_str = f"{silver_df.count():,}" if silver_df is not None else "N/A"
        logging.error(f"[Stage: Transform Silver] Error: {e}, Row count: {row_count_str}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    run_metadata = {
        "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
        "run_date": run_date,
        "run_id": "run_id_123",
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

        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)

    except Exception as e:
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        run_metadata["completed_at"] = datetime.now().isoformat()

        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)

        logging.error(f"[Stage: Run Gold] Error: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        silver_transactions = spark.read.parquet(silver_path).filter(col("transaction_timestamp") == run_date)
        merchant_performance = silver_transactions.groupBy("merchant_id", "merchant_name", "category", "city", "date") \
            .agg(
                sum("transaction_amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("transaction_status") == "FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        merchant_performance.write.partitionBy("date").parquet(output_path)

        logging.info(f"[Stage: Build Merchant Performance] Output count: {merchant_performance.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        silver_transactions = spark.read.parquet(silver_path)
        customer_ltv = silver_transactions.groupBy("customer_id") \
            .agg(
                sum("transaction_amount").alias("total_spent"),
                count("*").alias("total_txns"),
                max("transaction_timestamp").alias("first_txn_date"),
                min("transaction_timestamp").alias("last_txn_date"),
                coalesce(expr("mode(preferred_payment_method)"), lit(None)).alias("preferred_payment_method")
            )

        shutil.rmtree(output_path, ignore_errors=True)
        customer_ltv.write.parquet(output_path)

        logging.info(f"[Stage: Build Customer LTV] Output count: {customer_ltv.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        silver_transactions = spark.read.parquet(silver_path).filter(col("transaction_timestamp") == run_date)
        daily_summary = silver_transactions.groupBy("date") \
           .agg(
                sum("transaction_amount").alias("total_revenue"),
                count("*").alias("total_txns"),
                count(when(col("transaction_status") == "COMPLETED")).alias("unique_txns"),
                 countDistinct("customer_id").alias("unique_customers"),
                 countDistinct("merchant_id").alias("unique_merchants"),
                (count(when(col("transaction_status") == "FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)

        daily_summary.write.partitionBy("date").parquet(output_path)

        logging.info(f"[Stage: Build Daily Summary] Output count: {daily_summary.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        raise

def main():
    spark = (SparkSession.builder
            .appName("Customer Segmentation Pipeline")
             .getOrCreate())

    input_path = "s3://path/to/bronze/transactions"
    bronze_output_path = "s3://path/to/silver/transactions"
    merchants_path = "s3://path/to/merchants_dim"
    silver_output_path = "s3://path/to/silver/transactions"
    gold_output_dir = "s3://path/to/gold_output"
    run_date = "2023-10-01"
    run_id = "run_id_123"

    ingest_bronze(spark, input_path, bronze_output_path, run_date, run_id)
    transform_silver(spark, bronze_output_path, merchants_path, silver_output_path, run_date)
    run_gold(spark, silver_output_path, gold_output_dir, run_date)

if __name__ == "__main__":
    main()
