# Pipeline Overview

This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to ensure data is available for downstream analytics and reporting. If it stops, critical business metrics and reports will be unavailable.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection`.
2. Set up required tables using `setup_tables`.
3. Load merchant data into the `merchants` table using `load_merchants`.
4. Load all transactions into the `bronze_transactions` table using `load_bronze`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver`.
6. Load transformed data into the `silver_transactions` table using `load_silver`.
7. Compute merchant performance metrics using `compute_merchant_performance`.
8. Compute daily summary metrics using `compute_daily_summary`.
9. Load performance and summary data into the `gold_merchant_performance` and `gold_daily_summary` tables using `load_gold`.

## Schedule / Trigger

The pipeline runs every hour, triggered by a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection` fails.
2. **Table Setup Failure**
   - **Root Cause:** SQL syntax error in `setup_tables`.
   - **Symptom:** Table creation fails.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants` throws an exception.
4. **Bronze Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze` fails to insert records.
5. **Silver Transformation Failure**
   - **Root Cause:** Missing merchant ID in transactions.
   - **Symptom:** `transform_bronze_to_silver` produces incomplete data.

## Recovery Actions

1. **Database Connection Failure**
   - Check DuckDB service status.
   - Restart the DuckDB service if down.
   - Retry the pipeline.
2. **Table Setup Failure**
   - Review and correct SQL in `setup_tables`.
   - Rerun the pipeline.
3. **Merchant Data Load Failure**
   - Inspect `MERCHANTS` data for corruption.
   - Correct the data and rerun `load_merchants`.
4. **Bronze Load Failure**
   - Validate transaction data for correctness.
   - Correct invalid records and retry `load_bronze`.
5. **Silver Transformation Failure**
   - Ensure all transactions have a valid `merchant_id`.
   - Correct missing IDs and rerun `transform_bronze_to_silver`.

## Known Bugs

- Hardcoded AWS credentials in the source code.
- Lack of null handling in `transform_bronze_to_silver`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the count of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is set correctly in `silver_transactions`.
- Check for missing or incorrect `merchant_name`, `category`, and `city` in `silver_transactions`.
- Validate the sums and counts in `gold_merchant_performance` and `gold_daily_summary`.