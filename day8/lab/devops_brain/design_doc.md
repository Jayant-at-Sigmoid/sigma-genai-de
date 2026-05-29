# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, cleans it, enriches it with merchant information, and then aggregates it into merchant performance metrics and daily summaries.

## Data Flow Diagram

```plaintext
+--------------------+     +--------------------+     +--------------------+     +--------------------+
|  Source            |     |  Bronze Layer       |     |  Silver Layer       |     |  Gold Layer         |
|  (TRANSACTIONS)    |     |  (bronze_transactions) |     |  (silver_transactions) |     |  (gold_merchant_performance,  |
|                    |     |                       |     |                     |     |  gold_daily_summary) |
+--------------------+     +--------------------+     +--------------------+     +--------------------+
|                    |     |                     |     |                     |     |                    |
| Load Transactions  | --> | Clean & Enrich      | --> | Aggregate Metrics  | --> | Store Processed    |
|                    |     |                     |     |                     |     | Data               |
+--------------------+     +--------------------+     +--------------------+     +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a three-layer approach (Bronze, Silver, Gold) to ensure data is progressively enriched and aggregated.
- **Data Quality Flags**: The Silver layer includes a quality flag to distinguish between clean and dirty data.
- **Aggregative Metrics**: The Gold layer computes both merchant-specific and daily summary metrics for analytical purposes.
- **DuckDB**: Utilized for its lightweight and efficient in-memory database capabilities.

## Known Limitations
- **Data Volume**: The pipeline is not optimized for extremely large datasets; performance may degrade with very high volumes.
- **Single Source**: Currently designed to work with a single source of transactions; extending to multiple sources would require changes.
- **Static Merchant Data**: Merchant data is loaded once; changes in merchant information are not dynamically updated.
- **Failure Handling**: The pipeline does not currently handle retry logic for failed transactions or database operations.

## Dependencies
- **DuckDB**: The database engine used for storing and querying data.
- **MERCHANTS**: A predefined list of merchant information used for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing transaction records.