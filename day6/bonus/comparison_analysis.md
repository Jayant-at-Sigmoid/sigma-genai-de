# NL2SQL vs Cortex Analyst — Sigma DataTech Evaluation
Team: Sigma DataTech Interns
Date: 2026-05-25

## 5-Question Head-to-Head Results

| # | Question | Module 2 SQL Correct? | Cortex SQL Correct? | Module 2 Time | Cortex Time |
|---|----------|--------------------|---------------------|------------|-------------|
| 1 | Total transaction count | YES | YES | ~4.8s | 19.5s |
| 2 | Failed transaction count | YES | YES | ~5.8s | 17.3s |
| 3 | Highest revenue merchant | YES | YES | ~6.4s | 107.7s |
| 4 | Failure rate by payment method | YES | YES | ~4.5s | 86.7s |
| 5 | Total revenue (with COMPLETED filter) | YES | YES | ~5.0s | 22.2s |

## Observations

### Where Module 2 NL2SQL was better:
- **Performance / Latency:** Module 2's implementation using Amazon Nova Lite was significantly faster (averaging under 6 seconds per question) compared to the Cortex setup which suffered from high latency (ranging from 17 seconds to over 100 seconds).
- **Custom SQL formatting:** Module 2 used uppercase formatting and clean aliases consistently as dictated by our customized prompt.
- **Cost:** Boto3 API calls to a lightweight model (Nova Lite) are extremely cheap compared to multiple `mistral-large2` completions inside Snowflake.

### Where Cortex Analyst was better:
- **No Prompt Engineering:** We didn't have to construct custom python files with schema strings, validation, and manual parsing code. The system reads directly from a structured YAML model (`sigma_semantic_model.yaml`).
- **Reliability of Business Rules:** Since metrics (e.g. `total_revenue`, `failure_rate_pct`) are predefined in the YAML configuration, Cortex Analyst maps the query structure directly to these formulas instead of relying on the LLM's raw generation, ensuring higher deterministic consistency.
- **Data Governance:** No data needs to leave Snowflake to external cloud service endpoints since the complete pipeline executes inside Snowflake's security perimeter.

### Business Rule Accuracy
Question 5 is the critical test — revenue must only count COMPLETED transactions. Did both systems apply this rule correctly?
- **Module 2:** Yes, it used `SUM(CASE WHEN STATUS='COMPLETED' THEN AMOUNT ELSE 0 END)` directly inside the query, correctly adhering to the schema context prompt.
- **Cortex:** Yes, it filtered the SELECT query with `WHERE STATUS = 'COMPLETED'`, matching the definition logic mapped from the YAML.

## Your Recommendation

Which approach would you deploy at Sigma DataTech for production self-serve analytics, and why?

### Recommendation: Hybrid/Cortex Analyst approach with optimizations
We recommend deploying **Cortex Analyst** for production self-serve analytics, but optimized to run using the native REST API rather than dual `CORTEX.COMPLETE` prompts, or combined in a hybrid setup. 

**Reasons:**
1. **Maintenance and Scalability:** Maintaining a semantic model YAML is significantly easier and cleaner than maintaining complex system prompts with embedded DDLs and few-shot examples for dozens of tables. As the database schema grows, we can simply version control the YAML without modifying application code.
2. **Security & Compliance:** For enterprise data engineering, keeping transaction queries and responses entirely within the Snowflake security boundary is a key compliance advantage over transmitting schema details and queries to third-party endpoints.
3. **Accuracy & Governance:** Defining metrics like `revenue` centrally in a YAML prevents different developers (or LLM instances) from calculating key metrics in diverging ways, creating a single source of truth for the business.

---

## Stretch Goal — Multi-Turn Conversation Results

We successfully implemented a conversation thread wrapper ([cortex_conversational.py](file:///Users/as-mac-1214/genai-training/sigma-genai-de/day6/bonus/cortex_conversational.py)) and ran follow-up tests:
1. **Q1:** *"Which merchant had the highest revenue?"*
   - **SQL:** Joined `FACT_TRANSACTIONS` and `DIM_MERCHANT` and successfully determined **Zepto** had the highest revenue ($5485.49).
2. **Q2:** *"How many of their transactions failed?"*
   - **SQL:** Resolved the pronoun **"their"** to **Zepto** and performed a subquery filter to correctly find **0** failed transactions.
3. **Q3:** *"What payment method did those customers prefer?"*
   - **SQL:** Correctly identified **"those customers"** as customers who made completed transactions at Zepto, filtering and grouping by `PAYMENT_METHOD` to show **UPI** (4 transactions) as preferred.

**Key Takeaway:** By retaining conversational history in the context window, Cortex Analyst behaves as a stateful agent, allowing users to drill down iteratively into insights without needing to restate the entire business context in each query.

