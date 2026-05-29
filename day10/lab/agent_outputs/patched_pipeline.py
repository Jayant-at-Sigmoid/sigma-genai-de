# Patched by Self-Healing Agent — 2026-05-29T16:27:32.890673
# Attempts needed: 2

import duckdb, os

DB_PATH = r"/Users/as-mac-1214/genai-training/sigma-genai-de/day10/lab/sigma_platform.duckdb"

def run_student_pipeline():
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM silver_transactions WHERE amount > 0").fetchdf()
    total = df["amount"].sum()
    df_grouped = df.groupby("merchant_id").agg({"amount": "mean"}).reset_index()
    conn.close()
    print(f"Done. Total: {total}")

if __name__ == "__main__":
    run_student_pipeline()