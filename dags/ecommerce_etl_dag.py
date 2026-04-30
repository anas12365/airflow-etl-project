from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import logging
import os

# ── Config ──────────────────────────────────────────────
DATA_PATH = "/workspaces/airflow-etl-project/data/data.csv"
DB_PATH   = "/workspaces/airflow-etl-project/data/ecommerce.db"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── 1. EXTRACT ───────────────────────────────────────────
def extract(**context):
    logging.info("📥 Extracting data...")
    df = pd.read_csv(DATA_PATH, encoding="ISO-8859-1")
    logging.info(f"✅ Loaded {len(df)} rows, {df.shape[1]} columns")
    
    # Save raw stats to XCom
    context["ti"].xcom_push(key="raw_rows", value=len(df))
    
    # Save to temp CSV for next task
    tmp = "/tmp/raw_ecommerce.csv"
    df.to_csv(tmp, index=False)
    return tmp

# ── 2. TRANSFORM ─────────────────────────────────────────
def transform(**context):
    logging.info("🔄 Transforming data...")
    tmp = "/tmp/raw_ecommerce.csv"
    df = pd.read_csv(tmp)
    
    raw_count = len(df)
    
    # 2a. Drop duplicates
    df.drop_duplicates(inplace=True)
    
    # 2b. Drop rows with missing CustomerID or Description
    df.dropna(subset=["CustomerID", "Description"], inplace=True)
    
    # 2c. Fix data types
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df["CustomerID"]  = df["CustomerID"].astype(int)
    df["Quantity"]    = df["Quantity"].astype(int)
    df["UnitPrice"]   = df["UnitPrice"].astype(float)
    
    # 2d. Remove cancelled orders (InvoiceNo starts with 'C')
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]
    
    # 2e. Remove negative quantities and zero prices
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
    
    # 2f. Add Revenue column
    df["Revenue"] = df["Quantity"] * df["UnitPrice"]
    
    # 2g. Add date parts for easier analysis
    df["Year"]  = df["InvoiceDate"].dt.year
    df["Month"] = df["InvoiceDate"].dt.month
    df["Day"]   = df["InvoiceDate"].dt.day
    
    clean_count = len(df)
    logging.info(f"✅ Clean rows: {clean_count} (removed {raw_count - clean_count})")
    
    # Save cleaned data
    clean_path = "/tmp/clean_ecommerce.csv"
    df.to_csv(clean_path, index=False)
    context["ti"].xcom_push(key="clean_rows", value=clean_count)
    return clean_path

# ── 3. LOAD ──────────────────────────────────────────────
def load(**context):
    logging.info("📤 Loading into SQLite...")
    clean_path = "/tmp/clean_ecommerce.csv"
    df = pd.read_csv(clean_path)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Main transactions table
    df.to_sql("transactions", conn, if_exists="replace", index=False)
    
    # ── Aggregated tables (bonus for CV!) ──
    
    # Monthly revenue
    monthly = (
        df.groupby(["Year", "Month"])["Revenue"]
        .sum()
        .reset_index()
        .rename(columns={"Revenue": "TotalRevenue"})
    )
    monthly.to_sql("monthly_revenue", conn, if_exists="replace", index=False)
    
    # Top products
    top_products = (
        df.groupby("Description")["Revenue"]
        .sum()
        .reset_index()
        .sort_values("Revenue", ascending=False)
        .head(20)
    )
    top_products.to_sql("top_products", conn, if_exists="replace", index=False)
    
    # Customer summary
    customer_summary = (
        df.groupby("CustomerID")
        .agg(
            TotalOrders=("InvoiceNo", "nunique"),
            TotalRevenue=("Revenue", "sum"),
            TotalItems=("Quantity", "sum"),
        )
        .reset_index()
    )
    customer_summary.to_sql("customer_summary", conn, if_exists="replace", index=False)
    
    conn.close()
    
    clean_rows = context["ti"].xcom_pull(key="clean_rows", task_ids="transform_task")
    logging.info(f"✅ Loaded {clean_rows} rows into {DB_PATH}")
    logging.info("✅ Created tables: transactions, monthly_revenue, top_products, customer_summary")

# ── 4. VALIDATE ──────────────────────────────────────────
def validate(**context):
    logging.info("🔍 Validating data quality...")
    conn = sqlite3.connect(DB_PATH)
    
    checks = {
        "transactions":     "SELECT COUNT(*) FROM transactions",
        "monthly_revenue":  "SELECT COUNT(*) FROM monthly_revenue",
        "top_products":     "SELECT COUNT(*) FROM top_products",
        "customer_summary": "SELECT COUNT(*) FROM customer_summary",
    }
    
    for table, query in checks.items():
        count = conn.execute(query).fetchone()[0]
        assert count > 0, f"❌ Table '{table}' is empty!"
        logging.info(f"✅ {table}: {count} rows")
    
    # Check no nulls in Revenue
    nulls = conn.execute("SELECT COUNT(*) FROM transactions WHERE Revenue IS NULL").fetchone()[0]
    assert nulls == 0, f"❌ Found {nulls} null Revenue values!"
    logging.info("✅ No null Revenue values")
    
    conn.close()
    logging.info("🎉 All validation checks passed!")

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    description="E-Commerce ETL: Extract → Transform → Load → Validate",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "ecommerce", "sqlite"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    validate_task = PythonOperator(
        task_id="validate_task",
        python_callable=validate,
    )

    # Pipeline order
    extract_task >> transform_task >> load_task >> validate_task