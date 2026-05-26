from datetime import datetime, timedelta
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp
import logging
import json

# DAG configuration
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# DAG definition
with DAG(
    dag_id='sigma_transaction_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions",
    sla_miss_callback=lambda context: send_email_smtp(to=["alerts@sigma.com"], subject="SLA Miss Alert", html_content=f"DAG: {context['dag'].dag_id}, Execution Date: {context['execution_date']}"),
) as dag:

    # Logging callback for task failures
    def on_failure_callback(context):
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        error_message = context['exception']
        logging.error(f"DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {error_message}")

    # Bronze Layer Task
    def extract_bronze(**context):
        """Ingest raw CSVs to Bronze Parquet"""
        ti = context['ti']
        ti.xcom_push(key='bronze_path', value='/path/to/bronze/layer')
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Started")
        # Ingestion logic here
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Ended")

    # Silver Layer Task
    def transform_silver(**context):
        """Clean, enrich, deduplicate to Silver"""
        ti = context['ti']
        bronze_path = ti.xcom_pull(task_ids='extract_bronze', key='bronze_path')
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Started")
        # Transformation logic here
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Ended")

    # Gold Layer Task
    def build_gold(**context):
        """Generate the 3 Gold aggregation tables"""
        ti = context['ti']
        silver_path = ti.xcom_pull(task_ids='transform_silver', key='silver_path')
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Started")
        # Aggregation logic here
        logging.info(f"[{ti.dag_id}][{ti.task_id}] Ended")

    # Task definitions
    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback,
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback,
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback,
    )

    # Task dependencies
    extract_bronze_task >> transform_silver_task >> build_gold_task
