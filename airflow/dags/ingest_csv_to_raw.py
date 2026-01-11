"""
DAG: Ingest messy customers CSV into PostgreSQL raw layer
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging

# Config
CSV_PATH = '/opt/airflow/data/messy_customers.csv'
TABLE_NAME = 'raw.csv_customers'
POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'dataeng',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}


def load_csv_to_postgres(**context):
    """Load CSV into PostgreSQL"""
    
    logging.info(f"Reading CSV: {CSV_PATH}")
    
    # Read CSV
    df = pd.read_csv(CSV_PATH)
    
    logging.info(f"Loaded {len(df):,} rows from CSV")
    
    # Add metadata
    df['source_file'] = 'messy_customers.csv'
    df['loaded_at'] = datetime.now()
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # TRUNCATE pour idempotency
        logging.info(f"Truncating {TABLE_NAME}...")
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")
        
        # Prepare batch insert
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]
        
        # Build INSERT query
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f"""
            INSERT INTO {TABLE_NAME} ({','.join(columns)})
            VALUES ({placeholders})
        """
        
        logging.info(f"Inserting {len(values):,} rows...")
        
        # Batch insert
        cursor.executemany(insert_query, values)
        
        conn.commit()
        logging.info(f"Successfully loaded {len(values):,} rows into {TABLE_NAME}")
        
        # Log stats
        cursor.execute(f"""
            INSERT INTO warehouse.pipeline_metadata 
            (pipeline_name, schema_name, table_name, rows_inserted, run_status, completed_at)
            VALUES ('ingest_csv', 'raw', 'csv_customers', %s, 'success', NOW())
        """, (len(values),))
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error: {str(e)}")
        
        cursor.execute(f"""
            INSERT INTO warehouse.pipeline_metadata 
            (pipeline_name, schema_name, table_name, run_status, error_message, completed_at)
            VALUES ('ingest_csv', 'raw', 'csv_customers', 'failed', %s, NOW())
        """, (str(e),))
        conn.commit()
        
        raise
    
    finally:
        cursor.close()
        conn.close()


# Define DAG
with DAG(
    dag_id='ingest_csv_to_raw',
    default_args=default_args,
    description='Load messy customers CSV into raw layer',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'csv', 'raw'],
) as dag:
    
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
        provide_context=True,
    )
