"""
weather_etl_dag.py — Apache Airflow DAG for ClimaData ETL Pipeline.

Schedules the weather extraction, transformation, and loading pipeline
to run every 6 hours using the main.py CLI interface.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Extracts forecast and historical weather data for ClimaData',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['weather', 'etl', 'climadata'],
) as dag:

    # Start dummy task
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # Main ETL task running via BashOperator
    # We use the existing main.py which already encapsulates Extract -> Transform -> Load
    run_etl = BashOperator(
        task_id='run_etl_forecast',
        bash_command='cd /opt/airflow/app && python main.py --mode forecast',
    )

    # End dummy task
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # Define the DAG dependencies
    start_pipeline >> run_etl >> end_pipeline
