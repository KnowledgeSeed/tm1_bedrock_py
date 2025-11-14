from pathlib import Path

from datetime import timedelta
import yaml
from airflow import DAG
from airflow.utils.dates import days_ago
from TM1_bedrock_py.airflow_executor.async_executor import csv_to_tm1_dynamic_executor_task_group, tm1_to_csv_dynamic_executor_task_group

tm1_connection = 'tm1_conn_win1'
logging_level = "DEBUG"


config_path = Path(__file__).with_name("dag_config.yaml")
with open(config_path, 'r') as f:
    BEDROCK_PARAMS = yaml.load(f, Loader=yaml.SafeLoader)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with (DAG(
        'async_csv_to_tm1',
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
        tags=[],
        catchup=False,
        max_active_runs=1
) as dag):

    tm1_to_csv = tm1_to_csv_dynamic_executor_task_group(
        tm1_connection=tm1_connection,
        bedrock_params=BEDROCK_PARAMS['tm1_to_csv'],
        dry_run=False,
        logging_level=logging_level
    )

    csv_to_tm1 = csv_to_tm1_dynamic_executor_task_group(
        tm1_connection=tm1_connection,
        bedrock_params=BEDROCK_PARAMS['csv_to_tm1'],
        dry_run=False,
        logging_level=logging_level
    )

    tm1_to_csv >> csv_to_tm1
