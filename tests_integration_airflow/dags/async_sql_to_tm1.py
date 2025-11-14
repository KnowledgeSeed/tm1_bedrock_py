from pathlib import Path

from datetime import timedelta
import yaml
from airflow import DAG
from airflow.utils.dates import days_ago
from TM1_bedrock_py.airflow_executor.async_executor import tm1_dynamic_executor_task_group, sql_to_tm1_dynamic_executor_task_group

tm1_connection = 'tm1_conn_win1'
sql_connection = 'test_postgres'
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
        'async_sql_to_tm1',
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
        tags=[],
        catchup=False,
        max_active_runs=1
) as dag):

    sql_to_tm1_dynamic_executor_task_group_trigger = sql_to_tm1_dynamic_executor_task_group(
        tm1_connection=tm1_connection,
        sql_connection=sql_connection,
        bedrock_params=BEDROCK_PARAMS['load_sql_to_tm1'],
        dry_run=False,
        logging_level=logging_level
    )

    tm1_dynamic_executor_task_group_trigger = tm1_dynamic_executor_task_group(
        tm1_connection=tm1_connection,
        bedrock_params=BEDROCK_PARAMS['data_copy_intercube'],
        dry_run=False,
        logging_level=logging_level
    )

    sql_to_tm1_dynamic_executor_task_group_trigger >> tm1_dynamic_executor_task_group_trigger
