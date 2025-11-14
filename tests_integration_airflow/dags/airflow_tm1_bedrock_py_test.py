from datetime import timedelta
from time import sleep

import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task

from airflow_provider_tm1.operators.tm1_mdx_query import TM1MDXQueryOperator

from TM1_bedrock_py import bedrock, extractor, transformer, utility, loader
from airflow_provider_tm1.hooks.tm1 import TM1Hook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


def parse_and_filter(df: pd.DataFrame, **kwargs):
    print("test1 dim values:" + str(df.time.values))
    print("test2 dim values:" + str(df.location.values))
    tm1_conn_id = kwargs.get('task').tm1_conn_id
    with TM1Hook(tm1_conn_id=tm1_conn_id).get_conn() as tm1:
        md = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=tm1, cube_name="Revenue")
        print("test1 dim values:" + str(md.time.values))
        print("test2 dim values:" + str(md.location.values))
    return df


with DAG(
        'airflow_tm1_bedrock_py_test',
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(1),
        tags=[],
        catchup=False,
        max_active_runs=1
) as dag:

    @task
    def reduce(dataframes):
        # Returning big dataframes in parse_and_filter and joining them in a list might have performance degration. 
        # If the purpose of reduce() is merely to wait for all the chunks to complete, parse_and_filter should return only a flag or maybe the size of dataframe, but not the entire dataframe
        # However, it might still have acceptable performance even when returning entire mapped dataframes if they are relatively small (and fit to xcom capabilities)
        print("Returned dataframe size: " + str(len(dataframes)))


    dataframes = TM1MDXQueryOperator.partial(
        task_id="map",
        tm1_conn_id='tm1_conn',
        post_callable=parse_and_filter,
        mdx="""
                SELECT 
                NON EMPTY 
                {[time].[ {{ task.op_kwargs['time']}}]} 
                ON COLUMNS , 
                NON EMPTY 
                {[location].[{{ task.op_kwargs['location']}}]}  
                ON ROWS 
                FROM [Revenue] 
                WHERE 
                (
                [measure].[measure].[value]
                )
               """
    ).expand(
        op_kwargs=[{"location": "US", "time": "202501"}, 
                   {"location": "US", "time": "202502"},
                   {"location": "EU", "time": "202501"},
                   {"location": "EU", "time": "202502"}]
    )

    reduce(dataframes.output)


