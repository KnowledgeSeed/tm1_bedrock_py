from airflow_provider_tm1.hooks.tm1 import TM1Hook
from airflow.decorators import task_group
from async_executor_common import *
from TM1_bedrock_py.bedrock import load_csv_data_to_tm1_cube, load_tm1_cube_to_csv_file

import inspect
from string import Template


@task
def execute_slice_task_csv_to_tm1(
        tm1_service: Any,
        sql_engine: Any,
        sql_query_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        **kwargs
) -> int:

    sql_query = Template(sql_query_template).substitute(**expand_kwargs)

    print(expand_kwargs)

    load_csv_data_to_tm1_cube(
        tm1_service=tm1_service,
        sql_engine=sql_engine,
        sql_query=sql_query,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        **kwargs
    )

    return 0


@task
def execute_slice_task_tm1_to_csv(
        tm1_service: Any,
        data_mdx_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        **kwargs
) -> int:

    data_mdx = Template(data_mdx_template).substitute(**expand_kwargs)

    print(expand_kwargs)

    load_tm1_cube_to_csv_file(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        **kwargs
    )

    return 0


@task_group
def csv_to_tm1_dynamic_executor_task_group(
        tm1_connection: str,
        sql_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO"
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()

    if not dry_run:
        generate_mapping_data = generate_mapping_data_task(
            tm1_service=tm1_service,
            shared_mapping=bedrock_params.get('shared_mapping'),
            mapping_steps=bedrock_params.get('mapping_steps')
        )

        clear_target_cube = clear_tm1_cube_task(
            tm1_service=tm1_service,
            cube_name=bedrock_params.get('target_cube_name'),
            clear_set_mdx_list=bedrock_params.get('target_clear_set_mdx_list')
        )

        param_set_mdx_list = bedrock_params.get('param_set_mdx_list')

        target_metadata_obj = gather_target_metadata_function(
            tm1_service=tm1_service,
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements')
        )

        def target_metadata_function(): return target_metadata_obj

        execute_slice_csv_to_tm1 = execute_slice_task_csv_to_tm1.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_metadata_function=target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_csv_output_dir=bedrock_params.get('target_csv_output_dir'),
            decimal=bedrock_params.get('decimal'),
            delimiter= bedrock_params.get('delimiter'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements')
        ).expand(
            expand_kwargs=generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=param_set_mdx_list
            )
        )
        [generate_mapping_data, clear_target_cube] >> execute_slice_csv_to_tm1

    else:
        func_name = inspect.currentframe().f_code.co_name
        dry_run_task(func_name, bedrock_params)


@task_group
def tm1_to_csv_dynamic_executor_task_group(
        tm1_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO"
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()

    if not dry_run:
        generate_mapping_data = generate_mapping_data_task(
            tm1_service=tm1_service,
            shared_mapping=bedrock_params.get('shared_mapping'),
            mapping_steps=bedrock_params.get('mapping_steps')
        )

        target_cube_name = utility._get_cube_name_from_mdx(bedrock_params.get('data_mdx_template'))
        target_metadata_obj = gather_target_metadata_function(
            tm1_service=tm1_service,
            target_cube_name=target_cube_name,
            ignore_missing_elements=False
        )

        def target_metadata_function():
            return target_metadata_obj

        execute_slice_tm1_to_csv = execute_slice_task_csv_to_tm1.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_metadata_function=target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_csv_output_dir=bedrock_params.get('target_csv_output_dir'),
            decimal=bedrock_params.get('decimal'),
            delimiter= bedrock_params.get('delimiter')
        ).expand(
            expand_kwargs=generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=bedrock_params.get('param_set_mdx_list')
            )
        )
        generate_mapping_data >> execute_slice_tm1_to_csv

    else:
        func_name = inspect.currentframe().f_code.co_name
        dry_run_task(func_name, bedrock_params)
