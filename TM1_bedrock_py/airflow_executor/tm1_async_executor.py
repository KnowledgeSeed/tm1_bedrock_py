from airflow_provider_tm1.hooks.tm1 import TM1Hook
from airflow.decorators import task_group
from TM1_bedrock_py.bedrock import data_copy_intercube
from async_executor_common import *

import inspect
from string import Template


@task
def execute_slice_tm1_task(
        tm1_service: Any,
        data_mdx_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        **kwargs
) -> int:
    """
    Airflow task to execute a single slice of data loading using trigger_tm1_bedrock.
    """

    data_mdx = Template(data_mdx_template).substitute(**expand_kwargs)

    utility.basic_logger.debug(expand_kwargs)

    data_copy_intercube(
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
def tm1_dynamic_executor_task_group(
        tm1_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO",
        **kwargs
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

        target_metadata_obj = gather_target_metadata_function(
            tm1_service=tm1_service,
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements')
        )

        def target_metadata_function():
            return target_metadata_obj

        execute_slice_tm1 = execute_slice_tm1_task.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_metadata_function=target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            **kwargs
        ).expand(
            expand_kwargs=generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=bedrock_params.get('param_set_mdx_list')
            )
        )
        [generate_mapping_data, clear_target_cube] >> execute_slice_tm1

    else:
        func_name = inspect.currentframe().f_code.co_name
        dry_run_task(func_name, bedrock_params)