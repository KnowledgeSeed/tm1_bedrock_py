"""
Airflow task-group factories for orchestrating TM1 Bedrock workloads.

Each task group follows the same workflow as the synchronous helpers in `bedrock.py`:
mapping data frames are prepared up front, bulk clears run on the full target cube
or table, and metadata for the slice execution is gathered once to avoid redundant
TM1 calls. After those deterministic steps, the data load/export is fanned out into
parallel slices using the same slicing logic as the synchronous implementation, but
the parallelisation is delegated entirely to Airflow's scheduler rather than Python
threads. This lets Airflow manage concurrency, retries, and backfills while the
underlying TM1 operations stay unchanged.
"""

import glob

from airflow_provider_tm1.hooks.tm1 import TM1Hook
from airflow.hooks.base import BaseHook
from airflow.decorators import task_group
from TM1_bedrock_py import utility
from TM1_bedrock_py.airflow_executor import common
import inspect


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> TM1 data copy task group
# ------------------------------------------------------------------------------------------------------------

@task_group
def tm1_dynamic_executor_task_group(
        tm1_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO",
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()

    if not dry_run:
        bedrock_params["param_set_mdx_list"] = common.remove_whitespace_from_list(
            bedrock_params.get("param_set_mdx_list")
        )

        clear_target_cube = common.clear_tm1_cube_task(
            tm1_service=tm1_service,
            cube_name=bedrock_params.get('target_cube_name'),
            clear_set_mdx_list=bedrock_params.get('target_clear_set_mdx_list')
        )

        target_metadata_obj = common.gather_target_metadata(
            tm1_service=tm1_service,
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            collect_measure_types=bedrock_params.get('use_mixed_datatypes')
        )

        def _target_metadata_function(**_kwargs):
            return target_metadata_obj

        execute_slice_tm1 = common.execute_slice_tm1_task.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_metadata_function=_target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            remove_blob=bedrock_params.get('remove_blob'),
            verbose_logging_mode=bedrock_params.get('verbose_logging_mode'),
            verbose_logging_output_dir=bedrock_params.get('verbose_logging_mode'),
            decimal=bedrock_params.get('decimal'),
            use_mixed_datatypes=bedrock_params.get('use_mixed_datatypes'),
            skip_rule_derived_cells=bedrock_params.get('skip_rule_derived_cells')
        ).expand(
            expand_kwargs=common.generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=bedrock_params.get('param_set_mdx_list')
            )
        )
        clear_target_cube >> execute_slice_tm1

    else:
        func_name = inspect.currentframe().f_code.co_name
        common.dry_run_task(func_name, bedrock_params)


# ------------------------------------------------------------------------------------------------------------
# SQL <-> TM1 data copy task groups
# ------------------------------------------------------------------------------------------------------------

@task_group
def sql_to_tm1_dynamic_executor_task_group(
        tm1_connection: str,
        sql_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO",
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()
    conn = BaseHook.get_connection(sql_connection)
    hook = conn.get_hook()
    sql_engine = hook.get_sqlalchemy_engine()

    if not dry_run:

        bedrock_params["param_set_mdx_list"] = common.remove_whitespace_from_list(
            bedrock_params.get("param_set_mdx_list")
        )

        clear_target_cube = common.clear_tm1_cube_task(
            tm1_service=tm1_service,
            cube_name=bedrock_params.get('target_cube_name'),
            clear_set_mdx_list=bedrock_params.get('target_clear_set_mdx_list')
        )

        param_set_mdx_list = bedrock_params.get('param_set_mdx_list')
        target_metadata_obj = common.gather_target_metadata(
            tm1_service=tm1_service,
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            collect_measure_types=bedrock_params.get('use_mixed_datatypes')
        )

        def _target_metadata_function(**_kwargs): return target_metadata_obj

        execute_slice_sql_to_tm1 = common.execute_slice_task_sql_to_tm1.partial(
            tm1_service=tm1_service,
            sql_engine=sql_engine,
            logging_level=logging_level,
            sql_query_template=bedrock_params.get('sql_query_template'),
            target_metadata_function=_target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            use_mixed_datatypes=bedrock_params.get('use_mixed_datatypes')
        ).expand(
            expand_kwargs=common.generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=param_set_mdx_list
            )
        )
        clear_target_cube >> execute_slice_sql_to_tm1

    else:
        func_name = inspect.currentframe().f_code.co_name
        common.dry_run_task(func_name, bedrock_params)


@task_group
def tm1_to_sql_dynamic_executor_task_group(
        tm1_connection: str,
        sql_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO"
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()
    conn = BaseHook.get_connection(sql_connection)
    hook = conn.get_hook()
    sql_engine = hook.get_sqlalchemy_engine()

    if not dry_run:
        clear_target_table = common.clear_sql_table_task(
            sql_engine=sql_engine,
            table_name=bedrock_params.get('table_name'),
            sql_delete_statement=bedrock_params.get('sql_delete_statement')
        )

        param_set_mdx_list = bedrock_params.get('param_set_mdx_list')

        target_cube_name = utility.get_cube_name_from_mdx(bedrock_params.get('data_mdx_template'))

        target_metadata_obj = common.gather_target_metadata(
            tm1_service=tm1_service,
            target_cube_name=target_cube_name,
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            collect_measure_types=False
        )

        def _target_metadata_function(**_kwargs): return target_metadata_obj

        execute_slice_tm1_to_sql = common.execute_slice_task_tm1_to_sql.partial(
            tm1_service=tm1_service,
            sql_engine=sql_engine,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_table_name= bedrock_params.get('target_table_name'),
            sql_schema=bedrock_params.get('sql_schema'),
            related_dimensions=bedrock_params.get('related_dimensions'),
            decimal=bedrock_params.get('decimal'),
            target_metadata_function=_target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping')
        ).expand(
            expand_kwargs=common.generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=param_set_mdx_list
            )
        )
        clear_target_table >> execute_slice_tm1_to_sql

    else:
        func_name = inspect.currentframe().f_code.co_name
        common.dry_run_task(func_name, bedrock_params)


# ------------------------------------------------------------------------------------------------------------
# CSV <-> TM1 data copy task groups
# ------------------------------------------------------------------------------------------------------------

@task_group
def csv_to_tm1_dynamic_executor_task_group(
        tm1_connection: str,
        bedrock_params: dict,
        dry_run: bool = False,
        logging_level: str = "INFO"
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()

    if not dry_run:
        source_csv_files = glob.glob(f"{bedrock_params.get('source_directory')}/*.csv")

        clear_target_cube = common.clear_tm1_cube_task(
            tm1_service=tm1_service,
            cube_name=bedrock_params.get('target_cube_name'),
            clear_set_mdx_list=bedrock_params.get('target_clear_set_mdx_list')
        )

        target_metadata_obj = common.gather_target_metadata(
            tm1_service=tm1_service,
            target_cube_name=bedrock_params.get('target_cube_name'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            collect_measure_types=bedrock_params.get('use_mixed_datatypes')
        )

        def _target_metadata_function(**_kwargs): return target_metadata_obj

        execute_slice_csv_to_tm1 = common.execute_slice_task_csv_to_tm1.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            target_cube_name=bedrock_params.get('target_cube_name'),
            target_metadata_function=_target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            decimal=bedrock_params.get('decimal'),
            delimiter= bedrock_params.get('delimiter'),
            ignore_missing_elements=bedrock_params.get('ignore_missing_elements'),
            use_mixed_datatypes=bedrock_params.get('use_mixed_datatypes')
        ).expand(
            source_csv_file_path=source_csv_files
        )
        clear_target_cube >> execute_slice_csv_to_tm1

    else:
        func_name = inspect.currentframe().f_code.co_name
        common.dry_run_task(func_name, bedrock_params)


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
        target_cube_name = utility.get_cube_name_from_mdx(bedrock_params.get('data_mdx_template'))
        target_metadata_obj = common.gather_target_metadata(
            tm1_service=tm1_service,
            target_cube_name=target_cube_name,
            ignore_missing_elements=False,
            collect_measure_types=False
        )

        def _target_metadata_function(**_kwargs): return target_metadata_obj

        execute_slice_tm1_to_csv = common.execute_slice_task_tm1_to_csv.partial(
            tm1_service=tm1_service,
            logging_level=logging_level,
            data_mdx_template=bedrock_params.get('data_mdx_template'),
            target_metadata_function=_target_metadata_function,
            mapping_steps=bedrock_params.get('mapping_steps'),
            shared_mapping=bedrock_params.get('shared_mapping'),
            target_csv_output_dir=bedrock_params.get('target_csv_output_dir'),
            decimal=bedrock_params.get('decimal'),
            delimiter= bedrock_params.get('delimiter')
        ).expand(
            expand_kwargs=common.generate_expand_kwargs_task(
                tm1_service=tm1_service, param_set_mdx_list=bedrock_params.get('param_set_mdx_list')
            )
        )
        execute_slice_tm1_to_csv
    else:
        func_name = inspect.currentframe().f_code.co_name
        common.dry_run_task(func_name, bedrock_params)


@task_group
def copy_cube_data_on_elements(
        tm1_connection: str,
        cube_names: list[str],
        unified_bedrock_params: dict,
        logging_level: str = "INFO",
):
    bedrock_params_list = common.build_bedrock_params_list(
        tm1_connection=tm1_connection,
        unified_bedrock_params=unified_bedrock_params,
        cube_names=cube_names,
    )

    for i, bedrock_params in enumerate(bedrock_params_list):
        group_id = cube_names[i].replace(" ", "_")
        tm1_dynamic_executor_task_group.override(group_id=group_id)(
            tm1_connection=tm1_connection,
            dry_run=False,
            logging_level=logging_level,
            bedrock_params=bedrock_params
        )
