from typing import Any, Callable, Dict, List, Optional, Tuple
from itertools import product
from string import  Template
from airflow.decorators import task
from airflow_provider_tm1.hooks.tm1 import TM1Hook

from TM1_bedrock_py import extractor
from TM1_bedrock_py.utility import (
    basic_logger,
    get_dimensions_from_set_mdx_list,
    generate_element_lists_from_set_mdx_list,
    TM1CubeObjectMetadata
)
from TM1_bedrock_py.loader import clear_cube, clear_table
from TM1_bedrock_py.bedrock import (
    data_copy_intercube,
    load_sql_data_to_tm1_cube,
    load_tm1_cube_to_sql_table,
    load_csv_data_to_tm1_cube,
    load_tm1_cube_to_csv_file
)



# ------------------------------------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------------------------------------

@task
def dry_run_task(function_name: str, bedrock_params: dict) -> int:
    print(f"Triggering TM1 {function_name} in dry-run mode with parameters ", bedrock_params)
    return 0


def remove_whitespace_from_list(strings: list[str]) -> list[str]:
    return ["".join(s.split()) for s in strings]


def gather_target_metadata(
        tm1_service: Any,
        target_cube_name: str,
        ignore_missing_elements: bool,
        collect_measure_types: bool
) -> Any:
    metadata_obj = TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        collect_dim_element_identifiers=ignore_missing_elements,
        collect_measure_types=collect_measure_types
    )
    return metadata_obj


def validate_param_set_mdx_list(
        dimension_names: list[str],
        param_set_mdx_list: list[str]
) -> list[str]:
    validated_list = []
    for param_set_mdx in param_set_mdx_list:
        param_dimension = get_dimensions_from_set_mdx_list([param_set_mdx])[0]
        if param_dimension in dimension_names:
            validated_list.append(param_set_mdx)

    return validated_list


# ------------------------------------------------------------------------------------------------------------
# Mapping and MDX transform tasks and functions
# ------------------------------------------------------------------------------------------------------------

def build_view_mdx_template(
        cube_name: str,
        dimension_names: list[str],
        dimensions_parallel: list[str]
) -> str:
    """
    Builds an MDX view string given all dimensions and the parallel (WHERE) dimensions.

    Example:
        build_view_mdx(
            ["Versions", "Periods", "Employees", "Measures"],
            ["Versions", "Periods"]
        )
    """
    select_dims = [d.replace(" ", "") for d in dimension_names if d not in dimensions_parallel]
    select_parts = [
        f"{{TM1FilterByLevel({{Tm1SubsetAll([{d}])}}, 0)}}" for d in select_dims
    ]
    select_clause = " *\n".join(select_parts)

    where_dims = [d.replace(" ", "") for d in dimensions_parallel if d in dimension_names]
    where_parts = [f"[{d}].[${d}]" for d in where_dims]
    where_clause = ",\n".join(where_parts)

    mdx = f"""
        SELECT
        NON EMPTY
        {select_clause}
        ON 0
        FROM [{cube_name}]
        WHERE (
            {where_clause}
        )
    """
    print(mdx)
    return mdx


@task
def generate_expand_kwargs_task(
        tm1_service: Any,
        param_set_mdx_list: List[str]
) -> List[Dict]:
    param_names = get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_dict = dict(zip(param_names, param_values))
    return [dict(zip(param_dict.keys(), values)) for values in product(*param_dict.values())]


def generate_mapping_queries_for_slice(
        expand_kwargs: Dict,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None
) -> Tuple[List[Dict] | None, Dict | None]:
    def apply_template(obj: Dict):
        if "mapping_mdx_template" in obj:
            obj["mapping_mdx"] = Template(obj["mapping_mdx_template"]).substitute(**expand_kwargs)
        elif "mapping_sql_template" in obj:
            obj["mapping_sql_query"] = Template(obj["mapping_sql_template"]).substitute(**expand_kwargs)

    def clone_and_apply_template(obj: Dict) -> Dict:
        new_obj = obj.copy()
        apply_template(new_obj)
        return new_obj

    slice_mapping_steps = ([clone_and_apply_template(step) for step in mapping_steps]
                           if mapping_steps else None)

    slice_shared_mapping = (clone_and_apply_template(shared_mapping)
                            if shared_mapping else None)

    return slice_mapping_steps, slice_shared_mapping


def build_bedrock_params_list(
        tm1_connection: str,
        cube_names: list[str],
        unified_bedrock_params: dict,
):
    tm1_hook = TM1Hook(tm1_conn_id=tm1_connection)
    tm1_service = tm1_hook.get_conn()

    bedrock_params_list = []
    for cube_name in cube_names:
        cube_dimensions = tm1_service.cubes.get_dimension_names(cube_name)

        unified_param_list = unified_bedrock_params.get("param_set_mdx_list")
        validated_param_set_mdx_list = validate_param_set_mdx_list(
            dimension_names=cube_dimensions,
            param_set_mdx_list=unified_param_list
        )

        dimensions_parallel = get_dimensions_from_set_mdx_list(validated_param_set_mdx_list)
        mdx_template = build_view_mdx_template(
            cube_name=cube_name,
            dimension_names=cube_dimensions,
            dimensions_parallel=dimensions_parallel
        )

        unified_clear_set_mdx_list = unified_bedrock_params.get("target_clear_set_mdx_list")
        validated_clear_set_mdx_list = validate_param_set_mdx_list(
            dimension_names=cube_dimensions,
            param_set_mdx_list=unified_clear_set_mdx_list
        )

        cube_specific_bedrock_params = unified_bedrock_params.copy()
        # add new parameters
        cube_specific_bedrock_params["target_cube_name"] = cube_name
        cube_specific_bedrock_params["data_mdx_template"] = mdx_template
        # override unified parameters
        cube_specific_bedrock_params["param_set_mdx_list"] = validated_param_set_mdx_list
        cube_specific_bedrock_params["target_clear_set_mdx_list"] = validated_clear_set_mdx_list

        bedrock_params_list.append(cube_specific_bedrock_params)

    return bedrock_params_list


# ------------------------------------------------------------------------------------------------------------
# Clear target functions
# ------------------------------------------------------------------------------------------------------------

@task
def clear_tm1_cube_task(
        tm1_service: Any,
        cube_name: str,
        clear_set_mdx_list: List[str]
) -> int:
    clear_cube(tm1_service=tm1_service, cube_name=cube_name, clear_set_mdx_list=clear_set_mdx_list)
    return 0


@task
def clear_sql_table_task(
        sql_engine: Any,
        table_name: Optional[str],
        sql_delete_statement: Optional[str]
) -> int:
    clear_table(engine=sql_engine, table_name=table_name, delete_statement=sql_delete_statement)
    return 0


# ------------------------------------------------------------------------------------------------------------
# Slice executor tasks:
#     Airflow tasks to execute single slices of data loading using TM1_bedrock_py.bedrock data copy functions.
#     These tasks are used for parallelization based on the expand_kwargs parameter.
# ------------------------------------------------------------------------------------------------------------

@task
def execute_slice_tm1_task(
        tm1_service: Any,
        data_mdx_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        **kwargs
) -> int:

    data_mdx = Template(data_mdx_template).substitute(**expand_kwargs)

    slice_mapping_steps, slice_shared_mapping = generate_mapping_queries_for_slice(mapping_steps=mapping_steps,
                                                                                   shared_mapping=shared_mapping,
                                                                                   expand_kwargs=expand_kwargs)

    basic_logger.info(f"Executing slice for expand_kwargs: {expand_kwargs}")

    data_copy_intercube(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        mapping_steps=slice_mapping_steps,
        shared_mapping=slice_shared_mapping,
        **kwargs
    )

    return 0


@task
def execute_slice_task_sql_to_tm1(
        tm1_service: Any,
        sql_engine: Any,
        sql_query_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        **kwargs
) -> int:

    sql_query = Template(sql_query_template).substitute(**expand_kwargs)

    slice_mapping_steps, slice_shared_mapping = generate_mapping_queries_for_slice(mapping_steps=mapping_steps,
                                                                                   shared_mapping=shared_mapping,
                                                                                   expand_kwargs=expand_kwargs)

    basic_logger.info(f"Executing slice for expand_kwargs: {expand_kwargs}")

    load_sql_data_to_tm1_cube(
        tm1_service=tm1_service,
        sql_query=sql_query,
        sql_engine=sql_engine,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        mapping_steps=slice_mapping_steps,
        shared_mapping=slice_shared_mapping,
        **kwargs
    )

    return 0


@task
def execute_slice_task_tm1_to_sql(
        tm1_service: Any,
        sql_engine: Any,
        data_mdx_template: str,
        target_metadata_function: Callable,
        logging_level: str,
        expand_kwargs: Dict,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        **kwargs
) -> int:

    data_mdx = Template(data_mdx_template).substitute(**expand_kwargs)

    slice_mapping_steps, slice_shared_mapping = generate_mapping_queries_for_slice(mapping_steps=mapping_steps,
                                                                                   shared_mapping=shared_mapping,
                                                                                   expand_kwargs=expand_kwargs)

    basic_logger.info(f"Executing slice for expand_kwargs: {expand_kwargs}")

    load_tm1_cube_to_sql_table(
        tm1_service=tm1_service,
        sql_engine=sql_engine,
        data_mdx=data_mdx,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        mapping_steps=slice_mapping_steps,
        shared_mapping=slice_shared_mapping,
        **kwargs
    )

    return 0


@task
def execute_slice_task_csv_to_tm1(
        tm1_service: Any,
        target_cube_name: str,
        source_csv_file_path: str,
        target_metadata_function: Callable,
        logging_level: str,
        **kwargs
) -> int:

    basic_logger.info(f"Executing slice for source csv file: {source_csv_file_path}")

    load_csv_data_to_tm1_cube(
        tm1_service=tm1_service,
        target_cube_name=target_cube_name,
        source_csv_file_path=source_csv_file_path,
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
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        **kwargs
) -> int:

    data_mdx = Template(data_mdx_template).substitute(**expand_kwargs)

    slice_mapping_steps, slice_shared_mapping = generate_mapping_queries_for_slice(mapping_steps=mapping_steps,
                                                                                   shared_mapping=shared_mapping,
                                                                                   expand_kwargs=expand_kwargs)

    basic_logger.info(f"Executing slice for expand_kwargs: {expand_kwargs}")

    load_tm1_cube_to_csv_file(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        target_metadata_function=target_metadata_function,
        logging_level=logging_level,
        use_blob=True,
        clear_target=False,
        mapping_steps=slice_mapping_steps,
        shared_mapping=slice_shared_mapping,
        **kwargs
    )

    return 0
