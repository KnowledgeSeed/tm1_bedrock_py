from typing import Any, Callable, Dict, List, Optional
from itertools import product
from airflow.decorators import task

from TM1_bedrock_py import extractor, utility
from TM1_bedrock_py.loader import clear_cube


def gather_target_metadata_function(
        tm1_service: Any,
        target_cube_name: str,
        ignore_missing_elements: bool
) -> Callable:
    metadata_obj = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        collect_dim_element_identifiers=ignore_missing_elements
    )

    def target_metadata_function(**_kwargs): return metadata_obj

    return target_metadata_function


@task
def generate_expand_kwargs_task(
        tm1_service: Any,
        param_set_mdx_list: List[str]
) -> List[Dict]:
    param_names = utility.__get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.__generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_dict = dict(zip(param_names, param_values))
    return [dict(zip(param_dict.keys(), values)) for values in product(*param_dict.values())]


@task
def generate_mapping_data_task(
        tm1_service: Any,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None
) -> int:
    if mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=tm1_service,
        )

    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
        )

    return 0


@task
def clear_tm1_cube_task(
        tm1_service: Any,
        cube_name: str,
        clear_set_mdx_list: List[str]
) -> int:
    clear_cube(tm1_service=tm1_service, cube_name=cube_name, clear_set_mdx_list=clear_set_mdx_list)
    return 0


@task
def dry_run_task(function_name: str, bedrock_params: dict) -> int:
    print(f"Triggering TM1 {function_name} in dry-run mode with parameters ", bedrock_params)
    return 0



