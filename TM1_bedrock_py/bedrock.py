from string import Template
import glob
import math
from pathlib import Path
from datetime import datetime
from typing import Callable, List, Dict, Optional, Any, Sequence, Hashable, Mapping, Iterable, Literal, Union, Tuple

import pandas as pd
from pandas import DataFrame

from concurrent.futures import ThreadPoolExecutor
import asyncio
from TM1py.Exceptions import TM1pyRestException
from requests.cookies import CookieConflictError

from TM1_bedrock_py import utility, transformer, loader, extractor, basic_logger, exception_handling

from TM1_bedrock_py.dimension_builder import apply, normalize
from TM1_bedrock_py.dimension_builder.io import execute_dimension_dataframe_writers
from TM1_bedrock_py.dimension_builder.utility import (
    init_hierarchy_rename_map_for_cloning,
    attr_column_names_from_attr_names,
    get_attribute_columns_list
)

from TM1_bedrock_py.dimension_builder.validate import (
    validate_dimension_for_copy,
    validate_hierarchy_for_copy,
    validate_attribute_name_for_dimension,
    validate_dimension_for_modify,
    validate_schema_for_single_hierarchy
)

from TM1_bedrock_py import validation


# ------------------------------------------------------------------------------------------------------------
# Bedrock: Complex Input Handler functions
#     - input_handler: define a sequence of business logic calculation steps for a custom complex input process
#                      supports functions such as "sumif", "countif", python statement based "if"
# ------------------------------------------------------------------------------------------------------------


@exception_handling.public_operation()
@utility.log_exec_metrics
def input_handler(
        tm1_service: Any,
        input_value: Union[float, int],
        target_cube_name: str,

        domain_coordinates: dict[str, str] = None,
        domain_mdx: str = None,

        pre_calc_mapping_steps: list[dict[str, Any]] = None,
        calculation_steps: list[dict[str, Any]] = None,
        post_calc_mapping_steps: list[dict[str, Any]] = None,

        input_column_name: str = None,
        remove_zero_inputs: bool = True,
        clear_target: bool = False,
        target_clear_set_mdx_list: List[str] = None,

        use_ti_for_load: bool = False,
        use_blob_for_load: bool = False,

        increment: bool = False,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        output_final_state_dataframe: bool = False,
        do_write: bool = True,
        **kwargs
) -> Optional[DataFrame]:
    utility.set_logging_level(logging_level=logging_level)

    if calculation_steps:
        validation.validate_calculation_pipeline_configuration(calculation_steps, basic_logger)

    dataframe = extractor.build_input_domain(
        tm1_service=tm1_service, domain_mdx=domain_mdx, domain_coords=domain_coordinates, **kwargs
    )
    transformer.cast_coordinates_to_str(dataframe.columns, dataframe)
    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value={"Input": input_value})
    dataframe["Input"] = dataframe["Input"].astype(float)

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        **kwargs
    )
    target_cube_dims = target_metadata.get_cube_dims()

    if pre_calc_mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=pre_calc_mapping_steps, tm1_service=tm1_service, **kwargs)
        dataframe = transformer.dataframe_execute_mappings(
            data_df=dataframe, mapping_steps=pre_calc_mapping_steps, **kwargs)

    if calculation_steps:
        for i, step in enumerate(calculation_steps):
            extractor.generate_dataframe_for_calculation_info(
                tm1_service=tm1_service, step=step, data_df=dataframe,
                step_specific_string=str(i + 1), **kwargs)
            dataframe = transformer.dataframe_execute_calculation(
                data_df=dataframe, step=step, **kwargs
            )

    if post_calc_mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=post_calc_mapping_steps, tm1_service=tm1_service, **kwargs)
        dataframe = transformer.dataframe_execute_mappings(
            data_df=dataframe, mapping_steps=post_calc_mapping_steps, **kwargs)

    if output_final_state_dataframe:
        final_state_dataframe = dataframe.copy()

    input_column_name = (
        input_column_name if input_column_name is not None
        else calculation_steps[-1]["name"] if calculation_steps
        else "Input"
    )
    transformer.dataframe_relabel(
        dataframe=dataframe,
        columns={input_column_name: "Value"})
    dataframe["Value"] = dataframe["Value"].astype(float)

    if remove_zero_inputs:
        dataframe = transformer.dataframe_remove_zero_records(dataframe)

    dataframe = transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=target_cube_dims, **kwargs
    )

    if do_write:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        loader.dataframe_to_cube(
            tm1_service=tm1_service,
            dataframe=dataframe,
            cube_name=target_cube_name,
            cube_dims=target_cube_dims,
            use_ti=use_ti_for_load,
            increment=increment,
            use_blob=use_blob_for_load,
            sum_numeric_duplicates=False,
            **kwargs
        )

    if output_final_state_dataframe:
        return final_state_dataframe
    return None


# ------------------------------------------------------------------------------------------------------------
# Bedrock: Cube Builder Module functions
#     - cube_builder: build cube from manual input data or copy structure from source server
# ------------------------------------------------------------------------------------------------------------


@exception_handling.public_operation()
@utility.log_exec_metrics
def cube_builder(
        tm1_service: Any,
        build_mode: Literal["create_from_map", "copy_from_source"] = "create_from_map",
        if_cube_exist_strategy: Literal["rebuild", "skip", "raise_error"] = "skip",

        cube_dimension_create_map: dict[str, list[str]] = None,

        copy_source_tm1_service: Any = None,
        copy_source_cubes: Union[list[str], str] = None,
        copy_cube_rename_map: dict[str, str] = None,
        copy_dimension_rename_map: dict[str, str] = None,
        missing_dimension_strategy: Literal["copy_from_source", "raise_error"] = "raise_error",

        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        input_error_mode: Literal["strict", "loose"] = "strict",
        **dim_builder_kwargs
) -> None:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_choice("build_mode", build_mode, {"create_from_map", "copy_from_source"})
    validation.validate_choice("if_cube_exist_strategy", if_cube_exist_strategy, {"rebuild", "skip", "raise_error"})
    validation.validate_choice(
        "missing_dimension_strategy", missing_dimension_strategy, {"copy_from_source", "raise_error"}
    )
    validation.validate_choice("input_error_mode", input_error_mode, {"strict", "loose"})
    utility.validate_cube_create_inputs(
        build_mode=build_mode,
        cube_dimension_create_map=cube_dimension_create_map,
        copy_source_cubes=copy_source_cubes,
        copy_cube_rename_map=copy_cube_rename_map,
        copy_dimension_rename_map=copy_dimension_rename_map,
        input_error_mode=input_error_mode,
        missing_dimension_strategy=missing_dimension_strategy,
        tm1_service=tm1_service, copy_source_tm1_service=copy_source_tm1_service
    )

    copy_source_tm1_service = copy_source_tm1_service or tm1_service
    copy_source_cubes = [copy_source_cubes] if isinstance(copy_source_cubes, str) \
        else copy_source_cubes if isinstance(copy_source_cubes, list) \
        else []
    copy_cube_rename_map = copy_cube_rename_map or {}
    copy_dimension_rename_map = copy_dimension_rename_map or {}
    cube_dimension_create_map = cube_dimension_create_map or {}

    if build_mode == "copy_from_source":
        utility.fetch_cube_structure_data(copy_source_tm1_service, cube_dimension_create_map,
                                          copy_source_cubes, copy_cube_rename_map, copy_dimension_rename_map)

    unique_dimensions_list = utility.create_unique_dim_list_from_cube_dim_map(cube_dimension_create_map)
    missing_dimensions = utility.check_dimensions_existance(
        tm1_service,
        unique_dimensions_list,
        missing_dimension_strategy=missing_dimension_strategy
    )

    if missing_dimension_strategy == "copy_from_source" and len(missing_dimensions) > 0:
        if copy_source_tm1_service is tm1_service:
            raise ValueError(
                "Missing dimensions cannot be copied because source and target "
                "TM1 services are the same."
            )
        missing_dimensions_rename_map = utility.get_dimension_copy_map_for_missing(
            missing_dimensions, copy_dimension_rename_map)
        for source, target in missing_dimensions_rename_map.items():
            basic_logger.debug(f"Copying missing dimension from source to target")
            dimension_copy(
                tm1_service=copy_source_tm1_service,
                target_tm1_service=tm1_service,
                source_dimension_name=source,
                target_dimension_name=target,
                logging_level=logging_level,
                **dim_builder_kwargs
            )

    utility.create_cubes(tm1_service, cube_dimension_create_map, if_cube_exist_strategy)


# ------------------------------------------------------------------------------------------------------------
# Bedrock: Dimension Builder Module functions
#     - dimension builder, hierarchy builder (imports)
#     - dimension copy, hierarchy copy
#     - dimension modify, hierarchy modify
#     - hierarchy from attributes
#     - dimension export
# ------------------------------------------------------------------------------------------------------------


@exception_handling.public_operation()
@utility.log_exec_metrics
def dimension_builder(
        dimension_name: str,
        input_format: Literal["parent_child", "indented_levels", "filled_levels"],
        build_strategy: Literal["rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"],
        tm1_service: Any,
        hierarchy_name: str = None,

        old_orphan_parent_name: str = "OrphanParent",
        new_orphan_parent_name: str = "OrphanParent",

        input_datasource: Optional[Union[str, Path]] = None,
        sql_engine: Optional[Any] = None,
        sql_table_name: Optional[str] = None,
        sql_query: Optional[str] = None,
        filter_input_columns: Optional[list[str]] = None,
        raw_input_df: pd.DataFrame = None,

        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        level_columns: Optional[list[str]] = None, type_column: Optional[str] = None,
        weight_column: Optional[str] = None,

        input_elements_datasource: Optional[Union[str, Path]] = None,
        input_elements_df_element_column: Optional[str] = None,
        sql_elements_engine: Optional[Any] = None,
        sql_table_elements_name: Optional[str] = None,
        sql_elements_query: Optional[str] = None,
        filter_input_elements_columns: Optional[list[str]] = None,
        raw_input_elements_df: pd.DataFrame = None,

        override_input_edges_df: pd.DataFrame = None,
        override_input_elements_df: pd.DataFrame = None,

        allow_type_changes: bool = False,
        remove_empty_subtrees: bool = False,
        output_mode: Literal["build", "build_and_output", "output"] = "build",

        dimension_sort_order_config: dict[str, str] = None,
        hierarchy_sort_order_config: dict[str, dict[str, str]] = None,

        attribute_parser: Union[Literal["colon", "square_brackets", "square_brackets_start"], Callable] = "colon",
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        **kwargs
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_choice("input_format", input_format, {"parent_child", "indented_levels", "filled_levels"})
    validation.validate_choice(
        "build_strategy", build_strategy, {"rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"}
    )
    validation.validate_choice("output_mode", output_mode, {"build", "build_and_output", "output"})

    if build_strategy == 'update' and allow_type_changes:
        basic_logger.warning("Update mode doesnt allow type change, parameter was set to false")
        allow_type_changes = False

    # Copied flat dimensions / hierarchies can legitimately have elements but no parent-child edges.
    # `override_input_elements_df` is therefore the required signal for override mode, while
    # `override_input_edges_df` may remain None for edge-less structures retrieved from TM1.
    if override_input_elements_df is not None:
        input_edges_df = override_input_edges_df
        input_elements_df = override_input_elements_df
    elif override_input_edges_df is not None:
        raise ValueError(
            "'override_input_elements_df' is required when using override input dataframes."
        )
    else:
        input_edges_df, input_elements_df = apply.init_input_schema(
            dimension_name=dimension_name, hierarchy_name=hierarchy_name, input_format=input_format,

            input_datasource=input_datasource,
            sql_engine=sql_engine, sql_table_name=sql_table_name, sql_query=sql_query,
            filter_input_columns=filter_input_columns, raw_input_df=raw_input_df,
            dim_column=dim_column, hier_column=hier_column,
            parent_column=parent_column, child_column=child_column, level_columns=level_columns,
            weight_column=weight_column, type_column=type_column,

            input_elements_datasource=input_elements_datasource,
            input_elements_df_element_column=input_elements_df_element_column,
            sql_elements_engine=sql_elements_engine,
            sql_table_elements_name=sql_table_elements_name, sql_elements_query=sql_elements_query,
            filter_input_elements_columns=filter_input_elements_columns,
            raw_input_elements_df=raw_input_elements_df,
            attribute_parser=attribute_parser,
            **kwargs
        )

    # get existing if dim exists - important for type check consistency too
    existing_edges_df, existing_elements_df = apply.init_existing_schema_for_builder(
        tm1_service=tm1_service, dimension_name=dimension_name, old_orphan_parent_name=old_orphan_parent_name)

    # clear conflicts and make updates on input using existing
    updated_edges_df, updated_elements_df = apply.resolve_schema(
        tm1_service=tm1_service, dimension_name=dimension_name,
        input_edges_df=input_edges_df, input_elements_df=input_elements_df,
        existing_edges_df=existing_edges_df, existing_elements_df=existing_elements_df,
        orphan_parent_name=new_orphan_parent_name,
        mode=build_strategy,
        allow_type_changes=allow_type_changes)

    if remove_empty_subtrees:
        updated_edges_df, updated_elements_df = apply.remove_empty_subtrees(updated_edges_df, updated_elements_df)

    # upload updated dim structure using tm1py dimension/hierarchy/element objects
    dimension = apply.build_dimension_object(dimension_name=dimension_name, edges_df=updated_edges_df,
                                             elements_df=updated_elements_df)

    if output_mode in ("build", "build_and_output"):
        tm1_service.dimensions.update_or_create(dimension)

        apply.apply_hierarchy_sort_order_attributes(tm1_service, dimension_name,
                                                    dimension_sort_order_config, hierarchy_sort_order_config)

        # upload updated attribute values using bedrock load
        attr_columns = get_attribute_columns_list(updated_elements_df)
        if len(attr_columns) != 0:
            writable_attr_df, attr_cube_name, attr_cube_dims = apply.prepare_attributes_for_load(
                dimension_name=dimension_name, elements_df=updated_elements_df)

            apply.create_attribute_structure(
                tm1_service=tm1_service, attr_cols=attr_columns,
                attr_cube_name=attr_cube_name, dimension_name=dimension_name, attribute_parser=attribute_parser)

            loader.dataframe_to_cube(
                tm1_service=tm1_service,
                dataframe=writable_attr_df,
                cube_name=attr_cube_name,
                cube_dims=attr_cube_dims,
                use_blob=True,
            )

    if output_mode in ("output", "build_and_output"):
        return updated_edges_df, updated_elements_df


@exception_handling.public_operation()
@utility.log_exec_metrics
def hierarchy_builder(
        dimension_name: str,
        hierarchy_name: str,
        input_format: Literal["parent_child", "indented_levels", "filled_levels"],
        build_strategy: Literal["rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"],
        tm1_service: Any,

        old_orphan_parent_name: str = "OrphanParent",
        new_orphan_parent_name: str = "OrphanParent",

        input_datasource: Optional[Union[str, Path]] = None,
        sql_engine: Optional[Any] = None,
        sql_table_name: Optional[str] = None,
        sql_query: Optional[str] = None,
        filter_input_columns: Optional[list[str]] = None,
        raw_input_df: pd.DataFrame = None,

        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        level_columns: Optional[list[str]] = None, type_column: Optional[str] = None,
        weight_column: Optional[str] = None,

        input_elements_datasource: Optional[Union[str, Path]] = None,
        input_elements_df_element_column: Optional[str] = None,
        sql_elements_engine: Optional[Any] = None,
        sql_table_elements_name: Optional[str] = None,
        sql_elements_query: Optional[str] = None,
        filter_input_elements_columns: Optional[list[str]] = None,
        raw_input_elements_df: pd.DataFrame = None,

        override_input_edges_df: pd.DataFrame = None,
        override_input_elements_df: pd.DataFrame = None,

        remove_empty_subtrees: bool = False,
        output_mode: Literal["build", "build_and_output", "output"] = "build",

        hierarchy_sort_order_config: dict[str, str] = None,

        attribute_parser: Union[Literal["colon", "square_brackets", "square_brackets_start"], Callable] = "colon",
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        **kwargs
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_choice("input_format", input_format, {"parent_child", "indented_levels", "filled_levels"})
    validation.validate_choice(
        "build_strategy", build_strategy, {"rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"}
    )
    validation.validate_choice("output_mode", output_mode, {"build", "build_and_output", "output"})

    # Copied flat dimensions / hierarchies can legitimately have elements but no parent-child edges.
    # `override_input_elements_df` is therefore the required signal for override mode, while
    # `override_input_edges_df` may remain None for edge-less structures retrieved from TM1.
    if override_input_elements_df is not None:
        input_edges_df = override_input_edges_df
        input_elements_df = override_input_elements_df
    elif override_input_edges_df is not None:
        raise ValueError(
            "'override_input_elements_df' is required when using override input dataframes."
        )
    else:
        input_edges_df, input_elements_df = apply.init_input_schema(
            dimension_name=dimension_name, hierarchy_name=hierarchy_name, input_format=input_format,

            input_datasource=input_datasource,
            sql_engine=sql_engine, sql_table_name=sql_table_name, sql_query=sql_query,
            filter_input_columns=filter_input_columns, raw_input_df=raw_input_df,
            dim_column=dim_column, hier_column=hier_column,
            parent_column=parent_column, child_column=child_column, level_columns=level_columns,
            weight_column=weight_column, type_column=type_column,

            input_elements_datasource=input_elements_datasource,
            input_elements_df_element_column=input_elements_df_element_column,
            sql_elements_engine=sql_elements_engine,
            sql_table_elements_name=sql_table_elements_name, sql_elements_query=sql_elements_query,
            filter_input_elements_columns=filter_input_elements_columns,
            raw_input_elements_df=raw_input_elements_df,
            attribute_parser=attribute_parser,
            **kwargs
        )

    validate_schema_for_single_hierarchy(input_elements_df)

    # get existing if dim exists - important for type check consistency too
    existing_edges_df, existing_elements_df = apply.init_existing_schema_for_builder(
        tm1_service, dimension_name, old_orphan_parent_name)

    # clear conflicts and make updates on input using existing
    updated_edges_df, updated_elements_df = apply.resolve_schema(
        tm1_service=tm1_service, dimension_name=dimension_name,
        input_edges_df=input_edges_df, input_elements_df=input_elements_df,
        existing_edges_df=existing_edges_df, existing_elements_df=existing_elements_df,
        orphan_parent_name=new_orphan_parent_name,
        mode=build_strategy,
        hierarchy_build_mode=True,
        hierarchy_name=hierarchy_name)

    if remove_empty_subtrees:
        updated_edges_df, updated_elements_df = apply.remove_empty_subtrees(updated_edges_df, updated_elements_df)

    # upload updated dim structure using tm1py dimension/hierarchy/element objects
    hierarchy = apply.build_hierarchy_object(dimension_name=dimension_name, hierarchy_name=hierarchy_name,
                                             edges_df=updated_edges_df, elements_df=updated_elements_df)

    if output_mode in ("build", "build_and_output"):
        tm1_service.hierarchies.update_or_create(hierarchy)

        if hierarchy_sort_order_config is not None:
            apply.apply_hierarchy_sort_order_attributes(
                tm1_service=tm1_service, dimension_name=dimension_name,
                hierarchy_sort_order_config={hierarchy_name: hierarchy_sort_order_config})

        # upload updated attribute values using bedrock load
        attr_columns = get_attribute_columns_list(updated_elements_df)
        if len(attr_columns) != 0:
            writable_attr_df, attr_cube_name, attr_cube_dims = apply.prepare_attributes_for_load(
                dimension_name=dimension_name, elements_df=updated_elements_df)

            apply.create_attribute_structure(
                tm1_service=tm1_service, attr_cols=attr_columns,
                attr_cube_name=attr_cube_name, dimension_name=dimension_name, attribute_parser=attribute_parser)

            loader.dataframe_to_cube(
                tm1_service=tm1_service,
                dataframe=writable_attr_df,
                cube_name=attr_cube_name,
                cube_dims=attr_cube_dims,
                use_blob=True,
            )

    if output_mode in ("output", "build_and_output"):
        return updated_edges_df, updated_elements_df


@exception_handling.public_operation()
@utility.log_exec_metrics
def dimension_copy(
        tm1_service: Any,
        source_dimension_name: str,
        target_dimension_name: str = None,
        build_strategy: Literal["rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"] = "rebuild",
        source_hierarchy_filter: list[str] = None,
        hierarchy_rename_map: dict = None,
        rename_default_hierarchy: bool = True,
        allow_type_changes: bool = False,
        target_tm1_service: Any = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        **builder_kwargs
) -> None:
    utility.set_logging_level(logging_level=logging_level)

    # prepare steps for target
    target_tm1_service = target_tm1_service or tm1_service
    target_dimension_name = target_dimension_name or source_dimension_name

    # manage source hierarchies and renaming
    source_hierarchies_actual = list(
        set(tm1_service.hierarchies.get_all_names(source_dimension_name)) - set(['Leaves']))
    hierarchy_scope = source_hierarchy_filter if source_hierarchy_filter is not None else source_hierarchies_actual
    hierarchy_rename_map = init_hierarchy_rename_map_for_cloning(
        source_dimension_name, source_hierarchies_actual, target_dimension_name,
        hierarchy_rename_map, rename_default_hierarchy
    )

    # validate for copy
    validate_dimension_for_copy(tm1_service, source_dimension_name, source_hierarchies_actual, hierarchy_rename_map,
                                source_hierarchy_filter)

    # get source data and normalize it
    edges_df, elements_df = apply.init_existing_schema_filtered(tm1_service, source_dimension_name, hierarchy_scope)

    # transforms
    edges_df, elements_df = normalize.transform_hierarchy_structure_for_copy(
        edges_df, elements_df, hierarchy_rename_map, target_dimension_name)

    dimension_builder(
        dimension_name=target_dimension_name,
        input_format='parent_child',
        tm1_service=target_tm1_service,
        build_strategy=build_strategy,
        override_input_edges_df=edges_df,
        override_input_elements_df=elements_df,
        allow_type_changes=allow_type_changes,
        logging_level=logging_level,
        **builder_kwargs
    )


@exception_handling.public_operation()
@utility.log_exec_metrics
def hierarchy_copy(
        tm1_service: Any,
        source_dimension_name: str,
        source_hierarchy_name: str,
        target_tm1_service: Any = None,
        target_dimension_name: str = None,
        target_hierarchy_name: str = None,
        build_strategy: Literal["rebuild", "safe_rebuild", "safe_rebuild_unwind", "update"] = "rebuild",
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        **builder_kwargs
) -> None:
    utility.set_logging_level(logging_level=logging_level)

    # prepare steps
    target_tm1_service = target_tm1_service or tm1_service
    target_hierarchy_name = target_hierarchy_name or source_hierarchy_name
    target_dimension_name = target_dimension_name or source_dimension_name

    validate_hierarchy_for_copy(tm1_service, source_dimension_name, source_hierarchy_name)

    # get source data
    edges_df, elements_df = apply.init_existing_schema_filtered(
        tm1_service, source_dimension_name, [source_hierarchy_name])

    # transform steps
    edges_df, elements_df = normalize.transform_hierarchy_structure_for_copy(
        edges_df, elements_df, hierarchy_rename_map={source_hierarchy_name: target_hierarchy_name},
        target_dimension_name=target_dimension_name
    )

    hierarchy_builder(
        dimension_name=target_dimension_name,
        hierarchy_name=target_hierarchy_name,
        input_format='parent_child',
        tm1_service=target_tm1_service,
        build_strategy=build_strategy,
        override_input_edges_df=edges_df,
        override_input_elements_df=elements_df,
        logging_level=logging_level,
        **builder_kwargs
    )


@exception_handling.public_operation()
def dimension_modify(
        tm1_service: Any,
        dimension_name: str,
        modify_function: Callable,
        modify_function_args: List = None,
        modify_function_kwargs: Dict = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
) -> None:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_callable("modify_function", modify_function)
    validate_dimension_for_modify(tm1_service, dimension_name)

    # retrieve and normalize existing schema that is ready for back upload
    edges_df, elements_df = apply.init_existing_schema_full(tm1_service=tm1_service, dimension_name=dimension_name)

    # call custom modify function:
    #     expects edges_df, elements_df as first two parameters
    #     other parameters can be passed through args and kwargs
    #     expected to output edges_df, elements_df in this exact order

    modified_edges_df, modified_elements_df = validation.validate_schema_callback_result(
        "modify_function",
        modify_function(
            edges_df,
            elements_df,
            *(modify_function_args or []),
            **(modify_function_kwargs or {})
        )
    )
    dimension_builder(
        dimension_name=dimension_name,
        input_format='parent_child',
        tm1_service=tm1_service,
        build_strategy='rebuild',
        override_input_edges_df=modified_edges_df,
        override_input_elements_df=modified_elements_df,
        logging_level=logging_level
    )


@exception_handling.public_operation()
def hierarchy_modify(
        tm1_service: Any,
        dimension_name: str,
        hierarchy_name: str,
        modify_function: Callable,
        modify_function_args: List = None,
        modify_function_kwargs: Dict = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
) -> None:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_callable("modify_function", modify_function)
    validate_dimension_for_modify(tm1_service, dimension_name, hierarchy_name)

    # retrieve and normalize existing schema that is ready for back upload
    edges_df, elements_df = apply.init_existing_schema_filtered(tm1_service=tm1_service,
                                                                dimension_name=dimension_name,
                                                                hierarchy_names=[hierarchy_name])

    # call custom modify function:
    #     expects edges_df, elements_df as first two parameters
    #     other parameters can be passed through args and kwargs
    #     expected to output edges_df, elements_df in this exact order

    modified_edges_df, modified_elements_df = validation.validate_schema_callback_result(
        "modify_function",
        modify_function(
            edges_df,
            elements_df,
            *(modify_function_args or []),
            **(modify_function_kwargs or {})
        )
    )
    hierarchy_builder(
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
        input_format='parent_child',
        tm1_service=tm1_service,
        build_strategy='rebuild',
        override_input_edges_df=modified_edges_df,
        override_input_elements_df=modified_elements_df,
        logging_level=logging_level
    )


@exception_handling.public_operation()
def hierarchy_build_from_attributes(
        tm1_service: Any,
        dimension_name: str,
        attributes: list[str],
        new_hierarchy_name: str = None,
        target_tm1_service: Any = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
) -> None:
    utility.set_logging_level(logging_level=logging_level)
    if not isinstance(attributes, list) or not attributes:
        raise ValueError("'attributes' must be a non-empty list of attribute names.")
    if not all(isinstance(attribute, str) and attribute.strip() for attribute in attributes):
        raise ValueError("Every item in 'attributes' must be a non-empty string.")

    target_tm1_service = target_tm1_service or tm1_service

    source_hierarchy_name = utility.get_default_hierarchy(tm1_service, dimension_name)

    validate_hierarchy_for_copy(tm1_service, dimension_name, source_hierarchy_name)
    validate_attribute_name_for_dimension(tm1_service, dimension_name, attributes)

    new_hierarchy_name = new_hierarchy_name or '_'.join(attributes)

    _, existing_elements_df = apply.init_existing_schema_filtered(
        tm1_service, dimension_name, hierarchy_names=[source_hierarchy_name])
    existing_elements_df = existing_elements_df.loc[
        existing_elements_df["ElementType"].isin(["Numeric", "String"])].copy()

    attr_columns = attr_column_names_from_attr_names(attributes, existing_elements_df)

    attr_hier_edges_df, attr_hier_elements_df = apply.generate_schema_from_attributes(
        existing_elements_df, attr_columns
    )
    attr_hier_edges_df, attr_hier_elements_df = normalize.normalize_existing_schema_full(attr_hier_edges_df,
                                                                                         attr_hier_elements_df)
    hierarchy_builder(
        dimension_name=dimension_name,
        hierarchy_name=new_hierarchy_name,
        input_format='parent_child',
        tm1_service=target_tm1_service,
        build_strategy='rebuild',
        override_input_edges_df=attr_hier_edges_df,
        override_input_elements_df=attr_hier_elements_df,
        logging_level=logging_level
    )


@exception_handling.public_operation()
@utility.log_exec_metrics
def dimension_export(
        dimension_name: str,
        output_format: Literal["parent_child", "indented_levels", "filled_levels"],
        target_destinations: List[Literal["sql", "csv", "xlsx", "yaml", "json"]],
        tm1_service: Any = None,
        edges_df: Optional[pd.DataFrame] = None,
        elements_df: pd.DataFrame = None,
        column_rename_mapping: Dict = None,
        *,
        file_path_destination: Optional[str] = None,
        table_name: Optional[str] = None,
        sql_engine: Optional[Any] = None,
        sql_connection: Optional[Any] = None,
        sql_function: Optional[Union[
            Callable[..., DataFrame],
            Literal["sqlalchemy", "pyodbc", "psycopg2", "snowflake"]
        ]] = None,
        if_exists_strategy: Literal["fail", "replace", "append"] = "append",
        database_schema: Optional[str] = None,
        table_column_order: Optional[list[str]] = None,
        csv_separator: str = None,
        decimal_separator: str = None,
        excel_sheet_name: str = "Sheet1",
        include_index: bool = False,
        yaml_default_flow_style: bool = False,
        json_orientation: Literal["split", "records", "index", "columns", "values", "table"] = "records",
        maximum_levels_depth: int = None,
        orphan_parent_name: str = "OrphanParent",
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        **writer_kwargs
) -> None:
    utility.set_logging_level(logging_level=logging_level)
    validation.validate_choice("output_format", output_format, {"parent_child", "indented_levels", "filled_levels"})
    validation.validate_choice("if_exists_strategy", if_exists_strategy, {"fail", "replace", "append"})
    if not isinstance(target_destinations, list) or not target_destinations:
        raise ValueError("'target_destinations' must contain at least one output destination.")

    if tm1_service is None and elements_df is None:
        raise ValueError("Must provide at least one source of local/server")

    if elements_df is None:
        edges_df, elements_df = apply.init_existing_schema_for_builder(
            tm1_service=tm1_service, dimension_name=dimension_name, old_orphan_parent_name=orphan_parent_name)

    column_rename_mapping = column_rename_mapping or {}

    combined_schema = apply.combine_schema_for_export(
        elements_df=elements_df, edges_df=edges_df, orphan_parent_name=orphan_parent_name,
        column_rename_mapping=column_rename_mapping, format_selector=output_format,
        maximum_levels_depth=maximum_levels_depth
    )

    execute_dimension_dataframe_writers(
        dataframe=combined_schema,
        target_destinations=target_destinations,
        file_path_destination=file_path_destination,
        table_name=table_name,
        sql_engine=sql_engine,
        sql_connection=sql_connection,
        sql_function=sql_function,
        table_column_order=table_column_order,
        if_exists_strategy=if_exists_strategy,
        database_schema=database_schema,
        csv_separator=csv_separator,
        decimal_separator=decimal_separator,
        excel_sheet_name=excel_sheet_name,
        include_index=include_index,
        yaml_default_flow_style=yaml_default_flow_style,
        json_orientation=json_orientation,
        **writer_kwargs
    )


# ------------------------------------------------------------------------------------------------------------
# Bedrock: ETL Module functions
#     - data copy, data copy intercube
#     - sql to tm1, tm1 to sql
#     - csv to tm1, tm1 to csv
#     - async executors
# ------------------------------------------------------------------------------------------------------------


@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def data_copy_intercube(tm1_service: Optional[Any],
                        target_cube_name: str,
                        target_tm1_service: Optional[Any] = None,
                        target_metadata_function: Optional[Callable[..., Any]] = None,
                        data_mdx: Optional[str] = None,
                        mdx_function: Optional[
                            Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
                        data_mdx_list: Optional[list[str]] = None,

                        skip_zeros: Optional[bool] = False,
                        skip_consolidated_cells: Optional[bool] = False,
                        skip_rule_derived_cells: Optional[bool] = False,

                        sql_engine: Optional[Any] = None,
                        sql_function: Optional[Callable[..., DataFrame]] = None,
                        csv_function: Optional[Callable[..., DataFrame]] = None,

                        case_and_space_insensitive_inputs: Optional[bool] = False,

                        check_missing_elements: Optional[bool] = False,
                        dimensions_to_check: Optional[list[str]] = None,
                        log_missing_elements: Optional[bool] = False,
                        output_missing_elements: Optional[bool] = False,
                        fallback_elements: Optional[Dict] = None,
                        raise_error_if_missing_found: Optional[bool] = False,
                        element_query_mode: Literal['bulk', 'on_demand'] = 'bulk',
                        audit_mode: bool = False,
                        check_missing_elements_audit: Optional[bool] = False,

                        mapping_steps: Optional[List[Dict]] = None,
                        shared_mapping: Optional[Dict] = None,

                        clear_target: Optional[bool] = False,
                        target_clear_set_mdx_list: Optional[List[str]] = None,
                        clear_source: Optional[bool] = False,
                        source_clear_set_mdx_list: Optional[List[str]] = None,

                        value_function: Optional[Callable[..., Any]] = None,
                        pre_load_function: Optional[Callable] = None,
                        pre_load_args: Optional[List] = None,
                        pre_load_kwargs: Optional[Dict] = None,

                        async_write: Optional[bool] = False,
                        slice_size_of_dataframe: Optional[int] = 50000,
                        use_ti: Optional[bool] = False,
                        use_blob: Optional[bool] = False,
                        cast_cell_type_mapping_on_values: Optional[bool] = False,

                        increment: Optional[bool] = False,
                        sum_numeric_duplicates: Optional[bool] = False,
                        aggregate_numeric_duplicates: bool = False,

                        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
                        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
                        verbose_logging_output_dir: Optional[str] = None, **kwargs) -> Optional[DataFrame]:
    """
    Copy TM1 data into another TM1 cube or server.

    This is the main TM1-to-TM1 wrapper in Bedrock. It extracts source data
    from ``data_mdx`` or ``data_mdx_list``, applies optional mappings and value
    transformations, aligns the result to the target cube structure, and writes
    it to ``target_cube_name``.

    Commonly used options include ``mapping_steps``, ``shared_mapping``,
    ``source_dim_mapping``, ``related_dimensions``, ``target_dim_mapping``,
    ``check_missing_elements``, ``clear_target``, ``async_write``, ``use_ti``,
    and ``use_blob``.
    """
    if not target_tm1_service:
        target_tm1_service = tm1_service
    validation.validate_choice("element_query_mode", element_query_mode, {"bulk", "on_demand"})

    native_view_correction_enabled = (
            mdx_function == "native_view_extractor" and not case_and_space_insensitive_inputs)

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        tm1_service=tm1_service,
        collect_measure_types=cast_cell_type_mapping_on_values,
    )
    source_cube_name = data_metadata_queryspecific.get_cube_name()

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_itemskip_info=check_missing_elements,
        dimension_check_filter=dimensions_to_check,
        itemskip_query_mode=element_query_mode,
        **kwargs
    )

    source_cube_dims = data_metadata_queryspecific.get_cube_dims()
    target_cube_dims = target_metadata.get_cube_dims()

    dataframe = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        cube_dimensions=source_cube_dims,
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    transformer.cast_coordinates_to_str(source_cube_dims, dataframe)

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=source_cube_dims
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata_queryspecific.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs)

    if cast_cell_type_mapping_on_values:
        measure_dim_name = source_cube_dims[-1]
        measure_types = data_metadata_queryspecific.get_measure_element_types()

        transformer.dataframe_cast_value_by_measure_type(
            dataframe=dataframe,
            measure_dimension_name=measure_dim_name,
            measure_element_types=measure_types,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            **kwargs
        )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_data_copy_intercube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
            **kwargs
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine,
        sql_function=sql_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, 
        audit_mode=audit_mode,
        **kwargs)

    final_row_count = len(dataframe)
    basic_logger.debug(f"initial row count was: {initial_row_count}, Final row count was: {final_row_count}")

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    missing_elements_dataframe = None
    if check_missing_elements:
        missing_elements_dataframe = transformer.dataframe_itemskip_elements(
            dataframe=dataframe,
            check_dfs=target_metadata.get_dimension_check_dfs(),
            check_hierarchies=target_metadata.get_dimension_check_hiers(),
            logging_enabled=log_missing_elements,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            fallback_elements=fallback_elements,
            raise_error_if_missing_found=raise_error_if_missing_found,
            query_mode=element_query_mode,
            check_missing_elements_audit=check_missing_elements_audit,
            return_dropped_rows=output_missing_elements,
            **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    if value_function is not None:
        transformer.dataframe_value_scale(
            dataframe=dataframe, value_function=value_function,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
        )

    dataframe = transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=target_cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="end_data_copy_intercube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if aggregate_numeric_duplicates:
        dataframe = transformer.dataframe_aggregate_numeric_values(dataframe, target_cube_dims)

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loader.dataframe_to_cube(
        tm1_service=target_tm1_service,
        dataframe=dataframe,
        cube_name=target_cube_name,
        cube_dims=target_cube_dims,
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
        slice_size_of_dataframe=slice_size_of_dataframe,
        **kwargs
    )

    if clear_source:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=source_cube_name,
                          clear_set_mdx_list=source_clear_set_mdx_list,
                          **kwargs)

    basic_logger.info("Execution ended.")
    if output_missing_elements:
        return missing_elements_dataframe


@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def data_copy(
        tm1_service: Optional[Any],
        target_tm1_service: Optional[Any] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[List[str]] = None,

        check_missing_elements: Optional[bool] = False,
        dimensions_to_check: Optional[list[str]] = None,
        log_missing_elements: Optional[bool] = False,
        output_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
        raise_error_if_missing_found: Optional[bool] = False,
        element_query_mode: Literal['bulk', 'on_demand'] = 'bulk',
        audit_mode: bool = False,
        check_missing_elements_audit: Optional[bool] = False,
        
        case_and_space_insensitive_inputs: Optional[bool] = False,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        async_write: bool = False,
        slice_size_of_dataframe: int = 50000,
        use_ti: bool = False,
        use_blob: bool = False,
        cast_cell_type_mapping_on_values: Optional[bool] = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = False,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> Optional[DataFrame]:
    """
    Copy TM1 data back into the same cube.

    ``data_copy`` shares the same extraction, mapping, validation, and write
    pipeline as :func:`data_copy_intercube`, but is intended for same-cube
    transformations where a separate target cube name is not needed.

    For cross-cube or cross-server workflows, prefer
    :func:`data_copy_intercube`.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")
    validation.validate_choice("element_query_mode", element_query_mode, {"bulk", "on_demand"})

    if not target_tm1_service:
        target_tm1_service = tm1_service

    native_view_correction_enabled = (
            mdx_function == "native_view_extractor" and not case_and_space_insensitive_inputs)

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_measure_types=cast_cell_type_mapping_on_values,
        tm1_service=tm1_service
    )
    cube_name = data_metadata_queryspecific.get_cube_name()
    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service,
        cube_name=cube_name,
        metadata_function=target_metadata_function,
        collect_itemskip_info=check_missing_elements,
        dimension_check_filter=dimensions_to_check,
        itemskip_query_mode=element_query_mode,
        **kwargs
    )

    cube_dims = target_metadata.get_cube_dims()
    source_cube_dims = data_metadata_queryspecific.get_cube_dims()

    dataframe = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        cube_dimensions=source_cube_dims,
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    transformer.cast_coordinates_to_str(source_cube_dims, dataframe)

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_cube_dims()
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe,
        column_value=data_metadata_queryspecific.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
        **kwargs
    )

    if cast_cell_type_mapping_on_values:
        measure_dim_name = source_cube_dims[-1]
        measure_types = data_metadata_queryspecific.get_measure_element_types()
        transformer.dataframe_cast_value_by_measure_type(
            dataframe=dataframe,
            measure_dimension_name=measure_dim_name,
            measure_element_types=measure_types,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            **kwargs
        )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_data_copy",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
            **kwargs
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine,
        sql_function=sql_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, 
        audit_mode=audit_mode,
        **kwargs
    )

    final_row_count = len(dataframe)
    basic_logger.debug(f"initial row count was: {initial_row_count}, Final row count was: {final_row_count}")
    if initial_row_count < final_row_count:
        msg = f"Initial row count: {initial_row_count} does not match Final row count: {final_row_count}"
        basic_logger.error(msg)
        raise ValueError(msg)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    missing_elements_dataframe = None
    if check_missing_elements:
        missing_elements_dataframe = transformer.dataframe_itemskip_elements(
            dataframe=dataframe,
            check_dfs=target_metadata.get_dimension_check_dfs(),
            check_hierarchies=target_metadata.get_dimension_check_hiers(),
            logging_enabled=log_missing_elements,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            fallback_elements=fallback_elements,
            raise_error_if_missing_found=raise_error_if_missing_found,
            query_mode=element_query_mode,
            check_missing_elements_audit=check_missing_elements_audit,
            return_dropped_rows=output_missing_elements,
            **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    dataframe = transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="end_data_copy",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loader.dataframe_to_cube(
        tm1_service=target_tm1_service,
        dataframe=dataframe,
        cube_name=cube_name,
        cube_dims=cube_dims,
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
        slice_size_of_dataframe=slice_size_of_dataframe,
        **kwargs
    )

    basic_logger.info("Execution ended.")
    if output_missing_elements:
        return missing_elements_dataframe


@exception_handling.public_operation()
@utility.log_async_benchmark_metrics
@utility.log_async_exec_metrics
async def async_executor_tm1(
        tm1_service: Any,
        param_set_mdx_list: List[str],
        data_mdx_template: str,
        shared_mapping: Optional[Dict] = None,
        mapping_steps: Optional[List[Dict]] = None,
        data_copy_function: Callable = data_copy,
        target_clear_set_mdx_list: List[str] = None,
        max_workers: int = 8,
        **kwargs):

    """
    Run a TM1-sliced workflow in parallel.

    The executor expands ``param_set_mdx_list`` into element combinations,
    renders ``data_mdx_template`` for each slice, and calls
    ``data_copy_function`` concurrently. It is typically used with
    :func:`data_copy_intercube`, :func:`data_copy`, or
    :func:`load_tm1_cube_to_csv_file`.

    Use this after a single-slice version of the workflow is already working
    and the remaining issue is throughput.
    """

    target_tm1_service = kwargs.get("target_tm1_service", tm1_service)

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
    validation.validate_parallel_mdx_inputs(
        param_names, param_tuples, data_mdx_template, data_copy_function, max_workers
    )
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_cube_name = kwargs.get("target_cube_name")
    dim_identifier = kwargs.get("check_missing_elements", False)

    if data_copy_function is data_copy:
        target_cube_name = utility.get_cube_name_from_mdx(data_mdx_template)
        dim_identifier = False

    target_metadata_provider = None
    if data_copy_function in (data_copy, data_copy_intercube):
        target_metadata_kwargs = kwargs.copy()
        target_metadata_function = target_metadata_kwargs.pop("target_metadata_function", None)
        target_metadata = utility.TM1CubeObjectMetadata.collect(
            tm1_service=target_tm1_service,
            cube_name=target_cube_name,
            metadata_function=target_metadata_function,
            collect_itemskip_info=dim_identifier,
            **target_metadata_kwargs
        )

        def get_target_metadata(**_kwargs):
            return target_metadata

        target_metadata_provider = get_target_metadata

    if mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=tm1_service,
            **kwargs
        )

    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            **kwargs
        )

    def wrapper(
        _tm1_service: Any,
        _data_mdx: str,
        _mapping_steps: Optional[List[Dict]],
        _shared_mapping: Optional[Dict],
        _target_metadata_func: Optional[Callable],
        _execution_id: int,
        _executor_kwargs: Dict
    ):
        try:
            copy_func_kwargs = {
                **_executor_kwargs,
                "tm1_service": _tm1_service,
                "data_mdx": _data_mdx,
                "shared_mapping": _shared_mapping,
                "mapping_steps": _mapping_steps,
                "_execution_id": _execution_id,
                "target_metadata_function": _target_metadata_func,
                "async_write": False,
                "clear_target": False
            }

            data_copy_function(**copy_func_kwargs)

        except Exception as e:
            exception_handling.redact_exception_values(
                e,
                {_data_mdx: f"<query text, {len(_data_mdx)} characters>"},
            )
            basic_logger.error(
                "Async TM1 worker %s failed. Error: %s", _execution_id, e, exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    if target_clear_set_mdx_list:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, current_tuple in enumerate(param_tuples):
            template_kwargs = {
                param_name: current_tuple[j]
                for j, param_name in enumerate(param_names)
            }
            data_mdx = Template(data_mdx_template).substitute(**template_kwargs)

            futures.append(loop.run_in_executor(
                executor, wrapper,
                tm1_service,
                data_mdx,
                mapping_steps, shared_mapping,
                target_metadata_provider,
                i, kwargs
            ))

        results = await asyncio.gather(*futures, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                basic_logger.error(f"Task {i} failed with exception: {result}")
        utility.raise_async_worker_errors(results, "TM1 parallel execution")


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> SQL data copy functions
# ------------------------------------------------------------------------------------------------------------

@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_sql_data_to_tm1_cube(
        target_cube_name: str,
        tm1_service: Optional[Any],
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        sql_query: Optional[str] = None,
        sql_table_name: Optional[str] = None,
        sql_table_columns: Optional[str] = None,
        sql_schema: Optional[str] = None,
        sql_column_mapping: Optional[dict] = None,
        sql_columns_to_drop: Optional[list] = None,
        chunksize: Optional[int] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,

        check_missing_elements: Optional[bool] = False,
        dimensions_to_check: Optional[list[str]] = None,
        log_missing_elements: Optional[bool] = False,
        output_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
        raise_error_if_missing_found: Optional[bool] = False,
        element_query_mode: Literal['bulk', 'on_demand'] = 'bulk',
        audit_mode: bool = False,
        check_missing_elements_audit: Optional[bool] = False,

        cast_cell_type_mapping_on_values: bool = False,
        
        case_and_space_insensitive_inputs: Optional[bool] = False,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
        clear_source: Optional[bool] = False,
        sql_delete_statement: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        async_write: bool = False,
        slice_size_of_dataframe: int = 250000,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = False,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> Optional[DataFrame]:
    """
    Load SQL data into a TM1 cube.

    The wrapper extracts from ``sql_query`` or ``sql_table_name``, normalizes
    the tabular result into Bedrock's cube-write format, applies optional
    mapping and validation steps, and writes the final DataFrame to
    ``target_cube_name``.

    Common options include ``sql_column_mapping``, ``sql_columns_to_drop``,
    ``mapping_steps``, ``check_missing_elements``, ``clear_target``,
    ``clear_source``, ``async_write``, ``use_ti``, and ``use_blob``.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")
    validation.validate_choice("element_query_mode", element_query_mode, {"bulk", "on_demand"})

    dataframe = extractor.sql_to_dataframe(
        sql_function=sql_function,
        engine=sql_engine,
        sql_query=sql_query,
        table_name=sql_table_name,
        table_columns=sql_table_columns,
        schema=sql_schema,
        chunksize=chunksize,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_itemskip_info=check_missing_elements,
        collect_measure_types=cast_cell_type_mapping_on_values,
        dimension_check_filter=dimensions_to_check,
        itemskip_query_mode=element_query_mode,
        **kwargs
    )

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        column_mapping=sql_column_mapping,
        columns_to_drop=sql_columns_to_drop,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    try:
        tm1_service.server.get_server_name()
    except (CookieConflictError, TM1pyRestException):
        try:
            tm1_service.re_connect()
            basic_logger.warning("TM1 service reconnected.")
        except Exception:
            basic_logger.error("Lost TM1 connection after reconnect attempt.")
            raise

    cube_dims = target_metadata.get_cube_dims()

    transformer.cast_coordinates_to_str(cube_dims, dataframe)

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_load_sql_data_to_tm1_cube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine,
        sql_function=sql_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, 
        audit_mode=audit_mode,
        **kwargs
    )
    
    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    missing_elements_dataframe = None
    if check_missing_elements:
        missing_elements_dataframe = transformer.dataframe_itemskip_elements(
            dataframe=dataframe,
            check_dfs=target_metadata.get_dimension_check_dfs(),
            check_hierarchies=target_metadata.get_dimension_check_hiers(),
            logging_enabled=log_missing_elements,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            fallback_elements=fallback_elements,
            raise_error_if_missing_found=raise_error_if_missing_found,
            query_mode=element_query_mode,
            check_missing_elements_audit=check_missing_elements_audit,
            return_dropped_rows=output_missing_elements,
            **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if cast_cell_type_mapping_on_values:
        measure_dim_name = cube_dims[-1]
        measure_types = target_metadata.get_measure_element_types()
        transformer.dataframe_cast_value_by_measure_type(
            dataframe=dataframe,
            measure_dimension_name=measure_dim_name,
            measure_element_types=measure_types,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            **kwargs
        )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    dataframe = transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="end_load_sql_data_to_tm1_cube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if clear_target:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=dataframe,
        cube_name=target_cube_name,
        cube_dims=cube_dims,
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
        slice_size_of_dataframe=slice_size_of_dataframe
    )

    if clear_source:
        loader.clear_table(database_engine_or_connection=sql_engine,
                           table_name=sql_table_name,
                           delete_statement=sql_delete_statement)

    basic_logger.info("Execution ended.")
    if output_missing_elements:
        return missing_elements_dataframe


@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_tm1_cube_to_sql_table(
        tm1_service: Optional[Any],

        target_table_name: str,

        data_mdx: Optional[str] = None,
        chunksize: Optional[int] = None,
        mdx_function: Optional[Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., Any]] = None,

        sql_engine: Optional[Any] = None,
        sql_connection: Optional[Any] = None,
        sql_column_mapping: Optional[dict] = None,
        sql_function: Optional[Union[Callable[..., DataFrame], Literal["sqlalchemy", "pyodbc"]]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        sql_schema: Optional[str] = None,
        if_table_exists: Literal["fail", "replace_data","replace_table", "append"] = "append",

        case_and_space_insensitive_inputs: Optional[bool] = False,

        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,

        clear_target: Optional[bool] = False,
        sql_delete_statement: Optional[str] = None,
        clear_function: Optional[Union[
            Callable[..., Any],
            Literal["sqlalchemy", "pyodbc", "psycopg2", "snowflake"]
        ]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,

        value_function: Optional[Callable[..., Any]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,

        dtype: Optional[dict] = None,
        decimal: Optional[str] = None,

        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> None:
    """
    Export TM1 cube data to a SQL table.

    The wrapper extracts TM1 data with ``data_mdx`` or ``data_mdx_list``,
    applies optional mapping and value transformations, and writes the result
    to ``target_table_name`` through a SQLAlchemy or DB-API based route.

    Common options include ``sql_engine``, ``sql_connection``,
    ``sql_column_mapping``, ``if_table_exists``, ``clear_target``,
    ``clear_source``, ``dtype``, ``mapping_steps``, and ``shared_mapping``.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    sql_engine_or_connection = sql_connection if sql_connection is not None else sql_engine

    native_view_correction_enabled = (
            mdx_function == "native_view_extractor" and not case_and_space_insensitive_inputs)

    dataframe = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
        decimal=decimal,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_table(clear_function=clear_function,
                               database_engine_or_connection=sql_engine_or_connection,
                               table_name=target_table_name,
                               delete_statement=sql_delete_statement)
        return

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service, mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs)

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata.get_cube_dims()
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_load_tm1_cube_to_sql_table",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine_or_connection,
            sql_function=sql_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine_or_connection,
        sql_function=sql_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="end_load_tm1_cube_to_sql_table",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if dataframe.empty:
        basic_logger.warning("Transformed dataframe is empty. Skipping SQL write and source clear.")
        return

    if clear_target:
        loader.clear_table(clear_function=clear_function,
                           database_engine_or_connection=sql_engine_or_connection,
                           table_name=target_table_name,
                           schema_name=sql_schema,
                           delete_statement=sql_delete_statement)

    loader.dataframe_to_sql(
        dataframe=dataframe,
        table_name=target_table_name,
        engine=sql_engine,
        database_engine_or_connection=sql_engine_or_connection,
        sql_function=sql_function,
        schema=sql_schema,
        chunksize=chunksize,
        dtype=dtype,
        if_exists=if_table_exists,
        **kwargs
    )

    try:
        tm1_service.server.get_server_name()
    except (CookieConflictError, TM1pyRestException):
        try:
            tm1_service.re_connect()
            basic_logger.warning("TM1 service reconnected.")
        except Exception:
            basic_logger.error("Lost TM1 connection after reconnect attempt.")
            raise

    if clear_source:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=data_metadata.get_cube_name(),
                          clear_set_mdx_list=source_clear_set_mdx_list,
                          **kwargs)

    basic_logger.info("Execution ended.")


@exception_handling.public_operation()
@utility.log_async_benchmark_metrics
@utility.log_async_exec_metrics
async def async_executor_tm1_to_sql(
        tm1_service: Any,
        target_table_name: str,
        sql_engine: Any,
        param_set_mdx_list: List[str],
        data_mdx_template: str,
        shared_mapping: Optional[Dict] = None,
        mapping_steps: Optional[List[Dict]] = None,
        data_copy_function: Callable = load_tm1_cube_to_sql_table,
        clear_target: Optional[bool] = False,
        sql_delete_statement: Optional[str] = None,
        max_workers: int = 8,
        **kwargs):

    """
    Export TM1 slices to SQL in parallel.

    This executor expands TM1 parameter sets, renders one MDX query per slice,
    and calls ``data_copy_function`` for each worker. By default the worker is
    :func:`load_tm1_cube_to_sql_table`.

    Use it for large sliceable TM1 exports where throughput matters more than
    single-thread simplicity.
    """

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
    validation.validate_parallel_mdx_inputs(
        param_names, param_tuples, data_mdx_template, data_copy_function, max_workers
    )
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_metadata_provider = None
    data_metadata_provider = None

    if clear_target:
        loader.clear_table(database_engine_or_connection=sql_engine,
                           table_name=target_table_name,
                           delete_statement=sql_delete_statement)

    if data_copy_function is load_tm1_cube_to_sql_table:
        source_cube_name = utility.get_cube_name_from_mdx(data_mdx_template)
        if source_cube_name:
            def get_data_metadata(**metadata_kwargs):
                current_mdx = metadata_kwargs.get("mdx")
                return utility.TM1CubeObjectMetadata.collect(
                    tm1_service=tm1_service,
                    mdx=current_mdx,
                    cube_name=source_cube_name if current_mdx is None else None,
                    metadata_function=kwargs.get("data_metadata_function"),
                    collect_itemskip_info=kwargs.get("check_missing_elements", False),
                    **kwargs
                )
            data_metadata_provider = get_data_metadata
        else:
            basic_logger.warning(
                f"Could not determine cube name from MDX, skipping metadata collection.")

    if mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=tm1_service,
            **kwargs
        )

    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            **kwargs
        )

    def wrapper(
        _tm1_service: Any,
        _sql_engine: Any,
        _target_table_name: str,
        _data_mdx: str,
        _mapping_steps: Optional[List[Dict]],
        _shared_mapping: Optional[Dict],
        _data_metadata_func: Optional[Callable],
        _target_metadata_func: Optional[Callable],
        _execution_id: int,
        _executor_kwargs: Dict
    ):
        try:
            copy_func_kwargs = {
                **_executor_kwargs,
                "tm1_service": _tm1_service,
                "sql_engine": _sql_engine,
                "target_table_name": _target_table_name,
                "data_mdx": _data_mdx,
                "shared_mapping": _shared_mapping,
                "mapping_steps": _mapping_steps,
                "clear_target": False,
                "_execution_id": _execution_id,
                "async_write": False
            }

            if _data_metadata_func:
                copy_func_kwargs["data_metadata_function"] = _data_metadata_func
            data_copy_function(**copy_func_kwargs)

        except Exception as e:
            exception_handling.redact_exception_values(
                e,
                {_data_mdx: f"<query text, {len(_data_mdx)} characters>"},
            )
            basic_logger.error(
                "Async TM1-to-SQL worker %s failed. Error: %s", _execution_id, e, exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, current_tuple in enumerate(param_tuples):
            template_kwargs = {
                param_name: current_tuple[j]
                for j, param_name in enumerate(param_names)
            }
            data_mdx = Template(data_mdx_template).substitute(**template_kwargs)

            futures.append(loop.run_in_executor(
                executor, wrapper,
                tm1_service, sql_engine,
                target_table_name, data_mdx,
                mapping_steps, shared_mapping,
                data_metadata_provider, target_metadata_provider,
                i, kwargs
            ))

        results = await asyncio.gather(*futures, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                basic_logger.error(f"Task {i} failed with exception: {result}")
        utility.raise_async_worker_errors(results, "TM1-to-SQL parallel execution")


@exception_handling.public_operation()
@utility.log_async_benchmark_metrics
@utility.log_async_exec_metrics
async def async_executor_sql_to_tm1(
        tm1_service: Any,
        sql_engine: Any,
        sql_query_template: str,
        sql_table_for_count: str,
        target_cube_name: str,
        slice_size: int = 100000,
        shared_mapping: Optional[Dict] = None,
        mapping_steps: Optional[List[Dict]] = None,
        data_copy_function: Callable = load_sql_data_to_tm1_cube,
        target_clear_set_mdx_list: List[str] = None,
        max_workers: int = 8,
        **kwargs):

    """
    Load one large SQL source into TM1 in parallel slices.

    The executor paginates a SQL result set with ``{offset}`` and ``{fetch}``
    placeholders in ``sql_query_template`` and dispatches each slice to
    ``data_copy_function``. By default the worker is
    :func:`load_sql_data_to_tm1_cube`.

    Use a deterministic ``ORDER BY`` in the SQL template so the slices remain
    stable and non-overlapping.
    """

    validation.validate_sql_pagination_inputs(
        sql_query_template, slice_size, data_copy_function, max_workers
    )
    total_records = extractor._get_sql_table_count(sql_engine, sql_table_for_count)
    if total_records == 0:
        basic_logger.warning("Source SQL table has 0 records. Nothing to load.")
        return

    iterations = math.ceil(total_records / slice_size)
    basic_logger.info(
        f"Total records: {total_records}. Slice size: {slice_size}. "
        f"Executing in {iterations} parallel chunks."
    )

    target_tm1_service = kwargs.get("target_tm1_service", tm1_service)
    target_metadata_provider = None

    if data_copy_function is load_sql_data_to_tm1_cube:
        if target_cube_name:
            target_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=target_tm1_service,
                cube_name=target_cube_name,
                metadata_function=kwargs.get("target_metadata_function"),
                collect_itemskip_info=kwargs.get("check_missing_elements", False),
                **kwargs
            )

            def get_target_metadata(**_kwargs):
                return target_metadata

            target_metadata_provider = get_target_metadata
        else:
            basic_logger.warning(
                f"target_cube_name not provided, skipping metadata collection.")

    if mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=tm1_service,
            **kwargs
        )

    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            **kwargs
        )

    def wrapper(
            _tm1_service: Any,
            _sql_engine: Any,
            _sql_query: str,
            _target_cube_name: str,
            _mapping_steps: Optional[List[Dict]],
            _shared_mapping: Optional[Dict],
            _target_metadata_func: Optional[Callable],
            _execution_id: int,
            _executor_kwargs: Dict
    ):
        try:
            copy_func_kwargs = {
                **_executor_kwargs,
                "tm1_service": _tm1_service,
                "sql_engine": _sql_engine,
                "sql_query": _sql_query,
                "target_cube_name": _target_cube_name,
                "mapping_steps": _mapping_steps,
                "shared_mapping": _shared_mapping,
                "_execution_id": _execution_id,
                "async_write": False
            }

            if _target_metadata_func:
                copy_func_kwargs["target_metadata_function"] = _target_metadata_func
            data_copy_function(**copy_func_kwargs)

        except Exception as e:
            exception_handling.redact_exception_values(
                e,
                {_sql_query: f"<query text, {len(_sql_query)} characters>"},
            )
            basic_logger.error(
                "Async SQL-to-TM1 worker %s failed. Error: %s", _execution_id, e, exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    if target_clear_set_mdx_list:
        kwargs["clear_target"] = False
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(iterations):
            offset = i * slice_size
            sql_query = sql_query_template.format(offset=offset, fetch=slice_size)

            futures.append(loop.run_in_executor(
                executor, wrapper,
                tm1_service, sql_engine, sql_query,
                target_cube_name,
                mapping_steps, shared_mapping,
                target_metadata_provider,
                i, kwargs
            ))

        results = await asyncio.gather(*futures, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                basic_logger.error(f"Task {i} failed with exception: {result}")
        utility.raise_async_worker_errors(results, "SQL-to-TM1 parallel execution")


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> CSV data copy functions
# ------------------------------------------------------------------------------------------------------------

@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_csv_data_to_tm1_cube(
        target_cube_name: str,
        source_csv_file_path: str,
        tm1_service: Optional[Any],
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        csv_column_mapping: Optional[dict] = None,
        csv_columns_to_drop: Optional[list] = None,
        delimiter: Optional[str] = None,
        decimal: Optional[str] = None,
        dtype: Optional[dict] = None,
        cast_cell_type_mapping_on_values: Optional[bool] = False,
        nrows: Optional[int] = None,
        chunksize: Optional[int] = None,
        parse_dates: Optional[Union[bool, Sequence[Hashable]]] = None,
        na_values: Optional[Union[
            Hashable,
            Iterable[Hashable],
            Mapping[Hashable, Iterable[Hashable]]
        ]] = None,
        keep_default_na: Optional[bool] = True,
        low_memory: bool = True,
        memory_map: bool = True,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,

        check_missing_elements: Optional[bool] = False,
        dimensions_to_check: Optional[list[str]] = None,
        log_missing_elements: Optional[bool] = False,
        output_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
        raise_error_if_missing_found: Optional[bool] = False,
        element_query_mode: Literal['bulk', 'on_demand'] = 'bulk',
        audit_mode: bool = False,
        check_missing_elements_audit: Optional[bool] = False,
        
        target_clear_set_mdx_list: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        async_write: bool = False,
        use_ti: bool = False,
        increment: bool = False,
        use_blob: bool = False,
        sum_numeric_duplicates: bool = False,
        slice_size_of_dataframe: int = 50000,
        clear_target: Optional[bool] = False,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> Optional[DataFrame]:
    """
    Load CSV data into a TM1 cube.

    The wrapper reads ``source_csv_file_path`` with pandas-compatible parsing
    options, normalizes the resulting table into a TM1-ready DataFrame, applies
    optional mapping and validation steps, and writes the result to
    ``target_cube_name``.

    Common options include ``csv_column_mapping``, ``csv_columns_to_drop``,
    ``delimiter``, ``decimal``, ``dtype``, ``mapping_steps``,
    ``check_missing_elements``, ``clear_target``, ``async_write``, ``use_ti``,
    and ``use_blob``.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")
    validation.validate_choice("element_query_mode", element_query_mode, {"bulk", "on_demand"})

    dataframe = extractor.csv_to_dataframe(
        csv_file_path=source_csv_file_path,
        sep=delimiter,
        decimal=decimal,
        dtype=dtype,
        nrows=nrows,
        chunksize=chunksize,
        parse_dates=parse_dates,
        na_values=na_values,
        keep_default_na=keep_default_na,
        low_memory=low_memory,
        memory_map=memory_map,
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_itemskip_info=check_missing_elements,
        collect_measure_types=cast_cell_type_mapping_on_values,
        dimension_check_filter=dimensions_to_check,
        itemskip_query_mode=element_query_mode,
        **kwargs
    )
    

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        column_mapping=csv_column_mapping,
        columns_to_drop=csv_columns_to_drop,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_load_csv_data_to_tm1_cube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )
    cube_dims = target_metadata.get_cube_dims()

    transformer.cast_coordinates_to_str(cube_dims, dataframe)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
            **kwargs
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, 
        audit_mode=audit_mode, **kwargs
    )

    missing_elements_dataframe = None
    if check_missing_elements:
        missing_elements_dataframe = transformer.dataframe_itemskip_elements(
            dataframe=dataframe,
            check_dfs=target_metadata.get_dimension_check_dfs(),
            check_hierarchies=target_metadata.get_dimension_check_hiers(),
            logging_enabled=log_missing_elements,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            fallback_elements=fallback_elements,
            raise_error_if_missing_found=raise_error_if_missing_found,
            query_mode=element_query_mode,
            check_missing_elements_audit=check_missing_elements_audit,
            return_dropped_rows=output_missing_elements,
            **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    dataframe = transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    if cast_cell_type_mapping_on_values:
        measure_dim_name = cube_dims[-1]
        measure_types = target_metadata.get_measure_element_types()
        transformer.dataframe_cast_value_by_measure_type(
            dataframe=dataframe,
            measure_dimension_name=measure_dim_name,
            measure_element_types=measure_types,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            **kwargs
        )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="end_load_csv_data_to_tm1_cube",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if clear_target:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=dataframe,
        cube_name=target_cube_name,
        cube_dims=cube_dims,
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
        slice_size_of_dataframe=slice_size_of_dataframe,
        **kwargs
    )

    basic_logger.info("Execution ended.")
    if output_missing_elements:
        return missing_elements_dataframe


@exception_handling.public_operation()
@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_tm1_cube_to_csv_file(
        tm1_service: Optional[Any],

        data_mdx: Optional[str] = None,
        mdx_function: Optional[Union[Callable[..., DataFrame], Literal["native_view_extractor"]]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., Any]] = None,

        target_csv_file_name: Optional[str] = None,
        target_csv_output_dir: Optional[str] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        mode: str = "w",
        chunksize: Optional[int] = None,
        float_format: Optional[Union[str, Callable]] = None,
        delimiter: Optional[str] = None,
        decimal: Optional[str] = None,
        na_rep: Optional[str] = "NULL",
        compression: Optional[Union[str, dict]] = None,
        index: Optional[bool] = False,

        case_and_space_insensitive_inputs: Optional[bool] = False,

        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,

        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,

        value_function: Optional[Callable[..., Any]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,

        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> None:
    """
    Export TM1 cube data to a CSV file.

    The wrapper extracts TM1 data with ``data_mdx`` or ``data_mdx_list``,
    applies optional mapping and value transformations, and writes the result
    to a CSV file with configurable formatting.

    Common options include ``target_csv_output_dir``, ``target_csv_file_name``,
    ``delimiter``, ``decimal``, ``float_format``, ``mapping_steps``,
    ``shared_mapping``, and ``clear_source``.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    native_view_correction_enabled = (
            mdx_function == "native_view_extractor" and not case_and_space_insensitive_inputs)

    dataframe = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
        decimal=decimal,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    if dataframe.empty:
        return

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_base_cube_metadata=False,
        tm1_service=tm1_service
    )

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service, mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs)

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_cube_dims()
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    utility.dataframe_verbose_logger(
        dataframe=dataframe,
        step_number="start_load_tm1_cube_to_csv_file",
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
        **kwargs
    )

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            verbose_logging_output_dir=verbose_logging_output_dir,
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        csv_function=csv_function,
        verbose_logging_mode=verbose_logging_mode,
        verbose_logging_output_dir=verbose_logging_output_dir,
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode, verbose_logging_output_dir=verbose_logging_output_dir,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    if target_csv_file_name is None:
        source_cube_name = data_metadata_queryspecific.get_cube_name()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        target_csv_file_name = f"{source_cube_name}_{timestamp}.csv"

    if pre_load_function is not None:
        if pre_load_args is None:
            pre_load_args = []
        if pre_load_kwargs is None:
            pre_load_kwargs = {}

        validation.validate_callable("pre_load_function", pre_load_function)
        dataframe = validation.validate_dataframe_callback_result(
            "pre_load_function",
            pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)
        )

    if dataframe.empty:
        basic_logger.warning("Transformed dataframe is empty. Skipping CSV write and source clear.")
        return

    loader.dataframe_to_csv(
        dataframe=dataframe,
        csv_file_name=target_csv_file_name,
        csv_output_dir=target_csv_output_dir,
        mode=mode,
        chunksize=chunksize,
        float_format=float_format,
        sep=delimiter,
        decimal=decimal,
        na_rep=na_rep,
        compression=compression,
        index=index,
        **kwargs
    )

    if clear_source:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=data_metadata.get_cube_name(),
                          clear_set_mdx_list=source_clear_set_mdx_list,
                          **kwargs)

    basic_logger.info("Execution ended.")


@exception_handling.public_operation()
@utility.log_async_benchmark_metrics
@utility.log_async_exec_metrics
async def async_executor_csv_to_tm1(
        tm1_service: Any,
        target_cube_name: str,
        source_directory: str,
        param_set_mdx_list: List[str],
        data_mdx_template: str,
        shared_mapping: Optional[Dict] = None,
        mapping_steps: Optional[List[Dict]] = None,
        data_copy_function: Callable = load_csv_data_to_tm1_cube,
        target_clear_set_mdx_list: Optional[bool] = False,
        max_workers: int = 8,
        **kwargs):

    """
    Load multiple CSV files into TM1 in parallel.

    The executor pairs discovered CSV files in ``source_directory`` with the
    TM1 parameter combinations generated from ``param_set_mdx_list`` and calls
    ``data_copy_function`` for each pair. By default the worker is
    :func:`load_csv_data_to_tm1_cube`.

    Use it when the file set and the intended TM1 slice set have a deliberate
    one-to-one relationship.
    """

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
    validation.validate_parallel_mdx_inputs(
        param_names, param_tuples, data_mdx_template, data_copy_function, max_workers
    )
    source_path = Path(source_directory)
    if not source_path.is_dir():
        raise ValueError(f"CSV source directory does not exist or is not a directory: {source_directory}")
    source_csv_files = sorted(str(path) for path in source_path.glob("*.csv"))
    if len(source_csv_files) != len(param_tuples):
        raise ValueError(
            "CSV file count must match the number of parameter tuples exactly. "
            f"Found {len(source_csv_files)} CSV file(s) and {len(param_tuples)} parameter tuple(s)."
        )
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_metadata_provider = None
    data_metadata_provider = None

    if data_copy_function is load_csv_data_to_tm1_cube:
        source_cube_name = utility.get_cube_name_from_mdx(data_mdx_template)
        if source_cube_name:
            data_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=tm1_service,
                cube_name=source_cube_name,
                metadata_function=kwargs.get("data_metadata_function"),
                collect_itemskip_info=kwargs.get("check_missing_elements", False),
                **kwargs
            )
            def get_data_metadata(**_kwargs): return data_metadata
            data_metadata_provider = get_data_metadata
        else:
            basic_logger.warning(
                f"Could not determine cube name from MDX, skipping metadata collection.")

    if mapping_steps:
        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=tm1_service,
            **kwargs
        )

    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            **kwargs
        )

    def wrapper(
        _tm1_service: Any,
        _source_csv_file_path: Any,
        _target_cube_name: str,
        _data_mdx: str,
        _mapping_steps: Optional[List[Dict]],
        _shared_mapping: Optional[Dict],
        _data_metadata_func: Optional[Callable],
        _target_metadata_func: Optional[Callable],
        _execution_id: int,
        _executor_kwargs: Dict
    ):
        try:
            copy_func_kwargs = {
                **_executor_kwargs,
                "tm1_service": _tm1_service,
                "source_csv_file_path": _source_csv_file_path,
                "target_cube_name": _target_cube_name,
                "data_mdx": _data_mdx,
                "mapping_steps": _mapping_steps,
                "shared_mapping": _shared_mapping,
                "_execution_id": _execution_id,
                "clear_target": False,
                "async_write": False
            }

            if _data_metadata_func:
                copy_func_kwargs["data_metadata_function"] = _data_metadata_func
            data_copy_function(**copy_func_kwargs)

        except Exception as e:
            exception_handling.redact_exception_values(
                e,
                {_data_mdx: f"<query text, {len(_data_mdx)} characters>"},
            )
            basic_logger.error(
                "Async CSV-to-TM1 worker %s failed. Error: %s", _execution_id, e, exc_info=True)
            return e

    if target_clear_set_mdx_list:
        kwargs["clear_target"] = False
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loop = asyncio.get_event_loop()
    futures = []
    i = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for current_tuple, source_csv_file_path in zip(param_tuples, source_csv_files):
            template_kwargs = {
                param_name: current_tuple[j]
                for j, param_name in enumerate(param_names)
            }
            data_mdx = Template(data_mdx_template).substitute(**template_kwargs)

            futures.append(loop.run_in_executor(
                executor, wrapper,
                tm1_service, source_csv_file_path,
                target_cube_name, data_mdx,
                mapping_steps, shared_mapping,
                data_metadata_provider, target_metadata_provider,
                i, kwargs
            ))
            i += 1

        results = await asyncio.gather(*futures, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                basic_logger.error(f"Task {i} failed with exception: {result}")
        utility.raise_async_worker_errors(results, "CSV-to-TM1 parallel execution")
