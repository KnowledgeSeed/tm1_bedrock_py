"""
This file is a collection of upgraded TM1 bedrock functionality, ported to python / pandas with the help of TM1py.
"""
import asyncio
import glob
import math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from string import Template
from typing import Callable, List, Dict, Optional, Any, Sequence, Hashable, Mapping, Iterable, Literal

from TM1py.Exceptions import TM1pyRestException
from requests.cookies import CookieConflictError
from pandas import DataFrame

from TM1_bedrock_py import utility, transformer, loader, extractor, basic_logger


@utility.log_benchmark_metrics
@utility.log_exec_metrics
def data_copy_intercube(
        target_cube_name: str,
        tm1_service: Optional[Any],
        target_tm1_service: Optional[Any] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[Dict] = None,
        related_dimensions: Optional[Dict] = None,
        target_dim_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
        clear_target: Optional[bool] = False,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        async_write: Optional[bool] = False,
        slice_size_of_dataframe: Optional[int] = 50000,
        use_ti: Optional[bool] = False,
        use_blob: Optional[bool] = False,
        use_mixed_datatypes: Optional[bool] = False,
        increment: Optional[bool] = False,
        sum_numeric_duplicates: Optional[bool] = True,
        logging_level: Optional[str] = "ERROR",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> None:
    """
    Copies data from a source cube to a target cube in TM1, with optional transformations, mappings,
    and basic value scale.

    Parameters:
    ----------
    tm1_service : Optional[Any]
        TM1 service instance used to interact with the TM1 server.
    data_mdx : Optional[str]
        MDX query string for retrieving source data. Currently, this can be the only source
    mdx_function : Optional[Callable[..., DataFrame]]
        Function to execute an MDX query and return a DataFrame.
    sql_engine : Optional[Any]
        A sql connection engine that pandas.read_sql expects, preferably a SQLAlchemy engine.
    sql_function : Optional[Callable[..., DataFrame]]
        Function to execute a SQL query and return a DataFrame.
    data_mdx_list : Optional[list[str]]
        List of MDX queries for retrieving multiple data sets.
    skip_zeros : Optional[bool], default=False
        Whether to skip zero values when retrieving source data.
    skip_consolidated_cells : Optional[bool], default=False
        Whether to skip consolidated cells in the source data.
    skip_rule_derived_cells : Optional[bool], default=False
        Whether to skip rule-derived cells in the source data.
    data_metadata_function : Optional[Callable[..., DataFrame]]
        Function to retrieve metadata about the data source.
    target_cube_name : Optional[str]
        Name of the target cube where the data should be copied. If omitted, it will be set as the source cube name.
    target_metadata_function: Optional[Callable[..., DataFrame]]
            Function to retrieve metadata for the target cube.
    mapping_steps : Optional[List[Dict]]
        Steps for mapping data from source to target.
    shared_mapping: Optional[Dict]
        Information about the shared mapping data that can be used for the mapping steps.
        Will generate a dataframe if any inputs are provided.
        Has the same format as a singular mapping step
    source_dim_mapping : Optional[dict]
        Declaration of the dimensions present in the source dataframe, but not present in the target cube.
        If there are such dimensions, these need to be specified, with for each dimension.
        Rows will be filtered with the specified element, and then the dimension (column) will be dropped.
    related_dimensions : Optional[dict]
        Dictionary defining related dimensions for transformation. Source dimensions will be relabeled to the target
        dimensions. Dimensionality and elements will stay unchanged.
    target_dim_mapping : Optional[dict]
        Declarations of the dimensions present in the target cube, but not in the data dataframe,
        after all mapping steps. If there are such dimensions, these need to be specified, with an element for each
        dimension. Dimensions (columns) will be added to the dataframe, and their values will be set uniformly.
    value_function : Optional[Callable[..., Any]]
        Function for transforming values before writing to the target cube.
    clear_set_mdx_list : Optional[List[str]]
        List of MDX queries to clear specific data areas in the target cube.
    clear_target : Optional[bool], default=False
        Whether to clear target before writing.
    target_clear_set_mdx_list: Optional[List[str]]
        List of MDX queries to clear specific data areas in the target cube.
    clear_source: Optional[bool], default=False
        Whether to clear the source after writing.
    source_clear_set_mdx_list: Optional[List[str]]
        List of MDX queries to clear specific data areas in the source cube.
    async_write : bool, default=False
        Whether to write data asynchronously. Currently, divides the data into 250.000 row chunks.
    use_ti : bool, default=False
        Whether to use TurboIntegrator (TI) for writing data.
    use_blob : bool, default=False
        Whether to use BLOB storage for data transfer.
    increment : bool, default=False
        Whether to increment existing values instead of replacing them in the cube.
    sum_numeric_duplicates : bool, default=True
        Whether to sum duplicate numeric values in the dataframe instead of overwriting them.
    **kwargs
        Additional keyword arguments for customization.

    Returns:
    -------
    None
        This function does not return anything; it writes data directly into TM1

    Notes:
    ------
    - The function first collects metadata for the source data, and target cube.
    - It then retrieves the dataframe with the provided mdx function and returns a dataframe.
    - It adds filter dimensions with elements to the dataframe from the query metadata.
    - It applies transformation and mapping logic.
        Within this step, it also creates the shared and step-specific mapping dataframes using the provided mdx
        function if necessary.
    - It applies the provided value scale function
    - It rearranges the columns to the expected shape of the target cube.
    - If needed, it clears the target cube with the provided set MDX's
    - Finally, it writes the processed data into the target cube in TM1.

    Example of shared mapping:
        {
            "mapping_mdx": "////valid mdx////",
            "mapping_metadata_function": mapping_metadata_function_name
            "mapping_df": mapping_dataframe
        }

    Example of the 'mapping_steps' inside mapping_data::
    -------

        [
            {
                "method": "replace",
                "mapping": {
                    "dim1tochange": {"source": "target"},
                    "dim2tochange": {"source3": "target3", "source4": "target4"}
                }
            },
            {
                "method": "map_and_replace",
                "mapping_mdx": "////valid mdx////",
                "mapping_metadata_function": mapping_metadata_function_name
                "mapping_df": mapping_dataframe
                "mapping_filter": {
                    "dim": "element",
                    "dim2": "element2"
                },
                "mapping_dimensions": {
                    "dimname_to_change_values_of_in_source":"dim_to_change_the_values_with_in_mapping"
                },
                "relabel_dimensions": false
            },
            {
                "method": "map_and_join",
                "mapping_mdx": "////valid mdx////",
                "mapping_metadata_function": mapping_metadata_function
                "mapping_df": mapping_dataframe
                "mapping_filter": {
                    "dim": "element",
                    "dim2": "element2"
                },
                "joined_columns": ["dim1tojoin", "dim2tojoin"],
                "dropped_columns": ["dim1todrop", "dim2todrop"]
            }
        ]
    """

    input_parameters = locals()

    if not target_tm1_service:
        target_tm1_service = tm1_service

    native_view_correction_enabled = (
            mdx_function == "native_view_extractor" and not case_and_space_insensitive_inputs)

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    dataframe = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=data_mdx,
        data_mdx_list=data_mdx_list,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells,
        mdx_function=mdx_function,
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_base_cube_metadata=False,
        collect_source_cube_metadata=native_view_correction_enabled,
        tm1_service=tm1_service
    )
    source_cube_name = data_metadata_queryspecific.get_cube_name()

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_dim_element_identifiers=ignore_missing_elements,
        collect_measure_types=use_mixed_datatypes,
        **kwargs
    )

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_source_cube_dims()
        )

    target_cube_dims = target_metadata.get_cube_dims()

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata_queryspecific.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs)

    if use_mixed_datatypes:
        measure_dim_name = target_cube_dims[-1]
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
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs)

    final_row_count = len(dataframe)
    basic_logger.debug(f"initial row count was: {initial_row_count}, Final row count was: {final_row_count}")
    if initial_row_count < final_row_count:
        msg = f"Initial row count: {initial_row_count} does not match Final row count: {final_row_count}"
        basic_logger.error(msg)
        raise ValueError(msg)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
        **kwargs
    )

    if ignore_missing_elements:
        dimension_check_dfs = target_metadata.get_dimension_check_dfs()

        transformer.dataframe_itemskip_elements(dataframe=dataframe, check_dfs=dimension_check_dfs,
                                                logging_enabled=verbose_logging_mode is not None,
                                                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
                                                fallback_elements=fallback_elements,
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

    transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=target_cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)
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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

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


@utility.log_benchmark_metrics
@utility.log_exec_metrics
def data_copy(
        tm1_service: Optional[Any],
        target_tm1_service: Optional[Any] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
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
        use_mixed_datatypes: Optional[bool] = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        logging_level: str = "ERROR",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> None:
    """
    Copies data within cube in TM1, with optional transformations, mappings, and basic value scale.

    Parameters:
    ----------
    tm1_service : Optional[Any]
        TM1 service instance used to interact with the TM1 server.
    data_mdx : Optional[str]
        MDX query string for retrieving source data. Currently, this can be the only source
    mdx_function : Optional[Callable[..., DataFrame]]
        Function to execute an MDX query and return a DataFrame.
    sql_engine : Optional[Any]
        A sql connection engine that pandas.read_sql expects, preferably a SQLAlchemy engine.
    sql_function : Optional[Callable[..., DataFrame]]
        Function to execute a SQL query and return a DataFrame.
    data_mdx_list : Optional[list[str]]
        List of MDX queries for retrieving multiple data sets.
    skip_zeros : Optional[bool], default=False
        Whether to skip zero values when retrieving source data.
    skip_consolidated_cells : Optional[bool], default=False
        Whether to skip consolidated cells in the source data.
    skip_rule_derived_cells : Optional[bool], default=False
        Whether to skip rule-derived cells in the source data.
    data_metadata_function : Optional[Callable[..., DataFrame]]
        Function to retrieve metadata about the data source.
    mapping_steps : Optional[List[Dict]]
        Steps for mapping data from source to target.
    shared_mapping: Optional[Dict]
        Information about the shared mapping data that can be used for the mapping steps.
        Will generate a dataframe if any inputs are provided.
        Has the same format as a singular mapping step
    shared_mapping_metadata_function : Optional[Callable[..., Any]]
        Function to retrieve metadata for the shared mapping.
    source_dim_mapping : Optional[dict]
        Declaration of the dimensions present in the source dataframe, but not present in the target cube.
        If there are such dimensions, these need to be specified, with for each dimension.
        Rows will be filtered with the specified element, and then the dimension (column) will be dropped.
    related_dimensions : Optional[dict]
        Dictionary defining related dimensions for transformation. Source dimensions will be relabeled to the target
        dimensions. Dimensionality and elements will stay unchanged.
    target_dim_mapping : Optional[dict]
        Declarations of the dimensions present in the target cube, but not in the data dataframe,
        after all mapping steps. If there are such dimensions, these need to be specified, with an element for each
        dimension. Dimensions (columns) will be added to the dataframe, and their values will be set uniformly.
    value_function : Optional[Callable[..., Any]]
        Function for transforming values before writing to the target cube.
    clear_set_mdx_list : Optional[List[str]]
        List of MDX queries to clear specific data areas in the target cube.
    clear_target : Optional[bool], default=False
        Whether to clear target before writing.
    async_write : bool, default=False
        Whether to write data asynchronously. Currently, divides the data into 250.000 row chunks.
    use_ti : bool, default=False
        Whether to use TurboIntegrator (TI) for writing data.
    use_blob : bool, default=False
        Whether to use BLOB storage for data transfer.
    increment : bool, default=False
        Whether to increment existing values instead of replacing them in the cube.
    sum_numeric_duplicates : bool, default=True
        Whether to sum duplicate numeric values in the dataframe instead of overwriting them.
    **kwargs
        Additional keyword arguments for customization.

    Returns:
    -------
    None
        This function does not return anything; it writes data directly into TM1

    Notes:
    ------
    - The function first collects metadata for the source data.
    - It then retrieves the dataframe with the provided mdx function and returns a dataframe.
    - It adds filter dimensions with elements to the dataframe from the query metadata.
    - It applies transformation and mapping logic.
        Within this step, it also creates the shared and step-specific mapping dataframes using the provided mdx
        function if necessary.
    - It applies the provided value scale function
    - It rearranges the columns to the expected shape of the cube.
    - If needed, it clears the cube with the provided set MDX's
    - Finally, it writes the processed data back into the cube in TM1.

    Example of the 'mapping_steps' inside mapping_data::
    -------
        [
            {
                "method": "replace",
                "mapping": {
                    "dim1tochange": {"source": "target"},
                    "dim2tochange": {"source3": "target3", "source4": "target4"}
                }
            },
            {
                "method": "map_and_replace",
                "mapping_mdx": "////valid mdx////",
                "mapping_metadata_function": mapping_metadata_function_name
                "mapping_df": mapping_dataframe
                "mapping_filter": {
                    "dim": "element",
                    "dim2": "element2"
                },
                "mapping_dimensions": {
                    "dimname_to_change_values_of_in_source":"dim_to_change_the_values_with_in_mapping"
                },
                "relabel_dimensions": false
            }
        ]

    Map and join and the relabeling functionality of map and replace is not viable in case of in-cube transformations.
    Using them will raise an error at writing
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    if not target_tm1_service:
        target_tm1_service = tm1_service

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
        **kwargs
    )

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_base_cube_metadata=False,
        collect_source_cube_metadata=native_view_correction_enabled,
        tm1_service=tm1_service
    )
    cube_name = data_metadata_queryspecific.get_cube_name()

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service, cube_name=cube_name,
        metadata_function=target_metadata_function,
        collect_dim_element_identifiers=False,
        collect_measure_types=use_mixed_datatypes,
        **kwargs)
    cube_dims = data_metadata.get_cube_dims()

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_source_cube_dims()
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe,
        column_value=data_metadata_queryspecific.get_filter_dict(),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
        **kwargs
    )

    if use_mixed_datatypes:
        measure_dim_name = data_metadata.get_cube_dims()[-1]
        measure_types = data_metadata.get_measure_element_types()
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
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
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

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)
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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

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
    Executes a target data copy function in parallel for multiple TM1 data slices.

    This function is a powerful and versatile orchestrator for running parallel,
    parameterized data operations that originate from TM1. It works by defining
    a set of TM1 dimension elements (e.g., all months in a year) and then
    creating a separate, parallel worker thread for each element.

    Each worker is assigned a unique data slice and executes the specified
    `data_copy_function` on that slice. This architecture is ideal for
    parallelizing large data copy, transformation, or export tasks.

    This executor is designed to work with several bedrock functions, including:
    - `data_copy`: For parallel, in-cube transformations.
    - `data_copy_intercube`: For parallel data movement between cubes.
    - `load_tm1_cube_to_csv_file`: For exporting multiple TM1 data slices to
      separate CSV files in parallel.

    Args:
        tm1_service: An active TM1Service object. This is used for initial setup
            (like fetching parameter elements) and is passed to each worker.
        param_set_mdx_list: A list of MDX set queries that define the parameters
            for slicing the data. Each query should return a set of elements from
            a single dimension. Example: `["{[Period].[All Months]}"]`.
        data_mdx_template: A string template for the main MDX query. It must
            contain placeholders that match the dimension names from
            `param_set_mdx_list`, prefixed with a '$'.
            Example: `"... WHERE ([Period].[$Period])"`.
        shared_mapping: A dictionary for a shared mapping DataFrame, passed to
            each worker.
        mapping_steps: A list of transformation steps, passed to each worker.
        data_copy_function: The bedrock function to be executed by each worker.
            Built-in options are `data_copy`, `data_copy_intercube`, and
            `load_tm1_cube_to_csv_file`.
        target_clear_set_mdx_list: A list of set MDXs.
        max_workers: The number of parallel worker threads to execute. This is the
            primary performance tuning parameter.
        **kwargs: Additional keyword arguments to be passed down to each call of
            the `data_copy_function`. This is used for function-specific
            parameters like `target_cube_name`, `target_csv_output_dir`,
            `skip_zeros`, etc.

    Raises:
        Exception: Aggregates and logs exceptions from worker threads.

    Notes:
        - Slicing Mechanism: The function first executes the MDX queries in
          `param_set_mdx_list` to get a list of element tuples (e.g.,
          `('202401',), ('202402',)`). It then iterates through these tuples,
          substituting the values into the `data_mdx_template` to create a
          unique MDX query for each worker.
        - Performance Tuning: The optimal `max_workers` (typically 4-12) depends
          on the source TM1 server's CPU capacity for handling concurrent queries.
        - Metadata Caching: The executor intelligently pre-caches metadata for the
          source and target cubes once, before starting the parallel workers.
          This avoids redundant metadata calls and improves performance.
    """

    target_tm1_service = kwargs.get("target_tm1_service", tm1_service)

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_cube_name = kwargs.get("target_cube_name")
    dim_identifier = kwargs.get("ignore_missing_elements", False)

    if data_copy_function is data_copy:
        target_cube_name = utility.get_cube_name_from_mdx(data_mdx_template)
        dim_identifier = False

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service,
        cube_name=target_cube_name,
        metadata_function=kwargs.get("target_metadata_function"),
        collect_dim_element_identifiers=dim_identifier,
        **kwargs
    )
    def get_target_metadata(**_kwargs): return target_metadata
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
            basic_logger.error(
                f"Error during execution {_execution_id} with MDX: {_data_mdx}. Error: {e}", exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    if target_clear_set_mdx_list:
        loader.clear_cube(tm1_service=tm1_service,
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


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> SQL data copy functions
# ------------------------------------------------------------------------------------------------------------

@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_sql_data_to_tm1_cube(
        target_cube_name: str,
        tm1_service: Optional[Any],
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
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
        case_and_space_insensitive_inputs: Optional[bool] = False,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
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
        sum_numeric_duplicates: bool = True,
        logging_level: str = "ERROR",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        _execution_id: int = 0,
        **kwargs
) -> None:
    """
    Extracts data from a SQL database, transforms it, and loads it into a TM1 cube.

    This function orchestrates a complete ETL (Extract, Transform, Load) process
    for moving data from a relational database to TM1. It extracts data using
    either a full SQL query or by reading a database table. It then normalizes
    the source data into a structure suitable for TM1 (one value per row with
    dimension columns).

    After extraction, it can apply a powerful series of optional transformation
    and mapping steps before finally writing the processed data into the target
    TM1 cube with performance-tuning options.

    Args:
        target_cube_name: The name of the destination cube in TM1.
        tm1_service: An active TM1Service object for the target TM1 connection.
        target_metadata_function: A custom function to retrieve metadata about the
            target TM1 cube.
        mdx_function: A custom function to execute MDX queries, used by mapping steps.
        csv_function: A function to read CSV files, used by mapping steps.
        sql_query: A full SQL query string to execute for data extraction. Use this
            or `sql_table_name`, but not both.
        sql_table_name: The name of the SQL table to extract data from.
        sql_table_columns: A list of specific columns to select from the table.
        sql_schema: The schema of the source table in the SQL database (e.g., 'dbo').
        sql_column_mapping: A dictionary to rename columns from the SQL source to
            match the dimension names in the target TM1 cube.
            Example: `{'PRODUCT_CODE': 'Product', 'SALES_AMT': 'Value'}`.
        sql_value_column_name: The name of the column in the SQL source that
            contains the data values to be loaded. This column will be renamed
            to 'Value'.
        sql_columns_to_keep: A list of columns to keep from the SQL source after
            renaming. Used with `drop_other_sql_columns`.
        drop_other_sql_columns: If True, any columns not in `sql_columns_to_keep`
            or not renamed via `sql_column_mapping` will be dropped.
        chunksize: The number of rows to read from the SQL database at a time.
            This is a memory optimization for very large source tables.
        sql_engine: A pre-configured SQLAlchemy Engine object for the source
            database connection.
        sql_function: A custom function to execute the SQL data extraction.
        mapping_steps: A list of dictionaries defining transformation steps to be
            applied to the data after extraction and normalization.
        shared_mapping: A dictionary defining a shared mapping DataFrame that can
            be used by multiple mapping steps.
        source_dim_mapping: A dictionary to filter and then drop columns from the
            DataFrame after SQL extraction.
        related_dimensions: A dictionary to rename columns in the DataFrame.
        target_dim_mapping: A dictionary to add new columns with constant values
            to the DataFrame, representing target dimensions not present in the source.
        value_function: A custom function to apply transformations to the 'Value'
            column.
        ignore_missing_elements: If True, rows with elements that do not exist
            in the target TM1 dimensions will be silently dropped.
        target_clear_set_mdx_list: A list of MDX set expressions defining the
            slice to be cleared in the target TM1 cube before loading.
        clear_target: If True, the target slice in the TM1 cube will be cleared.
        clear_source: If True, the source SQL table will be cleared after a
            successful load.
        sql_delete_statement: A specific SQL statement to execute for clearing
            the source table. If not provided, a `TRUNCATE` command is used.
        async_write: If True, the final write to the TM1 cube will be performed
            asynchronously in smaller chunks for higher performance.
        slice_size_of_dataframe: The number of rows per chunk when `async_write`
            is True.
        use_ti: If True, use TurboIntegrator for the write-back to TM1.
        use_blob: If True, use a high-performance binary transfer for writing
            data to TM1. Recommended.
        increment: If True, values will be added to existing values in the cube
            instead of overwriting them.
        sum_numeric_duplicates: If True, rows with duplicate dimension intersections
            will have their numeric values summed before loading.
        logging_level: The logging verbosity level (e.g., "DEBUG", "INFO").
        _execution_id: An identifier used by async executors for logging.

    Raises:
        ValueError: If the configuration is invalid (e.g., both `sql_query` and
            `sql_table_name` are provided).
        sqlalchemy.exc.DBAPIError: If the source SQL query fails.
        TM1py.Exceptions.TM1pyRestException: If the final write to TM1 fails.

    Notes:
        - Workflow: The function follows a strict sequence:
            1. Extract data from the SQL source.
            2. Normalize the DataFrame (rename columns, identify 'Value' column).
            3. Apply all transformation and mapping steps.
            4. Add or remove dimensions to match the target cube structure.
            5. Clear the target TM1 cube slice (if requested).
            6. Load the final, processed DataFrame into TM1.
        - Performance: For the fastest load into TM1, it is recommended to set
          `async_write=True` and `use_blob=True`.
        - Tested databases: MS SQL, PostgeSQL.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    dataframe = extractor.sql_to_dataframe(
        sql_function=sql_function,
        engine=sql_engine,
        sql_query=sql_query,
        table_name=sql_table_name,
        table_columns=sql_table_columns,
        schema=sql_schema,
        chunksize=chunksize
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
        collect_dim_element_identifiers=ignore_missing_elements,
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
        except Exception as e:
            basic_logger.error(f"Lost TM1 connection. Error {e}", exc_info=True)
            raise e

    cube_dims = target_metadata.get_cube_dims()

    utility.cast_coordinates_to_str(cube_dims, dataframe)

    if ignore_missing_elements:
        transformer.dataframe_itemskip_elements(dataframe=dataframe,
                                                check_dfs=target_metadata.get_dimension_check_dfs(),
                                                logging_enabled=verbose_logging_mode is not None,
                                                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
                                                fallback_elements=fallback_elements,
                                                **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

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
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine,
        sql_function=sql_function,
        csv_function=csv_function
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    if clear_target:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

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
        loader.clear_table(engine=sql_engine,
                           table_name=sql_table_name,
                           delete_statement=sql_delete_statement)

    basic_logger.info("Execution ended.")


@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_tm1_cube_to_sql_table(
        tm1_service: Optional[Any],
        target_table_name: str,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        sql_schema: Optional[str] = None,
        chunksize: Optional[int] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
        data_mdx_list: Optional[list[str]] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        clear_target: Optional[bool] = False,
        sql_delete_statement: Optional[str] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        dtype: Optional[dict] = None,
        decimal: Optional[str] = None,
        logging_level: str = "ERROR",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        _execution_id: int = 0,
        **kwargs
) -> None:
    """
    Extracts data from a TM1 cube, transforms it, and loads it into a SQL table.

    This function orchestrates a complete ETL (Extract, Transform, Load) process.
    It begins by extracting a dataset from a TM1 cube based on an MDX query.
    It then applies a series of optional, configurable transformation and mapping
    steps to the resulting DataFrame. Finally, it loads the processed data into a
    specified SQL database table with robust error handling and performance
    optimizations.

    Key features include advanced data mapping, redimensioning, robust data type
    cleaning, and explicit control over the SQL insertion method for performance
    tuning.

    Args:
        tm1_service: An active TM1Service object for the source TM1 connection.
        target_table_name: The name of the destination table in the SQL database.
        sql_engine: A pre-configured SQLAlchemy Engine object for the target
            database connection.
        sql_function: A function to execute SQL queries, used by mapping steps.
        csv_function: A function to read CSV files, used by mapping steps.
        sql_schema: The schema of the target table in the SQL database (e.g., 'dbo').
        chunksize: The number of rows to write to the SQL table in a single batch.
            This is a memory optimization for very large datasets. For moderate
            datasets using a high-speed insert method like `fast_executemany`,
            setting this to `None` is often fastest.
        data_mdx: The MDX query string to extract the source data from TM1.
        mdx_function: A custom function to execute the MDX query.
        data_mdx_list: A list of MDX queries to be executed and concatenated.
        skip_zeros: If True, cells with zero values will be excluded from the
            TM1 extraction. Recommended for performance.
        skip_consolidated_cells: If True, consolidated cells will be excluded.
            Recommended for data integrity in data movement tasks.
        skip_rule_derived_cells: If True, rule-derived cells will be excluded.
        data_metadata_function: A custom function to retrieve metadata about the source.
        mapping_steps: A list of dictionaries defining the transformation steps
            to be applied to the data.
        shared_mapping: A dictionary defining a shared mapping DataFrame that can
            be used by multiple mapping steps.
        source_dim_mapping: A dictionary to filter and then drop columns from the
            source DataFrame.
        related_dimensions: A dictionary to rename columns in the DataFrame.
        target_dim_mapping: A dictionary to add new columns with constant values
            to the DataFrame.
        value_function: A custom function to apply transformations to the 'Value'
            column.
        clear_target: If True, the target SQL table will be cleared before loading.
        sql_delete_statement: A specific SQL statement to execute for clearing the
            target table (e.g., a `DELETE` with a `WHERE` clause). If not
            provided, a `TRUNCATE` command is used.
        clear_source: If True, the source data in the TM1 cube will be cleared
            after a successful SQL load, effectively making this a "move" operation.
        source_clear_set_mdx_list: A list of MDX set expressions defining the
            slice to be cleared in the source TM1 cube.
        dtype: A dictionary mapping column names to SQLAlchemy data types
            (e.g., `{'Value': types.FLOAT, 'Version': types.VARCHAR(50)}`).
            Providing this is a best practice for ensuring reliability.
        decimal: Optional parameter to set the decimal separator.
            If None, it detects the local decimal separator of the user.
        logging_level: The logging verbosity level (e.g., "DEBUG", "INFO").
        _execution_id: An identifier used by async executors for logging.

    Raises:
        ValueError: If the configuration is invalid (e.g., cannot find the SQL table).
        KeyError: If the DataFrame columns do not match the SQL table columns after
            all transformations.
        sqlalchemy.exc.DBAPIError: If a database-level error occurs during the
            data insertion.

    Notes:
        - Performance: For MS SQL Server, the fastest load is achieved by creating
          the `sql_engine` with `use_fast_executemany=True` and setting
          `sql_insert_method=None`.
        - Data Integrity: The function includes a robust pre-processing step that
          cleans the 'Value' column extracted from TM1. It handles mixed numeric
          and string data and correctly interprets numbers that TM1 may format as
          strings with comma decimal separators due to server locale settings.
        - Session Management: If `clear_source` is True, the function will
          proactively call `tm1_service.re_connect()` before clearing. This
          prevents `CookieConflictError` issues that can arise if the TM1 session
          times out during a long SQL write operation.
        - Column Order: The function automatically inspects the target SQL table
          and reorders the DataFrame columns to match the table's physical order,
          preventing a common class of insertion errors.
        - Tested databases: MS SQL, PostgeSQL.
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
        **kwargs
    )

    if dataframe.empty:
        if clear_target:
            loader.clear_table(engine=sql_engine,
                               table_name=target_table_name,
                               delete_statement=sql_delete_statement)
        return

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service, mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs)

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_base_cube_metadata=False,
        collect_source_cube_metadata=native_view_correction_enabled,
        tm1_service=tm1_service
    )

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_source_cube_dims()
        )

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata_queryspecific.get_filter_dict(),
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
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        sql_engine=sql_engine,
        sql_function=sql_function,
        csv_function=csv_function
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    if clear_target:
        loader.clear_table(engine=sql_engine,
                           table_name=target_table_name,
                           delete_statement=sql_delete_statement)

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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

    loader.dataframe_to_sql(
        dataframe=dataframe,
        table_name=target_table_name,
        engine=sql_engine,
        schema=sql_schema,
        chunksize=chunksize,
        dtype=dtype,
        **kwargs
    )

    try:
        tm1_service.server.get_server_name()
    except (CookieConflictError, TM1pyRestException):
        try:
            tm1_service.re_connect()
            basic_logger.warning("TM1 service reconnected.")
        except Exception as e:
            basic_logger.error(f"Lost TM1 connection. Error {e}", exc_info=True)
            raise e

    if clear_source:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=data_metadata.get_cube_name(),
                          clear_set_mdx_list=source_clear_set_mdx_list,
                          **kwargs)

    basic_logger.info("Execution ended.")


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
    Executes multiple `load_tm1_cube_to_sql_table` operations in parallel.

    This function is a high-performance orchestrator designed to export large,
    sliceable datasets from a TM1 cube to a SQL table. It works by defining a
    set of parameters (e.g., all months in a year) and then creating a
    separate, parallel worker thread for each parameter. Each worker executes a
    unique MDX query to extract its assigned data slice, transforms it, and
    loads it into the target SQL table.

    This architecture is ideal for maximizing throughput by parallelizing the
    data extraction from TM1 and the data insertion into SQL.

    Args:
        tm1_service: An active TM1Service object. This is used for initial setup
            (like fetching parameter elements) and can be used by mapping steps.
        sql_engine: A pre-configured SQLAlchemy Engine object for the target
            database connection.
        target_table_name: The name of the destination table in the SQL database.
        param_set_mdx_list: A list of MDX set queries that define the parameters
            for slicing the data. Each query should return a set of elements from
            a single dimension. Example: `["{[Period].[All Months]}"]`.
        data_mdx_template: A string template for the main MDX query. It must
            contain placeholders that match the dimension names from
            `param_set_mdx_list`, prefixed with a '$'.
            Example: `"... WHERE ([Period].[$Period])"`.
        shared_mapping: A dictionary for a shared mapping DataFrame, passed to
            each `load_tm1_cube_to_sql_table` call.
        mapping_steps: A list of transformation steps, passed to each
            `load_tm1_cube_to_sql_table` call.
        data_copy_function: The function to be executed by each worker. Defaults
            to `load_tm1_cube_to_sql_table`.
        clear_target: If True, the entire target SQL table is cleared once with a
            single TRUNCATE or DELETE operation before any workers start.
        sql_delete_statement: A specific SQL statement to use for clearing the
            target table if `clear_target` is True.
        max_workers: The number of parallel worker threads to execute. This is the
            primary performance tuning parameter.
        **kwargs: Additional keyword arguments to be passed down to each
            `load_tm1_cube_to_sql_table` call. This is used for parameters like
            `sql_dtypes`, `chunksize`, `skip_zeros`, `sql_insert_method`, etc.

    Raises:
        Exception: Aggregates and logs exceptions from worker threads. If a
            worker fails, the overall process will complete, but an error will
        be logged.

    Notes:
        - Slicing Mechanism: The function first executes the MDX queries in
          `param_set_mdx_list` to get a list of element tuples (e.g.,
          `('202401',), ('202402',)`). It then iterates through these tuples,
          substituting the values into the `data_mdx_template` to create a
          unique MDX query for each worker.
        - Performance Tuning:
            - `max_workers`: The most critical parameter. The optimal value
              (typically 4-12) depends on the source TM1 server's CPU capacity.
            - Slicing Strategy: The granularity of `param_set_mdx_list` is key.
              Slicing by a time dimension (e.g., month) often creates well-balanced
              workloads.
            - SQL Insertion: For maximum performance, pass the necessary `kwargs`
              to enable high-speed writing for your target database (e.g., use
              an `sql_engine_factory` that enables `fast_executemany` for MSSQL).
            - Tested databases: MS SQL, PostgeSQL.
    """

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_metadata_provider = None
    data_metadata_provider = None

    if clear_target:
        loader.clear_table(engine=sql_engine,
                           table_name=target_table_name,
                           delete_statement=sql_delete_statement)

    if data_copy_function is load_tm1_cube_to_sql_table:
        source_cube_name = utility.get_cube_name_from_mdx(data_mdx_template)
        if source_cube_name:
            data_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=tm1_service,
                cube_name=source_cube_name,
                metadata_function=kwargs.get("data_metadata_function"),
                collect_dim_element_identifiers=kwargs.get("ignore_missing_elements", False),
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
            basic_logger.error(
                f"Error during execution {_execution_id} with MDX: {_data_mdx}. Error: {e}", exc_info=True)
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
    Asynchronously loads data from a single SQL source to a TM1 cube in parallel slices.

    This function is a high-performance orchestrator designed for loading a single,
    large SQL table or query result into a TM1 cube. It works by "blindly
    paginating" the source data: it first determines the total number of rows,
    then divides the work into slices based on row position.

    Each worker thread is assigned a unique slice (e.g., rows 0-99999,
    100000-199999, etc.) and executes a SQL query using `OFFSET` and `FETCH`
    to retrieve only its assigned data. It then runs the full
    `load_sql_data_to_tm1_cube` process on its slice.

    Args:
        tm1_service: An active TM1Service object for the target TM1 connection.
        sql_engine: A pre-configured SQLAlchemy Engine object for the source
            database connection.
        sql_query_template: A SQL query string that must contain an `ORDER BY`
            clause for deterministic slicing. It must also contain two
            placeholders for the pagination logic: `{offset}` and `{fetch}`.
        target_cube_name: The name of the destination cube in TM1.
        sql_table_for_count: The name of the SQL table to perform a `COUNT(*)` on.
            This is used to calculate the total number of slices needed.
        slice_size: The number of rows each parallel worker will fetch from the
            database. This is a key performance tuning parameter.
        shared_mapping: A dictionary for a shared mapping DataFrame, passed to
            each worker.
        mapping_steps: A list of transformation steps, passed to each worker.
        data_copy_function: The function to be executed by each worker. Defaults
            to `load_sql_data_to_tm1_cube`.
        target_clear_set_mdx_list: A list of MDX set expressions defining the
            slice to be cleared in the target TM1 cube if `clear_target` is True.
        max_workers: The number of parallel worker threads to execute. This is the
            primary performance tuning parameter.
        **kwargs: Additional keyword arguments to be passed down to each
            `load_sql_data_to_tm1_cube` call. This is used for parameters like
            `sql_dtypes`, `async_write`, `sql_column_mapping`, etc.

    Raises:
        ValueError: If the source table has zero rows.
        Exception: Aggregates and logs exceptions from worker threads.

    Notes:
        - Slicing Mechanism: This executor does not use TM1 elements for slicing.
          It parallelizes based on the physical position of rows in the ordered
          SQL result set. It is the correct tool for loading a single large table.
        - `ORDER BY` Requirement: For the `OFFSET`/`FETCH` slicing to be reliable
          and produce a consistent, non-overlapping result, the `sql_query_template`
          **must** include a deterministic `ORDER BY` clause (e.g., `ORDER BY
          PrimaryKey`).
        - Performance Tuning:
            - `max_workers`: The most critical parameter. The optimal value
              (typically 4-16) depends on the source SQL server's capacity for
              handling concurrent queries.
            - `slice_size`: Controls the trade-off between network overhead and
              client-side memory usage. A good starting point is 50,000-250,000.
        - Database Compatibility: The `OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY`
          syntax is standard for MS SQL Server, Oracle, and DB2. Other databases
          like PostgreSQL and MySQL use `LIMIT {fetch} OFFSET {offset}`. The
          template must be adjusted accordingly for those databases.
        - Tested databases: MS SQL, PostgeSQL.
    """

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
                collect_dim_element_identifiers=kwargs.get("ignore_missing_elements", False),
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
            basic_logger.error(
                f"Error during execution {_execution_id} with SQL query: {_sql_query}. Error: {e}", exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    if target_clear_set_mdx_list:
        kwargs["clear_target"] = False
        loader.clear_cube(tm1_service=tm1_service,
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


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> CSV data copy functions
# ------------------------------------------------------------------------------------------------------------

@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_csv_data_to_tm1_cube(
        target_cube_name: str,
        source_csv_file_path: str,
        tm1_service: Optional[Any],
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        csv_column_mapping: Optional[dict] = None,
        csv_columns_to_drop: Optional[list] = None,
        delimiter: Optional[str] = None,
        decimal: Optional[str] = None,
        dtype: Optional[dict] = None,
        use_mixed_datatypes: Optional[bool] = False,
        nrows: Optional[int] = None,
        chunksize: Optional[int] = None,
        parse_dates: Optional[bool | Sequence[Hashable]] = None,
        na_values: Optional[Hashable
                            | Iterable[Hashable]
                            | Mapping[Hashable, Iterable[Hashable]]
                            ] = None,
        keep_default_na: Optional[bool] = True,
        low_memory: bool = True,
        memory_map: bool = True,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: Optional[bool] = False,
        fallback_elements: Optional[Dict] = None,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        async_write: bool = False,
        use_ti: bool = False,
        increment: bool = False,
        use_blob: bool = False,
        sum_numeric_duplicates: bool = True,
        slice_size_of_dataframe: int = 50000,
        clear_target: Optional[bool] = False,
        logging_level: str = "ERROR",
        _execution_id: int = 0,
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        **kwargs
) -> None:
    """
    Extracts data from a CSV file, transforms it, and loads it into a TM1 cube.

    This function orchestrates a complete ETL (Extract, Transform, Load) process
    for moving data from a flat file source to TM1. It offers extensive options
    for parsing the CSV, normalizing the tabular data into a TM1-ready format,
    and applying a powerful series of subsequent transformations before writing
    the data to the target cube.

    The function includes several built-in data integrity and robustness features,
    such as automatic data type validation and consistent handling of dimension
    element names.

    Args:
        target_cube_name: The name of the destination cube in TM1.
        source_csv_file_path: The full path to the source CSV file.
        tm1_service: An active TM1Service object for the target TM1 connection.
        target_metadata_function: A custom function to retrieve metadata about the
            target TM1 cube.
        mdx_function: A custom function to execute MDX queries, used by mapping steps.
        csv_function: A custom function to read CSV files, used by mapping steps.
        csv_column_mapping: A dictionary to rename columns from the CSV source to
            match the dimension names in the target TM1 cube.
                csv_columns_to_drop:
        csv_columns_to_drop: Columns present in the .csv file that should not be loaded to the TM1 cube.
        delimiter: The delimiter to use when parsing the CSV file (e.g., ',', ';').
            Passed directly to `pandas.read_csv`.
        decimal: The character to recognize as a decimal point (e.g., '.', ',').
            Crucial for correctly parsing numbers in different locales. Passed
            directly to `pandas.read_csv`.
        dtype: A dictionary mapping column names to specific data types for
            `pandas.read_csv` to use during parsing.
        use_mixed_datatypes: If True, the function will validate that the 'Value'
            column's data type matches the type (Numeric or String) of the
            corresponding measure element in the TM1 cube. It will also ensure
            all numeric values are cast to `float` to meet TM1 API requirements.
        nrows: The number of rows to read from the CSV file.
        chunksize: The number of rows to read from the CSV file at a time.
        parse_dates: A boolean, list of integers or names, list of lists, or
            dictionary to control date parsing. Passed directly to `pandas.read_csv`.
            Example: `['order_date', 'ship_date']`.
        na_values: A scalar, string, list-like, or dictionary of values to
            interpret as `NaN` (Not a Number) when reading the CSV. Passed
            directly to `pandas.read_csv`. Example: `['', '#N/A', 'NULL']`.
        keep_default_na: If True, default `NaN` values (e.g., '', '#N/A', 'NA')
            are included in the list of strings recognized as `NaN`. If False,
            only the values specified in `na_values` are treated as `NaN`.
            Passed directly to `pandas.read_csv`.
        low_memory: If True (default), the file is processed in chunks internally,
            resulting in lower memory usage while parsing. Passed directly to
            `pandas.read_csv`.
        memory_map: If True and the file path is a local file, maps the file
            object directly into memory, which can improve performance by
            eliminating I/O overhead. Passed directly to `pandas.read_csv`.
        mapping_steps: A list of dictionaries defining transformation steps to be
            applied to the data after extraction and normalization.
        shared_mapping: A dictionary defining a shared mapping DataFrame that can
            be used by multiple mapping steps.
        value_function: A custom function to apply transformations to the 'Value'
            column.
        ignore_missing_elements: If True, rows with elements that do not exist
            in the target TM1 dimensions will be silently dropped.
        target_clear_set_mdx_list: A list of MDX set expressions defining the
            slice to be cleared in the target TM1 cube before loading.
        clear_target: If True, the target slice in the TM1 cube will be cleared.
        async_write: If True, the final write to the TM1 cube will be performed
            asynchronously in smaller chunks for higher performance.
        use_ti : bool, default=False
            Whether to use TurboIntegrator (TI) for writing data.
        increment : bool, default=False
            Whether to increment existing values instead of replacing them in the cube.
        sum_numeric_duplicates : bool, default=True
            Whether to sum duplicate numeric values in the dataframe instead of overwriting them.
        slice_size_of_dataframe: The number of rows per chunk when `async_write`
            is True.
        use_blob: If True, use a high-performance binary transfer for writing
            data to TM1. Recommended.
        logging_level: The logging verbosity level (e.g., "DEBUG", "INFO").
        _execution_id: An identifier used by async executors for logging.
        verbose_logging_mode: If True, intermediate DataFrames will be logged to
            CSV files for debugging.

    Raises:
        ValueError: If the configuration is invalid or a mapping step fails.
        TypeError: If `use_mixed_datatypes` is True and a value for a numeric
            measure cannot be converted to a number.
        TM1py.Exceptions.TM1pyRestException: If the final write to TM1 fails.

    Notes:
        - Data Integrity: The function automatically enforces two best practices:
            1. All columns in the DataFrame that correspond to a TM1 dimension
               are converted to the `string` data type to ensure consistency.
            2. If `use_mixed_datatypes` is True, the 'Value' column is rigorously
               cleaned to match the TM1 cube's measure types (Numeric vs. String),
               ensuring that numbers are loaded as `float` and strings as `str`.
        - Performance: For the fastest load into TM1, it is recommended to set
          `async_write=True` and `use_blob=True`.
    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

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
        collect_dim_element_identifiers=ignore_missing_elements,
        collect_measure_types=use_mixed_datatypes,
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

    utility.cast_coordinates_to_str(cube_dims, dataframe)

    if ignore_missing_elements:
        transformer.dataframe_itemskip_elements(dataframe=dataframe,
                                                check_dfs=target_metadata.get_dimension_check_dfs(),
                                                logging_enabled=verbose_logging_mode is not None,
                                                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
                                                fallback_elements=fallback_elements,
                                                **kwargs)

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            csv_function=csv_function,
            verbose_logging_mode=verbose_logging_mode,
            **kwargs
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        csv_function=csv_function,
        **kwargs
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function,
                                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=cube_dims,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

    if use_mixed_datatypes:
        measure_dim_name = target_metadata.get_cube_dims()[-1]
        measure_types = target_metadata.get_measure_element_types()
        transformer.dataframe_cast_value_by_measure_type(
            dataframe=dataframe,
            measure_dimension_name=measure_dim_name,
            measure_element_types=measure_types,
            case_and_space_insensitive_inputs=case_and_space_insensitive_inputs,
            **kwargs
        )

    if clear_target:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

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


@utility.log_benchmark_metrics
@utility.log_exec_metrics
def load_tm1_cube_to_csv_file(
        tm1_service: Optional[Any],
        target_csv_file_name: Optional[str] = None,
        target_csv_output_dir: Optional[str] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        mode: str = "w",
        chunksize: Optional[int] = None,
        float_format: Optional[str | Callable] = None,
        delimiter: Optional[str] = None,
        decimal: Optional[str] = None,
        na_rep: Optional[str] = "NULL",
        compression: Optional[str | dict] = None,
        index: Optional[bool] = False,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame] | Literal["native_view_extractor"]] = None,
        data_mdx_list: Optional[list[str]] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        pre_load_function: Optional[Callable] = None,
        pre_load_args: Optional[List] = None,
        pre_load_kwargs: Optional[Dict] = None,
        logging_level: str = "ERROR",
        verbose_logging_mode: Optional[Literal["file", "print_console"]] = None,
        verbose_logging_output_dir: Optional[str] = None,
        _execution_id: int = 0,
        **kwargs
) -> None:
    """
    Extracts data from a TM1 cube, transforms it, and writes it to a CSV file.

    This function orchestrates a complete ETL (Extract, Transform, Load) process
    for exporting data from TM1 to a flat file. It begins by extracting a
    dataset from a TM1 cube based on an MDX query. It can then apply a powerful
    series of optional transformation and mapping steps to the resulting
    DataFrame before writing the final, clean data to a CSV file with
    configurable formatting.

    Args:
        tm1_service: An active TM1Service object for the source TM1 connection.
        target_csv_file_name: The name of the output CSV file. If not provided,
            a name will be generated automatically based on the source cube
            name and a timestamp.
        target_csv_output_dir: The directory where the CSV file will be saved.
            Defaults to a local `./dataframe_to_csv` directory.
        csv_function: A custom function to handle the final DataFrame to CSV
            writing logic.
        mode: The file write mode ('w' for write/truncate, 'a' for append).
            Passed to `pandas.DataFrame.to_csv`.
        chunksize: The number of rows to write to the CSV file at a time. This
            is a memory optimization for extremely large DataFrames.
        float_format: A format string for floating-point numbers (e.g., '%.2f').
        delimiter: The field delimiter for the output CSV file (e.g., ',', ';').
        decimal: The character to use for the decimal point in numeric values
            (e.g., '.', ','). The function includes robust pre-processing to
            ensure this is respected.
        na_rep: The string representation for missing (`NaN`) values (e.g., 'NULL', '').
        compression: The compression type for the output file (e.g., 'gzip', 'zip').
        index: If True, the DataFrame's index will be written to the CSV file.
            Defaults to False.
        data_mdx: The MDX query string to extract the source data from TM1.
        mdx_function: A custom function to execute the MDX query.
        data_mdx_list: A list of MDX queries to be executed and concatenated.
        skip_zeros: If True, cells with zero values will be excluded from the
            TM1 extraction. Highly recommended for performance.
        skip_consolidated_cells: If True, consolidated cells will be excluded.
            Recommended for data integrity.
        skip_rule_derived_cells: If True, rule-derived cells will be excluded.
        data_metadata_function: A custom function to retrieve metadata about the source.
        mapping_steps: A list of dictionaries defining transformation steps to be
            applied to the data before writing.
        shared_mapping: A dictionary for a shared mapping DataFrame.
        source_dim_mapping: A dictionary to filter and then drop columns.
        related_dimensions: A dictionary to rename columns.
        target_dim_mapping: A dictionary to add new columns with constant values.
        value_function: A custom function to apply transformations to the 'Value'
            column.
        clear_source: If True, the source data in the TM1 cube will be cleared
            after a successful CSV export, effectively making this a "move" or
            "archive" operation.
        source_clear_set_mdx_list: A list of MDX set expressions defining the
            slice to be cleared in the source TM1 cube if `clear_source` is True.
        logging_level: The logging verbosity level (e.g., "DEBUG", "INFO").
        _execution_id: An identifier used by async executors for logging.

    Raises:
        TM1py.Exceptions.TM1pyRestException: If the MDX query to TM1 fails.
        IOError: If the function cannot write to the specified output file path.
        ValueError: If a mapping step is configured incorrectly.

    Notes:
        - Robust Decimal Handling: This function contains a crucial pre-processing
          step that surgically cleans the data before writing. It correctly
          handles numeric values that the TM1 server may send as strings with
          comma decimal separators (e.g., "123,45") due to its locale settings.
          It identifies only these number-like strings, converts them to true
          numeric types, and leaves legitimate text data (like comments)
          untouched. This ensures the `decimal` parameter is always respected.
        - Workflow: The function follows a strict sequence:
            1. Extract data from TM1 into a DataFrame.
            2. Apply all transformation and mapping steps.
            3. Perform the robust data type cleaning on the 'Value' column.
            4. Write the final, clean DataFrame to the CSV file.
            5. Clear the source data in TM1 (if requested).
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
        **kwargs
    )

    if dataframe.empty:
        return

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx,
        collect_base_cube_metadata=False,
        collect_source_cube_metadata=native_view_correction_enabled,
        tm1_service=tm1_service
    )

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service, mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs)

    if native_view_correction_enabled:
        dataframe = transformer.rename_columns_by_reference(
            dataframe=dataframe,
            column_names=data_metadata_queryspecific.get_source_cube_dims()
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
            verbose_logging_mode=verbose_logging_mode
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function,
        csv_function=csv_function
    )

    initial_row_count = len(dataframe)

    dataframe = transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df,
        verbose_logging_mode=verbose_logging_mode,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs, **kwargs
    )

    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs
    )

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

        dataframe = pre_load_function(dataframe, *pre_load_args, **pre_load_kwargs)

    loader.dataframe_to_csv(
        dataframe=dataframe,
        csv_file_name=target_csv_file_name,
        csv_output_dir=target_csv_output_dir,
        mode=mode,
        chunsize=chunksize,
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
        data_copy_function: Callable = data_copy,
        target_clear_set_mdx_list: Optional[bool] = False,
        max_workers: int = 8,
        **kwargs):

    """
    Executes multiple `load_csv_data_to_tm1_cube` operations in parallel.

    This function is a high-performance orchestrator designed to load multiple
    CSV files from a directory into a TM1 cube concurrently. It uses a unique
    parameterization model where each CSV file found in the `source_directory`
    is paired with a set of TM1 element parameters.

    Each worker thread is assigned one CSV file and its corresponding parameter
    set. The worker then executes the full `load_csv_data_to_tm1_cube` process:
    it reads its assigned file, applies transformations, and loads the data
    into a specific slice of the target cube defined by the parameters.

    Args:
        tm1_service: An active TM1Service object. This is used for initial setup
            (like fetching parameter elements) and can be used by mapping steps.
        target_cube_name: The name of the destination cube in TM1.
        source_directory: The full path to the directory containing the source
            CSV files to be loaded.
        param_set_mdx_list: A list of MDX set queries that define the parameters
            for slicing the target cube. The number of parameter tuples generated
            from this list should correspond to the number of CSV files.
        data_mdx_template: An MDX query template used solely for metadata inference
            by the underlying `load_csv_data_to_tm1_cube` function. It should
            contain placeholders (e.g., `$Period`) that will be populated by the
            parameters for each worker.
        shared_mapping: A dictionary for a shared mapping DataFrame, passed to
            each worker.
        mapping_steps: A list of transformation steps, passed to each worker.
        data_copy_function: The function to be executed by each worker. Defaults
            to `load_csv_data_to_tm1_cube`.
        target_clear_set_mdx_list: A list of set MDXs.
        max_workers: The number of parallel worker threads to execute. This should
            be tuned based on the TM1 server's capacity for concurrent writes.
        **kwargs: Additional keyword arguments to be passed down to each
            `load_csv_data_to_tm1_cube` call. This is used for parameters like
            `delimiter`, `decimal`, `async_write`, etc.

    Raises:
        Exception: Aggregates and logs exceptions from worker threads.

    Notes:
        - Slicing Mechanism: The function first discovers all `.csv` files in the
          `source_directory`. It then generates a list of parameter tuples from
          `param_set_mdx_list`. It then `zip`s these two lists together, pairing
          the first CSV file with the first parameter set, the second with the
          second, and so on. The execution stops at the length of the shorter list.
        - Prerequisite: For this process to be logical, the number of CSV files
          in the directory and their order should intentionally match the number
          and order of the parameter tuples generated by `param_set_mdx_list`.
        - Clearing: Unlike other executors, clearing is handled within each
          worker. The `clear_param_templates` are used to generate a specific
          clear operation for each worker's target slice, which is executed
          just before that worker loads its data.
    """

    param_names = utility.get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.generate_cartesian_product(param_values)
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
                collect_dim_element_identifiers=kwargs.get("ignore_missing_elements", False),
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
            basic_logger.error(
                f"Error during execution {_execution_id} with MDX: {_data_mdx}. Error: {e}", exc_info=True)
            return e

    if target_clear_set_mdx_list:
        kwargs["clear_target"] = False
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loop = asyncio.get_event_loop()
    futures = []
    source_csv_files = glob.glob(f"{source_directory}/*.csv")
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