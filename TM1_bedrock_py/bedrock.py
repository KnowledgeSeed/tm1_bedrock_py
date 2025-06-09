"""
This file is a collection of upgraded TM1 bedrock functionality, ported to python / pandas with the help of TM1py.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from string import Template
from typing import Callable, List, Dict, Optional, Any
from pandas import DataFrame

from TM1_bedrock_py import utility, transformer, loader, extractor, basic_logger


@utility.log_benchmark_metrics
@utility.log_exec_metrics
def data_copy_intercube(
        tm1_service: Optional[Any],
        target_tm1_service: Optional[Any] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        target_cube_name: Optional[str] = None,
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: bool = False,
        clear_target: Optional[bool] = False,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        async_write: Optional[bool] = False,
        slice_size_of_dataframe: Optional[int] = 50000,
        use_ti: Optional[bool] = False,
        use_blob: Optional[bool] = False,
        increment: Optional[bool] = False,
        sum_numeric_duplicates: Optional[bool] = True,
        logging_level: Optional[str] = "ERROR",
        df_verbose_logging: Optional[bool] = False,
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
    if not target_tm1_service:
        target_tm1_service = tm1_service

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

    data_metadata_queryspecific = utility.TM1CubeObjectMetadata.collect(
        mdx=data_mdx, collect_base_cube_metadata=False)
    source_cube_name = data_metadata_queryspecific.get_cube_name()

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=target_tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_dim_element_identifiers=ignore_missing_elements,
        **kwargs
    )
    target_cube_name = target_metadata.get_cube_name()

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=target_tm1_service,
                              cube_name=target_cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe, column_value=data_metadata_queryspecific.get_filter_dict(), **kwargs)
    # transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe, **kwargs)
    utility.dataframe_verbose_logger(dataframe, "start_data_copy_intercube", df_verbose_logging=df_verbose_logging)

    if ignore_missing_elements:
        transformer.dataframe_itemskip_elements(
            dataframe=dataframe, check_dfs=target_metadata.get_dimension_check_dfs(), **kwargs)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            df_verbose_logging=df_verbose_logging,
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
        df_verbose_logging=df_verbose_logging, **kwargs)
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
        **kwargs
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    transformer.dataframe_reorder_dimensions(
        dataframe=dataframe, cube_dimensions=target_metadata.get_cube_dims(), **kwargs)

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=target_cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)
    utility.dataframe_verbose_logger(dataframe, "end_data_copy_intercube", df_verbose_logging=df_verbose_logging)

    target_cube_dims = target_metadata.get_cube_dims()
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


@utility.log_exec_metrics
def data_copy(
        tm1_service: Optional[Any],
        target_tm1_service: Optional[Any] = None,
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., Any]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: bool = False,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
        async_write: bool = False,
        slice_size_of_dataframe: int = 250000,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        logging_level: str = "ERROR",
        df_verbose_logging: Optional[bool] = False,
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

    Map and join and the relabeling functonality of map and replace is not viable in case of in-cube transformations.
    Using them will raise an error at writing
    """
    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    if not target_tm1_service:
        target_tm1_service = tm1_service

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
        mdx=data_mdx, collect_base_cube_metadata=False)
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
        metadata_function=data_metadata_function,
        collect_dim_element_identifiers=ignore_missing_elements,
        **kwargs)
    cube_dims = data_metadata.get_cube_dims()

    transformer.dataframe_add_column_assign_value(
        dataframe=dataframe,
        column_value=data_metadata_queryspecific.get_filter_dict(),
        **kwargs
    )
    # transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe, **kwargs)
    utility.dataframe_verbose_logger(dataframe, "start_data_copy", df_verbose_logging=df_verbose_logging)

    if ignore_missing_elements:
        transformer.dataframe_itemskip_elements(
            dataframe=dataframe, check_dfs=data_metadata.get_dimension_check_dfs(), **kwargs)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function,
            df_verbose_logging=df_verbose_logging,
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
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df, **kwargs)
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
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    transformer.dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=cube_dims, **kwargs)

    if clear_target:
        loader.clear_cube(tm1_service=target_tm1_service,
                          cube_name=cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)
    utility.dataframe_verbose_logger(dataframe, "end_data_copy", df_verbose_logging=df_verbose_logging)

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
async def async_executor(
        tm1_service: Any,
        param_set_mdx_list: List[str],
        data_mdx_template: str,
        shared_mapping: Optional[Dict] = None,
        mapping_steps: Optional[List[Dict]] = None,
        data_copy_function: Callable = data_copy,
        clear_param_templates: List[str] = None,
        max_workers: int = 8,
        **kwargs):

    target_tm1_service = kwargs.get("target_tm1_service", tm1_service)

    param_names = utility.__get_dimensions_from_set_mdx_list(param_set_mdx_list)
    param_values = utility.__generate_element_lists_from_set_mdx_list(tm1_service, param_set_mdx_list)
    param_tuples = utility.__generate_cartesian_product(param_values)
    basic_logger.info(f"Parameter tuples ready. Count: {len(param_tuples)}")

    target_metadata_provider = None
    data_metadata_provider = None

    if data_copy_function in (data_copy_intercube, load_sql_data_to_tm1_cube):
        target_cube_name = kwargs.get("target_cube_name")
        if target_cube_name:
            target_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=target_tm1_service,
                cube_name=target_cube_name,
                metadata_function=kwargs.get("target_metadata_function"),
                collect_dim_element_identifiers=kwargs.get("ignore_missing_elements", False),
                **kwargs
            )
            def get_target_metadata(**_kwargs): return target_metadata
            target_metadata_provider = get_target_metadata
        else:
            basic_logger.warning(
                 f"target_cube_name not provided, skipping metadata collection.")

    if data_copy_function in (data_copy, load_tm1_cube_to_sql_table):
        source_cube_name = utility._get_cube_name_from_mdx(data_mdx_template)
        if source_cube_name:
            data_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=target_tm1_service,
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
        _data_mdx: str,
        _target_clear_set_mdx_list: List[str],
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
                "data_mdx": _data_mdx,
                "shared_mapping": _shared_mapping,
                "mapping_steps": _mapping_steps,
                "target_clear_set_mdx_list": _target_clear_set_mdx_list,
                "_execution_id": _execution_id,
                "async_write": False
            }

            if _data_metadata_func:
                copy_func_kwargs["data_metadata_function"] = _data_metadata_func
            if _target_metadata_func:
                copy_func_kwargs["target_metadata_function"] = _target_metadata_func
            data_copy_function(**copy_func_kwargs)

        except Exception as e:
            basic_logger.error(
                f"Error during execution {_execution_id} with MDX: {_data_mdx}. Error: {e}", exc_info=True)
            return e

    loop = asyncio.get_event_loop()
    futures = []

    if clear_param_templates is None:
        clear_param_templates = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, current_tuple in enumerate(param_tuples):
            template_kwargs = {
                param_name: current_tuple[j]
                for j, param_name in enumerate(param_names)
            }
            data_mdx = Template(data_mdx_template).substitute(**template_kwargs)

            target_clear_set_mdx_list = [
                Template(clear_param_template).substitute(**template_kwargs)
                for clear_param_template in clear_param_templates
            ]
            futures.append(loop.run_in_executor(
                executor, wrapper,
                tm1_service,
                data_mdx, target_clear_set_mdx_list,
                mapping_steps, shared_mapping,
                data_metadata_provider, target_metadata_provider,
                i, kwargs
            ))

        results = await asyncio.gather(*futures, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                basic_logger.error(f"Task {i} failed with exception: {result}")


@utility.log_exec_metrics
def load_sql_data_to_tm1_cube(
        target_cube_name: str,
        tm1_service: Optional[Any],
        target_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        csv_function: Optional[Callable[..., DataFrame]] = None,
        sql_query: Optional[str] = None,
        sql_table_name: Optional[str] = None,
        sql_table_columns: Optional[str] = None,
        sql_schema: Optional[str] = None,
        sql_column_mapping: Optional[dict] = None,
        sql_value_column_name: Optional[str] = None,
        sql_columns_to_keep: Optional[list] = None,
        drop_other_sql_columns: bool = False,
        chunksize: Optional[int] = None,
        sql_engine: Optional[Any] = None,
        sql_function: Optional[Callable[..., DataFrame]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        ignore_missing_elements: bool = False,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
        clear_source: Optional[bool] = False,
        sql_delete_statement: Optional[List[str]] = None,
        async_write: bool = False,
        slice_size_of_dataframe: int = 250000,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        logging_level: str = "ERROR",
        _execution_id: int = 0,
        **kwargs
) -> None:
    """

    """

    utility.set_logging_level(logging_level=logging_level)
    basic_logger.info("Execution started.")

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        collect_dim_element_identifiers=ignore_missing_elements,
        **kwargs
    )

    dataframe = extractor.sql_to_dataframe(
        sql_function=sql_function,
        engine=sql_engine,
        sql_query=sql_query,
        table_name=sql_table_name,
        table_columns=sql_table_columns,
        schema=sql_schema,
        chunksize=chunksize
    )

    transformer.normalize_table_source_dataframe(
        dataframe=dataframe,
        column_mapping=sql_column_mapping,
        value_column_name=sql_value_column_name,
        columns_to_keep=sql_columns_to_keep,
        drop_other_columns=drop_other_sql_columns,
    )

    cube_name = target_metadata.get_cube_name()

    if dataframe.empty:
        if clear_target:
            loader.clear_cube(tm1_service=tm1_service,
                              cube_name=cube_name,
                              clear_set_mdx_list=target_clear_set_mdx_list,
                              **kwargs)
        return

    cube_dims = target_metadata.get_cube_dims()

    # transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe)

    if ignore_missing_elements:
        transformer.dataframe_itemskip_elements(
            dataframe=dataframe, check_dfs=target_metadata.get_dimension_check_dfs())

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function
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
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df, **kwargs)
    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    transformer.dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=cube_dims)

    if clear_target:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=cube_name,
                          clear_set_mdx_list=target_clear_set_mdx_list,
                          **kwargs)

    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=dataframe,
        cube_name=cube_name,
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
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
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
        sql_delete_statement: Optional[List[str]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        logging_level: str = "ERROR",
        _execution_id: int = 0,
        **kwargs
) -> None:
    """

    """

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

    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=data_metadata.get_filter_dict())
    # transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function,
            sql_engine=sql_engine,
            sql_function=sql_function,
            csv_function=csv_function
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
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df, **kwargs)
    final_row_count = len(dataframe)
    if initial_row_count != final_row_count:
        filtered_count = initial_row_count - final_row_count
        basic_logger.warning(f"Number of rows filtered out through inner joins: {filtered_count}/{initial_row_count}")

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    if clear_target:
        loader.clear_table(engine=sql_engine,
                           table_name=target_table_name,
                           delete_statement=sql_delete_statement)

    loader.dataframe_to_sql(
        dataframe=dataframe,
        table_name=target_table_name,
        engine=sql_engine,
        schema=sql_schema,
        chunsize=chunksize
    )

    if clear_source:
        loader.clear_cube(tm1_service=tm1_service,
                          cube_name=data_metadata.get_cube_name(),
                          clear_set_mdx_list=source_clear_set_mdx_list,
                          **kwargs)

    basic_logger.info("Execution ended.")
