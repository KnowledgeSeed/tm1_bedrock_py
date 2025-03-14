"""
This file is a collection of upgraded TM1 bedrock functionality, ported to python / pandas with the help of TM1py.
"""
from typing import Callable, List, Dict, Optional, Any

from pandas import DataFrame

from TM1_bedrock_py import utility, transformer, loader, extractor, basic_logger


@utility.log_exec_metrics
def data_copy_intercube(
        tm1_service: Optional[Any],
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        target_cube_name: Optional[str] = None,
        target_metadata_function: Optional[Callable[..., DataFrame]] = None,
        data_metadata_function: Optional[Callable[..., DataFrame]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        clear_target: Optional[bool] = False,
        target_clear_set_mdx_list: Optional[List[str]] = None,
        clear_source: Optional[bool] = False,
        source_clear_set_mdx_list: Optional[List[str]] = None,
        async_write: bool = False,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        logging_level: str = "ERROR",
        _execution_id: int = 0,
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
        return

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs
    )

    target_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        cube_name=target_cube_name,
        metadata_function=target_metadata_function,
        **kwargs
    )
    target_cube_name = target_metadata.get_cube_name()

    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=data_metadata.get_filter_dict())
    transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function
    )

    transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df
    )

    transformer.dataframe_redimension_and_transform(
        dataframe=dataframe,
        source_dim_mapping=source_dim_mapping,
        related_dimensions=related_dimensions,
        target_dim_mapping=target_dim_mapping
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    transformer.dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=target_metadata.get_cube_dims())

    if clear_target:
        loader.clear_cube(
            tm1_service=tm1_service,
            cube_name=target_cube_name,
            clear_set_mdx_list=target_clear_set_mdx_list,
            **kwargs
        )

    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=dataframe,
        cube_name=target_cube_name,
        cube_dims=target_metadata.get_cube_dims(),
        async_write=async_write,
        use_ti=use_ti,
        increment=increment,
        use_blob=use_blob,
        sum_numeric_duplicates=sum_numeric_duplicates,
    )

    if clear_source:
        loader.clear_cube(
            tm1_service=tm1_service,
            cube_name=data_metadata.get_cube_name(),
            clear_set_mdx_list=source_clear_set_mdx_list,
            **kwargs
        )

    basic_logger.info("Execution ended.")


@utility.log_exec_metrics
def data_copy(
        tm1_service: Optional[Any],
        data_mdx: Optional[str] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        data_mdx_list: Optional[list[str]] = None,
        skip_zeros: Optional[bool] = False,
        skip_consolidated_cells: Optional[bool] = False,
        skip_rule_derived_cells: Optional[bool] = False,
        data_metadata_function: Optional[Callable[..., DataFrame]] = None,
        mapping_steps: Optional[List[Dict]] = None,
        shared_mapping: Optional[Dict] = None,
        value_function: Optional[Callable[..., Any]] = None,
        clear_set_mdx_list: Optional[List[str]] = None,
        clear_target: Optional[bool] = False,
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
    Copies data within cube in TM1, with optional transformations, mappings, and basic value scale.

    Parameters:
    ----------
    tm1_service : Optional[Any]
        TM1 service instance used to interact with the TM1 server.
    data_mdx : Optional[str]
        MDX query string for retrieving source data. Currently, this can be the only source
    mdx_function : Optional[Callable[..., DataFrame]]
        Function to execute an MDX query and return a DataFrame.
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
        return

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_service,
        mdx=data_mdx,
        metadata_function=data_metadata_function,
        **kwargs
    )
    cube_name = data_metadata.get_cube_name()
    cube_dims = data_metadata.get_cube_dims()

    transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=data_metadata.get_filter_dict())
    transformer.dataframe_force_float64_on_numeric_values(dataframe=dataframe)

    shared_mapping_df = None
    if shared_mapping:
        extractor.generate_dataframe_for_mapping_info(
            mapping_info=shared_mapping,
            tm1_service=tm1_service,
            mdx_function=mdx_function
        )
        shared_mapping_df = shared_mapping["mapping_df"]

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_service,
        mdx_function=mdx_function
    )

    transformer.dataframe_execute_mappings(
        data_df=dataframe, mapping_steps=mapping_steps, shared_mapping_df=shared_mapping_df
    )

    if value_function is not None:
        transformer.dataframe_value_scale(dataframe=dataframe, value_function=value_function)

    transformer.dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=cube_dims)

    if clear_target:
        loader.clear_cube(
            tm1_service=tm1_service,
            cube_name=cube_name,
            clear_set_mdx_list=clear_set_mdx_list,
            **kwargs
        )

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

    basic_logger.info("Execution ended.")
