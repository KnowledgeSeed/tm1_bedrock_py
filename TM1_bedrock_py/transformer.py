from typing import Callable, List, Dict, Optional, Any, Literal

import pandas as pd
import numpy as np
from pandas import DataFrame

from TM1_bedrock_py import utility, basic_logger
from TM1_bedrock_py.utility import create_audit_columns_for_step


def normalize_dataframe_for_testing(
        dataframe: DataFrame, metadata_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> DataFrame:
    """
    Returns a normalized dataframe using the raw output dataframe of the execute mdx function, and necessary cube
    and query based metadata. Makes sure that all cube dimensions are present in the dataframe and that they are in
    the right order.

    Args:
        dataframe (DataFrame): The DataFrame to normalize.
        metadata_function (Optional[Callable]): A function to collect metadata for normalization.
                                                If None, a default function is used.
        **kwargs (Any): Additional keyword arguments for the metadata function.

    Returns:
        None: modifies the dataframe in place
    """

    metadata = utility.TM1CubeObjectMetadata.collect(metadata_function=metadata_function, **kwargs)

    dataframe_add_column_assign_value(dataframe=dataframe, column_value=metadata.get_filter_dict())
    dataframe = dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=metadata.get_cube_dims())
    return dataframe


def cast_coordinates_to_str(cube_dims: list, dataframe: DataFrame):
    """
        Convert all dimension (coordinate) columns in the given DataFrame to string type
        for TM1py compatibility. The 'Value' column, if present, is left unchanged.

        Args:
            cube_dims: List of cube dimension (coordinate) column names.
            dataframe: DataFrame whose dimension columns will be cast to string.

        Returns:
             The same DataFrame instance with dimension columns converted to string type.
    """
    basic_logger.info("Converting dimension columns to string type for consistency.")
    for dim_col in cube_dims:
        if dim_col in dataframe.columns:
            dataframe[dim_col] = dataframe[dim_col].astype(str)


def dataframe_cast_value_by_measure_type(
        dataframe: DataFrame,
        measure_dimension_name: str,
        measure_element_types: Dict[str, str],
        case_and_space_insensitive_inputs: Optional[bool] = False,
        **_kwargs
) -> None:
    """
    Validates and casts the 'Value' column of a DataFrame based on the data type
    of the corresponding element in the measure dimension.

    Args:
        dataframe (DataFrame): The DataFrame to be modified.
        measure_dimension_name (str): The name of the cube's measure dimension.
        measure_element_types (Dict[str, str]): A dictionary mapping measure elements
                                                to their type ('Numeric' or 'String').
        **_kwargs (Any): Additional keyword arguments.

    Raises:
        ValueError: If measures exist in the data that are not defined in the cube.
        TypeError: If a value for a 'Numeric' measure cannot be converted to a number.
    """
    numeric_flags = ['Numeric', 'Consolidated']
    string_flag = 'String'
    value_column_name = 'Value'

    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        measure_dimension_name = utility.normalize_string(measure_dimension_name)
        measure_element_types = utility.normalize_structure_strings(measure_element_types)
        numeric_flags = ['numeric', 'consolidated']
        string_flag = 'string'
        value_column_name = 'value'

    if measure_dimension_name not in dataframe.columns:
        basic_logger.error(
            f"Measure dimension '{measure_dimension_name}' not in DataFrame. Skipping datatype validation."
        )
        return

    numeric_measures = {elem for elem, dtype in measure_element_types.items() if dtype in numeric_flags}
    string_measures = {elem for elem, dtype in measure_element_types.items() if dtype == string_flag}

    all_measures_in_data = set(dataframe[measure_dimension_name].unique())
    known_measures = numeric_measures.union(string_measures)
    unknown_measures = all_measures_in_data - known_measures
    if unknown_measures:
        msg = f"Unknown measures found in data that are not in the cube's measure dimension: {unknown_measures}"
        basic_logger.error(msg)
        raise ValueError(msg)

    numeric_mask = dataframe[measure_dimension_name].isin(numeric_measures)
    string_mask = dataframe[measure_dimension_name].isin(string_measures)

    if numeric_mask.any():
        numeric_values = pd.to_numeric(
            dataframe.loc[numeric_mask, value_column_name].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )

        if numeric_values.isnull().any():
            failed_rows = dataframe[numeric_mask & numeric_values.isnull()]
            msg = (f"Failed to convert values to a numeric type for the following rows:"
                   f"\n{failed_rows.to_string()}")
            basic_logger.error(msg)
            raise TypeError(msg)

        dataframe.loc[numeric_mask, value_column_name] = numeric_values.astype(np.float64)

    if string_mask.any():
        dataframe.loc[string_mask, value_column_name] = dataframe.loc[string_mask, value_column_name].astype(str)

    dataframe[value_column_name] = dataframe[value_column_name].astype(object)


@utility.log_exec_metrics
def dataframe_reorder_dimensions(
        dataframe: DataFrame,
        cube_dimensions: List[str],
        case_and_space_insensitive_inputs: Optional[bool] = False,
        **_kwargs
) -> DataFrame:
    """
    Rearranges the columns of a DataFrame based on the specified cube dimensions.

    The column Value is added to the cube dimension list, since the tm1 loader function expects it to exist at
    the last column index of the dataframe.

    Parameters:
    -----------
    dataframe : DataFrame
        The input Pandas DataFrame to be rearranged.
    cube_dimensions : List[str]
        A list of column names defining the order of dimensions. The "Value"
        column will be appended if it is not already included.
    **kwargs (Any): Additional keyword arguments.

    Returns:
    --------
    None, mutates the dataframe in place

    Raises:
    -------
    KeyError:
        If any column in `cube_dimensions` does not exist in the DataFrame.
    """
    value_column_name = 'Value'
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        cube_dimensions = utility.normalize_structure_strings(cube_dimensions)
        value_column_name = 'value'

    new_order = cube_dimensions + [value_column_name]
    reordered_dataframe = dataframe[new_order]
    return reordered_dataframe


def dataframe_filter_inplace(
        dataframe: pd.DataFrame,
        filter_condition: Dict[str, Any],
        case_and_space_insensitive_inputs: Optional[bool] = False
) -> None:
    """
    Filters a DataFrame in-place based on a given filter_condition.

    - If at least one valid condition is met, it modifies the DataFrame in-place.
    - If no valid condition is met, it clears the DataFrame.

    Args:
        dataframe (pd.DataFrame): The DataFrame to filter.
        filter_condition (Dict[str, Any]): Dictionary with column names as keys and values to filter for.

    Returns:
        None: Modifies the DataFrame in-place.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        filter_condition = utility.normalize_structure_strings(filter_condition)

    valid_columns = [col for col in filter_condition.keys() if col in dataframe.columns]

    if not valid_columns:
        dataframe.drop(dataframe.index, inplace=True)
        return

    condition = dataframe[valid_columns].eq(
        pd.Series({col: filter_condition[col] for col in valid_columns})
    ).all(axis=1)

    dataframe.drop(index=dataframe.index[~condition], inplace=True)
    dataframe.reset_index(drop=True, inplace=True)


def dataframe_filter(
        dataframe: DataFrame,
        filter_condition: Dict[str, Any],
        case_and_space_insensitive_inputs: Optional[bool] = False
) -> DataFrame:
    """
    Filters a DataFrame based on a given filter_condition.

    - If at least one valid condition is met, it returns a filtered DataFrame.
    - If no valid condition is met, it returns an empty DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame to filter.
        filter_condition (Dict[str, Any]): Dictionary with column names as keys and values to filter for.

    Returns:
        DataFrame: The filtered DataFrame.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        filter_condition = utility.normalize_structure_strings(filter_condition)

    valid_columns = [col for col in filter_condition.keys() if col in dataframe.columns]

    if not valid_columns:
        return dataframe.iloc[0:0]

    condition = dataframe[valid_columns].eq(
        pd.Series({col: filter_condition[col] for col in valid_columns})
    ).all(axis=1)

    return dataframe.loc[condition].reset_index(drop=True)


def dataframe_drop_column(
        dataframe: DataFrame,
        column_list: List[str],
        case_and_space_insensitive_inputs: Optional[bool] = False
) -> None:
    """
    Drops columns from DataFrame in-place if the values in the input column_list are found in the DataFrame.
    If a column_list value is not found in the DataFrame, it is ignored.

    Args:
        dataframe (DataFrame): The DataFrame from which columns are to be dropped.
        column_list (list): Name of the columns to be dropped.

    Returns:
        None: The DataFrame is modified in-place.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        column_list = utility.normalize_structure_strings(column_list)

    columns_to_drop = [col for col in column_list if col in dataframe.columns]

    if columns_to_drop:
        dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
        dataframe.reset_index(drop=True, inplace=True)


@utility.log_exec_metrics
def dataframe_add_column_assign_value(
        dataframe: DataFrame,
        column_value: dict,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        **_kwargs
) -> None:
    """
    Ads columns with assigned values to DataFrame if the column_value pairs are not found in the DataFrame.
    If a column from the column_value pair is found in the DataFrame, the pair is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame to which columns are to be added.
        column_value: (dict): Column:value pairs to be added.
        **_kwargs (Any): Additional keyword arguments.

    Returns:
        DataFrame: The updated DataFrame.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        column_value = utility.normalize_structure_strings(column_value)

    new_columns = {col: value for col, value in column_value.items() if col not in dataframe.columns}

    if new_columns:
        dataframe[list(new_columns)] = DataFrame([new_columns], index=dataframe.index)
        dataframe.reset_index(drop=True, inplace=True)


def dataframe_drop_filtered_column(
        dataframe: DataFrame,
        filter_condition: dict,
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> None:
    """
    Filters DataFrame based on filter_condition and drops columns given in column_list.
    Only filters the DataFrame if at least one condition is met. If non is met, it returns an empty DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to filter.
        filter_condition: (dict) Dimension:element key,value pairs for filtering the DataFrame.
    Returns:
        DataFrame: The updated DataFrame.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        filter_condition = utility.normalize_structure_strings(filter_condition)

    dataframe_filter_inplace(dataframe=dataframe, filter_condition=filter_condition)
    column_list = list(map(str, filter_condition.keys()))
    dataframe_drop_column(dataframe=dataframe, column_list=column_list)


def dataframe_relabel(
        dataframe: DataFrame,
        columns: dict,
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> None:
    """
    Relabels DataFrame column(s) if the original label is found in the DataFrame.
    If an original label is not found, then it is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame to relabel.
        columns: (dict): The original and the new column labels as key-value pairs.
                         The key stands for the original column label, the value for the new label.
    Return: None
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        columns = utility.normalize_structure_strings(columns)

    dataframe.rename(columns=columns, inplace=True)


def rename_columns_by_reference(dataframe: DataFrame, column_names: List[str]) -> DataFrame:
    """
    Rename columns in `df` to match the names in `column_names`,
    matching case/whitespace-insensitively, without reordering.
    """
    ref_map = {utility.normalize_string(col): col for col in column_names}

    rename_map = {}
    for col in dataframe.columns:
        norm_col = utility.normalize_string(col)
        if norm_col in ref_map:
            rename_map[col] = ref_map[norm_col]

    return dataframe.rename(columns=rename_map)


@utility.log_exec_metrics
def dataframe_value_scale(
        dataframe: DataFrame,
        value_function: callable,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        **_kwargs
) -> None:
    """
    Applies an input function to the 'Value' column of the DataFrame.

    Args:
        dataframe (DataFrame): The input DataFrame.
        value_function (callable): A function to apply to the 'Value' column.
        **_kwargs (Any): Additional keyword arguments.

    Returns:
        DataFrame: The modified DataFrame (in place).
    """
    value_column_name = 'Value'
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        value_column_name = 'value'

    dataframe[value_column_name] = dataframe[value_column_name].apply(value_function)


def dataframe_redimension_and_transform(
        dataframe: DataFrame,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        **_kwargs
) -> None:
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        source_dim_mapping = utility.normalize_structure_strings(source_dim_mapping)
        related_dimensions = utility.normalize_structure_strings(related_dimensions)
        target_dim_mapping = utility.normalize_structure_strings(target_dim_mapping)

    if source_dim_mapping is not None:
        dataframe_drop_filtered_column(dataframe=dataframe, filter_condition=source_dim_mapping)

    if related_dimensions is not None:
        dataframe_relabel(dataframe=dataframe, columns=related_dimensions)

    if target_dim_mapping is not None:
        dataframe_add_column_assign_value(dataframe=dataframe, column_value=target_dim_mapping)


def normalize_table_source_dataframe(
        dataframe: DataFrame,
        column_mapping: Optional[dict] = None,
        columns_to_drop: Optional[list] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> None:
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        column_mapping = utility.normalize_structure_strings(column_mapping)
        columns_to_drop = utility.normalize_structure_strings(columns_to_drop)

    if column_mapping is None:
        column_mapping = {}
    if column_mapping:
        dataframe_relabel(dataframe=dataframe, columns=column_mapping)
    if columns_to_drop:
        dataframe_drop_column(dataframe=dataframe, column_list=columns_to_drop)


@utility.log_exec_metrics
def dataframe_itemskip_elements(
        dataframe: pd.DataFrame,
        tm1_service: Any = None,
        check_hierarchies: dict[str] = None,
        check_dfs: Optional[Dict[str, pd.DataFrame]] = None,
        fallback_elements: Optional[Dict[str, str]] = None,
        logging_enabled: Optional[bool] = False,
        raise_error_if_missing_found: Optional[bool] = False,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        query_mode: Literal['bulk', 'on_demand'] = 'bulk',
        check_missing_elements_audit: bool = False,
        return_dropped_rows: bool = False,
        **_kwargs: Any
) -> Optional[DataFrame]:
    if query_mode == 'on_demand' and tm1_service is None:
        raise ValueError("TM1Service object is mandatory for on_demand mode.")

    if query_mode == 'bulk' and check_dfs is None:
        raise ValueError("The parameter 'check_dfs' is mandatory for bulk query mode.")

    if query_mode == 'on_demand' and check_hierarchies is None:
        raise ValueError("The parameter 'check_dimensions' is mandatory for on_demand query mode.")

    check_dimensions = check_hierarchies.keys() or check_dfs.keys()
    fallback_elements = fallback_elements or {}

    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        check_dfs = utility.normalize_structure_strings(check_dfs)
        fallback_elements = utility.normalize_structure_strings(fallback_elements)
        check_dimensions = utility.normalize_structure_strings(check_dimensions)

    missing_values_mask = dataframe.isna().any(axis=1).to_numpy()
    nan_dropped_dataframe = dataframe.loc[missing_values_mask].copy() if return_dropped_rows else None

    dataframe.drop(index=dataframe.index[missing_values_mask], inplace=True)
    dataframe.reset_index(drop=True, inplace=True)

    global_validity_mask = np.ones(len(dataframe), dtype=bool)
    exit_with_error = False

    for stray_dimension_name in set(fallback_elements) - set(check_dimensions):
        basic_logger.warning(
            f"Specified dimension name {stray_dimension_name} in fallback elements "
            f"is not present in the list of dimensions to check. "
            f"Checked dimensions: {check_dimensions}"
        )

    for dimension_name in check_dimensions:
        dimension_prefix = f"{dimension_name}@"
        matching_dataframe_columns = [
            column_name for column_name in dataframe.columns
            if
            column_name == dimension_name or (check_missing_elements_audit and column_name.startswith(dimension_prefix))
        ]

        for dataframe_column in matching_dataframe_columns:
            if query_mode == 'bulk':
                validation_dataframe = check_dfs[dimension_name]
                valid_elements_set = set(validation_dataframe[dimension_name])
                current_column_validity_mask = dataframe[dataframe_column].isin(valid_elements_set).to_numpy()
            else:
                unique_element_list = dataframe[dataframe_column].astype(str).unique().tolist()
                element_validity_map = {
                    element_name: tm1_service.elements.exists(
                        tm1_service=tm1_service,
                        dimension_name=dimension_name,
                        hierarchy_name=check_hierarchies[dimension_name],
                        element_name=element_name
                    )
                    for element_name in unique_element_list
                }
                current_column_validity_mask = dataframe[dataframe_column].map(element_validity_map).to_numpy()

            if not current_column_validity_mask.all():
                fallback_value = fallback_elements.get(dimension_name)
                if fallback_value is not None and dataframe_column == dimension_name:
                    if logging_enabled:
                        invalid_records_dataframe = dataframe.loc[~current_column_validity_mask, [dataframe_column]]
                        basic_logger.debug(
                            f"Records of dimension {dimension_name} that will be changed to default '{fallback_value}'")
                        basic_logger.debug(invalid_records_dataframe)

                    dataframe.loc[~current_column_validity_mask, dataframe_column] = fallback_value
                else:
                    if logging_enabled:
                        invalid_records_dataframe = dataframe.loc[~current_column_validity_mask, [dataframe_column]]
                        basic_logger.debug(f"Invalid records for dimension {dimension_name}")
                        basic_logger.debug(invalid_records_dataframe)

                    if raise_error_if_missing_found:
                        exit_with_error = True

                    global_validity_mask &= current_column_validity_mask

    invalid_record_count = np.count_nonzero(~global_validity_mask)
    basic_logger.debug(f"Total invalid records: {invalid_record_count}")

    invalid_elements_dropped_dataframe = dataframe.loc[
        ~global_validity_mask].copy() if return_dropped_rows else None

    if exit_with_error:
        raise ValueError("Invalid records found with raise error mode enabled, exiting...")

    dataframe.drop(index=dataframe.index[~global_validity_mask], inplace=True)
    dataframe.reset_index(drop=True, inplace=True)

    if return_dropped_rows:
        return pd.concat([nan_dropped_dataframe, invalid_elements_dropped_dataframe], ignore_index=True)

    return None


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------


def dataframe_find_and_replace(
        dataframe: DataFrame,
        mapping: Dict[str, Dict[Any, Any]],
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> DataFrame:
    """
    Remaps elements in a DataFrame based on a provided mapping.

    Args:
        dataframe (DataFrame): The DataFrame to remap.
        mapping (Dict[str, Dict[Any, Any]]): A dictionary where keys are column names (dimensions),
                                             and values are dictionaries mapping old elements to new elements.

    Returns:
        DataFrame: The updated DataFrame with elements remapped.
    """
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(dataframe)
        mapping = utility.normalize_structure_strings(mapping)

    dataframe.replace({col: mapping[col] for col in mapping.keys() if col in dataframe.columns}, inplace=True)
    return dataframe


def dataframe_map_and_replace(
        data_df: DataFrame,
        mapping_df: DataFrame,
        mapped_dimensions: Dict[str, str],
        include_mapped_in_join: Optional[bool] = False,
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> DataFrame:
    """
    Map specified dimension columns in 'data_df' using 'mapping_df',
    optimized for memory efficiency by modifying dataframes in-place.

    Parameters
    ----------
    data_df : DataFrame
        The original source dataframe, whose columns we want to preserve except
        where we overwrite certain dimension values.
    mapping_df : DataFrame
        The dataframe containing the mapped values for certain columns.
    mapped_dimensions : dict
        A dictionary that specifies which columns in 'data_df' should be replaced
        by which columns in 'mapping_df'.

    Returns
    -------
    DataFrame
        A dataframe with the same columns (and order) as 'data_df',
        but with specified dimensions mapped from 'mapping_df'.
    """
    value_column_name = 'Value'
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(data_df)
        utility.normalize_dataframe_strings(mapping_df)
        mapped_dimensions = utility.normalize_structure_strings(mapped_dimensions)
        value_column_name = 'value'

    shared_dimensions_set = set(data_df.columns) & set(mapping_df.columns) - {value_column_name}
    if not include_mapped_in_join:
        shared_dimensions_set -= set(mapped_dimensions.keys())
    shared_dimensions = list(shared_dimensions_set)

    if len(shared_dimensions) == 0:
        raise ValueError

    original_columns = data_df.columns

    merged_df = data_df.merge(mapping_df[shared_dimensions + list(mapped_dimensions.values())],
                              how='left',
                              on=shared_dimensions,
                              suffixes=('', '_mapped'))

    columns_to_drop = []
    for data_col, map_col in mapped_dimensions.items():
        map_col = f"{map_col}_mapped" if map_col == data_col or map_col in original_columns else map_col
        merged_df[data_col] = merged_df[map_col]
        columns_to_drop.append(map_col)

    merged_df.drop(columns=columns_to_drop, inplace=True)

    if case_and_space_insensitive_inputs:
        merged_df.normalized = True

    return merged_df


def dataframe_map_and_join(
        data_df: DataFrame,
        mapping_df: DataFrame,
        joined_columns: List[str],
        case_and_space_insensitive_inputs: Optional[bool] = False,
) -> DataFrame:
    """
    Joins specified columns from 'mapping_df' to 'data_df' based on shared dimensions.

    This function identifies the common dimensions between `data_df` and `mapping_df`
    and performs an in-place left-join of specified columns.

    Parameters
    ----------
    data_df : DataFrame
        The DataFrame to mutate in-place.
    mapping_df : DataFrame
        The DataFrame containing columns to join.
    joined_columns : List[str]
        Column names from `mapping_df` to join into `data_df`.

    Returns
    -------
    None
        The original DataFrame (`data_df`) is modified in-place.
    """
    if not set(joined_columns).issubset(mapping_df.columns):
        raise ValueError("Some or all columns were not found in mapping df.")

    value_column_name = 'Value'
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(data_df)
        utility.normalize_dataframe_strings(mapping_df)
        joined_columns = utility.normalize_structure_strings(joined_columns)
        value_column_name = 'value'

    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - {value_column_name})

    merged_df = data_df.merge(mapping_df[shared_dimensions + joined_columns],
                              how='left',
                              on=shared_dimensions)

    if case_and_space_insensitive_inputs:
        merged_df.normalized = True

    return merged_df


def dataframe_cartesian_product(
        data_df: DataFrame,
        mapping_df: DataFrame,
        joined_columns: List[str],
        case_and_space_insensitive_inputs: Optional[bool] = False,
):
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(data_df)
        utility.normalize_dataframe_strings(mapping_df)
        joined_columns = utility.normalize_structure_strings(joined_columns)

    merged_df = data_df.merge(
        mapping_df[joined_columns].drop_duplicates(),
        how="cross"
    )

    if case_and_space_insensitive_inputs:
        merged_df.normalized = True

    return merged_df


# ------------------------------------------------------------------------------------------------------------
# Main: mapping executor and its apply functions
# ------------------------------------------------------------------------------------------------------------


def __apply_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
) -> DataFrame:
    """
    Handle the 'replace' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The DataFrame to apply replacements on.
    mapping_step : Dict[str, Any]
        The dictionary containing information about the current mapping step.
    shared_mapping_df: DataFrame
        pandas dataframe containing shared mapping data. Is ignored here

    Returns
    -------
    DataFrame
        The modified DataFrame after applying the literal remap.
    """
    _ = shared_mapping_df
    mapping = mapping_step["mapping"]
    if audit_mode:
        create_audit_columns_for_step(data_df=data_df,
                                      mapping=mapping_step["mapping"],
                                      step_number=step_number)

    return dataframe_find_and_replace(
        dataframe=data_df, mapping=mapping,
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)


def __apply_map_and_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
) -> DataFrame:
    """
    Handle the 'map_and_replace' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame that will be remapped using the MDX approach.
    mapping_step : Dict[str, Any]
        The dictionary specifying how to map, which may contain 'mapping_filter',
        'mapping_mdx', 'mapping_dimensions', etc.
    shared_mapping_df: DataFrame
        pandas dataframe containing shared mapping data.

    Returns
    -------
    None
        Modifies the dataframe in place
    """
    if audit_mode:
        create_audit_columns_for_step(data_df=data_df,
                                      mapping=mapping_step["mapping_dimensions"],
                                      step_number=step_number)

    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else shared_mapping_df
    )

    if "mapping_filter" in mapping_step:
        if step_uses_independent_mapping:
            dataframe_filter_inplace(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)
        else:
            mapping_df = dataframe_filter(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    data_df = dataframe_map_and_replace(
        data_df=data_df, mapping_df=mapping_df, mapped_dimensions=mapping_step["mapping_dimensions"],
        include_mapped_in_join=mapping_step.get("include_mapped_in_join"),
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    if mapping_step.get("relabel_dimensions"):
        dataframe_relabel(dataframe=data_df, columns=mapping_step["mapping_dimensions"],
                          case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    return data_df


def __apply_map_and_join(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
) -> DataFrame:
    """
    Handle the 'map_and_join' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame that will be remapped using the MDX approach.
    mapping_step : Dict[str, Any]
        The dictionary specifying how to map, which may contain 'mapping_filter',
        'mapping_mdx', 'mapping_dimensions', etc.
    shared_mapping_df: DataFrame
        pandas dataframe containing shared mapping data.

    Returns
    -------
    None
        Modifies the dataframe in place
    """
    if audit_mode and "dropped_columns" in mapping_step:
        create_audit_columns_for_step(data_df=data_df,
                                      mapping=mapping_step["dropped_columns"],
                                      step_number=step_number)

    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else shared_mapping_df
    )

    if "mapping_filter" in mapping_step:
        if step_uses_independent_mapping:
            dataframe_filter_inplace(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)
        else:
            mapping_df = dataframe_filter(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    data_df = dataframe_map_and_join(
        data_df=data_df, mapping_df=mapping_df, joined_columns=mapping_step["joined_columns"],
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    if "dropped_columns" in mapping_step:
        dataframe_drop_column(dataframe=data_df, column_list=mapping_step["dropped_columns"],
                              case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    return data_df


def __apply_cartesian_product(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
) -> DataFrame:
    """
    Handle the 'map_and_join' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame that will be remapped using the MDX approach.
    mapping_step : Dict[str, Any]
        The dictionary specifying how to map, which may contain 'mapping_filter',
        'mapping_mdx', 'mapping_dimensions', etc.
    shared_mapping_df: DataFrame
        pandas dataframe containing shared mapping data.

    Returns
    -------
    None
        Modifies the dataframe in place
    """
    _, _ = audit_mode, step_number

    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else shared_mapping_df
    )

    if "mapping_filter" in mapping_step:
        if step_uses_independent_mapping:
            dataframe_filter_inplace(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)
        else:
            mapping_df = dataframe_filter(
                dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"],
                case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    data_df = dataframe_cartesian_product(
        data_df=data_df, mapping_df=mapping_df, joined_columns=mapping_step["joined_columns"],
        case_and_space_insensitive_inputs=case_and_space_insensitive_inputs)

    return data_df


def __apply_pivot(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
):
    _, _, _ = audit_mode, step_number, shared_mapping_df
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(data_df)
        utility.normalize_structure_strings(mapping_step)

    # make columns from row values
    dataframe_pivoted = data_df.pivot(
        index=mapping_step["index"],
        columns=mapping_step["columns"],
        values=mapping_step["values"]
    )

    if case_and_space_insensitive_inputs:
        dataframe_pivoted.normalized = True

    return dataframe_pivoted


def __apply_unpivot(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
):
    _, _, _ = audit_mode, step_number, shared_mapping_df
    if case_and_space_insensitive_inputs:
        utility.normalize_dataframe_strings(data_df)
        utility.normalize_structure_strings(mapping_step)

    # make row values from columns
    dataframe_unpivot = data_df.melt(
        id_vars=mapping_step["id_vars"],
        var_name=mapping_step["var_name"],
        value_name=mapping_step["value_name"]
    )

    if case_and_space_insensitive_inputs:
        dataframe_unpivot.normalized = True

    return dataframe_unpivot


def __apply_basic_dimension_reshaping(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        step_number: int = 1,
):
    # either or: literal row filter, literal column drop, literal column add with value assign, literal relabel
    # can be used in any combination.
    # tip: for more complex reshaping, call this method in sequence
    _ = shared_mapping_df

    columns_to_save = {}
    if "filter_condition" in mapping_step:
        existing_filter_columns = [col for col in mapping_step["filter_condition"].keys() if col in data_df.columns]
        columns_to_save.update(dict.fromkeys(existing_filter_columns))

    if "columns_to_drop" in mapping_step:
        existing_drop_columns = [col for col in mapping_step["columns_to_drop"].keys() if col in data_df.columns]
        columns_to_save.update(dict.fromkeys(existing_drop_columns))

    if audit_mode:
        create_audit_columns_for_step(data_df=data_df,
                                      mapping=columns_to_save,
                                      step_number=step_number)

    if "filter_condition" in mapping_step:
        dataframe_filter_inplace(data_df, mapping_step["filter_condition"],
                                 case_and_space_insensitive_inputs)

    if "columns_to_drop" in mapping_step:
        dataframe_drop_column(data_df, mapping_step["columns_to_drop"],
                              case_and_space_insensitive_inputs)

    if "new_columns_with_values" in mapping_step:
        dataframe_add_column_assign_value(data_df, mapping_step["new_columns_with_values"],
                                          case_and_space_insensitive_inputs)

    if "column_relabel_map" in mapping_step:
        dataframe_relabel(data_df, mapping_step["column_relabel_map"],
                          case_and_space_insensitive_inputs)

    return data_df


method_handlers = {
    "replace": __apply_replace,
    "map_and_replace": __apply_map_and_replace,
    "map_and_join": __apply_map_and_join,
    "cartesian": __apply_cartesian_product,
    "pivot": __apply_pivot,
    "unpivot": __apply_unpivot,
    "basic_reshaping": __apply_basic_dimension_reshaping
}


@utility.log_exec_metrics
def dataframe_execute_mappings(
        data_df: DataFrame,
        mapping_steps: List[Dict],
        shared_mapping_df: Optional[DataFrame] = None,
        case_and_space_insensitive_inputs: Optional[bool] = False,
        audit_mode: bool = False,
        **kwargs
) -> DataFrame:
    """
    Execute a series of mapping steps on data_df.
    Uses mutation for memory efficiency.
    Mapping filters mutate the step specific mapping dataframes, but don't mutate the shared one.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame to be transformed.
    mapping_steps : List[Dict[str, Any]]
        A list of dicts specifying each mapping step procedure
    shared_mapping_df: Optional[DataFrame]
        A shared DataFrame that may be used by multiple steps.
    **_kwargs (Any): Additional keyword arguments.

    Returns
    -------
    DataFrame
        The transformed DataFrame after all mapping steps have been applied.

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

    if not mapping_steps:
        return data_df

    for i, step in enumerate(mapping_steps):
        step_number = str(i+1)
        method = step["method"]
        if method in method_handlers:
            data_df = method_handlers[method](
                data_df, step, shared_mapping_df,
                case_and_space_insensitive_inputs,
                audit_mode, step_number
            )
            utility.dataframe_verbose_logger(
                dataframe=data_df,
                step_number=f"mapping_step_{step_number}_result",
                **kwargs
            )
        else:
            raise ValueError(f"Unsupported mapping method: {method}")

    return data_df
