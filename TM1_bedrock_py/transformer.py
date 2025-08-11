from typing import Callable, List, Dict, Optional, Any

import pandas as pd
import numpy as np
from pandas import DataFrame

from TM1_bedrock_py import utility, basic_logger


def normalize_dataframe(
        dataframe: DataFrame,
        metadata_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> None:
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
    dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=metadata.get_cube_dims())


def dataframe_cast_value_by_measure_type(
        dataframe: DataFrame,
        measure_dimension_name: str,
        measure_element_types: Dict[str, str],
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
    if measure_dimension_name not in dataframe.columns:
        basic_logger.warning(
            f"Measure dimension '{measure_dimension_name}' not in DataFrame. Skipping datatype validation."
        )
        return

    numeric_measures = {elem for elem, dtype in measure_element_types.items() if dtype == 'Numeric'}
    string_measures = {elem for elem, dtype in measure_element_types.items() if dtype == 'String'}

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
            dataframe.loc[numeric_mask, 'Value'], errors='coerce'
        )

        if numeric_values.isnull().any():
            failed_rows = dataframe[numeric_mask & numeric_values.isnull()]
            msg = (f"Failed to convert values to a numeric type for the following rows:"
                   f"\n{failed_rows.to_string()}")
            basic_logger.error(msg)
            raise TypeError(msg)

        dataframe.loc[numeric_mask, 'Value'] = numeric_values.astype(np.float64)

    if string_mask.any():
        dataframe.loc[string_mask, 'Value'] = dataframe.loc[string_mask, 'Value'].astype(str)

    dataframe['Value'] = dataframe['Value'].astype(object)


@utility.log_exec_metrics
def dataframe_reorder_dimensions(
        dataframe: DataFrame,
        cube_dimensions: List[str],
        **_kwargs
) -> None:
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
    temp_reordered = dataframe[cube_dimensions+["Value"]]
    dataframe.drop(columns=dataframe.columns, inplace=True)
    for col in temp_reordered.columns:
        dataframe[col] = temp_reordered[col]


@utility.log_exec_metrics
def dataframe_force_float64_on_numeric_values(dataframe: DataFrame, **_kwargs) -> None:
    """
    Format and then enforce numpy float values in pandas dataframes, if the value is numeric, otherwise keep strings.

    Parameter:
    --------
    dataframe: DataFrame - the input dataframe to mutate
    **kwargs (Any): Additional keyword arguments.

    Returns:
    --------
    None, mutates the dataframe in place
    """
    dataframe["Value"] = dataframe["Value"].apply(utility.force_float64_on_numeric_values)


def dataframe_filter_inplace(
        dataframe: pd.DataFrame,
        filter_condition: Dict[str, Any]
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
        filter_condition: Dict[str, Any]
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
    valid_columns = [col for col in filter_condition.keys() if col in dataframe.columns]

    if not valid_columns:
        return dataframe.iloc[0:0]

    condition = dataframe[valid_columns].eq(
        pd.Series({col: filter_condition[col] for col in valid_columns})
    ).all(axis=1)

    return dataframe.loc[condition].reset_index(drop=True)


def dataframe_drop_column(
        dataframe: DataFrame,
        column_list: list[str]
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
    columns_to_drop = [col for col in column_list if col in dataframe.columns]

    if columns_to_drop:
        dataframe.drop(columns=columns_to_drop, axis=1, inplace=True)
        dataframe.reset_index(drop=True, inplace=True)


@utility.log_exec_metrics
def dataframe_add_column_assign_value(
        dataframe: DataFrame,
        column_value: dict,
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
    new_columns = {col: value for col, value in column_value.items() if col not in dataframe.columns}

    if new_columns:
        dataframe[list(new_columns)] = DataFrame([new_columns], index=dataframe.index)
        dataframe.reset_index(drop=True, inplace=True)


def dataframe_drop_filtered_column(
        dataframe: DataFrame,
        filter_condition: dict
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

    dataframe_filter_inplace(dataframe=dataframe, filter_condition=filter_condition)
    column_list = list(map(str, filter_condition.keys()))
    dataframe_drop_column(dataframe=dataframe, column_list=column_list)


def dataframe_drop_zero_and_values(
        dataframe: DataFrame
) -> None:
    """
    Drops all rows with zero values from DaraFrame, then drops the values column.

    Args:
        dataframe: (DataFrame): The DataFrame to update.
    Return:
        DataFrame: The updated DataFrame without the zero values.
    """

    dataframe.drop(dataframe[dataframe["Value"] == 0].index, inplace=True)
    dataframe.drop(columns=["Value"], inplace=True)
    dataframe.reset_index(drop=True, inplace=True)


def dataframe_relabel(
        dataframe: DataFrame,
        columns: dict
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
    dataframe.rename(columns=columns, inplace=True)


@utility.log_exec_metrics
def dataframe_value_scale(
        dataframe: DataFrame,
        value_function: callable,
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
    dataframe["Value"] = dataframe["Value"].apply(value_function)


def dataframe_redimension_and_transform(
        dataframe: DataFrame,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None,
        **_kwargs
) -> None:

    if source_dim_mapping is not None:
        dataframe_drop_filtered_column(dataframe=dataframe, filter_condition=source_dim_mapping)

    if related_dimensions is not None:
        dataframe_relabel(dataframe=dataframe, columns=related_dimensions)

    if target_dim_mapping is not None:
        dataframe_add_column_assign_value(dataframe=dataframe, column_value=target_dim_mapping)


def normalize_table_source_dataframe(
        dataframe: DataFrame,
        column_mapping: Optional[dict] = None,
        value_column_name: Optional[str] = None,
        columns_to_keep: Optional[list] = None,
        drop_other_columns: bool = False
) -> None:
    if column_mapping is None:
        column_mapping = {}
    if columns_to_keep is None:
        columns_to_keep = []
    if column_mapping:
        dataframe_relabel(dataframe=dataframe, columns=column_mapping)
    if value_column_name:
        dataframe_relabel(dataframe=dataframe, columns={value_column_name: "Value"})
    if "Value" not in dataframe.columns:
        dataframe_add_column_assign_value(dataframe=dataframe, column_value={"Value": 1.0})
    if drop_other_columns:
        columns_to_drop = list(
            set(dataframe.columns) - set(columns_to_keep) - set(column_mapping.values()) - {'Value'}
        )
        dataframe_drop_column(dataframe=dataframe, column_list=columns_to_drop)


@utility.log_exec_metrics
def dataframe_itemskip_elements(dataframe: DataFrame, check_dfs: list[DataFrame], **_kwargs) -> None:
    """
    Filters the given dataframe in place based on valid values from check dataframes.

    Optimized for when check_dfs are always single-column DataFrames.

    Parameters:
    - dataframe: The main DataFrame containing multiple columns + a 'Value' column.
    - check_dfs: A list of single-column check DataFrames,
                 where each one corresponds to a column in df_source (in order).
    **_kwargs (Any): Additional keyword arguments.

    Modifies df_source in place, removing rows that do not match the valid values.
    """
    mask = np.ones(len(dataframe), dtype=bool)
    for df_check in check_dfs:
        col = df_check.columns[0]
        valid_values = df_check[col].to_numpy()
        mask &= np.isin(dataframe[col].to_numpy(), valid_values)
    dataframe.drop(index=dataframe.index[~mask], inplace=True)


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------


def dataframe_find_and_replace(
        dataframe: DataFrame,
        mapping: Dict[str, Dict[Any, Any]]
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
    dataframe.replace({col: mapping[col] for col in mapping.keys() if col in dataframe.columns}, inplace=True)
    return dataframe


def dataframe_map_and_replace(
        data_df: DataFrame,
        mapping_df: DataFrame,
        mapped_dimensions: Dict[str, str]
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

    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - set(mapped_dimensions.keys()) - {"Value"})
    if len(shared_dimensions) == 0:
        shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - {"Value"})
        if len(shared_dimensions) == 0:
            raise ValueError

    original_columns = data_df.columns

    data_df = data_df.merge(
        mapping_df[shared_dimensions + list(mapped_dimensions.values())],
        how='inner',
        on=shared_dimensions,
        suffixes=('', '_mapped')
    )

    columns_to_drop = []
    for data_col, map_col in mapped_dimensions.items():
        map_col = f"{map_col}_mapped" if map_col == data_col or map_col in original_columns else map_col
        data_df[data_col] = data_df[map_col]
        columns_to_drop.append(map_col)

    data_df.drop(columns=columns_to_drop, inplace=True)
    return data_df


def dataframe_map_and_join(
        data_df: DataFrame,
        mapping_df: DataFrame,
        joined_columns: List[str]
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
    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - set(joined_columns) - {"Value"})

    return data_df.merge(
        mapping_df[shared_dimensions + joined_columns],
        how='inner',
        on=shared_dimensions
    )


# ------------------------------------------------------------------------------------------------------------
# Main: mapping executor and its apply functions
# ------------------------------------------------------------------------------------------------------------


def __apply_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df
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
    return dataframe_find_and_replace(dataframe=data_df, mapping=mapping_step["mapping"])


def __apply_map_and_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df: DataFrame
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
            dataframe_filter_inplace(dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"])
        else:
            mapping_df = dataframe_filter(dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"])

    data_df = dataframe_map_and_replace(
        data_df=data_df, mapping_df=mapping_df, mapped_dimensions=mapping_step["mapping_dimensions"]
    )

    if mapping_step.get("relabel_dimensions"):
        dataframe_relabel(dataframe=data_df, columns=mapping_step["mapping_dimensions"])

    return data_df


def __apply_map_and_join(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        shared_mapping_df
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
            dataframe_filter_inplace(dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"])
        else:
            mapping_df = dataframe_filter(dataframe=mapping_df, filter_condition=mapping_step["mapping_filter"])

    data_df = dataframe_map_and_join(data_df=data_df, mapping_df=mapping_df, joined_columns=mapping_step["joined_columns"])

    if "dropped_columns" in mapping_step:
        dataframe_drop_column(dataframe=data_df, column_list=mapping_step["dropped_columns"])

    return data_df


@utility.log_exec_metrics
def dataframe_execute_mappings(
        data_df: DataFrame,
        mapping_steps: List[Dict],
        shared_mapping_df: Optional[DataFrame] = None,
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
    method_handlers = {
        "replace": __apply_replace,
        "map_and_replace": __apply_map_and_replace,
        "map_and_join": __apply_map_and_join,
    }
    for i, step in enumerate(mapping_steps):
        method = step["method"]
        if method in method_handlers:
            data_df = method_handlers[method](data_df, step, shared_mapping_df)
            utility.dataframe_verbose_logger(data_df, f"mapping_step_{i+1}_result", **kwargs)
        else:
            raise ValueError(f"Unsupported mapping method: {method}")

    return data_df
