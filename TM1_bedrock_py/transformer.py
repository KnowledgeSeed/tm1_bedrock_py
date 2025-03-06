from typing import Callable, List, Dict, Optional, Any

import pandas as pd
from pandas import DataFrame

from TM1_bedrock_py import utility


def normalize_dataframe(
        dataframe: DataFrame,
        metadata_function: Optional[Callable[..., Any]] = None,
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
        DataFrame: The normalized DataFrame.
    """

    metadata = utility.TM1CubeObjectMetadata.collect(metadata_function=metadata_function, **kwargs)

    dataframe = dataframe_add_column_assign_value(dataframe=dataframe, column_value=metadata.get_filter_dict())
    return dataframe_reorder_dimensions(dataframe=dataframe, cube_dimensions=metadata.get_cube_dims())


def dataframe_reorder_dimensions(
        dataframe: DataFrame,
        cube_dimensions: List[str]
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

    Returns:
    --------
    DataFrame
        A new DataFrame containing only the specified cube dimensions in order.

    Raises:
    -------
    KeyError:
        If any column in `cube_dimensions` does not exist in the DataFrame.
    """
    if 'Value' not in cube_dimensions:
        cube_dimensions.append('Value')
    return dataframe.loc[:, cube_dimensions]


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe transform utility functions
# ------------------------------------------------------------------------------------------------------------
# naming review needed!!!


def dataframe_filter(
        dataframe: DataFrame,
        filter_condition: Dict[str, Any],
        inplace: bool = False
) -> DataFrame:
    """
    Filters a DataFrame based on a given filter_condition.

    - If at least one valid condition is met, it filters the DataFrame.
    - If no valid condition is met, it returns an empty DataFrame.
    - If 'inplace' is True, the function modifies the original DataFrame and returns it.
    - If 'inplace' is False (default), it returns a new filtered DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame to filter.
        filter_condition (Dict[str, Any]): Dictionary with column names as keys and values to filter for.
        inplace (bool, optional): If True, modifies the original DataFrame in-place. Defaults to False.

    Returns:
        DataFrame: The filtered DataFrame (either new or modified in-place).
    """
    valid_columns = list(filter(lambda col: col in dataframe.columns, filter_condition.keys()))

    # in case of inplace=True, don't return empty data
    if not valid_columns:
        return dataframe if inplace else dataframe.iloc[0:0]

    # build condition boolean string for row dropping
    condition = dataframe[valid_columns].eq(
        pd.Series({col: filter_condition[col] for col in valid_columns})
    ).all(axis=1)

    return (
        dataframe.drop(index=dataframe.index[~condition]).reset_index(drop=True)
        if inplace
        else dataframe.loc[condition]
    )


def dataframe_drop_column(
        dataframe: DataFrame,
        column_list: list[str]
) -> DataFrame:
    """
    Drops columns from DataFrame if the values in the input column_list are found in the columns of the DataFrame.
    If a column_list value is not found in the DataFrame, it is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame from which columns are to be dropped.
        column_list: (list): Name of the columns to be dropped.
    Returns:
        DataFrame: The updated DataFrame.
    """
    if not all(item in dataframe.columns for item in column_list):
        return dataframe
    return dataframe.drop(column_list, axis=1).reset_index(drop=True)


def dataframe_add_column_assign_value(
        dataframe: DataFrame,
        column_value: dict
) -> DataFrame:
    """
    Ads columns with assigned values to DataFrame if the column_value pairs are not found in the DataFrame.
    If a column from the column_value pair is found in the DataFrame, the pair is ignored.

    Args:
        dataframe: (DataFrame): The DataFrame to which columns are to be added.
        column_value: (dict): Column:value pairs to be added.
    Returns:
        DataFrame: The updated DataFrame.
    """
    new_columns = {col: value for col, value in column_value.items() if col not in dataframe.columns}

    if new_columns:
        dataframe[list(new_columns)] = DataFrame([new_columns], index=dataframe.index)

    return dataframe.reset_index(drop=True)


def dataframe_drop_filtered_column(
        dataframe: DataFrame,
        filter_condition: dict
) -> DataFrame:
    """
    Filters DataFrame based on filter_condition and drops columns given in column_list.
    Only filters the DataFrame if at least one condition is met. If non is met, it returns an empty DataFrame.

    Args:
        dataframe: (DataFrame): The DataFrame to filter.
        filter_condition: (dict) Dimension:element key,value pairs for filtering the DataFrame.
    Returns:
        DataFrame: The updated DataFrame.
    """

    filtered_dataframe = dataframe_filter(dataframe=dataframe, filter_condition=filter_condition)
    column_list = list(map(str, filter_condition.keys()))
    return dataframe_drop_column(dataframe=filtered_dataframe, column_list=column_list).reset_index(drop=True)


def dataframe_drop_zero_and_values(
        dataframe: DataFrame
) -> DataFrame:
    """
    Drops all rows with zero values from DaraFrame, then drops the values column.

    Args:
        dataframe: (DataFrame): The DataFrame to update.
    Return:
        DataFrame: The updated DataFrame without the zero values.
    """

    dataframe.drop(dataframe[dataframe["Value"] == 0].index, inplace=True)
    dataframe.drop(columns=["Value"], inplace=True)
    return dataframe.reset_index(drop=True)


def dataframe_relabel(
        dataframe: DataFrame,
        columns: dict
) -> DataFrame:
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
    return dataframe


def dataframe_value_scale(
        dataframe: DataFrame,
        value_function: callable
) -> DataFrame:
    """
    Applies an input function to the 'Value' column of the DataFrame.

    Args:
        dataframe (DataFrame): The input DataFrame.
        value_function (callable): A function to apply to the 'Value' column.

    Returns:
        DataFrame: The modified DataFrame (in place).
    """
    dataframe["Value"] = dataframe["Value"].apply(value_function)
    return dataframe.reset_index(drop=True)


def dataframe_redimension_and_transform(
        dataframe: DataFrame,
        source_dim_mapping: Optional[dict] = None,
        related_dimensions: Optional[dict] = None,
        target_dim_mapping: Optional[dict] = None
) -> DataFrame:

    if source_dim_mapping is not None:
        dataframe = dataframe_drop_filtered_column(dataframe=dataframe, filter_condition=source_dim_mapping)

    if related_dimensions is not None:
        dataframe = dataframe_relabel(dataframe=dataframe, columns=related_dimensions)

    if target_dim_mapping is not None:
        dataframe = dataframe_add_column_assign_value(dataframe=dataframe, column_value=target_dim_mapping)

    return dataframe


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

    # 1) Compute shared dimensions, excluding those being remapped
    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - set(mapped_dimensions.keys()))

    # 2) Perform an in-place left join on shared dimensions
    data_df = data_df.merge(mapping_df, how='left', on=shared_dimensions, suffixes=('', '_mapped'))

    # 3) Overwrite columns in data_df with their mapped versions, avoiding extra copies
    for data_col, map_col in mapped_dimensions.items():
        mapped_col_name = f"{map_col}_mapped" if map_col == data_col else map_col
        data_df[data_col] = data_df[mapped_col_name]
        del data_df[mapped_col_name]

    # 4) Retain only the original columns from data_df
    return data_df[data_df.columns.intersection(set(mapping_df.columns))]


def dataframe_map_and_join(
        data_df: DataFrame,
        mapping_df: DataFrame,
        joined_columns: List[str]
) -> DataFrame:
    """
    Joins specified columns from 'mapping_df' to 'data_df' based on shared dimensions.

    This function identifies the common dimensions between `data_df` and `mapping_df`
    and performs an in-place left join, adding only the columns specified in `joined_columns`
    from `mapping_df` to `data_df`.

    Parameters
    ----------
    data_df : DataFrame
        The primary DataFrame to which additional columns will be joined.
    mapping_df : DataFrame
        The DataFrame containing additional data to be mapped.
    joined_columns : List[str]
        A list of column names that should be brought from `mapping_df` to `data_df`.

    Returns
    -------
    DataFrame
        The modified `data_df` with `joined_columns` added from `mapping_df`
        based on shared dimensions.

    Raises
    ------
    KeyError:
        If any column in `joined_columns` is not found in `mapping_df`.
    """
    # Validate that all joined columns exist in mapping_df
    missing_cols = [col for col in joined_columns if col not in mapping_df.columns]
    if missing_cols:
        raise KeyError(f"Columns {missing_cols} not found in mapping_df.")

    # Identify shared dimensions (excluding the explicitly joined columns)
    shared_dimensions = list(set(data_df.columns) & set(mapping_df.columns) - set(joined_columns))

    # Perform an in-place left join
    data_df = data_df.merge(mapping_df[shared_dimensions + joined_columns],
                            how='left', on=shared_dimensions)

    return data_df


def __apply_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        mapping_data: Dict[str, Any]
) -> DataFrame:
    """
    Handle the 'replace' mapping step.

    Parameters
    ----------
    data_df : DataFrame
        The DataFrame to apply replacements on.
    mapping_step : Dict[str, Any]
        The dictionary containing information about the current mapping step.
    mapping_data : Dict[str, Any]
        The overall mapping data dictionary which can include shared or step-specific resources.

    Returns
    -------
    DataFrame
        The modified DataFrame after applying the literal remap.
    """
    _ = mapping_data
    return dataframe_find_and_replace(
        dataframe=data_df,
        mapping=mapping_step["mapping"]
    )


def __apply_map_and_replace(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        mapping_data: Dict[str, Any]
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
    mapping_data : Dict[str, Any]
        The overall mapping data dictionary. Used to fetch a shared DataFrame if
        a step-specific one is not provided.

    Returns
    -------
    DataFrame
        The modified DataFrame after applying the MDX-based remap.
    """
    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else mapping_data["shared_mapping_df"]
    )

    if "mapping_filter" in mapping_step:
        mapping_df = dataframe_filter(
            dataframe=mapping_df,
            filter_condition=mapping_step["mapping_filter"],
            inplace=step_uses_independent_mapping
        )

    data_df = dataframe_map_and_replace(
        data_df=data_df,
        mapping_df=mapping_df,
        mapped_dimensions=mapping_step["mapping_dimensions"]
    )

    if mapping_step.get("relabel_dimensions"):
        data_df = dataframe_relabel(
            dataframe=data_df,
            columns=mapping_step["mapping_dimensions"]
        )

    return data_df


def __apply_map_and_join(
        data_df: DataFrame,
        mapping_step: Dict[str, Any],
        mapping_data: Dict[str, Any]
) -> DataFrame:
    step_uses_independent_mapping = (
        "mapping_df" in mapping_step and mapping_step["mapping_df"] is not None
    )

    mapping_df = (
        mapping_step["mapping_df"]
        if step_uses_independent_mapping
        else mapping_data["shared_mapping_df"]
    )

    if "mapping_filter" in mapping_step:
        mapping_df = dataframe_filter(
            dataframe=mapping_df,
            filter_condition=mapping_step["mapping_filter"],
            inplace=step_uses_independent_mapping
        )

    data_df = dataframe_map_and_join(
        data_df=data_df,
        mapping_df=mapping_df,
        joined_columns=mapping_step["joined_columns"]
    )

    if "dropped_columns" in mapping_step:
        data_df = dataframe_drop_column(dataframe=data_df, column_list=mapping_step["dropped_columns"])

    return data_df


def dataframe_execute_mappings(
        data_df: DataFrame,
        mapping_data: Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]
) -> DataFrame:
    """
    Execute a series of mapping steps on data_df based on the instructions in
    mapping_data. Uses mutation for memory efficiency.
    Mapping filters mutate the step specific mapping dataframes, but don't mutate the shared one.

    Parameters
    ----------
    data_df : DataFrame
        The main DataFrame to be transformed.
    mapping_data : Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]
        A dictionary containing:
          - "mapping_steps": a list of dicts specifying each mapping step.
          - "shared_mapping_df": a shared DataFrame that may be used by multiple steps.

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
    for mapping_step in mapping_data["mapping_steps"]:
        method = mapping_step["method"]
        if method in method_handlers:
            data_df = method_handlers[method](data_df, mapping_step, mapping_data)
        else:
            raise ValueError(f"Unsupported mapping method: {method}")

    return data_df
