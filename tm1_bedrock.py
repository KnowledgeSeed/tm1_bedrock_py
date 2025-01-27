"""
This file is a collection of upgraded TM1 bedrock functionality, ported to python / pandas with the help of TM1py.
"""

import pandas as pd
import re


def parse_from_clause(mdx_query):
    """
    Extracts the cube name from the FROM clause of an MDX query.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        str: The name of the cube specified in the FROM clause.

    Raises:
        ValueError: If the MDX query does not contain a valid FROM clause.
    """
    from_part_match = re.search(r"FROM\s*\[(.*?)\]", mdx_query, re.IGNORECASE)
    if not from_part_match:
        raise ValueError("MDX query is missing the FROM clause.")
    return from_part_match.group(1).strip()


def parse_where_clause(mdx_query):
    """
    Extracts the dimensions, hierarchies, and elements from the WHERE clause of an MDX query.

    Args:
        mdx_query (str): The MDX query string to parse.

    Returns:
        dict: A dictionary where keys are dimension names and values are a tuple containing
              the hierarchy (if present) and the element from the WHERE clause.
              If the hierarchy is not specified, it will return None for the hierarchy.
              If the WHERE clause is not present, returns an empty dictionary.
    """
    where_match = re.search(r'WHERE\s*\((.*?)\)', mdx_query, re.S)
    if not where_match:
        return {}
    where_content = where_match.group(1)
    hier_elements = re.findall(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]', where_content)
    result = {dim: (hier, elem) for dim, hier, elem in hier_elements}
    remaining_content = re.sub(r'\[(.*?)\]\.\[(.*?)\]\.\[(.*?)\]', '', where_content)
    dim_elements = re.findall(r'\[(.*?)\]\.\[(.*?)\]', remaining_content)
    for dim, elem in dim_elements:
        result[dim] = (dim, elem)
    return result


def get_hierarchy_default_element(tm1_service, dimension_name, hierarchy_name):
    """
    gets the default element of a dimension-hierarchy

    Args:
        tm1_service (obj): An active TM1Service object
        dimension_name (str)
        hierarchy_name (str)

    Returns:
        str: The name of the default element
    """
    hierarchy_name = hierarchy_name or dimension_name
    hierarchy = tm1_service.hierarchies.get(dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    return hierarchy.default_member


def element_is_writable(tm1_service, dimension_name, hierarchy_name, element_name):
    """
    Check if a dimension element is writable (not rule-derived, N type).

    Args:
        tm1_service: The TM1py service object.
        dimension_name (str): The name of the dimension.
        hierarchy_name (str): The name of the hierarchy (usually the same as the dimension name).
        element_name (str): The name of the element to check.

    Returns:
        bool: True if the element is writable, False otherwise.
    """
    element = tm1_service.elements.get(
        dimension_name=dimension_name, hierarchy_name=hierarchy_name, element_name=element_name
    )
    return not element.is_consolidated


# tm1 -> tm1py -> pd df -> pd df -> tm1py -> tm1


def mdx_to_dataframe_ordered(
    tm1_service, data_mdx,
    skip_zeros=False, skip_consolidated_cells=False, skip_rule_derived_cells=False
):
    """
    Execute an MDX query with optional filtering for non-zero, consolidated, rule-derived cells.
    Returns dataframe that has all dimensions of the source cube, in order, plus a value column.

    Args:
        tm1_service (TM1Service): An existing TM1Service object.
        data_mdx (str): The original MDX query to dissect and filter.
        skip_zeros (bool): Whether to include only non-zero cells.
        skip_consolidated_cells (bool): Whether to include consolidated cells.
        skip_rule_derived_cells (bool): Whether to include rule-derived cells.

    Returns:
        pd.DataFrame: DataFrame containing the filtered MDX results.
    """
    df = tm1_service.cells.execute_mdx_dataframe(
        mdx=data_mdx,
        skip_zeros=skip_zeros,
        skip_consolidated_cells=skip_consolidated_cells,
        skip_rule_derived_cells=skip_rule_derived_cells
    )
    cube_name = parse_from_clause(data_mdx)
    cube_dims = tm1_service.cubes.get_dimension_names(cube_name)
    cube_dims.append('Value')
    where_clause = parse_where_clause(data_mdx)
    for dim, (_, elem) in where_clause.items():
        df[dim] = elem
    df_reordered = df.loc[:, cube_dims]
    return df_reordered


def dataframe_to_cube_with_clear(tm1_service, dataframe, cube_name, clear_target=False, mode='default'):
    """
    Writes a Pandas DataFrame to a TM1 cube, with optional clearing of target data and
    support for TI (TurboIntegrator) and BLOB modes.

    Args:
        tm1_service (TM1Service): The active TM1py TM1Service object for interacting with the TM1 server.
        dataframe (pd.DataFrame): The Pandas DataFrame containing the data to be written to the cube.
        cube_name (str): The name of the TM1 cube where the data will be written.
        mode (str, optional): Specifies the mode for writing data. Options are:
                              - 'default' (default): Use the standard TM1py write mechanism.
                              - 'ti': Use TurboIntegrator for writing data.
                              - 'blob': Use the BLOB-based write mechanism.
        clear_target (bool, optional): If True, clears the target cube's data for the dimensions and elements
                                       present in the DataFrame before writing new data. Defaults to False.

    Behavior:
        - If `clear_target` is True, the function first clears the data in the specified cube
          for the intersections defined by the DataFrame.
        - Writes the data from the DataFrame into the specified TM1 cube.
        - Supports transactional log deactivation/reactivation during the write operation
          to optimize performance.
        - Allows for the use of TurboIntegrator (TI) or BLOB methods to write data when specified.
    """

    use_ti = mode == 'ti'
    use_blob = mode == 'blob'
    if clear_target:
        tm1_service.cells.clear_with_dataframe(cube=cube_name, dataframe=dataframe)
    tm1_service.cells.write_dataframe(
        cube_name=cube_name,
        data=dataframe,
        deactivate_transaction_log=True,
        reactivate_transaction_log=True,
        skip_non_updateable=True,
        use_ti=use_ti,
        use_blob=use_blob
    )


def mdx_to_dataframe_with_literal_remap(
    tm1_service, data_mdx, mapping,
    skip_zeros=False, skip_consolidated_cells=False, skip_rule_derived_cells=False
):
    """
    Execute an MDX query, remap dimension elements, return dataframe that has all dimensions of the cube, in order,
    plus a dimension for value
    The mapping dictionary can have multiple dimensions, and multiple elements in the dimensions as well.
    {dim1:{old1:new1, old3:new3, old4:new4, ...}, dim2:{old2:new2}, ...}

    Args:
        tm1_service (TM1Service): An active TM1Service instance.
        data_mdx (str): The MDX query to execute.
        mapping (dict): A dictionary where the key is the dimension name, and the value is a dictionary
                        mapping old elements to new elements.
        skip_zeros (bool): Whether to include only non-zero cells.
        skip_consolidated_cells (bool): Whether to include consolidated cells.
        skip_rule_derived_cells (bool): Whether to include rule-derived cells.

    Returns:
        None
    """
    df = mdx_to_dataframe_ordered(
        tm1_service, data_mdx,
        skip_zeros, skip_consolidated_cells, skip_rule_derived_cells
    )
    for dimension, element_mapping in mapping.items():
        if dimension in df.columns:
            df[dimension] = df[dimension].replace(element_mapping)
    return df


def mdx_to_cube_with_literal_remap(
    tm1_service, data_mdx, mapping, target_cube,
    skip_zeros=False, skip_consolidated_cells=False, skip_rule_derived_cells=False,
    clear_target=False, mode='default'
):
    df = mdx_to_dataframe_with_literal_remap(
        tm1_service, data_mdx, mapping,
        skip_zeros, skip_consolidated_cells, skip_rule_derived_cells
    )
    dataframe_to_cube_with_clear(tm1_service, df, target_cube, clear_target, mode)


def mdx_to_dataframe_with_settings_mapping(
    tm1_service, data_mdx, settings_mdx, target_mapping_dict,
    skip_zeros=False, skip_consolidated_cells=False, skip_rule_derived_cells=False
):
    """
    Map data from a source cube using settings from another cube and return pd dataframe.

    Args:
        tm1_service (TM1Service): An active TM1Service instance.
        data_mdx (str): The MDX query for the source data cube.
        settings_mdx (str): The MDX query for the settings cube with mappings.
        target_mapping_dict (dict): Dictionary where keys are dimensions in the data cube,
                                    and values are the mapping elements in the settings cube.
        skip_zeros (bool): Whether to include only non-zero cells in the data query.
        skip_consolidated_cells (bool): Whether to include consolidated cells in the data query.
        skip_rule_derived_cells (bool): Whether to include rule-derived cells in the data query.

    Returns:
        None
    """
    data_df = mdx_to_dataframe_ordered(
        tm1_service, data_mdx,
        skip_zeros, skip_consolidated_cells, skip_rule_derived_cells
    )
    settings_df = mdx_to_dataframe_ordered(
        tm1_service, settings_mdx
    )

    def extract_column_dimension(mdx):
        pattern = r"ON COLUMNS.*?\{.*?\[(.*?)\]\..*?\}"
        match = re.search(pattern, mdx, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
        else:
            raise ValueError("Unable to extract column dimension from the settings MDX.")

    column_mapping_dimension = extract_column_dimension(settings_mdx)
    shared_dimensions = [col for col in settings_df.columns if col != column_mapping_dimension]
    missing_dims = [dim for dim in shared_dimensions if dim not in data_df.columns]
    if missing_dims:
        raise ValueError(f"The following shared dimensions are missing in the data cube: {missing_dims}")

    for target_dimension, mapping_element in target_mapping_dict.items():
        if mapping_element not in settings_df.columns:
            raise ValueError(f"Mapping element '{mapping_element}' for dimension '{target_dimension}' "
                             f"is not found in the settings cube.")

    merged_df = data_df.merge(
        settings_df[shared_dimensions + list(target_mapping_dict.values())],
        how="left", on=shared_dimensions
    )

    for target_dimension, mapping_element in target_mapping_dict.items():
        merged_df[target_dimension] = merged_df[mapping_element]

    merged_df = merged_df.drop(columns=list(target_mapping_dict.values()), errors="ignore")

    return merged_df


def mdx_to_cube_with_settings_remap(
        tm1_service, data_mdx, settings_mdx, target_mapping_dict, target_cube,
        skip_zeros=False, skip_consolidated_cells=False, skip_rule_derived_cells=False,
        clear_target=False, mode='default'
):
    df = mdx_to_dataframe_with_settings_mapping(
        tm1_service, data_mdx, settings_mdx, target_mapping_dict,
        skip_zeros, skip_consolidated_cells, skip_rule_derived_cells
    )

    dataframe_to_cube_with_clear(tm1_service, df, target_cube, clear_target, mode)


def mdx_to_dataframe_with_cube_mapping(
        tm1_service, data_mdx, mapping_mdx, dimension_mapping,
        skip_zeros=True, skip_consolidated_cells=True, skip_rule_derived_cells=True
):
    """
    Map data from a target cube to new dimension values using a mapping cube.

    Args:
        tm1_service (TM1Service): An active TM1Service instance.
        data_mdx (str): MDX query to extract data from the target cube.
        mapping_mdx (str): MDX query to extract mapping basis data.
        dimension_mapping (dict): Dictionary where keys are data cube dimensions,
                                   and values are corresponding mapping cube dimensions.
        skip_zeros (bool): Whether to include only non-zero cells in the data query.
        skip_consolidated_cells (bool): Whether to include consolidated cells in the data query.
        skip_rule_derived_cells (bool): Whether to include rule-derived cells in the data query.

    Returns:
        pd.DataFrame: DataFrame with the same structure as data_mdx, but with mapped dimension values.
    """

    data_df = mdx_to_dataframe_ordered(
        tm1_service, data_mdx,
        skip_zeros, skip_consolidated_cells, skip_rule_derived_cells
    )

    mapping_df = mdx_to_dataframe_ordered(
        tm1_service, mapping_mdx
    )
    mapping_col_dim = re.search(r"ON COLUMNS\s+\{.*?\[(.*?)\]\..*?\}", mapping_mdx, re.IGNORECASE)
    if not mapping_col_dim:
        raise ValueError("The mapping MDX must have exactly one dimension in ON COLUMNS with a specified element.")
    mapping_measure_dim = mapping_col_dim.group(1)

    shared_dimensions = [dim for dim in data_df.columns if dim in mapping_df.columns]

    if not all(dim in data_df.columns for dim in dimension_mapping.keys()):
        raise ValueError("All keys in dimension_mapping must exist in the data MDX dimensions.")
    if not all(dim in mapping_df.columns for dim in dimension_mapping.values()):
        raise ValueError("All values in dimension_mapping must exist in the mapping MDX dimensions.")

    merged_df = data_df.merge(mapping_df, how="left", on=shared_dimensions)

    for data_dim, mapping_dim in dimension_mapping.items():
        if mapping_dim in merged_df.columns:
            merged_df[data_dim] = merged_df[mapping_dim]

    drop_columns = [mapping_dim for _, mapping_dim in dimension_mapping.items()]
    merged_df = merged_df.drop(columns=drop_columns + [mapping_measure_dim], errors="ignore")

    return merged_df


def transform_dataframe_to_target_dataframe(
    tm1_service,
    data_df,
    target_cube,
    source_dimension_mapping_for_copy,
    target_dimension_mapping_for_copy,
    dimension_mapping_for_copy
):
    """
    Transforms the input DataFrame to match the dimensionality of the target cube.

    Args:
        tm1_service (TM1Service): Active TM1py service instance.
        data_df (pd.DataFrame): DataFrame containing source cube's data.
        target_cube (str): Name of the target cube.
        source_dimension_mapping_for_copy (dict, optional): Mapping for extra source dimensions.
        target_dimension_mapping_for_copy (dict, optional): Mapping for extra target dimensions.
        dimension_mapping_for_copy (dict, optional): Mapping for dimensions unique to source and target cubes.

    Returns:
        pd.DataFrame: Transformed DataFrame in the dimensionality of the target cube.

    Raises:
        ValueError: If required mappings or defaults are missing or invalid.
    """

    source_dimensions = list(data_df.columns)
    target_dimensions = tm1_service.cubes.get_dimension_names(target_cube)

    # Handle cases where dimensions are unequal
    # Case 1: Dimensions are equal
    if set(source_dimensions) == set(target_dimensions):
        return data_df

    # Case 2: Source cube has extra dimensions
    for dim in set(source_dimensions) - set(target_dimensions):
        if source_dimension_mapping_for_copy and dim in source_dimension_mapping_for_copy:
            data_df = data_df[data_df[dim] == source_dimension_mapping_for_copy[dim]]
            data_df = data_df.drop(columns=[dim])
        else:
            default_element = get_hierarchy_default_element(tm1_service, dim, dim)
            if not default_element:
                raise ValueError(f"No mapping or default element for source dimension '{dim}'.")
            data_df = data_df[data_df[dim] == default_element]
            data_df = data_df.drop(columns=[dim])

    # Case 3: Target cube has extra dimensions
    for dim in set(target_dimensions) - set(source_dimensions):
        if target_dimension_mapping_for_copy and dim in target_dimension_mapping_for_copy:
            data_df[dim] = target_dimension_mapping_for_copy[dim]
        else:
            default_element = get_hierarchy_default_element(tm1_service, dim, dim)
            if not default_element or not element_is_writable(tm1_service, dim, dim, default_element):
                raise ValueError(f"No mapping, writable element, or default element for target dimension '{dim}'.")
            data_df[dim] = default_element

    # Case 4: Handle dimension mapping using dimension_mapping_for_copy
    if dimension_mapping_for_copy:
        for src_dim, tgt_dim in dimension_mapping_for_copy.items():
            if src_dim in data_df.columns and tgt_dim in target_dimensions:
                data_df[tgt_dim] = data_df[src_dim]
                data_df.drop(columns=[src_dim], inplace=True)

    # Ensure DataFrame has all target dimensions in the correct order
    missing_dims = set(target_dimensions) - set(data_df.columns)
    if missing_dims:
        raise ValueError(f"The transformed DataFrame is missing target dimensions: {missing_dims}")

    # Reorder DataFrame to match target cube dimensions
    data_df = data_df[target_dimensions + list(set(data_df.columns) - set(target_dimensions))]
    return data_df
