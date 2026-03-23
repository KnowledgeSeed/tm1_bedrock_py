from typing import Tuple, Callable, Literal, Optional, Union, Any
import re
from TM1_bedrock_py import utility, loader
from TM1_bedrock_py.dimension_builder import apply, normalize
import pandas as pd
from TM1_bedrock_py.dimension_builder.validate import (
    validate_dimension_for_copy,
    validate_hierarchy_for_copy,
    validate_element_type_consistency,
    validate_attribute_name_for_dimension,
    post_validate_schema,
    validate_dimension_for_modify,
    validate_schema_for_single_hierarchy
)


def get_hierarchy_list(input_df: pd.DataFrame) -> list[str]:
    return input_df["Hierarchy"].unique().tolist()


def get_attribute_columns_list(input_df: pd.DataFrame) -> list[str]:
    non_attribute_columns = [
        "Parent", "Child", "ElementName", "ElementType", "Weight", "Dimension", "Hierarchy"
    ]
    attr_columns = [c for c in input_df.columns if c not in non_attribute_columns]
    return attr_columns


def _parse_attribute_string_colon(attr_name_and_type: str) -> Tuple[str, str]:
    name_part, type_part = attr_name_and_type.split(sep=":", maxsplit=1)
    return name_part, type_part


def _parse_attribute_string_square_brackets(attr_name_and_type: str) -> Tuple[str, str]:
    pattern = r"^(.+)\[(.+)\]$"
    match = re.match(pattern, attr_name_and_type)
    if not match:
        raise ValueError(f"Invalid format: '{attr_name_and_type}'. Expected 'Name[Type]'.")

    name_part, type_part = match.groups()
    return name_part, type_part


def _parse_attribute_string_square_brackets_start(attr_name_and_type: str) -> tuple[str, str]:
    """
    Parses a string in the format '[type]name' and returns a (name, type) tuple.
    """
    # Regex breakdown:
    # \[([^\]]+)\] : Matches '[' then captures everything until ']' into group 1
    # (.+)         : Captures everything after the brackets into group 2
    match = re.match(r"\[([^\]]+)\](.+)", attr_name_and_type)

    if not match:
        raise ValueError(f"String '{attr_name_and_type}' does not match format '[type]name'")

    type_part, name_part = match.groups()
    return name_part, type_part


def parse_attribute_string(
        attr_name_and_type: str, parser: Union[Literal["colon", "square_brackets", "square_brackets_start"], Callable] = "colon"
) -> Tuple[str, str]:
    strategies = {
        "square_brackets": _parse_attribute_string_square_brackets,
        "square_brackets_start": _parse_attribute_string_square_brackets_start,
        "colon": _parse_attribute_string_colon
    }
    func = parser

    if isinstance(func, str):
        func = strategies[func]
    return func(attr_name_and_type)


def get_legacy_edges(existing_df: Optional[pd.DataFrame], input_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if existing_df is None:
        return None

    keys = ["Parent", "Child", "Dimension", "Hierarchy"]
    merged = existing_df.merge(
        input_df[keys].drop_duplicates(),
        on=keys,
        how='left',
        indicator=True
    )
    result = merged[merged['_merge'] == 'left_only']
    return result.drop(columns=['_merge'])


def get_legacy_elements(existing_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
    keys = ["ElementName", "Dimension", "Hierarchy"]
    merged = existing_df.merge(
        input_df[keys].drop_duplicates(),
        on=keys,
        how='left',
        indicator=True
    )
    result = merged[merged['_merge'] == 'left_only']
    return result.drop(columns=['_merge'])


def unpivot_attributes_to_cube_format(elements_df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    pd.options.mode.chained_assignment = None

    attribute_dimension_name = "}ElementAttributes_" + dimension_name
    attribute_columns = get_attribute_columns_list(input_df=elements_df)

    elements_df[dimension_name] = elements_df['Hierarchy'] + ':' + elements_df['ElementName']
    df_to_melt = elements_df.drop(columns=['ElementName', 'ElementType', 'Dimension', 'Hierarchy'])
    df_to_melt = df_to_melt.rename(columns={
        attr_string: parse_attribute_string(attr_string)[0]
        for attr_string in attribute_columns
    })
    melted_df = df_to_melt.melt(
        id_vars=[dimension_name],
        var_name=attribute_dimension_name,
        value_name='Value'
    )
    return melted_df


def get_delete_records_for_conflicting_elements(conflicts: pd.DataFrame) -> list[tuple]:
    conflicting_hierarchies = conflicts["Hierarchy"].unique().tolist()
    delete_dict = {
        hier: []
        for hier in conflicting_hierarchies
    }
    for _, conflicts_row in conflicts.iterrows():
        element_name = conflicts_row["ElementName"]
        hierarchy_name = conflicts_row["Hierarchy"]
        delete_dict[hierarchy_name].append(element_name)

    targets = [
        (hier, element)
        for hier, elements in delete_dict.items()
        for element in elements
    ]

    return targets


def init_hierarchy_rename_map_for_cloning(
        source_dimension_name: str,
        source_dimension_hierarchies: list[str],
        target_dimension_name: str = None,
        hierarchy_rename_map: dict = None,
        rename_default_hierarchy: bool = True
) -> dict:
    if hierarchy_rename_map is None:
        hierarchy_rename_map = {}

    dimension_has_default_hierarchy = source_dimension_name in source_dimension_hierarchies
    can_add_default_rename_to_rename_map = (dimension_has_default_hierarchy
                                            and source_dimension_name not in hierarchy_rename_map.keys()
                                            and rename_default_hierarchy)

    if can_add_default_rename_to_rename_map:
        hierarchy_rename_map[source_dimension_name] = target_dimension_name

    return hierarchy_rename_map


def attr_column_names_from_attr_names(attr_names: list[str], df: pd.DataFrame) -> list[str]:
    col_mapping = {c.split(':')[0]: c for c in df.columns if ':' in c}
    attr_columns = [col_mapping[name] for name in attr_names if name in col_mapping]
    return attr_columns


def get_first_row_value(dataframe: pd.DataFrame, column_name: str) -> Any:
    # .get_loc finds the integer index of the column name
    # .iat[0, col_index] retrieves the first row scalar directly
    column_index: int = dataframe.columns.get_loc(column_name)
    return dataframe.iat[0, column_index]


def map_list_values(source_list: list[Any], mapping_dict: dict[Any, Any]) -> list[Any]:
    # .get(item, item) looks up the item; if not found, it returns the item itself
    return [mapping_dict.get(item, item) for item in source_list]


def dimension_copy_direct(
        tm1_service: Any,
        source_dimension_name: str,
        target_dimension_name: str,
        source_hierarchy_filter: list[str] = None,
        hierarchy_rename_map: dict = None,
        rename_default_hierarchy: bool = True,
        allow_type_changes: bool = False,
        target_tm1_service: Any = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
) -> None:
    utility.set_logging_level(logging_level=logging_level)

    # prepare steps for target
    if target_tm1_service is None:
        target_tm1_service = tm1_service
    target_dim_exists = target_tm1_service.dimensions.exists(target_dimension_name)

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

    # pre-check before transform, manage conflicts
    if target_dim_exists:
        retained_edges_df, retained_elements_df = apply.init_existing_schema_for_builder(
            tm1_service=tm1_service,
            dimension_name=target_dimension_name,
            clear_orphan_parents=False
        )
        conflicts = validate_element_type_consistency(retained_elements_df, elements_df, allow_type_changes)
        if allow_type_changes and conflicts is not None:
            apply.delete_conflicting_elements(
                tm1_service=target_tm1_service, conflicts=conflicts, dimension_name=target_dimension_name)

    # transforms
    edges_df, elements_df = normalize.transform_hierarchy_structure_for_copy(
        edges_df, elements_df, hierarchy_rename_map, target_dimension_name)
    hierarchy_scope = map_list_values(hierarchy_scope, hierarchy_rename_map)

    # check type consistency, and build new hiers in existing dim
    if target_dim_exists:
        for hierarchy_name in hierarchy_scope:
            hierarchy = apply.build_hierarchy_object(target_dimension_name, hierarchy_name, edges_df, elements_df)
            target_tm1_service.hierarchies.update_or_create(hierarchy)

    # build new dim
    else:
        dimension = apply.build_dimension_object(dimension_name=target_dimension_name, edges_df=edges_df,
                                                 elements_df=elements_df)
        target_tm1_service.dimensions.update_or_create(dimension)

    # manage attributes
    writable_attr_df, attr_cube_name, attr_cube_dims = apply.prepare_attributes_for_load(
        dimension_name=target_dimension_name, elements_df=elements_df)

    loader.dataframe_to_cube(
        tm1_service=target_tm1_service,
        dataframe=writable_attr_df,
        cube_name=attr_cube_name,
        cube_dims=attr_cube_dims,
        use_blob=True,
    )


def hierarchy_copy_direct(
        tm1_service: Any,
        dimension_name: str,
        source_hierarchy_name: str,
        target_hierarchy_name: str,
        target_tm1_service: Any = None,
        logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "WARNING",
) -> None:
    utility.set_logging_level(logging_level=logging_level)

    # prepare steps
    if target_tm1_service is None:
        target_tm1_service = tm1_service

    validate_hierarchy_for_copy(tm1_service, dimension_name, source_hierarchy_name)

    # get source data
    edges_df, elements_df = apply.init_existing_schema_filtered(
        tm1_service, dimension_name, [source_hierarchy_name])

    # pre-check before transform, manage conflicts
    if target_tm1_service.dimensions.exists(dimension_name):
        retained_edges_df, retained_elements_df = apply.init_existing_schema_for_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            clear_orphan_parents=False
        )
        validate_element_type_consistency(retained_elements_df, elements_df,
                                          allow_type_changes=False)

    # transform steps
    edges_df, elements_df = normalize.transform_hierarchy_structure_for_copy(
        edges_df, elements_df, hierarchy_rename_map={source_hierarchy_name: target_hierarchy_name},
        target_dimension_name=dimension_name
    )

    # build hierarchy in dim
    hierarchy = apply.build_hierarchy_object(
        dimension_name, target_hierarchy_name, edges_df, elements_df)

    target_tm1_service.hierarchies.update_or_create(hierarchy)

    # manage attributes
    writable_attr_df, attr_cube_name, attr_cube_dims = apply.prepare_attributes_for_load(
        dimension_name=dimension_name, elements_df=elements_df)

    loader.dataframe_to_cube(
        tm1_service=target_tm1_service,
        dataframe=writable_attr_df,
        cube_name=attr_cube_name,
        cube_dims=attr_cube_dims,
        use_blob=True,
    )
