from typing import Callable, List, Dict, Optional, Any

from TM1py import TM1Service
from pandas import DataFrame

from TM1_bedrock_py import utility, transformer


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


# extract
def extract(
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        **kwargs: Any
) -> DataFrame:
    """
    Retrieves a DataFrame by executing the provided MDX function.

    Args:
        mdx_function (Optional[Callable]): A function to execute the MDX query and return a DataFrame.
                                           If None, the default function is used.
        **kwargs (Any): Additional keyword arguments passed to the MDX function.

    Returns:
        DataFrame: The DataFrame resulting from the MDX query.
    """
    if mdx_function is None:
        mdx_function = __extract_default

    return mdx_function(**kwargs)


# extract, internal
def __extract_default(
        tm1_service: TM1Service,
        data_mdx: Optional[str] = None,
        data_mdx_list:  Optional[list[str]] = None,
        skip_zeros: bool = False,
        skip_consolidated_cells: bool = False,
        skip_rule_derived_cells: bool = False
) -> DataFrame:
    """
    Executes an MDX query using the default TM1 service function and returns a DataFrame.
    If an MDX is given, it will execute it synchronously,
    if an MDX list is given, it will execute them asynchronously.

    Args:
        tm1_service (TM1Service): An active TM1Service object for connecting to the TM1 server.
        data_mdx (str): The MDX query string to execute.
        data_mdx_list (list[str]): A list of mdx queries to execute in an asynchronous way.
        skip_zeros (bool, optional): If True, cells with zero values will be excluded. Defaults to False.
        skip_consolidated_cells (bool, optional): If True, consolidated cells will be excluded. Defaults to False.
        skip_rule_derived_cells (bool, optional): If True, rule-derived cells will be excluded. Defaults to False.

    Returns:
        DataFrame: A DataFrame containing the result of the MDX query.
    """

    if data_mdx_list:
        return tm1_service.cells.execute_mdx_dataframe_async(
            mdx_list=data_mdx_list,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True
        )
    else:
        return tm1_service.cells.execute_mdx_dataframe(
            mdx=data_mdx,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            skip_rule_derived_cells=skip_rule_derived_cells,
            use_iterative_json=True
        )


# extract, internal
def __assign_mapping_dataframes(
        mapping_steps: List[Dict],
        shared_mapping_df: Optional[DataFrame] = None,
        shared_mapping_mdx: Optional[str] = None,
        shared_mapping_metadata_function: Optional[Callable[..., Any]] = None,
        mdx_function: Optional[Callable[..., DataFrame]] = None,
        tm1_service: Optional[Any] = None,
        **kwargs
) -> Dict[str, Optional[DataFrame] | List[Dict[str, Any]]]:
    """
    Assigns mapping DataFrames to mapping steps by either:
    - Using an existing 'mapping_df' in the step (if provided).
    - Generating a DataFrame from 'mapping_mdx' (if provided).
    - Falling back to 'shared_mapping_df' if neither is provided.

    Ensures that 'map_and_replace' steps have at least one valid mapping source
    (either a step-specific 'mapping_df' or 'mapping_mdx', or a shared_mapping_df).

    Parameters:
    ----------
    mapping_steps : List[Dict]
        A list of mapping step dictionaries, each containing at least a 'method' key.
        - If 'method' is "map_and_replace", it must include either 'mapping_df' or 'mapping_mdx',
          or else the shared_mapping_df must be provided.
        - If 'method' is "replace", no additional checks are applied.

    shared_mapping_df : Optional[DataFrame], default=None
        A shared DataFrame to be used if no 'mapping_df' or 'mapping_mdx' is provided.

    shared_mapping_mdx : Optional[str], default=None
        A shared MDX query string that will be converted into a DataFrame if no
        'mapping_df' or 'mapping_mdx' is provided.

    mdx_function : Optional[Callable[..., DataFrame]], default=None
        A function that takes an MDX query and returns a Pandas DataFrame.
        Used to convert 'mapping_mdx' queries into DataFrames.

    tm1_service : Optional[Any], default=None
        An optional service object used by 'mdx_function' if required.

    **kwargs : dict
        Additional keyword arguments that will be passed to 'mdx_function'.

    Returns:
    -------
    Dict[str, Any]
        A dictionary containing:
        - 'shared_mapping_df': The resolved shared mapping DataFrame.
        - 'mapping_data': The updated list of mapping steps, where each 'map_and_replace' step
          has an assigned 'mapping_df' if necessary.

    Raises:
    ------
    ValueError
        If any 'map_and_replace' step lacks both 'mapping_df' and 'mapping_mdx',
        AND 'shared_mapping_df' is also missing or empty.
    """

    def create_dataframe(mdx: str, metadata_function: Optional[Callable[..., Any]] = None) -> DataFrame:
        """Helper function to convert MDX to a normalized DataFrame."""
        dataframe = extract(
            mdx_function=mdx_function,
            tm1_service=tm1_service,
            data_mdx=mdx,
            skip_zeros=True,
            skip_consolidated_cells=True,
            **kwargs
        )
        filter_dict = utility.tm1_cube_object_metadata_collect(
            metadata_function=metadata_function, **kwargs
        ).get_filter_dict()
        return transformer.dataframe_add_column_assign_value(dataframe=dataframe, column_value=filter_dict)

    shared_mapping_df = shared_mapping_df or (
        create_dataframe(shared_mapping_mdx, shared_mapping_metadata_function) if shared_mapping_mdx else None
    )

    mapping_steps = [
        {
            **step,
            "mapping_df": step.get("mapping_df")
            or (create_dataframe(step["mapping_mdx"],
                                 step["mapping_metadata_function"]
                                 ) if "mapping_mdx" in step else None)
        }
        for step in mapping_steps
    ]

    missing_mdx_steps = [
        step for step in mapping_steps
        if step["method"] == "map_and_replace" and not step.get("mapping_df")
    ]

    if missing_mdx_steps and (shared_mapping_df is None or shared_mapping_df.empty):
        raise ValueError(
            f"Missing mapping source for 'map_and_replace' steps: {missing_mdx_steps}. "
            "Either provide 'mapping_mdx' or 'mapping_df' in these steps, or a valid 'shared_mapping_df'."
        )

    return {"shared_mapping_df": shared_mapping_df, "mapping_steps": mapping_steps}
