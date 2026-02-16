import pandas as pd
import os
from TM1py import TM1Service

from TM1_bedrock_py import bedrock
from TM1_bedrock_py.dimension_builder.io import read_source_to_df
from TM1_bedrock_py.utility import set_logging_level
from tests.tests_dimension_builder.test_data.test_data import generate_hierarchy_data


def complex_transform_demo():
    # letárolás másik verzióra
    # újrastruktúrálás mapping kockával (employee-orgunit) az eredeti idősíkon
    # adat áthelyezés egy évvel későbbre
    # számok felszorzása az inflációval

    tm1_params = {
        "address": "localhost",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    target_cube_name = "Sales"
    data_mdx = """
        SELECT
        NON EMPTY
            {[Period].[Period].[202406]}
        ON COLUMNS,
        NON EMPTY
            {[Lineitem Sales].[Lineitem Sales].Members}
            * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Currency].[Currency])} , 0)}
            * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Product].[Product])} , 0)}
            * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Organization Unit].[Organization Unit])} , 0)}
            * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Employee].[Employee])} , 0)}
        ON ROWS
        FROM [Sales]
        WHERE (
            [Version].[Version].[Actual],
            [Measures Sales].[Measures Sales].[Input] )
        """

    mapping_steps = [
        {
            "method": "replace",
            "mapping": {"Version": {"Actual": "Budget"}}
        },
        {
            "method": "map_and_replace",
            "mapping_mdx": """
                    SELECT
                        {[}ElementAttributes_Period].[}ElementAttributes_Period].[NEXT_Y_PERIOD]}
                    ON COLUMNS,
                        {TM1FILTERBYLEVEL( {TM1SUBSETALL([Period].[Period])} , 0)}
                    ON ROWS
                    FROM [}ElementAttributes_Period]
                """,
            "mapping_dimensions": {"Period": "Value"},
            "include_mapped_in_join": True
        },
        {
            "method": "map_and_replace",
            "mapping_mdx": """
                    SELECT
                    NON EMPTY 
                        {TM1FILTERBYLEVEL( {TM1SUBSETALL([Employee].[Employee])} , 0)}
                    ON COLUMNS, 
                    NON EMPTY 
                        {TM1FILTERBYLEVEL( {TM1SUBSETALL([Organization Unit].[Organization Unit])} , 0)} 
                    ON ROWS
                    FROM [Employee to Organization Unit]
                    WHERE (
                        [Version].[Version].[Budget],
                        [Measure Employee to Organization Unit].[Assign Flag]
                    )
                """,
            "mapping_dimensions": {"Organization Unit": "Organization Unit"}
        }
    ]

    clear_target = True
    target_clear_set_mdx_list = [
        "{[Version].[Version].[Budget]}",
        "{[Period].[Period].[202506]}"
    ]

    logging_level = "DEBUG"
    use_mixed_datatypes = True
    ignore_missing_elements = True
    use_blob = True

    def inflation_value_scale(x):
        return x * 1.0912

    try:
        #bedrock.data_copy_intercube(
        bedrock.data_copy(
            #target_cube_name=target_cube_name,
            tm1_service=tm1_service,
            data_mdx=data_mdx,
            mapping_steps=mapping_steps,
            value_function=inflation_value_scale,
            clear_target=clear_target,
            target_clear_set_mdx_list=target_clear_set_mdx_list,
            use_blob=use_blob,
            logging_level=logging_level,
            #use_mixed_datatypes=use_mixed_datatypes,
            #ignore_missing_elements=ignore_missing_elements,
            verbose_logging_mode="print_console"
        )
    finally:
        tm1_service.logout()


def run_dim_builder_wrapper():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)
    set_logging_level("DEBUG")

    dimension_name = "DimGenerator"
    hierarchy_names = ["DimGenerator", "Alt"]
    old_orphan_parent_name = "OrphanParent"
    orphan_parent_name = "OrphanParent"
    allow_type_changes = True

    data, level_columns = generate_hierarchy_data(
        dimension_name=dimension_name,
        hierarchy_names=hierarchy_names,
        nodes_per_hierarchy=10,
        max_depth=1,
        number_of_attributes=10,
        consistent_leaf_attributes=True
    )
    raw_input_df = pd.DataFrame(data)

    bedrock.dimension_builder(
        dimension_name=dimension_name,
        input_format="indented_levels",
        build_strategy="rebuild",
        allow_type_changes=allow_type_changes,
        tm1_service=tm1_service,
        old_orphan_parent_name=old_orphan_parent_name,
        new_orphan_parent_name=orphan_parent_name,
        level_columns=level_columns,
        raw_input_df=raw_input_df,
        logging_level="DEBUG"
    )


def dimension_builder_basic_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension_name = "DimBuilderDemo"
    file_path = os.path.join(os.path.dirname(__file__), "dimension_builder_init.xlsx")
    sheet_name = "Sheet1"
    input_format = 'indented_levels'
    build_strategy = 'rebuild'
    level_columns = ["Level1", "Level2", "Level3", "Level4"]

    pd.set_option('display.max_rows', None)  # None means show all rows
    pd.set_option('display.max_columns', None)  # None means show all columns
    pd.set_option('display.width', 1000)  # Prevents line-wrapping
    pd.set_option('display.max_colwidth', None)  # Shows full text inside cells

    input_df = read_source_to_df(source=file_path, sheet_name=sheet_name)
    print(type(input_df))
    print(input_df)



    try:
        bedrock.dimension_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            input_datasource=file_path,
            input_format=input_format,
            build_strategy=build_strategy,
            level_columns=level_columns,
            sheet_name=sheet_name
        )
    finally:
        tm1_service.logout()



if __name__ == '__main__':
    dimension_builder_basic_demo()
