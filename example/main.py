from TM1py import TM1Service
import pprint
from TM1_bedrock_py import extractor, transformer, loader, bedrock
from TM1_bedrock_py.transformer import normalize_table_source_dataframe
from string import Template
from TM1_bedrock_py.utility import create_sql_engine
from TM1_bedrock_py.context_metadata import ContextMetadata
from tm1_bench_py import tm1_bench, df_generator_for_dataset, dimension_builder, dimension_period_builder
import re
import os
import pandas as pd
from TM1_bedrock_py.dimension_builder import normalize


def hierarchy_attributes():
    tm1_params = {
        "address": "localhost",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension = "Period"
    hierarchy = "Period"

    try:
        hierarchy = tm1_service.hierarchies.get(dimension, hierarchy)
        edge_list = [
            {
                "Parent": parent,
                "Child": child,
                "Weight": weight,
                "Dimension": hierarchy.dimension_name,
                "Hierarchy": hierarchy.name
            }
            for (parent, child), weight in hierarchy.edges.items()
        ]
        df = pd.DataFrame(edge_list)
        print(df)
    finally:
        tm1_service.logout()


def element_attributes():
    tm1_params = {
        "address": "localhost",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension = "Period"
    hierarchy = "Period"
    try:
        attr_df = tm1_service.elements.get_elements_dataframe(
            dimension_name=dimension,
            hierarchy_name=hierarchy,
            skip_consolidations=False,
            attribute_suffix=True,
            skip_parents=True,
            skip_weights=True,
            element_type_column="ElementType"
        )
        attr_df.rename(columns={dimension: "ElementName"}, inplace=True)
        attr_df["ElementType"] = attr_df["ElementType"].replace({
            "Numeric": "N",
            "Consolidated": "C",
            "String": "S"
        })

        attr_df.insert(2, "Dimension", dimension)
        attr_df.insert(3, "Hierarchy", hierarchy)
        print(attr_df.columns)
        print(attr_df)
    finally:
        tm1_service.logout()


def dimension_attributes():
    tm1_params = {
        "address": "localhost",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension = "Hierarchy Test 2"
    hierarchy = "Hierarchy Test Obj"
    try:
        tm1_service.hierarchies.create(dimension_name=dimension, hierarchy_name=hierarchy)
        hier_obj = tm1_service.hierarchies.get(dimension_name=dimension, hierarchy_name=hierarchy)
        print(hier_obj)


    finally:
        tm1_service.logout()


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


def test_dim_builder_v1():
    dimension_name = "DimBuildTest"
    data = {
        "Level0": [
            "Total", None, None, None, None, None, None,
            "Total", None, None, None, None, None, None
        ],
        "Level1": [
            None, "Subtotal1", None, None, "Subtotal2", None, None,
            None, "Subtotal1", None, None, "Subtotal2", None, None
        ],
        "Level2": [
            None, None, "Element1", "Element2", None, "Element1", "Element3",
            None, None, "Element1", "Element2", None, "Element1", "Element3"
        ],
        "Dimension": [
            "DimBuildTest", "DimBuildTest", "DimBuildTest", "DimBuildTest",
            "DimBuildTest", "DimBuildTest", "DimBuildTest",
            "DimBuildTest", "DimBuildTest", "DimBuildTest", "DimBuildTest",
            "DimBuildTest", "DimBuildTest", "DimBuildTest"
        ],
        "Hierarchy": [
            "DimBuildTest", "DimBuildTest", "DimBuildTest", "DimBuildTest",
            "DimBuildTest", "DimBuildTest", "DimBuildTest",
            "AltHier", "AltHier", "AltHier", "AltHier",
            "AltHier", "AltHier", "AltHier"
        ],
        "Weight": [
            1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1
        ],
        "ElementType": [
            "C", "C", "N", "N", "C", "N", "N",
            "C", "C", "N", "N", "C", "N", "N"
        ],
        "TestAttribute1:s": [
            "Value1", "Value2", "Value3", "Value4", "Value5", "Value3", "Value7",
            "Value1", "Value2", "Value3", "Value4", "Value5", "Value3", "Value7"
        ],
        "TestAttribute2:s": [
            "Value01", "Value02", "Value03", "Value04", "Value05", "Value03", "Value07",
            "Value01", "Value02", "Value03", "Value04", "Value05", "Value03", "Value07"
        ],
    }
    input_df_indented_levels = pd.DataFrame(data)
    # print(input_df_indented_levels)
    value = input_df_indented_levels.iloc[0, 1]
    print(type(value))
    print(value)

    edges_df, attr_df = normalize.normalize_indented_level_columns(
        input_df=input_df_indented_levels,
        level_columns=["Level0", "Level1", "Level2"],
        dimension_name=dimension_name,
    )
    print(edges_df)
    print(attr_df)


if __name__ == '__main__':
    test_dim_builder_v1()
