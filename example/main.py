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

"""
def test_context_metadata():
    tm1_params = {
        "address": "localhost",
        "port": 5365,
        "user": "admin",
        "password": "",
        "ssl": False
    }

    sql_params = {
        "host": "localhost",
        "port": 5835,
        "username": "admin",
        "password": "apple",
        "connection_type": "mssql",
        "database": "HRDEMO"
    }

    tm1_service = TM1Service(**tm1_params)
    sql_engine = create_sql_engine(**sql_params)

    context_metadata = ContextMetadata(sql_engine=sql_engine,
                                       tm1_service=tm1_service,
                                       path_to_init_yaml="test_param_inputs.yaml")
    print(context_metadata.as_dict())

    rendered_yaml = context_metadata.render_template_yaml(yaml_path="test_template_render.yaml")

    print(rendered_yaml)


def test_nativeview_functions():
    set_mdx_list = ['{{[Period].[Period].[202406]}}', '{{[Product].[Product].[P0000001],[Product].[Product].[P0000004],[Product].[Product].[P0000005],[Product].[Product].[P0000007],[Product].[Product].[P0000008],[Product].[Product].[P0000009],[Product].[Product].[P0000012],[Product].[Product].[P0000014],[Product].[Product].[P0000015],[Product].[Product].[P0000017],[Product].[Product].[P0000019],[Product].[Product].[P0000023]}}', '{{[Employee].[Employee].[Employee1],[Employee].[Employee].[Employee8],[Employee].[Employee].[Employee35],[Employee].[Employee].[Employee56],[Employee].[Employee].[Employee81],[Employee].[Employee].[Employee87],[Employee].[Employee].[Employee99]}}', '{[Version].[Version].[Actual]}', '{[Currency].[Currency].[LC]}', '{[MeasuresSales].[MeasuresSales].[Input]}', '{[OrganizationUnit].[OrganizationUnit].[Company01]}', '{[LineitemSales].[LineitemSales].[Quantity]}']

    set = "{Tm1FilterByLevel(Tm1SubsetAll([Period]), 0)} "

    mdx = """"""
    SELECT 
      NON EMPTY 
       {[Period].[Period].[202406]}
       * {[Lineitem Sales].[Lineitem Sales].[Quantity], [Lineitem Sales].[Lineitem Sales].[Revenue]}
      ON COLUMNS , 
      NON EMPTY 
       {[Product].[Product].[P0000001],[Product].[Product].[P0000004],
       [Product].[Product].[P0000005],[Product].[Product].[P0000007],
       [Product].[Product].[P0000008],[Product].[Product].[P0000009],
       [Product].[Product].[P0000012],[Product].[Product].[P0000014],
       [Product].[Product].[P0000015],[Product].[Product].[P0000017],
       [Product].[Product].[P0000019],[Product].[Product].[P0000023]}
       * {[Employee].[Employee].[Employee1],[Employee].[Employee].[Employee8],
       [Employee].[Employee].[Employee35],[Employee].[Employee].[Employee56],
       [Employee].[Employee].[Employee81],[Employee].[Employee].[Employee87],
       [Employee].[Employee].[Employee99]}     
      ON ROWS 
    FROM [Sales] 
    WHERE 
      (
       [Version].[Version].[Actual],
       [Currency].[Currency].[LC],
       [Measures Sales].[Measures Sales].[Input],
       [Organization Unit].[Organization Unit].[Company01]
      )

    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)
    #utility.generate_element_lists_from_set_mdx_list(tm1_service=tm1_service, set_mdx_list=set_mdx_list)

    #df = extractor.__tm1_mdx_to_native_view_to_dataframe(tm1_service=tm1_service, data_mdx=mdx, skip_zeros=True)
    #print(df)

    bedrock.data_copy_intercube(
        tm1_service=tm1_service,
        target_cube_name="Sales",
        mdx_function="native_view_extractor",
        data_mdx=mdx,
        skip_zeros=True,
        #skip_rule_derived_cells=True,
        use_blob=True,
        logging_level="DEBUG",
        view_and_subset_cleanup=False,
        verbose_logging_mode="print_console"
    )



    utility.set_logging_level("DEBUG")
    df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_service, data_mdx=mdx)
    utility.normalize_dataframe_strings(df)
    df.attribute = "test"
    print(df)
    print(df.attribute)

def benchpy_sample():
    schema_dir = 'C:\\Users\\ullmann.david\\PycharmProjects\\tm1bedrockpy\\schema'
    _ENV = 'bedrock_test_10000'
    schemaloader = tm1_bench.SchemaLoader(schema_dir, _ENV)
    schema = schemaloader.load_schema()

    tm1_params = {
        "address": "localhost",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }

    tm1 = TM1Service(**tm1_params)

    _DEFAULT_DF_TO_CUBE_KWARGS = schema['config']['df_to_cube_default_kwargs']
    try:
        tm1_bench.build_model(tm1=tm1, schema=schema, env=_ENV, system_defaults=_DEFAULT_DF_TO_CUBE_KWARGS)
        #tm1_bench.destroy_model(tm1=tm1, schema=schema)
    finally:
        tm1.logout()
    """


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


if __name__ == '__main__':
    hierarchy_attributes()
