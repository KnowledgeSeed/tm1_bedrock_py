import pandas as pd
import os
from TM1py import TM1Service

from TM1_bedrock_py import bedrock, utility
from TM1_bedrock_py.utility import set_logging_level
from tests.tests_dimension_builder.test_data.test_data import generate_hierarchy_data
import pyodbc

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
    tm1_service = TM1Service(**tm1_params)  # tm1 szerver választó lista

    target_cube_name = "Sales"  # kocka választó lista / automatikusan kitöltve (jobbklikk a kockára)

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
        bedrock.data_copy_intercube(tm1_service=tm1_service, target_cube_name=target_cube_name, data_mdx=data_mdx,
                                    check_missing_elements=ignore_missing_elements, mapping_steps=mapping_steps,
                                    clear_target=clear_target, target_clear_set_mdx_list=target_clear_set_mdx_list,
                                    value_function=inflation_value_scale, use_blob=use_blob,
                                    use_mixed_datatypes=use_mixed_datatypes, logging_level=logging_level,
                                    verbose_logging_mode="print_console",
                                    audit_mode=True)
    finally:
        tm1_service.logout()



    """
    
    
    
    """


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

    dimension_name = "DimBuilderDemo2"
    file_path = os.path.join(os.path.dirname(__file__), "dimension_builder_init.xlsx")

    try:
        bedrock.dimension_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            input_datasource=file_path,
            input_format='indented_levels',
            build_strategy='rebuild',
            level_columns=["Level1", "Level2", "Level3", "Level4"]
        )
    finally:
        tm1_service.logout()


def build_cube_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    cube_dimensions = {
        "TestCube1": ["DimBuilderDemo", "DimBuilderDemo2"],
        "TestCube2": ["DimBuilderDemo", "DimBuilderDemo2"],
        "TestCube3": ["DimBuilderDemo", "DimBuilderDemo2"]
    }

    utility.create_cubes(tm1_service, cube_dimensions)


def dimension_builder_append_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension_name = "DimBuilderDemo"
    file_path = os.path.join(os.path.dirname(__file__), "dimension_builder_append.xlsx")
    sheet_name = "Sheet1"
    input_format = 'indented_levels'
    build_strategy = 'update'
    level_columns = ["Level1", "Level2", "Level3", "Level4"]

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


def dimension_builder_complex_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension_name = "DimBuilderDemo"
    file_path = os.path.join(os.path.dirname(__file__), "dimension_builder_update.xlsx")
    sheet_name = "Sheet1"
    input_format = 'indented_levels'
    attribute_parser = "square_brackets"
    build_strategy = 'safe_rebuild'
    level_columns = ["Level1", "Level2", "Level3", "Level4"]
    weight_column = "ElementWeight"
    allow_type_changes = True
    old_orphan_parent_name = "OrphanParent"
    new_orphan_parent_name = "NewOrphanParent"
    logging_level = "DEBUG"

    """
    what we expect:
        type added, values inferred (leaf elements are considered N type by default)
        weight column renamed to standard
        square bracket attr columns parsed
        
        new elements and edges added (subtotalX, elementX, elementY, element11 under element6)
        existing elements kept (total, subtotal1, etc.)
        
        orphan parent name changed from OrphanParent to NewOrphanParent
        old orphans kept (element7, oldsubtotal2 and its children)
        new orphans added (subtotal3 and children, element4, element5)
        
        element type of element6 changed from N to C
        
        hierarchy not specified (AltHier) left as is, no modification, no delete
        
        detailed logging enabled
    """

    try:
        bedrock.dimension_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            input_datasource=file_path,
            input_format=input_format,
            build_strategy=build_strategy,
            level_columns=level_columns,
            sheet_name=sheet_name,
            weight_column=weight_column,
            allow_type_changes=allow_type_changes,
            old_orphan_parent_name=old_orphan_parent_name,
            new_orphan_parent_name=new_orphan_parent_name,
            attribute_parser=attribute_parser,
            logging_level=logging_level
        )
    finally:
        tm1_service.logout()


def hierarchy_builder_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    dimension_name = "DimBuilderDemo"
    hierarchy_name = "AltHier"
    file_path = os.path.join(os.path.dirname(__file__), "hierarchy_builder_rebuild.xlsx")
    input_format = 'indented_levels'
    attribute_parser = "square_brackets"
    build_strategy = 'rebuild'
    level_columns = ["Level1", "Level2", "Level3", "Level4"]
    weight_column = "ElementWeight"
    old_orphan_parent_name = "OrphanParent"
    new_orphan_parent_name = "NewOrphanParent"
    logging_level = "DEBUG"

    try:
        bedrock.hierarchy_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            hierarchy_name=hierarchy_name,
            input_datasource=file_path,
            input_format=input_format,
            build_strategy=build_strategy,
            level_columns=level_columns,
            weight_column=weight_column,
            old_orphan_parent_name=old_orphan_parent_name,
            new_orphan_parent_name=new_orphan_parent_name,
            attribute_parser=attribute_parser,
            logging_level=logging_level
        )
    finally:
        tm1_service.logout()


def run_pyodbc_writer():
    server_address = 'localhost,5835'
    user_name = 'admin'
    password = 'apple'
    database = 'HRDEMO'
    driver_name = 'ODBC Driver 17 for SQL Server'
    connection_string: str = (
        f"DRIVER={{{driver_name}}};"
        f"SERVER={server_address};"
        f"DATABASE={database};"
        f"UID={user_name};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
    )
    sql_connection = pyodbc.connect(connection_string)

    tm1_params = {
        "address": "localhost",
        "port": 5365,
        "user": "admin",
        "password": "",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    data_mdx = """
        SELECT
        NON EMPTY
            [}ElementAttributes_Periods].[}ElementAttributes_Periods].Members
        ON COLUMNS,
        NON EMPTY
            {TM1FILTERBYLEVEL([Periods].[Periods].Members, 0)}
        ON ROWS
        FROM [}ElementAttributes_Periods]
    """
    target_table_name = "PeriodAttributes"
    try:
        bedrock.load_tm1_cube_to_sql_table(
            tm1_service=tm1_service,
            target_table_name=target_table_name,
            data_mdx=data_mdx,
            sql_connection=sql_connection,
            sql_function='pyodbc',
            sql_schema='dbo',
            if_table_exists='replace_data',
            table_column_order=['Periods', '}ElementAttributes_Periods', 'Value']
        )
    finally:
        tm1_service.logout()


def copy_dim_between_servers_demo():
    tm1params_hrdemo = {
        "address": "localhost",
        "port": 5365,
        "user": "admin",
        "password": "",
        "ssl": False
    }
    tm1srv_hrdemo = TM1Service(**tm1params_hrdemo)

    tm1params_ksacademy = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1srv_ksacademy = TM1Service(**tm1params_ksacademy)

    utility.configure_pandas_display(pd)

    try:
        bedrock.dimension_copy(
            tm1_service=tm1srv_ksacademy,
            target_tm1_service=tm1srv_hrdemo,
            source_dimension_name="DimBuilderDemo",
            target_dimension_name="DimBuilderDemoCopy",
            logging_level="DEBUG"
        )
    finally:
        tm1srv_ksacademy.logout()
        tm1srv_hrdemo.logout()


def copy_cube_structure_between_servers_demo():
    tm1params_hrdemo = {
        "address": "localhost",
        "port": 5365,
        "user": "admin",
        "password": "",
        "ssl": False
    }
    tm1srv_hrdemo = TM1Service(**tm1params_hrdemo)

    tm1params_ksacademy = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "admin",
        "password": "admin",
        "ssl": False
    }
    tm1srv_ksacademy = TM1Service(**tm1params_ksacademy)

    utility.configure_pandas_display(pd)

    try:
        bedrock.dimension_copy(
            tm1_service=tm1srv_ksacademy,
            target_tm1_service=tm1srv_hrdemo,
            source_dimension_name="DimBuilderDemo",
            target_dimension_name="DimBuilderDemoCopy",
            logging_level="DEBUG"
        )
    finally:
        tm1srv_ksacademy.logout()
        tm1srv_hrdemo.logout()


if __name__ == '__main__':
    # dimension_builder_basic_demo()
    # dimension_builder_append_demo()
    # dimension_builder_complex_demo()
    # hierarchy_builder_demo()
    # run_pyodbc_writer()
    # complex_transform_demo()
    # build_cube_demo()
    copy_dim_between_servers_demo()
