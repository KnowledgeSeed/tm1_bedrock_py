import configparser
from pathlib import Path

import pandas as pd
import os
from TM1py import TM1Service

from TM1_bedrock_py import bedrock, utility
from TM1_bedrock_py.utility import set_logging_level
from TM1_bedrock_py.context_metadata import ContextMetadata
from tests.tests_dimension_builder.test_data.test_data import generate_hierarchy_data
import pyodbc
from TM1_bedrock_py.dimension_builder.apply import create_attribute_structure


def create_tm1_connection(connection_name: str = 'ks_academy'):
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))
    return TM1Service(**config[connection_name])  # tm1 szerver választó lista


def complex_transform_demo():
    # letárolás másik verzióra
    # újrastruktúrálás mapping kockával (employee-orgunit) az eredeti idősíkon
    # adat áthelyezés egy évvel későbbre
    # számok felszorzása az inflációval

    tm1_service = create_tm1_connection('ks_academy')

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
    use_mixed_datatypes = False
    ignore_missing_elements = True
    use_blob = True

    def inflation_value_scale(x):
        return x * 1.0912

    try:
        bedrock.data_copy_intercube(tm1_service=tm1_service, target_cube_name=target_cube_name, data_mdx=data_mdx,
                                    check_missing_elements=ignore_missing_elements, mapping_steps=mapping_steps,
                                    clear_target=clear_target, target_clear_set_mdx_list=target_clear_set_mdx_list,
                                    value_function=None, use_blob=use_blob,
                                    use_mixed_datatypes=use_mixed_datatypes, logging_level=logging_level,
                                    verbose_logging_mode="print_console",
                                    audit_mode=True)
    finally:
        tm1_service.logout()


def run_dim_builder_wrapper():
    tm1_service = create_tm1_connection('ks_academy')
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
    tm1_service = create_tm1_connection('ks_academy')

    dimension_name = "DimBuilderDemo9"
    file_path = os.path.join(os.path.dirname(__file__), "dimension_builder_init2.xlsx")

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
    tm1_service = create_tm1_connection('ks_academy')

    cube_dimensions = {
        "TestCube1": ["DimBuilderDemo", "DimBuilderDemo2"],
        "TestCube2": ["DimBuilderDemo", "DimBuilderDemo2"],
        "TestCube3": ["DimBuilderDemo", "DimBuilderDemo2"]
    }

    utility.create_cubes(tm1_service, cube_dimensions)


def dimension_builder_append_demo():
    tm1_service = create_tm1_connection('ks_academy')

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
    tm1_service = create_tm1_connection('ks_academy')

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
    tm1_service = create_tm1_connection('ks_academy')

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


def tm1_to_sql_pyodbc_custom_writer_demo():
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))
    sql_config = config['sqlparams_hrdemo']

    server_address = f'{sql_config["host"]},{sql_config["port"]}'
    user_name = sql_config["username"]
    password = sql_config["password"]
    database = sql_config["database"]
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

    tm1_service = create_tm1_connection('hr_demo')

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
    tm1srv_hrdemo = create_tm1_connection('hr_demo')
    tm1srv_ksacademy = create_tm1_connection('ks_academy')

    utility.configure_pandas_display(pd)

    try:
        bedrock.dimension_copy(
            tm1_service=tm1srv_ksacademy,
            target_tm1_service=tm1srv_hrdemo,
            source_dimension_name="sys_group_nucleus_content_mapping_check_measure",
            logging_level="DEBUG"
        )
    finally:
        tm1srv_ksacademy.logout()
        tm1srv_hrdemo.logout()


def copy_data_between_servers_demo():
    tm1srv_hrdemo = create_tm1_connection('hr_demo')
    tm1srv_ksacademy = create_tm1_connection('ks_academy')

    utility.configure_pandas_display(pd)

    mdx = """
        SELECT
        NON EMPTY
        {Tm1FilterByLevel({Tm1SubsetAll([DimBuilderDemo].[DimBuilderDemo])}, 0)}
        * {Tm1FilterByLevel({Tm1SubsetAll([DimBuilderDemo2].[DimBuilderDemo])}, 0)}
        ON 0
        FROM [TestCube1]
    """
    relabel_step = {
        "method": "basic_reshaping",
        "column_relabel_map": {"DimBuilderDemo": "DimBuilderDemoCopy", "DimBuilderDemo2": "DimBuilderDemoCopy2"}
    }

    try:
        missing_df = bedrock.data_copy_intercube(
            tm1_service=tm1srv_ksacademy,
            target_tm1_service=tm1srv_hrdemo,
            target_cube_name="DimBuilderDemoCopyCube",
            data_mdx=mdx,
            mapping_steps=[relabel_step],
            check_missing_elements=True,
            audit_mode=True,
            skip_zeros=False,
            logging_level='DEBUG',
            verbose_logging_mode='print_console',
            log_missing_elements=True,
            output_missing_elements=True
        )

        print("missing df")
        print(missing_df)
    finally:
        tm1srv_ksacademy.logout()
        tm1srv_hrdemo.logout()


def dimension_builder_no_edges_old_format():
    tm1_service = create_tm1_connection('ks_academy')
    
    dimension_name = "DimBuilderDemo7"
    file_path = os.path.join(os.path.dirname(__file__), "company.csv")
    utility.configure_pandas_display(pd)

    """
    'vL1', 'vL2', 'vL3', 'vL4', 'vL5',
    'vL6', 'vL7', 'vL8', 'vL9', 'vL10',
    'vL11', 'vL12', 'vL13', 'vL14', 'vL15'
    """

    try:
        bedrock.dimension_builder(
            tm1_service=tm1_service,
            dimension_name=dimension_name,
            input_datasource=file_path,
            input_format='indented_levels',
            build_strategy='rebuild',
            level_columns=[
                'L1', 'L2', 'L3', 'L4', 'L5',
                'L6', 'L7', 'L8', 'L9', 'L10',
                'L11', 'L12', 'L13', 'L14', 'L15'
            ],
            weight_column="vWeight",
            type_column="vType",
            attribute_parser="square_brackets_start"
        )
    finally:
        tm1_service.logout()


def context_metadata_basic_demo():
    tm1_service = create_tm1_connection('ks_academy')

    context_metadata = ContextMetadata(tm1_service=tm1_service)

    mdx = """
    SELECT
        {[}ElementAttributes_Period].[}ElementAttributes_Period].[year]}
    ON COLUMNS,
        {[Period].[Period].[202401]}
    ON ROWS
    FROM [}ElementAttributes_Period]
    """
    context_metadata.add_parameter_from_tm1(param_name="current_year",
                                            mdx_query=mdx)

    file_path = os.path.join(os.path.dirname(__file__), "test_template_render_basic.yaml")
    yaml_contents = context_metadata.render_template_yaml(yaml_path=file_path)
    print(yaml_contents)


def context_metadata_complete_demo():
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))
    tm1_service = create_tm1_connection('ks_academy')
    sql_engine = utility.create_sql_engine(**config['sqlparams_hrdemo'])

    data_source_path = os.path.join(os.path.dirname(__file__), "test_param_inputs.yaml")
    render_template_path = os.path.join(os.path.dirname(__file__), "test_template_render.yaml")

    context_metadata = ContextMetadata(
        tm1_service=tm1_service, sql_engine=sql_engine, path_to_init_yaml=data_source_path)

    result_dict = context_metadata.render_template_yaml(yaml_path=render_template_path)
    print(result_dict)


def copy_cube_structure_between_servers_demo():
    tm1srv_target = create_tm1_connection('hr_demo')
    tm1srv_source = create_tm1_connection('ks_academy')

    utility.configure_pandas_display(pd)

    dimension_name_1 = 'CubeCopyDemo1'
    dimension_name_2 = 'CubeCopyDemo2'
    dimension_name_3 = 'CubeCopyDemo3'
    dimension_name_4 = 'CubeCopyDemo4'
    file_path_1 = os.path.join(os.path.dirname(__file__), "dimension_builder_init.xlsx")
    file_path_2 = os.path.join(os.path.dirname(__file__), "dimension_builder_init.xlsx")
    cube_name_source = 'CubeCopyDemoSource'
    cube_name_target = 'CubeCopyDemoTarget'

    try:
        bedrock.dimension_builder(
            dimension_name=dimension_name_1,
            input_format='indented_levels',
            build_strategy='rebuild',
            tm1_service=tm1srv_source,
            level_columns=['Level1', 'Level2', 'Level3', 'Level4'],
            input_datasource=file_path_1,
            logging_level='DEBUG'
        )

        bedrock.dimension_builder(
            dimension_name=dimension_name_2,
            input_format='indented_levels',
            build_strategy='rebuild',
            tm1_service=tm1srv_source,
            level_columns=['Level1', 'Level2', 'Level3', 'Level4'],
            input_datasource=file_path_2,
            logging_level='DEBUG'
        )

        bedrock.cube_builder(
            tm1_service=tm1srv_source,
            cube_dimension_create_map={cube_name_source: [dimension_name_1, dimension_name_2]}
        )

        bedrock.cube_builder(
            tm1_service=tm1srv_target,
            build_mode='copy_from_source',
            if_cube_exist_strategy='rebuild',
            copy_source_tm1_service=tm1srv_source,
            copy_source_cubes=cube_name_source,
            copy_cube_rename_map={cube_name_source: cube_name_target},
            copy_dimension_rename_map={dimension_name_1: dimension_name_3, dimension_name_2: dimension_name_4},
            missing_dimension_strategy='copy_from_source',
            logging_level='DEBUG'
        )
    finally:
        tm1srv_source.logout()
        tm1srv_target.logout()


def mvm_demo(tm1srv_source, tm1srv_target, cube_list):
    # cube_list = ["TestCube1", "TestCube2", "TestCube3"]

    dim_list_unique = list(set([
        dim
        for cube_name in cube_list
        for dim in tm1srv_source.cubes.get_dimension_names(cube_name)
    ]))

    try:
        for dim in dim_list_unique:
            bedrock.dimension_copy(
                tm1_service=tm1srv_source,
                target_tm1_service=tm1srv_target,
                source_dimension_name=dim,
                allow_type_changes=True,
                logging_level='DEBUG'
            )

        for cube in cube_list:
            bedrock.cube_builder(
                tm1_service=tm1srv_target,
                build_mode='copy_from_source',
                if_cube_exist_strategy='rebuild',
                copy_source_tm1_service=tm1srv_source,
                copy_source_cubes=cube,
                logging_level='DEBUG'
            )

        for cube in cube_list:
            cube_mdx = utility.generate_dynamic_mdx_query_string(
                tm1_service=tm1srv_source, target_cube_name=cube)

            bedrock.data_copy_intercube(
                tm1_service=tm1srv_source,
                target_tm1_service=tm1srv_target,
                target_cube_name=cube,
                data_mdx=cube_mdx,
                skip_zeros=True,
                clear_target=True,
                target_clear_set_mdx_list=[],
                check_missing_elements=True,
                use_blob=True,
                logging_level='DEBUG'
            )

    finally:
        tm1srv_source.logout()
        tm1srv_target.logout()


def mdx_gen_demo():
    tm1srv_target = create_tm1_connection('hr_demo')

    mdx = utility.generate_dynamic_mdx_query_string(
        tm1_service=tm1srv_target, target_cube_name='Group Employee',
        dimension_filter_mapping={"Groups": ['SingleGroup']})
    print(mdx)


def attribute_structure_creation_demo():
    tm1srv_target = create_tm1_connection('hr_demo')

    attr_cols = ["testAttr1:String", "TestAttr2:Numeric"]
    attr_cube_name = "}ElementAttributes_attributeTest"
    dimension_name = "attributeTest"
    create_attribute_structure(
        tm1_service=tm1srv_target,
        attr_cols=attr_cols,
        attr_cube_name=attr_cube_name,
        dimension_name=dimension_name
    )


def input_handler_leaf_domain():
    tm1_service = create_tm1_connection('ks_academy')

    cube_name = "testbenchPrice"

    domain_coords = {
        "testbenchVersion": "Actual",
        "testbenchPeriod": "202401",
        "testbenchMeasurePrice": "Price",
        "testbenchProduct": "P0000001"
    }

    bedrock.input_handler(
        tm1_service=tm1_service,
        target_cube_name=cube_name,
        domain_coordinates=domain_coords,
        input_value=120
    )


def input_handler_repeat_on_children():
    tm1_service = create_tm1_connection('ks_academy')

    cube_name = "testbenchPrice"

    domain_coords = {
        "testbenchProduct": "ProductSubCategory01",
        "testbenchVersion": "Actual",
        "testbenchPeriod": "202401",
        "testbenchMeasurePrice": "Price",
    }

    bedrock.input_handler(
        tm1_service=tm1_service,
        target_cube_name=cube_name,
        domain_coordinates=domain_coords,
        input_value=110
    )


def input_handler_equal_spread_children():
    tm1_service = create_tm1_connection('ks_academy')

    cube_name = "testbenchPrice"

    domain_coords = {
        "testbenchProduct": "ProductSubCategory01",
        "testbenchVersion": "Actual",
        "testbenchPeriod": "202401",
        "testbenchMeasurePrice": "Price",
    }

    calculation_steps = [
        {
            "name": "TotalCells",
            "method": "count"
        },
        {
            "name": "FinalValue",
            "method": "formula",
            "formula": "Input / TotalCells"
        }
    ]

    bedrock.input_handler(
        tm1_service=tm1_service,
        target_cube_name=cube_name,
        domain_coordinates=domain_coords,
        input_value=100,
        calculation_steps=calculation_steps
    )


def input_handler_conditional():
    tm1_service = create_tm1_connection('ks_academy')

    cube_name = "testbenchPrice"

    domain_coords = {
        "testbenchProduct": "ProductSubCategory01",
        "testbenchVersion": "Actual",
        "testbenchPeriod": "202401",
        "testbenchMeasurePrice": "Price",
    }

    calculation_steps = [
        {
            "name": "TotalCells",
            "method": "count"
        },
        {
            "name": "FinalValue",
            "method": "formula",
            "formula": "Input / TotalCells"
        },
        {
            "name": "OnlyForProduct1",
            "method": "if",
            "if_then": {
                "testbenchProduct == 'P0000001'": "{{FinalValue}}",
                "testbenchProduct == 'P0000002'": 2
            },
            "fallback": 1
        }
    ]

    bedrock.input_handler(
        tm1_service=tm1_service,
        target_cube_name=cube_name,
        domain_coordinates=domain_coords,
        input_value=100,
        calculation_steps=calculation_steps
    )


if __name__ == '__main__':
    # complex_transform_demo()
    # tm1_to_sql_pyodbc_custom_writer_demo()
    # context_metadata_basic_demo()
    # context_metadata_complete_demo()
    # dimension_builder_basic_demo()
    # dimension_builder_no_edges_old_format()
    # dimension_builder_append_demo()
    # dimension_builder_complex_demo()
    # hierarchy_builder_demo()
    # build_cube_demo()
    # copy_dim_between_servers_demo()
    # copy_data_between_servers_demo()
    # copy_cube_structure_between_servers_demo()
    # mdx_gen_demo()
    # attribute_structure_creation_demo()
    # input_handler_leaf_domain()
    # input_handler_repeat_on_children()
    # input_handler_equal_spread_children()
    input_handler_conditional()
