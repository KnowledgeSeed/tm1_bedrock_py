from TM1py import TM1Service
import pprint
from TM1_bedrock_py import utility, extractor, transformer, loader, bedrock
from TM1_bedrock_py.transformer import normalize_table_source_dataframe
from string import Template
from tm1_bench_py import tm1_bench, df_generator_for_dataset, dimension_builder, dimension_period_builder
import re
import os



def manage():
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

    sql_table_name = "Employee Group Mapping"

    data_mdx = """
        SELECT 
           {[Periods].[Periods].[202301],[Periods].[Periods].[202302],[Periods].[Periods].[202303],
           [Periods].[Periods].[202304],[Periods].[Periods].[202305],[Periods].[Periods].[202306],
           [Periods].[Periods].[202307],[Periods].[Periods].[202308],[Periods].[Periods].[202309],
           [Periods].[Periods].[202310],[Periods].[Periods].[202311],[Periods].[Periods].[202312]}  
          ON COLUMNS , 
           {[Groups].[Groups].Members}
           * {[Employees].[Employees].Members} 
          ON ROWS 
        FROM [Cost and FTE by Groups] 
        WHERE 
          (
           [Versions].[Versions].[Base Plan], 
           [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[FTE],
           [Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Value]
          )
         """

    mapping_target_data_mdx = """
        SELECT 
           {[Periods].[Periods].[202301],[Periods].[Periods].[202302],[Periods].[Periods].[202303],
           [Periods].[Periods].[202304],[Periods].[Periods].[202305],[Periods].[Periods].[202306],
           [Periods].[Periods].[202307],[Periods].[Periods].[202308],[Periods].[Periods].[202309],
           [Periods].[Periods].[202310],[Periods].[Periods].[202311],[Periods].[Periods].[202312]} 
          ON COLUMNS , 
           {[Groups].[Groups].Members}
           * {[Employees].[Employees].Members} 
          ON ROWS 
        FROM [Cost and FTE by Groups] 
        WHERE 
          (
           [Versions].[Versions].[TM1py Test Version], 
           [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[FTE],
           [Measures Cost and FTE by Group].[Measures Cost and FTE by Groups].[Value]
          )
         """

    literal_mapping = {
        "Versions": {"Base Plan": "TM1py Test Version"}
    }
    cube_name = "Cost and FTE by Groups"

    clear_set_mdx_list = ["{[Versions].[TM1py Test Version]}",
                          "{[Periods].[Periods].[2023].Children}"]

    """
    sql = utility.create_sql_engine(**sql_params)
    columninfo = utility.inspect_table(sql, "Write Test Table")
    print(columninfo)
    """
    tm1 = TM1Service(**tm1_params)


    """
    
    SELECT 
       {[Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Value],[Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Input]} 
      ON COLUMNS , 
       {[Versions].[Versions].[Base Plan],[Versions].[Versions].[Bedrock Input Test]} 
      ON ROWS 
    FROM [Cost and FTE by Groups] 
    WHERE 
      (
       [Periods].[Periods].[202307],
       [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[Caculated Salary],
       [Employees].[Employees].[Total Employees],
       [Groups].[Groups].[Total Groups]
      )
    
    
    unique_element_names=[
                "[Groups].[Groups].[Total Groups]",
                "[Employees].[Employees].[Total Employees]",
                "[Periods].[Periods].[202307]",
                "[Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[Caculated Salary]",
                "[Versions].[Versions].[Bedrock Input Test]",
                "[Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Input]"
            ],
    
    
    """

    try:
        mdx = """
            SELECT 
               {[Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Input]} 
              ON COLUMNS , 
               {[Versions].[Versions].[Bedrock Input Test]} 
              ON ROWS 
            FROM [Cost and FTE by Groups] 
            WHERE 
              (
               [Periods].[Periods].[202307],
               [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[Caculated Salary],
               [Employees].[Employees].[Total Employees],
               [Groups].[Groups].[Total Groups]
              )
              """
        try:
            all_server_files = tm1.files.get_all_names()
            print("Files found via TM1 REST API:")
            print(all_server_files)
            # Check if your expected file name (e.g., "your_unique_name.csv") is in the list
        except Exception as e:
            print(f"Error listing files via API: {e}")
    finally:
        tm1.logout()


def test_nativeview_functions():
    set_mdx_list = ['{{[Period].[Period].[202406]}}', '{{[Product].[Product].[P0000001],[Product].[Product].[P0000004],[Product].[Product].[P0000005],[Product].[Product].[P0000007],[Product].[Product].[P0000008],[Product].[Product].[P0000009],[Product].[Product].[P0000012],[Product].[Product].[P0000014],[Product].[Product].[P0000015],[Product].[Product].[P0000017],[Product].[Product].[P0000019],[Product].[Product].[P0000023]}}', '{{[Employee].[Employee].[Employee1],[Employee].[Employee].[Employee8],[Employee].[Employee].[Employee35],[Employee].[Employee].[Employee56],[Employee].[Employee].[Employee81],[Employee].[Employee].[Employee87],[Employee].[Employee].[Employee99]}}', '{[Version].[Version].[Actual]}', '{[Currency].[Currency].[LC]}', '{[MeasuresSales].[MeasuresSales].[Input]}', '{[OrganizationUnit].[OrganizationUnit].[Company01]}', '{[LineitemSales].[LineitemSales].[Quantity]}']
    mdx = """
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
    """
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


if __name__ == '__main__':
    test_nativeview_functions()
