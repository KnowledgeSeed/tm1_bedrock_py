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


def csrd_demo():
    tm1_params = {
        "address": "localhost",
        "port": 5382,
        "user": "IM",
        "password": "Washing2-Implosive-Nacho",
        "ssl": False
    }
    tm1 = TM1Service(**tm1_params)
    try:
        version_source = "Actual"
        version_target = "Actual"
        year_source = "2032"
        year_target = "2025"
        entity_source = "Entity NA"
        entity_target = "Entity NA"
        measures_list = """
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[ESRS Main Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[Materiality Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[SDG Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[GRI Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[Location Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[Supplier Relvant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[CSRD Input  Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[DMA Relevant]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[Materiality Assessment]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[SDG]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[GRI]},
            {[Analogic ESRS Mapping Measure].[Analogic ESRS Mapping Measure].[Driver]}
            """

        pattern = r'{(.*?)}'
        measures_list_of_strings = re.findall(pattern, measures_list)
        data_mdx_list = []
        for element_string in measures_list_of_strings:

            data_mdx = f"""
                       SELECT
                            NON EMPTY
                            {{{element_string}}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([ESRS Main]), 0)}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([ESRS Details 1]), 0)}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([ESRS Details 2]), 0)}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([ESRS Geography]), 0)}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([Custom 1]), 0)}}
                            *{{Tm1FilterByLevel(Tm1SubsetAll([Custom 2]), 0)}}
                       ON 0
                       FROM [Analogic ESRS Mapping]
                       WHERE (
                           [Year].[Year].[{year_source}],
                           [Entity].[Entity].[{entity_source}],
                           [Version].[Version].[{version_source}]
                       )
                       """
            data_mdx_list.append(data_mdx)

        data_mdx = f"""
        SELECT
            NON EMPTY
            {{TM1FilterByLevel(Tm1SubsetAll([Custom 1]), 0)}}
            *{{Tm1SubsetAll([ESRS Main])}}
            *{{TM1FilterByLevel(Tm1SubsetAll([ESRS Details 1]), 0)}}
            *{{TM1FilterByLevel(Tm1SubsetAll([ESRS Details 2]), 0)}}
            *{{TM1FilterByLevel(Tm1SubsetAll([ESRS Geography]), 0)}}
            *{{TM1FilterByLevel(Tm1SubsetAll([Custom 2]), 0)}}
            *{{{measures_list}}}
        ON 0
        FROM[Analogic ESRS Mapping]
        WHERE(
            [Year].[Year].[{year_source}],
            [Entity].[Entity].[{entity_source}],
            [Version].[Version].[{version_source}]
        )
        """

        mapping_steps = [
            {
                "method": "replace",
                "mapping": {
                    "Version": {version_source: version_target},
                    "Entity": {entity_source: entity_target},
                    "Year": {year_source: year_target}
                }
            }
        ]

        skip_zeros = True
        skip_consolidated_cells = False
        async_write = True
        clear_target = True
        clear_set_mdx_list = [f'{{[Version].[{version_target}]}}', f'{{[Entity].[{entity_target}]}}',
                              f'{{[Year].[{year_target}]}}']

        bedrock.data_copy(
            tm1_service=tm1,
            data_mdx=data_mdx,
            mapping_steps=mapping_steps,
            skip_zeros=skip_zeros,
            skip_consolidated_cells=skip_consolidated_cells,
            clear_target=clear_target,
            async_write=async_write,
            logging_level="DEBUG",
            target_clear_set_mdx_list=clear_set_mdx_list,
            slice_size_of_dataframe=50000
        )

    finally:
        tm1.logout()


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
    # manage()
    # csrd_demo()
    benchpy_sample()
