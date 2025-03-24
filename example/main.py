from TM1py import TM1Service
import pprint
from TM1_bedrock_py import utility, extractor, transformer, loader
from TM1_bedrock_py.transformer import normalize_table_source_dataframe


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

        """
        loader.input_relative_proportional_spread(
            tm1_service=tm1,
            value=100000,
            mdx=mdx,
            reference_unique_element_names=[
                "[Groups].[Groups].[Total Groups]",
                "[Employees].[Employees].[Total Employees]",
                "[Periods].[Periods].[202307]",
                "[Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[Caculated Salary]",
                "[Versions].[Versions].[Base Plan]",
                "[Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Value]"
            ],
            cube="Cost and FTE by Groups"
        )
        """

        mdx = """
        SELECT 
           {[Groups].[Groups].[Total Groups],[Groups].[Groups].[Total Groups^Group_1],[Groups].[Groups].[Total Groups^Group_2],[Groups].[Groups].[Total Groups^Group_3],[Groups].[Groups].[Group_4],[Groups].[Groups].[Group_5]} 
          ON COLUMNS , 
           {DRILLDOWNMEMBER({[Employees].[Employees].[Total Employees]}, {[Employees].[Employees].[Total Employees]})} 
          ON ROWS 
        FROM [Cost and FTE by Groups] 
        WHERE 
          (
           [Periods].[Periods].[202307],
           [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[Caculated Salary],
           [Versions].[Versions].[Bedrock Input Test],
           [Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Input]
          )
        """

        loader.input_repeat_value(
            tm1_service=tm1,
            value=5555,
            mdx=mdx,
            cube="Cost and FTE by Groups"
        )


    finally:
        tm1.logout()



if __name__ == '__main__':
    manage()
