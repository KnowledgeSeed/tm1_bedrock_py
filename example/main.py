from TM1py import TM1Service

from TM1_bedrock_py import utility


def manage():
    tm1_params = {
        "address": "localhost",
        "port": 5365,
        "user": "admin",
        "password": "",
        "ssl": False
    }

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

    tm1 = TM1Service(**tm1_params)

    try:
        print(utility.get_local_decimal_separator())

    finally:
        tm1.logout()


if __name__ == '__main__':
    manage()
