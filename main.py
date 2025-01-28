import tm1_bedrock
import developer_test_files.tm1_connection
from TM1py import TM1Service
from TM1py.Objects.Element import Element


def manage():
    tm1_params = developer_test_files.tm1_connection.params
    target_cube_name = "Cost and FTE by Groups"
    target_version_name = 'TM1py Test Version'

    data_mdx = """
        SELECT 
           {[Periods].[Periods].[202301],[Periods].[Periods].[202302],[Periods].[Periods].[202303],[Periods].[Periods].[202304],[Periods].[Periods].[202305],[Periods].[Periods].[202306],[Periods].[Periods].[202307],[Periods].[Periods].[202308],[Periods].[Periods].[202309],[Periods].[Periods].[202310],[Periods].[Periods].[202311],[Periods].[Periods].[202312]} 
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
           {[Periods].[Periods].[202301],[Periods].[Periods].[202302],[Periods].[Periods].[202303],[Periods].[Periods].[202304],[Periods].[Periods].[202305],[Periods].[Periods].[202306],[Periods].[Periods].[202307],[Periods].[Periods].[202308],[Periods].[Periods].[202309],[Periods].[Periods].[202310],[Periods].[Periods].[202311],[Periods].[Periods].[202312]} 
          ON COLUMNS , 
           {[Groups].[Groups].Members}
           * {[Employees].[Employees].Members} 
          ON ROWS 
        FROM [Cost and FTE by Groups] 
        WHERE 
          (
           [Versions].[Versions].[TM1py Test Version], 
           [Lineitems Cost and FTE by Groups].[Lineitems Cost and FTE by Groups].[FTE],
           [Measures Cost and FTE by Groups].[Measures Cost and FTE by Groups].[Value]
          )
         """
    literal_mapping = {
        "Versions": {"Base Plan": target_version_name}
    }

    clear_set_mdx = """{[Versions].[TM1py Test Version]}"""

    tm1 = TM1Service(**tm1_params)

    try:
        """
        target_version_element_object = Element(name='TM1 Test Version n2', element_type=Element.Types.NUMERIC, index=9)
        tm1.elements.create('Versions', 'Versions', target_version_element_object)
        print(tm1.elements.get_element_names('Versions', 'Versions'))
        """


        df = tm1_bedrock.mdx_to_dataframe_ordered(
            tm1,
            data_mdx,
            skip_zeros=True,
            skip_consolidated_cells=True
        )
        df.to_csv('developer_test_files/original_data.csv', index=False)

        df_remapped = tm1_bedrock.mdx_to_dataframe_with_literal_remap(
            tm1,
            data_mdx,
            literal_mapping,
            skip_zeros=True,
            skip_consolidated_cells=True
        )
        df_remapped.to_csv('developer_test_files/mapped_data.csv', index=False)

        df_mapping_target = tm1_bedrock.mdx_to_dataframe_ordered(
            tm1,
            mapping_target_data_mdx,
            skip_zeros=True,
            skip_consolidated_cells=True
        )
        print(df_remapped.shape)
        print(df_remapped.index)
        print(df_remapped.columns)
        df_mapping_target.to_csv('developer_test_files/mapping_target_data.csv', index=False)
        
        tm1_bedrock.dataframe_to_cube_with_clear(tm1, df_remapped, target_cube_name, True, 'default')

    finally:
        tm1.logout()


if __name__ == '__main__':
    manage()
