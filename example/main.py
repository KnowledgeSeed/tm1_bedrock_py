from TM1py import TM1Service
from TM1_bedrock_py import bedrock


def tm1_bedrock_py_demo():
    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

    target_cube_name = "Sales Reporting"
    data_mdx = """
    SELECT
    NON EMPTY
        {TM1FILTERBYLEVEL( {TM1SUBSETALL([Period].[Period])} , 0)}
    ON COLUMNS,
    NON EMPTY
        {[Lineitem Sales].[Lineitem Sales].Members}
        * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Currency].[Currency])} , 0)}
        * {TM1FILTERBYLEVEL( {TM1SUBSETALL([Product].[Product])} , 0)}
    ON ROWS
    FROM [Sales]
    WHERE (
        [Version].[Version].[Actual],
        [Organization Unit].[Organization Unit].[Group],
        [Employee].[Employee].[All Employee],
        [Measures Sales].[Measures Sales].[Corrected Value] )
    """
    mapping_steps = [{
        "method": "replace",
        "mapping": {"Version": {"Actual": "Actual Reporting"}}
    }]
    source_dim_mapping = {
        "Employee": "All Employee",
        "Organization Unit": "Group"
    }
    related_dimensions = {
        "Lineitem Sales": "Lineitem Sales Reporting"
    }

    clear_target = True
    target_clear_set_mdx_list = ["{[Version].[Version].[Actual Reporting]}"]
    logging_level = "DEBUG"
    use_blob = True

    try:
        bedrock.data_copy_intercube( target_cube_name=target_cube_name,
                                     tm1_service=tm1_service,
                                     data_mdx=data_mdx,
                                     mapping_steps=mapping_steps,
                                     source_dim_mapping=source_dim_mapping,
                                     related_dimensions=related_dimensions,
                                     clear_target=clear_target,
                                     target_clear_set_mdx_list=target_clear_set_mdx_list,
                                     use_blob=use_blob,
                                     logging_level=logging_level)
    finally:
        tm1_service.logout()


if __name__ == '__main__':
    tm1_bedrock_py_demo()
