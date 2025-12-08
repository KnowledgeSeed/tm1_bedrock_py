from TM1py import TM1Service
from TM1_bedrock_py import bedrock


def tm1_bedrock_py_demo():

    # source: Sales data -> on real existing employee-orgunit assignment -> Actual version, Sales cube
    # we're interested how the data would look after company restructuring -> on Budget version

    # helper mapping cube -> dimensions: version, employee, orgunit, measure
    # different company structure ( employee-orgunit assignment ) on different versions (Actual, Budget)

    # the task: sales cube query from source version (Actual)
    #           version rewrite from source to target (Budget)
    #           data restructuring with the help of the mapping cube, on version, employee
    #           writing the transformed data back to the cube

    tm1_params = {
        "address": "dev.knowledgeseed.local",
        "port": 5379,
        "user": "testbench",
        "password": "testbench",
        "ssl": False
    }
    tm1_service = TM1Service(**tm1_params)

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
        [Measures Sales].[Measures Sales].[Corrected Value] )
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
    target_clear_set_mdx_list = ["{[Version].[Version].[Budget]}", "{[Period].[Period].[202406]}"]

    logging_level = "DEBUG"
    use_blob = True

    try:
        bedrock.data_copy(tm1_service=tm1_service,
                          data_mdx=data_mdx,
                          mapping_steps=mapping_steps,
                          clear_target=clear_target,
                          target_clear_set_mdx_list=target_clear_set_mdx_list,
                          use_blob=use_blob,
                          logging_level=logging_level)
    finally:
        tm1_service.logout()


if __name__ == '__main__':
    tm1_bedrock_py_demo()

