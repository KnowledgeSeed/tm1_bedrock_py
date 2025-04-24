import itertools

_SOURCE_VERSION = "Actual"
_TARGET_VERSION = "ForeCast"
TARGET_CUBE_NAME = "testbenchPnL"

DATA_MDX_TEMPLATE = f"""
SELECT 
  NON EMPTY 
   {{TM1SUBSETALL([testbenchMeasureSales].[testbenchMeasureSales])}}
   * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchProduct].[testbenchProduct])}}, 0)}} 
   * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchCustomer].[testbenchCustomer])}}, 0)}} 
   * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchKeyAccountManager].[testbenchKeyAccountManager])}}, 0)}} 
  ON 0
FROM [testbenchSales] 
WHERE 
  (
   [testbenchVersion].[testbenchVersion].[{_SOURCE_VERSION}],
   [testbenchPeriod].[testbenchPeriod].[$testbenchPeriod]
  )
"""

CLEAR_PARAM_TEMPLATES = [
    f"{{[testbenchVersion].[testbenchVersion].[{_TARGET_VERSION}]}}",
    "{[testbenchPeriod].[testbenchPeriod].[$testbenchPeriod]}"
]

PARAM_SET_MDX_LIST = [
    "{TM1FILTERBYLEVEL({TM1DRILLDOWNMEMBER({[testbenchPeriod].[testbenchPeriod].[All Periods]}, {[testbenchPeriod].[testbenchPeriod].[All Periods]}, RECURSIVE )}, 0)}"
]

_STEP1 = {
    "method": "map_and_join",
    "mapping_mdx": f"""
        SELECT 
        NON EMPTY 
        {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchProduct].[testbenchProduct])}}, 0)}}
        * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchAccount].[testbenchAccount])}}, 0)}}
        * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchMeasureSales].[testbenchMeasureSales])}}, 0)}}
        ON 0
        FROM [testbenchMappingProductToAccount] 
        WHERE 
        (
        [testbenchVersion].[testbenchVersion].[{_SOURCE_VERSION}],
        [testbenchMeasureMappingProductToAccount].[testbenchMeasureMappingProductToAccount].[Assign Flag]
        )""",
    "joined_columns": ["testbenchAccount"],
    "dropped_columns": ["testbenchProduct", "testbenchMeasureSales"]
}

_STEP2 = {
    "method": "map_and_join",
    "mapping_mdx": f"""
        SELECT 
        NON EMPTY 
        {{[testbenchMeasureMappingKeyAccountManagerToOrganizationUnit].[Assign Flag]}} 
        * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchKeyAccountManager].[testbenchKeyAccountManager])}}, 0)}}
        * {{TM1FILTERBYLEVEL({{TM1SUBSETALL([testbenchOrganizationUnit].[testbenchOrganizationUnit])}}, 0)}} 
        * {{TM1FILTERBYLEVEL({{TM1DRILLDOWNMEMBER({{[testbenchPeriod].[testbenchPeriod].[All Periods]}}, {{[testbenchPeriod].[testbenchPeriod].[All Periods]}}, RECURSIVE )}}, 0)}}
        ON 0 
        FROM [testbenchMappingKeyAccountManagerToOrganizationUnit] 
        WHERE 
        (
        [testbenchVersion].[testbenchVersion].[{_SOURCE_VERSION}]
        )""",
    "joined_columns": ["testbenchOrganizationUnit"],
    "dropped_columns": ["testbenchKeyAccountManager", "testbenchCustomer"]
}

_STEP3 = {
    "method": "replace",
    "mapping": {"testbenchVersion": {_SOURCE_VERSION: _TARGET_VERSION}}
}

MAPPING_STEPS = [_STEP1, _STEP2, _STEP3]

TARGET_DIM_MAPPING = {"testbenchMeasurePnL": "Calculated from Sales"}


def testcase_parameters():
    num_runs = 1
    identical_run_ids = [i for i in range(num_runs)]
    number_of_cores = [8]
    number_of_records = [10000]
    combinations = list(itertools.product(number_of_cores, number_of_records, identical_run_ids))

    return combinations


SCHEMA_DIR = 'C:\\Users\\ullmann.david\\PycharmProjects\\tm1bedrockpy\\schema'
