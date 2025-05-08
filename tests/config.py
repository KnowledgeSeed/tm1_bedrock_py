import configparser
import itertools
import os
from pathlib import Path

import pytest
from TM1py import TM1Service
from TM1py.Exceptions import TM1pyRestException
#from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError, InterfaceError, ArgumentError

from TM1_bedrock_py import utility, basic_logger

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
    #"{TM1FILTERBYLEVEL({TM1DRILLDOWNMEMBER({[testbenchPeriod].[testbenchPeriod].[All Periods]}, {[testbenchPeriod].[testbenchPeriod].[All Periods]}, RECURSIVE )}, 0)}"
    "{EXCEPT({TM1FILTERBYLEVEL({TM1DRILLDOWNMEMBER({[testbenchPeriod].[testbenchPeriod].[All Periods]}, {[testbenchPeriod].[testbenchPeriod].[All Periods]}, RECURSIVE )}, 0)},{[testbenchPeriod].[testbenchPeriod].[202401],[testbenchPeriod].[testbenchPeriod].[202402],[testbenchPeriod].[testbenchPeriod].[202403],[testbenchPeriod].[testbenchPeriod].[202404],[testbenchPeriod].[testbenchPeriod].[202405],[testbenchPeriod].[testbenchPeriod].[202406],[testbenchPeriod].[testbenchPeriod].[202407],[testbenchPeriod].[testbenchPeriod].[202408],[testbenchPeriod].[testbenchPeriod].[202409],[testbenchPeriod].[testbenchPeriod].[202410],[testbenchPeriod].[testbenchPeriod].[202411],[testbenchPeriod].[testbenchPeriod].[202412],[testbenchPeriod].[testbenchPeriod].[202501],[testbenchPeriod].[testbenchPeriod].[202502],[testbenchPeriod].[testbenchPeriod].[202503]})}"
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


def benchmark_testcase_parameters():
    num_runs = 5
    identical_run_ids = [i for i in range(num_runs)]
    number_of_cores = [4, 8]
    number_of_records = [10000, 50000, 100000, 500000]
    combinations = list(itertools.product(number_of_cores, number_of_records, identical_run_ids))

    return combinations


SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'schema')


@pytest.fixture(scope="session")
def tm1_connection():
    """Creates a TM1 connection before tests and closes it after all tests."""
    load_dotenv()
    tm1 = None
    try:
        tm1 = TM1Service(
            address=os.environ.get("TM1_ADDRESS"),
            port=os.environ.get("TM1_PORT"),
            user=os.environ.get("TM1_USER"),
            password="",
            ssl=os.environ.get("TM1_SSL")
        )
        basic_logger.debug("Successfully connected to TM1.")
        yield tm1

    except (TM1pyRestException, TypeError):
        try:
            config = configparser.ConfigParser()
            config.read(Path(__file__).parent.joinpath('config.ini'))
            tm1 = TM1Service(**config['tm1srv'])
            basic_logger.debug("Successfully connected to TM1.")
            yield tm1
        except TM1pyRestException:
            basic_logger.error("Unable to connect to TM1: ", exc_info=True)
    finally:
        if tm1 is not None:
            tm1.logout()
            basic_logger.debug("Connection closed.")


@pytest.fixture(scope="session")
def sql_engine():
    """Creates a SQL connector engine before tests and closes it after all tests."""
    #load_dotenv()
    engine = None
    try:
        engine = utility.create_sql_engine(
            username=os.environ.get("SQL_USER"),
            password=os.environ.get("SQL_PASSWORD"),
            host=os.environ.get("SQL_HOST"),
            port=os.environ.get("SQL_PORT"),
            connection_type=os.environ.get("SQL_CONN_TYPE"),
            database=os.environ.get("SQL_DB")
        )
        basic_logger.debug("SQL engine successfully created")
        yield engine

    except ArgumentError:
        try:
            config = configparser.ConfigParser()
            config.read(Path(__file__).parent.joinpath('config.ini'))
            engine = utility.create_sql_engine(**config['mssqlsrv'])
            basic_logger.debug("SQL engine successfully created")
            yield engine
        except OperationalError or InterfaceError:
            basic_logger.error("Unable to connect to SQL: ", exc_info=True)
    finally:
        if engine is not None:
            engine.dispose()
            basic_logger.debug("SQL engine disposed.")
