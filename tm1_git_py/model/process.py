import json
from typing import Any, List, Dict, TYPE_CHECKING
from TM1py import TM1Service, Process
from requests import Response

# Importáljuk a TI osztályt a típus-ellenőrzéshez (type hinting)
if TYPE_CHECKING:
    from model.ti import TI 
# {
#   "@type":"Process",
# 	"Name":"airflow_test_success",
# 	"HasSecurityAccess":false,
# 	"Code@Code.link":"airflow_test_success.ti",
# 	"DataSource":
# 	{
# 		"Type":"None"
# 	},
# 	"Parameters":[],
# 	"Variables":[]
# }

class Process:
    def __init__(self, name, hasSecurityAccess, code_link, datasource, parameters, variables, ti):
        self.type = 'Process'
        self.name = name
        self.hasSecurityAccess = hasSecurityAccess
        self.code_link = code_link
        self.datasource = datasource
        self.parameters = parameters
        self.variables = variables
        self.ti = ti

    # def __init__(self, name: str, hasSecurityAccess: bool, parameters: List[Dict], variables: List[Dict], data_source: Dict, ti: 'TI', code_link: str):
    #     self.name = name
    #     self.hasSecurityAccess = hasSecurityAccess
    #     self.parameters = parameters
    #     self.variables = variables
    #     self.code_link = code_link

    #     self.data_source_type = data_source.get('Type')
    #     self.data_source_name = data_source.get('Name')

    #     self.ti = ti

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "HasSecurityAccess": self.hasSecurityAccess,
            "Code@Code.link": self.code_link,
            "DataSource": {"Type": "None"},
            "Parameters": self.parameters,
            "Variables": self.variables
        }, indent='\t')
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Process):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self) -> int:
        return hash((
            self.name,
            self.hasSecurityAccess,
            #self.data_source_type,
            #self.data_source_name,
            self.datasource,
            json.dumps(self.parameters, sort_keys=True),
            json.dumps(self.variables, sort_keys=True),
            self.ti
        ))

    def to_dict(self):
        return {
            'name': self.name,
            'has_security_access': self.hasSecurityAccess,
            #'data_source_type': self.data_source_type,
            #'data_source_name': self.data_source_name,
            'datasource' : self.datasource,
            'parameters': self.parameters,
            'variables': self.variables,
            'ti': self.ti.to_dict()
        }

    @staticmethod
    def as_link(name : str):
        # /processes/Process_A.json
        return '/processes/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_process(tm1_service: TM1Service, process: Process) -> Response:
    process_object = Process(
        name=process.name,
        has_security_access=process.hasSecurityAccess,
        datasource_type=process.datasource.get('Type'),
        parameters=process.parameters,
        variables=process.variables
    )
    return tm1_service.processes.create(process_object)


def update_process(tm1_service: TM1Service, process: Dict[str, Any]) -> Response:
    process_new = process.get('new')
    process_object_new = Process(
        name=process_new.name,
        has_security_access=process_new.hasSecurityAccess,
        datasource_type=process_new.datasource.get('Type'),
        parameters=process_new.parameters,
        variables=process_new.variables
    )

    if process.get('new').name == process.get('old').name:
        return tm1_service.processes.update(process_object_new)
    else:
        process_old = process.get('old')
        process_object_temp = Process(
            name=process_new.name,
            has_security_access=process_old.hasSecurityAccess,
            datasource_type=process_old.datasource.get('Type'),
            parameters=process_old.parameters,
            variables=process_old.variables
        )
        response = tm1_service.processes.create(process_object_temp)
        if response.status_code == 200:
            tm1_service.processes.delete(process_old.name)

        return tm1_service.processes.update(process_object_new)


def delete_process(tm1_service: TM1Service, process: Process) -> Response:
    return tm1_service.processes.delete(process.name)
