import json

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
