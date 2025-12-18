from dataclasses import dataclass
from typing import Optional, Any, Literal, Dict

import yaml
from jinja2 import Environment, BaseLoader, StrictUndefined

from TM1_bedrock_py import extractor


# ------------------------------------------------------------------------------------------------------------
# Metadata: Context metadata collection using Jinja templating
# ------------------------------------------------------------------------------------------------------------

@dataclass
class ContextParameter:
    name: str
    type: Optional[Literal["dimension_element", "dimension_element_list"]]
    type_context: Optional[str]
    value: Any

    def __repr__(self) -> str:
        return f"ContextParameter({self.name}={self.value})"

    def __str__(self) -> str:
        return str(self.value)

    @property
    def as_raw_value(self) -> Any:
        return self.value

    @property
    def as_member_unique_name(self) -> str:
        if self.type != "dimension_element":
            raise TypeError("Parameter type must be dimension element")
        return f"[{self.type_context}].[{self.value}]"

    @property
    def as_set_mdx(self) -> str:
        if self.type not in ("dimension_element", "dimension_element_list"):
            raise TypeError("Parameter type must be dimension element or dimension element list")
        if self.type == "dimension_element":
            return f"{{[{self.type_context}].[{self.value}]}}"
        else:
            return "{" + ",".join(f"[{self.type_context}].[{str(e)}]" for e in self.value) + "}"


class ContextMetadata:
    def __init__(self,
                 sql_engine: Optional[Any] = None,
                 tm1_service: Optional[Any] = None,
                 path_to_init_yaml: Optional[str] = None):
        self._params: Dict[str, ContextParameter] = {}
        self._sql_engine = sql_engine
        self._tm1_service = tm1_service

        if path_to_init_yaml:
            self.load_init_yaml_to_parameters(path_to_init_yaml)

    def register_context_parameter(self, param: ContextParameter):
        self._params[param.name] = param

    def add_parameter(self, param_name: str, value: Any,
                      parameter_type: Optional[str] = None,
                      parameter_type_context: Optional[str] = None):
        if parameter_type == "dimension_element" and not isinstance(value, str):
            raise TypeError("Dimension element type parameters must be of string type")
        if parameter_type == "dimension_element_list" and not isinstance(value, list):
            raise TypeError("Dimension element list type parameters must be of List type")
        if parameter_type in ("dimension_element_list", "dimension_element") and parameter_type_context is None:
            raise ValueError("Must fill parameter type context for this type of parameter")

        new_parameter = ContextParameter(param_name, parameter_type, parameter_type_context, value)
        self.register_context_parameter(new_parameter)

    def add_parameter_from_sql(self, param_name: str, sql_query: str,
                               parameter_type: Optional[str] = None,
                               parameter_type_context: Optional[str] = None):
        if self._sql_engine is None:
            raise RuntimeError("Cannot execute SQL query: 'sql_engine' was not provided during initialization.")

        extracted_df = extractor.sql_to_dataframe(engine=self._sql_engine, sql_query=sql_query)

        if parameter_type == "dimension_element":
            value = extracted_df.iloc[0, 0]
        elif parameter_type == "dimension_element_list":
            value = extracted_df.iloc[:, 0].tolist()
        else:
            value = extracted_df.iloc[0, 0]

        self.add_parameter(param_name, value, parameter_type, parameter_type_context)

    def add_parameter_from_tm1(self, param_name: str, mdx_query: str,
                               parameter_type: Optional[str] = None,
                               parameter_type_context: Optional[str] = None):
        if self._tm1_service is None:
            raise RuntimeError("Cannot execute MDX query: 'tm1_service' was not provided during initialization.")

        extracted_df = extractor.tm1_mdx_to_dataframe(tm1_service=self._tm1_service, data_mdx=mdx_query)

        if parameter_type == "dimension_element":
            value = extracted_df["Value"][0]
        elif parameter_type == "dimension_element_list":
            value = extracted_df["Value"].tolist()
        else:
            value = extracted_df["Value"][0]

        self.add_parameter(param_name, value, parameter_type, parameter_type_context)

    def get(self, name: str) -> ContextParameter:
        return self._params[name]

    def get_value(self, name: str):
        return self._params[name].value

    def __contains__(self, name: str):
        return name in self._params

    def as_dict(self):
        return {name: vars(p) for name, p in self._params.items()}

    def as_value_dict(self) -> Dict[str, Any]:
        return {name: param.value for name, param in self._params.items()}

    def load_init_yaml_to_parameters(self, yaml_path: str):
        with open(yaml_path, "r") as f:
            parameters_init = yaml.safe_load(f)

        for param_name, param_data in parameters_init.items():
            param_type = param_data.get("type")
            param_type_context = param_data.get("type_context")
            if "value" in param_data:
                value = param_data["value"]
                self.add_parameter(param_name, value, param_type, param_type_context)
            elif "sql_query" in param_data:
                sql_query = param_data["sql_query"]
                self.add_parameter_from_sql(param_name, sql_query, param_type, param_type_context)
            elif "mdx_query" in param_data:
                mdx_query = param_data["mdx_query"]
                self.add_parameter_from_tm1(param_name, mdx_query, param_type, param_type_context)

    def render_template_yaml(self,
                             yaml_path: str,
                             variable_start_string: str = "{{",
                             variable_end_string: str = "}}"):
        with open(yaml_path, encoding="utf-8") as f:
            yaml_text = f.read()

        env = Environment(
            loader=BaseLoader(),
            variable_start_string=variable_start_string,
            variable_end_string=variable_end_string,
            undefined=StrictUndefined)
        template = env.from_string(yaml_text)
        rendered = template.render(**self._params)
        return yaml.safe_load(rendered)
