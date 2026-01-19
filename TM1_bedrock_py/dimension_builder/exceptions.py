from typing import Hashable

# custom error handling for dimension builder module


class DimensionNotFoundError(Exception):
    def __init__(self, dimension: str = ""):
        super().__init__("The dimension " + dimension + " does not exist.")


class DimensionAlreadyExistsError(Exception):
    def __init__(self, dimension: str = ""):
        super().__init__("The dimension " + dimension + " already exists.")


class HierarchyNotFoundError(Exception):
    def __init__(self, dimension: str = "", hierarchy: str = ""):
        super().__init__("The hierarchy " + hierarchy + " does not exist in the dimension " + dimension + ".")


class HierarchyAlreadyExistsError(Exception):
    def __init__(self, hierarchy: str = "", dimension: str = ""):
        super().__init__("The hierarchy " + hierarchy + " already exists in the dimension" + dimension + ".")


class InvalidInputFormatError(Exception):
    def __init__(self, expected_list: list = None, input_list: list = None):
        if expected_list is None: expected_list = []
        if input_list is None: input_list = []
        expected_string = ",".join(expected_list)
        input_string = ",".join(input_list)
        super().__init__("Input dataframe format is invalid. "
                         "Expeted format: ["+expected_string+"] Input format: ["+input_string+"]")


class SchemaValidationError(Exception):
    def __init__(self, message):
        super().__init__(message)


class GraphValidationError(Exception):
    def __init__(self, message):
        super().__init__(message)


class ElementTypeConflictError(Exception):
    def __init__(self, element, type_old, type_new):
        super().__init__("Cannot resolve type change of element "+element+" from "+type_old+" to "+type_new+"."
                         "Edit parameter is disabled.")


class LevelColumnInvalidRowError(Exception):
    def __init__(self, row_index: Hashable, error_type: str):
        super().__init__("Invalid row at index "+str(row_index)+"."
                         "Cause of error: "+error_type)
