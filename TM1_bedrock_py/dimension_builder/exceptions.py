from typing import Hashable

# custom error handling for dimension builder module


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
    def __init__(self, message):
        super().__init__(message)


class InvalidAttributeColumnNameError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class InvalidInputParameterError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class InvalidLevelColumnRecordError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
