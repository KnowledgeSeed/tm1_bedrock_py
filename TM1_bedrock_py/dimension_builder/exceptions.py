from typing import Hashable

# custom error handling for dimension builder module


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


class MissingDimensionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class MissingHierarchyError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class DimensionAlreadyExistsError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class HierarchyAlreadyExistsError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
