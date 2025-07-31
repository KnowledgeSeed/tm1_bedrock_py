import json

class Rule:
    def __init__(self, area: str, full_statement: str, comment: str = ""):
        self.area = area
        self.full_statement = full_statement
        self.comment = comment

    def __eq__(self, other):
        if not isinstance(other, Rule):
            return NotImplemented
        return self.area == other.area and self.full_statement == other.full_statement and self.comment == other.comment

    def __hash__(self):
        return hash((self.area, self.full_statement, self.comment))