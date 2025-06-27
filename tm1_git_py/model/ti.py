
class TI:
    def __init__(self, prolog_procedure, metadata_procedure, data_procedure, epilog_procedure):
        self.prolog_procedure = prolog_procedure
        self.metadata_procedure = metadata_procedure
        self.data_procedure = data_procedure
        self.epilog_procedure = epilog_procedure

    @classmethod
    def get_string_between(cls, text: str, start_keyword, end_keyword):
        start_idx = text.find(start_keyword)
        end_idx = text.find(end_keyword, start_idx)

        if start_idx != -1 and end_idx != -1:
            start_idx += len(start_keyword)
            result = text[start_idx:end_idx]
            return result
        else:
            print("Keywords not found")

    def __eq__(self, other):
        if not isinstance(other, TI):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __hash__(self):
        return hash(tuple(sorted(self.to_dict().items())))

    def to_dict(self):
        return {
            'prolog_procedure': self.prolog_procedure,
            'metadata_procedure': self.metadata_procedure,
            'data_procedure': self.data_procedure,
            'epilog_procedure': self.epilog_procedure,
        }

    @classmethod
    def from_string(cls, ti):
        return TI(
            cls.get_string_between(ti, '#region Prolog', '#endregion').strip(),
            cls.get_string_between(
                ti, '#region Metadata', '#endregion').strip(),
            cls.get_string_between(ti, '#region Data', '#endregion').strip(),
            cls.get_string_between(ti, '#region Epilog', '#endregion').strip())

    def ti_as_string(self):
        ti = "#region Prolog\r\n{}\r\n#endregion\r\n".format(self.prolog_procedure) \
            + "#region Metadata\r\n{}\r\n#endregion\r\n".format(self.metadata_procedure) \
            + "#region Data\r\n{}\r\n#endregion\r\n".format(self.data_procedure) \
            + "#region Epilog\r\n{}\r\n#endregion\r\n".format(self.epilog_procedure)
        return ti
