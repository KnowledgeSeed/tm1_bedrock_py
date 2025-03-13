from json_logging import JSONLogFormatter as BaseJSONLogFormatter

def _sanitize_log_msg(record):
    return record.getMessage().replace('\n', '_').replace('\r', '_').replace('\t', '_')

class JSONLogFormatter(BaseJSONLogFormatter):
    def _format_log_object(self, record, request_util):
        json_log_object = super(BaseJSONLogFormatter, self)._format_log_object(record, request_util)
        props = {}

        for key in record.request:
            props[key] = record.request[key]

        json_log_object.update(props)
        if hasattr(record, 'props'):
            json_log_object.update(record.props)

        if record.exc_info or record.exc_text:
            json_log_object.update(self.get_exc_fields(record))

        return json_log_object