from datetime import datetime, timedelta

from formencode import validators as fev
from formencode import schema as fes
from formencode import foreach as fef

class DateTime(fev.FancyValidator):

    def _to_python(self, value, state=None):
        if '.' in value:
            iso, frac_s = value.split('.')
        else:
            iso, frac_s = value, '0'
        us = float('0.' + frac_s) * 1e6
        ts = datetime.strptime(iso, '%Y-%m-%d %H:%M:%S')
        return ts + timedelta(microseconds=us)


message_schema = fes.Schema(
    priority=fev.Int(if_empty=10, if_missing=10),
    delay=fev.Int(if_missing=0),
    timeout=fev.Int(if_missing=300),
    tags=fef.ForEach(fev.UnicodeString, convert_to_list=True, if_missing=[]))

get_schema = fes.Schema(
    client=fev.UnicodeString(required=True),
    count=fev.Int(if_missing=1),
    timeout=fev.Int(if_missing=0))

retry_schema = fes.Schema(
    delay=fev.Int(if_empty=5, if_missing=5))
