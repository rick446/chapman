from datetime import datetime, timedelta

from formencode import validators as fev
from formencode import schema as fes


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
    after=DateTime(if_empty=None, if_missing=None),
    timeout=fev.Int(if_missing=300))

get_schema = fes.Schema(
    client=fev.UnicodeString(required=True),
    timeout=fev.Int(if_missing=0))
