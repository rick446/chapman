from datetime import datetime
import bson


def default_json(x):
    if isinstance(x, datetime):
        return x.isoformat(' ')
    elif hasattr(x, '__json__'):
        return x.__json__()
    elif isinstance(x, bson.ObjectId):
        return str(x)
    elif isinstance(x, dict):
        return dict(
            (k, default_json(v))
            for k, v in x.iteritems())
    elif isinstance(x, list):
        return [default_json(v) for v in x]
    return x

