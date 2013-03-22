import logging
import cPickle as pickle

import bson

from ming import Session

doc_session = Session.by_name('chapman')
log = logging.getLogger(__name__)

def notify(key, data):
    log.debug('notify(%r, %r)', key, data)

def dumps(value):
    if value is None: return value
    return bson.Binary(pickle.dumps(value))

def loads(value):
    return pickle.loads(value)

class pickle_property(object):
    def __init__(self, pname):
        self._pname = pname

    def __get__(self, obj, type=None):
        if obj is None: return self
        return loads(getattr(obj, self._pname))

    def __set__(self, obj, value):
        setattr(obj, self._pname, dumps(value))
                
            
    
