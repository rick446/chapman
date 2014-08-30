import logging
import cPickle as pickle

import bson
from pymongo.cursor import _QUERY_OPTIONS

from ming import Session
from mongotools.util import LazyProperty
from mongotools.pubsub import Channel

doc_session = Session.by_name('chapman')
log = logging.getLogger(__name__)


def dumps(value):
    if value is None:
        return value
    return bson.Binary(pickle.dumps(value))


def loads(value):
    return pickle.loads(value)


class pickle_property(object):
    def __init__(self, pname):
        self._pname = pname

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        return loads(getattr(obj, self._pname))

    def __set__(self, obj, value):
        setattr(obj, self._pname, dumps(value))


class ChannelProxy(object):

    def __init__(self, name, session=None):
        self._name = name
        if session is None:
            self._session = doc_session
        else:
            self._session = session

    @LazyProperty
    def _channel(self):
        return self.new_channel()

    def __getattr__(self, name):
        return getattr(self._channel, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        return self._channel

    def new_channel(self):
        return Channel(self._session.db, self._name)


class Resource(object):

    def acquire(self, msg_id):
        '''Try to acquire the resource for msg_id.

        If successful, return True. Otherwise enqueue the message.
        '''
        raise NotImplementedError('acquire')

    def release(self, msg_id):
        '''Release the resource for msg_id.
        Returns a list of message ids that should be awakened.
        '''
        raise NotImplementedError('acquire')


