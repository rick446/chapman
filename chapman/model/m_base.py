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
    '''Wrapper for a model "cls" that has a queued and active field'''
    cls = None

    def acquire(self, msg_id, value):
        '''Try to acquire the resource for msg_id.

        If successful, return True. Otherwise enqueue the message.
        '''
        sem = self.cls.m.find_and_modify(
            {'_id': self.id},
            update={'$push': {
                'active': {'$each': [msg_id], '$slice': value},
                'queued': msg_id}},
            new=True)
        if msg_id in sem.active:
            self.cls.m.update_partial(
                {'_id': self.id},
                {'$pull': {'queued': msg_id}})
            return True
        else:
            return False

    def release(self, msg_id, size):
        '''Release the resource for msg_id.
        Yields a sequence of message ids that should be awakened.
        '''
        sem = self.cls.m.find_and_modify(
            {'_id': self.id},
            update={'$pull': {'active': msg_id}},
            new=True)
        while sem and len(sem.active) < size and sem.queued:
            wake_msg_id = sem.queued[0]
            updated = self.cls.m.find_and_modify(
                {'_id': self.id, 'queued': wake_msg_id},
                update={'$pull': {'queued': wake_msg_id}},
                new=True)
            if updated is None:
                sem = self.cls.m.get(_id=self.id)
            else:
                yield wake_msg_id
                sem = updated


