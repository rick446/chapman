import time
from cPickle import dumps, loads
from datetime import datetime

import bson

from ming import Session, Field
from ming import schema as S
from ming.declarative import Document

doc_session = Session.by_name('chapman')

class Sequence(Document):
    class __mongometa__:
        name = 'chapman.sequence'
        session = doc_session
    _id=Field(str)
    _next=Field('next', int)

    @classmethod
    def next(cls, name):
        doc = cls.m.find_and_modify(
            query={ '_id': name },
            update={ '$inc': { 'next': 1 } },
            upsert=True,
            new=True)
        return doc._next

class Event(Document):
    class __mongometa__:
        name='chapman.event'
        session = doc_session

    _id=Field(int)
    name=Field(str)
    value=Field(None)
    ts=Field(datetime, if_missing=datetime.utcnow)

    @classmethod
    def publish(cls, name, value):
        doc = cls.make(dict(
                _id=Sequence.next('event'),
                name=name,
                value=value))
        doc.m.insert()
        return doc

    @classmethod
    def await(cls, event_names, timeout=None, sleep=1):
        doc = Sequence.m.get(_id='event')
        if doc: last = doc._next
        else: last = 0
        start = time.time()
        while True:
            spec = { '_id': { '$gt': last } }
            if event_names:
                spec['name'] = { '$in': event_names }
            q = cls.m.find(spec, tailable=True, await_data=True)
            q = q.sort('$natural')
            for ev in q:
                return ev
            elapsed = time.time() - start
            if timeout is not None:
                if elapsed > timeout:
                    return None
            time.sleep(sleep)

class ActorState(Document):
    class __mongometa__:
        name='chapman.state'
        session = doc_session

    _id=Field(S.ObjectId)
    status=Field(str, if_missing='ready') # ready, busy, or complete
    worker=Field(str)              # worker currently reserving the actor
    type=Field(str)                # name of Actor subclass
    _data=Field('data', S.Binary)  # actor-specific data
    queue=Field(str, if_missing='chapman') # actor queue name
    immutable=Field(bool, if_missing=False) 
    mb=Field(
        [ { 'active': bool,
            'slot': str,
            'args': S.Binary,
            'kwargs': S.Binary,
            'cb_id': S.ObjectId(if_missing=None),
            'cb_slot': str }  ])

    @property
    def data(self):
        return loads(self._data)
    @data.setter
    def data(self, value):
        self._data = bson.Binary(dumps(value))

    @classmethod
    def reserve(cls, worker, queue='chapman', actor_id=None):
        '''Reserve a ready actor with unprocessed messages and return
        its state'''
        if actor_id is None:
            spec = {
                'status': 'ready',
                'queue': queue,
                'mb.active': False }
        else:
            spec = {
                'status': 'ready',
                'mb.active': False,
                '_id': actor_id }
        update = {
            '$set': {
                'status': 'busy',
                'worker': worker,
                'mb.$.active': True } }
        doc = cls.m.find_and_modify(query=spec, update=update, new=True)
        if doc is not None:
            Event.publish('reserve', doc._id)
        return doc

    @classmethod
    def send(cls, id, slot, args=None, kwargs=None, cb_id=None, cb_slot=None):
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        msg = dict(
            active=False,
            slot=slot,
            args=bson.Binary(dumps(args)),
            kwargs=bson.Binary(dumps(kwargs)),
            cb_id=cb_id,
            cb_slot=cb_slot)
        result = cls.m.update_partial(
            {'_id': id}, { '$push': { 'mb': msg } } )
        Event.publish('send', id)
        return result

    def active_message(self):
        active_messages = [ msg for msg in self.mb if msg.active ]
        assert len(active_messages) == 1, active_messages
        msg = active_messages[0]
        return dict(
            slot=msg.slot,
            args=loads(msg.args),
            kwargs=loads(msg.kwargs),
            cb_id=msg.cb_id,
            cb_slot=msg.cb_slot)

    def unlock(self, new_status):
        self.status = new_status
        self.mb = [
            msg for msg in self.mb
            if not msg.active ]
        result = ActorState.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': new_status },
              '$pull': { 'mb': { 'active': True } } } )
        Event.publish('unlock', dict(s=new_status, id=self._id))
        return result
