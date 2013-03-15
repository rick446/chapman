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
    actor_id=Field(S.ObjectId, if_missing=None)
    name=Field(str)
    value=Field(None)
    ts=Field(datetime, if_missing=datetime.utcnow)

    @classmethod
    def publish(cls, name, actor_id, value=None):
        doc = cls.make(dict(
                _id=Sequence.next('event'),
                actor_id=actor_id,
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
    parent_id = Field(S.ObjectId, if_missing=None)
    status=Field(str, if_missing='ready') # ready, busy, or complete
    worker=Field(str)              # worker currently reserving the actor
    type=Field(str)                # name of Actor subclass
    data=Field({str:None})         # actor-specific data
    cb_id=Field(S.ObjectId, if_missing=None)
    cb_slot=Field(str)
    _result=Field('result', S.Binary)
    options=Field(dict(
            queue=S.String(if_missing='chapman'),
            immutable=S.Bool(if_missing=False),
            ignore_result=S.Bool(if_missing=False)))
    mb=Field(
        [ { 'active': bool,
            'slot': str,
            'args': S.Binary,
            'kwargs': S.Binary,
            'cb_id': S.ObjectId(if_missing=None),
            'cb_slot': str }  ])

    @classmethod
    def ls(cls):
        for obj in cls.m.find():
            print '%s: %s %s @%s' % (
                obj._id, obj.status, obj.type, obj.worker)

    @classmethod
    def reserve(cls, worker, queue='chapman', actor_id=None):
        '''Reserve a ready actor with unprocessed messages and return
        its state'''
        if actor_id is None:
            spec = {
                'status': { '$in': [ 'ready', 'complete' ] },
                'options.queue': queue,
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
            Event.publish('reserve', doc._id, doc.active_message_raw())
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
        Event.publish('send', id, msg)
        return result

    @property
    def result(self):
        if self._result is None: return None
        return loads(self._result)
    @result.setter
    def result(self, value):
        presult = bson.Binary(dumps(value))
        self._result = presult

    def retire(self, result):
        cur = self
        ids = [cur._id]
        while cur.parent_id is not None:
            cur = ActorState.m.get(_id=cur.parent_id)
            ids.append(cur._id)
        result.actor_id = cur._id
        ActorState.m.update_partial(
            { '_id': { '$in': ids } },
            { '$set': {
                    'result': bson.Binary(dumps(result)),
                    'status': 'complete' },
              '$pull': { 'mb': { 'active': True } } },
            multi=True)
        Event.publish('retire', self._id, ids)
        self.result = result
        return result

    def active_message_raw(self):
        active_messages = [ msg for msg in self.mb if msg.active ]
        assert len(active_messages) == 1, active_messages
        return active_messages[0]

    def active_message(self):
        msg = self.active_message_raw()
        return dict(
            slot=msg.slot,
            args=loads(msg.args),
            kwargs=loads(msg.kwargs),
            cb_id=msg.cb_id,
            cb_slot=msg.cb_slot)

    def unlock(self, new_status):
        '''Change the status and remove any active messages from the mailbox'''
        self.status = new_status
        self.mb = [
            msg for msg in self.mb
            if not msg.active ]
        ActorState.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': new_status },
              '$pull': { 'mb': { 'active': True } } } )
        Event.publish('unlock', self._id, dict(s=new_status))

    def update_data(self, **kwargs):
        '''Updates the data field with pickled values'''
        pickled_kwargs = dict(
            (k, bson.Binary(dumps(v)))
            for k,v in kwargs.items() )
        return self.update_data_raw(**pickled_kwargs)

    def update_data_raw(self, **kwargs):
        '''Updates the data field with raw values'''
        updates = dict(
            ('data.%s' % k, v) for k,v in kwargs.items())
        self.data.update(kwargs)
        return ActorState.m.update_partial(
            { '_id': self._id },
            { '$set': updates } )

    def get_data(self, key):
        '''Unpickles values from the data field'''
        return loads(self.data[key])

    @classmethod
    def chain(cls, message, parent_id, child_id, slot, *args, **kwargs):
        msg = dict(
            active=False,
            slot=slot,
            args=bson.Binary(dumps(args)),
            kwargs=bson.Binary(dumps(kwargs)),
            cb_id=message['cb_id'],
            cb_slot=message['cb_slot'])
        ActorState.m.update_partial(
            { '_id': child_id },
            { '$push': { 'mb': msg },
              '$set': { 'parent_id': parent_id } })
        message['cb_id'] = message['cb_slot'] = None
