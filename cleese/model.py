from cPickle import dumps, loads

import bson

from ming import Session, Field
from ming import schema as S
from ming.declarative import Document

doc_session = Session.by_name('cleese')

class ActorState(Document):
    class __mongometa__:
        name='cleese.state'
        session = doc_session

    _id=Field(S.ObjectId)
    status=Field(str, if_missing='ready') # ready or busy
    worker=Field(str)              # worker currently reserving the actor
    type=Field(str)                # name of Actor subclass
    _data=Field('data', S.Binary)  # actor-specific data
    queue=Field(str, if_missing='cleese') # actor queue name
    mb=Field(
        [ { 'active': bool,
            'slot': str,
            'args': S.Binary,
            'kwargs': S.Binary,
            'cb_id': S.ObjectId(if_missing=None),
            'cb_slot': str }  ])

    @classmethod
    def reserve(cls, worker, queue='cleese', actor_id=None):
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
        return cls.m.update_partial(
            {'_id': id}, { '$push': { 'mb': msg } } )

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

    def unlock(self):
        self.status = 'ready'
        self.mb = [
            msg for msg in self.mb
            if not msg.active ]
        self.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': 'ready' },
              '$pull': { 'mb': { 'active': True } } } )
