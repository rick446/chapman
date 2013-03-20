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

class Message(Document):
    class __mongometa__:
        name = 'chapman.message'
        session = doc_session
        indexes = [
            [ ('q', 1),
              ('stat', 1),
              ('pri', -1),
              ('ts', 1),
              ('actor_id', 1) ] ]

    _id = Field(S.ObjectId)
    aid = Field(S.ObjectId, if_missing=None)
    q = Field(str)
    stat = Field(str, if_missing='ready')
    pri = Field(int, if_missing=10)
    ts = Field(datetime, if_missing=datetime.utcnow)
    wkr = Field(str)
    slot = Field(str)
    _args = Field('args', S.Binary)
    _kwargs = Field('kwargs', S.Binary)

    @property
    def args(self):
        if self._args is None: return None
        return loads(self._args)
    @args.setter
    def args(self, value):
        self._args = bson.Binary(dumps(value))
        
    @property
    def kwargs(self):
        if self._kwargs is None: return None
        return loads(self._kwargs)
    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = bson.Binary(dumps(value))

    @classmethod
    def reserve(cls, queue, worker):
        '''Find the first message for the first non-busy worker'''
        sort = [ ('pri', -1), ('ts', 1) ]
        spec = { 'q': queue, 'stat': 'ready' }
        for msg in cls.m.find(spec).sort(sort):
            astate = ActorState.m.find_and_modify(
                { '_id': msg.aid, 'status': 'ready' },
                update={'$set': { 'status':'busy', 'worker': worker } },
                new=True)
            if astate is None: continue
            cls.m.update_partial(
                { '_id': msg._id },
                { '$set': {
                        'stat': 'busy',
                        'wkr': worker } })
            msg.stat = 'busy'
            msg.wkr = worker
            return msg, astate
        return None, None

    @classmethod
    def create(cls, actor_id, slot='run', stat='ready',
             args=None, kwargs=None):
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        astate = ActorState.m.get(_id=actor_id)
        obj = cls.make(dict(
                aid=actor_id, slot=slot, stat=stat,
                q=astate.options.queue,
                pri=astate.options.priority, 
                args=bson.Binary(dumps(args)),
                kwargs=bson.Binary(dumps(kwargs))))
        return obj

    @classmethod
    def post(cls, actor_id, slot='run', stat='ready',
             args=None, kwargs=None):
        obj = cls.create(actor_id, slot, stat, args, kwargs)
        obj.m.insert()
        return obj

    @classmethod
    def post_callback(cls, cb_id, result):
        if not cb_id: return
        cls.m.update_partial(
            {'_id': cb_id},
            {'$set': {
                    'stat': 'ready',
                    'args': bson.Binary(dumps((result,))) } },
            multi=True)

class ActorState(Document):
    class __mongometa__:
        name='chapman.state'
        session = doc_session
        indexes = [
            [ ('status', 1) ]
            ]

    _id=Field(S.ObjectId)
    parent_id = Field(S.ObjectId, if_missing=None)
    status=Field(str, if_missing='ready') # ready, busy, or complete
    worker=Field(str)              # worker currently reserving the actor
    type=Field(str)                # name of Actor subclass
    data=Field({str:None})         # actor-specific data
    cb_id=Field(S.ObjectId, if_missing=None)
    _result=Field('result', S.Binary)
    options=Field(dict(
            queue=S.String(if_missing='chapman'),
            priority=S.Int(if_missing=10),
            immutable=S.Bool(if_missing=False),
            ignore_result=S.Bool(if_missing=False)))
    ts=Field({str: S.DateTime(if_missing=None)})

    @classmethod
    def ls(cls, status=None):
        from chapman import Actor
        q = {}
        if status is not None:
            q['status'] = { '$in': status.split(',') }
        result = []
        for obj in cls.m.find(q):
            result.append(obj)
            actor = Actor.by_id(obj._id)
            print '%s: %s @%s %r' % (
                obj._id, obj.status, obj.worker, actor)
        return result

    def show_path(self):
        def path_iter(cur, indent=''):
            from chapman import Actor
            actor = Actor.by_id(cur._id)
            fmt = indent + '%s: %s @%s %r'
            yield fmt % (
                self._id, self.status, self.worker, actor)
            if cur.parent_id:
                for line in path_iter(ActorState.m.get(
                        _id=cur.parent_id), indent + '    '):
                    yield line
        return '\n'.join(path_iter(self))

    @property
    def result(self):
        if self._result is None: return None
        return loads(self._result)
    @result.setter
    def result(self, value):
        presult = bson.Binary(dumps(value))
        self._result = presult

    def retire(self, message, result):
        cur = self
        # Collect chain
        chain = [ cur ]
        while cur.parent_id is not None:
            cur = ActorState.m.get(_id=cur.parent_id)
            chain.append(cur)
        result.actor_id = cur._id
        # Save result
        ActorState.m.update_partial(
            { '_id': cur._id },
            { '$set': { 'result': bson.Binary(dumps(result)),
                        'status': 'complete',
                        'ts.retire': datetime.utcnow() } })
        # Remove tail actor(s)
        if len(chain) > 1:
            ActorState.m.remove({'_id': {'$in': chain[:-1] } })
        # Perform callback(s)
        cb_ids = [ a.cb_id for a in chain if a.cb_id is not None ]
        Message.post_callback({'$in': cb_ids}, result)
        # Unlock this actor
        self.unlock(message, 'complete')
        Event.publish('retire', cur._id)
        return result

    def unlock(self, message, new_status):
        ActorState.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': new_status,
                        'ts.suspend_%s' % new_status: datetime.utcnow()} } )
        self.status = new_status

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
    def chain(cls, parent_id, message, child_id, slot, args, kwargs):
        ActorState.m.update_partial(
            { '_id': child_id },
            { '$set': { 'parent_id': parent_id } })
        Message.post(
            child_id,
            slot=slot,
            args=args,
            kwargs=kwargs)
