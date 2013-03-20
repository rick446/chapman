import logging
from cPickle import dumps, loads

import sys
import bson

from . import exc
from . import actor
from . import function
from . import model as M
from .decorators import slot
from .context import g

__all__ = ('Group','Pipeline')
log = logging.getLogger(__name__)

class _Element(function.FunctionActor):

    @classmethod
    def create(cls, parent_id, index, subid, **options):
        obj = super(_Element, cls).create(**options)
        M.ActorState.m.update_partial(
            { '_id': obj.id },
            { '$set': {
                    'parent_id': parent_id,
                    'data.index': index,
                    'data.subid': subid } } )
        obj.refresh()
        return obj

    def target(self, *args, **kwargs):
        msg_cb = M.Message.post(self.id, 'retire', stat='pending')
        subid = self._state.data.subid
        M.ActorState.m.update_partial(
            { '_id': subid },
            { '$set': { 'cb_id': msg_cb._id } })
        M.Message.post(
            self._state.data.subid, args=args, kwargs=kwargs)
        raise exc.Suspend()

    @slot()
    def retire(self, result):
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$set': { 'result': bson.Binary(dumps(result)) } })
        M.Message.post(
            self._state.parent_id, 'retire_element',
            args=(self._state.data.index,))
        raise exc.Suspend()

class Group(function.FunctionActor):

    def __repr__(self):
        return '<Group %s %r>' % (
            self.id,
            self._state.data.get('subids', None))

    @classmethod
    def spawn(cls, sub_actors):
        obj = cls.create()
        for i, sa in enumerate(sub_actors):
            _Element.create(obj.id, i, sa.id)
        obj.start()
        obj.refresh()
        return obj

    def target(self):
        num_sub = 0
        q = M.ActorState.m.find(
            { 'parent_id': self._id })
        q = q.sort('data.index')
        for el_state in q:
            M.Message.post(el_state._id)
            num_sub += 1
        self.update_data_raw(
            num_sub=num_sub,
            num_remain=num_sub)
        self.refresh()
        if not num_sub:
            return self.retire_group()
        raise exc.Suspend()

    def append(self, sub):
        self._state = M.ActorState.m.find_and_modify(
            { '_id': self.id },
            update={'$inc': {
                    'data.num_sub': 1,
                    'data.num_remain': 1 } },
            new=True)
        el = _Element.create(
            self.id, self._state.data.num_sub-1, sub.id)
        el.start()

    @slot()
    def retire_element(self, index):
        data = self._state.data
        if data.num_remain == 1:
            return self.retire_group()
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$inc': { 'data.num_remain': -1 } } )
        raise exc.Suspend()

    def retire_group(self):
        q = M.ActorState.m.find({'parent_id': self.id})
        q = q.sort('data.index')
        result = GroupResult(
            self.id, [ sub.result for sub in q ])
        return result

class Pipeline(function.FunctionActor):

    def __repr__(self):
        return '<Pipeline %s %r>' % (
            self.id,
            self._state.data.get('subids', None))

    @classmethod
    def spawn(cls, sub_actors):
        subids = [ s.id for s in sub_actors ]
        obj = cls.create(args=(subids,))
        obj.start()
        obj.refresh()
        return obj
        
    def target(self, subids):
        log.info('Start pipeline %r', subids)
        cb_id = g.message['cb_id']
        cb_slot = g.message['cb_slot']
        g.message.update(cb_id=None, cb_slot=None)
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$pushAll': { 'data.subids': subids,
                            'data.remaining': subids },
              '$set': { 'data.cb_id': cb_id,
                        'data.cb_slot': cb_slot } })
        actor.Actor.send(
            subids[0], 'run', cb_id=self.id, cb_slot='retire_child')
        raise exc.Suspend()

    @slot()
    def retire_child(self, result):
        data = self._state.data
        remaining = data['remaining']
        rid = result.actor_id
        assert remaining[0] == rid
        if len(remaining) == 1:
            return self.retire_pipeline(result)
        try:
            next_actor = actor.Actor.by_id(remaining[1])
            next_actor.curry(result.get())
            next_actor.start(cb_id=self.id, cb_slot='retire_child')
        except exc.ActorError:
            result = actor.Result.failure(
                self.id, 'Pipeline error',
                *sys.exc_info())
            return self.retire_pipeline(result)
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$pull': { 'data.remaining': rid } })
        raise exc.Suspend()

    def retire_pipeline(self, result):
        data = self._state.data
        M.ActorState.m.remove({'_id': { '$in': data['subids'] } })
        g.message.update(cb_id=data['cb_id'], cb_slot=data['cb_slot'])
        return result

class GroupResult(actor.Result):
    def __init__(self, actor_id, sub_results):
        self.actor_id = actor_id
        self.sub_results = sub_results

    def __repr__(self):
        return '<GroupResult for %s>' % (
            self.actor_id)

    def get(self):
        return [ sr.get() for sr in self.sub_results ]

