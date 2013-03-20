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

class CompositeActor(function.FunctionActor):

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__,
            self.id)

    @classmethod
    def create(
        cls,
        sub_actors,
        args=None,
        kwargs=None,
        cb_id=None,
        **options):
        obj = super(CompositeActor, cls).create(
            args=args, kwargs=kwargs, cb_id=cb_id, **options)
        for i, sa in enumerate(sub_actors):
            _Element.create(obj.id, i, sa.id, **sa._state.options)
        return obj

    @classmethod
    def s(cls, sub_actors, *args, **kwargs):
        return cls.create(
            sub_actors, args=args, kwargs=kwargs)

    @classmethod
    def si(cls, sub_actors, *args, **kwargs):
        return cls.create(
            sub_actors, args=args, kwargs=kwargs, immutable=True)

    @classmethod
    def spawn(cls, sub_actors, *args, **kwargs):
        obj = cls.create(sub_actors, args=args, kwargs=kwargs)
        obj.start()
        obj.refresh()
        return obj

class Group(CompositeActor):

    def target(self):
        num_sub = 0
        q = M.ActorState.m.find(
            { 'parent_id': self.id })
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
    def retire_element(self, index, result):
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
        M.ActorState.m.remove({'parent_id': self.id})
        return result

class Pipeline(CompositeActor):

    def target(self):
        el_state = M.ActorState.m.find({
                'parent_id':self.id,
                'data.index': 0 }).one()
        M.Message.post(el_state._id)
        self.update_data_raw(cur_index=0)
        self.refresh()
        raise exc.Suspend()

    @slot()
    def retire_element(self, index, result):
        cur_index = self._state.data.cur_index
        self.update_data_raw(cur_index=cur_index+1)
        el_state = M.ActorState.m.find({
                'parent_id':self.id,
                'data.index': cur_index+1 }).first()
        if el_state is None:
            return self.retire_pipeline(result)
        if el_state.options.immutable:
            M.Message.post(el_state._id)
        else:
            M.Message.post(el_state._id, args=(result.get(),))
        raise exc.Suspend()

    def retire_pipeline(self, result):
        M.ActorState.m.remove({'parent_id': self.id})
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

class _Element(function.FunctionActor):

    def __repr__(self):
        return '<Element %d of %r>' % (
            self._state.data.index, self._state.parent_id)

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
        M.ActorState.m.remove( { '_id': self._state.data.subid } )
        M.Message.post(
            self._state.parent_id, 'retire_element',
            args=(self._state.data.index,result))
        raise exc.Suspend()

