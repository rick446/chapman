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

class Group(function.FunctionActor):

    def __repr__(self):
        return '<Group %s %r>' % (
            self.id,
            self._state.data.get('subids', None))

    @classmethod
    def create(cls, args=None, kwargs=None, **options):
        '''Create an actor instance'''
        obj = super(Group, cls).create(args, kwargs, **options)
        obj.update_data_raw(
            results={},
            subids=[],
            waiting=[],
            cb_id=None,
            cb_slot=None)
        return obj

    @classmethod
    def spawn(cls, sub_actors):
        subids = [ s.id for s in sub_actors ]
        obj = cls.create(args=(subids,))
        obj.start()
        obj.refresh()
        return obj

    def target(self, subids):
        log.info('Start group %r', subids)
        cb_id = g.message['cb_id']
        cb_slot = g.message['cb_slot']
        g.message.update(cb_id=None, cb_slot=None)
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$pushAll': { 'data.subids': subids,
                            'data.waiting': subids },
              '$set': { 'data.cb_id': cb_id,
                        'data.cb_slot': cb_slot } })
        for subid in subids:
            actor.Actor.send(
                subid, 'run', cb_id=self.id, cb_slot='retire_child')
        self.refresh()
        if not self._state.data.waiting:
            return self.retire_group()
        raise exc.Suspend()

    def append(self, sub):
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$push': { 'data.subids': sub.id,
                         'data.waiting': sub.id } })
        self._state.data['subids'].append(sub.id)
        self._state.data['waiting'].append(sub.id)
        actor.Actor.send(
            sub.id, 'run', cb_id=self.id, cb_slot='retire_child')

    @slot()
    def retire_child(self, result):
        data = self._state.data
        results = data['results']
        waiting = data['waiting']
        presult = bson.Binary(dumps(result))
        rid = result.actor_id
        results[str(rid)] = presult
        waiting.remove(rid)
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$pull': { 'data.waiting': rid },
              '$set': { 'data.results.%s' % rid: presult } })
        if not waiting:
            return self.retire_group()
        raise exc.Suspend()

    def retire_group(self):
        data = self._state.data
        M.ActorState.m.remove({'_id': { '$in': data['subids'] } })
        results = data['results']
        result = GroupResult(
            self.id, [ loads(results[str(subid)]) for subid in data['subids'] ])
        self.update_data(result=result)
        g.message.update(cb_id=data['cb_id'], cb_slot=data['cb_slot'])
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

