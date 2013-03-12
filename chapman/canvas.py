from . import exc
from . import actor
from . import function
from . import model as M
from .decorators import slot
from .context import g

__all__ = ('Group','Pipeline')

class Group(function.FunctionActor):

    @classmethod
    def spawn(cls, sub_actors):
        subids = [ s.id for s in sub_actors ]
        obj = cls.create(args=(subids,))
        obj.start()
        obj.refresh()
        return obj

    def target(self, subids):
        cb_id = g.message['cb_id']
        cb_slot = g.message['cb_slot']
        g.message.update(cb_id=None, cb_slot=None)
        self.update_data(
            subids=subids,
            results={},
            waiting=set(subids),
            cb_id=cb_id,
            cb_slot=cb_slot)
        for subid in subids:
            actor.Actor.send(
                subid, 'run', cb_id=self.id, cb_slot='retire_sub_actor')
        raise exc.Suspend()

    @slot()
    def retire_sub_actor(self, result):
        data = self._state.data
        results, waiting = data['results'], data['waiting']
        results[result.actor_id] = result
        waiting.remove(result.actor_id)
        self.update_data(results=results, waiting=waiting)
        if not waiting:
            return self.retire_group()
        raise exc.Suspend()

    def retire_group(self):
        data = self._state.data
        M.ActorState.m.remove({'_id': { '$in': data['subids'] } })
        result = GroupResult(
            self.id, [ data['results'][subid] for subid in data['subids'] ])
        self.update_data(result=result)
        g.message.update(cb_id=data['cb_id'], cb_slot=data['cb_slot'])
        return result

class Pipeline(function.FunctionActor):

    @classmethod
    def spawn(cls, sub_actors):
        subids = [ s.id for s in sub_actors ]
        obj = cls.create(args=(subids,))
        obj.start()
        obj.refresh()
        return obj
        
    def target(self, subids):
        cb_id = g.message['cb_id']
        cb_slot = g.message['cb_slot']
        g.message.update(cb_id=None, cb_slot=None)
        self.update_data(
            subids=subids,
            remaining=subids,
            cb_id=cb_id,
            cb_slot=cb_slot)
        actor.Actor.send(
            subids[0], 'run', cb_id=self.id, cb_slot='retire_sub_actor')
        raise exc.Suspend()

    @slot()
    def retire_sub_actor(self, result):
        data = self._state.data
        remaining = data['remaining']
        assert remaining[0] == result.actor_id
        if len(remaining) == 1:
            return self.retire_chain(result)
        try:
            next_actor = actor.Actor.by_id(remaining[1])
            next_actor.curry(result.get())
            next_actor.start(cb_id=self.id, cb_slot='retire_sub_actor')
        except exc.ActorError:
            return self.retire_chain(result)
        self.update_data(remaining=remaining[1:])
        raise exc.Suspend()

    def retire_chain(self, result):
        data = self._state.data
        M.ActorState.m.remove({'_id': { '$in': data['subids'] } })
        g.message.update(cb_id=data['cb_id'], cb_slot=data['cb_slot'])
        return result

class GroupResult(actor.Result):
    def __init__(self, actor_id, sub_results):
        self.actor_id = actor_id
        self.sub_results = sub_results

    def get(self):
        return [ sr.get() for sr in self.sub_results ]

