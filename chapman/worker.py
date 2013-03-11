__all__ = (
    'Worker',
    )

import model as M
from .actor import Actor

class Worker(object):

    def __init__(self, name):
        self._name = name

    def actor_iterator(self, queue='chapman', waitfunc=None):
        while True:
            doc = M.ActorState.reserve(self._name, queue)
            if doc is None:
                if waitfunc is not None:
                    waitfunc()
                    continue
                else:
                    break
            ActorClass = Actor.by_name(doc.type)
            actor = ActorClass(doc)
            yield actor

    def reserve_actor(self, actor_id):
        doc = M.ActorState.reserve(self._name, actor_id=actor_id)
        if doc is None:
            return None
        ActorClass = Actor.by_name(doc.type)
        actor = ActorClass(doc)
        return actor

    def run_all(self, queue='chapman', waitfunc=None):
        for actor in self.actor_iterator(queue, waitfunc):
            actor.handle()
