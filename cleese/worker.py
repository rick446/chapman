__all__ = (
    'Worker',
    )
import time

import model as M
from .actor import Actor

class Worker(object):

    def __init__(self, name):
        self._name = name

    def actor_iterator(self, queue='cleese', waitfunc=None):
        if waitfunc is None:
            waitfunc = lambda:time.sleep(1)
               
        while True:
            doc = M.ActorState.reserve(self._name, queue)
            if doc is None:
                import ipdb; ipdb.set_trace()
                waitfunc()
                continue
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
        
