__all__ = (
    'Worker',
    )
import logging

import model as M
from .actor import Actor

log = logging.getLogger(__name__)

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
            msg = doc.active_message()
            log.info('Worker got actor %r %s', actor, msg['slot'])
            yield actor

    def reserve_actor(self, actor_id):
        doc = M.ActorState.reserve(self._name, actor_id=actor_id)
        if doc is None:
            return None
        ActorClass = Actor.by_name(doc.type)
        actor = ActorClass(doc)
        return actor

    def run_all(self, queue='chapman', waitfunc=None, raise_errors=False):
        for actor in self.actor_iterator(queue, waitfunc):
            actor.handle(raise_errors)
            M.doc_session.bind.bind.conn.end_request()

    def serve_forever(self, queues, sleep=1, raise_errors=False):
        waitfunc = lambda: M.Event.await(('send', 'unlock'), timeout=1, sleep=sleep)
        while True:
            self.run_all(queue={'$in': queues}, waitfunc=waitfunc, raise_errors=raise_errors)
       
            
