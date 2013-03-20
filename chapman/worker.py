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
            msg, doc = M.Message.reserve(queue, self._name)
            if doc is None:
                if waitfunc is not None:
                    waitfunc()
                    continue
                else:
                    break
            ActorClass = Actor.by_name(doc.type)
            actor = ActorClass(doc)
            log.info('Worker got actor %r %s', actor, msg['slot'])
            yield msg, actor
            msg.m.delete()

    def run_all(self, queue='chapman', waitfunc=None, raise_errors=False):
        for msg, actor in self.actor_iterator(queue, waitfunc):
            actor.handle(msg, raise_errors)
            assert actor._state.status != 'busy', actor._state
            M.doc_session.bind.bind.conn.end_request()

    def serve_forever(self, queues, sleep=1, raise_errors=False):
        waitfunc = lambda: M.Event.await(('send', 'unlock'), timeout=1, sleep=sleep)
        while True:
            self.run_all(queue={'$in': queues}, waitfunc=waitfunc, raise_errors=raise_errors)
       
            
