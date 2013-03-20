__all__ = (
    'Worker',
    )
import os
import time
import logging
import threading

import model as M
from .actor import Actor

log = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, name, queues,
                 num_threads=1, timeout=1, sleep=1,
                 raise_errors=False):
        self._name = name
        self._queues = queues
        self._num_threads = num_threads
        self._timeout = timeout
        self._sleep = sleep
        self._raise_errors = raise_errors
        self._event_thread = None
        self._handler_threads = []

    def start(self):
        self._event_thread = threading.Thread(target=self.event)
        self._event_thread.setDaemon(True)
        self._event_thread.start()
        self._handler_threads = [
            threading.Thread(target=self.handler)
            for x in range(self._num_threads) ]
        for t in self._handler_threads:
            t.setDaemon(True)
            t.start()
        
    def event(self):
        log.info('Entering event thread')
        while True:
            try:
                ev = M.Event.await(
                    ('ping','kill'),
                    timeout=None,
                    sleep=self._sleep)
                log.info('Got event %s', ev)
                if ev.name == 'kill' and ev.value == self._name:
                    log.error('... exiting')
                    os._exit(0)
                elif ev.name == 'ping':
                    M.Event.publish('pong', None, self._name)
            except Exception:
                log.exception('Unexpected error in event thread')
                time.sleep(1)

    def _waitfunc(self):
        M.Event.await(
            ('send', 'unlock'),
            timeout=self._timeout,
            sleep=self._sleep)

    def handler(self):
        while True:
            log.info('Entering handler thread')
            try:
                self.run_all(
                    queue={'$in': self._queues},
                    waitfunc=self._waitfunc,
                    raise_errors=self._raise_errors)
            except Exception:
                log.exception('Unexpected error in handler thread')
                time.sleep(1)

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
       
            
