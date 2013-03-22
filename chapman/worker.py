__all__ = (
    'Worker',
    )
import os
import time
import logging
import threading

import model as M
from .task import Task

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
        self._chan = M.Event.channel()

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
        @self._chan.sub('ping')
        def handle_ping(chan, msg):
            self._chan.pub('pong', self._name)
        @self._chan.sub('kill')
        def handle_kill(chan, msg):
            if msg['data'] in (self._name, '*'):
                log.error('Received %r, exiting', msg)
                os._exit(0)
        while True:
            self._chan.handle_ready(await=True)
            time.sleep(0.2)

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

    def run_all(self, queue='chapman', waitfunc=None, raise_errors=False):
        for msg, actor in self.actor_iterator(queue, waitfunc):
            actor.handle(msg, raise_errors)
            msg.m.delete()
            assert actor._state.status != 'busy', actor._state
            M.doc_session.bind.bind.conn.end_request()

    def serve_forever(self, queues, sleep=1, raise_errors=False):
        waitfunc = lambda: M.Event.await(('send', 'unlock'), timeout=1, sleep=sleep)
        while True:
            self.run_all(queue={'$in': queues}, waitfunc=waitfunc, raise_errors=raise_errors)
       
            
