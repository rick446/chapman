__all__ = (
    'Worker',
    )
import sys
import time
import logging
import threading

import model as M
from .task import Task, Function

log = logging.getLogger(__name__)

class Worker(object):

    def __init__(self, name, queues,
                 num_threads=1, sleep=1,
                 raise_errors=False):
        self._name = name
        self._queues = queues
        self._num_threads = num_threads
        self._sleep = sleep
        Function.raise_errors = raise_errors
        self._handler_threads = []

    def start(self):
        self._handler_threads = [
            threading.Thread(target=self.handler)
            for x in range(self._num_threads) ]
        for t in self._handler_threads:
            t.setDaemon(True)
            t.start()
        
    def run(self):
        log.info('Entering event thread')
        chan = M.Message.channel.new_channel()
        @chan.sub('ping')
        def handle_ping(chan, msg):
            data = msg['data']
            if data['worker'] in (self._name, '*'):
                data['worker'] = self._name
                chan.pub('pong', data)
        @chan.sub('kill')
        def handle_kill(chan, msg):
            if msg['data'] in (self._name, '*'):
                log.error('Received %r, exiting', msg)
                sys.exit(0)
        while True:
            chan.handle_ready(await=True, raise_errors=True)
            time.sleep(0.2)

    def _waitfunc(self):
        chan = M.Message.channel.new_channel()
        chan.sub('send')
        for event in chan.cursor(await=True):
            return
        time.sleep(self._sleep)

    def handler(self):
        log.info('Entering handler thread')
        while True:
            conn = M.doc_session.bind.bind.conn
            try:
                msg, state = M.Message.reserve(self._name, self._queues)
                if msg is None:
                    self._waitfunc()
                if state is None:
                    continue
                log.info('Worker reserved %r', msg)
                task = Task.from_state(state)
                task.handle(msg)
            except Exception:
                log.exception('Unexpected error in handler thread')
                time.sleep(1)
            finally:
                try:
                    conn.end_request()
                except Exception:
                    log.exception('Could not end request')
            
