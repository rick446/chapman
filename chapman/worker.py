__all__ = (
    'Worker',
)
import sys
import time
import logging
import threading
from Queue import Queue

import model as M
from .task import Task, Function

log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, name, qnames,
                 num_threads=1, sleep=1,
                 raise_errors=False):
        self._name = name
        self._qnames = qnames
        self._num_threads = num_threads
        self._sleep = sleep
        Function.raise_errors = raise_errors
        self._handler_threads = []
        self._num_active_messages = 0

    def start(self):
        chan = M.Message.channel.new_channel()
        chan.pub('start', self._name)
        sem = threading.Semaphore(self._num_threads)
        q = Queue()
        self._handler_threads = [
            threading.Thread(
                name='dispatch',
                target=self.dispatcher,
                args=(sem, q))]
        self._handler_threads += [
            threading.Thread(
                name='worker-%d' % x,
                target=self.worker,
                args=(sem, q))
            for x in range(self._num_threads)]
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

    def dispatcher(self, sem, q):
        log.info('Entering dispatcher thread')
        while True:
            sem.acquire()
            msg, state = _reserve_msg(self._name, self._qnames, self._waitfunc)
            self._num_active_messages += 1
            log.info('Reserved %r (%s)',
                     msg, self._num_active_messages)
            q.put((msg, state))

    def worker(self, sem, q):
        log.info('Entering worker thread')
        while True:
            conn = M.doc_session.bind.bind.conn
            try:
                msg, state = q.get()
                if msg is None:
                    break
                log.info('Received %r', msg)
                task = Task.from_state(state)
                task.handle(msg, 25)
            except Exception:
                log.exception('Unexpected error in worker thread')
                time.sleep(1)
            finally:
                self._num_active_messages -= 1
                sem.release()
                try:
                    conn.end_request()
                except Exception:
                    log.exception('Could not end request')


def _reserve_msg(name, qnames, waitfunc):
    while True:
        msg, state = M.Message.reserve(name, qnames)
        if msg is None:
            waitfunc()
            continue
        if state is None:
            continue
        return msg, state
