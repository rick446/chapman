__all__ = (
    'Worker',
)
import sys
import time
import logging
import threading
from Queue import Queue, Empty

from pyramid.request import Request

import model as M
from .task import Task, Function

log = logging.getLogger(__name__)
exc_log = logging.getLogger('exc_logger')


class Worker(object):

    def __init__(
            self, app, name, qnames,
            chapman_path, registry,
            num_threads=1, sleep=0.2, raise_errors=False):
        self._app = app
        self._name = name
        self._qnames = qnames
        self._chapman_path = chapman_path
        self._registry = registry
        self._num_threads = num_threads
        self._sleep = sleep
        Function.raise_errors = raise_errors
        self._handler_threads = []
        self._num_active_messages = 0
        self._send_event = threading.Event()
        self._shutdown = False  # flag to indicate worker is shutting down

    def start(self):
        M.doc_session.db.collection_names()  # force connection & auth
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
        conn = M.doc_session.bind.bind.conn
        conn.start_request()
        chan = M.Message.channel.new_channel()
        chan.pub('start', self._name)

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

        @chan.sub('shutdown')
        def handle_shutdown(chan, msg):
            if msg['data'] in (self._name, '*'):
                log.error('Received %r, shutting down gracefully', msg)
                self._shutdown = True
                raise StopIteration()

        @chan.sub('send')
        def handle_send(chan, msg):
            self._send_event.set()

        while True:
            try:
                chan.handle_ready(await=True, raise_errors=True)
            except StopIteration:
                break
            time.sleep(self._sleep)

        for t in self._handler_threads:
            t.join()

    def _waitfunc(self):
        if self._shutdown:
            raise StopIteration()
        self._send_event.clear()
        self._send_event.wait(self._sleep)

    def dispatcher(self, sem, q):
        log.info('Entering dispatcher thread')
        while not self._shutdown:
            sem.acquire()
            try:
                msg, state = _reserve_msg(
                    self._name, self._qnames, self._waitfunc)
            except StopIteration:
                break
            except Exception as err:
                exc_log.exception(
                    'Error reserving message: %r, waiting 5s and continuing',
                    err)
                sem.release()
                time.sleep(5)
                continue
            self._num_active_messages += 1
            q.put((msg, state))
        log.info('Exiting dispatcher thread')

    def worker(self, sem, q):
        log.info('Entering chapmand worker thread')
        while not self._shutdown:
            try:
                msg, state = q.get(timeout=0.25)
            except Empty:
                continue
            try:
                log.info('Received %r', msg)
                task = Task.from_state(state)
                # task.handle(msg, 25)
                req = Request.blank(self._chapman_path, method='CHAPMAN')
                req.registry = self._registry
                req.environ['chapmand.task'] = task
                req.environ['chapmand.message'] = msg
                for x in self._app(req.environ, lambda *a,**kw:None):
                    pass
            except Exception as err:
                exc_log.exception('Unexpected error in worker thread: %r', err)
                time.sleep(self._sleep)
            finally:
                self._num_active_messages -= 1
                sem.release()
        log.info('Exiting chapmand worker thread')

    def handle_messages(self):
        '''Handle messages until there are no more'''
        while True:
            msg, state = M.Message.reserve(self._name, self._qnames)
            if msg is None:
                return
            task = Task.from_state(state)
            task.handle(msg)


def _reserve_msg(name, qnames, waitfunc):
    while True:
        msg, state = M.Message.reserve(name, qnames)
        if msg is None:
            waitfunc()
            continue
        if state is None:
            continue
        return msg, state
