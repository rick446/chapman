import time
import logging

from docopt import docopt
from pyramid.paster import bootstrap, setup_logging

logging.basicConfig(level=logging.WARN)


CHUNKSIZE = 4096

log = logging.getLogger(__name__)


def chapmand():
    args = docopt("""Usage:
            chapmand <config> [options]

    Options:
      -h --help                 show this help message and exit
      -c,--concurrency THREADS  number of threads to run [default: 1]
      -d,--debug                drop into a debugger on task errors?
    """)
    config = args['<config>']
    if '#' not in config:
        config += '#chapman'
    setup_logging(config)
    app_context = bootstrap(config)
    app_context['app'].run(
        registry=app_context['registry'],
        concurrency=int(args['--concurrency']),
        debug=bool(args['--debug']))


def hq_ping():
    args = docopt("""Usage:
        chapman-hq-ping <secret> <qname>

    Options:
        -h --help                 show this help message and exit
    """)
    from chapman import hq

    class PingListener(hq.Listener):

        def __init__(self, qname, secret):
            self.q0 = hq.HQueue(qname, secret)
            self.q1 = hq.HQueue(qname, secret)
            self.msgs = {}
            super(PingListener, self).__init__(self.q1, 'listener')

        def ping(self):
            now = time.time()
            result = self.q0.put({}).content
            self.msgs[result] = now

        def handle(self, id, msg):
            now = time.time()
            elapsed = now - self.msgs.pop(id, 0)
            print 'Latency %s: %dms' % (
                id, (elapsed * 1000))

    listener = PingListener(args['<qname>'], args['<secret>'])
    listener.start()

    while True:
        listener.ping()
        time.sleep(1)



