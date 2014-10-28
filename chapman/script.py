import time
import os.path
import logging.config
from ConfigParser import ConfigParser

from docopt import docopt
from pyramid.paster import bootstrap
from formencode import validators as fev
from formencode import schema as fes
from formencode import variabledecode as fevd
from formencode import foreach as fef

from chapman import worker

CHUNKSIZE = 4096

log = logging.getLogger(__name__)


class Chapman(object):
    """Usage:
            chapmand <config> [options]

    Options:
      -h --help                 show this help message and exit
      -c,--concurrency THREADS  number of threads to run [default: 1]
      -d,--debug                drop into a debugger on task errors?
    """
    settings_schema = fes.Schema(
        pre_validators=[fevd.NestedVariables()],
        name=fev.String(),
        queues=fef.ForEach(if_missing=['chapman']),
        path=fev.String(),
        sleep_ms=fev.Int())

    def __init__(self, name, path, queues, sleep_ms):
        self.name = name
        self.path = path
        self.queues = queues
        self.sleep_ms = sleep_ms

    @classmethod
    def script(cls):
        args = docopt(cls.__doc__)
        config = args['<config>']
        if '#' in config:
            config, section = config.split('#')
        else:
            section = 'chapman'
        _setup_logging(config)
        cp = ConfigParser()
        cp.read(config)
        settings = dict(cp.items(section))
        app_section = settings.pop('app')
        app_context = bootstrap('{}#{}'.format(config, app_section))
        settings = cls.settings_schema.to_python(settings)
        self = cls(**settings)
        self.run(
            app_context['app'],
            app_context['registry'],
            int(args['--concurrency']),
            bool(args['--debug']))


    def run(self, app, registry, concurrency, debug):
        name = '{}:{}'.format(self.name, os.getpid())
        log.info('Starting Chapman')
        log.info('    path:        %s', self.path)
        log.info('    name:        %s', name)
        log.info('    queues:      %s', self.queues)
        log.info('    concurrency: %s', concurrency)
        log.info('    debug:       %s', debug)
        log.info('    sleep_ms:    %s', self.sleep_ms)
        w = worker.Worker(
            app=app,
            name=name,
            qnames=self.queues,
            chapman_path=self.path,
            registry=registry,
            num_threads=concurrency,
            sleep=self.sleep_ms / 1000.0,
            raise_errors=debug)
        w.start()
        w.run()


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


def _setup_logging(config_file):
    '''Setup logging like pyramid.paster.setup_logging but does
    NOT disable existing loggers
    '''
    if '#' in config_file:
        path, _ = config_file.split('#')
    else:
        path = config_file
    full_path = os.path.abspath(path)
    here = os.path.dirname(full_path)
    return logging.config.fileConfig(
        full_path, dict(__file__=full_path, here=here),
        disable_existing_loggers=False)


