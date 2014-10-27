import os
import logging

from formencode import validators as fev
from formencode import schema as fes
from formencode import variabledecode as fevd
from formencode import foreach as fef

from chapman import worker

log = logging.getLogger(__name__)


class SettingsSchema(fes.Schema):
    pre_validators=[fevd.NestedVariables()]
    name = fev.String()
    queues = fef.ForEach(if_missing=['chapman'])
    path = fev.String()
    sleep_ms = fev.Int()


settings_schema = SettingsSchema()


def filter_factory(global_conf, **local_settings):
    settings = settings_schema.to_python(local_settings)
    flt = Chapman(**settings)
    return flt


class Chapman(object):

    def __init__(self, name, path, queues, sleep_ms, app=None):
        self.name = name
        self.path = path
        self.queues = queues
        self.sleep_ms = sleep_ms
        self.app = app

    def __call__(self, app):
        self.app = app
        return self

    def run(self, registry, concurrency, debug):
        name = '{}:{}'.format(self.name, os.getpid())
        log.info('Starting Chapman')
        log.info('    path:        %s', self.path)
        log.info('    name:        %s', name)
        log.info('    queues:      %s', self.queues)
        log.info('    concurrency: %s', concurrency)
        log.info('    debug:       %s', debug)
        log.info('    sleep_ms:    %s', self.sleep_ms)
        w = worker.Worker(
            app=self.app,
            name=name,
            qnames=self.queues,
            chapman_path=self.path,
            registry=registry,
            num_threads=concurrency,
            sleep=self.sleep_ms / 1000.0,
            raise_errors=debug)
        w.start()
        w.run()
