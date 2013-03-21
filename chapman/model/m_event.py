import logging

log = logging.getLogger(__name__)

def notify(key, data):
    log.debug('notify(%r, %r)', key, data)
