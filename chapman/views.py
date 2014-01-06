import logging
from datetime import datetime, timedelta

import gevent.queue
import gevent.event
from pyramid.view import view_config, notfound_view_config
from paste.deploy.converters import asint
import pyramid.httpexceptions as exc
import formencode as fe
from formencode.variabledecode import variable_decode

from chapman import model as M
from chapman import validators as V

log = logging.getLogger(__name__)


@view_config(
    route_name='chapman.1_0.queue',
    request_method='POST',
    renderer='string')
def put(request):
    params = variable_decode(request.GET)
    metadata = V.message_schema.to_python(params, request)
    data = request.json
    after = datetime.utcnow() + timedelta(metadata['delay'])
    msg = M.HTTPMessage.new(
        data=data,
        tags=metadata['tags'],
        timeout=metadata['timeout'],
        after=after,
        q=request.matchdict['qname'],
        pri=metadata['priority'])
    request.response.status_int = 201
    return msg.url(request)

@view_config(
    route_name='chapman.1_0.queue',
    request_method='GET')
def get(request):
    data = V.get_schema.to_python(request.params, request)
    sleep_ms = asint(request.registry.settings['chapman.sleep_ms'])
    # Ignore gets from the queue, as they skew our response time results
    try:
        import newrelic.agent
        newrelic.agent.set_background_task()
    except ImportError:
        pass
    messages = MessageGetter.get(
        request.matchdict['qname'],
        sleep_ms,
        data['client'],
        data['timeout'],
        data['count'])
    if messages:
        return dict((msg.url(request), msg.data) for msg in messages)
    else:
        return exc.HTTPNoContent()


@view_config(
    route_name='chapman.1_0.message',
    request_method='DELETE')
def delete_message(request):
    M.HTTPMessage.m.remove(dict(_id=int(request.matchdict['message_id'])))
    return exc.HTTPNoContent()


@view_config(
    route_name='chapman.1_0.message',
    request_method='POST')
def retry_message(request):
    '''Unlocks and retries the message at a point in the future'''
    data = V.retry_schema.to_python(request.json, request)
    after = datetime.utcnow() + timedelta(seconds=data['delay'])
    M.HTTPMessage.m.update_partial(
        dict(_id=int(request.matchdict['message_id'])),
        {'s.status': 'ready',
         's.after': after})
    M.HTTPMessage.channel.pub('enqueue', int(request.matchdict['message_id']))
    return exc.HTTPNoContent()


@view_config(context=exc.HTTPUnauthorized, renderer='json')
@view_config(context=exc.HTTPForbidden, renderer='json')
def on_auth_error(exception, request):
    request.response.status = exception.status
    return dict(
        status=exception.status_int,
        errors=exception.status)


@view_config(context=fe.Invalid, renderer='json')
def on_invalid(exception, request):
    request.response.status = 400
    return dict(
        status=400,
        errors=exception.unpack_errors())


@notfound_view_config(
    append_slash=True,
    renderer='json')
def on_notfound(context, request):
    request.response.status = context.status
    return dict(
        status=context.status_int,
        errors=context.status)


class MessageGetter(object):
    _registry = {}

    def __init__(self, qname, sleep):
        self.qname = qname
        self.sleep = sleep
        self.q = gevent.queue.PriorityQueue()
        gevent.spawn(self._gl_handler)
        self.backlog = 0

    @classmethod
    def get(cls, qname, sleep, client, timeout, count):
        getter = cls._registry.get(qname, None)
        if getter is None:
            getter = cls._registry[qname] = cls(qname, sleep)
        return getter._get(client, timeout, count)

    def _get(self, client, timeout, count):
        if not timeout:
            timeout = 1
        exp = datetime.utcnow() + timedelta(seconds=timeout)
        messages = []
        event = gevent.event.Event()
        self.q.put((exp, count, client, messages, event))
        event.wait()
        return messages

    def _gl_handler(self):
        chan = M.Message.channel.new_channel()
        while True:
            # Get request
            (exp, count, client, messages, event) = self.q.get()
            try:
                # If request is expired, signal it and move on
                if exp < datetime.utcnow():
                    event.set()
                    continue

                # Get all the ready messages, up to the number requested
                for x in range(count):
                    msg = M.HTTPMessage.reserve(client, [self.qname])
                    if msg is None:
                        break
                    messages.append(msg)

                # Found messages, notify caller and get next request
                if messages or exp < datetime.utcnow():
                    event.set()
                    continue

                # No messages, so we must put the request back onto the queue
                # and wait for a channel event
                self.q.put((exp, count, client, messages, event))

                cursor = chan.cursor(True)
                try:
                    cursor.next()
                except StopIteration:
                    gevent.sleep(self.sleep / 1e3)

            except:
                log.exception('Error in handler greenlet')
                event.set()

