'''This file contains the fake Pyramid view for running chapmand.

The pyramid app hosting chapman *must* route the special url /__chapman__ to this
view. Here is an example of how to do that:

    config.add_route('__chapman__', '/__chapman__')
    config.add_view(
        'chapman.chapmand.handle_task',
        route_name='__chapman__',
        request_method='CHAPMAN')

'''

from pyramid import httpexceptions as exc
from chapman.context import g

def handle_task(request):
    env = request.environ
    task = env.get('chapmand.task')
    msg = env.get('chapmand.message')
    if task is None or msg is None:
        raise exc.HTTPNotFound()
    with g.set_context(request=request, registry=request.registry):
        task.handle(msg)
    return exc.HTTPOk()
