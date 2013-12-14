'''This file contains the fake Pyramid view for running chapmand.

The pyramid app hosting chapman must route the special url /__chapman__ to this
view.
'''

from pyramid import httpexceptions as exc

def chapmand(request):
    env = request.environ
    if 'chapmand.worker' not in env:
        raise exc.HTTPNotFound()
    worker = env['chapmand.worker']
    worker()
