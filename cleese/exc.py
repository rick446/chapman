import traceback

__all__ = ('ActorError',)

class ActorError(Exception):
    @classmethod
    def from_exc_info(cls, message, ex_type, ex_value, ex_tb):
        tb = traceback.format_exception(ex_type, ex_value, ex_tb)
        tb_arg = ''.join(
            [ message + ', ', 
              'original exception follows:\n'] + tb)
        self = cls(ex_type, ex_value, tb_arg)
        return self

    def format(self, indent=''):
        for line in self.args[2].splitlines():
            yield indent + line

class Chain(Exception):

    def __init__(self, actor_id, slot, *args, **kwargs):
        self.actor_id = actor_id
        self.slot = slot
        self.args = args
        self.kwargs = kwargs
