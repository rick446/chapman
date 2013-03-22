import traceback

__all__ = ('TaskError',)

class ChapmanError(Exception): pass

class TaskError(ChapmanError):
    @classmethod
    def from_exc_info(cls, message, ex_type, ex_value, ex_tb):
        tb = traceback.format_exception(ex_type, ex_value, ex_tb)
        tb_arg = ''.join(
            [ message + ', ', 
              'original exception follows:\n'] + tb)
        self = cls(ex_type, ex_value, tb_arg)
        return self

    def __repr__(self):
        lines = [ '<TaskError>, original exception follows:' ]
        try:
            lines += list(self.format())
        except: # pragma no cover
            lines.append('... could not print original exception')
        return '\n'.join(lines)

    __str__ = __repr__

    def format(self, indent=''):
        for line in self.args[2].splitlines():
            yield indent + line

class Timeout(ChapmanError): pass

class Suspend(ChapmanError):

    def __init__(self, status='ready'):
        super(Suspend, self).__init__()
        self.status = status
