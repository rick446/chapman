import sys
import logging

from chapman import model as M

from .t_base import Result
from .t_composite import Composite

log = logging.getLogger(__name__)


class Group(Composite):

    def run(self, msg):
        if self._state.data.n_waiting == 0:
            return self.retire()
        else:
            self._state.m.set(dict(status='active'))
        for st_state in self.subtask_iter():
            if st_state.status == 'pending':
                st = self.from_state(st_state)
                st.start(*msg.args, **msg.kwargs)

    def retire_subtask(self, msg):
        try:
            try:
                result, position = msg.args
            except ValueError:
                log.exception(
                    'Wrong number of args in retire_subtask message: %r',
                    msg.args)
            M.TaskState.m.update_partial(
                {'_id': self.id},
                {'$inc': {'data.n_waiting': -1}})
            if (self._state.options.ignore_result
                    and result.status == 'success'):
                M.TaskState.m.remove({'_id': result.task_id})
            self.refresh()
            if (self._state.data.n_waiting <= 0
                    and self._state.status == 'active'):
                self.retire()
        except:
            result = Result.failure(
                self._state._id, 'Error in %r' % self, *sys.exc_info())
            self.complete(result)

    def retire(self):
        results = []
        for st in self.subtask_iter():
            if st.status in ('pending', 'ready'):
                return  # group isn't really done
            results.append(st.result)
        gr = GroupResult(self.id, results)
        self.remove_subtasks()
        self.complete(gr)


class GroupResult(Result):
    def __init__(self, task_id, sub_results):
        self.task_id = task_id
        self.sub_results = sub_results
        if all(sr.status == 'success'
               for sr in sub_results):
            self.status = 'success'
        else:
            self.status = 'failure'

    def __repr__(self):  # pragma no cover
        return '<GroupResult for %s>' % (self.task_id)

    def __getitem__(self, index):
        return self.sub_results[index]

    def get(self):
        return [sr.get() for sr in self.sub_results]
