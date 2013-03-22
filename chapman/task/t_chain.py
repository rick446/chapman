from chapman.context import g
from chapman import exc
from chapman import model as M

from .t_composite import Composite

class Chain(Composite):

    @classmethod
    def call(cls, subtask, *args, **kwargs):
        self = cls.n(subtask)
        self._state.m.set(dict(
                parent_id=g.task.id,
                status='active'))
        subtask.start(*args, **kwargs)
        raise exc.Suspend('chained')

    def retire_subtask(self, msg):
        result, position = msg.args
        parent_task_state = M.TaskState.m.get(
            _id=self._state.parent_id)
        parent_task = self.from_state(parent_task_state)
        parent_task.complete(result)
        self.remove_subtasks()
        self.forget()

    error = retire_subtask
