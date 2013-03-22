from chapman import model as M

from .t_base import Result
from .t_composite import Composite

class Group(Composite):

    @classmethod
    def s(cls, subtasks, **options):
        self = super(Group, cls).s(subtasks, **options)
        self._state.m.set({'data.n_waiting': len(subtasks)})
        return self

    def run(self, msg):
        for st_state in self.subtask_iter():
            st = self.from_state(st_state)
            st.start(*msg.args, **msg.kwargs)

    def retire_subtask(self, msg):
        M.TaskState.m.update_partial(
            { '_id': self.id },
            { '$inc': { 'data.n_waiting': -1 } })
        self.refresh()
        if self._state.data.n_waiting == 0:
            self.retire()

    error = retire_subtask

    def retire(self):
        gr = GroupResult(
            self.id, 
            [ st_state.result
              for st_state in self.subtask_iter() ])
        self.complete(gr)
        self.remove_subtasks()

class GroupResult(Result):
    def __init__(self, task_id, sub_results):
        self.task_id = task_id
        self.sub_results = sub_results

    def __repr__(self):
        return '<GroupResult for %s>' % (self.task_id)

    def get(self):
        return [ sr.get() for sr in self.sub_results ]

        
