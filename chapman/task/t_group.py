from chapman import model as M

from .t_base import Result
from .t_composite import Composite

class Group(Composite):

    @classmethod
    def s(cls, subtasks=None, **options):
        if subtasks is None:
            subtasks = []
        self = super(Group, cls).s(subtasks, **options)
        self._state.m.set({'data.n_waiting': len(subtasks)})
        return self

    def run(self, msg):
        if self._state.data.n_waiting == 0:
            return self.retire()
        else:
            self._state.m.set(dict(status='active'))
        for st_state in self.subtask_iter():
            st = self.from_state(st_state)
            st.start(*msg.args, **msg.kwargs)

    def retire_subtask(self, msg):
        M.TaskState.m.update_partial(
            { '_id': self.id },
            { '$inc': { 'data.n_waiting': -1 } })
        self.refresh()
        if self._state.data.n_waiting == 0 and self._state.status == 'active':
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
        if all(sr.status == 'success'
               for sr in sub_results):
            self.status = 'success'
        else:
            self.status = 'failure'

    def __repr__(self): # pragma no cover
        return '<GroupResult for %s>' % (self.task_id)

    def __getitem__(self, index):
        return self.sub_results[index]

    def get(self):
        return [ sr.get() for sr in self.sub_results ]

        
