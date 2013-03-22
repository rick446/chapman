from chapman import model as M

from .t_base import Task

class Composite(Task):

    @classmethod
    def s(cls, subtasks, **options):
        self = super(Composite, cls).s(**options)
        self._state.m.set({'data.n_subtask': 0})
        for st in subtasks:
            self.append(st)
        return self

    def append(self, st):
        position = self._state.data.n_subtask
        st.link(self, 'retire_subtask', position)
        st._state.m.set({
                'parent_id': self.id,
                'data.composite_position': position,
                'options.preserve_result': True,
                })
        self._state.m.set({'data.n_subtask': position+1})

    def subtask_iter(self):
        q = M.TaskState.m.find({'parent_id': self.id })
        q = q.sort('data.composite_position')
        return q

    def run(self, msg):
        raise NotImplementedError, 'run'

    def retire_subtask(self, msg):
        raise NotImplementedError, 'retire_subtask'

    def remove_subtasks(self):
        '''Removes all subtasks AND messages for this task'''
        M.Message.m.remove({ 'task_id': self.id })
        M.TaskState.m.remove({ 'parent_id': self.id })
        
    
