from chapman import model as M

from .t_base import Task


class Composite(Task):

    @classmethod
    def n(cls, *subtasks):
        return cls.new(subtasks)

    @classmethod
    def new(cls, subtasks, **options):
        self = super(Composite, cls).new(
            dict(n_subtask=0, n_waiting=0),
            'pending', **options)
        for st in subtasks:
            self.append(st)
        return self

    def append(self, st, start=False):
        position = self._state.data.n_subtask
        msg = st.link(self, 'retire_subtask', position)
        msg.s.pri = st._state.options['priority'] + 1
        msg.m.update_partial(
            {'_id': msg._id},
            {'$set': {'s.pri', msg.s.pri}})
        st._state.m.set({
            'parent_id': self.id,
            'data.composite_position': position,
            'options.ignore_result': False,
        })
        M.TaskState.m.update_partial(
            {'_id': self.id},
            {'$inc': {
                'data.n_subtask': 1,
                'data.n_waiting': 1}})
        self._state.data.n_subtask += 1
        self._state.data.n_waiting += 1
        if start:
            st.start()

    def subtask_iter(self):
        q = M.TaskState.m.find({'parent_id': self.id})
        q = q.sort('data.composite_position')
        return q

    def run(self, msg):
        raise NotImplementedError('run')

    def retire_subtask(self, msg):
        raise NotImplementedError('retire_subtask')

    def remove_subtasks(self):
        '''Removes all subtasks AND messages for this task'''
        M.Message.m.remove({'task_id': self.id})
        M.TaskState.m.remove({'parent_id': self.id})
