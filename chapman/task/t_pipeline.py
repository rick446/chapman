from chapman import model as M

from .t_composite import Composite

class Pipeline(Composite):

    def run(self, msg):
        for st_state in self.subtask_iter():
            st = self.from_state(st_state)
            st.start(*msg.args, **msg.kwargs)
            break

    def retire_subtask(self, msg):
        result, position = msg.args
        next_state = M.TaskState.m.find(
            { 'parent_id': self.id,
              'data.composite_position': position + 1 } ).first()
        if next_state is None:
            self.retire(result)
        else:
            next_task = self.from_state(next_state)
            next_task.start(result.get())

    def error(self, msg):
        self.retire(msg.args[0])

    def retire(self, result):
        result.task_id = self.id
        self.complete(result)
        self.remove_subtasks()
