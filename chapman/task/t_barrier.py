import sys
import logging

from chapman import model as M

from .t_base import Result
from .t_group import Group

log = logging.getLogger(__name__)


class Barrier(Group):
    '''Just like a group, but doesn't care about the sub-results'''

    def retire(self):
        for st in self.subtask_iter():
            if st.status == 'failure':
                self._state.m.set({'status': 'fail-child'})
                return  # group isn't really done
            elif st.status != 'success':
                return  # group isn't really done
        self.remove_subtasks()
        self.complete(Result.success(self._state._id, None))
