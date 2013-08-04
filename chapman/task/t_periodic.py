import logging
from cPickle import dumps
from datetime import datetime, timedelta

import bson

from chapman import model as M

from .t_base import Task
from .t_composite import Composite

log = logging.getLogger(__name__)


class Periodic(Composite):

    def __repr__(self):
        return '<Periodic %s (%s s) %s>' % (
            self._state._id,
            self._state.data.interval,
            self._state.data.subtask_repr)

    @classmethod
    def schedule(cls, subtask, first, interval, *args, **kwargs):
        '''First is a timestamp, interval is in seconds'''
        t = cls.n()
        t._state.data.update(
            next=first,
            interval=interval,
            subtask_id=subtask.id,
            subtask_repr=repr(subtask),
            args=bson.Binary(dumps(args)),
            kwargs=bson.Binary(dumps(kwargs)))
        t._state.status = 'active'
        t._state.m.save()
        t._schedule_subtask(subtask)
        return t

    def cancel(self):
        M.TaskState.m.remove(dict(parent_id=self._state._id))
        M.Message.m.remove(
            dict(task_id={'$in': [
                self._state._id, self._state.data.subtask_id]}))
        self._state.m.delete()

    def schedule_options(self):
        return dict(
            q=self._state.options.queue,
            pri=self._state.options.priority,
            after=self._state.data.next)

    def reschedule_subtask(self, msg=None):
        msg.m.delete()
        st_state = M.TaskState.m.get(_id=self._state.data.subtask_id)
        st = Task.from_state(st_state)
        self._schedule_subtask(st)

    def _schedule_subtask(self, subtask):
        td_interval = timedelta(seconds=self._state.data.interval)
        next = self._state.data.next + td_interval
        while next < datetime.utcnow():
            next += td_interval
        subtask.link(self, 'reschedule_subtask')
        self._state.m.set({'data.next': next})
        subtask._state.m.set({
            'parent_id': self.id,
            'status': 'pending',
            'options.ignore_result': False})
        msg = M.Message.make(dict(
            slot='run',
            task_id=self._state.data.subtask_id,
            task_repr=self._state.data.subtask_repr,
            args=self._state.data.args,
            kwargs=self._state.data.kwargs))
        msg.s.after = next
        msg.m.insert()
        msg.send()
