from ming import Session, Field
from ming import schema as S
from ming.declarative import Document

class Task(Document):

    def __init__(self, id):
        self._id = id
    
    def run(self, msg):
        '''Do the work of the task'''
        raise NotImplementedError, 'run'

    def start(self, *args, **kwargs):
        '''Set the task to ready status and send a 'run' message'''
        TaskState.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': 'ready' } } )
        msg = Message.s(self._id, 'run', *args, **kwargs)
        msg.post()

    @classmethod
    def s(cls):
        return cls()

    def complete(self, result):
        doc = Task.m.get(_id=self._id)
        if doc.on_complete:
            msg = Message.m.get(_id=doc.on_complete)
            msg.post()
            self.m.delete()
        elif doc.ignore_result:
            self.m.delete()
        else:
            Task.m.update_partial(
                { '_id': self._id },
                { '$set': { 'result': result,
                            'status': 'complete' } } )

class Group(Task):

    @classmethod
    def s(cls, subtask_ids):
        self = super(Group, cls).s()
        for id in subtask_ids:
            self.append(id)
        return self

    def run(self):
        '''Suspend the group and start all pending elements'''
        Task.m.update_partial(
            { '_id': self._id },
            { '$set': { 'status': 'suspend' } } )
        for st in Task.m.find(
            { 'parent_id': self._id, 'status': 'pending' }):
            st.start()

    def append(self, subtask_id):
        '''Link the given (pending) subtask_id to the group'''
        doc = Task.m.find_and_modify(
            { '_id': self._id },
            { '$inc': { 'data.n_el': 1 } },
            new=True)
        return _El.s(self.id, doc['data']['n_el']-1, subtask_id)
        
    def retire_group(self):
        '''Called by the last element in the group to save the result
        and handle '''
        q = Task.m.find({'parent_id': self._id })
        q = q.sort('index')
        results = []
        for t in q:
            results.append(t.result)
            t.m.delete()
        self.complete(GroupResult(results))

class _El(Task):
    '''Elements exist as glue between composite tasks and their subtasks. They
    remeber their position within the composite as well as the result of the
    subtask.
    '''

    @classmethod
    def s(cls, parent_id, index, subtask_id):
        self = super(_El, cls).s()
        cls.m.update_partial(
            { '_id': self._id },
            { '$set': {
                    'parent_id': parent_id,
                    'data': index } })
        retire_msg = Message.s(self._id, 'retire')
        cls.m.update_partial(
            { '_id': subtask_id },
            { '$set': { 'on_complete': retire_msg._id } } )
        return self
    
    def target(self):
        for t in Task.m.find(
            { 'parent_id': self.group_id,
              'status': { '$in': [ 'pending', 'busy' ] } }):
            return
        group = Task.get(self.group_id)
        group.retire()
        self.complete()

class Message(object): pass
class Result(object): pass
class GroupResult(Result): pass
