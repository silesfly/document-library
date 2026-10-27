"""Private inode-bound flock owner; admission TTL is never physical stop proof."""
from contextlib import ExitStack, contextmanager
import fcntl
import json
import os
import threading
import uuid

from .storage import StorageError, _identity, sync_file


OWNER_DIRECTORY = 'jobs/locks/download-owners'


def _description(value):
    try:
        row = json.loads(value)
        if (not isinstance(row,dict) or set(row) != {'v','uuid','dev','ino'} or row['v'] != 1
                or str(uuid.UUID(row['uuid'])) != row['uuid']
                or any(type(row[key]) is not int or row[key] < 0 for key in ('dev','ino'))):
            raise ValueError('Invalid owner descriptor')
        return row
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise StorageError('Invalid transfer owner descriptor') from exc


def _verify(root, parent, fd, name, row):
    info = root._check(fd,directory=False)
    path = os.stat(name,dir_fd=parent,follow_symlinks=False)
    if _identity(info) != (row['dev'],row['ino']) or _identity(path) != _identity(info):
        raise StorageError('Transfer owner inode changed')
    root.validate()


@contextmanager
def stopped_owner(root, descriptor):
    """None means busy/uncertain. Never creates a missing lock or guesses a PID."""
    stack = ExitStack()
    try:
        row = _description(descriptor)
        parent = stack.enter_context(root.directory(OWNER_DIRECTORY))
        name = row['uuid']+'.lock'
        fd = stack.enter_context(root.file(OWNER_DIRECTORY+'/'+name))
        _verify(root,parent,fd,name,row)
        fcntl.flock(fd,fcntl.LOCK_EX | fcntl.LOCK_NB)
        _verify(root,parent,fd,name,row)
    except (StorageError, OSError):
        stack.close()
        yield None
        return
    try:
        # Descriptor stays exclusively held throughout the caller's terminal writer.
        yield descriptor
    finally:
        stack.close()


class TransferOwner:
    def __init__(self, root):
        self.root, self._stack = root, ExitStack()
        self._mutex, self._handles = threading.RLock(), set()
        self._process, self._accepting, self._closed = os.getpid(), True, False
        self.close_error = None
        ident = str(uuid.uuid4())
        self._name = ident+'.lock'
        try:
            self._parent = self._stack.enter_context(root.directory(OWNER_DIRECTORY,create=True))
            self._fd = self._stack.enter_context(root.file(OWNER_DIRECTORY+'/'+self._name,create=True))
            fcntl.flock(self._fd,fcntl.LOCK_EX | fcntl.LOCK_NB)
            info = root._check(self._fd,directory=False)
            self._row = dict(v=1,uuid=ident,dev=info.st_dev,ino=info.st_ino)
            self.descriptor = json.dumps(self._row,sort_keys=True,separators=(',',':'))
            sync_file(self._fd)
            os.fsync(self._parent)
            self.validate()
        except BaseException:
            self._stack.close()
            raise

    def validate(self):
        self._assert_process()
        if self._closed:
            raise StorageError('Transfer owner belongs to its original live process')
        _verify(self.root,self._parent,self._fd,self._name,self._row)

    def _assert_process(self):
        if self._process != os.getpid():
            raise StorageError('Transfer owner belongs to its original live process')

    @contextmanager
    def admitting(self):
        with self._mutex:
            self.validate()
            if not self._accepting:
                raise StorageError('Transfer owner is shutting down')
            yield

    def track(self, handle):
        with self._mutex:
            self.validate()
            if not self._accepting:
                raise StorageError('Transfer owner is shutting down')
            self._handles.add(handle)

    def forget(self, handle):
        with self._mutex:
            if not self._stopped(handle):
                raise StorageError('Actual producer exit required')
            self._handles.discard(handle)

    @staticmethod
    def _stopped(handle):
        never_started = getattr(handle,'never_started',None)
        proven = handle.closed.is_set() or (never_started is not None and never_started.is_set())
        return proven and handle.wait_exited(0)

    def close(self, *, wait=True, release=True):
        """Stop known local threads even when pathname validation fails.

        release=False retains the owner descriptor for application finalizers.
        A corrupt pathname remains an observable error, never a new identity.
        """
        error = None
        with self._mutex:
            self._assert_process()
            if self._closed:
                return True
            self._accepting = False
            handles = tuple(self._handles)
            try:
                self.validate()
            except BaseException as caught:
                error = self.close_error = caught
            for handle in handles:
                handle.request_stop('owner_shutdown')
        if wait:
            for handle in handles:
                handle.wait_closed()
                handle.wait_exited()
        with self._mutex:
            stopped = all(self._stopped(handle) for handle in self._handles)
            if stopped and release:
                self._handles.clear()
                try:
                    self._stack.close()
                finally:
                    self._closed = True
        if error is not None:
            raise error
        return stopped
