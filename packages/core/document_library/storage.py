"""Local storage authority. Callers still own publication/retention DB policy."""
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
import fcntl
import os
from pathlib import Path, PurePosixPath
import plistlib
import re
import stat
import subprocess
import sys
import threading
import uuid


class StorageError(RuntimeError):
    pass


class StorageBusy(StorageError):
    pass


class LockOrderError(StorageError):
    pass


def _parts(relative):
    if not isinstance(relative, str) or not relative or str(PurePosixPath(relative)) != relative:
        raise StorageError("Canonical relative path required")
    parts = relative.split('/')
    if any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', part) for part in parts):
        raise StorageError("Unsafe path component")
    return parts


def _identity(info):
    return info.st_dev, info.st_ino


def _open_absolute_directory(path):
    """Walk every ancestor without following symlinks, including the leaf."""
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise StorageError("Absolute physical path required")
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        for name in path.parts[1:]:
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            previous, fd = fd, child
            os.close(previous)
        return fd
    except BaseException:
        os.close(fd)
        raise


@dataclass(frozen=True)
class MacAPFSVolume:
    mountpoint: Path
    volume_uuid: str

    def verify(self, root, root_fd):
        if sys.platform != 'darwin':
            raise StorageError("Production storage requires qualified macOS APFS")
        if not self.mountpoint.is_absolute() or root == self.mountpoint or self.mountpoint not in root.parents:
            raise StorageError("Storage root must be inside the approved mount")
        mount_fd = _open_absolute_directory(self.mountpoint)
        try:
            device = os.fstat(mount_fd).st_dev
            if not os.path.ismount(self.mountpoint) or device != os.fstat(root_fd).st_dev:
                raise StorageError("Expected mounted filesystem is absent or changed")
            raw = subprocess.run(['/usr/sbin/diskutil', 'info', '-plist', str(self.mountpoint)],
                                 check=True, capture_output=True, timeout=10).stdout
            info = plistlib.loads(raw)
            if (info.get('VolumeUUID') != self.volume_uuid or info.get('MountPoint') != str(self.mountpoint)
                    or info.get('FilesystemType') != 'apfs' or info.get('Writable') is not True
                    or info.get('GlobalPermissionsEnabled') is not True):
                raise StorageError("Volume identity, format or write authority differs")
            if _identity(os.stat(self.mountpoint, follow_symlinks=False)) != _identity(os.fstat(mount_fd)):
                raise StorageError("Mount changed during verification")
        finally:
            os.close(mount_fd)


def sync_file(fd):
    os.fsync(fd)
    if sys.platform == 'darwin':
        # macOS fsync alone need not flush a drive's volatile write cache.
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)


class StorageRoot:
    """An already provisioned private root. Never creates a missing volume/root.

    volume is a trusted local verifier, not a setting accepted from an HTTP user.
    Production supplies MacAPFSVolume; tests supply a disposable-volume verifier.
    """
    def __init__(self, path, volume):
        self.path = Path(path)
        self.volume = volume
        self.uid = os.geteuid()
        self.fd = _open_absolute_directory(self.path)
        self.device = os.fstat(self.fd).st_dev
        try:
            self.validate()
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _check(self, fd, *, directory):
        info = os.fstat(fd)
        kind = stat.S_ISDIR if directory else stat.S_ISREG
        if (not kind(info.st_mode) or info.st_uid != self.uid or info.st_dev != self.device
                or info.st_mode & 0o077 or (not directory and info.st_nlink != 1)):
            raise StorageError("Storage entry must be private, owned, same-device and of the expected type")
        return info

    def validate(self):
        if self.fd is None:
            raise StorageError("Storage root is closed")
        self._check(self.fd, directory=True)
        current = _open_absolute_directory(self.path)
        try:
            if _identity(os.fstat(current)) != _identity(os.fstat(self.fd)):
                raise StorageError("Storage root was replaced")
            self.volume.verify(self.path, self.fd)
        finally:
            os.close(current)

    @contextmanager
    def directory(self, relative, *, create=False):
        parts = _parts(relative)
        self.validate()
        fd = os.dup(self.fd)
        try:
            for name in parts:
                if create:
                    try:
                        os.mkdir(name, mode=0o700, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    self._check(child, directory=True)
                except BaseException:
                    os.close(child)
                    raise
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    @contextmanager
    def file(self, relative, *, create=False):
        """Exclusive creation or read-only open. Never replaces existing bytes."""
        parts = _parts(relative)
        if len(parts) < 2:
            raise StorageError("Files must be in a service subdirectory")
        with self.directory('/'.join(parts[:-1]), create=create) as parent:
            flags = (os.O_WRONLY | os.O_CREAT | os.O_EXCL) if create else os.O_RDONLY
            fd = os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
            try:
                self._check(fd, directory=False)
                yield fd
                if create:
                    sync_file(fd)
                    os.fsync(parent)
            finally:
                os.close(fd)

    def move(self, source, target):
        """Caller holds global/object locks and has verified content/ownership.

        Same-device, no replacement; no unlink of a conflicting destination.
        """
        src, dst = PurePosixPath(source), PurePosixPath(target)
        with self.file(source), self.directory(str(src.parent)) as src_fd, self.directory(str(dst.parent),create=True) as dst_fd:
            try:
                os.stat(dst.name,dir_fd=dst_fd,follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise StorageError('Refusing to replace an existing destination')
            os.rename(src.name,dst.name,src_dir_fd=src_fd,dst_dir_fd=dst_fd)
            os.fsync(dst_fd)
            os.fsync(src_fd)

    def sync_parent(self, path):
        with self.directory(str(PurePosixPath(path).parent)) as parent:
            os.fsync(parent)

    def sync_existing(self, path):
        """Flush verified existing bytes without replacing or writing content.

        Caller owns mutation authority and its current content verification.
        SMB FULLFSYNC requires a write-capable descriptor, unlike local APFS.
        """
        value = PurePosixPath(path)
        with self.file(path) as reference, self.directory(str(value.parent)) as parent:
            fd = os.open(value.name,os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK,dir_fd=parent)
            try:
                info = self._check(fd,directory=False)
                if _identity(info) != _identity(os.fstat(reference)):
                    raise StorageError('File changed before synchronization')
                sync_file(fd)
            finally:
                os.close(fd)
            os.fsync(parent)

    def unlink(self, path):
        """Caller owns current reference/policy checks and exclusive object lock."""
        value = PurePosixPath(path)
        with self.file(path), self.directory(str(value.parent)) as parent:
            os.unlink(value.name,dir_fd=parent)
            os.fsync(parent)

    @contextmanager
    def stage(self, lease):
        # The lease provides namespacing, not current publication authority.
        relative = f'{lease.staging_key}/{uuid.uuid4().hex}.part'
        with self.file(relative, create=True) as fd:
            yield relative, fd


_lock_state = threading.local()


class StorageLocks:
    """Synchronous, process-common flock; lock files are never removed/replaced.

    A held reader can outlive the global lock. Do not fork while holding locks;
    create worker processes before opening storage (or use spawn).
    Enter, body and exit must run on the same OS thread. Do not hand a live
    context/iterator to a thread pool; HTTP streaming adapters are not wired yet.
    """
    def __init__(self, root):
        self.root = root

    @contextmanager
    def _operation(self):
        if getattr(_lock_state, 'held', False):
            raise LockOrderError("Do not nest storage/backup/reader lock operations")
        _lock_state.held = True
        try:
            yield
        finally:
            _lock_state.held = False

    @contextmanager
    def _lock(self, relative, *, shared=False, blocking=True):
        parts = _parts(relative)
        with self.root.directory('/'.join(parts[:-1]), create=True) as parent:
            fd = os.open(parts[-1], os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
            try:
                info = self.root._check(fd, directory=False)
                op = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                try:
                    fcntl.flock(fd, op | (0 if blocking else fcntl.LOCK_NB))
                except BlockingIOError as exc:
                    raise StorageBusy("Storage object is in use") from exc
                if _identity(os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)) != _identity(info):
                    raise StorageError("Lock entry changed during acquisition")
                self.root.validate()
                yield
            finally:
                # close releases this open description's lock on every exit path.
                os.close(fd)

    def _objects(self, stack, shas, *, shared, blocking):
        values = sorted(set(shas))
        if any(not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{64}', sha) for sha in values):
            raise StorageError("Canonical SHA-256 required")
        for sha in values:
            stack.enter_context(self._lock(f'jobs/locks/objects/{sha[:2]}/{sha}.lock', shared=shared, blocking=blocking))

    @contextmanager
    def mutation(self, shas=(), *, blocking=True):
        """Global -> sorted exclusive object locks -> caller's SQLite writer.

        GC MUST pass blocking=False and skip StorageBusy, never wait for a reader.
        """
        with self._operation(), self._lock('jobs/locks/storage-mutation.lock', blocking=blocking):
            with ExitStack() as objects:
                self._objects(objects, shas, shared=False, blocking=blocking)
                yield

    @contextmanager
    def readers(self, shas, open_files, *, blocking=True):
        """Open under global protection, then retain only shared locks for streaming.

        open_files(stack) must check current authority/LIVE state and register all
        file contexts on stack before returning. The owning service supplies it.
        """
        with self._operation(), ExitStack() as objects, ExitStack() as streams:
            with self._lock('jobs/locks/storage-mutation.lock', blocking=blocking):
                self._objects(objects, shas, shared=True, blocking=False)
                opened = open_files(streams)
            yield opened

    @contextmanager
    def backup_mutation(self, *, blocking=True):
        # Separate local lock. Storage and backup locks may never overlap.
        with self._operation(), self._lock('jobs/locks/backup-mutation.lock', blocking=blocking):
            yield
