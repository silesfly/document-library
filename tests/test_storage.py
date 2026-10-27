"""Filesystem authority and real process locks in disposable synthetic roots."""
from dataclasses import replace
import multiprocessing
import os
from pathlib import Path
import plistlib
import tempfile
import unittest
from unittest.mock import patch
import uuid

from document_library.jobs import Lease
from document_library.storage import (
    StorageRoot, StorageLocks, MacAPFSVolume, StorageError, StorageBusy, LockOrderError,
)


class SyntheticVolume:
    """Test-only verifier: never selected by an application configuration."""
    enabled = True

    def verify(self, root, root_fd):
        if not self.enabled or root.stat().st_dev != os.fstat(root_fd).st_dev:
            raise StorageError('Synthetic volume unavailable')


def hold_in_child(path, mode, shas, channel):
    try:
        with StorageRoot(path, SyntheticVolume()) as root:
            locks = StorageLocks(root)
            if mode == 'reader':
                operation = locks.readers(shas, lambda stack: None)
            elif mode == 'backup':
                operation = locks.backup_mutation(blocking=False)
            else:
                operation = locks.mutation(shas, blocking=False)
            try:
                with operation:
                    channel.send('held')
                    channel.recv()
            except StorageBusy:
                channel.send('busy')
    except BaseException as exc:
        channel.send(('error', type(exc).__name__, str(exc)))
    finally:
        channel.close()


class StorageAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='document-library-storage-')
        self.base = Path(self.temp.name).resolve()
        self.path = self.base / 'storage'
        self.path.mkdir(mode=0o700)
        self.volume = SyntheticVolume()
        self.root = StorageRoot(self.path, self.volume)
        self.locks = StorageLocks(self.root)
        self.children = []
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for process, channel in self.children:
            if process.is_alive():
                try:
                    channel.send('release')
                except (BrokenPipeError, EOFError, OSError):
                    pass
                process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(5)
            self.assertFalse(process.is_alive(), 'Test worker must terminate')
            channel.close()
            process.close()
        self.root.close()
        self.temp.cleanup()
        self.assertFalse(self.base.exists(), 'Synthetic root and lock/staging files must be absent')

    def child(self, mode='mutation', shas=(), expected='held'):
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        process = context.Process(target=hold_in_child, args=(str(self.path), mode, shas, child))
        process.start()
        child.close()
        self.children.append((process, parent))
        self.assertTrue(parent.poll(10), 'Worker did not report lock acquisition')
        self.assertEqual(parent.recv(), expected)
        return process, parent

    def test_missing_root_symlink_ancestors_and_nonprivate_root_are_rejected(self):
        with self.assertRaises(FileNotFoundError):
            StorageRoot(self.base / 'absent', self.volume)
        self.assertFalse((self.base / 'absent').exists())
        link = self.base / 'alias'
        link.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(OSError):
            StorageRoot(link / 'storage', self.volume)
        self.path.chmod(0o755)
        with self.assertRaises(StorageError):
            StorageRoot(self.path, self.volume)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o755)
        self.path.chmod(0o700)

    def test_apfs_inspector_rejects_wrong_identity_format_and_authority(self):
        mount = self.base
        good = dict(VolumeUUID='synthetic-volume', MountPoint=str(mount), FilesystemType='apfs',
                    Writable=True, GlobalPermissionsEnabled=True)
        verifier = MacAPFSVolume(mount, 'synthetic-volume')
        with patch('document_library.storage.sys.platform', 'darwin'), patch('os.path.ismount', return_value=True):
            with patch('document_library.storage.subprocess.run') as command:
                command.return_value.stdout = plistlib.dumps(good)
                verifier.verify(self.path, self.root.fd)
                for field, wrong in [('VolumeUUID','other'), ('MountPoint',str(self.path)),
                                     ('FilesystemType','smbfs'), ('Writable',False), ('GlobalPermissionsEnabled',False)]:
                    with self.subTest(field=field):
                        command.return_value.stdout = plistlib.dumps(dict(good, **{field:wrong}))
                        with self.assertRaises(StorageError):
                            verifier.verify(self.path, self.root.fd)
                command.return_value.stdout = plistlib.dumps(good)
                with patch('os.path.ismount', return_value=False):
                    with self.assertRaises(StorageError):
                        verifier.verify(self.path, self.root.fd)

    def test_volume_recheck_and_root_replacement_block_writes(self):
        self.volume.enabled = False
        with self.assertRaises(StorageError):
            with self.root.file('incoming/rejected', create=True):
                self.fail('Unavailable volume admitted a write')
        self.assertFalse((self.path / 'incoming').exists())
        self.volume.enabled = True
        self.path.rename(self.base / 'original')
        self.path.mkdir(mode=0o700)
        with self.assertRaises(StorageError):
            with self.root.file('incoming/rejected', create=True):
                self.fail('Replacement root admitted a write')
        self.assertEqual(list(self.path.iterdir()), [])

    def test_relative_paths_symlinks_hardlinks_and_special_files_are_rejected(self):
        for relative in ['../outside/x','/outside/x','incoming/../x','incoming//x','incoming/./x','incoming\\x']:
            with self.subTest(relative=relative), self.assertRaises(StorageError):
                with self.root.file(relative, create=True):
                    self.fail('Unsafe path admitted')
        outside = self.base / 'outside'
        outside.mkdir(mode=0o700)
        (self.path / 'escape').symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            with self.root.file('escape/created', create=True):
                self.fail('Symlink followed')
        self.assertEqual(list(outside.iterdir()), [])
        with self.root.file('incoming/normal', create=True) as fd:
            os.write(fd, b'synthetic')
        os.link(self.path / 'incoming/normal', self.path / 'incoming/alias')
        os.mkfifo(self.path / 'incoming/fifo', 0o600)
        for name in ['alias','fifo']:
            with self.subTest(name=name), self.assertRaises(StorageError):
                with self.root.file('incoming/' + name):
                    self.fail('Unsafe entry opened')

    def test_staging_is_unique_per_generation_and_never_overwrites(self):
        lease = Lease(str(uuid.uuid4()), 1, 'worker-a')
        paths = []
        for attempt in [lease, lease, replace(lease,generation=2,owner='worker-b')]:
            with self.root.stage(attempt) as (relative, fd):
                os.write(fd, b'complete synthetic payload')
                paths.append(relative)
        self.assertEqual(len(set(paths)), 3)
        self.assertNotEqual(Path(paths[0]).parent, Path(paths[2]).parent)
        with self.assertRaises(FileExistsError):
            with self.root.file(paths[0], create=True):
                self.fail('Existing stage overwritten')
        with self.root.file(paths[0]) as fd:
            self.assertEqual(os.read(fd, 100), b'complete synthetic payload')

    def test_global_lock_serializes_processes_and_survives_process_exit(self):
        process, channel = self.child()
        path = self.path / 'jobs/locks/storage-mutation.lock'
        inode = path.stat().st_ino
        with self.assertRaises(StorageBusy):
            with self.locks.mutation(blocking=False):
                self.fail('Concurrent global mutation admitted')
        process.terminate()
        process.join(5)
        self.assertFalse(process.is_alive())
        with self.locks.mutation(blocking=False):
            self.assertEqual(path.stat().st_ino, inode)
        self.assertEqual(path.stat().st_ino, inode)

    def test_reader_releases_global_but_keeps_object_lock_for_stream_lifetime(self):
        sha = 'a' * 64
        with self.root.file('objects/synthetic', create=True) as fd:
            os.write(fd, b'payload')
        def open_file(stack):
            # Real publication service must additionally check current DB LIVE/authority.
            self.child(expected='busy')
            return stack.enter_context(self.root.file('objects/synthetic'))
        with self.locks.readers([sha], open_file) as fd:
            self.child(shas=[sha], expected='busy')
            process, channel = self.child(shas=['b' * 64])
            channel.send('release')
            process.join(5)
            self.assertEqual(os.read(fd, 100), b'payload')
        with self.assertRaises(OSError):
            os.fstat(fd)
        with self.locks.mutation([sha], blocking=False):
            pass

    def test_nonblocking_multilock_failure_releases_already_acquired_objects(self):
        process, channel = self.child(mode='reader', shas=['b' * 64])
        with self.assertRaises(StorageBusy):
            with self.locks.mutation(['b' * 64, 'a' * 64], blocking=False):
                self.fail('Reader excluded from multi-object GC')
        with self.locks.mutation(['a' * 64], blocking=False):
            pass
        channel.send('release')
        process.join(5)
        with self.locks.mutation(['b' * 64, 'a' * 64], blocking=False):
            pass

    def test_lock_order_and_failed_opener_release_all_locks(self):
        for operation in [self.locks.mutation, self.locks.backup_mutation]:
            with operation():
                with self.assertRaises(LockOrderError):
                    with self.locks.backup_mutation():
                        self.fail('Nested backup lock admitted')
                with self.assertRaises(LockOrderError):
                    with StorageLocks(self.root).mutation():
                        self.fail('Nested storage lock admitted')
        def fail_open(stack):
            stack.enter_context(self.root.file('incoming/partial', create=True))
            raise RuntimeError('synthetic permission rejection')
        with self.assertRaises(RuntimeError):
            with self.locks.readers(['c' * 64], fail_open):
                self.fail('Failed opener returned a stream')
        with self.locks.mutation(['c' * 64], blocking=False):
            pass

    def test_lock_symlink_and_hardlink_are_rejected(self):
        with self.locks.mutation():
            pass
        path = self.path / 'jobs/locks/storage-mutation.lock'
        os.link(path, path.with_name('unexpected-alias'))
        with self.assertRaises(StorageError):
            with self.locks.mutation(blocking=False):
                self.fail('Hardlinked lock admitted')
        path.unlink()
        path.symlink_to(path.with_name('unexpected-alias'))
        with self.assertRaises(OSError):
            with self.locks.mutation(blocking=False):
                self.fail('Symlink lock admitted')


class AbsoluteDirectoryOwnership(unittest.TestCase):
    def test_parent_close_exception_closes_child_once_without_retrying_parent(self):
        import errno
        from document_library.storage import _open_absolute_directory
        original_open, original_close = os.open, os.close
        with tempfile.TemporaryDirectory(prefix='document-library-dirfd-owner-') as folder:
            target = Path(folder) / 'child'
            target.mkdir()
            for release_before_error in (False, True):
                with self.subTest(release_before_error=release_before_error):
                    acquired, closed, close_calls = [], set(), []
                    injected = False
                    def acquire(*args, **kwargs):
                        descriptor = original_open(*args, **kwargs)
                        acquired.append(descriptor)
                        return descriptor
                    def release(descriptor):
                        nonlocal injected
                        close_calls.append(descriptor)
                        if acquired and descriptor == acquired[0] and not injected:
                            injected = True
                            if release_before_error:
                                original_close(descriptor)
                                closed.add(descriptor)
                            raise OSError(errno.EIO, 'Synthetic directory parent close failure')
                        original_close(descriptor)
                        closed.add(descriptor)
                    try:
                        with patch('document_library.storage.os.open', acquire), \
                                patch('document_library.storage.os.close', release):
                            with self.assertRaisesRegex(OSError, 'Synthetic directory parent close failure'):
                                _open_absolute_directory(target)
                        self.assertTrue(injected)
                        self.assertEqual(len(acquired), 2)
                        self.assertIn(acquired[1], closed, 'Successful child acquisition lost ownership')
                        self.assertEqual(close_calls.count(acquired[0]), 1,
                                         'Parent descriptor was retried after ambiguous close')
                        self.assertEqual(close_calls.count(acquired[1]), 1)
                        with self.assertRaises(OSError):
                            os.fstat(acquired[1])
                    finally:
                        for descriptor in acquired:
                            if descriptor not in closed:
                                original_close(descriptor)


if __name__ == '__main__':
    unittest.main()
