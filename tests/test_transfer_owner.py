"""Real kernel locks and own synthetic spawn/kill, no PID stop inference."""
from contextlib import ExitStack
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from document_library.storage import StorageRoot, StorageError
from document_library.transfer_owner import TransferOwner, stopped_owner, OWNER_DIRECTORY
from test_storage import SyntheticVolume


def hold_owner(path, pipe):
    with StorageRoot(Path(path),SyntheticVolume()) as root:
        owner=TransferOwner(root)
        pipe.send(owner.descriptor)
        pipe.recv()
        owner.close()


class TransferOwnerAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='document-library-transfer-owner-')
        self.path=Path(self.temp.name).resolve()
        self.root=StorageRoot(self.path,SyntheticVolume())
        self.owners=[]
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for owner in self.owners:
            owner.close()
        self.root.close()
        self.temp.cleanup()
        self.assertFalse(self.path.exists())

    def owner(self):
        owner=TransferOwner(self.root)
        self.owners.append(owner)
        return owner

    def test_live_owner_busy_then_same_inode_stop_proof_and_no_file_deletion(self):
        owner=self.owner()
        row=json.loads(owner.descriptor)
        path=self.path/OWNER_DIRECTORY/(row['uuid']+'.lock')
        with stopped_owner(self.root,owner.descriptor) as proof:
            self.assertIsNone(proof)
        owner.close()
        self.assertTrue(path.exists())
        with stopped_owner(self.root,owner.descriptor) as proof:
            self.assertEqual(proof,owner.descriptor)
            with stopped_owner(self.root,owner.descriptor) as other:
                self.assertIsNone(other)

    def test_missing_replaced_symlink_and_malformed_owner_remain_uncertain(self):
        owner=self.owner()
        descriptor=owner.descriptor
        row=json.loads(descriptor)
        path=self.path/OWNER_DIRECTORY/(row['uuid']+'.lock')
        owner.close()
        path.unlink()
        with stopped_owner(self.root,descriptor) as proof:
            self.assertIsNone(proof)
        self.assertFalse(path.exists(),'Recovery must not create a missing owner')
        path.write_bytes(b'new inode')
        path.chmod(0o600)
        with stopped_owner(self.root,descriptor) as proof:
            self.assertIsNone(proof)
        path.unlink()
        path.symlink_to(self.path/'nonexistent')
        with stopped_owner(self.root,descriptor) as proof:
            self.assertIsNone(proof)
        for invalid in ('{}','not json',json.dumps(dict(v=1,uuid='../escape',dev=1,ino=1))):
            with stopped_owner(self.root,invalid) as proof:
                self.assertIsNone(proof)

    def test_shutdown_does_not_unlock_blocked_actual_thread(self):
        owner=self.owner()
        unblock=threading.Event()
        class Handle:
            closed=threading.Event()
            def request_stop(self,*_args):
                pass
            def wait_closed(self):
                self.closed.wait()
            def wait_exited(self,timeout=None):
                self.thread.join(timeout)
                return not self.thread.is_alive()
        handle=Handle()
        def run():
            unblock.wait()
            handle.closed.set()
        handle.thread=threading.Thread(target=run)
        handle.thread.start()
        self.addCleanup(unblock.set)
        owner.track(handle)
        self.assertFalse(owner.close(wait=False))
        with stopped_owner(self.root,owner.descriptor) as proof:
            self.assertIsNone(proof)
        with self.assertRaises(StorageError),owner.admitting():
            pass
        unblock.set()
        handle.thread.join()
        self.assertTrue(owner.close())
        with stopped_owner(self.root,owner.descriptor) as proof:
            self.assertIsNotNone(proof)

    def test_spawn_kill_releases_actual_owner_kernel_lock(self):
        context=multiprocessing.get_context('spawn')
        parent,child=context.Pipe()
        process=context.Process(target=hold_owner,args=(str(self.path),child))
        process.start()
        child.close()
        self.addCleanup(parent.close)
        def cleanup_process():
            if process.is_alive():
                process.kill()
            process.join()
        self.addCleanup(cleanup_process)
        self.assertTrue(parent.poll(10))
        descriptor=parent.recv()
        with stopped_owner(self.root,descriptor) as proof:
            self.assertIsNone(proof)
        process.kill()
        process.join(10)
        self.assertFalse(process.is_alive())
        with stopped_owner(self.root,descriptor) as proof:
            self.assertEqual(proof,descriptor)

    def test_volume_verifier_exception_still_stops_and_closes_known_threads(self):
        owner=self.owner()
        stop,release,closed=(threading.Event() for _ in range(3))
        class Handle:
            def request_stop(self,*_args):
                stop.set()
            def wait_closed(self,timeout=None):
                return closed.wait(timeout)
            def wait_exited(self,timeout=None):
                self.thread.join(timeout)
                return not self.thread.is_alive()
        handle=Handle()
        handle.closed=closed
        def run():
            stop.wait()
            release.wait()
            closed.set()
        handle.thread=threading.Thread(target=run)
        handle.thread.start()
        owner.track(handle)
        try:
            with patch.object(owner,'validate',side_effect=RuntimeError('synthetic volume verifier')):
                with self.assertRaises(RuntimeError):
                    owner.close(wait=False)
                self.assertTrue(stop.is_set())
                self.assertFalse(owner._closed)
                release.set()
                with self.assertRaises(RuntimeError):
                    owner.close(wait=True)
                self.assertTrue(owner._closed)
        finally:
            stop.set();release.set();handle.thread.join()


if __name__=='__main__':
    unittest.main()
