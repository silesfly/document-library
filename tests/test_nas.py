"""Synthetic NAS transfer, protection, conflicts and sealing boundaries."""
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import text

from document_library.backup import BackupError
from document_library.backup_coordinator import BackupCoordinator
from document_library.database import write_transaction
from document_library.jobs import LostLease
from document_library.nas import MacSMBVolume, NASBackupStore
from document_library.publication import PublicationError, object_path
from document_library.storage import StorageError, StorageRoot, LockOrderError
from test_storage import SyntheticVolume
import test_backup as fixtures


class NASAcceptance(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.SnapshotAcceptance()
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.h = self.f.h
        self.temp = tempfile.TemporaryDirectory(prefix='document-library-nas-')
        self.addCleanup(self.cleanup_remote)
        self.remote = StorageRoot(Path(self.temp.name).resolve(),SyntheticVolume())
        self.store = NASBackupStore(self.f.backups,self.remote)

    def cleanup_remote(self):
        self.remote.close()
        path = Path(self.temp.name)
        self.temp.cleanup()
        self.assertFalse(path.exists())

    def capture(self):
        _,_,asset = self.h.first_revision()
        lease = self.f.backup_job()
        manifest = self.f.backups.capture(lease)
        return lease,manifest,asset

    def test_complete_physical_set_and_retry_reuse_without_local_success_receipt(self):
        lease,manifest,asset = self.capture()
        result = self.store.transfer(lease)
        self.assertEqual(result['manifest'],manifest)
        self.assertEqual(self.store.verify_set(manifest['set_id']),result)
        self.assertEqual((self.remote.path/object_path(asset['sha256'])).read_bytes(),b'novel 1-150')
        self.h.clock.advance(10)
        with patch.object(self.store,'_stage',wraps=self.store._stage) as copied:
            self.assertEqual(self.store.transfer(lease),result)
            # Existing object/DB are re-read and verified, without copying their
            # bytes from SSD again. Only the small manifest is staged for retry.
            self.assertNotIn(asset['sha256'],[call.args[2]['sha256'] for call in copied.call_args_list])
            self.assertNotIn(manifest['database']['sha256'],[call.args[2]['sha256'] for call in copied.call_args_list])
        self.assertEqual(self.f.record()['state'],'BUILDING')
        self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'running')
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),0)
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)

    def test_building_manifest_precedes_original_and_local_reader_does_not_overlap_backup_lock(self):
        lease,manifest,asset = self.capture()
        stage = self.store._stage
        seen = []
        def inspect(set_id,source,expected):
            if expected['sha256'] == asset['sha256']:
                self.assertTrue((self.remote.path/self.store.paths(set_id)['manifest']).is_file())
                with self.assertRaises(LockOrderError):
                    with self.h.publisher.locks.backup_mutation():
                        pass
                seen.append(True)
            return stage(set_id,source,expected)
        with patch.object(self.store,'_stage',side_effect=inspect):
            self.store.transfer(lease)
        self.assertEqual(seen,[True])

    def test_close_reopen_hash_detects_remote_corruption_before_final_install(self):
        lease,manifest,asset = self.capture()
        stage = self.store._stage
        def corrupt(set_id,source,expected):
            path = stage(set_id,source,expected)
            if expected['sha256'] == asset['sha256']:
                (self.remote.path/path).write_bytes(b'corrupt')
            return path
        with patch.object(self.store,'_stage',side_effect=corrupt), self.assertRaises(PublicationError):
            self.store.transfer(lease)
        self.assertFalse((self.remote.path/object_path(asset['sha256'])).exists())
        self.assertFalse((self.remote.path/self.store.paths(manifest['set_id'])['seal']).exists())
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_pins'),1)

    def test_conflicting_shared_object_is_not_overwritten(self):
        lease,manifest,asset = self.capture()
        path = object_path(asset['sha256'])
        with self.remote.file(path,create=True) as fd:
            os.write(fd,b'conflicting-original')
        with self.assertRaises(PublicationError):
            self.store.transfer(lease)
        self.assertEqual((self.remote.path/path).read_bytes(),b'conflicting-original')
        self.assertFalse((self.remote.path/self.store.paths(manifest['set_id'])['seal']).exists())

    def test_cancel_after_copy_blocks_install_and_keeps_building_protection(self):
        lease,manifest,asset = self.capture()
        stage = self.store._stage
        def cancel(set_id,source,expected):
            path = stage(set_id,source,expected)
            if expected['sha256'] == asset['sha256']:
                self.h.jobs.cancel(lease.job_id)
            return path
        with patch.object(self.store,'_stage',side_effect=cancel), self.assertRaises(LostLease):
            self.store.transfer(lease)
        self.assertTrue((self.remote.path/self.store.paths(manifest['set_id'])['manifest']).is_file())
        self.assertFalse((self.remote.path/object_path(asset['sha256'])).exists())
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_pins'),1)

    def test_missing_or_changed_member_is_never_a_verified_sealed_set(self):
        lease,manifest,asset = self.capture()
        self.store.transfer(lease)
        path = self.remote.path/object_path(asset['sha256'])
        path.write_bytes(b'damaged')
        with self.assertRaises(PublicationError):
            self.store.verify_set(manifest['set_id'])
        path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.store.verify_set(manifest['set_id'])

    def test_seal_is_last_and_retry_reflushes_a_marker_after_sync_failure(self):
        lease,manifest,_ = self.capture()
        path = self.store.paths(manifest['set_id'])['seal']
        sync = self.remote.sync_existing
        def fail(target):
            if target == path:
                raise OSError('synthetic flush failure')
            return sync(target)
        with patch.object(self.remote,'sync_existing',side_effect=fail), self.assertRaises(OSError):
            self.store.transfer(lease)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),0)
        with patch.object(self.remote,'sync_existing',wraps=sync) as flushed:
            result = self.store.transfer(lease)
        self.assertIn(unittest.mock.call(path),flushed.call_args_list)
        self.assertEqual(self.store.verify_set(manifest['set_id']),result)

    def test_database_or_manifest_damage_rejects_seal(self):
        for name in ('database','manifest','seal'):
            with self.subTest(name=name):
                lease,manifest,_ = self.capture() if name == 'database' else (lease,manifest,None)
                self.store.transfer(lease)
                path = self.remote.path/self.store.paths(manifest['set_id'])[name]
                original = path.read_bytes()
                path.write_bytes(b'{}')
                with self.assertRaises((PublicationError,KeyError,ValueError)):
                    self.store.verify_set(manifest['set_id'])
                path.write_bytes(original)

    def test_symlinked_destination_and_unmounted_production_volume_are_rejected(self):
        lease,manifest,_ = self.capture()
        (self.remote.path/'sets').symlink_to(self.h.path,target_is_directory=True)
        with self.assertRaises((OSError,StorageError)):
            self.store.transfer(lease)
        with patch('document_library.nas.sys.platform','linux'), self.assertRaises(StorageError):
            MacSMBVolume(Path('/unmounted'),'//synthetic/share').verify(self.remote.path,self.remote.fd)

    def test_empty_library_is_a_valid_physical_set(self):
        result = self.store.transfer(self.f.backup_job())
        self.assertEqual(result['manifest']['objects'],[])
        self.assertEqual(self.store.verify_set(result['manifest']['set_id']),result)

    def test_sync_existing_never_flushes_a_replaced_file(self):
        with self.remote.file('synthetic/original.bin',create=True) as fd:
            os.write(fd,b'preserved-bytes')
        original = self.remote.path/'synthetic/original.bin'
        self.remote.sync_existing('synthetic/original.bin')
        self.assertEqual(original.read_bytes(),b'preserved-bytes')
        real_open = os.open
        def replace_before_write_open(path,flags,*args,**kwargs):
            if path == 'original.bin' and flags & os.O_WRONLY:
                original.rename(original.with_name('prior.bin'))
                original.write_bytes(b'replacement')
                original.chmod(0o600)
            return real_open(path,flags,*args,**kwargs)
        with patch('document_library.storage.os.open',side_effect=replace_before_write_open), self.assertRaisesRegex(StorageError,'changed before synchronization'):
            self.remote.sync_existing('synthetic/original.bin')
        self.assertEqual(original.read_bytes(),b'replacement')

    def test_archived_original_requires_its_real_prior_sealed_set(self):
        lease,manifest,asset = self.capture()
        prior = self.store.transfer(lease)
        BackupCoordinator(self.store).run(lease)
        damaged_set = str(uuid.uuid4())
        with self.remote.file(self.store.paths(damaged_set)['manifest'],create=True) as fd:
            os.write(fd,b'{')
        old = self.h.scalar('SELECT id FROM edition_revisions WHERE is_active=1')
        self.f.publish_next(old)
        self.h.jobs.cancel(self.h.scalar("SELECT id FROM jobs WHERE kind='backup_revision' AND input_revision_id=:id",id=old))
        # The coordinator supplies the valid receipt. A separate corrupted
        # receipt exercises fallback independently of that real success path.
        with write_transaction(self.h.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'),dict(id=old))
            conn.execute(text("UPDATE objects SET state='PURGED' WHERE sha256=:sha"),dict(sha=asset['sha256']))
            conn.execute(text("INSERT INTO object_backup_receipts VALUES('a-damaged-prior',:sha,:size,:set_id,:path,:manifest,:now,1)"),
                         dict(sha=asset['sha256'],size=asset['byte_size'],set_id=damaged_set,path=object_path(asset['sha256']),manifest='f'*64,now=self.h.clock.wall))
        (self.h.path/object_path(asset['sha256'])).unlink()
        result = self.store.transfer(self.f.backup_job())
        self.assertEqual([obj['sha256'] for obj in result['manifest']['archived_originals']],[asset['sha256']])
        (self.remote.path/object_path(asset['sha256'])).unlink()
        with self.assertRaisesRegex(BackupError,'physically verified'):
            self.store.verify_set(result['manifest']['set_id'])


if __name__ == '__main__':
    unittest.main()
