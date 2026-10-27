"""Backup success, final ownership rollback and physically stopped cleanup."""
from dataclasses import replace
import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.backup import BackupError
from document_library.backup_coordinator import BackupCoordinator
from document_library.database import write_transaction
from document_library.jobs import LostLease, ClockBlocked
from document_library.lifecycle import LifecycleService
from document_library.publication import object_path
from document_library.storage import StorageBusy, StorageError
import test_nas as fixtures


class BackupCoordinatorAcceptance(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.NASAcceptance()
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.h, self.store = self.f.h, self.f.store
        self.coordinator = BackupCoordinator(self.store)

    def stop(self, lease):
        self.h.jobs.cancel(lease.job_id)
        return self.coordinator.confirm_stopped(lease,disposition='cancelled',confirmation='Synthetic original process exit observed')

    def assert_protected(self, set_id):
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup' AND reference_id=:id",id=set_id),1)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts WHERE set_id=:id',id=set_id),0)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM backup_execution_stops WHERE set_id=:id',id=set_id),0)

    def test_success_commits_receipt_stop_job_and_pin_handoff_once(self):
        lease,manifest,asset = self.f.capture()
        result = self.coordinator.run(lease)
        self.assertEqual(result['set_id'],manifest['set_id'])
        self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'completed')
        self.assertEqual(self.f.f.record()['state'],'SEALED')
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),0)
        with self.h.engine.connect() as conn:
            receipt = conn.execute(text('SELECT * FROM object_backup_receipts')).mappings().one()
            self.assertEqual((receipt['object_sha'],receipt['byte_size'],receipt['set_id'],receipt['manifest_sha'],receipt['valid']),
                             (asset['sha256'],asset['byte_size'],manifest['set_id'],result['manifest_sha256'],1))
            self.assertEqual(receipt['backup_relpath'],object_path(asset['sha256']))
        with patch.object(self.store,'transfer',side_effect=AssertionError('Completed execution must not recapture')):
            self.assertEqual(self.coordinator.run(lease),result)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),1)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM backup_execution_stops'),1)
        self.assertEqual(self.store.verify_set(manifest['set_id'])['manifest'],manifest)
        with self.assertRaisesRegex(BackupError,'execution differs'):
            self.coordinator.run(replace(lease,owner='another-owner'))

    def test_empty_catalog_completion_does_not_require_a_phantom_object(self):
        lease = self.f.f.backup_job()
        result = self.coordinator.run(lease)
        self.assertEqual(self.f.f.record()['state'],'SEALED')
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),0)
        self.assertEqual(self.store.verify_set(result['set_id'])['manifest']['objects'],[])

    def test_final_lease_expiry_rolls_back_all_success_effects(self):
        lease,manifest,_ = self.f.capture()
        cas = self.h.jobs._cas
        def expire(conn,current,assignments,*args,**kwargs):
            if "state='completed'" in assignments:
                self.h.clock.advance(300001)
            return cas(conn,current,assignments,*args,**kwargs)
        with patch.object(self.h.jobs,'_cas',side_effect=expire), self.assertRaises(LostLease):
            self.coordinator.run(lease)
        self.assert_protected(manifest['set_id'])
        self.assertEqual(self.f.f.record()['state'],'BUILDING')
        self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'running')
        self.assertEqual(self.store.verify_set(manifest['set_id'])['manifest'],manifest)

    def test_clock_change_at_completion_rolls_back_and_persists_clock_fence(self):
        lease,manifest,_ = self.f.capture()
        cas = self.h.jobs._cas
        def reboot(conn,current,assignments,*args,**kwargs):
            if "state='completed'" in assignments:
                self.h.clock.epoch = 'synthetic-reboot'
            return cas(conn,current,assignments,*args,**kwargs)
        with patch.object(self.h.jobs,'_cas',side_effect=reboot), self.assertRaises(ClockBlocked):
            self.coordinator.run(lease)
        self.assert_protected(manifest['set_id'])
        self.assertEqual(self.h.jobs.clock_status()['blocked'],1)

    def test_cancel_after_physical_verification_does_not_create_success(self):
        lease,manifest,_ = self.f.capture()
        transfer = self.store.transfer
        def cancel(current):
            result = transfer(current)
            self.h.jobs.cancel(current.job_id)
            return result
        with patch.object(self.store,'transfer',side_effect=cancel), self.assertRaises(LostLease):
            self.coordinator.run(lease)
        self.assert_protected(manifest['set_id'])

    def test_execution_lock_blocks_reentry_and_stop_while_copy_is_alive(self):
        lease,manifest,_ = self.f.capture()
        with self.coordinator._execution(manifest['set_id']):
            with self.assertRaises(StorageBusy): self.coordinator.run(lease)
            self.h.jobs.cancel(lease.job_id)
            with self.assertRaises(StorageBusy):
                self.coordinator.confirm_stopped(lease,disposition='cancelled',confirmation='premature')
        self.assert_protected(manifest['set_id'])

    def test_external_hold_prevents_success_and_stop_until_observed_release(self):
        lease,manifest,_ = self.f.capture()
        hold = self.h.jobs.acquire_resource(lease,'local-qwen')
        with self.assertRaisesRegex(BackupError,'still holds a resource'):
            self.coordinator.run(lease)
        self.assert_protected(manifest['set_id'])
        self.h.jobs.cancel(lease.job_id)
        with self.assertRaisesRegex(BackupError,'still holds a resource'):
            self.coordinator.confirm_stopped(lease,disposition='cancelled',confirmation='process exited')
        self.h.jobs.confirm_resource_stopped(hold,disposition='cancelled',confirmation='synthetic response terminal')
        self.assertTrue(self.stop(lease))

    def test_generic_pin_release_and_direct_sql_cannot_bypass_coordinator(self):
        lease,manifest,_ = self.f.capture()
        with self.assertRaisesRegex(ValueError,'BackupCoordinator'):
            LifecycleService(self.h.publisher).release_pin('backup',manifest['set_id'],confirmation='timeout')
        for sql in ("DELETE FROM object_pins WHERE kind='backup'",
                    "UPDATE object_pins SET kind='ticket' WHERE kind='backup'",
                    "UPDATE backup_sets SET state='FAILED'"):
            with self.subTest(sql=sql), self.assertRaises(IntegrityError), write_transaction(self.h.engine) as conn:
                conn.execute(text(sql))
        self.assert_protected(manifest['set_id'])

    def test_timeout_retry_wait_and_cancel_request_are_not_stop_evidence(self):
        lease,manifest,_ = self.f.capture()
        with self.assertRaisesRegex(BackupError,'still run or retry'):
            self.coordinator.confirm_stopped(lease,disposition='completed',confirmation='timer expired')
        self.h.jobs.fail(lease,'synthetic-transient')
        with self.assertRaisesRegex(BackupError,'still run or retry'):
            self.coordinator.confirm_stopped(lease,disposition='completed',confirmation='call returned')
        self.h.jobs.cancel(lease.job_id)
        with self.assertRaisesRegex(BackupError,'stop is required'):
            self.coordinator.retire(manifest['set_id'])
        with self.assertRaisesRegex(BackupError,'execution differs'):
            self.coordinator.confirm_stopped(replace(lease,owner='wrong'),disposition='cancelled',confirmation='wrong process')
        self.assert_protected(manifest['set_id'])

    def test_partial_cleanup_retains_shared_objects_and_is_repeatable(self):
        lease,manifest,asset = self.f.capture()
        paths = self.store.paths(manifest['set_id'])
        with patch.object(self.store,'_contents',side_effect=BackupError('synthetic interruption')):
            with self.assertRaises(BackupError): self.coordinator.run(lease)
        staged = f"staging/{manifest['set_id']}/{uuid.uuid4().hex}.part"
        with self.store.remote.file(staged,create=True) as fd: os.write(fd,b'partial')
        self.assertTrue(self.stop(lease))
        self.assertFalse(self.stop(lease))
        with self.assertRaisesRegex(BackupError,'receipt conflict'):
            self.coordinator.confirm_stopped(lease,disposition='not_started',confirmation='different')
        self.assertTrue(self.coordinator.retire(manifest['set_id']))
        self.assertFalse(self.coordinator.retire(manifest['set_id']))
        for path in [staged,paths['database'],paths['manifest']]:
            self.assertFalse((self.store.remote.path/path).exists())
        for path in self.f.f.backups.paths(manifest['set_id']).values():
            self.assertFalse((self.h.path/path).exists())
        self.assertTrue((self.store.remote.path/object_path(asset['sha256'])).is_file())
        self.assertTrue((self.h.path/object_path(asset['sha256'])).is_file())
        self.assertEqual(self.f.f.record()['state'],'FAILED')
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),0)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),0)
        with self.assertRaises(IntegrityError), write_transaction(self.h.engine) as conn:
            conn.execute(text("UPDATE backup_sets SET state='BUILDING'"))

    def test_failed_capture_without_nas_directories_can_retire(self):
        lease = self.f.f.backup_job()
        with patch.object(self.f.f.backups,'_memory_snapshot',side_effect=BackupError('synthetic failed capture')):
            with self.assertRaises(BackupError): self.coordinator.run(lease)
        set_id = self.f.f.backups.set_id(lease)
        self.stop(lease)
        self.assertTrue(self.coordinator.retire(set_id))
        self.assertEqual(self.f.f.record()['state'],'FAILED')

    def test_sealed_physical_copy_survives_failed_execution_retirement(self):
        lease,manifest,_ = self.f.capture()
        physical = self.store.transfer(lease)
        self.stop(lease)
        self.assertTrue(self.coordinator.retire(manifest['set_id']))
        self.assertEqual(self.store.verify_set(manifest['set_id']),physical)
        self.assertEqual(self.f.f.record()['failure'],'observed_stop_sealed_preserved')
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_backup_receipts'),0)

    def test_cleanup_io_failure_keeps_pin_and_retry_reflushes(self):
        lease,manifest,_ = self.f.capture()
        self.stop(lease)
        unlink = self.h.root.unlink
        def interrupted(path):
            unlink(path)
            raise OSError('synthetic failure after unlink')
        with patch.object(self.h.root,'unlink',side_effect=interrupted), self.assertRaises(OSError):
            self.coordinator.retire(manifest['set_id'])
        self.assertEqual(self.f.f.record()['state'],'BUILDING')
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)
        self.assertTrue(self.coordinator.retire(manifest['set_id']))

    def test_unknown_or_symlinked_cleanup_entry_fails_closed(self):
        lease,manifest,_ = self.f.capture()
        self.stop(lease)
        directory = f"staging/{manifest['set_id']}"
        with self.store.remote.directory(directory,create=True): pass
        parent = self.store.remote.path/directory
        unknown = parent/'unknown.txt'
        unknown.write_text('unknown')
        with self.assertRaisesRegex(BackupError,'Unexpected'):
            self.coordinator.retire(manifest['set_id'])
        unknown.unlink()
        link = parent/(uuid.uuid4().hex+'.part')
        link.symlink_to(self.h.path/self.f.f.backups.paths(manifest['set_id'])['database'])
        with self.assertRaises((OSError,StorageError)):
            self.coordinator.retire(manifest['set_id'])
        self.assertTrue(link.is_symlink())
        self.assertEqual(self.h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)

    def test_successor_generation_is_not_retired_with_its_predecessor(self):
        lease,old,_ = self.f.capture()
        self.h.clock.advance(300001)
        successor = self.h.jobs.claim('successor',kind='backup_catalog',lease_ms=300000)
        current = self.f.f.backups.capture(successor)
        self.coordinator.confirm_stopped(lease,disposition='completed',confirmation='old process exit observed')
        self.coordinator.retire(old['set_id'])
        self.assertEqual(self.f.f.record(current['set_id'])['state'],'BUILDING')
        self.assertEqual(self.h.jobs.get(successor.job_id)['state'],'running')
        self.assertEqual(self.h.scalar("SELECT reference_id FROM object_pins WHERE kind='backup'"),current['set_id'])
        self.assertEqual(self.coordinator.run(successor)['set_id'],current['set_id'])


if __name__ == '__main__':
    unittest.main()
