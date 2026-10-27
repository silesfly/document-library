"""Stopped generation installation cleanup with real synthetic files and DB."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
import unittest
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.database import write_transaction
from document_library.jobs import ClockBlocked, LostLease
from document_library.orphans import OrphanService
from document_library.publication import PublicationError, object_path
from document_library.storage import StorageError
import test_publication as fixtures


class OrphanAcceptance(unittest.TestCase):
    def setUp(self):
        self.h = fixtures.PublicationAcceptance()
        self.addCleanup(self.h.doCleanups)
        self.h.setUp()
        self.orphans = OrphanService(self.h.publisher,quarantine_grace_ms=20)

    def failed_installation(self, *, before_install=False, preexisting=False):
        h = self.h
        lease = h.make_job()
        asset = h.asset(lease)
        h.plan(lease,[asset])
        if preexisting:
            with h.root.file(object_path(asset['sha256']),create=True) as fd: os.write(fd,b'novel 1-150')
        method = '_install' if before_install else '_write_revision'
        with patch.object(h.publisher,method,side_effect=RuntimeError('synthetic stopped execution')):
            with self.assertRaises(RuntimeError): h.publisher.publish(lease)
        intent = h.scalar('SELECT id FROM storage_intents WHERE job_id=:id',id=lease.job_id)
        return lease,asset,intent

    def stopped(self, lease):
        self.h.jobs.cancel(lease.job_id)
        return self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='synthetic call returned and no process remains')

    def record(self, intent):
        with self.h.engine.connect() as conn:
            return dict(conn.execute(text('SELECT * FROM installation_retirements WHERE intent_id=:id'),dict(id=intent)).mappings().one())

    def test_cancellation_and_expired_lease_are_not_observed_termination(self):
        lease,asset,intent = self.failed_installation()
        h = self.h
        h.clock.advance(30000)
        with self.assertRaises(PublicationError):
            self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='timeout')
        h.jobs.cancel(lease.job_id)
        with self.assertRaises(PublicationError): self.orphans.retire(intent)
        self.assertTrue((h.path/object_path(asset['sha256'])).is_file())
        self.assertEqual(h.scalar('SELECT state FROM storage_intents'),'prepared')
        self.assertTrue(self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='synthetic worker exited'))
        result = self.orphans.retire(intent)
        self.assertEqual(result['state'],'quarantined')
        self.assertEqual(result['target_action'],'moved')
        # Validation precedes the ownership CAS: retired bytes fail to open.
        with patch.object(h.publisher,'_install',wraps=h.publisher._install) as install:
            with self.assertRaises(FileNotFoundError): h.publisher.publish(lease)
            install.assert_not_called()
            # Even if another writer restores identical canonical bytes, the
            # stale execution must fail its CAS before installation/publication.
            with h.root.file(object_path(asset['sha256']),create=True) as fd:
                os.write(fd,b'novel 1-150')
            with self.assertRaises(LostLease): h.publisher.publish(lease)
            install.assert_not_called()
        self.assertEqual(h.scalar('SELECT count(*) FROM edition_revisions'),0)

    def test_retryable_generation_is_preserved_until_successor_fences_it(self):
        lease,asset,intent = self.failed_installation(before_install=True)
        h = self.h
        h.jobs.fail(lease,'synthetic-retry')
        with self.assertRaises(PublicationError):
            self.orphans.confirm_stopped(lease,disposition='completed',confirmation='old-call-returned')
        h.clock.advance(1000)
        successor = h.jobs.claim('successor',kind='ingest_upload')
        self.assertGreater(successor.generation,lease.generation)
        self.orphans.confirm_stopped(lease,disposition='completed',confirmation='old-call-returned')
        result = self.orphans.retire(intent)
        self.assertEqual(result['prepared_action'],'moved')
        self.assertEqual(result['target_action'],'absent')
        h.plan(successor,[h.asset(successor,b'new-generation')])
        h.publisher.publish(successor)
        self.assertEqual(h.jobs.get(lease.job_id)['state'],'completed')

    def test_external_hold_must_be_confirmed_stopped_separately(self):
        lease,_,intent = self.failed_installation()
        h = self.h
        hold = h.jobs.acquire_resource(lease,'local-ocr')
        h.jobs.cancel(lease.job_id)
        with self.assertRaises(PublicationError):
            self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='local-worker-exit')
        h.jobs.confirm_resource_stopped(hold,disposition='cancelled',confirmation='synthetic remote terminal observed')
        self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='local-worker-exit')
        self.assertEqual(self.orphans.retire(intent)['state'],'quarantined')

    def test_stop_receipt_owner_generation_identity_and_immutability(self):
        lease,_,_ = self.failed_installation()
        h = self.h
        h.jobs.cancel(lease.job_id)
        for wrong in (replace(lease,owner='other'),replace(lease,generation=lease.generation+1)):
            with self.subTest(lease=wrong),self.assertRaises(PublicationError):
                self.orphans.confirm_stopped(wrong,disposition='cancelled',confirmation='synthetic-exit')
        self.assertTrue(self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='synthetic-exit'))
        self.assertFalse(self.orphans.confirm_stopped(lease,disposition='cancelled',confirmation='synthetic-exit'))
        with self.assertRaises(PublicationError):
            self.orphans.confirm_stopped(lease,disposition='completed',confirmation='different')
        for sql in ('DELETE FROM installation_stops',"UPDATE installation_stops SET confirmation='changed'"):
            with self.subTest(sql=sql),self.assertRaises(IntegrityError):
                with write_transaction(h.engine) as conn: conn.execute(text(sql))

    def test_other_pending_intent_and_committed_object_are_never_moved(self):
        old,asset,old_intent = self.failed_installation()
        h = self.h
        newer = h.make_job()
        next_asset = h.asset(newer)
        h.plan(newer,[next_asset])
        with patch.object(h.publisher,'_install',side_effect=RuntimeError('paused before install')):
            with self.assertRaises(RuntimeError): h.publisher.publish(newer)
        self.stopped(old)
        result = self.orphans.retire(old_intent)
        self.assertEqual(result['state'],'released')
        self.assertEqual(result['target_action'],'protected')
        self.assertTrue((h.path/object_path(asset['sha256'])).is_file())
        revision = h.publisher.publish(newer)
        new_intent = h.scalar('SELECT id FROM storage_intents WHERE job_id=:id',id=newer.job_id)
        self.orphans.confirm_stopped(newer,disposition='completed',confirmation='publisher-call-returned')
        result = self.orphans.retire(new_intent)
        self.assertEqual(result['state'],'quarantined')
        self.assertEqual(result['prepared_action'],'moved')
        self.assertEqual(result['target_action'],'protected')
        self.assertEqual(h.scalar('SELECT state FROM storage_intents WHERE id=:id',id=new_intent),'committed')
        h.clock.advance(20)
        self.assertEqual(self.orphans.purge(new_intent)['state'],'purged')
        with h.publisher.read_revision(h.token,revision,[0]) as files:
            self.assertEqual(os.read(files[0],100),b'novel 1-150')

    def test_new_publication_of_same_sha_survives_old_archive_purge(self):
        lease,asset,intent = self.failed_installation()
        h = self.h
        self.stopped(lease)
        self.orphans.retire(intent)
        new = h.make_job()
        h.plan(new,[h.asset(new)])
        revision = h.publisher.publish(new)
        self.assertEqual(self.orphans.purge(intent)['state'],'quarantined')
        h.clock.advance(20)
        self.assertEqual(self.orphans.purge(intent)['state'],'purged')
        self.assertEqual(self.orphans.purge(intent)['state'],'purged')
        with h.publisher.read_revision(h.token,revision,[0]) as files:
            self.assertEqual(os.read(files[0],100),b'novel 1-150')
        self.assertTrue((h.path/object_path(asset['sha256'])).is_file())

    def test_retirement_crash_before_and_after_move_restarts_full_archive_grace(self):
        lease,asset,intent = self.failed_installation(preexisting=True)
        h = self.h
        self.stopped(lease)
        with patch.object(self.orphans,'_finish_retirement',side_effect=RuntimeError('before-move')):
            with self.assertRaises(RuntimeError): self.orphans.retire(intent)
        self.assertEqual(self.record(intent)['state'],'retiring')
        h.clock.advance(30)
        move = h.root.move
        calls = []
        def fail_after_first(*args):
            move(*args)
            calls.append(args)
            raise RuntimeError('after-first-rename')
        with patch.object(h.root,'move',side_effect=fail_after_first):
            with self.assertRaises(RuntimeError): self.orphans.retire(intent)
        self.assertEqual(len(calls),1)
        h.clock.advance(30)
        result = self.orphans.retire(intent)
        self.assertEqual(result['prepared_action'],'moved')
        self.assertEqual(result['target_action'],'moved')
        self.assertEqual(result['quarantined_at'],h.clock.wall)
        self.assertEqual(self.orphans.purge(intent)['state'],'quarantined')
        h.clock.advance(20)
        self.assertEqual(self.orphans.purge(intent)['state'],'purged')
        # Original worker staging is outside this cleanup authority.
        self.assertTrue(any((h.path/lease.staging_key).iterdir()))

    def test_partial_unlink_failure_reconciles_only_owned_archives(self):
        lease,_,intent = self.failed_installation(preexisting=True)
        h = self.h
        self.stopped(lease)
        self.orphans.retire(intent)
        h.clock.advance(20)
        unlink = h.root.unlink
        def fail_after_unlink(path):
            unlink(path)
            raise RuntimeError('unlink-before-DB')
        with patch.object(h.root,'unlink',side_effect=fail_after_unlink):
            with self.assertRaises(RuntimeError): self.orphans.purge(intent)
        self.assertEqual(self.record(intent)['state'],'purging')
        self.assertEqual(self.orphans.purge(intent)['state'],'purged')

    def test_missing_bytes_are_reported_and_archive_tampering_is_rejected(self):
        lease,asset,intent = self.failed_installation(before_install=True)
        h = self.h
        self.stopped(lease)
        (h.path/asset['prepared_relpath']).unlink()
        self.assertEqual(self.orphans.retire(intent)['state'],'missing')
        self.assertEqual(self.orphans.purge(intent)['state'],'missing')
        other,asset2,intent2 = self.failed_installation()
        self.stopped(other)
        self.orphans.retire(intent2)
        path = h.path/f'trash/installations/{intent2}/object.bin'
        path.write_bytes(b'changed')
        with self.assertRaises(PublicationError): self.orphans.purge(intent2)
        self.assertEqual(self.record(intent2)['state'],'quarantined')

    def test_shared_reader_and_clock_fence_prevent_retirement_mutation(self):
        lease,asset,intent = self.failed_installation()
        h = self.h
        self.stopped(lease)
        with h.publisher.locks.readers([asset['sha256']],lambda stack:None):
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(self.orphans.retire,intent).result(timeout=5)['state'],'busy')
        h.clock.wall += 100000
        with self.assertRaises(ClockBlocked): self.orphans.retire(intent)
        self.assertEqual(h.scalar('SELECT state FROM storage_intents'),'prepared')
        self.assertTrue((h.path/object_path(asset['sha256'])).is_file())

    def test_new_file_after_hash_is_not_retired_using_old_proof(self):
        lease,asset,intent = self.failed_installation()
        h = self.h
        self.stopped(lease)
        verify = self.orphans._verified
        def change_after_hash(*args,**kwargs):
            result = verify(*args,**kwargs)
            (h.path/object_path(asset['sha256'])).write_bytes(b'changed-after-hash')
            return result
        with patch.object(self.orphans,'_verified',side_effect=change_after_hash):
            with self.assertRaises(PublicationError): self.orphans.retire(intent)
        self.assertEqual(h.scalar('SELECT count(*) FROM installation_retirements'),0)

    def test_shared_move_never_replaces_existing_destination(self):
        h = self.h
        for path,body in [('synthetic/source',b'original'),('synthetic/target',b'preserve')]:
            with h.root.file(path,create=True) as fd: os.write(fd,body)
        with h.publisher.locks.mutation():
            with self.assertRaises(StorageError): h.root.move('synthetic/source','synthetic/target')
        self.assertEqual((h.path/'synthetic/source').read_bytes(),b'original')
        self.assertEqual((h.path/'synthetic/target').read_bytes(),b'preserve')


if __name__ == '__main__':
    unittest.main()
