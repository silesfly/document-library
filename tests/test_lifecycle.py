"""Synthetic retention, crash recovery and reference/stream isolation."""
from concurrent.futures import ThreadPoolExecutor
import os
import unittest
from unittest.mock import patch
import uuid

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.jobs import ClockBlocked
from document_library.lifecycle import LifecycleService
from document_library.publication import PublicationError, object_path
import test_publication as fixtures


class LifecycleAcceptance(unittest.TestCase):
    def setUp(self):
        self.h = fixtures.PublicationAcceptance()
        self.addCleanup(self.h.doCleanups)
        self.h.setUp()
        self.gc = LifecycleService(self.h.publisher,unreferenced_grace_ms=10,quarantine_grace_ms=20)
        self.csrf = 'synthetic-csrf'
        with write_transaction(self.h.engine) as conn:
            conn.execute(text("UPDATE users SET role='admin' WHERE id='u'"))

    def retired(self, *, receipt=True):
        h = self.h
        first,_,asset = h.first_revision()
        old_option = h.scalar('SELECT id FROM revision_options')
        lease = h.make_job(first)
        option = dict(fixtures.option(0,end=160),replaces_option_id=old_option,replacement_receipt='synthetic-covered')
        h.plan(lease,[h.asset(lease,b'new 1-160')],[option],previous=first)
        h.publisher.publish(lease)
        backup = h.scalar("SELECT id FROM jobs WHERE kind='backup_revision' AND input_revision_id=:id",id=first)
        h.jobs.cancel(backup)
        self.gc.release_revision(h.token,self.csrf,first,reason='superseded-verified')
        if receipt:
            self.receipt(asset)
        return first,asset['sha256']

    def receipt(self, asset):
        # Synthetic proof fixture only. Production has no receipt-creation API
        # until G0-D3 physically verifies a backup set and its retained objects.
        with write_transaction(self.h.engine) as conn:
            conn.execute(text('INSERT INTO object_backup_receipts VALUES(:id,:sha,:size,:set,:path,:manifest,:now,1)'),
                         dict(id=str(uuid.uuid4()),sha=asset['sha256'],size=asset['byte_size'],set='synthetic-set',
                              path='objects/'+asset['sha256'],manifest='a'*64,now=self.h.clock.wall))

    def quarantine(self):
        revision,sha = self.retired()
        self.h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'quarantined')
        return revision,sha

    def test_two_graces_backup_and_immutable_history_survive_purge(self):
        revision,sha = self.retired(receipt=False)
        h = self.h
        self.assertEqual(self.gc.collect(sha),'ineligible')
        h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'ineligible')
        with h.engine.connect() as conn:
            row = conn.execute(text('SELECT * FROM objects WHERE sha256=:sha'),dict(sha=sha)).mappings().one()
        self.receipt(row)
        self.assertEqual(self.gc.collect(sha),'quarantined')
        self.assertTrue((h.path/self.gc.trash_path(sha)).is_file())
        self.assertFalse((h.path/object_path(sha)).exists())
        self.assertEqual(self.gc.collect(sha),'quarantined')
        h.clock.advance(20)
        self.assertEqual(self.gc.collect(sha),'purged')
        self.assertFalse((h.path/self.gc.trash_path(sha)).exists())
        self.assertEqual(h.scalar('SELECT object_sha FROM revision_assets WHERE edition_revision_id=:id',id=revision),sha)
        self.assertEqual(h.scalar('SELECT count(*) FROM object_backup_receipts'),1)
        self.assertEqual(self.gc.collect(sha),'purged')

    def test_admin_only_release_refuses_active(self):
        h = self.h
        revision,_,_ = h.first_revision()
        with self.assertRaises(PublicationError):
            self.gc.release_revision(h.token,self.csrf,revision,reason='active')
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE users SET role='member' WHERE id='u'"))
        with self.assertRaises(AuthError):
            self.gc.release_revision(h.token,self.csrf,revision,reason='unauthorized')
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE users SET role='admin' WHERE id='u'"))
        # First revision already exists; separate synthetic test below covers
        # release idempotence without rebuilding the publication chain.
        self.assertEqual(h.scalar('SELECT retained FROM revision_seals'),1)

    def test_new_reference_between_scans_restarts_unreferenced_grace(self):
        revision,sha = self.retired()
        h = self.h
        self.assertFalse(self.gc.release_revision(h.token,self.csrf,revision,reason='repeat'))
        h.clock.advance(10)
        job = h.jobs.enqueue(kind='read_input',idempotency_key='new-reference',payload={},object_sha=sha)
        h.jobs.cancel(job)
        self.assertEqual(self.gc.collect(sha),'grace')
        self.assertEqual(self.gc.collect(sha),'ineligible')
        h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'quarantined')

    def test_pin_release_resets_grace_and_preserves_other_pins(self):
        _,sha = self.retired()
        h = self.h
        h.publisher.pin('ticket','reader',[sha])
        h.publisher.pin('restore','copy',[sha])
        self.assertEqual(self.gc.release_pin('ticket','reader',confirmation='stream-closed'),1)
        h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'protected')
        self.assertEqual(self.gc.release_pin('restore','copy',confirmation='process-exit'),1)
        self.assertEqual(self.gc.collect(sha),'ineligible')
        h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'quarantined')

    def test_open_stream_skips_gc_even_without_remaining_database_references(self):
        h = self.h
        revision,_,asset = h.first_revision()
        sha = asset['sha256']
        # Actual read_revision owns its shared object lock after global unlock.
        with h.publisher.read_revision(h.token,revision,[0]) as files:
            # Remove DB references synthetically while an actual reader is open:
            # the kernel lock must protect bytes independently of those rows.
            with write_transaction(h.engine) as conn:
                conn.execute(text('UPDATE edition_revisions SET is_active=0'))
                conn.execute(text('UPDATE revision_seals SET retained=0'))
                conn.execute(text("UPDATE jobs SET state='cancelled' WHERE state='queued'"))
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(self.gc.collect,sha).result(timeout=5),'busy')
            self.assertEqual(os.read(files[0],100),b'novel 1-150')
        self.assertEqual(self.gc.collect(sha),'grace')

    def test_quarantine_crash_before_and_after_move_reconciles(self):
        _,sha = self.retired()
        self.h.clock.advance(10)
        with patch.object(self.gc,'_finish_collection',side_effect=RuntimeError('before-move')):
            with self.assertRaises(RuntimeError): self.gc.collect(sha)
        self.assertEqual(self.h.scalar('SELECT state FROM objects WHERE sha256=:sha',sha=sha),'QUARANTINED')
        self.h.clock.advance(30)  # Pre-rename downtime must not consume trash grace.
        move = self.gc._move
        def fail_after_move(*args):
            move(*args)
            raise RuntimeError('after-move-before-db')
        with patch.object(self.gc,'_move',side_effect=fail_after_move):
            with self.assertRaises(RuntimeError): self.gc.collect(sha)
        self.assertEqual(self.h.scalar('SELECT operation FROM object_lifecycle WHERE object_sha=:sha',sha=sha),'quarantine')
        self.h.clock.advance(30)  # A move without committed completion starts a fresh grace on recovery.
        self.assertEqual(self.gc.collect(sha),'quarantined')
        self.assertEqual(self.h.scalar('SELECT quarantined_at FROM object_lifecycle WHERE object_sha=:sha',sha=sha),self.h.clock.wall)
        self.assertEqual(self.gc.collect(sha),'quarantined')
        self.h.clock.advance(19)
        self.assertEqual(self.gc.collect(sha),'quarantined')
        self.h.clock.advance(1)
        self.assertEqual(self.gc.collect(sha),'purged')

    def test_purge_crash_after_unlink_reconciles_and_rechecks_backup(self):
        _,sha = self.quarantine()
        h = self.h
        h.clock.advance(20)
        unlink = self.gc._unlink
        def fail_after_unlink(path):
            unlink(path)
            raise RuntimeError('after-unlink-before-db')
        with patch.object(self.gc,'_unlink',side_effect=fail_after_unlink):
            with self.assertRaises(RuntimeError): self.gc.collect(sha)
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE object_backup_receipts SET valid=0'))
        self.assertEqual(self.gc.collect(sha),'ineligible')
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE object_backup_receipts SET valid=1'))
        self.assertEqual(self.gc.collect(sha),'purged')

    def test_restore_after_move_failure_requires_current_admin_and_preserves_revision_state(self):
        revision,sha = self.quarantine()
        h = self.h
        move = self.gc._move
        def fail_after_move(*args):
            move(*args)
            raise RuntimeError('restore-after-rename')
        with patch.object(self.gc,'_move',side_effect=fail_after_move):
            with self.assertRaises(RuntimeError): self.gc.restore_quarantine(h.token,self.csrf,sha)
        self.assertEqual(self.gc.collect(sha),'restore_pending')
        with self.assertRaises(AuthError): self.gc.restore_quarantine('bad',self.csrf,sha)
        self.assertEqual(self.gc.restore_quarantine(h.token,self.csrf,sha),'live')
        self.assertEqual(h.scalar('SELECT is_active FROM edition_revisions WHERE id=:id',id=revision),0)
        self.assertEqual(h.scalar('SELECT retained FROM revision_seals WHERE revision_id=:id',id=revision),0)
        with h.root.file(object_path(sha)) as fd:
            self.assertEqual(os.read(fd,100),b'novel 1-150')

    def test_non_live_objects_reject_all_new_reference_routes(self):
        revision,sha = self.quarantine()
        h = self.h
        with self.assertRaises(PublicationError): h.publisher.pin('ticket','no',[sha])
        for kwargs in [dict(object_sha=sha),dict(input_revision_id=revision)]:
            with self.subTest(kwargs=kwargs),self.assertRaises(IntegrityError):
                h.jobs.enqueue(kind='invalid',idempotency_key=str(uuid.uuid4()),payload={},**kwargs)
        for sql,params in [
            ('UPDATE revision_seals SET retained=1 WHERE revision_id=:id',dict(id=revision)),
            ("INSERT INTO object_pins VALUES('ticket','raw',:sha,0)",dict(sha=sha)),
            ('UPDATE edition_revisions SET is_active=1 WHERE id=:id',dict(id=revision)),
        ]:
            with self.subTest(sql=sql),self.assertRaises(IntegrityError):
                with write_transaction(h.engine) as conn: conn.execute(text(sql),params)

    def test_new_retained_seal_cannot_acquire_an_already_quarantined_draft_asset(self):
        _,sha = self.retired()
        h = self.h
        active = h.scalar('SELECT id FROM edition_revisions WHERE is_active=1')
        old_option = h.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id',id=active)
        lease = h.make_job(active)
        replacement = dict(fixtures.option(0,end=170),replaces_option_id=old_option,replacement_receipt='synthetic-draft-validated')
        draft = h.plan(lease,[h.asset(lease)],[replacement],previous=active)
        # Model a valid intermediate DB draft while all bytes are still LIVE.
        with write_transaction(h.engine) as conn:
            conn.execute(text("INSERT INTO edition_revisions VALUES(:id,'e',3,0,'synthetic')"),dict(id=draft))
            conn.execute(text("INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format) VALUES(:id,0,:sha,'txt')"),dict(id=draft,sha=sha))
        h.jobs.cancel(lease.job_id)
        self.assertEqual(self.gc.collect(sha),'grace')
        h.clock.advance(10)
        self.assertEqual(self.gc.collect(sha),'quarantined')
        for retained in (None,1):
            with self.subTest(retained=retained),self.assertRaises(IntegrityError):
                with write_transaction(h.engine) as conn:
                    columns = 'revision_id,source_job_id,source_generation,sealed_at'
                    values = ':id,:job,:generation,:now'
                    if retained is not None:
                        columns += ',retained'
                        values += ',1'
                    conn.execute(text(f'INSERT INTO revision_seals({columns}) VALUES({values})'),
                                 dict(id=draft,job=lease.job_id,generation=lease.generation,now=h.clock.wall))
        with h.engine.connect() as conn:
            self.assertNotIn(('revision',draft),h.publisher.reference_sources(conn,sha))

    def test_missing_or_corrupt_or_ambiguous_bytes_are_never_purged_as_success(self):
        _,sha = self.retired()
        h = self.h
        h.clock.advance(10)
        target = h.path/object_path(sha)
        original = target.read_bytes()
        target.write_bytes(b'corrupt')
        with self.assertRaises(PublicationError): self.gc.collect(sha)
        target.write_bytes(original)
        with h.root.file(self.gc.trash_path(sha),create=True) as fd: os.write(fd,original)
        with self.assertRaises(PublicationError): self.gc.collect(sha)
        (h.path/self.gc.trash_path(sha)).unlink()
        target.unlink()
        self.assertEqual(self.gc.collect(sha),'missing')
        self.assertEqual(h.scalar('SELECT state FROM objects WHERE sha256=:sha',sha=sha),'MISSING')

    def test_changed_file_after_hash_and_revoked_backup_before_transition_are_rejected(self):
        _,sha = self.retired()
        h = self.h
        h.clock.advance(10)
        verify = self.gc._verified_paths
        def changed_after_verify(value):
            result = verify(value)
            (h.path/object_path(sha)).write_bytes(b'changed-after-hash')
            return result
        with patch.object(self.gc,'_verified_paths',side_effect=changed_after_verify):
            with self.assertRaises(PublicationError): self.gc.collect(sha)
        (h.path/object_path(sha)).write_bytes(b'novel 1-150')
        def revoke_after_verify(value):
            result = verify(value)
            with write_transaction(h.engine) as conn:
                conn.execute(text('UPDATE object_backup_receipts SET valid=0'))
            return result
        with patch.object(self.gc,'_verified_paths',side_effect=revoke_after_verify):
            self.assertEqual(self.gc.collect(sha),'ineligible')
        self.assertTrue((h.path/object_path(sha)).is_file())

    def test_clock_jump_blocks_collection_without_file_change(self):
        _,sha = self.retired()
        h = self.h
        h.clock.wall += 100000
        with self.assertRaises(ClockBlocked): self.gc.collect(sha)
        self.assertTrue((h.path/object_path(sha)).is_file())
        self.assertEqual(h.scalar('SELECT state FROM objects WHERE sha256=:sha',sha=sha),'LIVE')


if __name__ == '__main__':
    unittest.main()
