"""Full isolated restore, lost-source recovery and fail-closed activation."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import text

from document_library.backup import BackupError, DISABLED_PASSWORD, DISABLED_RESOURCE, canonical_json
from document_library.backup_coordinator import BackupCoordinator
from document_library.database import database_engine, write_transaction
from document_library.jobs import JobService, ClockBlocked
from document_library.nas import NASBackupReader
from document_library.publication import PublicationError, object_path
from document_library.restore import RestoreService
from document_library.library_contracts import register_sqlite_functions
from document_library.library_projection import verify_projection_in
from document_library.storage import StorageRoot, StorageLocks, StorageError, StorageBusy
from test_storage import SyntheticVolume
import test_nas as fixtures


class RestoreAcceptance(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.NASAcceptance()
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.h = self.f.h
        self.control = self.isolated_root('control')
        self.target = self.isolated_root('restore')
        self.reader = NASBackupReader(self.f.remote,StorageLocks(self.control))
        self.restorer = RestoreService(self.reader)

    def isolated_root(self, label):
        temp = tempfile.TemporaryDirectory(prefix=f'document-library-{label}-')
        root = StorageRoot(Path(temp.name).resolve(),SyntheticVolume())
        def cleanup():
            root.close()
            temp.cleanup()
            self.assertFalse(Path(temp.name).exists())
        self.addCleanup(cleanup)
        return root

    def backed_up(self):
        lease,manifest,asset = self.f.capture()
        BackupCoordinator(self.f.store).run(lease)
        return manifest,asset

    def db(self, source=False):
        relative = self.restorer.SOURCE_DB if source else self.restorer.DATABASE
        return closing(sqlite3.connect(self.target.path/relative,autocommit=True))

    def no_verified_marker(self):
        self.assertFalse((self.target.path/self.restorer.VERIFIED).exists())

    def rewrite_nas_db(self, set_id, mutate, *, change_manifest=None):
        # Synthetic fault injection on the disposable NAS. The physical hashes
        # are made consistent to exercise semantic validation beyond hashes.
        paths = self.f.store.paths(set_id)
        with closing(sqlite3.connect(self.f.remote.path/paths['database'],autocommit=True)) as db:
            register_sqlite_functions(db)
            mutate(db)
        manifest = json.loads((self.f.remote.path/paths['manifest']).read_bytes())
        data = (self.f.remote.path/paths['database']).read_bytes()
        manifest['database'] = dict(sha256=hashlib.sha256(data).hexdigest(),byte_size=len(data))
        if change_manifest: change_manifest(manifest)
        encoded = canonical_json(manifest).encode()
        (self.f.remote.path/paths['manifest']).write_bytes(encoded)
        seal = json.loads((self.f.remote.path/paths['seal']).read_bytes())
        seal.update(manifest_sha256=hashlib.sha256(encoded).hexdigest(),snapshot_sha256=manifest['database']['sha256'])
        (self.f.remote.path/paths['seal']).write_text(canonical_json(seal))


    def test_restore_keeps_preferences_favorites_ratings_and_removes_quota_authority(self):
        self.h.first_revision()
        from document_library.member_service import MemberService
        from document_library.request_quota import AccountQuota
        quota = AccountQuota(self.h.auth)
        members = MemberService(self.h.auth, quota=quota, read_social_in=lambda *args, **kwargs: {})
        value = members.get_preferences(self.h.token)
        value.pop('updated_at')
        value['filters']['genres'] = ['fantasy']
        saved = members.set_preferences(self.h.token, 'synthetic-csrf', value)
        quota.admit(self.h.token, action='download')
        with write_transaction(self.h.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w',7,'test')"))
            conn.execute(text("INSERT INTO library_favorites VALUES('u','w','test')"))
        lease = self.f.f.backup_job()
        result = BackupCoordinator(self.f.store).run(lease)
        restored = self.restorer.restore(result['set_id'], self.target)
        self.assertEqual(restored['status'], 'VERIFIED_BLOCKED')
        self.assertFalse(restored['activation_allowed'])
        with self.db(source=True) as source, self.db() as db:
            for table in ('member_preferences','ratings','library_favorites'):
                self.assertEqual(source.execute(f'SELECT * FROM {table}').fetchall(), db.execute(f'SELECT * FROM {table}').fetchall())
            raw, updated = db.execute('SELECT preferences_json,updated_at FROM member_preferences').fetchone()
            self.assertEqual(json.loads(raw), value)
            self.assertEqual(updated, saved['updated_at'])
            for table in ('auth_sessions','auth_tokens','auth_limits'):
                self.assertEqual(db.execute(f'SELECT count(*) FROM {table}').fetchone(), (0,))
            self.assertEqual(db.execute('SELECT password_hash,status FROM users').fetchone(), (DISABLED_PASSWORD,'suspended'))
            register_sqlite_functions(db)
            verify_projection_in(db)
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual(self.h.scalar("SELECT count(*) FROM auth_limits WHERE bucket LIKE 'g3:%'"), 2)

    def test_full_restore_preserves_content_ratings_and_raw_snapshot_but_blocks_execution(self):
        self.h.first_revision()
        with write_transaction(self.h.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w',9,'synthetic')"))
            conn.execute(text("INSERT INTO upload_batches VALUES('pending-batch','u',12,'open',1)"))
            conn.execute(text("INSERT INTO upload_sessions VALUES('pending-upload','pending-batch','u',NULL,1,12,3,9999999999999,'receiving')"))
        lease = self.f.f.backup_job()
        result = BackupCoordinator(self.f.store).run(lease)
        raw = (self.f.remote.path/self.f.store.paths(result['set_id'])['database']).read_bytes()
        restored = self.restorer.restore(result['set_id'],self.target)
        self.assertEqual(restored['status'],'VERIFIED_BLOCKED')
        self.assertFalse(restored['activation_allowed'])
        self.assertEqual((self.target.path/self.restorer.SOURCE_DB).read_bytes(),raw)
        self.assertEqual(self.restorer.verify(self.target),restored)
        with self.db(source=True) as source, self.db() as db:
            for table in ('works','work_editions','edition_revisions','revision_assets','revision_options','option_assets','ratings','user_permissions'):
                self.assertEqual(source.execute(f'SELECT * FROM {table}').fetchall(),db.execute(f'SELECT * FROM {table}').fetchall())
            self.assertEqual(db.execute('SELECT password_hash,status FROM users').fetchone(),(DISABLED_PASSWORD,'suspended'))
            self.assertEqual(db.execute("SELECT status FROM upload_sessions WHERE id='pending-upload'").fetchone(),('revoked',))
            old = source.execute('SELECT lease_generation FROM jobs WHERE id=?',(lease.job_id,)).fetchone()[0]
            self.assertEqual(db.execute('SELECT state,lease_generation,lease_owner FROM jobs WHERE id=?',(lease.job_id,)).fetchone(),('retry_wait',old+1,None))
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
        engine = database_engine(self.target.path/self.restorer.DATABASE)
        try:
            with self.assertRaises(ClockBlocked): JobService(engine,clock=self.h.clock).claim('new-worker')
        finally: engine.dispose()

    def test_stale_derivatives_rebuilt_only_in_isolated_copy_and_backup_verifier_rejects_them(self):
        from document_library.backup import SnapshotService
        manifest,_=self.backed_up()
        def stale(db):
            db.execute("UPDATE library_read_works SET computed_epoch=computed_epoch+7,size_bytes=987654 WHERE work_id='w'")
            db.execute('DELETE FROM library_read_formats')
            db.execute("UPDATE library_rating_stats SET votes=1,half_sum=8 WHERE work_id='w'")
        self.rewrite_nas_db(manifest['set_id'],stale)
        raw=(self.f.remote.path/self.f.store.paths(manifest['set_id'])['database']).read_bytes()
        restored=self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(restored['status'],'VERIFIED_BLOCKED')
        self.assertEqual((self.target.path/self.restorer.SOURCE_DB).read_bytes(),raw)
        with self.db(source=True) as source,self.db() as prepared:
            register_sqlite_functions(source);register_sqlite_functions(prepared)
            self.assertEqual(source.execute("SELECT size_bytes,input_epoch<>computed_epoch FROM library_read_works WHERE work_id='w'").fetchone(),(987654,1))
            with self.assertRaisesRegex(BackupError,'projection'):
                SnapshotService._integrity(source)
            verify_projection_in(prepared)
            self.assertEqual(prepared.execute("SELECT votes,half_sum FROM library_rating_stats WHERE work_id='w'").fetchone(),(0,0))
            self.assertNotEqual(prepared.execute("SELECT size_bytes FROM library_read_works WHERE work_id='w'").fetchone(),(987654,))
        self.assertEqual(self.restorer.verify(self.target),restored)
        for relative in (self.restorer.SOURCE_DB,self.restorer.DATABASE):
            with self.restorer._database(self.target,relative) as db:
                self.assertEqual(db.execute('PRAGMA recursive_triggers').fetchone(),(1,))

    def test_missing_projection_rebuild_does_not_repair_invalid_raw_plan_digest(self):
        manifest,_=self.backed_up()
        def malformed(db):
            guard=db.execute("SELECT sql FROM sqlite_master WHERE name='plan_identity_update'").fetchone()[0]
            db.execute('DROP TRIGGER plan_identity_update')
            db.execute("UPDATE publication_plans SET payload_digest='0' WHERE job_id IN (SELECT source_job_id FROM revision_seals)")
            db.execute(guard)
            db.execute('DELETE FROM library_read_works')
            db.execute('DELETE FROM library_rating_stats')
        self.rewrite_nas_db(manifest['set_id'],malformed)
        raw=(self.f.remote.path/self.f.store.paths(manifest['set_id'])['database']).read_bytes()
        self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual((self.target.path/self.restorer.SOURCE_DB).read_bytes(),raw)
        with self.db() as prepared:
            register_sqlite_functions(prepared)
            verify_projection_in(prepared)
            self.assertEqual(prepared.execute("SELECT visible,basis_option_id FROM library_read_works WHERE work_id='w'").fetchone(),(0,None))
            self.assertEqual(prepared.execute("SELECT count(*) FROM publication_plans WHERE payload_digest='0'").fetchone(),(1,))
            self.assertEqual(prepared.execute("SELECT votes,half_sum FROM library_rating_stats WHERE work_id='w'").fetchone(),(0,0))
        self.restorer.verify(self.target)

    def test_restore_after_source_database_and_ssd_root_are_gone(self):
        manifest,asset = self.backed_up()
        self.h.doCleanups()
        self.assertFalse(self.h.path.exists())
        self.assertFalse(hasattr(self.reader,'jobs'))
        result = self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(result['copied_originals'],1)
        self.assertEqual((self.target.path/object_path(asset['sha256'])).read_bytes(),b'novel 1-150')
        self.f.remote.close()
        self.assertEqual(self.restorer.verify(self.target),result)

    def test_empty_catalog_is_valid_and_missing_seal_is_not(self):
        lease = self.f.f.backup_job()
        manifest = self.f.f.backups.capture(lease)
        with self.assertRaises(FileNotFoundError): self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(list(self.target.path.iterdir()),[])
        BackupCoordinator(self.f.store).run(lease)
        result = self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(result['copied_originals'],0)
        self.assertEqual(self.restorer.verify(self.target),result)

    def test_missing_original_refuses_restore_before_writing_target(self):
        manifest,asset = self.backed_up()
        (self.f.remote.path/object_path(asset['sha256'])).unlink()
        with self.assertRaises(FileNotFoundError): self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(list(self.target.path.iterdir()),[])

    def test_nonempty_or_overlapping_target_is_never_overwritten(self):
        manifest,_ = self.backed_up()
        sentinel = self.target.path/'keep'
        sentinel.write_bytes(b'user-owned')
        with self.assertRaisesRegex(BackupError,'must be empty'):
            self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(sentinel.read_bytes(),b'user-owned')
        for root in (self.f.remote,self.control):
            with self.subTest(root=root.path), self.assertRaisesRegex(BackupError,'separate isolated'):
                self.restorer.restore(manifest['set_id'],root)
        with self.f.remote.directory('nested-restore',create=True): pass
        with StorageRoot(self.f.remote.path/'nested-restore',SyntheticVolume()) as nested:
            with self.assertRaisesRegex(BackupError,'separate isolated'):
                self.restorer.restore(manifest['set_id'],nested)

    def test_destination_lock_blocks_duplicate_execution(self):
        manifest,_ = self.backed_up()
        with self.restorer._execution(self.target):
            with self.assertRaises(StorageBusy): self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(list(self.target.path.iterdir()),[])

    def test_interrupted_copy_has_no_verified_marker_and_requires_fresh_target(self):
        manifest,_ = self.backed_up()
        original = self.restorer._copy
        def interrupt(*args):
            original(*args)
            raise OSError('synthetic copy interrupted')
        with patch.object(self.restorer,'_copy',side_effect=interrupt), self.assertRaises(OSError):
            self.restorer.restore(manifest['set_id'],self.target)
        self.no_verified_marker()
        before = {str(p):p.read_bytes() for p in self.target.path.rglob('*') if p.is_file()}
        with self.assertRaisesRegex(BackupError,'must be empty'):
            self.restorer.restore(manifest['set_id'],self.target)
        self.assertEqual(before,{str(p):p.read_bytes() for p in self.target.path.rglob('*') if p.is_file()})

    def test_corruption_after_copy_is_detected_before_verified_marker(self):
        manifest,asset = self.backed_up()
        original = self.restorer._copy
        def corrupt(source,path,target,target_path,expected):
            original(source,path,target,target_path,expected)
            if path == object_path(asset['sha256']): (target.path/target_path).write_bytes(b'corrupt')
        with patch.object(self.restorer,'_copy',side_effect=corrupt), self.assertRaises(PublicationError):
            self.restorer.restore(manifest['set_id'],self.target)
        self.no_verified_marker()

    def test_hash_consistent_but_incomplete_manifest_is_rejected(self):
        manifest,_ = self.backed_up()
        self.rewrite_nas_db(manifest['set_id'],lambda db:None,change_manifest=lambda value:value.update(objects=[]))
        with self.assertRaisesRegex(BackupError,'does not cover'):
            self.restorer.restore(manifest['set_id'],self.target)
        self.no_verified_marker()

    def test_unsealed_active_revision_is_rejected_even_with_valid_foreign_keys(self):
        manifest,_ = self.backed_up()
        def incomplete(db):
            db.execute('UPDATE edition_revisions SET is_active=0')
            db.execute("INSERT INTO edition_revisions VALUES('unsealed','e',999,1,'synthetic')")
        self.rewrite_nas_db(manifest['set_id'],incomplete)
        with self.assertRaisesRegex(BackupError,'incomplete sealed references'):
            self.restorer.restore(manifest['set_id'],self.target)
        self.no_verified_marker()

    def test_authentication_schema_and_fk_corruption_are_rejected(self):
        manifest,_ = self.backed_up()
        paths = self.f.store.paths(manifest['set_id'])
        originals = {p:(self.f.remote.path/p).read_bytes() for p in paths.values()}
        cases = (
            ("UPDATE users SET password_hash='synthetic-forbidden-auth'",'reauthentication'),
            ('CREATE TABLE unreviewed_schema(value TEXT)','policy review'),
            ("DELETE FROM users WHERE id='u'",'foreign keys'),
        )
        for statement,reason in cases:
            with self.subTest(statement=statement):
                for p,data in originals.items(): (self.f.remote.path/p).write_bytes(data)
                self.rewrite_nas_db(manifest['set_id'],lambda db:db.execute(statement))
                target = self.isolated_root('invalid-restore')
                with self.assertRaisesRegex(BackupError,reason): self.restorer.restore(manifest['set_id'],target)
                self.assertFalse((target.path/self.restorer.VERIFIED).exists())

    def test_external_occupancy_survives_while_unfinished_uploads_and_jobs_are_fenced(self):
        _,_,asset = self.h.first_revision()
        self.h.jobs.enqueue(kind='summary',idempotency_key='restore-summary',payload={},object_sha=asset['sha256'])
        worker = self.h.jobs.claim('synthetic-summary',kind='summary',lease_ms=300000)
        self.h.jobs.acquire_resource(worker,'local-qwen')
        result = BackupCoordinator(self.f.store).run(self.f.f.backup_job())
        self.restorer.restore(result['set_id'],self.target)
        with self.db() as db:
            self.assertEqual(db.execute("SELECT job_id,generation,token_hash FROM resource_slots WHERE name='local-qwen'").fetchone(),(worker.job_id,worker.generation,DISABLED_RESOURCE))
            self.assertEqual(db.execute('SELECT lease_generation FROM jobs WHERE id=?',(worker.job_id,)).fetchone(),(worker.generation+1,))
            self.assertEqual(db.execute('SELECT blocked FROM job_clock').fetchone(),(1,))
        plan = json.loads((self.target.path/self.restorer.PLAN).read_bytes())
        self.assertEqual(plan['held_external_resources'],1)
        self.assertFalse(plan['activation_allowed'])

    def test_archived_original_is_copied_and_recovered_live_without_reusing_old_gc_authority(self):
        old,_,asset = self.h.first_revision()
        prior = BackupCoordinator(self.f.store).run(self.f.f.backup_job())
        self.f.f.publish_next(old)
        self.h.jobs.cancel(self.h.scalar("SELECT id FROM jobs WHERE kind='backup_revision' AND input_revision_id=:id",id=old))
        with write_transaction(self.h.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'),dict(id=old))
            conn.execute(text("UPDATE objects SET state='PURGED' WHERE sha256=:sha"),dict(sha=asset['sha256']))
        (self.h.path/object_path(asset['sha256'])).unlink()
        final = BackupCoordinator(self.f.store).run(self.f.f.backup_job())
        physical = self.reader.verify_set(final['set_id'])
        self.assertEqual(len(physical['manifest']['archived_originals']),1)
        result = self.restorer.restore(final['set_id'],self.target)
        self.assertEqual(result['copied_originals'],2)
        with self.db() as db:
            self.assertEqual(db.execute('SELECT state FROM objects WHERE sha256=?',(asset['sha256'],)).fetchone(),('LIVE',))
            self.assertEqual(db.execute('SELECT count(*) FROM object_backup_receipts WHERE valid=1').fetchone(),(0,))
        self.assertEqual((self.target.path/object_path(asset['sha256'])).read_bytes(),b'novel 1-150')
        self.assertEqual(self.reader.verify_set(prior['set_id'])['seal']['manifest_sha256'],prior['manifest_sha256'])
        self.f.remote.close()
        self.assertEqual(self.restorer.verify(self.target),result)

    def test_unreferenced_unbacked_object_is_missing_in_the_restored_database(self):
        self.h.first_revision()
        with write_transaction(self.h.engine) as conn:
            conn.execute(text("INSERT INTO objects VALUES(:sha,9,'text/plain',:path,'LIVE','synthetic')"),dict(sha='f'*64,path=object_path('f'*64)))
        backup = BackupCoordinator(self.f.store).run(self.f.f.backup_job())
        self.restorer.restore(backup['set_id'],self.target)
        with self.db() as db:
            self.assertEqual(db.execute('SELECT state FROM objects WHERE sha256=?',('f'*64,)).fetchone(),('MISSING',))
        self.assertFalse((self.target.path/object_path('f'*64)).exists())

    def test_verified_artifact_rejects_later_object_damage_and_activation_claim(self):
        manifest,asset = self.backed_up()
        self.restorer.restore(manifest['set_id'],self.target)
        path = self.target.path/object_path(asset['sha256'])
        data = path.read_bytes();path.write_bytes(b'changed')
        with self.assertRaises(PublicationError): self.restorer.verify(self.target)
        path.write_bytes(data)
        marker = self.target.path/self.restorer.VERIFIED
        result = json.loads(marker.read_bytes());result['activation_allowed']=True
        marker.write_text(canonical_json(result))
        with self.assertRaisesRegex(BackupError,'activation policy'): self.restorer.verify(self.target)


if __name__ == '__main__':
    unittest.main()
