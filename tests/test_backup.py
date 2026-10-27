"""Fixed snapshot, redaction and durable-pin acceptance with synthetic data."""
from contextlib import closing
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from document_library.auth import digest
from document_library.backup import SnapshotService, BackupError, DISABLED_PASSWORD, DISABLED_RESOURCE
from document_library.database import write_transaction
from document_library.jobs import LostLease, ClockBlocked
from document_library.lifecycle import LifecycleService
from document_library.publication import PublicationError, object_path
from document_library.storage import sync_file
from document_library.metadata_generation import run_generation
from test_metadata_generation import GeneratedFixture, SyntheticAdapter, novel_source
import test_publication as fixtures


class SnapshotAcceptance(unittest.TestCase):
    def setUp(self):
        self.h = fixtures.PublicationAcceptance()
        self.addCleanup(self.h.doCleanups)
        self.h.setUp()
        # Runtime provisioning must create a private DB, not rely on umask.
        os.chmod(self.h.path/'database/test.sqlite3',0o600)
        self.backups = SnapshotService(self.h.publisher)

    def backup_job(self):
        job = self.h.jobs.enqueue(kind='backup_catalog',idempotency_key=str(uuid.uuid4()),payload={})
        lease = self.h.jobs.claim('synthetic-backup-worker',kind='backup_catalog',lease_ms=300000)
        self.assertEqual(lease.job_id,job)
        return lease

    def database_path(self, manifest):
        return self.h.path/self.backups.paths(manifest['set_id'])['database']

    def record(self, set_id=None):
        with self.h.engine.connect() as conn:
            if set_id is None:
                return dict(conn.execute(text('SELECT * FROM backup_sets')).mappings().one())
            return dict(self.backups._row(conn,set_id))

    def publish_next(self, previous):
        h = self.h
        old = h.scalar('SELECT id FROM revision_options WHERE edition_revision_id=:id',id=previous)
        lease = h.make_job(previous)
        asset = h.asset(lease,b'new-original-1-160')
        option = dict(fixtures.option(0,end=160),replaces_option_id=old,replacement_receipt='synthetic-contained')
        h.plan(lease,[asset],[option],previous=previous)
        return h.publisher.publish(lease),asset


    def test_member_preferences_social_rows_preserved_but_g3_quota_redacted(self):
        h = self.h
        h.first_revision()
        from document_library.member_service import MemberService
        from document_library.request_quota import AccountQuota
        quota = AccountQuota(h.auth)
        members = MemberService(h.auth, quota=quota, read_social_in=lambda *args, **kwargs: {})
        value = members.get_preferences(h.token)
        value.pop('updated_at')
        value['view'] = 'list'
        saved = members.set_preferences(h.token, 'synthetic-csrf', value)
        quota.admit(h.token, action='recommend')
        quota.admit(h.token, action='download')
        with write_transaction(h.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w',9,'test')"))
            conn.execute(text("INSERT INTO library_favorites VALUES('u','w','test')"))
        result = self.backups.capture(self.backup_job())
        with closing(sqlite3.connect(self.database_path(result), autocommit=True)) as db:
            self.backups._schema(db)
            self.backups._integrity(db)
            self.backups._redaction_verified(db)
            payload, updated = db.execute('SELECT preferences_json,updated_at FROM member_preferences').fetchone()
            self.assertEqual(json.loads(payload), value)
            self.assertEqual(updated, saved['updated_at'])
            self.assertEqual(db.execute('SELECT rating_half_steps FROM ratings').fetchone(), (9,))
            self.assertEqual(db.execute('SELECT count(*) FROM library_favorites').fetchone(), (1,))
            for table in ('auth_sessions','auth_tokens','auth_limits'):
                self.assertEqual(db.execute(f'SELECT count(*) FROM {table}').fetchone(), (0,))
        self.assertEqual(h.scalar("SELECT count(*) FROM auth_limits WHERE bucket LIKE 'g3:%'"), 3)

    def test_fixed_content_snapshot_keeps_originals_but_is_not_a_sealed_backup(self):
        h = self.h
        _,_,asset = h.first_revision()
        lease = self.backup_job()
        result = self.backups.capture(lease)
        self.assertEqual([o['sha256'] for o in result['objects']],[asset['sha256']])
        self.assertEqual(result['storage_sequence'],1)
        self.assertEqual(self.record()['state'],'BUILDING')
        self.assertIsNone(self.record()['sealed_at'])
        self.assertEqual(h.scalar('SELECT count(*) FROM object_backup_receipts'),0)
        self.assertEqual(h.jobs.get(lease.job_id)['state'],'running')
        before = self.database_path(result).read_bytes()
        self.assertEqual(self.backups.capture(lease),result)
        self.assertEqual(self.backups.verify_local(result['set_id']),result)
        self.assertEqual(self.database_path(result).read_bytes(),before)
        self.assertEqual(h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)

    def test_authentication_material_removed_before_any_snapshot_disk_output(self):
        h = self.h
        h.first_revision()
        secrets = ['synthetic-password-sentinel-'+uuid.uuid4().hex,'synthetic-token-sentinel-'+uuid.uuid4().hex,
                   'synthetic-preview-sentinel-'+uuid.uuid4().hex,'synthetic-rate-sentinel-'+uuid.uuid4().hex]
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE users SET password_hash=:value'),dict(value=secrets[0]))
            conn.execute(text("INSERT INTO auth_tokens VALUES(:value,'invite',NULL,'u',1,2,NULL,NULL)"),dict(value=secrets[1]))
            conn.execute(text("INSERT INTO permission_previews VALUES(:value,'u','{}',1,2,NULL)"),dict(value=secrets[2]))
            conn.execute(text('INSERT INTO auth_limits VALUES(:value,1,1,2)'),dict(value=secrets[3]))
        write_db = self.backups._write_database
        def inspect_before_write(db,path):
            self.assertEqual(db.execute('SELECT password_hash,status FROM users').fetchone(),(DISABLED_PASSWORD,'suspended'))
            for table in ('auth_sessions','auth_tokens','auth_limits','permission_previews'):
                self.assertEqual(db.execute(f'SELECT count(*) FROM {table}').fetchone()[0],0)
            return write_db(db,path)
        with patch.object(self.backups,'_write_database',side_effect=inspect_before_write):
            result = self.backups.capture(self.backup_job())
        blob = self.database_path(result).read_bytes()
        for value in secrets+[digest(h.token),digest('synthetic-csrf')]:
            self.assertNotIn(value.encode(),blob)
        self.assertEqual(h.scalar('SELECT password_hash FROM users'),secrets[0])
        self.assertEqual(h.scalar('SELECT status FROM users'),'active')
        self.assertEqual(h.scalar('SELECT count(*) FROM auth_sessions'),1)
        self.assertFalse(any(Path(str(self.database_path(result))+suffix).exists() for suffix in ('-wal','-shm','-journal')))

    def test_optimizer_statistics_removed_before_disk_and_live_stats_untouched(self):
        h = self.h
        with closing(sqlite3.connect(h.path/'database/test.sqlite3',autocommit=True)) as db:
            db.execute('ANALYZE')
            self.assertGreater(db.execute('SELECT count(*) FROM sqlite_stat1').fetchone()[0],0)
        original_write = self.backups._write_database
        def inspect_before_write(db,path):
            self.assertEqual(db.execute('SELECT count(*) FROM sqlite_stat1').fetchone()[0],0)
            # Verification must independently reject residual optimizer data.
            db.execute("INSERT INTO sqlite_stat1 VALUES('users',NULL,'1')")
            with self.assertRaisesRegex(BackupError,'Optimizer statistics'):
                self.backups._redaction_verified(db)
            db.execute('DELETE FROM sqlite_stat1')
            return original_write(db,path)
        with patch.object(self.backups,'_write_database',side_effect=inspect_before_write):
            result = self.backups.capture(self.backup_job())
        with closing(sqlite3.connect(self.database_path(result))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sqlite_stat1').fetchone()[0],0)
        self.assertGreater(h.scalar('SELECT count(*) FROM sqlite_stat1'),0)

    def test_persisted_stat4_credential_sample_does_not_reach_output(self):
        # Explicit cross-build fixture: a non-STAT4 runtime cannot naturally
        # generate these samples, but can receive a DB analyzed by another build.
        h = self.h
        marker = 'synthetic-stat4-' + uuid.uuid4().hex
        with closing(sqlite3.connect(h.path/'database/test.sqlite3',autocommit=True)) as db:
            db.execute("INSERT INTO auth_sessions VALUES(?,'synthetic-csrf','u',1,1,2)",(marker,))
            db.execute('ANALYZE')
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='sqlite_stat4'").fetchone():
                db.execute('PRAGMA writable_schema=ON')
                db.execute('CREATE TABLE sqlite_stat4(tbl,idx,neq,nlt,ndlt,sample)')
                db.execute('PRAGMA writable_schema=OFF')
                rowid = db.execute('SELECT rowid FROM auth_sessions WHERE token_hash=?',(marker,)).fetchone()[0]
                self.assertLess(rowid,128)
                sample = bytes([3,13+2*len(marker),1]) + marker.encode() + bytes([rowid])
                db.execute("INSERT INTO sqlite_stat4 VALUES('auth_sessions','sqlite_autoindex_auth_sessions_1','1 1','0 0','0 0',?)",(sample,))
            self.assertTrue(any(marker.encode() in row[0] for row in db.execute('SELECT sample FROM sqlite_stat4')))
        result = self.backups.capture(self.backup_job())
        self.assertNotIn(marker.encode(),self.database_path(result).read_bytes())
        with closing(sqlite3.connect(self.database_path(result))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sqlite_stat4').fetchone()[0],0)
        self.assertGreater(h.scalar('SELECT count(*) FROM sqlite_stat4'),0)

    def test_snapshot_point_excludes_a_later_publication_and_pins_precede_disk_work(self):
        h = self.h
        first,_,asset = h.first_revision()
        write_db = self.backups._write_database
        later = []
        def publish_after_snapshot(db,path):
            self.assertEqual(h.scalar('SELECT count(*) FROM backup_members'),1)
            self.assertEqual(h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)
            later.append(self.publish_next(first))
            h.clock.advance(10)
            return write_db(db,path)
        lease = self.backup_job()
        with patch.object(self.backups,'_write_database',side_effect=publish_after_snapshot):
            result = self.backups.capture(lease)
        self.assertEqual(result['storage_sequence'],1)
        self.assertEqual(h.scalar('SELECT value FROM storage_sequence'),2)
        self.assertLess(result['snapshot_at_ms'],h.clock.wall)
        self.assertEqual([o['sha256'] for o in result['objects']],[asset['sha256']])
        with closing(sqlite3.connect(self.database_path(result))) as db:
            self.assertEqual(db.execute('SELECT id FROM edition_revisions').fetchall(),[(first,)])
        self.assertTrue((h.path/object_path(later[0][1]['sha256'])).is_file())

    def test_pins_preserve_original_after_all_snapshot_source_references_are_released(self):
        h = self.h
        revision,_,asset = h.first_revision()
        write_db = self.backups._write_database
        def release_original_refs(db,path):
            with write_transaction(h.engine) as conn:
                conn.execute(text('UPDATE edition_revisions SET is_active=0'))
                conn.execute(text('UPDATE revision_seals SET retained=0'))
            backup = h.scalar("SELECT id FROM jobs WHERE kind='backup_revision'")
            h.jobs.cancel(backup)
            self.assertEqual(LifecycleService(h.publisher).collect(asset['sha256']),'protected')
            return write_db(db,path)
        with patch.object(self.backups,'_write_database',side_effect=release_original_refs):
            result = self.backups.capture(self.backup_job())
        self.assertEqual([o['sha256'] for o in result['objects']],[asset['sha256']])
        self.assertEqual(LifecycleService(h.publisher).collect(asset['sha256']),'protected')

    def test_missing_or_changed_required_bytes_never_become_ready(self):
        h = self.h
        _,_,asset = h.first_revision()
        (h.path/object_path(asset['sha256'])).unlink()
        lease = self.backup_job()
        with self.assertRaises(FileNotFoundError): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')
        self.assertEqual(h.scalar('SELECT count(*) FROM object_pins'),1)
        with h.root.file(object_path(asset['sha256']),create=True) as fd: os.write(fd,b'corrupt')
        with self.assertRaises(PublicationError): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')

    def test_failed_disk_write_retains_pin_and_does_not_recapture_a_new_catalog(self):
        h = self.h
        h.first_revision()
        lease = self.backup_job()
        with patch.object(self.backups,'_write_database',side_effect=OSError('synthetic-disk-error')):
            with self.assertRaises(OSError): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')
        self.assertEqual(h.scalar('SELECT count(*) FROM backup_members'),1)
        with patch.object(self.backups,'_memory_snapshot',side_effect=AssertionError('must not take newer snapshot')):
            with self.assertRaises(BackupError): self.backups.capture(lease)
        self.assertEqual(h.scalar('SELECT count(*) FROM object_pins'),1)

    def test_completed_artifacts_resume_after_final_database_failure(self):
        h = self.h
        h.first_revision()
        lease = self.backup_job()
        with patch.object(self.backups,'_finish',side_effect=RuntimeError('before-ready')):
            with self.assertRaises(RuntimeError): self.backups.capture(lease)
        row = self.record()
        path = h.path/self.backups.paths(row['id'])['database']
        original = path.read_bytes()
        result = self.backups.capture(lease)
        self.assertEqual(path.read_bytes(),original)
        self.assertEqual(self.record()['state'],'BUILDING')
        self.assertEqual(result['set_id'],row['id'])

    def test_lost_ownership_after_database_output_retains_capture_without_ready(self):
        h = self.h
        h.first_revision()
        lease = self.backup_job()
        write_db = self.backups._write_database
        def lose_lease(db,path):
            write_db(db,path)
            h.jobs.cancel(lease.job_id)
        with patch.object(self.backups,'_write_database',side_effect=lose_lease):
            with self.assertRaises(LostLease): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')
        self.assertEqual(h.scalar('SELECT count(*) FROM object_pins'),1)
        self.assertIsNone(self.record()['manifest_sha'])

    def test_manifest_flush_failure_requires_fresh_durability_before_ready(self):
        h = self.h
        h.first_revision()
        lease = self.backup_job()
        set_id = str(uuid.uuid5(uuid.UUID(lease.job_id),f'backup:{lease.generation}'))
        manifest_path = h.path/self.backups.paths(set_id)['manifest']
        injected = []
        def fail_manifest_sync(fd):
            if manifest_path.exists() and os.fstat(fd).st_ino == manifest_path.stat().st_ino:
                injected.append(True)
                raise OSError('synthetic-manifest-flush')
            sync_file(fd)
        with patch('document_library.storage.sync_file',side_effect=fail_manifest_sync):
            with self.assertRaisesRegex(OSError,'synthetic-manifest-flush'): self.backups.capture(lease)
        self.assertEqual(injected,[True])
        self.assertEqual(self.record()['state'],'CAPTURING')
        with patch('document_library.backup.sync_file',wraps=sync_file) as flush, patch.object(h.root,'sync_parent',wraps=h.root.sync_parent) as parent_flush:
            self.backups.capture(lease)
            self.assertEqual(flush.call_count,2)
            self.assertEqual(parent_flush.call_count,2)
        self.assertEqual(self.record()['state'],'BUILDING')

    def test_external_resource_input_survives_redaction_without_reusing_capability(self):
        h = self.h
        revision,_,asset = h.first_revision()
        job = h.jobs.enqueue(kind='synthetic-ocr',idempotency_key=str(uuid.uuid4()),payload={},input_revision_id=revision)
        execution = h.jobs.claim('synthetic-ocr-worker',kind='synthetic-ocr')
        hold = h.jobs.acquire_resource(execution,'local-ocr')
        h.jobs.cancel(job)
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE edition_revisions SET is_active=0'))
            conn.execute(text('UPDATE revision_seals SET retained=0'))
        h.jobs.cancel(h.scalar("SELECT id FROM jobs WHERE kind='backup_revision'"))
        result = self.backups.capture(self.backup_job())
        self.assertEqual([o['sha256'] for o in result['objects']],[asset['sha256']])
        with closing(sqlite3.connect(self.database_path(result))) as db:
            self.assertEqual(db.execute("SELECT job_id,generation,token_hash FROM resource_slots WHERE name='local-ocr'").fetchone(),(job,execution.generation,DISABLED_RESOURCE))
            self.assertEqual(db.execute('SELECT blocked FROM job_clock').fetchone(),(1,))
        self.assertNotIn(digest(hold.token).encode(),self.database_path(result).read_bytes())
        self.assertEqual(h.scalar("SELECT token_hash FROM resource_slots WHERE name='local-ocr'"),digest(hold.token))

    def test_unknown_schema_and_metadata_limit_fail_before_snapshot_disk_output(self):
        h = self.h
        lease = self.backup_job()
        with write_transaction(h.engine) as conn:
            conn.execute(text('CREATE TABLE unknown_auth_material(value TEXT)'))
        with self.assertRaisesRegex(BackupError,'policy review'): self.backups.capture(lease)
        self.assertFalse((h.path/'backup').exists())
        with write_transaction(h.engine) as conn: conn.execute(text('DROP TABLE unknown_auth_material'))
        h.jobs.cancel(lease.job_id)
        bounded = SnapshotService(h.publisher,max_snapshot_bytes=4096)
        with self.assertRaisesRegex(BackupError,'memory policy'): bounded.capture(self.backup_job())
        self.assertFalse((h.path/'backup').exists())

    def test_manifest_and_membership_identity_are_immutable(self):
        h = self.h
        h.first_revision()
        result = self.backups.capture(self.backup_job())
        for sql in ("UPDATE backup_sets SET snapshot_at=0","UPDATE backup_sets SET manifest_json='{}'",
                    "UPDATE backup_members SET set_id=set_id","DELETE FROM backup_members"):
            with self.subTest(sql=sql),self.assertRaises(IntegrityError):
                with write_transaction(h.engine) as conn: conn.execute(text(sql))
        path = h.path/self.backups.paths(result['set_id'])['manifest']
        path.write_bytes(b'{}')
        with self.assertRaises(PublicationError): self.backups.verify_local(result['set_id'])

    def test_sqlite_similar_user_table_cannot_bypass_redaction_policy(self):
        h = self.h
        with write_transaction(h.engine) as conn:
            conn.execute(text('CREATE TABLE sqliteXauth(value TEXT)'))
            conn.execute(text("INSERT INTO sqliteXauth VALUES('synthetic-hidden-auth-sentinel')"))
        with self.assertRaisesRegex(BackupError,'policy review'):
            self.backups.capture(self.backup_job())
        self.assertFalse((h.path/'backup').exists())
        self.assertEqual(self.record()['state'],'CAPTURING')
        self.assertIsNone(self.record()['manifest_sha'])

    def test_removed_pin_after_validation_prevents_ready(self):
        h = self.h
        _,_,asset = h.first_revision()
        lease = self.backup_job()
        validate = self.backups._validate
        def unpin_after_hash(set_id):
            result = validate(set_id)
            with write_transaction(h.engine) as conn: conn.execute(text('DELETE FROM object_pins'))
            return result
        with patch.object(self.backups,'_validate',side_effect=unpin_after_hash):
            with self.assertRaisesRegex(IntegrityError,'terminal coordinator record'): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')
        self.assertEqual(h.scalar("SELECT count(*) FROM object_pins WHERE kind='backup'"),1)
        self.assertTrue((h.path/object_path(asset['sha256'])).is_file())

    def test_changed_file_after_validation_prevents_ready(self):
        h = self.h
        _,_,asset = h.first_revision()
        lease = self.backup_job()
        validate = self.backups._validate
        def change_after_hash(set_id):
            result = validate(set_id)
            (h.path/object_path(asset['sha256'])).write_bytes(b'changed-after-validation')
            return result
        with patch.object(self.backups,'_validate',side_effect=change_after_hash):
            with self.assertRaises(PublicationError): self.backups.capture(lease)
        self.assertEqual(self.record()['state'],'CAPTURING')

    def test_historical_live_original_is_backed_up_before_it_has_a_receipt(self):
        h = self.h
        old,_,asset = h.first_revision()
        _,new_asset = self.publish_next(old)
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'),dict(id=old))
        h.jobs.cancel(h.scalar("SELECT id FROM jobs WHERE kind='backup_revision' AND input_revision_id=:id",id=old))
        result = self.backups.capture(self.backup_job())
        self.assertEqual({o['sha256'] for o in result['objects']},{asset['sha256'],new_asset['sha256']})
        self.assertEqual(result['archived_originals'],[])

    def test_unavailable_historical_original_requires_separate_retained_receipt(self):
        h = self.h
        old,_,asset = h.first_revision()
        self.publish_next(old)
        with write_transaction(h.engine) as conn:
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'),dict(id=old))
        h.jobs.cancel(h.scalar("SELECT id FROM jobs WHERE kind='backup_revision' AND input_revision_id=:id",id=old))
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE objects SET state='PURGED' WHERE sha256=:sha"),dict(sha=asset['sha256']))
        (h.path/object_path(asset['sha256'])).unlink()
        lease = self.backup_job()
        with self.assertRaisesRegex(BackupError,'retained backup receipt'): self.backups.capture(lease)
        h.jobs.cancel(lease.job_id)
        with write_transaction(h.engine) as conn:
            conn.execute(text("INSERT INTO object_backup_receipts VALUES('synthetic-receipt',:sha,:size,'synthetic-older-set','objects/retained',:manifest,:now,1)"),
                         dict(sha=asset['sha256'],size=asset['byte_size'],manifest='a'*64,now=h.clock.wall))
        result = self.backups.capture(self.backup_job())
        self.assertEqual([o['sha256'] for o in result['archived_originals']],[asset['sha256']])
        self.assertNotIn(asset['sha256'],[o['sha256'] for o in result['objects']])

    def test_clock_fence_and_wrong_job_cannot_start_capture(self):
        h = self.h
        wrong = h.make_job()
        with self.assertRaises(BackupError): self.backups.capture(wrong)
        self.assertEqual(h.scalar('SELECT count(*) FROM backup_sets'),0)
        lease = self.backup_job()
        h.clock.wall += 100000
        with self.assertRaises(ClockBlocked): self.backups.capture(lease)
        self.assertEqual(h.scalar('SELECT count(*) FROM backup_sets'),0)

    def test_empty_catalog_still_has_verified_redacted_database(self):
        result = self.backups.capture(self.backup_job())
        self.assertEqual(result['objects'],[])
        self.assertEqual(result['archived_originals'],[])
        self.assertEqual(self.backups.verify_local(result['set_id']),result)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM object_pins'),0)


class GeneratedSnapshotAcceptance(GeneratedFixture):
    def test_generated_binding_and_review_survive_auth_redaction(self):
        option_id=self.prepare_job(source_text=novel_source())
        generated=asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
            adapter=SyntheticAdapter(self.profile),profile=self.profile,codec=self.codec,
            context_tokens=4096,output_reserve_tokens=1024))
        token,csrf=self.reviewer_session()
        detail=self.metadata.generated_review_detail(token,candidate_id=generated['candidate_id'])
        tail=detail['excluded_tail_range']
        ranges=[dict(start=0,end=100),dict(start=tail['start'],end=tail['start']+40)]
        evidence=self.metadata.read_generated_evidence(token,candidate_id=generated['candidate_id'],ranges=ranges)
        flags=[code for code in detail['qa_codes']
               if code not in {'SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'}]
        self.metadata.review_generated_candidate(token,csrf,candidate_id=generated['candidate_id'],
            expected_control_version=evidence['expected_control_version'],
            expected_head_version=evidence['expected_head_version'],
            expected_rights_version=evidence['expected_rights_version'],request_id=str(uuid.uuid4()),
            review_context_digest=evidence['review_context_digest'],ranges=ranges,
            review_units=[dict(unit_id=detail['review_units'][0]['unit_id'],range_indexes=[0])],
            grounding_disposition='grounded',spoiler_disposition='acceptable',
            flag_resolutions=[dict(code=code,resolution='accept_with_evidence',range_indexes=[0])
                              for code in flags],decision='approved',select=True)

        backup=SnapshotService(self.publisher)
        self.h.jobs.enqueue(kind='backup_catalog',idempotency_key=str(uuid.uuid4()),payload={})
        lease=self.h.jobs.claim('synthetic-generated-backup',kind='backup_catalog',lease_ms=300000)
        self.assertEqual(self.h.jobs.get(lease.job_id)['kind'],'backup_catalog')
        manifest=backup.capture(lease)
        self.assertEqual(backup.verify_local(manifest['set_id']),manifest)
        database=self.h.path/backup.paths(manifest['set_id'])['database']
        with closing(sqlite3.connect(database,autocommit=True)) as db:
            backup._schema(db)
            backup._integrity(db)
            backup._redaction_verified(db)
            self.assertEqual(db.execute('SELECT count(*) FROM metadata_generated_bindings').fetchone(),(1,))
            self.assertEqual(db.execute('SELECT count(*) FROM metadata_generated_payloads').fetchone(),(1,))
            self.assertEqual(db.execute("SELECT review_status FROM metadata_candidate_control WHERE candidate_id=?",
                                        (generated['candidate_id'],)).fetchone(),('approved',))
            audit=db.execute('SELECT audit_context_json FROM metadata_candidate_control WHERE candidate_id=?',
                             (generated['candidate_id'],)).fetchone()[0]
            self.assertNotIn('가'*300,audit)
            self.assertEqual(db.execute('SELECT count(*) FROM auth_sessions').fetchone(),(0,))
            self.assertEqual(db.execute('SELECT count(*) FROM users WHERE status<>\'suspended\' OR password_hash<>?',
                                        (DISABLED_PASSWORD,)).fetchone(),(0,))


if __name__ == '__main__':
    unittest.main()
