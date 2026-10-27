"""Real synthetic files plus SQLite publication failure and authority acceptance."""
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from document_library.auth import AuthService, AuthError, digest
from document_library.database import database_engine, migrate, write_transaction
from document_library.jobs import JobService, LostLease
from document_library.permissions import PermissionService
from document_library.publication import PublicationService, PublicationError, object_path
from document_library.storage import StorageRoot, StorageLocks, StorageBusy, sync_file
from test_jobs import FakeClock
from test_storage import SyntheticVolume

ROOT = Path(__file__).resolve().parents[1]


def option(position, format='txt', end=150):
    return dict(format=format,delivery_kind='single_file',positions=[position],
                coverage=dict(unit='episode',intervals=[[1,end]],gaps=[],confidence='verified'))


class PublicationAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='document-library-publication-')
        self.path = Path(self.temp.name).resolve()
        self.root = StorageRoot(self.path,SyntheticVolume())
        with self.root.directory('database',create=True):
            pass
        self.engine = database_engine(self.path / 'database/test.sqlite3')
        self.addCleanup(self.cleanup)
        migrate(self.engine,ROOT / 'migrations')
        self.clock = FakeClock()
        self.jobs = JobService(self.engine,clock=self.clock)
        self.auth = AuthService(self.engine,clock=lambda:self.clock.wall//1000)
        self.permissions = PermissionService(self.auth)
        self.publisher = PublicationService(self.root,self.jobs,self.permissions)
        self.token = 'synthetic-session-only'
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('u','reader','disabled-synthetic','test')"))
            conn.execute(text("INSERT INTO user_permissions VALUES('u','content.upload','u','test')"))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,'u',1,:now,:end)"),dict(token=digest(self.token),csrf=digest('synthetic-csrf'),now=self.auth.now(),end=self.auth.now()+3600))
            conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('w','novel','Synthetic','synthetic','test','test')"))
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,source_form,edition_label,created_at) VALUES('e','w','text','text','born_digital','Text','test')"))

    def cleanup(self):
        self.engine.dispose()
        self.root.close()
        self.temp.cleanup()
        self.assertFalse(self.path.exists(),'Synthetic objects/DB/WAL/SHM must be absent')

    def scalar(self,sql,**params):
        with self.engine.connect() as conn:
            return conn.execute(text(sql),params).scalar_one()

    def make_job(self,previous=None):
        ident = str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO upload_batches VALUES(:id,'u',100,'open',:now)"),dict(id=ident,now=self.auth.now()))
            conn.execute(text("INSERT INTO upload_sessions VALUES(:id,:id,'u',NULL,1,100,100,:end,'staged')"),dict(id=ident,end=self.auth.now()+300))
        job = self.jobs.enqueue(kind='ingest_upload',idempotency_key=ident,payload={},work_id='w',input_revision_id=previous,upload_session_id=ident)
        lease = self.jobs.claim('synthetic-worker',kind='ingest_upload')
        self.assertEqual(lease.job_id,job)
        return lease

    def asset(self,lease,payload=b'novel 1-150',format='txt'):
        with self.root.stage(lease) as (path,fd):
            os.write(fd,payload)
        return self.publisher.prepare(lease,path,sha256=hashlib.sha256(payload).hexdigest(),byte_size=len(payload),detected_mime='text/plain',file_format=format)

    def plan(self,lease,assets=None,options=None,previous=None,**kwargs):
        assets = assets or [self.asset(lease)]
        options = options or [option(0)]
        return self.publisher.register_validated_upload(lease,edition_id='e',expected_revision_id=previous,
            assets=assets,options=options,validation_receipt='synthetic-validation',rights_status='allowed',**kwargs)

    def first_revision(self):
        lease = self.make_job()
        asset = self.asset(lease)
        self.plan(lease,[asset])
        return self.publisher.publish(lease),lease,asset

    def assert_unpublished(self,lease):
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_seals'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_revision'"),0)
        self.assertEqual(self.jobs.get(lease.job_id)['state'],'running')

    def test_publication_atomically_seals_revision_source_backup_and_job(self):
        revision,lease,asset = self.first_revision()
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'),revision)
        self.assertEqual(json.loads(self.jobs.get(lease.job_id)['result_json']),{'revision_id':revision})
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'),1)
        self.assertEqual(self.scalar("SELECT count(*) FROM storage_intents WHERE state='committed'"),1)
        self.assertEqual(self.scalar("SELECT status FROM upload_sessions"),'published')
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_revision' AND state='queued'"),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),1)
        with self.root.file(object_path(asset['sha256'])) as fd:
            self.assertEqual(os.read(fd,100),b'novel 1-150')
        with self.assertRaises(LostLease):
            self.publisher.publish(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'),1)

    def test_more_than_100_objects_get_one_revision_backup_with_all_input_references(self):
        lease = self.make_job()
        assets = [self.asset(lease,f'page {n}'.encode()) for n in range(101)]
        bundle = dict(option(0),delivery_kind='bundle',positions=list(range(len(assets))))
        revision = self.plan(lease,assets,[bundle])
        self.assertEqual(self.publisher.publish(lease),revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_assets'),101)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_revision'"),1)
        backup = self.scalar("SELECT id FROM jobs WHERE kind='backup_revision'")
        self.assertEqual(self.jobs.get(backup)['input_revision_id'],revision)
        # Remove revision retention in the synthetic DB: the backup job alone
        # must still pin every input, rather than just a representative object.
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE edition_revisions SET is_active=0'))
            conn.execute(text('UPDATE revision_seals SET retained=0'))
            for asset in assets:
                self.assertEqual(self.publisher.reference_sources(conn,asset['sha256']),[('job',backup)])

    def test_upload_expiry_during_each_completion_effect_rolls_back_everything(self):
        lease = self.make_job()
        self.plan(lease)
        for marker in ('INSERT INTO jobs','INSERT INTO job_outbox'):
            with self.subTest(effect=marker):
                with write_transaction(self.engine) as conn:
                    conn.execute(text('UPDATE upload_sessions SET expires_at=:end'),dict(end=self.auth.now()+1))
                triggered = []
                def expire(conn,cursor,statement,parameters,context,executemany):
                    if marker in statement:
                        triggered.append(marker)
                        self.clock.advance(1000)
                event.listen(self.engine,'after_cursor_execute',expire)
                try:
                    with self.assertRaises(AuthError):
                        self.publisher.publish(lease)
                finally:
                    event.remove(self.engine,'after_cursor_execute',expire)
                self.assertEqual(triggered,[marker])
                self.assert_unpublished(lease)
                self.assertEqual(self.scalar('SELECT status FROM upload_sessions'),'staged')
                self.assertEqual(self.scalar('SELECT count(*) FROM revision_asset_sources'),0)

    def test_rename_then_db_failure_preserves_intent_and_retry_recovers(self):
        lease = self.make_job()
        asset = self.asset(lease)
        revision = self.plan(lease,[asset])
        write = self.publisher._write_revision
        def fail_after_rows(*args):
            write(*args)
            raise RuntimeError('synthetic commit-side failure')
        with patch.object(self.publisher,'_write_revision',side_effect=fail_after_rows):
            with self.assertRaises(RuntimeError):
                self.publisher.publish(lease)
        self.assert_unpublished(lease)
        intent = self.scalar('SELECT id FROM storage_intents')
        self.assertEqual(self.publisher.inspect_intent(intent)['state'],'installed_uncommitted')
        self.assertTrue((self.path / object_path(asset['sha256'])).is_file())
        self.assertEqual(self.publisher.publish(lease),revision)
        self.assertEqual(self.publisher.inspect_intent(intent)['state'],'committed')

    def test_prepare_detaches_live_worker_fd_and_rejects_wrong_hash(self):
        lease = self.make_job()
        with self.root.stage(lease) as (path,worker_fd):
            os.write(worker_fd,b'original')
            asset = self.publisher.prepare(lease,path,sha256=hashlib.sha256(b'original').hexdigest(),byte_size=8,detected_mime='text/plain',file_format='txt')
            os.lseek(worker_fd,0,os.SEEK_SET)
            os.write(worker_fd,b'changed!')
            self.plan(lease,[asset])
            self.publisher.publish(lease)
        with self.root.file(object_path(asset['sha256'])) as fd:
            self.assertEqual(os.read(fd,100),b'original')
        with self.assertRaises(PublicationError):
            self.publisher.prepare(lease,path,sha256='f'*64,byte_size=8,detected_mime='text/plain',file_format='txt')

    def test_rename_flush_failure_is_not_success_and_retry_flushes_existing_object(self):
        lease = self.make_job()
        asset = self.asset(lease)
        self.plan(lease,[asset])
        destination = self.path / object_path(asset['sha256'])
        original = os.fsync
        def fail_after_rename(fd):
            if destination.exists():
                raise OSError('synthetic directory sync failure')
            return original(fd)
        with patch('document_library.publication.os.fsync',side_effect=fail_after_rename):
            with self.assertRaises(OSError):
                self.publisher.publish(lease)
        self.assertTrue(destination.is_file())
        self.assert_unpublished(lease)
        with patch('document_library.publication.sync_file',wraps=sync_file) as sync:
            self.publisher.publish(lease)
            sync.assert_called_once()

    def test_permission_revoked_before_install_rejects_without_files(self):
        lease = self.make_job()
        self.plan(lease)
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM user_permissions WHERE user_id='u'"))
        with patch.object(self.publisher,'_install') as install:
            with self.assertRaises(AuthError):
                self.publisher.publish(lease)
            install.assert_not_called()
        self.assert_unpublished(lease)

    def test_permission_revoked_after_install_blocks_atomic_commit(self):
        lease = self.make_job()
        self.plan(lease)
        install = self.publisher._install
        def revoke(asset,verified):
            install(asset,verified)
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE users SET authz_version=authz_version+1 WHERE id='u'"))
        with patch.object(self.publisher,'_install',side_effect=revoke):
            with self.assertRaises(AuthError):
                self.publisher.publish(lease)
        self.assert_unpublished(lease)
        self.assertEqual(self.publisher.inspect_intent(self.scalar('SELECT id FROM storage_intents'))['state'],'installed_uncommitted')

    def test_rights_revoked_after_install_and_expiry_during_commit_block_publication(self):
        lease = self.make_job()
        self.plan(lease)
        install = self.publisher._install
        def revoke(asset,verified):
            install(asset,verified)
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE publication_plans SET rights_status='blocked'"))
        with patch.object(self.publisher,'_install',side_effect=revoke):
            with self.assertRaises(PublicationError):
                self.publisher.publish(lease)
        self.assert_unpublished(lease)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed'"))
            conn.execute(text('UPDATE upload_sessions SET expires_at=:end'),dict(end=self.auth.now()+1))
        write = self.publisher._write_revision
        def expire(*args):
            write(*args)
            self.clock.advance(1000)
        with patch.object(self.publisher,'_write_revision',side_effect=expire):
            with self.assertRaises(AuthError):
                self.publisher.publish(lease)
        self.assert_unpublished(lease)

    def test_stale_generation_and_lease_expiry_after_install_cannot_publish(self):
        lease = self.make_job()
        self.plan(lease)
        install = self.publisher._install
        def expire(asset,verified):
            install(asset,verified)
            self.clock.advance(30000)
        with patch.object(self.publisher,'_install',side_effect=expire):
            with self.assertRaises(LostLease):
                self.publisher.publish(lease)
        self.assert_unpublished(lease)
        newer = self.jobs.claim('new-worker',kind='ingest_upload')
        self.assertGreater(newer.generation,lease.generation)
        with patch.object(self.publisher,'_install') as install:
            with self.assertRaises(LostLease):
                self.publisher.publish(lease)
            install.assert_not_called()

    def test_conflicting_existing_object_is_never_overwritten(self):
        lease = self.make_job()
        asset = self.asset(lease)
        self.plan(lease,[asset])
        with self.root.file(object_path(asset['sha256']),create=True) as fd:
            os.write(fd,b'corrupt bytes')
        with self.assertRaises(PublicationError):
            self.publisher.publish(lease)
        with self.root.file(object_path(asset['sha256'])) as fd:
            self.assertEqual(os.read(fd,100),b'corrupt bytes')
        self.assert_unpublished(lease)

    def test_new_txt_preserves_epub_coverage_and_old_revision_is_immutable(self):
        lease = self.make_job()
        txt,epub = self.asset(lease),self.asset(lease,b'epub 1-150','epub')
        self.plan(lease,[txt,epub],[option(0),option(1,'epub')])
        first = self.publisher.publish(lease)
        old_option = self.scalar("SELECT id FROM revision_options WHERE format='txt'")
        newer = self.make_job(first)
        next_txt = self.asset(newer,b'txt 1-160')
        retained_epub = dict(epub,prepared_relpath=None)
        replaced = dict(option(0,end=160),replaces_option_id=old_option,replacement_receipt='synthetic-overlap-verified')
        self.plan(newer,[next_txt,retained_epub],[replaced,option(1,'epub')],previous=first)
        second = self.publisher.publish(newer)
        self.assertNotEqual(first,second)
        self.assertEqual(json.loads(self.scalar("SELECT coverage_json FROM revision_options WHERE edition_revision_id=:id AND format='epub'",id=second))['intervals'],[[1,150]])
        self.assertEqual(json.loads(self.scalar("SELECT coverage_json FROM revision_options WHERE id=:id",id=old_option))['intervals'],[[1,150]])
        statements = ["DELETE FROM revision_assets WHERE edition_revision_id=:id",
                      "UPDATE revision_assets SET file_format='pdf' WHERE edition_revision_id=:id",
                      "UPDATE revision_options SET format='pdf' WHERE edition_revision_id=:id",
                      "DELETE FROM option_assets WHERE edition_revision_id=:id",
                      "DELETE FROM revision_asset_sources WHERE edition_revision_id=:id",
                      "UPDATE revision_asset_sources SET source_generation=99 WHERE edition_revision_id=:id",
                      "DELETE FROM revision_seals WHERE revision_id=:id",
                      "UPDATE edition_revisions SET revision_no=99 WHERE id=:id"]
        for statement in statements:
            with self.subTest(sql=statement), self.assertRaises(IntegrityError):
                with write_transaction(self.engine) as conn:
                    conn.execute(text(statement),dict(id=first))

    def test_inherited_source_revocation_blocks_before_and_after_install_and_later_reads(self):
        first,original,asset = self.first_revision()
        newer = self.make_job(first)
        reused = dict(asset,prepared_relpath=None)
        self.plan(newer,[reused],previous=first)
        def source_rights(status):
            with write_transaction(self.engine) as conn:
                conn.execute(text('UPDATE publication_plans SET rights_status=:status WHERE job_id=:id'),
                             dict(status=status,id=original.job_id))
        source_rights('blocked')
        with patch.object(self.publisher,'_install') as install:
            with self.assertRaises(PublicationError):
                self.publisher.publish(newer)
            install.assert_not_called()
        source_rights('allowed')
        install = self.publisher._install
        def revoke_after_install(*args):
            install(*args)
            source_rights('blocked')
        with patch.object(self.publisher,'_install',side_effect=revoke_after_install):
            with self.assertRaises(PublicationError):
                self.publisher.publish(newer)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'),first)
        self.assertEqual(self.jobs.get(newer.job_id)['state'],'running')
        source_rights('allowed')
        second = self.publisher.publish(newer)
        third_job = self.make_job(second)
        self.plan(third_job,[reused],previous=second)
        third = self.publisher.publish(third_job)
        self.assertEqual(self.scalar('SELECT source_job_id FROM revision_asset_sources WHERE edition_revision_id=:id',id=third),original.job_id)
        source_rights('blocked')
        for revision in (second,third):
            with self.subTest(revision=revision), self.assertRaises(PublicationError):
                with self.publisher.read_revision(self.token,revision,[0]):
                    self.fail('Inherited source revocation was replaced by a new approval')
        # An explicit fresh, independently approved upload has its own source.
        fresh_job = self.make_job(third)
        self.plan(fresh_job,[self.asset(fresh_job)],previous=third)
        fresh = self.publisher.publish(fresh_job)
        with self.publisher.read_revision(self.token,fresh,[0]) as files:
            self.assertEqual(os.read(files[0],100),b'novel 1-150')

    def test_raw_object_reuse_outside_expected_revision_is_rejected(self):
        _,_,asset = self.first_revision()
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,source_form,edition_label,created_at) VALUES('other','w','other','text','born_digital','Other','test')"))
        lease = self.make_job()
        with self.assertRaises(PublicationError):
            self.publisher.register_validated_upload(lease,edition_id='other',expected_revision_id=None,
                assets=[dict(asset,prepared_relpath=None)],options=[option(0)],
                validation_receipt='synthetic-other',rights_status='allowed')

    def test_drop_or_unverified_shrink_of_old_option_is_rejected(self):
        previous,_,asset = self.first_revision()
        lease = self.make_job(previous)
        new_asset = self.asset(lease,b'new incomplete text')
        old_id = self.scalar('SELECT id FROM revision_options')
        for new in [option(0,'epub'),option(0,end=160),dict(option(0,end=100),replaces_option_id=old_id,replacement_receipt='synthetic')]:
            with self.subTest(option=new), self.assertRaises(PublicationError):
                self.plan(lease,[new_asset],[new],previous=previous)
        with self.assertRaises(PublicationError):
            self.plan(lease,[dict(asset,prepared_relpath=None,file_format='jpg')],[option(0)],previous=previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_plans'),1)

    def test_bundle_order_and_missing_positions_do_not_fall_back(self):
        lease = self.make_job()
        assets = [self.asset(lease,b'first'),self.asset(lease,b'second')]
        bundle = dict(option(0),delivery_kind='bundle',positions=[1,0])
        self.plan(lease,assets,[bundle])
        revision = self.publisher.publish(lease)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text('SELECT asset_position FROM option_assets ORDER BY ordinal')).scalars().all(),[1,0])
        with self.publisher.read_revision(self.token,revision,[1,0]) as files:
            self.assertEqual(os.read(files[1],100),b'second')
        with self.assertRaises(PublicationError):
            with self.publisher.read_revision(self.token,revision,[0,99]):
                self.fail('Missing asset silently replaced')

    def test_current_session_rights_and_live_object_required_for_pinned_read(self):
        revision,lease,asset = self.first_revision()
        with self.assertRaises(AuthError):
            with self.publisher.read_revision('invalid',revision,[0]):
                self.fail('Unauthenticated read admitted')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked'"))
        with self.assertRaises(PublicationError):
            with self.publisher.read_revision(self.token,revision,[0]):
                self.fail('Revoked source approval opened a new stream')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed'"))
        def attempt_gc():
            try:
                with StorageLocks(self.root).mutation([asset['sha256']],blocking=False):
                    return 'acquired'
            except StorageBusy:
                return 'busy'
        with self.publisher.read_revision(self.token,revision,[0]) as files:
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(attempt_gc).result(timeout=5),'busy')
            with write_transaction(self.engine) as conn:
                conn.execute(text("DELETE FROM auth_sessions"))
            self.assertEqual(os.read(files[0],100),b'novel 1-150')
        self.assertEqual(attempt_gc(),'acquired')
        with self.assertRaises(AuthError):
            with self.publisher.read_revision(self.token,revision,[0]):
                self.fail('Revoked session opened a new stream')

    def test_reference_truth_and_atomic_live_only_pins(self):
        revision,lease,asset = self.first_revision()
        sha = asset['sha256']
        self.publisher.pin('ticket','synthetic-ticket',[sha])
        with self.engine.connect() as conn:
            refs = self.publisher.reference_sources(conn,sha)
        self.assertIn(('revision',revision),refs)
        self.assertIn(('ticket','synthetic-ticket'),refs)
        self.assertTrue(any(kind == 'job' for kind,_ in refs))
        external = self.jobs.enqueue(kind='external',idempotency_key='external',payload={},object_sha=sha)
        external_lease = self.jobs.claim('external-worker',kind='external')
        hold = self.jobs.acquire_resource(external_lease,'local-qwen')
        self.jobs.cancel(external)
        with self.engine.connect() as conn:
            self.assertIn(('resource','local-qwen'),self.publisher.reference_sources(conn,sha))
        self.jobs.confirm_resource_stopped(hold,disposition='not_started',confirmation='synthetic never dispatched')
        with self.assertRaises(PublicationError):
            self.publisher.pin('backup','atomic',[sha,'f'*64])
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE reference_id='atomic'"),0)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE objects SET state='MISSING'"))
        with self.assertRaises(PublicationError):
            self.publisher.pin('restore','missing',[sha])
        with self.assertRaises(PublicationError):
            with self.publisher.read_revision(self.token,revision,[0]):
                self.fail('Missing object treated as downloadable')

    def test_active_revision_change_and_plan_identity_mutation_are_rejected(self):
        revision,_,asset = self.first_revision()
        lease = self.make_job(revision)
        self.plan(lease,[dict(asset,prepared_relpath=None)],[option(0)],previous=revision)
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE edition_revisions SET is_active=0 WHERE id=:id'),dict(id=revision))
        with self.assertRaises(PublicationError):
            self.publisher.publish(lease)
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE edition_revisions SET is_active=1 WHERE id=:id'),dict(id=revision))
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE publication_plans SET payload_json='{}' WHERE job_id=:id"),dict(id=lease.job_id))

    def test_option_validation_rejects_conflicting_coverage_and_unlisted_assets(self):
        lease = self.make_job()
        asset = self.asset(lease)
        bad = option(0)
        bad['coverage']['gaps'] = [[5,10]]
        for assets,options in [([asset],[bad]),([asset,copy.deepcopy(asset)],[option(0)]),([asset],[dict(option(0),positions=[0,0])])]:
            with self.subTest(options=options), self.assertRaises(PublicationError):
                self.plan(lease,assets,options)


if __name__ == '__main__':
    unittest.main()
