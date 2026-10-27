"""Synthetic G4 authority/publication/retention acceptance; no live provider/model."""
from contextlib import closing
import copy
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from PIL import Image
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.backup import SnapshotService, BackupError, schema_signature, SCHEMA_SIGNATURE
from document_library.database import database_engine, migrate, write_transaction
from document_library.jobs import LostLease
from document_library.library_contracts import register_sqlite_functions, rebuild_search_in, LibraryQuery, canonical_json
from document_library.library_queries import LibraryQueries
from document_library.metadata_contracts import (COVER_SCHEMA, reconcile_metadata_restore_in,
    rebuild_metadata_publication_in, verify_metadata_publication_in)
from document_library.metadata_service import MetadataService, MetadataError
from document_library.orphans import OrphanService
from document_library.publication import PublicationError, object_path
from document_library.restore import RestoreService
from library_helpers import LibraryFixture
from test_library_queries import seed_library

ROOT=Path(__file__).resolve().parents[1]


class MetadataPublicationAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp();self.admin();self.metadata=MetadataService(self.publisher)
        def verified_cleanup():
            self.assertTrue(self.h.doCleanups());self.assertFalse(self.h.path.exists())
        self.addCleanup(verified_cleanup)

    def head(self,work='w'):
        return self.scalar('SELECT version FROM metadata_heads WHERE work_id=:work',work=work)

    def grant(self,kind='description',origin='EDITORIAL',work='w',**changes):
        ident=str(uuid.uuid4());decision=dict(state='allowed',officiality='verified' if origin=='PUBLISHER_OFFICIAL' else 'not_applicable',
            match_state='exact' if origin=='PUBLISHER_OFFICIAL' else 'not_applicable',display_allowed=True,cache_allowed=True,
            derivative_allowed=origin in {'QWEN_TEXT','QWEN_OCR','EMBEDDED','FIRST_PAGE'},cache_until_ms=None,
            retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'license':'synthetic-owned'})
        decision.update(changes);self.metadata.review_rights(self.token,self.csrf,rights_id=ident,work_id=work,kind=kind,expected_version=0,request_id=str(uuid.uuid4()),decision=decision)
        return ident,decision

    def editorial(self,body='unique licensed description'):
        rights,decision=self.grant();head=self.head();request=str(uuid.uuid4())
        receipt=self.metadata.edit_description(self.token,self.csrf,work_id='w',text=body,rights_id=rights,expected_rights_version=1,expected_head_version=head,request_id=request)
        return receipt,rights,decision,head,request

    def enqueue(self,rights,origin,revision=None,work='w'):
        kind=self.scalar('SELECT kind FROM metadata_rights WHERE id=:id',id=rights)
        ident=self.metadata.enqueue(self.token,self.csrf,work_id=work,origin=origin,rights_id=rights,expected_rights_version=1,expected_head_version=self.head(work),source_revision_id=revision,request_id=str(uuid.uuid4()))
        lease=self.h.jobs.claim('metadata-synthetic',kind='metadata.'+kind+'.publish');self.assertEqual(ident,lease.job_id);return lease

    def cover(self,rights=None,*,publish=True):
        rights=rights or self.grant('cover')[0];lease=self.enqueue(rights,'EDITORIAL')
        buffer=io.BytesIO();Image.new('RGB',(3,4),(20,30,40)).save(buffer,format='PNG');payload=buffer.getvalue()
        with self.root.stage(lease) as (path,fd):os.write(fd,payload)
        plan=self.metadata.register_cover(lease,stage_path=path,filename='cover.png',provenance={})
        receipt=self.metadata.publish_cover(lease) if publish else None
        return lease,plan,receipt,path

    def public_text(self):
        return self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'")

    def sql_rejected(self,statement,**params):
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:conn.execute(text(statement),params)

    def test_A01_empty_work_lifecycle_preserves_authority_and_published_evidence(self):
        with write_transaction(self.engine) as conn:
            for work in ('empty','metadata-only'):
                conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES(:id,'unknown',:id,:id,'test','test')"),dict(id=work))
            conn.exec_driver_sql("INSERT OR REPLACE INTO works SELECT * FROM works WHERE id='empty'")
            self.assertEqual(conn.exec_driver_sql("SELECT version,description_id,cover_id FROM metadata_heads WHERE work_id='empty'").one(),(0,None,None))
            conn.exec_driver_sql("DELETE FROM works WHERE id='empty'")
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_heads WHERE work_id='empty'"),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_backup_dirty WHERE work_id='empty'"),0)
        rights,_=self.grant(work='metadata-only')
        for statement in ("DELETE FROM works WHERE id='metadata-only'","INSERT OR REPLACE INTO works SELECT * FROM works WHERE id='metadata-only'"):
            self.sql_rejected(statement)
        self.metadata.edit_description(self.token,self.csrf,work_id='metadata-only',text='protected ledger',rights_id=rights,
            expected_rights_version=1,expected_head_version=self.head('metadata-only'),request_id=str(uuid.uuid4()))
        with self.engine.connect() as conn:
            before={table:conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY 1').fetchall() for table in
                ('metadata_rights','metadata_rights_history','metadata_candidates','metadata_payloads','metadata_candidate_control','metadata_control_events','metadata_heads')}
        for statement in ("DELETE FROM works WHERE id='metadata-only'","INSERT OR REPLACE INTO works SELECT * FROM works WHERE id='metadata-only'"):
            self.sql_rejected(statement)
        with self.engine.connect() as conn:
            for table,rows in before.items():self.assertEqual(conn.exec_driver_sql('SELECT * FROM '+table+' ORDER BY 1').fetchall(),rows)
        self.assertEqual(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='metadata-only'"),'protected ledger')

    def test_A02_editorial_replay_priority_and_private_provenance(self):
        receipt,rights,decision,head,request=self.editorial()
        self.assertEqual(self.public_text(),'unique licensed description')
        self.assertEqual(self.metadata.edit_description(self.token,self.csrf,work_id='w',text='unique licensed description',rights_id=rights,expected_rights_version=1,expected_head_version=head,request_id=request),receipt)
        with self.assertRaises(MetadataError):self.metadata.edit_description(self.token,self.csrf,work_id='w',text='different',rights_id=rights,expected_rights_version=1,expected_head_version=head,request_id=request)
        derived,_=self.grant(origin='PUBLISHER_OFFICIAL');lease=self.enqueue(derived,'PUBLISHER_OFFICIAL')
        other=self.metadata.publish_description(lease,text='lower priority summary',provenance={'model_id':'synthetic-model','prompt_sha256':'a'*64})
        self.assertEqual(other['description_id'],receipt['description_id']);self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),2)
        self.assertEqual(self.public_text(),'unique licensed description')
        self.assertFalse(any('text_value' in row for row in [json.loads(self.scalar('SELECT provenance_json FROM metadata_candidates ORDER BY id LIMIT 1'))]))
        with self.assertRaises(MetadataError):self.metadata.edit_description(self.token,self.csrf,work_id='wrong',text='body',rights_id=rights,expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))

    def test_A03_direct_qwen_publish_cannot_skip_pending_human_review(self):
        revision,root,_=self.first();revision,asset_job,_=self.publish(previous=revision)
        rights,_=self.grant(origin='QWEN_TEXT');lease=self.enqueue(rights,'QWEN_TEXT',revision)
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='searchable dragon summary',provenance={})
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id),0)
        for producer in (root,asset_job):
            with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=producer.job_id))
            self.assertIsNone(self.public_text());self.assertEqual(self.scalar("SELECT count(*) FROM library_search_fts WHERE library_search_fts MATCH 'dragon'"),0)
            with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='allowed' WHERE job_id=:id"),dict(id=producer.job_id))
        with write_transaction(self.engine) as conn:
            upload=conn.execute(text('SELECT upload_session_id FROM jobs WHERE id=:id'),dict(id=root.job_id)).scalar_one()
            conn.execute(text("INSERT INTO ingestion_inputs VALUES(:job,'e',NULL,:upload,NULL,NULL,NULL,'book.txt',100,'synthetic-tus','allowed','synthetic','u','digest')"),dict(job=root.job_id,upload=upload))
            conn.execute(text("UPDATE ingestion_inputs SET rights_status='blocked' WHERE job_id=:id"),dict(id=root.job_id))
        self.assertIsNone(self.public_text())
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id),0)
        self.sql_rejected("UPDATE library_metadata SET description_state='ready',description_text='unproved' WHERE work_id='w'")
        self.sql_rejected("UPDATE library_metadata SET summary_state='ready' WHERE work_id='w'")

    def test_A03_source_input_changes_do_not_create_a_generated_candidate(self):
        revision,producer,_=self.first();rights,_=self.grant(origin='QWEN_TEXT');lease=self.enqueue(rights,'QWEN_TEXT',revision)
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='mutation sentinel description',provenance={})
        upload=self.scalar('SELECT upload_session_id FROM jobs WHERE id=:id',id=producer.job_id)
        for mutation,ready in (("INSERT INTO ingestion_inputs VALUES(:job,'e',NULL,:upload,NULL,NULL,NULL,'book.txt',100,'synthetic-tus','blocked','synthetic','u','digest')",False),
                ("UPDATE ingestion_inputs SET rights_status='allowed' WHERE job_id=:job",True),
                ("INSERT OR REPLACE INTO ingestion_inputs SELECT job_id,edition_id,expected_revision_id,upload_id,observation_id,source_config_version,source_event_epoch,filename,byte_size,tus_id,'blocked',rights_receipt,decision_actor,request_digest FROM ingestion_inputs WHERE job_id=:job",False),
                ("DELETE FROM ingestion_inputs WHERE job_id=:job",True)):
            with self.subTest(mutation=mutation):
                with write_transaction(self.engine) as conn:conn.execute(text(mutation),dict(job=producer.job_id,upload=upload))
                self.assertIsNone(self.public_text())
                self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id),0)
                with self.engine.connect() as conn:SnapshotService._integrity(conn.connection.driver_connection)

    def test_A03_replaced_source_cannot_authorize_direct_generated_publish(self):
        revision,_,_=self.first();seed_library(self.engine,1,ratings=False)
        with write_transaction(self.engine) as conn:
            conn.exec_driver_sql("UPDATE work_editions SET work_id='w',edition_key='review-secondary' WHERE id='e000000-text'")
        rights,_=self.grant(origin='QWEN_TEXT');lease=self.enqueue(rights,'QWEN_TEXT',revision)
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='sourcebinding sentinel description',provenance={})
        with write_transaction(self.engine) as conn:
            conn.exec_driver_sql("INSERT OR REPLACE INTO work_editions SELECT id,'w000000',edition_key,representation,source_form,edition_label,language,source_series,created_at FROM work_editions WHERE id='e'")
        self.assertIsNone(self.public_text())
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_eligible_candidates_v1'),0)
        with self.engine.connect() as conn:SnapshotService._integrity(conn.connection.driver_connection)

    def test_A04_direct_generated_publish_never_creates_historical_approval(self):
        revision,_,_=self.first();rights,_=self.grant(origin='QWEN_TEXT');lease=self.enqueue(rights,'QWEN_TEXT',revision)
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='approved old result',provenance={})
        pending=self.enqueue(rights,'QWEN_TEXT',revision);new,_,_=self.publish(previous=revision)
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(pending,text='late old input',provenance={})
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),0)
        self.assertIsNone(self.public_text())
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE objects SET state='PURGED' WHERE sha256 IN (SELECT object_sha FROM revision_assets WHERE edition_revision_id=:id)"),dict(id=revision))
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),0)

    def test_A05_real_raster_typed_install_and_prepared_copy(self):
        lease,plan,receipt,path=self.cover(publish=False)
        with self.root.file(path) as fd:
            fd2=os.open(self.root.path/path,os.O_WRONLY);os.write(fd2,b'changed');os.close(fd2)
        result=self.metadata.publish_cover(lease);self.assertEqual(result['cover_id'],plan['candidate_id'])
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'),0);self.assertEqual(self.scalar('SELECT count(*) FROM revision_seals'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'),0);self.assertEqual(self.scalar("SELECT state FROM storage_intents"),'committed')
        self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'completed')
        with self.root.file(object_path(plan['asset']['sha256'])) as fd:self.assertEqual(hashlib.sha256(os.read(fd,8192)).hexdigest(),plan['asset']['sha256'])
        self.assertEqual(self.scalar("SELECT cover_state FROM metadata_expected_heads_v1 WHERE work_id='w'"),'ready')
        self.assertEqual(self.scalar("SELECT cover_state FROM library_metadata WHERE work_id='w'"),'pending')

    def test_A05_invalid_raster_stage_boundary_and_repeat_registration(self):
        rights,_=self.grant('cover');lease=self.enqueue(rights,'EDITORIAL')
        with self.root.stage(lease) as (path,fd):os.write(fd,b'<svg onload="bad"/>')
        for filename in ('cover.svg','cover.png'):
            with self.assertRaises((MetadataError,ValueError)):self.metadata.register_cover(lease,stage_path=path,filename=filename,provenance={})
        with self.assertRaises(MetadataError),patch('document_library.metadata_service.inspect_fd',side_effect=AssertionError('unowned read')):
            self.metadata.register_cover(lease,stage_path='database/test.sqlite3',filename='cover.png',provenance={})
        lease,plan,_,path=self.cover(publish=False)
        with patch.object(self.publisher,'prepare',side_effect=AssertionError('duplicate prepared copy')):
            self.assertEqual(self.metadata.register_cover(lease,stage_path=path,filename='cover.png',provenance={}),plan)
        with self.assertRaises(MetadataError):
            self.metadata.register_cover(lease,stage_path=path,filename='cover.png',provenance={'model_id':'conflicting'})
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_plans WHERE job_id=:id',id=lease.job_id),1)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE metadata_rights SET version=version+1,request_id=:request WHERE id=:id"),dict(request=str(uuid.uuid4()),id=plan['rights_id']))
        with self.assertRaises(MetadataError),patch('document_library.metadata_service.inspect_fd',side_effect=AssertionError('revoked input inspected')):
            self.metadata.register_cover(lease,stage_path=path,filename='cover.png',provenance={})

    def test_A06_metadata_plan_never_approves_originals_and_input_is_durable(self):
        lease,plan,_,_=self.cover(publish=False)
        with self.engine.connect() as conn:
            with self.assertRaises(PublicationError):self.publisher._plan(conn,lease)
        with self.assertRaises(PublicationError):self.publisher.publish(lease)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO edition_revisions VALUES('fake','e',1,1,'test')"))
            asset=plan['asset'];conn.execute(text("INSERT INTO objects(sha256,byte_size,detected_mime,local_relpath,created_at) VALUES(:sha,:size,:mime,:path,'test')"),dict(sha=asset['sha256'],size=asset['byte_size'],mime=asset['detected_mime'],path=object_path(asset['sha256'])))
            conn.execute(text("INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format,asset_role) VALUES('fake',0,:sha,'png','content')"),dict(sha=asset['sha256']))
        self.sql_rejected("INSERT INTO revision_seals VALUES('fake',:id,:g,1,1)",id=lease.job_id,g=lease.generation)
        self.sql_rejected("INSERT INTO revision_asset_sources VALUES('fake',0,:id,:g)",id=lease.job_id,g=lease.generation)
        self.sql_rejected('DELETE FROM metadata_job_inputs WHERE job_id=:id',id=lease.job_id)
        self.sql_rejected("UPDATE metadata_job_inputs SET actor_authz_version=actor_authz_version+1 WHERE job_id=:id",id=lease.job_id)
        self.sql_rejected('INSERT OR REPLACE INTO metadata_job_inputs SELECT * FROM metadata_job_inputs WHERE job_id=:id',id=lease.job_id)
        self.sql_rejected("INSERT INTO ingestion_inputs VALUES(:id,'e',NULL,NULL,NULL,NULL,NULL,'bad',1,NULL,'allowed','synthetic','u','digest')",id=lease.job_id)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_job_inputs WHERE job_id=:id',id=lease.job_id),1)

    def test_A06_jobs_fk_requires_exact_running_metadata_input_and_rejects_replacement(self):
        revision,_,_=self.first();rights,_=self.grant();lease=self.enqueue(rights,'EDITORIAL')
        with self.engine.connect() as conn:source=dict(conn.execute(text('SELECT * FROM metadata_job_inputs WHERE job_id=:id'),dict(id=lease.job_id)).mappings().one())
        params=dict(id='fake-candidate',work='w',rights=rights,job=lease.job_id,g=lease.generation,sha='a'*64)
        insert="INSERT INTO metadata_candidates VALUES(:id,:work,'description','EDITORIAL',:rights,1,NULL,NULL,NULL,:sha,'{}',:job,:g,1)"
        for change in ({'g':lease.generation+1},{'job':self.h.jobs.enqueue(kind='ingestion_event',idempotency_key=str(uuid.uuid4()),payload={},work_id='w')},{'work':'wrong'}):
            with self.subTest(change=change):self.sql_rejected(insert,**(params|change))
        missing=self.h.jobs.enqueue(kind='metadata.description.publish',idempotency_key=str(uuid.uuid4()),payload={},work_id='w');missing_lease=self.h.jobs.claim('missing-input',kind='metadata.description.publish');self.assertEqual(missing_lease.job_id,missing)
        self.sql_rejected(insert,**(params|{'job':missing,'g':missing_lease.generation}))
        self.sql_rejected(insert.replace('1,NULL,NULL,NULL,:sha','1,:revision,NULL,NULL,:sha'),**params,revision=revision)
        self.h.jobs.cancel(lease.job_id);self.sql_rejected(insert,**params)
        lease=self.enqueue(rights,'EDITORIAL');receipt=self.metadata.publish_description(lease,text='durable result',provenance={});candidate=receipt['description_id']
        for statement in ("DELETE FROM metadata_candidates WHERE id=:id","UPDATE metadata_candidates SET content_sha256=:sha WHERE id=:id","INSERT OR REPLACE INTO metadata_candidates SELECT * FROM metadata_candidates WHERE id=:id"):
            self.sql_rejected(statement,id=candidate,sha='b'*64)
        self.sql_rejected('DELETE FROM jobs WHERE id=:id',id=lease.job_id)
        self.sql_rejected('INSERT OR REPLACE INTO jobs SELECT * FROM jobs WHERE id=:id',id=lease.job_id)

    def test_A07_finalizer_failure_rolls_back_all_effects_and_actor_change(self):
        lease,plan,_,_=self.cover(publish=False);before=self.head()
        with patch.object(self.metadata,'_finalize_cover_in',side_effect=MetadataError('injected final authority drift')):
            with self.assertRaises(MetadataError):self.metadata.publish_cover(lease)
        self.assertEqual(self.head(),before);self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'),0);self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),0)
        self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'running')
        self.assertTrue((self.root.path/object_path(plan['asset']['sha256'])).exists())
        result=self.metadata.publish_cover(lease);self.assertEqual(result['cover_id'],plan['candidate_id'])
        rights,_=self.grant();lease=self.enqueue(rights,'EDITORIAL')
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE users SET role='member' WHERE id='u'"))
        with self.assertRaises(MetadataError):self.metadata.publish_description(lease,text='unauthorized',provenance={})

    def test_A08_orphan_requires_actual_stop_and_protects_registered_shared_bytes(self):
        lease,plan,_,_=self.cover(publish=False)
        with patch.object(self.metadata,'_commit_cover_in',side_effect=RuntimeError('after rename')):
            with self.assertRaises(RuntimeError):self.metadata.publish_cover(lease)
        intent=self.scalar('SELECT id FROM storage_intents');orphans=OrphanService(self.publisher);self.h.jobs.cancel(lease.job_id)
        with self.assertRaises(PublicationError):orphans.retire(intent)
        self.assertTrue(orphans.confirm_stopped(lease,disposition='cancelled',confirmation='Own synthetic synchronous call returned; no external worker'))
        orphans.retire(intent);self.assertNotEqual(self.scalar('SELECT state FROM installation_retirements'),'pending')
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),0)

    def test_A09_retained_cover_reference_backup_and_public_release_guard(self):
        lease,plan,receipt,_=self.cover();sha=plan['asset']['sha256']
        with self.engine.connect() as conn:self.assertIn(('metadata_cover',receipt['cover_id']),self.publisher.reference_sources(conn,sha))
        with self.assertRaises(MetadataError):self.metadata.release_cover(self.token,self.csrf,candidate_id=receipt['cover_id'],expected_control_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()),reason='manual')
        self.sql_rejected('UPDATE metadata_candidate_control SET retained=0,version=version+1 WHERE candidate_id=:id',id=receipt['cover_id'])
        backup=SnapshotService(self.publisher);job=self.h.jobs.enqueue(kind='backup_catalog',idempotency_key=str(uuid.uuid4()),payload={});lease=self.h.jobs.claim('metadata-backup',kind='backup_catalog',lease_ms=300000)
        manifest=backup.capture(lease);self.assertIn(sha,[obj['sha256'] for obj in manifest['objects']])

    def test_A10_conflicting_future_restore_event_cannot_skip_real_floor(self):
        rights,_=self.grant(origin='PUBLISHER_OFFICIAL');lease=self.enqueue(rights,'PUBLISHER_OFFICIAL')
        candidate=self.metadata.publish_description(lease,text='official replay sentinel',provenance={})['description_id']
        snapshot='a'*64;request=str(uuid.uuid5(uuid.NAMESPACE_URL,'restore-metadata:synthetic-conflict:'+snapshot+':'+candidate))
        with write_transaction(self.engine) as conn:
            before=dict(conn.execute(text('SELECT * FROM metadata_candidate_control WHERE candidate_id=:id'),dict(id=candidate)).mappings().one())
            after=before|dict(version=2,actor_kind='system_restore',actor_id=None,source_snapshot_sha256=snapshot,request_id=request,updated_at_ms=12345,review_status='pending')
            conn.execute(text("INSERT INTO metadata_control_events VALUES(:id,:candidate,:request,'system_restore',1,2,:before,:after,'system_restore',NULL,:sha,12345)"),dict(id=candidate+':2',candidate=candidate,request=request,before=canonical_json(before),after=canonical_json(after),sha=snapshot))
        with self.assertRaises(ValueError):
            with write_transaction(self.engine) as conn:
                reconcile_metadata_restore_in(conn.connection.driver_connection,snapshot_set_id='synthetic-conflict',snapshot_sha256=snapshot,observed_at_ms=12345)
        with self.engine.connect() as conn:
            self.assertEqual(dict(conn.execute(text('SELECT * FROM metadata_candidate_control WHERE candidate_id=:id'),dict(id=candidate)).mappings().one()),before)
        self.assertEqual(self.public_text(),'official replay sentinel')

    def test_A10_restore_official_floor_event_replay_and_historical_author(self):
        self.editorial('persistent editorial')
        with write_transaction(self.engine) as conn:conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('official-work','novel','Official','official','test','test')"))
        rights,decision=self.grant(origin='PUBLISHER_OFFICIAL',work='official-work');lease=self.enqueue(rights,'PUBLISHER_OFFICIAL',work='official-work')
        official=self.metadata.publish_description(lease,text='official history',provenance={'source_url':'https://synthetic.invalid/book'})
        candidate=self.scalar("SELECT id FROM metadata_candidates WHERE origin='PUBLISHER_OFFICIAL'")
        with closing(sqlite3.connect(self.engine.url.database,autocommit=True)) as db:
            register_sqlite_functions(db);db.execute('PRAGMA foreign_keys=ON');history=db.execute('SELECT * FROM metadata_rights_history').fetchall()
            db.execute("CREATE TRIGGER injected_restore_event BEFORE INSERT ON metadata_control_events WHEN NEW.actor_kind='system_restore' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END")
            before_control=db.execute('SELECT version,minimum_rights_version FROM metadata_candidate_control WHERE candidate_id=?',(candidate,)).fetchone()
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.IntegrityError):reconcile_metadata_restore_in(db,snapshot_set_id='synthetic-set',snapshot_sha256='a'*64,observed_at_ms=12345)
            db.execute('ROLLBACK');self.assertEqual(db.execute('SELECT version,minimum_rights_version FROM metadata_candidate_control WHERE candidate_id=?',(candidate,)).fetchone(),before_control)
            db.execute('DROP TRIGGER injected_restore_event')
            db.execute('BEGIN IMMEDIATE');result=reconcile_metadata_restore_in(db,snapshot_set_id='synthetic-set',snapshot_sha256='a'*64,observed_at_ms=12345);rebuild_metadata_publication_in(db);db.execute('COMMIT')
            row=db.execute('SELECT version,minimum_rights_version,actor_kind,actor_id,source_snapshot_sha256 FROM metadata_candidate_control WHERE candidate_id=?',(candidate,)).fetchone();self.assertEqual(row,(2,2,'system_restore',None,'a'*64))
            self.assertEqual(db.execute("SELECT description_state,description_text FROM library_metadata WHERE work_id='official-work'").fetchone(),('pending',None))
            self.assertEqual(result['metadata_official_revalidated'],1);self.assertEqual(db.execute('SELECT * FROM metadata_rights_history').fetchall(),history)
            db.execute('BEGIN IMMEDIATE');self.assertEqual(reconcile_metadata_restore_in(db,snapshot_set_id='synthetic-set',snapshot_sha256='a'*64,observed_at_ms=99999)['metadata_official_revalidated'],0);db.execute('COMMIT')
            self.metadata.review_rights(self.token,self.csrf,rights_id=rights,work_id='official-work',kind='description',expected_version=1,request_id=str(uuid.uuid4()),decision=decision)
            self.metadata.review_candidate(self.token,self.csrf,candidate_id=candidate,expected_control_version=2,expected_head_version=self.head('official-work'),request_id=str(uuid.uuid4()),review_status='approved',select=True)
            newer=db.execute('SELECT * FROM metadata_candidate_control WHERE candidate_id=?',(candidate,)).fetchone()
            self.assertEqual(db.execute("SELECT description_state,description_text FROM library_metadata WHERE work_id='official-work'").fetchone(),('ready','official history'))
            db.execute('BEGIN IMMEDIATE');self.assertEqual(reconcile_metadata_restore_in(db,snapshot_set_id='synthetic-set',snapshot_sha256='a'*64,observed_at_ms=999999)['metadata_official_revalidated'],0);db.execute('COMMIT')
            self.assertEqual(db.execute('SELECT * FROM metadata_candidate_control WHERE candidate_id=?',(candidate,)).fetchone(),newer)
            db.execute("UPDATE users SET status='suspended'");verify_metadata_publication_in(db);self.assertEqual(db.execute("SELECT description_text FROM library_metadata WHERE work_id='w'").fetchone(),('persistent editorial',))
            self.assertEqual(db.execute("SELECT count(*) FROM metadata_control_events WHERE actor_kind='system_restore'").fetchone(),(1,))
        self.sql_rejected("UPDATE metadata_control_events SET action='fake'")

    def test_A12_cache_revocation_deletes_text_but_keeps_hash_and_audit(self):
        receipt,rights,decision,_,_=self.editorial();candidate=receipt['description_id']
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE metadata_rights SET version=version+1,display_allowed=0,request_id=:id WHERE id=:rights"),dict(id=str(uuid.uuid4()),rights=rights))
        self.assertIsNone(self.public_text());self.assertEqual(self.scalar('SELECT count(*) FROM metadata_payloads'),1)
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE metadata_rights SET version=version+1,cache_allowed=0,request_id=:id WHERE id=:rights"),dict(id=str(uuid.uuid4()),rights=rights))
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_payloads'),0);self.assertEqual(self.scalar('SELECT count(*) FROM metadata_retention_tombstones'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),1);self.assertGreater(self.scalar('SELECT count(*) FROM metadata_control_events'),0)
        self.sql_rejected("INSERT INTO metadata_payloads VALUES(:id,'unique licensed description',:sha)",id=candidate,sha=hashlib.sha256(b'unique licensed description').hexdigest())

    def test_A12_selected_cover_policy_clear_then_release_is_atomic(self):
        lease,plan,receipt,_=self.cover();candidate=receipt['cover_id'];rights=plan['rights_id']
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE metadata_rights SET version=version+1,cache_allowed=0,request_id=:id WHERE id=:rights"),dict(id=str(uuid.uuid4()),rights=rights))
        self.assertEqual(self.scalar("SELECT cover_state FROM metadata_expected_heads_v1 WHERE work_id='w'"),'pending');self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control'),1)
        with self.engine.connect() as c:
            db=c.connection.driver_connection
            with self.assertRaises(BackupError):SnapshotService._inventory(db)
        before_head=self.head();before_sequence=self.scalar('SELECT value FROM storage_sequence')
        with write_transaction(self.engine) as conn:conn.exec_driver_sql("CREATE TRIGGER injected_retention_event BEFORE INSERT ON metadata_control_events WHEN NEW.actor_kind='system_retention' BEGIN SELECT RAISE(ABORT,'injected audit failure'); END")
        with self.assertRaises(IntegrityError):self.metadata.reconcile_retention()
        self.assertEqual(self.head(),before_head);self.assertEqual(self.scalar('SELECT value FROM storage_sequence'),before_sequence)
        self.assertEqual(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"),candidate)
        with write_transaction(self.engine) as conn:conn.exec_driver_sql('DROP TRIGGER injected_retention_event')
        result=self.metadata.reconcile_retention();self.assertEqual(result['released'],1)
        self.assertIsNone(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"));self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_control_events WHERE actor_kind='system_retention'"),1)
        self.assertTrue((self.root.path/object_path(plan['asset']['sha256'])).exists())


    def test_A07_head_rights_old_lease_and_followup_rollback(self):
        for failure in ('head','rights','lease','followup'):
            with self.subTest(failure=failure):
                rights,_=self.grant();lease=self.enqueue(rights,'EDITORIAL');before=self.scalar('SELECT count(*) FROM metadata_candidates')
                if failure=='head':
                    with write_transaction(self.engine) as conn:conn.execute(text("UPDATE metadata_heads SET version=version+1 WHERE work_id='w'"))
                elif failure=='rights':
                    with write_transaction(self.engine) as conn:conn.execute(text('UPDATE metadata_rights SET version=version+1,request_id=:request WHERE id=:id'),dict(request=str(uuid.uuid4()),id=rights))
                elif failure=='lease':self.h.clock.advance(31000)
                original=self.h.jobs.enqueue_in;calls=[]
                def followup(conn,**values):
                    calls.append(values)
                    if failure=='followup' and len(calls)>1:raise RuntimeError('injected followup failure')
                    return original(conn,**values)
                with patch.object(self.h.jobs,'enqueue_in',side_effect=followup):
                    with self.assertRaises((MetadataError,LostLease,RuntimeError)):self.metadata.publish_description(lease,text='must roll back',provenance={})
                self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),before)
                self.assertEqual(self.h.jobs.get(lease.job_id)['state'],'running')
                self.h.jobs.cancel(lease.job_id)

    def test_A09_same_sha_release_preserves_other_independent_reference(self):
        _,first,one,_=self.cover();_,second,two,_=self.cover();self.assertEqual(first['asset']['sha256'],second['asset']['sha256'])
        self.metadata.release_cover(self.token,self.csrf,candidate_id=one['cover_id'],expected_control_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()),reason='replace selection')
        with self.engine.connect() as conn:
            refs=self.publisher.reference_sources(conn,first['asset']['sha256']);self.assertIn(('metadata_cover',two['cover_id']),refs);self.assertNotIn(('metadata_cover',one['cover_id']),refs)
        self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control WHERE candidate_id=:id',id=two['cover_id']),1)

    def test_A10_restore_event_failure_and_suspended_description_author(self):
        rights,_=self.grant(origin='EDITORIAL');lease=self.enqueue(rights,'EDITORIAL')
        self.metadata.publish_description(lease,text='retained generated result',provenance={})
        with closing(sqlite3.connect(self.engine.url.database,autocommit=True)) as db:
            register_sqlite_functions(db);db.execute("UPDATE users SET status='suspended'");rebuild_metadata_publication_in(db)
            self.assertEqual(db.execute("SELECT description_text FROM library_metadata WHERE work_id='w'").fetchone(),('retained generated result',));verify_metadata_publication_in(db)

    def test_A12_inventory_rejects_new_forbidden_cover_without_tombstone(self):
        _,plan,_,_=self.cover();sha=plan['asset']['sha256']
        with write_transaction(self.engine) as conn:conn.execute(text('UPDATE metadata_rights SET version=version+1,cache_allowed=0,request_id=:request WHERE id=:id'),dict(request=str(uuid.uuid4()),id=plan['rights_id']))
        self.metadata.reconcile_retention()
        for state,cache,deadline in (('blocked',True,None),('blocked',True,self.h.clock.wall+100000),('allowed',False,None),('allowed',False,self.h.clock.wall+100000)):
            with self.subTest(state=state,cache=cache,deadline=deadline):
                rights,_=self.grant('cover',state=state,cache_allowed=cache,display_allowed=False,cache_until_ms=deadline)
                with self.assertRaises(MetadataError):self.enqueue(rights,'EDITORIAL')
                candidate='synthetic-forbidden-'+uuid.uuid4().hex
                with write_transaction(self.engine) as conn:
                    conn.execute(text("INSERT INTO metadata_candidates VALUES(:id,'w','cover','EDITORIAL',:rights,1,NULL,:sha,NULL,:sha,'{}',NULL,NULL,1)"),dict(id=candidate,rights=rights,sha=sha))
                    conn.execute(text("INSERT INTO metadata_candidate_control VALUES(:id,1,'pending',1,1,'admin','u',:request,1,NULL,'{}')"),dict(id=candidate,request=str(uuid.uuid4())))
                self.assertEqual(self.scalar('SELECT count(*) FROM metadata_retention_tombstones WHERE candidate_id=:id',id=candidate),0)
                with self.engine.connect() as conn:
                    SnapshotService._integrity(conn.connection.driver_connection)
                    with self.assertRaisesRegex(BackupError,'RETENTION_RECONCILIATION_REQUIRED'):
                        SnapshotService._inventory(conn.connection.driver_connection,snapshot_at_ms=self.h.clock.wall)
                self.metadata.reconcile_retention()
                self.assertEqual(self.scalar('SELECT count(*) FROM metadata_retention_tombstones WHERE candidate_id=:id',id=candidate),1)
                self.assertEqual(self.scalar('SELECT reason FROM metadata_retention_tombstones WHERE candidate_id=:id',id=candidate),'cache_revoked')
                with self.engine.connect() as conn:
                    objects,_=SnapshotService._inventory(conn.connection.driver_connection,snapshot_at_ms=self.h.clock.wall)
                    self.assertNotIn(sha,[obj['sha256'] for obj in objects])

    def test_A12_snapshot_capture_rejects_deadline_crossed_after_last_writer(self):
        deadline=self.h.clock.wall+1000;rights,_=self.grant(cache_until_ms=deadline)
        self.metadata.edit_description(self.token,self.csrf,work_id='w',text='expiry snapshot sentinel',rights_id=rights,expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))
        backup=SnapshotService(self.publisher);lease=self.h.jobs.claim('synthetic-expiry-backup',kind='backup_catalog',lease_ms=300000)
        original=backup._memory_snapshot
        def cross_deadline_after_last_writer():
            self.h.clock.advance(2000);return original()
        with patch.object(backup,'_memory_snapshot',side_effect=cross_deadline_after_last_writer):
            with self.assertRaisesRegex(BackupError,'RETENTION_RECONCILIATION_REQUIRED'):backup.capture(lease)
        paths=backup.paths(backup.set_id(lease))
        self.assertFalse((self.root.path/paths['database']).exists());self.assertFalse((self.root.path/paths['manifest']).exists())
        self.assertGreaterEqual(self.h.clock.wall,deadline)

    def test_A12_snapshot_validation_keeps_its_fixed_permitted_observation_time(self):
        deadline=self.h.clock.wall+1000;rights,_=self.grant(cache_until_ms=deadline)
        self.metadata.edit_description(self.token,self.csrf,work_id='w',text='permitted historical snapshot',rights_id=rights,expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))
        backup=SnapshotService(self.publisher);lease=self.h.jobs.claim('synthetic-fixed-backup',kind='backup_catalog',lease_ms=300000)
        manifest=backup.capture(lease);self.assertLess(manifest['snapshot_at_ms'],deadline)
        self.h.clock.advance(2000);self.assertGreater(self.h.clock.wall,deadline)
        self.assertEqual(backup.verify_local(manifest['set_id']),manifest)
        with closing(sqlite3.connect(self.root.path/backup.paths(manifest['set_id'])['database'],autocommit=True)) as db:
            with self.assertRaisesRegex(BackupError,'RETENTION_RECONCILIATION_REQUIRED'):
                SnapshotService.validate_database(db,manifest|dict(snapshot_at_ms=deadline))
            self.assertEqual(db.execute('SELECT text_value FROM metadata_payloads').fetchone(),('permitted historical snapshot',))

    def _retention_busy_prefix(self, hold_kind):
        ids=iter(sorted([uuid.UUID(int=1),uuid.UUID(int=2)],key=lambda value:str(uuid.uuid5(value,'metadata:1'))))
        original=self.h.jobs.enqueue_in
        def fixed_cover_job(conn,**values):
            if values['kind']=='metadata.cover.publish':
                with patch('document_library.jobs.uuid.uuid4',return_value=next(ids)):return original(conn,**values)
            return original(conn,**values)
        with patch.object(self.h.jobs,'enqueue_in',side_effect=fixed_cover_job):
            lease_a,plan_a,_,_=self.cover(publish=False)
            old_deadline=self.scalar('SELECT lease_until FROM jobs WHERE id=:id',id=lease_a.job_id)
            hold=self.h.jobs.acquire_resource(lease_a,'local-qwen') if hold_kind=='resource' else None
            self.metadata.publish_cover(lease_a)
            if hold_kind=='prepared_intent':
                with write_transaction(self.engine) as conn:conn.execute(text("UPDATE storage_intents SET state='prepared' WHERE job_id=:id AND generation=:generation"),dict(id=lease_a.job_id,generation=lease_a.generation))
            with write_transaction(self.engine) as conn:conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('other','unknown','Other','other','test','test')"))
            rights,_=self.grant('cover',work='other');lease_b=self.enqueue(rights,'EDITORIAL',work='other')
            with self.root.file(object_path(plan_a['asset']['sha256'])) as fd:payload=os.read(fd,8192)
            with self.root.stage(lease_b) as (stage,fd):os.write(fd,payload)
            plan_b=self.metadata.register_cover(lease_b,stage_path=stage,filename='cover.png',provenance={});self.metadata.publish_cover(lease_b)
        self.assertLess(plan_a['candidate_id'],plan_b['candidate_id'])
        with write_transaction(self.engine) as conn:
            for rights in (plan_a['rights_id'],plan_b['rights_id']):
                conn.execute(text('UPDATE metadata_rights SET version=version+1,cache_allowed=0,request_id=:request WHERE id=:rights'),dict(request=str(uuid.uuid4()),rights=rights))
        self.h.clock.advance(31000);frozen=self.h.clock.wall;self.assertGreater(frozen,old_deadline)
        passes=[self.metadata.reconcile_retention(limit=1) for _ in range(3)]
        self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control WHERE candidate_id=:id',id=plan_a['candidate_id']),1)
        self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control WHERE candidate_id=:id',id=plan_b['candidate_id']),0)
        self.assertEqual([item['local_object_pending'] for item in passes],[1,1,1])
        self.assertIsNone(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"))
        self.assertEqual(self.h.clock.wall,frozen)
        if hold:
            self.assertEqual(self.scalar('SELECT count(*) FROM resource_slots WHERE job_id=:id AND generation=:generation',id=lease_a.job_id,generation=lease_a.generation),1)
            self.h.jobs.confirm_resource_stopped(hold,disposition='not_started',confirmation='Synthetic hold only; no external model was started')
        else:
            # Reconcile the injected prepared-state fault to the observed own
            # completed synchronous install; a deadline is never stop evidence.
            self.assertEqual(self.h.jobs.get(lease_a.job_id)['state'],'completed')
            self.assertTrue((self.root.path/object_path(plan_a['asset']['sha256'])).exists())
            with write_transaction(self.engine) as conn:conn.execute(text("UPDATE storage_intents SET state='committed' WHERE job_id=:id AND generation=:generation"),dict(id=lease_a.job_id,generation=lease_a.generation))
        final=self.metadata.reconcile_retention(limit=1)
        self.assertEqual(final['released'],1);self.assertEqual(final['local_object_pending'],0)
        self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control WHERE candidate_id=:id',id=plan_a['candidate_id']),0)
        self.assertEqual(self.h.clock.wall,frozen)

    def test_A12_resource_busy_prefix_does_not_starve_idle_cover(self):
        self._retention_busy_prefix('resource')

    def test_A12_prepared_intent_busy_prefix_does_not_starve_idle_cover(self):
        self._retention_busy_prefix('prepared_intent')

    def test_A12_unconfirmed_use_keeps_retention_after_clear_and_expiry_is_bounded(self):
        lease,plan,_,_=self.cover(publish=False);hold=self.h.jobs.acquire_resource(lease,'local-qwen');self.metadata.publish_cover(lease)
        with write_transaction(self.engine) as conn:conn.execute(text('UPDATE metadata_rights SET version=version+1,cache_allowed=0,request_id=:request WHERE id=:id'),dict(request=str(uuid.uuid4()),id=plan['rights_id']))
        result=self.metadata.reconcile_retention();self.assertEqual(result['local_object_pending'],1);self.assertEqual(result['released'],0)
        self.assertIsNone(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"));self.assertEqual(self.scalar('SELECT retained FROM metadata_candidate_control'),1);head=self.head()
        self.h.jobs.confirm_resource_stopped(hold,disposition='not_started',confirmation='Synthetic hold only; no external process/request was started')
        result=self.metadata.reconcile_retention();self.assertEqual(result['released'],1);self.assertEqual(self.head(),head)
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_control_events WHERE actor_kind='system_retention'"),2)
        rights,_=self.grant(cache_until_ms=self.h.clock.wall+1000);self.metadata.edit_description(self.token,self.csrf,work_id='w',text='expiring allowed payload',rights_id=rights,expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))
        self.h.clock.advance(2000);result=self.metadata.reconcile_retention(limit=1);self.assertEqual(result['local_payload_removed'],1);self.assertIsNone(self.public_text());self.assertGreaterEqual(result['backup_obligation_pending'],2)

    def test_A13_revoked_head_does_not_block_eligible_candidate_selection(self):
        receipt,rights,decision,head,request=self.editorial()
        self.assertEqual(self.public_text(),'unique licensed description')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE metadata_rights SET version=version+1,display_allowed=0,request_id=:request WHERE id=:id"),dict(request=str(uuid.uuid4()),id=rights))
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_eligible_candidates_v1 WHERE id=:id",id=receipt['description_id']),0)
        derived,_=self.grant(origin='EDITORIAL');lease=self.enqueue(derived,'EDITORIAL')
        self.metadata.publish_description(lease,text='eligible fallback body',provenance={})
        self.assertEqual(self.public_text(),'eligible fallback body')
        self.assertNotEqual(self.scalar('SELECT description_id FROM metadata_heads WHERE work_id="w"'),receipt['description_id'])

    def test_A14_expired_description_is_fail_closed_at_read_without_reconciliation(self):
        revision,_,_=self.first();self.publish(previous=revision)
        deadline=self.h.clock.wall+1000;rights,_=self.grant(cache_until_ms=deadline)
        self.metadata.edit_description(self.token,self.csrf,work_id='w',text='expiring readable description',rights_id=rights,expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))
        queries=LibraryQueries(self.auth,now_ms=lambda:self.h.clock.wall)
        self.assertEqual([w.description_state for w in (c.work for c in queries.list(self.token,LibraryQuery(limit=10)).items) if w.work_id=='w'],['ready'])
        self.assertEqual([c.work.work_id for c in queries.list(self.token,LibraryQuery(search='expiring',limit=10)).items],['w'])
        detail=queries.detail(self.token,'w');self.assertEqual(detail['description']['state'],'ready')
        self.assertEqual(detail['description']['text'],'expiring readable description');self.assertEqual(detail['description']['source'],'EDITORIAL')
        self.h.clock.advance(2000);self.assertGreater(self.h.clock.wall,deadline)
        self.assertEqual([w.description_state for w in (c.work for c in queries.list(self.token,LibraryQuery(limit=10)).items) if w.work_id=='w'],['pending'])
        self.assertEqual([c.work.work_id for c in queries.list(self.token,LibraryQuery(search='expiring',limit=10)).items],[])
        detail=queries.detail(self.token,'w');self.assertEqual(detail['description']['state'],'pending');self.assertIsNone(detail['description']['text'])
        self.assertEqual(self.scalar('SELECT description_state FROM library_metadata WHERE work_id="w"'),'ready')

    def test_A15_metadata_review_queue_and_admin_review(self):
        empty=self.metadata.review_queue(self.token)
        self.assertEqual(empty['candidates'],[]);self.assertEqual(empty['rights'],[])
        rights_id=str(uuid.uuid4())
        self.metadata.review_rights(self.token,self.csrf,rights_id=rights_id,work_id='w',kind='description',expected_version=0,request_id=str(uuid.uuid4()),
            decision=dict(state='review',officiality='not_applicable',match_state='not_applicable',display_allowed=False,cache_allowed=False,derivative_allowed=False,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'note':'synthetic review'}))
        queue=self.metadata.review_queue(self.token)
        self.assertEqual([r['rights_id'] for r in queue['rights']],[rights_id]);self.assertEqual(queue['rights'][0]['state'],'review')
        receipt,*_=self.editorial();candidate=receipt['description_id']
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE metadata_candidate_control SET version=version+1,review_status='pending',actor_kind='admin',actor_id='u',source_snapshot_sha256=NULL,request_id=:req,updated_at_ms=:now,audit_context_json='{}' WHERE candidate_id=:id"),dict(id=candidate,req=str(uuid.uuid4()),now=self.h.clock.wall))
        queue=self.metadata.review_queue(self.token)
        self.assertEqual([c['candidate_id'] for c in queue['candidates']],[candidate]);self.assertEqual(queue['candidates'][0]['origin'],'EDITORIAL');self.assertTrue(queue['candidates'][0]['text_preview'])
        control=self.scalar('SELECT version FROM metadata_candidate_control WHERE candidate_id=:id',id=candidate)
        self.metadata.review_candidate(self.token,self.csrf,candidate_id=candidate,expected_control_version=control,expected_head_version=self.head(),request_id=str(uuid.uuid4()),review_status='approved',select=False)
        self.assertEqual(self.metadata.review_queue(self.token)['candidates'],[])
        self.metadata.review_rights(self.token,self.csrf,rights_id=rights_id,work_id='w',kind='description',expected_version=1,request_id=str(uuid.uuid4()),
            decision=dict(state='allowed',officiality='not_applicable',match_state='not_applicable',display_allowed=True,cache_allowed=True,derivative_allowed=False,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'note':'synthetic allowed'}))
        self.assertEqual(self.metadata.review_queue(self.token)['rights'],[])

    def test_A16_per_kind_priority_cover_and_description_are_independent(self):
        receipt,_,_,_,_=self.editorial('controlled description')
        self.assertEqual(self.public_text(),'controlled description')
        po_rights,_=self.grant('description',origin='PUBLISHER_OFFICIAL');po_lease=self.enqueue(po_rights,'PUBLISHER_OFFICIAL')
        other=self.metadata.publish_description(po_lease,text='publisher description',provenance={})
        self.assertEqual(other['description_id'],receipt['description_id']);self.assertEqual(self.public_text(),'controlled description')
        _,_,cover_high,_=self.cover()
        self.assertEqual(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"),cover_high['cover_id'])
        po_cover_rights,_=self.grant('cover',origin='PUBLISHER_OFFICIAL');po_cover_lease=self.enqueue(po_cover_rights,'PUBLISHER_OFFICIAL')
        buffer=io.BytesIO();Image.new('RGB',(3,4),(50,60,70)).save(buffer,format='PNG')
        with self.root.stage(po_cover_lease) as (path,fd):os.write(fd,buffer.getvalue())
        self.metadata.register_cover(po_cover_lease,stage_path=path,filename='cover.png',provenance={})
        po_cover_receipt=self.metadata.publish_cover(po_cover_lease)
        self.assertEqual(po_cover_receipt['cover_id'],cover_high['cover_id'])
        self.assertEqual(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"),cover_high['cover_id'])
        self.assertEqual(self.scalar("SELECT description_id FROM metadata_heads WHERE work_id='w'"),receipt['description_id'])
        self.assertEqual(self.public_text(),'controlled description')

    def test_A17_low_priority_origin_cannot_override_controlled_description(self):
        rights,_=self.grant('description',origin='PUBLISHER_OFFICIAL');lease=self.enqueue(rights,'PUBLISHER_OFFICIAL')
        official=self.metadata.publish_description(lease,text='official ending without spoiler',provenance={})
        self.assertEqual(self.public_text(),'official ending without spoiler')
        spoiler_rights,_=self.grant('description',origin='QWEN_TEXT');revision,_,_=self.first()
        spoiler_lease=self.enqueue(spoiler_rights,'QWEN_TEXT',revision)
        before=self.scalar('SELECT count(*) FROM metadata_candidates')
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(spoiler_lease,text='spoiler: the protagonist dies',provenance={'model_id':'synthetic-model','prompt_sha256':'a'*64})
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates'),before)
        self.assertEqual(self.scalar('SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id',id=official['description_id']),'approved')
        self.assertEqual(self.public_text(),'official ending without spoiler')

    def test_A18_restart_retry_reclaims_inflight_metadata_and_keeps_kind_consistent(self):
        rights,_=self.grant('description',origin='EDITORIAL')
        job_id=self.metadata.enqueue(self.token,self.csrf,work_id='w',origin='EDITORIAL',rights_id=rights,
            expected_rights_version=1,expected_head_version=self.head(),source_revision_id=None,request_id=str(uuid.uuid4()))
        lease_a=self.h.jobs.claim('worker-a',kind='metadata.description.publish');self.assertEqual(lease_a.job_id,job_id)
        self.h.clock.advance(30001)
        lease_b=self.h.jobs.claim('worker-b',kind='metadata.description.publish')
        self.assertEqual(lease_b.job_id,job_id);self.assertNotEqual(lease_b.generation,lease_a.generation)
        before=self.head()
        receipt=self.metadata.publish_description(lease_b,text='recovered description',provenance={})
        self.assertEqual(self.head(),before+1)
        self.assertEqual(self.scalar('SELECT state FROM jobs WHERE id=:id',id=job_id),'completed')
        _,_,cover,_=self.cover()
        self.assertEqual(self.scalar("SELECT description_id FROM metadata_heads WHERE work_id='w'"),receipt['description_id'])
        self.assertEqual(self.scalar("SELECT cover_id FROM metadata_heads WHERE work_id='w'"),cover['cover_id'])

    def test_A19_synthetic_lease_publish_completes_job_and_records_result(self):
        rights,_=self.grant('description',origin='EDITORIAL')
        job_id=self.metadata.enqueue(self.token,self.csrf,work_id='w',origin='EDITORIAL',rights_id=rights,
            expected_rights_version=1,expected_head_version=self.head(),source_revision_id=None,request_id=str(uuid.uuid4()))
        lease=self.h.jobs.claim('worker-x',kind='metadata.description.publish');self.assertEqual(lease.job_id,job_id)
        before=self.head();receipt=self.metadata.publish_description(lease,text='real worker description',provenance={})
        self.assertEqual(self.scalar('SELECT state FROM jobs WHERE id=:id',id=job_id),'completed')
        self.assertIsNotNone(self.scalar('SELECT result_json FROM jobs WHERE id=:id',id=job_id))
        self.assertEqual(self.head(),before+1)
        self.assertEqual(self.public_text(),'real worker description')


    def test_expired_editorial_does_not_block_fresh_description_and_expiry_is_not_ready(self):
        self.first()
        deadline=self.h.clock.wall+1000
        rights,_=self.grant(cache_until_ms=deadline)
        old=self.metadata.edit_description(self.token,self.csrf,work_id='w',text='old editorial',rights_id=rights,
            expected_rights_version=1,expected_head_version=self.head(),request_id=str(uuid.uuid4()))
        self.h.clock.advance(1000)
        summary_rights,_=self.grant(origin='EDITORIAL',cache_until_ms=self.h.clock.wall+1000)
        lease=self.enqueue(summary_rights,'EDITORIAL')
        current=self.metadata.publish_description(lease,text='new summary sentinel',provenance={})
        self.assertNotEqual(current['description_id'],old['description_id'])
        queries=LibraryQueries(self.auth,now_ms=lambda:self.h.clock.wall)
        self.assertEqual(queries.detail(self.token,'w')['description']['text'],'new summary sentinel')
        self.h.clock.advance(1000)
        detail=queries.detail(self.token,'w')
        self.assertEqual(detail['work']['summary_state'],'pending');self.assertIsNone(detail['description']['text'])
        self.assertIn('w',[x.work.work_id for x in queries.list(self.token,LibraryQuery(has_summary=False)).items])
        from document_library.recommendations import RecommendationQueries
        from document_library.library_contracts import RecommendationQuery, RankingConfig
        queries=LibraryQueries(self.auth,ranking=RankingConfig(),now_ms=lambda:self.h.clock.wall)
        preferences=dict(filters=dict(categories=[],states=[],formats=[],representations=[],genres=[]),recommendations=dict(exclude_rated=False,exclude_downloaded=False))
        with self.engine.connect() as conn:
            result=RecommendationQueries(queries).recommend_in(conn,actor=self.auth.current(conn,self.token),query=RecommendationQuery(mode='top_rated'),preferences=preferences,as_of_ms=self.h.clock.wall)
        self.assertEqual([x['work']['summary_state'] for x in result['items'] if x['work']['work_id']=='w'],['pending'])

    def test_embedded_cover_wins_in_both_publication_orders_without_replacing_description(self):
        revision,_,_=self.first()
        editorial,*_=self.editorial()
        published={}
        for origin in ('PUBLISHER_OFFICIAL','EMBEDDED','PUBLISHER_OFFICIAL'):
            rights,_=self.grant('cover',origin=origin)
            lease=self.enqueue(rights,origin,revision if origin=='EMBEDDED' else None)
            buffer=io.BytesIO();Image.new('RGB',(3,4),(10,20,30)).save(buffer,format='PNG')
            with self.root.stage(lease) as (path,fd):os.write(fd,buffer.getvalue())
            self.metadata.register_cover(lease,stage_path=path,filename='cover.png',provenance={})
            receipt=self.metadata.publish_cover(lease)
            candidate=self.scalar('SELECT id FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id)
            if origin=='EMBEDDED':published['embedded']=candidate
            if 'embedded' in published:self.assertEqual(receipt['cover_id'],published['embedded'])
            self.assertEqual(receipt['description_id'],editorial['description_id'])

    def test_explicit_failed_quality_never_publishes_candidate_or_completes_job(self):
        revision,_,_=self.first();rights,_=self.grant(origin='QWEN_TEXT')
        lease=self.enqueue(rights,'QWEN_TEXT',revision);before=self.head()
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='ending should not publish',
                provenance={'quality':{'passed':False,'reason_code':'SPOILER_HIGH'}})
        self.assertEqual(self.head(),before);self.assertIsNone(self.public_text())
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=lease.job_id),0)
        self.assertNotEqual(self.h.jobs.get(lease.job_id)['state'],'completed')

    def test_admin_rights_state_preserves_independent_decision_and_current_eligibility(self):
        rights,decision=self.grant(cache_until_ms=self.h.clock.wall+1000,derivative_allowed=False)
        state=self.metadata.work_metadata_state(self.token,work_id='w')
        row=next(r for r in state['rights'] if r['rights_id']==rights)
        self.assertTrue(row['editorial_eligible']);self.assertFalse(row['derivative_allowed'])
        self.assertEqual(row['retention_policy'],decision['retention_policy']);self.assertEqual(row['evidence'],decision['evidence'])
        self.h.clock.advance(1000)
        row=next(r for r in self.metadata.work_metadata_state(self.token,work_id='w')['rights'] if r['rights_id']==rights)
        self.assertFalse(row['editorial_eligible'])


class MetadataMigrationAcceptance(unittest.TestCase):
    def test_A01_upgrade_preserves_original_content_and_schema_signature(self):
        with tempfile.TemporaryDirectory(prefix='g4-migration-') as folder:
            engine=database_engine(Path(folder)/'synthetic.sqlite3')
            try:
                config=Config();config.set_main_option('script_location',str(ROOT/'migrations'))
                with engine.begin() as conn:
                    config.attributes['connection']=conn;command.upgrade(config,'0016_member_features')
                    conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('u','synthetic','disabled-synthetic','test')"))
                    conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('w','novel','Old','old','test','test')"))
                    conn.execute(text("INSERT INTO ratings VALUES('u','w',7,'test')"));conn.execute(text("UPDATE library_metadata SET description_text='legacy body',description_origin='legacy',description_state='ready' WHERE work_id='w'"))
                migrate(engine,ROOT/'migrations');migrate(engine,ROOT/'migrations')
                with closing(sqlite3.connect(Path(folder)/'synthetic.sqlite3',autocommit=True)) as db:
                    register_sqlite_functions(db);self.assertEqual(db.execute('SELECT * FROM ratings').fetchone(),('u','w',7,'test'))
                    self.assertEqual(db.execute('SELECT description_text,description_origin,description_state FROM library_metadata').fetchone(),('legacy body','legacy','pending'))
                    self.assertEqual(db.execute('SELECT count(*) FROM metadata_candidates').fetchone(),(0,));self.assertEqual(schema_signature(db),SCHEMA_SIGNATURE)
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(),[('ok',)]);self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
            finally:engine.dispose()
        self.assertFalse(Path(folder).exists())


if __name__=='__main__':unittest.main()
