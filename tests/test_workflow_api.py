"""Actual ASGI workflow boundary and explicit synthetic ingestion execution."""
import asyncio
import json
import os
import uuid
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from sqlalchemy import text
from document_library.api import create_app,SESSION_COOKIE
from document_library.database import write_transaction
from document_library.download_service import DownloadService
from document_library.library_api import LibraryFacade
from document_library.library_policy import LibraryReadPolicy
from document_library.library_queries import LibraryQueries
from document_library.preview_service import PreviewService
from document_library.streaming import StreamAdmission
from document_library.ingestion import IngestionWorker
import test_ingestion as ingestion_fixtures

ORIGIN='https://workflow.synthetic.invalid'


class WorkflowHTTPAcceptance(unittest.TestCase):
    def setUp(self):
        self.h=ingestion_fixtures.IngestionAcceptance('runTest');self.h.setUp();self.addCleanup(self.h.doCleanups)
        os.chmod(self.h.path/'synthetic.sqlite3',0o600)
        self.gate=StreamAdmission(reserve_fds=4,transient_fds=2,soft_limit=4096)
        self.downloads=DownloadService(self.h.publisher,admission=self.gate)
        policy=LibraryReadPolicy(self.h.auth,now_ms=lambda:self.h.clock.wall)
        self.library=LibraryFacade(queries=LibraryQueries(self.h.auth),policy=policy,
            previews=PreviewService(self.h.publisher,policy,admission=self.gate),downloads=self.downloads)
        self.app=create_app(self.h.auth,origin=ORIGIN,uploads=self.h.uploads,library=self.library,ingestion=self.h.ingest)
        self.client=TestClient(self.app,base_url=ORIGIN,raise_server_exceptions=False)
        self.addCleanup(lambda:self.downloads.close(wait=False))
        self.addCleanup(lambda:asyncio.run(self.library.supervisor.shutdown()))
        self.addCleanup(self.client.close)

    def request(self, method, path, *, actor=None,body=None,csrf=True,origin=ORIGIN,params=None):
        actor=actor or self.h.admin
        headers={'Origin':origin,'Cookie':SESSION_COOKIE+'='+actor.token}
        if csrf:headers['X-CSRF-Token']=actor.csrf
        return self.client.request(method,'/api/v1'+path,headers=headers,json=body,params=params)

    def error(self, response, status):
        self.assertEqual(response.status_code,status,response.text)
        self.assertEqual(set(response.json()),{'error'})
        self.assertEqual(set(response.json()['error']),{'code','message','request_id','details'})
        uuid.UUID(response.json()['error']['request_id'])
        for forbidden in ('internal-secret','/private/','token_hash','request_digest','tus_id','lease_owner','SELECT '):self.assertNotIn(forbidden,response.text)
        self.assertIn('no-store',response.headers['cache-control'])
        self.assertTrue(all(value.strip()=='nosniff' for value in response.headers['x-content-type-options'].split(',')))

    def test_catalog_http_create_defaults_retry_classification_and_legacy_edition_lookup(self):
        ident=str(uuid.uuid4());path='/admin/catalog/works/'+ident
        first=self.request('PUT',path,body={'title_display':'UI synthetic'})
        self.assertEqual(first.status_code,200,first.text);self.assertEqual(first.json()['item_type'],'unknown')
        self.assertEqual(self.request('PUT',path,body={'title_display':'UI synthetic'}).json(),first.json())
        self.error(self.request('PUT',path,body={'title_display':'Other'}),409)
        classify=path+'/classification/'+str(uuid.uuid4())
        body=dict(item_type='novel',publication_state='ongoing',expected_item_type='unknown',expected_publication_state='unknown',evidence='Explicit synthetic')
        self.assertEqual(self.request('PUT',classify,body=body).status_code,200)
        replay=self.request('PUT',path,body={'title_display':'UI synthetic'})
        self.assertEqual(replay.status_code,200,replay.text);self.assertEqual(replay.json()['item_type'],'novel')
        edition=path+'/editions/'+str(uuid.uuid4())
        body=dict(edition_key='text',edition_label='Text',representation='text')
        response=self.request('PUT',edition,body=body)
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(response.json()['source_form'],'unknown');self.assertIsNone(response.json()['language'])
        self.assertEqual(self.request('GET',path+'/editions').json()['editions'][0]['active_revision_id'],None)
        self.assertEqual(len(self.request('GET','/admin/catalog/works',params={'q':'UI'}).json()['works']),1)
        self.assertEqual(self.request('GET','/admin/catalog/works/w/editions').status_code,200)

    def test_owner_enqueue_and_distinct_admin_bind_are_idempotent_without_publishing(self):
        upload=self.h.staged();owner=self.h.uploader;body=dict(edition_id='e',expected_revision_id=None)
        path='/uploads/'+upload+'/enqueue'
        self.error(self.request('POST',path,body=body),403)
        response=self.request('POST',path,actor=owner,body=body)
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(response.json()['ingestion_state'],'review_pending')
        self.assertEqual(response.json()['rights_status'],'review');self.assertTrue(response.json()['worker_required']);self.assertNotIn('job_id',response.text)
        self.assertEqual(self.request('POST',path,actor=owner,body=body).json(),response.json())
        bound=self.request('POST','/admin/uploads/'+upload+'/bind',body=body)
        self.assertEqual(bound.status_code,200,bound.text)
        self.error(self.request('POST','/admin/uploads/'+upload+'/bind',body={'edition_id':'images'}),409)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM ingestion_inputs'),1);self.assertEqual(self.h.scalar('SELECT count(*) FROM edition_revisions'),0)
        self.error(self.request('POST',path,actor=self.h.member,body=body),403)

    def test_actual_workflow_waits_review_then_explicit_worker_publishes_and_status_links(self):
        work=str(uuid.uuid4());edition=str(uuid.uuid4())
        self.assertEqual(self.request('PUT','/admin/catalog/works/'+work,body={'title_display':'E2E only','item_type':'novel'}).status_code,200)
        self.assertEqual(self.request('PUT','/admin/catalog/works/'+work+'/editions/'+edition,body=dict(edition_key='text',edition_label='Text',representation='text')).status_code,200)
        upload=self.h.staged();path='/admin/uploads/'+upload
        bound=self.request('POST',path+'/bind',body={'edition_id':edition})
        self.assertEqual(bound.status_code,200,bound.text)
        worker=IngestionWorker(self.h.ingest,owner='explicit-http-test',lease_ms=1000)
        self.assertIsNone(asyncio.run(worker.run_once()))
        reviewed=self.request('POST',path+'/rights-review',body={'rights_status':'allowed','rights_receipt':'synthetic-rights'})
        self.assertEqual(reviewed.status_code,200,reviewed.text);self.assertEqual(reviewed.json()['ingestion_state'],'queued')
        audit=self.h.scalar("SELECT count(*) FROM audit_events WHERE action='ingestion.rights'")
        self.assertEqual(self.request('POST',path+'/rights-review',body={'rights_status':'allowed','rights_receipt':'synthetic-rights'}).status_code,200)
        self.assertEqual(self.h.scalar("SELECT count(*) FROM audit_events WHERE action='ingestion.rights'"),audit)
        revision=asyncio.run(worker.run_once());self.assertIsInstance(revision,str)
        batch=self.h.scalar('SELECT batch_id FROM upload_sessions WHERE id=:id',id=upload)
        detail=self.request('GET','/upload-batches/'+batch,actor=self.h.uploader)
        self.assertEqual(detail.status_code,200,detail.text);row=detail.json()['files'][0]
        self.assertEqual(row['ingestion_state'],'completed');self.assertEqual(row['publication_state'],'published')
        self.assertEqual(row['published'],dict(work_id=work,edition_id=edition,revision_id=revision))
        self.assertEqual(self.request('GET','/items/'+work,actor=self.h.member).status_code,200)
        self.assertEqual(self.request('GET','/admin/uploads/review-queue').json()['uploads'],[])
        blocked=self.request('POST',path+'/rights-review',body={'rights_status':'blocked','rights_receipt':'synthetic-revocation'})
        self.assertEqual(blocked.status_code,200,blocked.text)
        self.assertEqual(blocked.json()['ingestion_state'],'completed');self.assertFalse(blocked.json()['worker_required'])
        historical=self.request('GET','/upload-batches/'+batch,actor=self.h.uploader).json()['files'][0]
        self.assertEqual(historical['publication_state'],'published');self.assertIsNone(historical['published'])
        self.error(self.request('GET','/items/'+work,actor=self.h.member),404)

    def test_reads_current_owner_admin_and_query_cursor_bounds(self):
        one=self.h.batch();self.h.batch();self.h.batch(self.h.admin)
        listing=self.request('GET','/upload-batches',actor=self.h.uploader,params={'limit':'1'})
        self.assertEqual(listing.status_code,200,listing.text);self.assertEqual(len(listing.json()['batches']),1)
        cursor=listing.json()['next_cursor'];self.assertIsNotNone(cursor)
        self.error(self.request('GET','/upload-batches',actor=self.h.admin,params={'cursor':cursor}),422)
        self.error(self.request('GET','/admin/uploads/review-queue',actor=self.h.uploader),403)
        for params in ({'limit':'0'},{'limit':'101'},{'limit':'true'},{'x':'secret'},[('limit','1'),('limit','2')],{'cursor':'invalid'}):
            self.error(self.request('GET','/upload-batches',actor=self.h.uploader,params=params),422)
        anonymous=self.client.get('/api/v1/upload-batches',headers={'Origin':ORIGIN})
        self.error(anonymous,401)
        with write_transaction(self.h.engine) as conn:conn.execute(text("UPDATE users SET role='member' WHERE id=:id"),dict(id=self.h.admin.id))
        self.error(self.request('GET','/admin/catalog/works'),403)

    def test_origin_csrf_strict_body_and_same_engine_configuration(self):
        upload=self.h.staged();path='/admin/uploads/'+upload+'/bind';body={'edition_id':'e'}
        self.error(self.request('POST',path,body=body,origin='https://other.invalid'),403)
        self.error(self.request('POST',path,body=body,csrf=False),403)
        for bad in ({'edition_id':3},{'edition_id':'e','extra':'internal-secret'},{'edition_id':'e','expected_revision_id':True}):
            self.error(self.request('POST',path,body=bad),422)
        self.error(self.request('POST',path,body={'edition_id':'e','expected_revision_id':'stale'}),409)
        self.error(self.request('PUT','/admin/catalog/works/'+str(uuid.uuid4()),body={'title_display':'bad\nprivate'}),422)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM ingestion_inputs'),0)
        with self.assertRaises(ValueError):create_app(self.h.auth,origin=ORIGIN,ingestion=self.h.ingest)
        with self.assertRaises(ValueError):create_app(self.h.auth,origin=ORIGIN,uploads=object(),library=self.library,ingestion=self.h.ingest)

    def test_owner_revocation_and_expiry_prevent_admin_bind_and_expired_queue_display(self):
        upload=self.h.staged();path='/admin/uploads/'+upload+'/bind'
        with write_transaction(self.h.engine) as conn:conn.execute(text('UPDATE upload_sessions SET expires_at=:now WHERE id=:id'),dict(id=upload,now=self.h.auth.now()))
        self.assertEqual(self.request('GET','/admin/uploads/review-queue').json()['uploads'],[])
        self.error(self.request('POST',path,body={'edition_id':'e'}),403)
        with write_transaction(self.h.engine) as conn:conn.execute(text('UPDATE upload_sessions SET expires_at=:now WHERE id=:id'),dict(id=upload,now=self.h.auth.now()+100))
        self.h.revoke()
        self.error(self.request('POST',path,body={'edition_id':'e'}),403)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM ingestion_inputs'),0)

    def test_rights_http_writer_failure_rolls_back_and_never_exposes_raw_error(self):
        upload=self.h.staged();base='/admin/uploads/'+upload
        self.assertEqual(self.request('POST',base+'/bind',body={'edition_id':'e'}).status_code,200)
        self.error(self.request('POST',base+'/rights-review',actor=self.h.uploader,body={'rights_status':'allowed','rights_receipt':'synthetic'}),403)
        with patch.object(self.h.auth,'audit',side_effect=RuntimeError('/private/internal-secret SQL SELECT failure')):
            self.error(self.request('POST',base+'/rights-review',body={'rights_status':'allowed','rights_receipt':'synthetic'}),500)
        self.assertEqual(self.h.scalar('SELECT rights_status FROM ingestion_inputs'),'review')
        self.assertEqual(self.h.scalar("SELECT kind FROM jobs WHERE upload_session_id=:id",id=upload),'ingest.review_upload')
