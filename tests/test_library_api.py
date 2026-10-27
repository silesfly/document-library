"""Actual ASGI library flows over disposable original files and authorization."""
import asyncio
import io
import json
import unittest
import uuid
import zipfile
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import text

from document_library.api import create_app, SESSION_COOKIE
from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.download_service import DownloadService
from document_library.library_api import LibraryFacade
from document_library.library_contracts import LibraryQuery, SORTS
from document_library.library_queries import LibraryQueries
from document_library.metadata_service import MetadataService
from document_library.preview_service import PreviewService
from document_library.streaming import StreamAdmission
from library_helpers import LibraryFixture
import test_preview_service as preview_fixtures

ORIGIN = 'https://library.synthetic.invalid'


class LibraryHTTPAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.gate = StreamAdmission(reserve_fds=4,transient_fds=2,soft_limit=4096)
        self.downloads = DownloadService(self.publisher,admission=self.gate)
        self.previews = PreviewService(self.publisher,self.policy,admission=self.gate)
        self.library = LibraryFacade(queries=LibraryQueries(self.auth),policy=self.policy,previews=self.previews,downloads=self.downloads)
        self.metadata = MetadataService(self.publisher)
        self.app = create_app(self.auth,origin=ORIGIN,library=self.library,metadata=self.metadata)
        self.addCleanup(lambda:self.downloads.close(wait=False))
        self.addCleanup(lambda:asyncio.run(self.library.supervisor.shutdown()))
        self.client = TestClient(self.app,base_url=ORIGIN,headers={'Origin':ORIGIN,'Cookie':SESSION_COOKIE+'='+self.token})
        self.addCleanup(self.client.close)

    def test_metadata_routes_reject_malformed_input_and_missing_candidate_without_writes(self):
        self.admin()
        headers={'X-CSRF-Token':self.csrf}
        before=self.scalar('SELECT count(*) FROM metadata_rights')
        decision=dict(state=[],officiality='not_applicable',match_state='not_applicable',
            display_allowed=True,cache_allowed=True,derivative_allowed=False,cache_until_ms=None,
            retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={})
        response=self.client.post('/api/v1/admin/metadata/rights/new/review',headers=headers,
            json=dict(work_id='w',kind='description',expected_version=0,request_id='bad-input',decision=decision))
        self.envelope(response,422)
        decision['state']='review'
        for key in ('cache_until_ms','expected_version'):
            for value in (2**63,10**100):
                payload=dict(work_id='w',kind='description',expected_version=0,request_id='oversized-number',decision=dict(decision))
                if key=='cache_until_ms':payload['decision'][key]=value
                else:payload[key]=value
                with self.subTest(key=key,value=value):
                    response=self.client.post('/api/v1/admin/metadata/rights/new/review',headers=headers,json=payload)
                    self.envelope(response,422)
        response=self.client.post('/api/v1/admin/metadata/candidates/absent/review',headers=headers,
            json=dict(expected_control_version=1,expected_head_version=0,request_id='absent',review_status='approved',select=True))
        self.envelope(response,404)
        for query in ('limit=1&limit=100','limit=1&unknown=yes','limit=-1','limit=+1'):
            with self.subTest(query=query):self.envelope(self.client.get('/api/v1/admin/metadata/review-queue?'+query),422)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_rights'),before)

    def test_metadata_routes_reject_new_requests_after_supervisor_shutdown(self):
        self.admin();before=self.scalar('SELECT count(*) FROM metadata_rights')
        asyncio.run(self.library.supervisor.shutdown())
        decision=dict(state='review',officiality='unknown',match_state='review',display_allowed=False,
            cache_allowed=False,derivative_allowed=False,cache_until_ms=None,
            retention_policy=dict(backup_allowed=False,all_copies_deletion_required=False),evidence={})
        requests=[('GET','/api/v1/admin/metadata/review-queue',None),('GET','/api/v1/admin/metadata/works/w',None),
            ('POST','/api/v1/admin/metadata/rights/new/review',dict(work_id='w',kind='description',expected_version=0,request_id='after-shutdown',decision=decision)),
            ('POST','/api/v1/admin/metadata/works/w/description',dict(text='body',rights_id='new',expected_rights_version=1,expected_head_version=0,request_id='after-shutdown')),
            ('POST','/api/v1/admin/metadata/candidates/absent/review',dict(expected_control_version=1,expected_head_version=0,request_id='after-shutdown',review_status='approved',select=True))]
        for method,path,body in requests:
            with self.subTest(path=path):
                response=self.client.request(method,path,json=body,headers={'X-CSRF-Token':self.csrf})
                self.envelope(response,409);self.assertEqual(response.json()['error']['code'],'LIBRARY_SHUTTING_DOWN')
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_rights'),before)

    def choice(self, revision):
        with self.engine.connect() as conn:
            ident = conn.execute(text('SELECT id FROM revision_options WHERE edition_revision_id=:revision ORDER BY id'),dict(revision=revision)).scalar_one()
        return dict(edition_id='e',revision_id=revision,download_option_id=ident)

    def issue(self, revision, **kwargs):
        headers = {'X-CSRF-Token':self.csrf,**kwargs.pop('headers',{})}
        return self.client.post('/api/v1/items/w/download-selections',json=dict(selection=[self.choice(revision)],delivery='zip'),headers=headers,**kwargs)

    def test_A_metadata_admin_review_queue_and_routes(self):
        self.admin()
        response=self.client.get('/api/v1/admin/metadata/review-queue')
        self.assertEqual(response.status_code,200);body=response.json()
        self.assertEqual(body['candidates'],[]);self.assertEqual(body['rights'],[])
        rights_id=str(uuid.uuid4())
        self.metadata.review_rights(self.token,self.csrf,rights_id=rights_id,work_id='w',kind='description',expected_version=0,request_id=str(uuid.uuid4()),
            decision=dict(state='review',officiality='not_applicable',match_state='not_applicable',display_allowed=False,cache_allowed=False,derivative_allowed=False,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'note':'http review'}))
        listed=self.client.get('/api/v1/admin/metadata/review-queue').json()
        self.assertEqual([r['rights_id'] for r in listed['rights']],[rights_id]);self.assertEqual(listed['rights'][0]['state'],'review')
        response=self.client.post('/api/v1/admin/metadata/rights/%s/review'%rights_id,headers={'X-CSRF-Token':self.csrf},
            json=dict(work_id='w',kind='description',expected_version=1,request_id=str(uuid.uuid4()),
                decision=dict(state='allowed',officiality='not_applicable',match_state='not_applicable',display_allowed=True,cache_allowed=True,derivative_allowed=False,cache_until_ms=None,
                    retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'note':'http allowed'})))
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(response.json()['version'],2)
        self.assertEqual(self.client.get('/api/v1/admin/metadata/review-queue').json()['rights'],[])
        state=self.client.get('/api/v1/admin/metadata/works/w').json()
        self.assertIsNone(state['head']['description_id']);self.assertEqual([r['rights_id'] for r in state['rights'] if r['state']=='allowed'],[rights_id])
        allowed=[r for r in state['rights'] if r['state']=='allowed'][0]
        response=self.client.post('/api/v1/admin/metadata/works/w/description',headers={'X-CSRF-Token':self.csrf},
            json=dict(text='G4 HTTP editorial description',rights_id=allowed['rights_id'],expected_rights_version=allowed['version'],expected_head_version=state['head']['version'],request_id=str(uuid.uuid4())))
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(response.json()['version'],state['head']['version']+1)
        self.assertIsNotNone(self.client.get('/api/v1/admin/metadata/works/w').json()['head']['description_id'])

    def envelope(self, response, status):
        self.assertEqual(response.status_code,status,response.text)
        payload = response.json()
        self.assertEqual(set(payload),{'error'})
        self.assertEqual(set(payload['error']),{'code','message','request_id','details'})
        self.assertEqual(str(uuid.UUID(payload['error']['request_id'])),payload['error']['request_id'])
        self.assertEqual(response.headers['cache-control'].split(',')[-1].strip(),'no-store')
        self.assertEqual(response.headers['x-content-type-options'].split(',')[-1].strip(),'nosniff')
        self.assertEqual(response.headers['referrer-policy'].split(',')[-1].strip(),'no-referrer')
        for forbidden in ('session_token_hash','source_job_id','bundle_relpath','database/test','synthetic-session-only','payload_digest'):
            self.assertNotIn(forbidden,response.text)
        return payload['error']

    def test_explicit_facade_all_routes_and_legacy_strings(self):
        self.first()
        paths = {route.path for route in self.app.routes}
        for tail in ('items','items/{work_id}','items/{work_id}/editions','items/{work_id}/labels/{kind}',
            'items/{work_id}/editions/{edition_id}/revisions','items/{work_id}/preview','items/{work_id}/chapters',
            'items/{work_id}/pages/{page}','items/{work_id}/cover','items/{work_id}/download','items/{work_id}/download-selections','download-tickets/{ticket_id}'):
            self.assertIn('/api/v1/'+tail,paths)
        self.assertEqual(self.client.get('/me').status_code,200)
        old = self.client.get('/me',headers={'Cookie':''})
        self.assertEqual(old.status_code,401)
        self.assertEqual(old.json(),{'error':'authentication_required'})
        new = self.client.get('/api/v1/items',headers={'Cookie':''})
        self.envelope(new,401)
        self.assertFalse(any(route.path.startswith('/api/v1/') for route in create_app(self.auth,origin=ORIGIN).routes))

    def test_auth_engine_and_publisher_must_be_shared(self):
        with self.assertRaises(ValueError):
            create_app(object(),origin=ORIGIN,library=self.library)
        with patch.object(self.downloads,'publisher',object()):
            with self.assertRaises(ValueError):
                LibraryFacade(queries=LibraryQueries(self.auth),policy=self.policy,previews=self.previews,downloads=self.downloads)

    def test_all_sort_filters_and_strict_unknown_or_duplicate_inputs(self):
        self.first()
        for sort in SORTS:
            response = self.client.get('/api/v1/items',params={'sort':sort})
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(len(response.json()['items']),1)
        query = [('categories','novel_text'),('categories','unknown'),('states','ongoing'),('states','unknown'),
            ('formats','txt'),('representations','text'),('genres','synthetic'),('unrated','true'),
            ('favorite','false'),('has_summary','false'),('downloaded','false'),('has_gaps','false')]
        self.assertEqual(self.client.get('/api/v1/items',params=query).status_code,200)
        for params in ({'sort':'secret_column'},{'favorite':'1'},{'has_summary':'False'},{'limit':'1.0'},
            {'unknown':'x'},[('limit','1'),('limit','2')],[('genres','g')]*65):
            self.envelope(self.client.get('/api/v1/items',params=params),422)

    def test_detail_editions_labels_legacy_identifiers_and_public_fields(self):
        revision,_,_ = self.first()
        item = self.client.get('/api/v1/items/w')
        self.assertEqual(item.status_code,200,item.text)
        editions = self.client.get('/api/v1/items/w/editions').json()
        self.assertEqual(editions['default_edition_id'],'e')
        option = editions['editions'][0]['download_options'][0]
        self.assertEqual(option['revision_id'],revision)
        self.assertEqual(option['kind'],'single_file')
        for key in ('tags','genres'):
            response = self.client.get('/api/v1/items/w/labels/'+key)
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.json()['items'],[])
        self.envelope(self.client.get('/api/v1/items/'+'x'*200),404)
        self.envelope(self.client.get('/api/v1/items/'+'x'*201),422)
        for secret in ('source_job_id','source_generation','local_relpath','session_hash','payload_digest'):
            self.assertNotIn(secret,item.text)

    def test_single_original_full_range_head_and_work_binding(self):
        revision,_,_ = self.first()
        url = f'/api/v1/items/w/download?revision_id={revision}&asset_position=0'
        response = self.client.get(url)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.content,b'novel 1-150')
        self.assertTrue(response.headers['content-disposition'].startswith('attachment;'))
        part = self.client.get(url,headers={'Range':'bytes=1-3'})
        self.assertEqual((part.status_code,part.content),(206,b'ove'))
        changed = self.client.get(url,headers={'Range':'bytes=1-3','If-Range':'"other"'})
        self.assertEqual((changed.status_code,changed.content),(200,b'novel 1-150'))
        head = self.client.head(url,headers={'Range':'bytes=bad'})
        self.assertEqual((head.status_code,head.content),(200,b''))
        self.assertEqual(head.headers['content-length'],'11')
        bad = self.client.get(url,headers={'Range':'bytes=100-'})
        self.envelope(bad,416)
        self.assertEqual(bad.headers['content-range'],'bytes */11')
        self.envelope(self.client.get(url.replace('/items/w/','/items/other/')),404)
        self.assertEqual(self.scalar("SELECT count(*) FROM download_events WHERE status='completed'"),3)

    def test_current_rights_and_active_revision_are_rechecked(self):
        previous,_,_ = self.first()
        current,lease,_ = self.publish(previous=previous)
        self.envelope(self.client.get(f'/api/v1/items/w/download?revision_id={previous}&asset_position=0'),404)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),dict(job=lease.job_id))
        self.envelope(self.client.get(f'/api/v1/items/w/download?revision_id={current}&asset_position=1'),403)

    def test_ticket_head_does_not_claim_and_zip_is_one_use_session_bound(self):
        revision,_,_ = self.first()
        issued = self.issue(revision)
        self.assertEqual(issued.status_code,201,issued.text)
        value = issued.json()
        self.assertEqual(set(value),{'ticket_id','expires_at','download_url'})
        head = self.client.head(value['download_url'])
        self.assertEqual((head.status_code,head.content),(200,b''))
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        alternate = self.session('synthetic-second-session')
        self.envelope(self.client.get(value['download_url'],headers={'Cookie':SESSION_COOKIE+'='+alternate}),403)
        response = self.client.get(value['download_url'])
        self.assertEqual(response.status_code,200,response.text)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            self.assertEqual([archive.read(name) for name in archive.namelist()],[b'novel 1-150'])
        self.assertEqual(self.scalar('SELECT count(*) FROM object_pins WHERE kind=\'ticket\''),0)
        self.envelope(self.client.get(value['download_url']),409)
        self.assertEqual(self.scalar("SELECT count(*) FROM download_events WHERE status='completed'"),1)

    def test_invalid_same_work_selection_is_409_and_issues_no_partial_ticket(self):
        revision,_,_ = self.first()
        bad = self.choice(revision)
        bad['download_option_id'] = 'missing-option'
        response = self.client.post('/api/v1/items/w/download-selections',json=dict(selection=[self.choice(revision),bad],delivery='zip'),headers={'X-CSRF-Token':self.csrf})
        self.envelope(response,409)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)
        payload = dict(selection=[dict(self.choice(revision),work_id='other')],delivery='zip')
        self.envelope(self.client.post('/api/v1/items/w/download-selections',json=payload,headers={'X-CSRF-Token':self.csrf}),422)
        self.envelope(self.client.post('/api/v1/items/w/download-selections',json=dict(selection=[self.choice(revision)],delivery='raw'),headers={'X-CSRF-Token':self.csrf}),422)

    def test_versioned_boundary_origin_json_size_csrf_and_duplicate_range(self):
        revision,_,_ = self.first()
        self.envelope(self.issue(revision,headers={'Origin':'https://other.invalid'}),403)
        self.envelope(self.client.post('/api/v1/items/w/download-selections',content='{}',headers={'Content-Type':'text/plain'}),415)
        self.envelope(self.client.post('/api/v1/items/w/download-selections',json=dict(selection=[self.choice(revision)],delivery='zip')),403)
        self.envelope(self.client.post('/api/v1/items/w/download-selections',content='x'*65537,headers={'Content-Type':'application/json'}),413)
        url = f'/api/v1/items/w/download?revision_id={revision}&asset_position=0'
        self.envelope(self.client.get(url,headers=[('Range','bytes=0-1'),('Range','bytes=2-3')]),400)
        self.envelope(self.client.get(url,headers=[('If-Range','a'),('If-Range','b')]),400)
        self.envelope(self.client.get('/api/v1/unknown'),404)
        self.envelope(self.client.get('/api/v1'),404)

    def test_errors_never_echo_sql_paths_or_unknown_codes(self):
        self.first()
        leak = 'SELECT password_hash FROM /private/synthetic.db token=synthetic-session-only'
        with patch.object(self.library.queries,'detail',side_effect=AuthError(leak,409)):
            response = self.client.get('/api/v1/items/w')
        error = self.envelope(response,409)
        self.assertEqual(error['code'],'ASSET_UNAVAILABLE')
        self.assertNotIn('SELECT',response.text)
        self.assertNotIn('/private/',response.text)

    def test_revisions_current_admin_retained_binding_and_live_options(self):
        old,_,_ = self.first()
        current,_,_ = self.publish(previous=old)
        url = '/api/v1/items/w/editions/e/revisions'
        response = self.client.get(url)
        self.assertEqual([r['id'] for r in response.json()['revisions']],[current])
        self.admin()
        response = self.client.get(url,params={'limit':'1'})
        self.assertEqual([r['id'] for r in response.json()['revisions']],[current])
        cursor = response.json()['next_cursor']
        self.assertIsNotNone(cursor)
        next_page = self.client.get(url,params={'cursor':cursor,'limit':'1'})
        self.assertEqual([r['id'] for r in next_page.json()['revisions']],[old])
        self.envelope(self.client.get(url.replace('/items/w/','/items/other/')),404)
        self.envelope(self.client.get(url.replace('/editions/e/','/editions/other/'),params={'cursor':cursor}),422)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE objects SET state='MISSING'"))
        self.envelope(self.client.get(url),404)

    def test_missing_preview_enqueues_fixed_job_after_actual_close_only(self):
        self.first()
        response = self.client.get('/api/v1/items/w/preview')
        error = self.envelope(response,409)
        self.assertEqual(error['code'],'PREVIEW_NOT_READY')
        self.assertEqual(error['details'],[dict(state='repair_queued',retry_after_seconds=1)])
        self.assertEqual(response.headers['retry-after'],'1')
        self.assertEqual(self.gate.usage()['producers'],0)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_repair_inputs'),1)
        self.assertNotIn('job_id',response.text)
        self.assertEqual(self.client.get('/api/v1/items/w/preview').status_code,409)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),1)

    def test_explicit_bundle_position_and_default_option_are_actual(self):
        old,_,_ = self.first()
        current,_,_ = self.publish(previous=old)
        # Existing publication builds independent real single options, not a
        # synthetic combined TXT. Default selection must identify a real option.
        details = self.client.get('/api/v1/items/w').json()
        default = details['work']['default_option']
        self.assertEqual(default['revision_id'],current)
        with patch.object(self.previews,'response',wraps=self.previews.response) as observed:
            response = self.client.get('/api/v1/items/w/preview')
        self.assertEqual(response.status_code,409)
        selected = observed.call_args.args[0]()
        self.assertEqual(selected.assets[0].asset_position,default['asset_positions'][0])
        self.assertEqual(selected.assets[0].option_id,default['option_id'])

    def test_pdf_inline_has_no_download_audit_and_attachment_is_separate(self):
        revision,_,_ = self.publish(b'%PDF synthetic original',format='pdf')
        inline = self.client.get('/api/v1/items/w/preview',params={'revision_id':revision,'asset_position':'0'})
        self.assertEqual(inline.status_code,200,inline.text)
        self.assertTrue(inline.headers['content-disposition'].startswith('inline;'))
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
        attachment = self.client.get('/api/v1/items/w/download',params={'revision_id':revision,'asset_position':'0'})
        self.assertEqual(attachment.status_code,200,attachment.text)
        self.assertTrue(attachment.headers['content-disposition'].startswith('attachment;'))
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)

    def test_verified_text_chapters_and_image_page_use_existing_cache(self):
        h = preview_fixtures.VerifiedPreviewAcceptance('runTest')
        h.setUp()
        self.addCleanup(h.doCleanups)
        revision,_ = h.publish()
        downloads = DownloadService(h.h.publisher,admission=self.gate)
        library = LibraryFacade(queries=LibraryQueries(h.h.auth),policy=h.policy,previews=h.service,downloads=downloads)
        self.addCleanup(lambda:downloads.close(wait=False))
        self.addCleanup(lambda:asyncio.run(library.supervisor.shutdown()))
        client = TestClient(create_app(h.h.auth,origin=ORIGIN,library=library),base_url=ORIGIN,
            headers={'Origin':ORIGIN,'Cookie':SESSION_COOKIE+'='+h.h.uploader.token})
        self.addCleanup(client.close)
        response = client.get('/api/v1/items/w/preview',params={'revision_id':revision,'asset_position':'0'})
        self.assertEqual(response.status_code,200,response.text)
        self.assertIn('synthetic chapter body',response.text)
        chapters = client.get('/api/v1/items/w/chapters',params={'revision_id':revision,'asset_position':'0'})
        self.assertEqual(chapters.status_code,200,chapters.text)
        self.assertEqual(chapters.json()['chapters'][0]['number'],1)
        from test_ingest_formats import archive_bytes, image_bytes
        with write_transaction(h.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('i','w','packed','imagepack','Images','test')"))
        image_revision,_ = h.publish(archive_bytes([('1.png',image_bytes())]),filename='Book.cbz',edition='i')
        page = client.get('/api/v1/items/w/pages/1',params={'edition_id':'i','revision_id':image_revision,'asset_position':'0'})
        self.assertEqual(page.status_code,200,page.text)
        self.assertEqual(page.headers['content-type'],'image/png')
        self.assertTrue(page.content.startswith(b'\x89PNG'))

    def test_uncalled_middleware_response_and_route_cancel_never_claim_ticket(self):
        from starlette.responses import JSONResponse
        import threading
        revision,_,_ = self.first()
        ticket = self.issue(revision).json()
        captured = []
        lazy = self.library.lazy
        def discard(factory,request):
            captured.append(lazy(factory,request))
            return JSONResponse({'intercepted':True})
        with patch.object(self.library,'lazy',discard):
            response = self.client.get(ticket['download_url'])
        self.assertEqual(response.json(),{'intercepted':True})
        self.assertEqual(len(captured),1)
        self.assertEqual(self.gate.usage()['producers'],0)
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        entered,release = threading.Event(),threading.Event()
        def blocked(factory,request):
            response = lazy(factory,request)
            entered.set()
            release.wait()
            return response
        async def cancel_before_call():
            scope = dict(type='http',asgi={'version':'3.0','spec_version':'2.0'},method='GET',scheme='https',
                path=ticket['download_url'],raw_path=ticket['download_url'].encode(),query_string=b'',
                root_path='',http_version='1.1',server=('library.synthetic.invalid',443),client=('synthetic',1),
                headers=[(b'host',b'library.synthetic.invalid'),(b'cookie',(SESSION_COOKIE+'='+self.token).encode())])
            async def receive():
                await asyncio.Event().wait()
            async def send(_):
                self.fail('Cancelled route must never invoke its response')
            task = asyncio.create_task(self.app(scope,receive,send))
            try:
                for _ in range(200):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.002)
                self.assertTrue(entered.is_set())
                task.cancel()
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            finally:
                release.set()
        with patch.object(self.library,'lazy',blocked):
            asyncio.run(cancel_before_call())
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        self.assertEqual(self.gate.usage()['producers'],0)

    def test_unknown_native_start_shutdown_retains_owner_and_pins_until_real_exit(self):
        if not callable(getattr(self.downloads,'retry_pending',None)):
            self.skipTest('D finalization public seam not frozen; integration remains INCOMPLETE')
        import threading
        from document_library.transfer_owner import stopped_owner
        revision,_,_ = self.first()
        issued = self.issue(revision).json()
        pending = []
        real_start = threading.Thread.start
        def unknown(thread):
            if thread.name == 'library-original-producer':
                pending.append(thread)
                raise RuntimeError('synthetic unknown launch')
            return real_start(thread)
        with patch.object(threading.Thread,'start',unknown):
            response = self.client.get(issued['download_url'])
        self.envelope(response,500)
        self.assertEqual(len(pending),1)
        self.assertFalse(pending[0].is_alive())
        result = asyncio.run(self.library.supervisor.shutdown())
        self.assertFalse(result['clean'])
        self.assertFalse(result['owner_closed'])
        self.assertEqual(result['unresolved_handles'],1)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),1)
        with stopped_owner(self.root,self.downloads.owner.descriptor) as stopped:
            self.assertIsNone(stopped)
        try:
            real_start(pending[0])
            pending[0].join()
            result = asyncio.run(self.library.supervisor.shutdown())
            self.assertTrue(result['clean'],result)
            self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)
        finally:
            if pending[0].ident is None:
                real_start(pending[0])
            pending[0].join()

    def test_two_actual_editions_zip_keeps_each_original_and_one_work_event(self):
        from test_publication import option
        revision,_,_ = self.first()
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('images','w','images','imagepack','Images','test')"))
        lease = self.h.make_job()
        asset = self.h.asset(lease,b'synthetic imagepack original','cbz')
        self.publisher.register_validated_upload(lease,edition_id='images',expected_revision_id=None,
            assets=[asset],options=[option(0,'cbz')],validation_receipt='synthetic-validation',rights_status='allowed')
        image_revision = self.publisher.publish(lease)
        with self.engine.connect() as conn:
            image_option = conn.execute(text('SELECT id FROM revision_options WHERE edition_revision_id=:revision'),dict(revision=image_revision)).scalar_one()
        choice = [self.choice(revision),dict(edition_id='images',revision_id=image_revision,download_option_id=image_option)]
        response = self.client.post('/api/v1/items/w/download-selections',json=dict(selection=choice,delivery='zip'),headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,201,response.text)
        downloaded = self.client.get(response.json()['download_url'])
        self.assertEqual(downloaded.status_code,200,downloaded.text)
        with zipfile.ZipFile(io.BytesIO(downloaded.content)) as archive:
            self.assertEqual([archive.read(name) for name in archive.namelist()],[b'novel 1-150',b'synthetic imagepack original'])
        self.assertEqual(self.scalar("SELECT count(*) FROM download_events WHERE status='completed'"),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_event_assets'),2)

    def test_bundle_preview_demands_real_original_position(self):
        lease = self.h.make_job()
        assets = [self.h.asset(lease,'1화\nfirst chapter'.encode()),self.h.asset(lease,'2화\nsecond chapter'.encode())]
        option = dict(format='txt',delivery_kind='bundle',positions=[0,1],
            coverage=dict(unit='episode',intervals=[[1,2]],gaps=[],confidence='verified'))
        self.h.plan(lease,assets,[option])
        revision = self.publisher.publish(lease)
        response = self.client.get('/api/v1/items/w/preview')
        error = self.envelope(response,409)
        self.assertEqual(error['code'],'ORIGINAL_SELECTION_REQUIRED')
        self.assertEqual(error['details'],[dict(asset_positions=[0,1])])
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),0)
        selected = self.client.get('/api/v1/items/w/preview',params={'revision_id':revision,'asset_position':'1'})
        self.envelope(selected,409)
        self.assertEqual(self.scalar('SELECT asset_position FROM preview_repair_inputs'),1)

    def test_standalone_original_ignores_sibling_but_rechecks_selected_source_rights(self):
        old,old_lease,_ = self.first()
        revision,new_lease,_ = self.publish(previous=old)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),dict(job=old_lease.job_id))
        url = f'/api/v1/items/w/download?revision_id={revision}&asset_position=1'
        response = self.client.get(url)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.content,b'new original')
        self.envelope(self.client.get(url.replace('asset_position=1','asset_position=0')),403)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),dict(job=new_lease.job_id))
        self.envelope(self.client.get(url),403)

    def test_finalization_callback_failure_retains_pins_and_reports_unclean_shutdown(self):
        if not callable(getattr(self.downloads,'retry_pending',None)):
            self.skipTest('D finalization public seam not frozen; integration remains INCOMPLETE')
        revision,_,_ = self.first()
        issued = self.issue(revision).json()
        with patch.object(self.downloads,'_finish',side_effect=RuntimeError('synthetic finalization failure')):
            with self.assertRaisesRegex(RuntimeError,'synthetic finalization failure'):
                self.client.get(issued['download_url'])
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),1)
        self.assertEqual(self.downloads.pending_finalizations(),1)
        result = asyncio.run(self.library.supervisor.shutdown())
        self.assertFalse(result['clean'])
        self.assertEqual(result['cleanup_failures'],1)
        self.assertEqual(result['unresolved_handles'],0)
        self.assertTrue(result['owner_closed'])
        self.assertEqual(result['pending_finalizations'],0)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)
        self.assertEqual(self.scalar('SELECT status FROM download_events'),'completed')
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_events'),self.scalar('SELECT archive_bytes FROM download_tickets'))
        self.assertFalse(self.downloads.recover(self.scalar('SELECT id FROM download_attempts')))

    def test_64_choices_of_200_character_ids_fit_only_the_versioned_selection_body_limit(self):
        choices = [dict(edition_id=('edition-'+str(i)).ljust(200,'e'),revision_id='r'*200,
            download_option_id=('option-'+str(i)).ljust(200,'o')) for i in range(64)]
        payload = dict(selection=choices,delivery='zip')
        self.assertGreater(len(json.dumps(payload).encode()),8192)
        self.assertLess(len(json.dumps(payload).encode()),65536)
        with patch.object(self.downloads,'issue',side_effect=AuthError('ASSET_UNAVAILABLE',409)) as issue:
            response = self.client.post('/api/v1/items/w/download-selections',json=payload,headers={'X-CSRF-Token':self.csrf})
        self.envelope(response,409)
        self.assertEqual(len(issue.call_args.args[3]),64)
        self.assertEqual(len(issue.call_args.args[3][0].edition_id),200)
        old = self.client.post('/auth/login',content='x'*8193,headers={'Content-Type':'application/json'})
        self.assertEqual((old.status_code,old.json()),(413,{'error':'body_too_large'}))

    def test_cancelled_issue_request_remains_inflight_until_real_sync_worker_finishes(self):
        import threading
        revision,_,_ = self.first()
        body = json.dumps(dict(selection=[self.choice(revision)],delivery='zip')).encode()
        entered,release = threading.Event(),threading.Event()
        issue = self.downloads.issue
        def blocked(*args,**kwargs):
            entered.set()
            release.wait()
            return issue(*args,**kwargs)
        async def exercise():
            scope = dict(type='http',asgi={'version':'3.0','spec_version':'2.0'},method='POST',scheme='https',
                path='/api/v1/items/w/download-selections',raw_path=b'/api/v1/items/w/download-selections',query_string=b'',
                root_path='',http_version='1.1',server=('library.synthetic.invalid',443),client=('synthetic',1),
                headers=[(b'host',b'library.synthetic.invalid'),(b'origin',ORIGIN.encode()),(b'content-type',b'application/json'),
                    (b'cookie',(SESSION_COOKIE+'='+self.token).encode()),(b'x-csrf-token',self.csrf.encode())])
            sent = False
            async def receive():
                nonlocal sent
                if not sent:
                    sent = True
                    return dict(type='http.request',body=body,more_body=False)
                await asyncio.Event().wait()
            async def send(_):
                self.fail('Cancelled issuer route must not send a response')
            task = asyncio.create_task(self.app(scope,receive,send))
            try:
                for _ in range(200):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.002)
                self.assertTrue(entered.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                result = await self.library.supervisor.shutdown()
                self.assertFalse(result['clean'])
                self.assertFalse(result['owner_closed'])
                self.assertEqual(result['inflight_operations'],1)
            finally:
                release.set()
            for _ in range(500):
                if self.library.supervisor.health()['inflight_operations']==0:
                    break
                await asyncio.sleep(0.002)
            self.assertEqual(self.library.supervisor.health()['inflight_operations'],0)
        with patch.object(self.downloads,'issue',blocked):
            asyncio.run(exercise())
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        ticket = self.scalar('SELECT id FROM download_tickets')
        self.h.clock.advance(120000)
        self.assertTrue(self.downloads.expire(ticket))
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)

    def test_legacy_unexpected_error_keeps_default_plaintext_response(self):
        @self.app.get('/synthetic-legacy-failure')
        def failure():
            raise RuntimeError('synthetic unhandled legacy exception')
        with TestClient(self.app,base_url=ORIGIN,raise_server_exceptions=False) as client:
            response = client.get('/synthetic-legacy-failure')
        self.assertEqual((response.status_code,response.text),(500,'Internal Server Error'))
        self.assertTrue(response.headers['content-type'].startswith('text/plain'))

    async def _raw_ticket_get(self, path, send):
        scope = dict(type='http',asgi={'version':'3.0','spec_version':'2.0'},method='GET',scheme='https',
            path=path,raw_path=path.encode(),query_string=b'',root_path='',http_version='1.1',
            server=('library.synthetic.invalid',443),client=('synthetic',1),
            headers=[(b'host',b'library.synthetic.invalid'),(b'cookie',(SESSION_COOKIE+'='+self.token).encode())])
        async def receive():
            await asyncio.Event().wait()
        await self.app(scope,receive,send)

    def test_actual_app_final_asgi_send_holds_response_barrier_and_owner(self):
        from document_library.transfer_owner import stopped_owner
        revision,_,_ = self.first()
        issued = self.issue(revision).json()
        async def exercise():
            entered,release = asyncio.Event(),asyncio.Event()
            async def send(message):
                if message['type']=='http.response.body' and not message.get('more_body',False):
                    entered.set()
                    await release.wait()
            task = asyncio.create_task(self._raw_ticket_get(issued['download_url'],send))
            try:
                await entered.wait()
                handles = tuple(self.library.supervisor._handles)
                self.assertEqual(len(handles),1)
                handle = handles[0]
                for _ in range(200):
                    if handle.closed.is_set() and handle.wait_exited(0):
                        break
                    await asyncio.sleep(0.002)
                self.assertTrue(handle.closed.is_set())
                self.assertTrue(handle.wait_exited(0))
                self.assertEqual(self.downloads.pending_finalizations(),1)
                self.assertEqual(self.downloads.retry_pending(),dict(pending=1,retried=0,failed=0))
                state = await self.library.supervisor.shutdown()
                self.assertFalse(state['clean'])
                self.assertFalse(state['owner_closed'])
                self.assertEqual(state['unfinalized_responses'],1)
                self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),1)
                with stopped_owner(self.root,self.downloads.owner.descriptor) as stopped:
                    self.assertIsNone(stopped)
            finally:
                release.set()
            await task
            state = await self.library.supervisor.shutdown()
            self.assertTrue(state['clean'],state)
            self.assertTrue(state['owner_closed'])
            self.assertEqual(state['pending_finalizations'],0)
        asyncio.run(exercise())
        self.assertEqual(self.scalar('SELECT status FROM download_events'),'completed')
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_events'),self.scalar('SELECT archive_bytes FROM download_tickets'))
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)

    def test_actual_app_repeated_cancel_keeps_finalizer_worker_and_pin_until_exit(self):
        import threading
        from document_library.transfer_owner import stopped_owner
        revision,_,_ = self.first()
        issued = self.issue(revision).json()
        entered,release = threading.Event(),threading.Event()
        finish = self.downloads._finish
        def blocked(*args,**kwargs):
            entered.set()
            release.wait()
            return finish(*args,**kwargs)
        async def exercise():
            body = asyncio.Event()
            async def send(message):
                if message['type']=='http.response.body' and message.get('body'):
                    body.set()
                    await asyncio.Event().wait()
            task = asyncio.create_task(self._raw_ticket_get(issued['download_url'],send))
            try:
                await body.wait()
                task.cancel()
                for _ in range(300):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.002)
                self.assertTrue(entered.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertEqual(self.downloads.pending_finalizations(),1)
                self.assertEqual(self.downloads.retry_pending(),dict(pending=1,retried=0,failed=0))
                state = await self.library.supervisor.shutdown()
                self.assertFalse(state['clean'])
                self.assertFalse(state['owner_closed'])
                self.assertGreater(state['inflight_operations'],0)
                self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),1)
                with stopped_owner(self.root,self.downloads.owner.descriptor) as stopped:
                    self.assertIsNone(stopped)
            finally:
                release.set()
            for _ in range(500):
                state = self.library.supervisor.health()
                if not state['cleanup_tasks'] and not state['inflight_operations']:
                    break
                await asyncio.sleep(0.002)
            self.assertEqual(self.downloads.pending_finalizations(),0)
            state = await self.library.supervisor.shutdown()
            self.assertTrue(state['clean'],state)
            self.assertTrue(state['owner_closed'])
        with patch.object(self.downloads,'_finish',blocked):
            asyncio.run(exercise())
        self.assertEqual(self.scalar('SELECT status FROM download_events'),'failed')
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_events'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)


class ConfiguredMemberHTTPAcceptance(LibraryFixture):
    """Real member/ranking/quota composition while G2 fixtures stay legacy."""
    choice = LibraryHTTPAcceptance.choice
    issue = LibraryHTTPAcceptance.issue
    envelope = LibraryHTTPAcceptance.envelope

    def setUp(self):
        from document_library.library_contracts import RankingConfig
        from document_library.member_service import MemberService
        from document_library.recommendations import RecommendationQueries
        from document_library.request_quota import AccountQuota
        super().setUp()
        self.quota = AccountQuota(self.auth)
        self.queries = LibraryQueries(self.auth,ranking=RankingConfig())
        self.members = MemberService(self.auth,quota=self.quota,read_social_in=self.queries.read_social_in)
        self.recommendations = RecommendationQueries(self.queries)
        self.gate = StreamAdmission(reserve_fds=4,transient_fds=2,soft_limit=4096)
        self.downloads = DownloadService(self.publisher,admission=self.gate,quota=self.quota)
        self.previews = PreviewService(self.publisher,self.policy,admission=self.gate)
        self.facade_args = dict(queries=self.queries,policy=self.policy,previews=self.previews,
            downloads=self.downloads,members=self.members,recommendations=self.recommendations,quota=self.quota)
        self.library = LibraryFacade(**self.facade_args)
        self.app = create_app(self.auth,origin=ORIGIN,library=self.library)
        self.addCleanup(lambda:self.downloads.close(wait=False))
        self.addCleanup(lambda:asyncio.run(self.library.supervisor.shutdown()))
        self.client = TestClient(self.app,base_url=ORIGIN,headers={'Origin':ORIGIN,'Cookie':SESSION_COOKIE+'='+self.token})
        self.addCleanup(self.client.close)

    def counter(self, action):
        from document_library.auth import digest
        return self.scalar('SELECT coalesce(sum(attempts),0) FROM auth_limits WHERE bucket=:bucket',bucket='g3:'+action+':'+digest('u'))

    def mutate(self, method, path, value):
        return self.client.request(method,path,json=value,headers={'X-CSRF-Token':self.csrf})

    def test_configured_facade_rejects_partial_and_mismatched_authorities(self):
        for name in ('members','recommendations','quota'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                LibraryFacade(**dict(self.facade_args,**{name:None}))
        other = LibraryQueries(self.auth,ranking=self.queries.ranking)
        for owner,name,value in ((self.members,'read_social_in',other.read_social_in),
                (self.recommendations,'queries',other),(self.queries,'ranking',None),
                (self.quota,'engine',object()),(self.downloads,'quota',None)):
            with self.subTest(name=name), patch.object(owner,name,value), self.assertRaises(ValueError):
                LibraryFacade(**self.facade_args)
        with patch.object(self.quota,'auth',object()), self.assertRaises(ValueError):
            DownloadService(self.publisher,quota=self.quota)
        with self.assertRaises(ValueError):
            LibraryFacade(queries=self.queries,policy=self.policy,previews=self.previews,downloads=self.downloads)

    def test_social_real_callback_uses_writer_and_returns_only_committed_public_fields(self):
        self.first()
        callback = self.members.read_social_in
        observed = []
        def social(conn, **kwargs):
            self.assertTrue(conn.connection.driver_connection.in_transaction)
            self.assertIs(current.call_args.args[0],conn)
            self.assertIs(reserve.call_args.args[0],conn)
            observed.append((conn,kwargs['actor']['id']))
            return callback(conn,**kwargs)
        with (patch.object(self.auth,'current',wraps=self.auth.current) as current,
                patch.object(self.quota,'reserve_in',wraps=self.quota.reserve_in) as reserve,
                patch.object(self.members,'read_social_in',social)):
            response = self.mutate('PUT','/api/v1/items/w/rating',{'rating_half_steps':9})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(current.call_count,1)
        self.assertEqual(reserve.call_count,1)
        self.assertEqual(len(observed),1)
        self.assertEqual(response.json(),dict(work_id='w',my_rating_half_steps=9,rating_average=4.5,
            rating_votes=1,weighted_rating=4.5,favorite=False))
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'),9)
        self.assertEqual(self.counter('social'),1)
        with patch.object(self.members,'read_social_in',side_effect=AuthError('private callback path/session',409)):
            self.envelope(self.mutate('PUT','/api/v1/items/w/rating',{'rating_half_steps':1}),409)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'),9)
        self.assertEqual(self.counter('social'),1)
        saved = self.mutate('PUT','/api/v1/items/w/favorite',{'favorite':True})
        self.assertTrue(saved.json()['favorite'])
        deleted = self.mutate('DELETE','/api/v1/items/w/rating',{})
        self.assertIsNone(deleted.json()['my_rating_half_steps'])
        self.assertEqual(deleted.json()['rating_votes'],0)
        self.assertIsNone(deleted.json()['weighted_rating'])
        self.assertEqual(self.mutate('DELETE','/api/v1/items/w/rating',{}).status_code,200)
        self.assertEqual(self.counter('social'),4)


    def test_strict_social_json_and_current_auth_source_boundaries(self):
        _,lease,_ = self.first()
        url = '/api/v1/items/w/rating'
        for value in (True,1.0,'1',None,0,11):
            self.envelope(self.mutate('PUT',url,{'rating_half_steps':value}),422)
        for value in ({},{'rating_half_steps':5,'user_id':'other'},[]):
            self.envelope(self.mutate('PUT',url,value),422)
        self.envelope(self.client.put(url,content='null',headers={'Content-Type':'application/json','X-CSRF-Token':self.csrf}),422)
        for value in ({'favorite':1},{'favorite':'true'},{'favorite':True,'work_id':'w'}):
            self.envelope(self.mutate('PUT','/api/v1/items/w/favorite',value),422)
        self.envelope(self.mutate('DELETE',url,{'extra':True}),422)
        self.envelope(self.client.request('DELETE',url,headers={'X-CSRF-Token':self.csrf}),415)
        self.envelope(self.client.put(url,content='{',headers={'Content-Type':'application/json'}),422)
        self.envelope(self.client.put(url,content='x'*8193,headers={'Content-Type':'application/json'}),413)
        self.envelope(self.client.put(url,json={'rating_half_steps':5}),403)
        self.envelope(self.client.put(url,json={'rating_half_steps':5},headers={'Cookie':'','X-CSRF-Token':self.csrf}),401)
        self.envelope(self.client.put(url,json={'rating_half_steps':5},headers={'Origin':'https://other.invalid','X-CSRF-Token':self.csrf}),403)
        self.envelope(self.mutate('PUT','/api/v1/items/missing/rating',{'rating_half_steps':5}),404)
        self.assertEqual(self.counter('social'),0)
        self.assertEqual(self.client.get('/api/v1/items/w').status_code,200)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),dict(job=lease.job_id))
        self.envelope(self.mutate('PUT',url,{'rating_half_steps':5}),404)
        self.envelope(self.mutate('DELETE',url,{}),404)
        self.assertEqual(self.counter('social'),0)

    def test_preferences_delegate_full_validation_and_get_creates_no_state(self):
        response = self.client.get('/api/v1/me/preferences')
        self.assertEqual(response.status_code,200,response.text)
        self.assertIsNone(response.json()['updated_at'])
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'),0)
        self.assertEqual(self.counter('social'),0)
        self.envelope(self.client.get('/api/v1/me/preferences',params={'user_id':'other'}),422)
        payload = response.json();payload.pop('updated_at')
        for bad in (dict(payload,updated_at='client'),dict(payload,filters={}),
                dict(payload,recommendations={'exclude_rated':1,'exclude_downloaded':False}),
                dict(payload,view='tiles')):
            self.envelope(self.mutate('PUT','/api/v1/me/preferences',bad),422)
        self.assertEqual(self.counter('social'),0)
        payload['view'] = 'list'
        payload['filters']['genres'] = ['Fantasy','Fantasy']
        saved = self.mutate('PUT','/api/v1/me/preferences',payload)
        self.assertEqual(saved.status_code,200,saved.text)
        self.assertEqual(saved.json()['filters']['genres'],['Fantasy'])
        self.assertEqual(self.client.get('/api/v1/me/preferences').json(),saved.json())
        self.assertEqual(self.counter('social'),1)

    def test_social_quota_shared_across_sessions_safe_429_and_bounded_retry_header(self):
        from document_library.library_api import error_response
        from document_library.request_quota import QuotaExceeded
        self.first()
        for _ in range(30):
            response = self.mutate('PUT','/api/v1/items/w/favorite',{'favorite':True})
            self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.counter('social'),30)
        second = self.session('synthetic-configured-second')
        response = self.client.put('/api/v1/items/w/rating',json={'rating_half_steps':5},
            headers={'Cookie':SESSION_COOKIE+'='+second,'X-CSRF-Token':self.csrf})
        self.assertEqual(self.envelope(response,429)['code'],'RATE_LIMITED')
        self.assertTrue(1<=int(response.headers['retry-after'])<=60)
        for value in ('g3:social','synthetic-configured-second','bucket','account'):
            self.assertNotIn(value,response.text)
        self.assertEqual(self.counter('social'),30)
        error = QuotaExceeded(3)
        for invalid in (True,0,61,'3'):
            error.retry_after_seconds = invalid
            result = error_response(error)
            self.assertEqual(result.status_code,429)
            self.assertNotIn('retry-after',result.headers)


    def test_recommendations_use_one_fresh_auth_preferences_query_snapshot_and_override_fields(self):
        from test_library_queries import seed_library
        seed_library(self.engine,3,ratings=False)
        self.assertEqual(self.mutate('PUT','/api/v1/items/w000000/rating',{'rating_half_steps':10}).status_code,200)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_genres(work_id,label) VALUES('w000001','Fantasy')"))
        preferences = self.client.get('/api/v1/me/preferences').json();preferences.pop('updated_at')
        preferences['filters']['genres'] = ['Fantasy']
        self.assertEqual(self.mutate('PUT','/api/v1/me/preferences',preferences).status_code,200)
        reader = self.members.preferences_in
        def observed(conn, **kwargs):
            self.assertIs(current.call_args.args[0],conn)
            self.assertTrue(conn.connection.driver_connection.in_transaction)
            return reader(conn,**kwargs)
        with (patch.object(self.engine,'connect',wraps=self.engine.connect) as opened,
                patch.object(self.auth,'current',wraps=self.auth.current) as current,
                patch.object(self.members,'preferences_in',observed),
                patch.object(self.recommendations,'recommend_in',wraps=self.recommendations.recommend_in) as recommend):
            response = self.client.get('/api/v1/recommendations')
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(opened.call_count,2)
        self.assertEqual(current.call_count,2)
        self.assertIs(recommend.call_args.args[0],current.call_args.args[0])
        self.assertIsNone(recommend.call_args.kwargs['query'].genres)
        self.assertEqual(recommend.call_args.kwargs['as_of_ms'],self.auth.now()*1000)
        self.assertEqual([item['work']['work_id'] for item in response.json()['items']],['w000001'])
        self.assertEqual(response.json()['items'][0]['reason'],'preferred_genre')
        cleared = self.client.get('/api/v1/recommendations',params={'genres':'','exclude_rated':'false'})
        self.assertEqual(len(cleared.json()['items']),3)
        forced = self.client.get('/api/v1/recommendations',params={'mode':'unrated','genres':'','exclude_rated':'false'})
        self.assertEqual(len(forced.json()['items']),2)
        self.assertNotIn('w000000',[item['work']['work_id'] for item in forced.json()['items']])
        self.assertEqual(self.client.get('/api/v1/me/preferences').json()['filters']['genres'],['Fantasy'])
        self.assertEqual(self.counter('recommend'),3)

    def test_recommendation_query_allowlist_rejects_before_quota(self):
        for params in ({'mode':'all'},{'limit':'0'},{'limit':'51'},{'limit':'1.0'},
                {'exclude_rated':'1'},{'exclude_downloaded':'False'},{'search':'x'},{'sort':'popular'},
                {'direction':'asc'},{'cursor':'x'},{'as_of':'0'},{'m':'5'},{'weights':'x'},
                {'peerlimit':'1'},{'user_id':'other'},[('mode','popular'),('mode','unrated')],
                [('genres',''),('genres','Fantasy')],[('genres','')]*2,[('genres','x')]*65):
            with self.subTest(params=params):
                self.envelope(self.client.get('/api/v1/recommendations',params=params),422)
        self.assertEqual(self.counter('recommend'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'),0)

    def test_committed_recommend_admission_rechecks_auth_and_keeps_downstream_failure_cost(self):
        admission = self.quota.admit
        def revoke(token, **kwargs):
            admission(token,**kwargs)
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE users SET status='suspended' WHERE id='u'"))
        with (patch.object(self.quota,'admit',revoke),
                patch.object(self.recommendations,'recommend_in',wraps=self.recommendations.recommend_in) as recommend):
            self.envelope(self.client.get('/api/v1/recommendations'),401)
            recommend.assert_not_called()
        self.assertEqual(self.counter('recommend'),1)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET status='active' WHERE id='u'"))
        with patch.object(self.recommendations,'recommend_in',side_effect=AuthError('private peer/session/sql',409)):
            response = self.client.get('/api/v1/recommendations')
        self.envelope(response,409)
        self.assertNotIn('private peer',response.text)
        self.assertEqual(self.counter('recommend'),2)

    def test_real_http_opposite_tastes_reverse_candidates_without_peer_disclosure(self):
        from document_library.auth import digest
        from test_library_queries import seed_library
        seed_library(self.engine,7,ratings=False)
        profiles = {'u':[10,8,2,4,6],'b':[2,4,10,8,6],
            'private-peer-a':[10,8,2,4,6,10,2],'private-peer-b':[2,4,10,8,6,2,10]}
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES(:id,:id,'disabled-synthetic','test')"),
                [dict(id=user) for user in profiles if user!='u'])
            conn.execute(text("INSERT INTO ratings VALUES(:user,:work,:rating,'test')"),
                [dict(user=user,work=f'w{number:06}',rating=rating) for user,ratings in profiles.items() for number,rating in enumerate(ratings)])
            conn.execute(text("INSERT INTO auth_sessions(token_hash,csrf_hash,user_id,credential_version,created_at,expires_at) SELECT :token,:csrf,id,credential_version,:now,:end FROM users WHERE id='b'"),
                dict(token=digest('synthetic-b-session'),csrf=digest(self.csrf),now=self.auth.now(),end=self.auth.now()+3600))
        first = self.client.get('/api/v1/recommendations')
        second = self.client.get('/api/v1/recommendations',headers={'Cookie':SESSION_COOKIE+'=synthetic-b-session'})
        self.assertEqual(first.status_code,200,first.text)
        self.assertEqual(second.status_code,200,second.text)
        self.assertEqual([item['work']['work_id'] for item in first.json()['items']],['w000005','w000006'])
        self.assertEqual([item['work']['work_id'] for item in second.json()['items']],['w000006','w000005'])
        for result in (first,second):
            self.assertIsNone(result.json()['fallback_reason'])
            self.assertEqual([item['predicted_rating'] for item in result.json()['items']],[5.0,1.0])
            for forbidden in ('private-peer','user_id','similarity','ratings_vector','synthetic-b-session'):
                self.assertNotIn(forbidden,result.text)


    def capture_lazy(self):
        from starlette.responses import JSONResponse
        captured = []
        lazy = self.library.lazy
        def intercept(factory, request):
            captured.append(lazy(factory,request))
            return JSONResponse({'intercepted':True})
        return captured,intercept

    def test_download_issue_and_actual_get_once_head_and_uncalled_lazy_zero(self):
        revision,_,_ = self.first()
        issued = self.issue(revision)
        self.assertEqual(issued.status_code,201,issued.text)
        self.assertEqual(self.counter('download'),1)
        ticket = issued.json()['download_url']
        original = f'/api/v1/items/w/download?revision_id={revision}&asset_position=0'
        captured,intercept = self.capture_lazy()
        with patch.object(self.library,'lazy',intercept):
            for url in (ticket,original):
                self.assertEqual(self.client.get(url).json(),{'intercepted':True})
        self.assertEqual(len(captured),2)
        self.assertEqual(self.counter('download'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        self.assertEqual(self.gate.usage()['producers'],0)
        for url in (ticket,original):
            self.assertEqual(self.client.head(url).status_code,200)
        self.assertEqual(self.counter('download'),1)
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        self.assertEqual(self.client.get(ticket).status_code,200)
        self.assertEqual(self.counter('download'),2)
        part = self.client.get(original,headers={'Range':'bytes=1-3'})
        self.assertEqual((part.status_code,part.content),(206,b'ove'))
        self.assertEqual(self.counter('download'),3)
        self.assertEqual(self.scalar("SELECT count(*) FROM download_events WHERE status='completed'"),2)

    def test_already_constructed_lazy_request_rechecks_current_auth_without_new_quota_cost(self):
        self.first()
        ticket = self.issue(self.scalar('SELECT id FROM edition_revisions')).json()['download_url']
        captured,intercept = self.capture_lazy()
        with patch.object(self.library,'lazy',intercept):
            self.client.get(ticket)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET status='suspended' WHERE id='u'"))
        async def actual_call():
            messages = []
            async def receive():
                await asyncio.Event().wait()
            async def send(message):
                messages.append(message)
            await captured[0](dict(type='http',method='GET'),receive,send)
            return messages
        messages = asyncio.run(actual_call())
        self.assertEqual(messages[0]['status'],401)
        self.assertEqual(self.counter('download'),1)
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        self.assertEqual(self.gate.usage()['producers'],0)
