"""Real ASGI reads over disposable upload/ingestion/auth snapshots."""
import asyncio
import json
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import event, text

from document_library.api import create_app, SESSION_COOKIE
from document_library.auth import digest
from document_library.cursors import encode_cursor
from document_library.database import write_transaction
from document_library.download_service import DownloadService
from document_library.library_api import LibraryFacade
from document_library.library_queries import LibraryQueries
from document_library.preview_service import PreviewService
from document_library.streaming import StreamAdmission
from document_library.uploads import UploadService
from library_helpers import LibraryFixture
import test_preview_service as preview_fixtures
from test_ingest_formats import archive_bytes, image_bytes

ORIGIN = 'https://reads.synthetic.invalid'


class ActivityHTTPAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.uploads = UploadService(self.auth)
        self.gate = StreamAdmission(reserve_fds=4,transient_fds=2,soft_limit=4096)
        self.downloads = DownloadService(self.publisher,admission=self.gate)
        self.library = LibraryFacade(queries=LibraryQueries(self.auth),policy=self.policy,
            previews=PreviewService(self.publisher,self.policy,admission=self.gate),downloads=self.downloads)
        self.app = create_app(self.auth,origin=ORIGIN,uploads=self.uploads,library=self.library)
        self.client = TestClient(self.app,base_url=ORIGIN,headers={'Origin':ORIGIN,'Cookie':SESSION_COOKIE+'='+self.token})
        self.addCleanup(lambda:self.downloads.close(wait=False))
        self.addCleanup(lambda:asyncio.run(self.library.supervisor.shutdown()))
        self.addCleanup(self.client.close)

    def batch(self):
        return self.uploads.create_batch(self.token,self.csrf,declared_total_bytes=10000,request_key=uuid.uuid4().hex)['batch_id']

    def files(self, batch, count=3, *, status='receiving', received=0):
        ids = sorted(str(uuid.uuid4()) for _ in range(count))
        with write_transaction(self.engine) as conn:
            for ident in ids:
                conn.execute(text('INSERT INTO upload_sessions VALUES(:id,:batch,\'u\',NULL,1,10,:received,:expiry,:status)'),
                    dict(id=ident,batch=batch,received=received,expiry=self.auth.now()+1000,status=status))
                conn.execute(text('INSERT INTO upload_reservations VALUES(:id,:batch,:key,:digest,:name)'),
                    dict(id=ident,batch=batch,key=ident,digest='0'*64,name='Book.txt'))
        return ids

    def get(self, batch, **kwargs):
        return self.client.get('/api/v1/upload-batches/'+batch,**kwargs)

    def error(self, response, status):
        self.assertEqual(response.status_code,status,response.text)
        self.assertEqual(set(response.json()),{'error'})
        self.assertEqual(set(response.json()['error']),{'code','message','request_id','details'})
        uuid.UUID(response.json()['error']['request_id'])
        for forbidden in ('internal-secret','/private/','token_hash','request_digest','tus_id','lease_owner','receipt','SELECT '):
            self.assertNotIn(forbidden,response.text)

    def user(self, ident='v', *, version=1, status='active'):
        with write_transaction(self.engine) as conn:
            conn.execute(text('INSERT INTO users(id,username,password_hash,credential_version,status,created_at) VALUES(:id,:id,\'disabled-synthetic\',:version,:status,\'test\')'),dict(id=ident,version=version,status=status))

    def session_for(self, user, name, *, version=1, expires=None):
        expiry=expires if expires is not None else self.auth.now()+1000
        with write_transaction(self.engine) as conn:
            conn.execute(text('INSERT INTO auth_sessions VALUES(:token,:csrf,:user,:version,:now,:expiry)'),
                dict(token=digest(name),csrf=digest('synthetic-csrf'),user=user,version=version,now=min(self.auth.now(),expiry-1),expiry=expiry))
        return {'Cookie':SESSION_COOKIE+'='+name}

    def test_owner_admin_revoked_capability_and_current_session_authority(self):
        batch = self.batch();self.files(batch,1)
        self.assertEqual(self.get(batch).status_code,200)
        self.user();other = self.session_for('v','synthetic-other')
        self.error(self.get(batch,headers=other),404)
        self.error(self.get(batch,headers={'Cookie':''}),401)
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM user_permissions WHERE user_id='u'"))
        self.assertEqual(self.get(batch).status_code,200)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET role='admin' WHERE id='v'"))
        self.assertEqual(self.get(batch,headers=other).status_code,200)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET credential_version=2 WHERE id='u'"))
        self.error(self.get(batch),401)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET status='suspended' WHERE id='v'"))
        self.error(self.get(batch,headers=other),401)

    def test_whole_counts_are_independent_of_file_page_and_offset_is_not_publication(self):
        batch = self.batch();ids=self.files(batch,4,status='staged',received=10)
        first=self.get(batch,params={'limit':'1'})
        self.assertEqual(first.status_code,200,first.text)
        value=first.json();self.assertEqual((value['file_count'],value['reserved_bytes'],value['received_bytes']),(4,40,40))
        self.assertEqual(value['ingestion_counts'],{'unqueued':4})
        self.assertEqual(value['transport_counts'],{'staged':4})
        self.assertEqual(value['files'][0]['ingestion_state'],'unqueued')
        self.assertEqual(value['files'][0]['publication_state'],'not_published')
        self.assertIsNone(value['files'][0]['published'])
        seen=[value['files'][0]['upload_id']]
        while value['next_cursor']:
            response=self.get(batch,params={'limit':'1','cursor':value['next_cursor']})
            self.assertEqual(response.status_code,200,response.text)
            value=response.json();seen.extend(item['upload_id'] for item in value['files'])
            self.assertEqual(value['file_count'],4)
        self.assertEqual(seen,ids)
        self.assertTrue(first.json()['created_at'].endswith('Z'))
        self.assertTrue(first.json()['files'][0]['reservation_expires_at'].endswith('Z'))

    def test_uuid_cursor_membership_limits_and_unknown_queries(self):
        batch=self.batch();self.files(batch,3)
        other=self.batch();other_ids=self.files(other,1)
        cursor=self.get(batch,params={'limit':'1'}).json()['next_cursor']
        self.error(self.get(other,params={'cursor':cursor}),422)
        forged=encode_cursor('upload.files',dict(batch_id=batch,last_id=other_ids[0]))
        self.error(self.get(batch,params={'cursor':forged}),422)
        for params in ({'limit':'0'},{'limit':'101'},{'limit':'1.0'},{'unknown':'x'},[('limit','1'),('limit','2')],{'cursor':'x'*4097}):
            self.error(self.get(batch,params=params),422)
        self.error(self.get('not-a-uuid'),404)
        self.error(self.get(str(uuid.uuid4())),404)

    def test_safe_filename_no_secret_projection_and_reads_make_no_writes(self):
        batch=self.batch();ident=self.files(batch,1)[0]
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE upload_reservations SET display_filename=:name WHERE upload_id=:id'),dict(id=ident,name='/private/internal-secret.txt'))
            conn.execute(text('INSERT INTO upload_transport_ops VALUES(:id,:upload,:backend,\'PATCH\',\'uncertain\',:now,NULL)'),
                dict(id=str(uuid.uuid4()),upload=ident,backend=str(uuid.uuid4()),now=self.auth.now()))
        statements=[]
        def capture(conn,cursor,sql,params,context,many):statements.append(sql.lstrip().upper())
        before=self.scalar('SELECT count(*) FROM audit_events')
        event.listen(self.engine,'before_cursor_execute',capture)
        try:response=self.get(batch)
        finally:event.remove(self.engine,'before_cursor_execute',capture)
        self.assertEqual(response.status_code,200,response.text)
        row=response.json()['files'][0]
        self.assertIsNone(row['display_filename']);self.assertEqual(row['filename_state'],'unavailable')
        self.assertEqual(row['transport_operation'],'uncertain')
        self.assertNotIn('/private/',response.text)
        self.assertNotIn('internal-secret',response.text)
        self.assertTrue(all(sql.startswith(('SELECT','WITH')) for sql in statements),statements)
        self.assertEqual(self.scalar('SELECT count(*) FROM audit_events'),before)

    def test_query_count_does_not_grow_with_returned_file_count(self):
        batch=self.batch();self.files(batch,120,status='published',received=10)
        counts=[]
        for limit in (1,100):
            statements=[]
            def capture(conn,cursor,sql,params,context,many):statements.append(sql)
            event.listen(self.engine,'before_cursor_execute',capture)
            try:response=self.get(batch,params={'limit':str(limit)})
            finally:event.remove(self.engine,'before_cursor_execute',capture)
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(len(response.json()['files']),limit)
            self.assertEqual(response.json()['file_count'],120)
            counts.append(len(statements))
        self.assertEqual(counts,[4,4])

    def test_counts_and_file_page_use_one_actual_read_snapshot(self):
        batch=self.batch();ident=self.files(batch,1)[0];changed=[]
        def mutate(conn,cursor,sql,params,context,many):
            if 'GROUP BY s.status,ingestion_state,decision' in sql and not changed:
                changed.append(True)
                with write_transaction(self.engine) as writer:
                    writer.execute(text('UPDATE upload_sessions SET received_size=7 WHERE id=:id'),dict(id=ident))
        event.listen(self.engine,'after_cursor_execute',mutate)
        try:response=self.get(batch)
        finally:event.remove(self.engine,'after_cursor_execute',mutate)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json()['received_bytes'],0)
        self.assertEqual(response.json()['files'][0]['received_size'],0)
        self.assertEqual(self.get(batch).json()['received_bytes'],7)

    def test_admin_activity_counts_only_currently_usable_sessions_and_preview_active_uploads(self):
        batch=self.batch();ids=self.files(batch,3)
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE upload_sessions SET expires_at=:old WHERE id=:id'),dict(old=self.auth.now()-1,id=ids[0]))
        self.user(version=2)
        self.session_for('v','synthetic-current',version=2)
        self.session_for('v','synthetic-old',version=1)
        self.session_for('v','synthetic-expired',version=2,expires=self.auth.now()-1)
        self.error(self.client.get('/api/v1/admin/users/u/activity'),403)
        self.admin()
        own=self.client.get('/api/v1/admin/users/u/activity')
        self.assertEqual(own.status_code,200,own.text)
        self.assertEqual(own.json()['active_upload_count'],2)
        self.assertEqual(own.json()['file_count'],3)
        self.assertEqual(own.json()['current_session_count'],1)
        target=self.client.get('/api/v1/admin/users/v/activity')
        self.assertEqual(target.json()['current_session_count'],1)
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE users SET status='suspended' WHERE id='v'"))
        self.assertEqual(self.client.get('/api/v1/admin/users/v/activity').json()['current_session_count'],0)
        self.error(self.client.get('/api/v1/admin/users/missing/activity'),404)
        self.error(self.client.get('/api/v1/admin/users/u/activity?limit=1'),422)

    def test_permission_audit_whitelist_stable_cursor_and_no_raw_details(self):
        self.user();self.admin()
        with write_transaction(self.engine) as conn:
            for number in range(3):
                self.auth.audit(conn,'u','permission_changed',target='v',details=dict(reason='권한 변경 '+str(number),old={'can_upload':0,'password_hash':'internal-secret'},new={'can_upload':True,'token_hash':'internal-secret'},path='/private/internal-secret',receipt='internal-secret'))
            self.auth.audit(conn,'u','permission_batch',details=dict(reason='일괄 처리'))
            self.auth.audit(conn,'u','login',details=dict(password='internal-secret'))
        seen=[];cursor=None
        while True:
            params={'limit':'1'}
            if cursor:params['cursor']=cursor
            response=self.client.get('/api/v1/admin/permission-audit',params=params)
            self.assertEqual(response.status_code,200,response.text)
            self.assertNotIn('internal-secret',response.text);self.assertNotIn('/private/',response.text)
            value=response.json();seen.extend(value['items']);cursor=value['next_cursor']
            if not cursor:break
        self.assertEqual(len(seen),4);self.assertEqual(len({x['event_id'] for x in seen}),4)
        self.assertEqual({x['action'] for x in seen},{'permission_changed','permission_batch'})
        changed=[x for x in seen if x['action']=='permission_changed']
        self.assertTrue(all(x['old_capability'] is False and x['new_capability'] is True for x in changed))
        self.assertTrue(all(x['created_at'].endswith('Z') for x in seen))
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE users SET role='member' WHERE id='u'"))
        self.error(self.client.get('/api/v1/admin/permission-audit'),403)

    def preview_fixture(self):
        h=preview_fixtures.VerifiedPreviewAcceptance('runTest');h.setUp();self.addCleanup(h.doCleanups)
        downloads=DownloadService(h.h.publisher,admission=self.gate)
        library=LibraryFacade(queries=LibraryQueries(h.h.auth),policy=h.policy,previews=h.service,downloads=downloads)
        client=TestClient(create_app(h.h.auth,origin=ORIGIN,library=library),base_url=ORIGIN,
            headers={'Origin':ORIGIN,'Cookie':SESSION_COOKIE+'='+h.h.uploader.token})
        self.addCleanup(lambda:downloads.close(wait=False));self.addCleanup(lambda:asyncio.run(library.supervisor.shutdown()));self.addCleanup(client.close)
        return h,client

    def test_real_published_result_link_is_current_authority_not_publication_history(self):
        h,client=self.preview_fixture();revision,lease=h.publish()
        with h.engine.connect() as conn:
            batch=conn.execute(text('SELECT batch_id FROM upload_sessions WHERE id=(SELECT upload_session_id FROM jobs WHERE id=:id)'),dict(id=lease.job_id)).scalar_one()
        response=client.get('/api/v1/upload-batches/'+batch)
        self.assertEqual(response.status_code,200,response.text)
        row=response.json()['files'][0]
        self.assertEqual(row['publication_state'],'published');self.assertEqual(row['ingestion_state'],'completed')
        self.assertEqual(row['published'],dict(work_id='w',edition_id='e',revision_id=revision))
        self.assertEqual(row['decision'],'initial')
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=lease.job_id))
            conn.execute(text("UPDATE ingestion_inputs SET rights_status='blocked' WHERE job_id=:id"),dict(id=lease.job_id))
        response=client.get('/api/v1/upload-batches/'+batch)
        row=response.json()['files'][0]
        self.assertEqual(row['publication_state'],'published');self.assertEqual(row['ingestion_state'],'completed')
        self.assertIsNone(row['published']);self.assertEqual(row['rights_state'],'blocked')

    def test_actual_image_header_get_head_and_out_of_range_do_not_leak_counts(self):
        h,client=self.preview_fixture()
        with write_transaction(h.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('packs','w','packs','imagepack','Packs','test')"))
        revision,lease=h.publish(archive_bytes([('1.png',image_bytes()),('2.png',image_bytes(size=(4,3)))]),filename='Book.cbz',edition='packs')
        path=f'/api/v1/items/w/pages/2?edition_id=packs&revision_id={revision}&asset_position=0'
        for method in ('GET','HEAD'):
            response=client.request(method,path)
            self.assertEqual(response.status_code,200,response.text)
            self.assertEqual(response.headers['x-preview-page-count'],'2')
            self.assertEqual(response.headers['x-preview-page'],'2')
            if method=='HEAD':self.assertEqual(response.content,b'')
            else:self.assertTrue(response.content.startswith(b'\x89PNG'))
        response=client.get(path.replace('/pages/2','/pages/3'))
        self.error(response,400);self.assertNotIn('x-preview-page-count',response.headers)
        response=client.get(path,headers={'Cookie':''})
        self.error(response,401);self.assertNotIn('x-preview-page-count',response.headers)
        manifest=h.manifest(lease)
        cache=h.h.path/manifest['bundle_relpath']
        raw=bytearray(cache.read_bytes());raw[10]^=1;cache.write_bytes(raw)
        # Corrupt/unverified cache, then the same cache while repair is queued.
        for _ in range(2):
            response=client.get(path)
            self.error(response,409)
            self.assertEqual(response.json()['error']['details'][0]['state'],'repair_queued')
            self.assertNotIn('x-preview-page-count',response.headers)
            self.assertNotIn('x-preview-page',response.headers)
        h.damage(lease)
        response=client.get(path)
        self.error(response,409)
        self.assertNotIn('x-preview-page-count',response.headers)
        self.assertNotIn('x-preview-page',response.headers)

    def test_actual_ingestion_stages_and_decisions_do_not_expose_job_material(self):
        batch=self.batch();ids=self.files(batch,6,status='staged',received=10)
        samples=[('ingest.review_upload','queued','review','review_pending',None),
            ('ingest.validation_review','queued','allowed','validation_review',None),
            ('ingest_upload','failed','allowed','failed',None),
            ('ingest_upload','running','allowed','running',None),
            ('ingest_upload','completed','blocked','completed','append_bundle'),
            ('backup_revision','completed','allowed','unavailable',None)]
        with write_transaction(self.engine) as conn:
            for ident,(kind,state,rights,_stage,decision) in zip(ids,samples):
                job=str(uuid.uuid4());tus=uuid.uuid4().hex
                conn.execute(text('UPDATE upload_sessions SET tus_id=:tus WHERE id=:id'),dict(tus=tus,id=ident))
                conn.execute(text('''INSERT INTO jobs(id,kind,work_id,upload_session_id,payload_json,payload_digest,idempotency_key,
                    priority,state,max_attempts,next_run_at,lease_generation,lease_owner,lease_until,result_json,error_code,created_at,updated_at)
                    VALUES(:id,:kind,'w',:upload,:payload,:digest,:id,10,:state,3,0,1,:owner,:expiry,:result,:error,:now,:now)'''),
                    dict(id=job,kind=kind,upload=ident,payload=json.dumps({'path':'/private/internal-secret'}),digest='0'*64,state=state,
                        owner='internal-secret' if state=='running' else None,expiry=self.h.clock.wall+100000 if state=='running' else None,
                        result=json.dumps({'revision_id':'not-an-existing-revision','reconciliation':{'outcome':decision},'receipt':'internal-secret'}),
                        error='/private/internal-secret',now=self.h.clock.wall))
                conn.execute(text('''INSERT INTO ingestion_inputs(job_id,edition_id,expected_revision_id,upload_id,filename,byte_size,
                    tus_id,rights_status,rights_receipt,decision_actor,request_digest)
                    VALUES(:job,'e',NULL,:upload,'Book.txt',10,:tus,:rights,'internal-secret','u',:digest)'''),
                    dict(job=job,upload=ident,tus=tus,rights=rights,digest='0'*64))
        response=self.get(batch,params={'limit':'100'})
        self.assertEqual(response.status_code,200,response.text)
        self.assertNotIn('internal-secret',response.text);self.assertNotIn('/private/',response.text)
        for row,sample in zip(response.json()['files'],samples):
            self.assertEqual(row['ingestion_state'],sample[3])
            self.assertEqual(row['job_state'],sample[1])
            self.assertEqual(row['decision'],sample[4])
            self.assertIsNone(row['published'])

    def test_legacy_unreserved_file_is_counted_and_wrong_owner_membership_fails_closed(self):
        batch=self.batch();self.user()
        # Genuine legacy session without an upload_reservations row is included.
        ident=str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO upload_sessions VALUES(:id,:batch,'u',NULL,1,10,10,:expiry,'published')"),
                dict(id=ident,batch=batch,expiry=self.auth.now()+1000))
        response=self.get(batch)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json()['file_count'],1)
        self.assertIsNone(response.json()['files'][0]['display_filename'])
        # Corruption probe only in this disposable fixture; restore FK mode.
        with self.engine.connect() as conn:
            db=conn.connection.driver_connection
            db.autocommit=True
            try:
                db.execute('PRAGMA foreign_keys=OFF')
                db.execute("INSERT INTO upload_sessions VALUES(?,?,'v',NULL,1,10,0,?,'receiving')",
                    (str(uuid.uuid4()),batch,self.auth.now()+1000))
            finally:
                db.execute('PRAGMA foreign_keys=ON')
                db.autocommit=False
        self.error(self.get(batch),409)

    def test_known_outcome_from_unsupported_job_does_not_enter_file_or_whole_counts(self):
        self.test_actual_ingestion_stages_and_decisions_do_not_expose_job_material()
        upload=self.scalar("SELECT upload_session_id FROM jobs WHERE kind='backup_revision'")
        batch=self.scalar('SELECT batch_id FROM upload_sessions WHERE id=:id',id=upload)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE jobs SET result_json=json_set(result_json,'$.reconciliation.outcome','initial') WHERE kind='backup_revision'"))
        response=self.get(batch,params={'limit':'100'})
        self.assertEqual(response.status_code,200,response.text)
        row=next(item for item in response.json()['files'] if item['upload_id']==upload)
        self.assertEqual(row['ingestion_state'],'unavailable')
        self.assertIsNone(row['decision'])
        self.assertNotIn('initial',response.json()['decision_counts'])

    def test_unsupported_job_result_does_not_create_link_or_published_history(self):
        h,client=self.preview_fixture();revision,lease=h.publish()
        with h.engine.connect() as conn:
            upload=conn.execute(text('SELECT upload_session_id FROM jobs WHERE id=:id'),dict(id=lease.job_id)).scalar_one()
            batch=conn.execute(text('SELECT batch_id FROM upload_sessions WHERE id=:id'),dict(id=upload)).scalar_one()
        path='/api/v1/upload-batches/'+batch
        control=client.get(path)
        self.assertEqual(control.status_code,200,control.text)
        self.assertEqual(control.json()['files'][0]['published'],dict(work_id='w',edition_id='e',revision_id=revision))
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE jobs SET kind='backup_revision' WHERE id=:id"),dict(id=lease.job_id))
        response=client.get(path)
        self.assertEqual(response.status_code,200,response.text)
        row=response.json()['files'][0]
        self.assertEqual(row['ingestion_state'],'unavailable')
        # Retain the actual upload's independent publication history, while the
        # malformed job's successful-looking result supplies no readable link.
        self.assertEqual(row['publication_state'],'published')
        self.assertIsNone(row['published'])
        self.assertIsNone(row['decision'])
        self.assertNotIn('initial',response.json()['decision_counts'])
        with write_transaction(h.engine) as conn:
            conn.execute(text("UPDATE upload_sessions SET status='staged' WHERE id=:id"),dict(id=upload))
        response=client.get(path)
        self.assertEqual(response.status_code,200,response.text)
        row=response.json()['files'][0]
        self.assertEqual(row['ingestion_state'],'unavailable')
        self.assertEqual(row['publication_state'],'not_published')
        self.assertIsNone(row['published'])
        self.assertIsNone(row['decision'])
