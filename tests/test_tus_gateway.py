"""Synthetic HTTP boundary/authority checks, explicitly not real tusd evidence."""
import asyncio
import base64
import json
import select
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import text

from document_library.api import create_app, SESSION_COOKIE
from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.permissions import PermissionService
from document_library.tus_gateway import BackendResponse, OwnedTusd, TusBackend, TusGateway
from document_library.uploads import CHUNK_LIMIT
from upload_helpers import ROOT, UploadFixture


class FakeBackend(TusBackend):
    def __init__(self):
        super().__init__(65531)
        self.files={};self.calls=[];self.fail=False;self.block=None
        self.entered=threading.Event();self.finished=threading.Event();self.before_reply=None

    def request(self, method, tus_id, headers, body):
        self.calls.append((method,tus_id,dict(headers),body));self.entered.set()
        if self.block is not None:self.block.wait(10)
        response=[('Tus-Resumable','1.0.0')]
        status=204
        if method=='POST':
            tus_id=uuid.uuid4().hex
            self.files[tus_id]=dict(data=b'',length=int(headers['Upload-Length']),metadata=headers['Upload-Metadata'])
            response.append(('Location',self.origin+'/files/'+tus_id));status=201
        elif method=='HEAD':
            item=self.files[tus_id]
            response.extend([('Upload-Offset',str(len(item['data']))),('Upload-Length',str(item['length']))]);status=200
        elif method=='PATCH':
            item=self.files[tus_id]
            if int(headers['Upload-Offset'])!=len(item['data']):return BackendResponse(409,tuple(response))
            item['data']+=body
            response.append(('Upload-Offset',str(len(item['data']))))
        elif method=='DELETE':
            del self.files[tus_id]
        if self.before_reply:self.before_reply()
        self.finished.set()
        if self.fail:raise OSError('synthetic connection lost after backend may have written')
        return BackendResponse(status,tuple(response))


class TusGatewayAcceptance(UploadFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.backend=FakeBackend();self.gateway=TusGateway(self.uploads,self.backend)
        self.app=create_app(self.auth,origin='https://library.test',uploads=self.uploads,tus=self.gateway)
        self.client=TestClient(self.app,base_url='https://library.test')
        self.addCleanup(self.client.close)
        self.upload=self.reserve()['upload_id']

    def request(self, method, *, upload=None, actor=None, csrf=None, headers=None, content=b''):
        actor=actor or self.uploader
        base={'Origin':'https://library.test','Tus-Resumable':'1.0.0','X-CSRF-Token':actor.csrf if csrf is None else csrf,
              'Cookie':SESSION_COOKIE+'='+actor.token}
        base.update(headers or {})
        return self.client.request(method,'/tus/'+(upload or self.upload),headers=base,content=content)

    def created(self):
        response=self.request('POST',headers={'Upload-Length':'3'})
        self.assertEqual(response.status_code,201,response.text)
        self.assertEqual(response.headers['location'],'/tus/'+self.upload)
        return self.scalar('SELECT tus_id FROM upload_sessions WHERE id=:id',id=self.upload)

    def patch(self, body=b'a', offset=0, **kw):
        return self.request('PATCH',headers={'Content-Type':'application/offset+octet-stream','Upload-Offset':str(offset)},content=body,**kw)

    def test_post_patch_head_resume_and_delete_use_current_owner(self):
        tus_id=self.created()
        self.assertEqual(self.request('POST',headers={'Upload-Length':'3'}).status_code,201)
        self.assertEqual(len(self.backend.calls),1)
        self.assertEqual(self.patch().status_code,204)
        resumed=self.request('HEAD');self.assertEqual(resumed.status_code,200)
        self.assertEqual(resumed.headers['upload-offset'],'1')
        self.assertEqual(self.patch(b'z',offset=0).status_code,409)
        self.assertEqual(self.backend.files[tus_id]['data'],b'a')
        self.assertEqual(self.patch(b'bc',offset=1).status_code,204)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id',id=self.upload),'staged')
        with self.engine.connect() as conn:PermissionService(self.auth).check_upload_in(conn,self.upload)
        self.assertEqual(self.request('DELETE',actor=self.admin).status_code,403)
        self.assertEqual(self.request('DELETE').status_code,204)
        self.assertNotIn(tus_id,self.backend.files)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id',id=self.upload),'expired')

    def test_json_batch_and_file_apis_keep_json_limit(self):
        headers={'Origin':'https://library.test','X-CSRF-Token':self.admin.csrf,'Cookie':SESSION_COOKIE+'='+self.admin.token}
        response=self.client.post('/uploads/batches',headers=headers,json={'declared_total_bytes':3,'request_key':'api'})
        self.assertEqual(response.status_code,201)
        batch=response.json()['batch_id']
        response=self.client.post('/uploads/batches/'+batch+'/files',headers=headers,
                                  json={'filename':'Book.txt','declared_size':3,'request_key':'file'})
        self.assertEqual(response.status_code,201)
        self.assertEqual(self.client.post('/uploads/batches',headers=headers,content=b'x'*8193).status_code,415)
        self.assertEqual(self.client.post('/uploads/batches',headers={**headers,'Content-Type':'application/json'},content=b'x'*8193).status_code,413)

    def test_every_tus_method_checks_missing_wrong_csrf_owner_and_member(self):
        self.created()
        for method in ('HEAD','PATCH','DELETE','POST'):
            for actor,csrf in ((self.uploader,''),(self.uploader,'wrong'),(self.admin,self.admin.csrf),(self.member,self.member.csrf)):
                with self.subTest(method=method,actor=actor.id,csrf=csrf):
                    response=self.request(method,actor=actor,csrf=csrf)
                    self.assertEqual(response.status_code,403)
        self.assertEqual(len(self.backend.calls),1)

    def test_revoke_between_chunks_and_during_final_response_prevents_staging(self):
        tus_id=self.created();self.assertEqual(self.patch().status_code,204)
        self.revoke()
        self.assertEqual(self.patch(b'bc',offset=1).status_code,403)
        self.assertEqual(self.request('HEAD').status_code,403)
        self.assertEqual(self.request('DELETE').status_code,403)
        self.assertEqual(self.backend.files[tus_id]['data'],b'a')
        actor=self.actor('racing-uploader',upload=True)
        upload=self.reserve(actor)['upload_id']
        self.assertEqual(self.request('POST',actor=actor,upload=upload,headers={'Upload-Length':'3'}).status_code,201)
        self.backend.before_reply=lambda:self.revoke(actor)
        result=self.patch(b'abc',actor=actor,upload=upload)
        self.assertEqual(result.status_code,403)
        self.assertEqual(self.scalar('SELECT received_size FROM upload_sessions WHERE id=:id',id=upload),3)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id',id=upload),'receiving')
        with self.engine.connect() as conn,self.assertRaises(AuthError):
            PermissionService(self.auth).check_upload_in(conn,upload)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state IN ('reserved','sent','uncertain')"),0)

    def test_full_received_then_revoke_fails_final_publication_check(self):
        self.created();self.assertEqual(self.patch(b'abc').status_code,204)
        self.revoke()
        with self.engine.connect() as conn,self.assertRaises(AuthError):
            PermissionService(self.auth).check_upload_in(conn,self.upload)

    def test_only_sanitized_headers_metadata_and_application_location_escape(self):
        self.created()
        method,tus_id,headers,body=self.backend.calls[0]
        self.assertEqual(set(headers),{'Tus-Resumable','Content-Length','Upload-Length','Upload-Metadata'})
        field,encoded=headers['Upload-Metadata'].split(' ')
        self.assertEqual((field,base64.b64decode(encoded).decode()),('upload_id',self.upload))
        self.assertNotIn(self.uploader.token,str(headers));self.assertNotIn(self.uploader.csrf,str(headers))
        for url in ('http://evil.test/files/'+'a'*32,'http://127.0.0.1:1/files/'+'a'*32,
                    '/files/../x','/files/'+'a'*32+'?x=1','http://[bad','/files/'+'a'*32+'/extra'):
            with self.subTest(url=url),self.assertRaises(AuthError):self.backend.location_id(url)

    def test_defer_concat_override_duplicate_header_and_unknown_paths_rejected(self):
        for key,value in [('Upload-Defer-Length','1'),('Upload-Concat','final;/x'),('Upload-Metadata','filename abc'),
                          ('X-HTTP-Method-Override','DELETE'),('Forwarded','host=evil'),('Authorization','synthetic')]:
            with self.subTest(key=key):
                self.assertEqual(self.request('POST',headers={'Upload-Length':'3',key:value}).status_code,400)
        self.assertEqual(self.request('POST',headers={'Upload-Length':'0'}).status_code,400)
        self.assertEqual(self.request('POST').status_code,400)
        duplicate=[('origin','https://library.test'),('tus-resumable','1.0.0'),('tus-resumable','1.0.0'),
                   ('x-csrf-token',self.uploader.csrf),('cookie',SESSION_COOKIE+'='+self.uploader.token)]
        self.assertEqual(self.client.request('HEAD','/tus/'+self.upload,headers=duplicate).status_code,400)
        self.assertEqual(self.request('GET').status_code,405)
        self.assertEqual(self.client.post('/tus',headers={'Origin':'https://library.test'}).status_code,415)
        plain=TestClient(create_app(self.auth,origin='https://library.test'),base_url='https://library.test')
        try:self.assertEqual(plain.post('/tus/'+self.upload,headers={'Origin':'https://library.test'},content=b'x').status_code,415)
        finally:plain.close()
        self.assertEqual(self.backend.calls,[])

    def test_invalid_chunk_length_offset_and_content_type_never_reach_backend(self):
        self.created()
        requests=[({'Content-Type':'text/plain','Upload-Offset':'0'},b'a',400),
                  ({'Content-Type':'application/offset+octet-stream','Upload-Offset':'-1'},b'a',400),
                  ({'Content-Type':'application/offset+octet-stream','Upload-Offset':'0','Content-Length':str(CHUNK_LIMIT+1)},b'a',413),
                  ({'Content-Type':'application/offset+octet-stream','Upload-Offset':'0','Content-Length':'2'},b'a',400),
                  ({'Content-Type':'application/offset+octet-stream','Upload-Offset':'0'},b'abcd',413)]
        for headers,body,status in requests:
            with self.subTest(headers=headers):self.assertEqual(self.request('PATCH',headers=headers,content=body).status_code,status)
        self.assertEqual(len(self.backend.calls),1)

    def test_network_loss_preserves_uncertain_slot_and_no_creation_retry(self):
        self.backend.fail=True
        self.assertEqual(self.request('POST',headers={'Upload-Length':'3'}).status_code,503)
        self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops'),'uncertain')
        self.assertEqual(self.request('POST',headers={'Upload-Length':'3'}).status_code,423)
        self.assertEqual(len(self.backend.files),1)
        with self.assertRaises(AuthError):self.gateway.confirm_stopped(object())
        self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops'),'uncertain')

    def test_idempotent_post_rechecks_authority_after_waiting_for_body(self):
        self.created()
        async def exercise():
            entered=asyncio.Event();release=asyncio.Event()
            class Request:
                method='POST'
                url=type('URL',(),{'query':''})()
                scope={'headers':[(b'tus-resumable',b'1.0.0'),(b'upload-length',b'3')]}
                async def stream(inner):
                    entered.set();await release.wait();yield b''
            replay=asyncio.create_task(self.gateway.handle(Request(),self.upload,self.uploader.token,self.uploader.csrf))
            await entered.wait()
            self.revoke()
            release.set()
            with self.assertRaises(AuthError) as rejected:
                await replay
            self.assertEqual(rejected.exception.status,403)
        asyncio.run(exercise())
        self.assertEqual(len(self.backend.calls),1)
        self.assertEqual(len(self.backend.files),1)

    def test_task_cancellation_retains_slot_until_actual_thread_response(self):
        self.created();self.backend.entered.clear();self.backend.finished.clear();self.backend.block=threading.Event()
        async def exercise():
            task=asyncio.create_task(self.gateway.transfer(self.uploader.token,self.uploader.csrf,self.upload,
                                                          method='PATCH',body=b'a',offset=0))
            try:
                while not self.backend.entered.is_set():await asyncio.sleep(0)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
                self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='uncertain'"),1)
                with self.assertRaises(AuthError):
                    self.uploads.begin(self.uploader.token,self.uploader.csrf,self.upload,method='HEAD',backend_id=self.backend.identity)
            finally:self.backend.block.set()
            async def completed():
                while self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='uncertain'"):
                    await asyncio.sleep(0.001)
            await asyncio.wait_for(completed(),5)
        asyncio.run(exercise())
        self.assertEqual(self.scalar('SELECT received_size FROM upload_sessions WHERE id=:id',id=self.upload),1)

    def test_slots_are_reserved_before_body_and_pre_send_cancel_is_safe(self):
        second=self.reserve()['upload_id'];third=self.reserve(self.admin)['upload_id']
        async def exercise():
            release=asyncio.Event();entered=[]
            class Request:
                method='POST'
                url=type('URL',(),{'query':''})()
                scope={'headers':[(b'tus-resumable',b'1.0.0'),(b'upload-length',b'3')]}
                async def stream(inner):
                    entered.append(True);await release.wait();yield b''
            one=asyncio.create_task(self.gateway.handle(Request(),self.upload,self.uploader.token,self.uploader.csrf))
            two=asyncio.create_task(self.gateway.handle(Request(),second,self.uploader.token,self.uploader.csrf))
            try:
                while len(entered)<2:await asyncio.sleep(0)
                self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),2)
                with self.assertRaises(AuthError) as raised:
                    await self.gateway.handle(Request(),third,self.admin.token,self.admin.csrf)
                self.assertEqual(raised.exception.code,'system_upload_limit')
                self.assertEqual(len(entered),2)
            finally:
                one.cancel();two.cancel()
                await asyncio.gather(one,two,return_exceptions=True)
        asyncio.run(exercise())
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='stopped'"),2)
        self.assertEqual(self.backend.calls,[])


    @staticmethod
    def stopped_controller(backend):
        # Only process ownership is synthetic; ingress flock tests use the
        # actual OS and the same isolated SQLite inode as the gateway.
        controller=object.__new__(OwnedTusd)
        controller.backend=backend;controller.dirfd=None;controller.lockfd=None
        controller.stopped_evidence=None;controller.stops={}
        class Process:
            returncode=None
            def poll(inner):return inner.returncode
            def terminate(inner):inner.returncode=0
            def wait(inner,timeout):return inner.returncode
        controller.process=Process()
        return controller

    def test_backend_exit_retains_live_ingress_slots_and_fences_old_admission(self):
        self.created()
        second=self.reserve()['upload_id']
        self.assertEqual(self.request('POST',upload=second,headers={'Upload-Length':'3'}).status_code,201)
        replacement_uploads=[self.reserve(self.admin)['upload_id'],self.reserve(self.admin)['upload_id']]
        controller=self.stopped_controller(self.backend)
        replacement=TusGateway(self.uploads,FakeBackend())
        async def exercise():
            entered=[];release=asyncio.Event()
            class Request:
                method='PATCH'
                url=type('URL',(),{'query':''})()
                scope={'headers':[(b'tus-resumable',b'1.0.0'),
                                 (b'content-type',b'application/offset+octet-stream'),
                                 (b'content-length',b'2'),(b'upload-offset',b'0')]}
                async def stream(inner):
                    entered.append(True);yield b'a'
                    await release.wait();yield b'b'
            class CreateRequest(Request):
                method='POST'
                scope={'headers':[(b'tus-resumable',b'1.0.0'),(b'upload-length',b'3')]}
            tasks=[asyncio.create_task(self.gateway.handle(Request(),upload,self.uploader.token,self.uploader.csrf))
                   for upload in (self.upload,second)]
            try:
                for _ in range(100):
                    if len(entered)==2:break
                    await asyncio.sleep(0)
                self.assertEqual(len(entered),2)
                controller.stop();self.gateway.confirm_stopped(controller)
                self.assertTrue(all(not task.done() for task in tasks))
                self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),2)
                self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved' AND stop_evidence IS NOT NULL"),2)
                for upload in replacement_uploads:
                    with self.assertRaises(AuthError) as rejected:
                        await replacement.handle(CreateRequest(),upload,self.admin.token,self.admin.csrf)
                    self.assertEqual(rejected.exception.code,'system_upload_limit')
                with self.assertRaises(AuthError) as rejected:
                    await self.gateway.handle(CreateRequest(),replacement_uploads[0],self.admin.token,self.admin.csrf)
                self.assertEqual(rejected.exception.code,'backend_stopping')
                self.assertEqual(len(entered),2)
                # A complete body still cannot be sent by the stopped generation.
                release.set()
                results=await asyncio.gather(*tasks,return_exceptions=True)
                self.assertTrue(all(isinstance(result,AuthError) and result.code=='upload_operation_stopped'
                                    for result in results))
            finally:
                for task in tasks:task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
            self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state IN ('reserved','sent','uncertain')"),0)
            self.assertEqual(len(self.backend.calls),2)
            self.assertEqual((await replacement.transfer(self.admin.token,self.admin.csrf,replacement_uploads[0],method='POST')).status_code,201)
        asyncio.run(exercise())

    def test_cancelled_ingress_keeps_guard_until_stream_cleanup_exits(self):
        self.created();controller=self.stopped_controller(self.backend)
        async def exercise():
            entered=asyncio.Event();cleaning=asyncio.Event();release=asyncio.Event()
            class Request:
                method='PATCH'
                url=type('URL',(),{'query':''})()
                scope={'headers':[(b'tus-resumable',b'1.0.0'),
                                 (b'content-type',b'application/offset+octet-stream'),
                                 (b'content-length',b'1'),(b'upload-offset',b'0')]}
                async def stream(inner):
                    entered.set()
                    try:await asyncio.Event().wait()
                    finally:
                        cleaning.set();await release.wait()
                    yield b'a'
            task=asyncio.create_task(self.gateway.handle(Request(),self.upload,self.uploader.token,self.uploader.csrf))
            try:
                await asyncio.wait_for(entered.wait(),5)
                task.cancel();await asyncio.wait_for(cleaning.wait(),5)
                controller.stop();self.gateway.confirm_stopped(controller)
                self.assertFalse(task.done())
                self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),1)
                with self.uploads.ingress_guard(reconcile=True) as quiescent:self.assertFalse(quiescent)
            finally:
                release.set()
                await asyncio.gather(task,return_exceptions=True)
            with self.uploads.ingress_guard(reconcile=True) as quiescent:self.assertTrue(quiescent)
            self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),0)
        asyncio.run(exercise())

    def child_line(self, child, *, phase, timeout):
        # Startup and the admitted operation have separate deadlines. The
        # child cannot begin the operation until the parent observes READY.
        self.assertTrue(select.select([child.stdout],[],[],timeout)[0],
                        'Child timed out during '+phase)
        line=child.stdout.readline()
        self.assertTrue(line,'Child exited before completing '+phase)
        return line.rstrip('\n')

    def test_dead_application_process_releases_guard_for_explicit_startup_recovery(self):
        second=self.reserve()['upload_id']
        script="""
import json,sys
from pathlib import Path
from document_library.auth import AuthService
from document_library.database import database_engine
from document_library.uploads import UploadService
args=json.loads(sys.stdin.readline())
engine=database_engine(Path(args['database']))
uploads=UploadService(AuthService(engine,clock=lambda:1800000000))
print('READY',flush=True)
if sys.stdin.readline().strip()!='GO':raise AssertionError('Parent did not admit ingress operation')
with uploads.ingress_guard():
    operations=[uploads.begin(args['token'],args['csrf'],ident,method='POST',backend_id=args['backend'])[0]
                for ident in args['uploads']]
    print(json.dumps(operations),flush=True)
    sys.stdin.read(1)
"""
        child=subprocess.Popen([sys.executable,'-c',script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,text=True,
                               env={'PYTHONPATH':str(ROOT/'packages/core')})
        controller=self.stopped_controller(self.backend)
        try:
            child.stdin.write(json.dumps(dict(database=str(self.path/'synthetic.sqlite3'),
                token=self.uploader.token,csrf=self.uploader.csrf,backend=self.backend.identity,
                uploads=[self.upload,second]))+'\n');child.stdin.flush()
            self.assertEqual(self.child_line(child,phase='startup imports/init',timeout=30),'READY')
            child.stdin.write('GO\n');child.stdin.flush()
            operations=json.loads(self.child_line(child,phase='ingress guard and reservations after GO',timeout=5))
            self.assertEqual(len(operations),2)
            controller.stop();self.gateway.confirm_stopped(controller)
            self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),2)
            with self.assertRaises(AuthError):self.uploads.sent(operations[0])
        finally:
            child.terminate();child.communicate(timeout=5)
            self.assertIsNotNone(child.returncode)
        # App exit alone cannot certify tusd death or free durable quota.
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"),2)
        # Backend ownership here is synthetic. The preceding ingress process
        # really exited without a handler finally block, releasing its flock.
        fresh=self.stopped_controller(FakeBackend())
        fresh.stops.update(controller.stops);fresh._check_owned=lambda:None
        replacement=TusGateway(self.uploads,fresh.backend)
        replacement.confirm_previous_stopped(fresh)
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state IN ('reserved','sent','uncertain')"),0)
        self.assertEqual(asyncio.run(replacement.transfer(self.uploader.token,self.uploader.csrf,self.upload,method='POST')).status_code,201)

    def test_unrelated_ingress_defers_orphan_recovery_until_explicit_reconciliation(self):
        second=self.reserve()['upload_id']
        old_operation,_=self.uploads.begin(self.uploader.token,self.uploader.csrf,self.upload,
                                          method='POST',backend_id=self.backend.identity)
        controller=self.stopped_controller(self.backend)
        replacement=TusGateway(self.uploads,FakeBackend())
        async def exercise():
            entered=asyncio.Event()
            class Request:
                method='POST'
                url=type('URL',(),{'query':''})()
                scope={'headers':[(b'tus-resumable',b'1.0.0'),(b'upload-length',b'3')]}
                async def stream(inner):
                    entered.set();await asyncio.Event().wait();yield b''
            task=asyncio.create_task(replacement.handle(Request(),second,self.uploader.token,self.uploader.csrf))
            try:
                await asyncio.wait_for(entered.wait(),5)
                controller.stop();self.gateway.confirm_stopped(controller)
                self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops WHERE id=:id',id=old_operation),'reserved')
            finally:
                task.cancel();await asyncio.gather(task,return_exceptions=True)
            self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops WHERE id=:id',id=old_operation),'reserved')
            # The shared guard is intentionally conservative; an explicit
            # second reconciliation is required after unrelated ingress exits.
            self.gateway.confirm_stopped(controller)
            self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops WHERE id=:id',id=old_operation),'stopped')
        asyncio.run(exercise())

    def test_ingress_rejects_changed_database_inode_and_symlink(self):
        database=self.path/'synthetic.sqlite3';original=self.path/'original.sqlite3'
        database.rename(original)
        try:
            for symlink in (False,True):
                with self.subTest(symlink=symlink):
                    if symlink:database.symlink_to(original)
                    else:database.write_bytes(b'changed database inode')
                    try:
                        with self.assertRaises(AuthError) as rejected,self.uploads.ingress_guard():pass
                        self.assertEqual(rejected.exception.code,'upload_ingress_unverified')
                    finally:database.unlink()
        finally:original.rename(database)

    def test_ingress_rejects_fifo_database_leaf_without_blocking(self):
        database=self.path/'synthetic.sqlite3';original=self.path/'fifo-original.sqlite3'
        script="""
import os,sys
from pathlib import Path
from types import SimpleNamespace
from document_library.auth import AuthError
from document_library.database import database_engine
from document_library.uploads import UploadService
database=Path(sys.argv[1]);original=database.with_name('fifo-original.sqlite3')
uploads=UploadService(SimpleNamespace(engine=database_engine(database)))
print('READY',flush=True)
if sys.stdin.readline().strip()!='GO':raise AssertionError('Parent did not admit FIFO operation')
database.rename(original);os.mkfifo(database,0o600)
try:
    with uploads.ingress_guard():raise AssertionError('FIFO was accepted')
except AuthError as error:
    if error.code!='upload_ingress_unverified':raise
    print(error.code,flush=True)
finally:
    database.unlink();original.rename(database)
"""
        child=subprocess.Popen([sys.executable,'-c',script,str(database)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,text=True,
                               env={'PYTHONPATH':str(ROOT/'packages/core')})
        try:
            self.assertEqual(self.child_line(child,phase='startup imports/init',timeout=30),'READY')
            try:output,error=child.communicate('GO\n',timeout=5)
            except subprocess.TimeoutExpired:
                child.kill();child.communicate(timeout=5)
                self.fail('FIFO guard operation did not finish after GO')
            self.assertEqual(child.returncode,0,error)
            self.assertEqual(output.strip(),'upload_ingress_unverified')
        finally:
            if child.poll() is None:child.kill();child.communicate(timeout=5)
            if database.is_fifo():database.unlink()
            if original.exists():original.rename(database)
        self.assertTrue(database.is_file());self.assertFalse(original.exists())


class LifecycleGateAcceptance(unittest.TestCase):
    def test_stop_waits_for_blocked_request_and_queued_request_never_connects(self):
        # Entirely offline: the connection and process are controlled objects.
        # This tests production request()/stop() ordering without opening a
        # socket or replaying the interrupted network reproduction.
        backend=TusBackend(65531)
        controller=object.__new__(OwnedTusd)
        controller.backend=backend;controller.dirfd=None
        controller.stopped_evidence=None;controller.stops={}
        entered=threading.Event();release=threading.Event();events=[]
        class Process:
            returncode=None
            def poll(inner):return inner.returncode
            def terminate(inner):events.append('terminate');inner.returncode=0
            def wait(inner,timeout):events.append('wait');return inner.returncode
        controller.process=Process()
        def ownership():
            self.assertIsNone(controller.process.poll())
        backend.ownership_check=ownership
        class Reply:
            status=200
            def read(inner,limit):return b''
            def getheaders(inner):return [('Tus-Resumable','1.0.0')]
        class Connection:
            def __init__(inner,*args,**kwargs):events.append('connect')
            def request(inner,*args,**kwargs):
                entered.set()
                if not release.wait(5):raise AssertionError('Controlled request was not released')
            def getresponse(inner):return Reply()
            def close(inner):events.append('close')
        with patch('document_library.tus_gateway.http.client.HTTPConnection',Connection):
            with ThreadPoolExecutor(max_workers=3) as pool:
                active=pool.submit(backend.request,'HEAD','a'*32,{},b'')
                try:
                    self.assertTrue(entered.wait(5))
                    stopped=pool.submit(controller.stop)
                    self.assertTrue(backend.stop_requested.wait(5))
                    queued=pool.submit(backend.request,'HEAD','a'*32,{},b'')
                    self.assertFalse(stopped.done())
                    self.assertIsNone(controller.stopped_evidence)
                    self.assertNotIn('terminate',events)
                finally:
                    release.set()
                self.assertEqual(active.result(timeout=5).status,200)
                stopped.result(timeout=5)
                with self.assertRaises(AuthError) as rejected:
                    queued.result(timeout=5)
                self.assertEqual(rejected.exception.code,'backend_stopping')
                with self.assertRaises(AuthError):
                    backend.request('HEAD','a'*32,{},b'')
        self.assertEqual(events.count('connect'),1)
        self.assertLess(events.index('close'),events.index('terminate'))
        self.assertLess(events.index('terminate'),events.index('wait'))
        self.assertIsNotNone(controller.stopped_evidence)


if __name__=='__main__':
    unittest.main()
