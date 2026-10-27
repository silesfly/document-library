"""Quota races, current authority and durable request-slot ownership."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import threading
import unittest
import uuid
import sqlite3

from document_library.auth import AuthError
from document_library.database import database_engine
from document_library.uploads import BATCH_LIMIT, FILE_LIMIT, UploadService
from document_library.permissions import PermissionService
from document_library.backup import SnapshotService
from upload_helpers import UploadFixture


class UploadAcceptance(UploadFixture,unittest.TestCase):
    def test_member_uploader_admin_csrf_and_owner(self):
        with self.assertRaises(AuthError):
            self.batch(self.member)
        self.reserve(self.admin)
        upload=self.reserve()['upload_id']
        for csrf in (None,'','wrong',False,1):
            with self.subTest(csrf=csrf),self.assertRaises(AuthError):
                self.uploads.authorize(self.uploader.token,csrf,upload)
        with self.assertRaises(AuthError):
            self.uploads.authorize(self.admin.token,self.admin.csrf,upload)
        self.assertEqual(self.uploads.authorize(self.uploader.token,self.uploader.csrf,upload)['owner_id'],self.uploader.id)

    def test_batch_file_size_filename_and_extension(self):
        for total in (0,-1,True,BATCH_LIMIT+1):
            with self.subTest(total=total),self.assertRaises(AuthError):
                self.batch(total=total)
        batch=self.batch(total=FILE_LIMIT)
        for length in (0,-1,False,FILE_LIMIT+1):
            with self.subTest(length=length),self.assertRaises(AuthError):
                self.reserve(batch=batch,length=length)
        for name in ('../x.txt','/x.txt','a\\x.txt','..／x.txt','a\x00.txt','a\u202e.txt','x.exe','.txt',' x.txt','x.txt.','C:x.txt'):
            with self.subTest(name=name),self.assertRaises(AuthError):
                self.reserve(batch=batch,filename=name)
        maximum=self.reserve(batch=batch,length=FILE_LIMIT,filename='Ｂｏｏｋ.TXT')
        self.assertEqual(maximum['declared_size'],FILE_LIMIT)
        self.assertEqual(self.scalar('SELECT display_filename FROM upload_reservations'),'Book.TXT')

    def test_idempotent_reservation_and_atomic_batch_and_user_limits(self):
        batch=self.batch(total=5,key='batch')
        self.assertEqual(batch,self.batch(total=5,key='batch'))
        with self.assertRaises(AuthError):
            self.batch(total=6,key='batch')
        first=self.reserve(batch=batch,key='file')
        self.assertEqual(first,self.reserve(batch=batch,key='file'))
        with self.assertRaises(AuthError):
            self.reserve(batch=batch,key='file',filename='Other.txt')
        with self.assertRaises(AuthError):
            self.reserve(batch=batch,length=3)
        self.reserve(batch=batch,length=2)
        with self.assertRaises(AuthError) as denied:
            self.reserve()
        self.assertEqual(denied.exception.code,'uploader_limit')
        self.assertEqual(self.scalar('SELECT count(*) FROM upload_sessions'),2)

    def test_two_connection_reservations_serialize_quota(self):
        batch=self.batch(total=3);barrier=threading.Barrier(2)
        second=database_engine(self.path/'synthetic.sqlite3')
        self.addCleanup(second.dispose)
        other=UploadService(type(self.auth)(second,clock=lambda:self.now))
        def reserve(service):
            barrier.wait()
            try:
                service.reserve_file(self.uploader.token,self.uploader.csrf,batch_id=batch,
                                     filename='Book.txt',declared_size=3,request_key=str(uuid.uuid4()))
                return 'reserved'
            except AuthError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            result=list(pool.map(reserve,[self.uploads,other]))
        self.assertCountEqual(result,['reserved','batch_limit'])
        self.assertEqual(self.scalar('SELECT sum(declared_size) FROM upload_sessions'),3)

    def test_revoke_suspend_and_authz_change_block_existing_reservation(self):
        for name,status,capability in [('version','active',True),('revoke','active',False),('suspend','suspended',True)]:
            actor=self.actor(name,upload=True)
            upload=self.reserve(actor)['upload_id']
            self.revoke(actor,status=status,capability=capability)
            with self.subTest(name=name),self.assertRaises(AuthError):
                self.uploads.authorize(actor.token,actor.csrf,upload)
            # A changed authz version with retained capability may create a new
            # batch, but cannot resume the old reservation.
            if name=='version':
                self.batch(actor)
            else:
                with self.assertRaises(AuthError):
                    self.batch(actor)

    def test_expired_reservations_free_user_quota_but_keep_uncertain_transport(self):
        first=self.reserve()['upload_id'];self.reserve()
        backend=str(uuid.uuid4())
        op,_row=self.uploads.begin(self.uploader.token,self.uploader.csrf,first,method='POST',backend_id=backend)
        self.uploads.sent(op);self.uploads.interrupted(op)
        self.now+=86400
        # Use a fresh valid session; old session expiry is independently checked.
        from document_library.auth import digest
        from document_library.database import write_transaction
        from sqlalchemy import text
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE auth_sessions SET expires_at=:expiry WHERE token_hash=:token'),
                         dict(expiry=self.now+1000,token=digest(self.uploader.token)))
        self.reserve()
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_sessions WHERE status='expired'"),2)
        self.assertEqual(self.scalar("SELECT state FROM upload_transport_ops WHERE id=:id",id=op),'uncertain')

    def test_backup_redaction_preserves_transport_occupancy_and_disables_resume(self):
        from document_library.jobs import JobService
        from test_jobs import FakeClock
        JobService(self.engine,clock=FakeClock()).enqueue(kind='synthetic.upload',idempotency_key='restore-probe',payload={})
        upload=self.reserve()['upload_id']
        op,_row=self.uploads.begin(self.uploader.token,self.uploader.csrf,upload,method='POST',backend_id=str(uuid.uuid4()))
        self.uploads.sent(op);self.uploads.interrupted(op)
        with closing(sqlite3.connect(self.path/'synthetic.sqlite3')) as source,closing(sqlite3.connect(':memory:',autocommit=True)) as target:
            source.backup(target)
            SnapshotService._schema(target)
            SnapshotService._redact(target);SnapshotService._redaction_verified(target)
            self.assertEqual(target.execute('SELECT state FROM upload_transport_ops').fetchone(),('uncertain',))
            self.assertEqual(target.execute('SELECT count(*) FROM auth_sessions').fetchone(),(0,))
            self.assertEqual(target.execute("SELECT count(*) FROM users WHERE status<>'suspended'").fetchone(),(0,))
            # Existing restore authority retires unfinished sessions/batches;
            # it must not manufacture a physical backend-stop receipt.
            from document_library.restore import RestoreService
            plan=RestoreService._reconcile(target,{'objects':[],'archived_originals':[]})
            self.assertEqual(plan['revoked_uploads'],1)
            self.assertEqual(target.execute('SELECT status FROM upload_sessions').fetchone(),('revoked',))
            self.assertEqual(target.execute('SELECT status FROM upload_batches').fetchone(),('revoked',))
            self.assertEqual(target.execute('SELECT state,stop_evidence FROM upload_transport_ops').fetchone(),('uncertain',None))

    def test_three_connection_system_slots_and_same_upload_lock_are_atomic(self):
        actors=[self.uploader,self.uploader,self.admin]
        uploads=[self.reserve()['upload_id'],self.reserve()['upload_id'],self.reserve(self.admin)['upload_id']]
        engines=[database_engine(self.path/'synthetic.sqlite3') for _ in range(3)]
        for engine in engines:self.addCleanup(engine.dispose)
        services=[UploadService(type(self.auth)(engine,clock=lambda:self.now)) for engine in engines]
        barrier=threading.Barrier(3);backend=str(uuid.uuid4())
        def begin(index):
            barrier.wait()
            try:
                services[index].begin(actors[index].token,actors[index].csrf,uploads[index],method='POST',backend_id=backend)
                return 'reserved'
            except AuthError as exc:return exc.code
        with ThreadPoolExecutor(max_workers=3) as pool:result=list(pool.map(begin,range(3)))
        self.assertCountEqual(result,['reserved','reserved','system_upload_limit'])
        with self.engine.connect() as conn:
            active=conn.execute(__import__('sqlalchemy').text("SELECT upload_id FROM upload_transport_ops WHERE state='reserved' LIMIT 1")).scalar_one()
        owner=self.admin if active==uploads[2] else self.uploader
        with self.assertRaises(AuthError) as raised:
            services[0].begin(owner.token,owner.csrf,active,method='POST',backend_id=backend)
        self.assertEqual(raised.exception.code,'upload_busy')

    def test_durable_system_two_same_upload_lock_and_pre_send_cancel(self):
        uploads=[self.reserve()['upload_id'],self.reserve()['upload_id'],self.reserve(self.admin)['upload_id']]
        backend=str(uuid.uuid4())
        def begin(index,actor=None):
            actor=actor or self.uploader
            return self.uploads.begin(actor.token,actor.csrf,uploads[index],method='POST',backend_id=backend)[0]
        one=begin(0);two=begin(1)
        with self.assertRaises(AuthError) as busy:begin(0)
        self.assertEqual(busy.exception.code,'upload_busy')
        with self.assertRaises(AuthError) as busy:begin(2,self.admin)
        self.assertEqual(busy.exception.code,'system_upload_limit')
        self.uploads.interrupted(one)
        with self.assertRaises(AuthError):self.uploads.sent(one)
        three=begin(2,self.admin)
        self.uploads.sent(two);self.uploads.interrupted(two)
        recreated=UploadService(self.auth)
        with self.assertRaises(AuthError):
            recreated.begin(self.uploader.token,self.uploader.csrf,uploads[1],method='POST',backend_id=backend)
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state IN ('reserved','sent','uncertain')"),2)


if __name__=='__main__':
    unittest.main()
