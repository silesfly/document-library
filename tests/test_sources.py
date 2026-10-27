"""Synthetic discovery transactions, lease fencing and complete history coverage."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib
import json
import tempfile
import threading
import unittest
from unittest.mock import patch

from sqlalchemy import text

from document_library.auth import AuthService, AuthError
from document_library.database import database_engine, migrate, write_transaction
from document_library.jobs import JobService, LostLease
from document_library.sources import SourceService, SourceError
from test_jobs import FakeClock
from test_auth import PASSWORD

ROOT=Path(__file__).resolve().parents[1]


def attachment(message_id, *, name='Book.txt', size=3, index=0, document_id=10):
    return dict(message_id=message_id,media_index=index,state='attached',metadata=dict(
        filename=name,byte_size=size,document_id=document_id,edited_at=0,grouped_id=None))


def missing(message_id, state='deleted'):
    return dict(message_id=message_id,media_index=-1,state=state,metadata={})


class SourceAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='document-library-source-')
        self.path=Path(self.temp.name).resolve()
        self.engine=database_engine(self.path/'synthetic.sqlite3')
        self.addCleanup(self.cleanup)
        migrate(self.engine,ROOT/'migrations')
        self.clock=FakeClock()
        self.jobs=JobService(self.engine,clock=self.clock)
        self.auth=AuthService(self.engine,clock=lambda:self.clock.wall//1000)
        self.auth.bootstrap('owner',PASSWORD)
        self.admin=self.auth.login('owner',PASSWORD,ip='synthetic')
        self.service=SourceService(self.auth,self.jobs)
        self.source=self.configure()

    def cleanup(self):
        self.engine.dispose();self.temp.cleanup()
        self.assertFalse(self.path.exists())

    def configure(self, **changes):
        args=dict(account_scope='synthetic-account',chat_id=-100,extensions=['txt','epub'])
        args.update(changes)
        return self.service.configure(self.admin.token,self.admin.csrf,**args)

    def scalar(self, sql, **params):
        with self.engine.connect() as conn:
            return conn.execute(text(sql),params).scalar_one()

    def begin(self,key,mode='incremental',upper=10):
        scan=self.service.start_scan(self.source,config_version=1,request_key=key,mode=mode,
                                     upper_bound=None if mode=='reconcile' else upper)
        lease=self.jobs.claim('source-worker',kind='source.scan',lease_ms=1000)
        self.assertEqual(lease.job_id,scan['job_id'])
        return scan,lease

    def discover(self, items=None, upper=10, key='initial'):
        scan,lease=self.begin(key,upper=upper)
        receipt=self.service.commit_page(lease,expected_cursor=scan['cursor'],next_cursor=upper,
                                         observations=items or [attachment(5)],done=True)
        self.service.finish_scan(lease)
        return receipt

    def downloaded(self, lease, data=b'abc'):
        with self.jobs.completion(lease,result={'synthetic_download':True}) as conn:
            return self.service.content_in(conn,lease,sha256=hashlib.sha256(data).hexdigest(),byte_size=len(data))

    def test_admin_current_auth_csrf_and_configuration_fencing(self):
        invite=self.auth.issue_token(self.admin.token,self.admin.csrf)
        self.auth.register(invite,'reader',PASSWORD)
        member=self.auth.login('reader',PASSWORD,ip='synthetic-member')
        for token,csrf in [(member.token,member.csrf),(self.admin.token,'wrong')]:
            with self.assertRaises(AuthError):
                self.service.configure(token,csrf,account_scope='other',chat_id=1,extensions=['txt'])
        self.assertEqual(len(self.service.list_sources(self.admin.token)),1)
        scan,lease=self.begin('config')
        self.configure(expected_version=1,enabled=False)
        with self.assertRaises(SourceError):
            self.service.commit_page(lease,expected_cursor=0,next_cursor=10,observations=[attachment(5)],done=True)
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'),0)
        with self.assertRaises(SourceError):
            self.configure(expected_version=1)
        self.assertEqual(self.service.get_scan(scan['id'])['state'],'cancelled')

    def test_page_registration_and_cursor_are_atomic_when_enqueue_fails(self):
        _scan,lease=self.begin('atomic')
        real=self.jobs.enqueue_in
        def fail_second(conn,**request):
            if request['kind']=='source.download' and conn.execute(text('SELECT count(*) FROM source_observations')).scalar_one():
                raise RuntimeError('synthetic disk registration failure')
            return real(conn,**request)
        with patch.object(self.jobs,'enqueue_in',side_effect=fail_second):
            with self.assertRaises(RuntimeError):
                self.service.commit_page(lease,expected_cursor=0,next_cursor=10,observations=[attachment(4),attachment(5)],done=True)
        for table in ('source_pages','source_messages','source_observations'):
            self.assertEqual(self.scalar('SELECT count(*) FROM '+table),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='source.download'"),0)
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'),0)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=10,observations=[attachment(4),attachment(5)],done=True)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_observations'),2)

    def test_missing_empty_or_nonstring_csrf_cannot_mutate_configuration(self):
        audits=self.scalar("SELECT count(*) FROM audit_events WHERE action='source.configure'")
        for csrf in (None,'',False,1,[],{},b'synthetic'):
            with self.subTest(csrf=csrf):
                with self.assertRaises(AuthError) as raised:
                    self.service.configure(self.admin.token,csrf,account_scope='synthetic-account',
                                           chat_id=-100,extensions=['txt'],expected_version=1,enabled=False)
                self.assertEqual((raised.exception.code,raised.exception.status),('csrf_rejected',403))
                self.assertEqual(self.scalar('SELECT config_version FROM ingestion_sources'),1)
                self.assertEqual(self.scalar('SELECT enabled FROM ingestion_sources'),1)
                self.assertEqual(self.scalar("SELECT count(*) FROM audit_events WHERE action='source.configure'"),audits)

    def test_terminal_scan_jobs_release_slot_without_losing_durable_pages(self):
        for terminal in ('cancelled','exhausted','completed'):
            with self.subTest(terminal=terminal):
                source=self.configure(chat_id={'cancelled':1,'exhausted':2,'completed':3}[terminal])
                request=dict(config_version=1,request_key='historical',mode='incremental',upper_bound=10)
                scan=self.service.start_scan(source,**request)
                lease=self.jobs.claim('scan-worker',kind='source.scan',lease_ms=1000)
                receipt=self.service.commit_page(lease,expected_cursor=0,next_cursor=5,observations=[attachment(5)])
                if terminal=='cancelled':
                    self.assertTrue(self.jobs.cancel(lease.job_id))
                elif terminal=='completed':
                    with self.jobs.completion(lease,result={'synthetic_stop':True}):
                        pass
                else:
                    for _ in range(99):
                        self.clock.advance(1000)
                        lease=self.jobs.claim('scan-worker',kind='source.scan',lease_ms=1000)
                    self.clock.advance(1000)
                    self.assertIsNone(self.jobs.claim('scan-worker',kind='source.scan'))
                    self.assertEqual(self.jobs.get(scan['job_id'])['state'],'failed')
                    self.assertEqual(self.jobs.get(scan['job_id'])['attempts'],100)
                historical=self.service.start_scan(source,**request)
                self.assertEqual((historical['id'],historical['state'],historical['cursor']),
                                 (scan['id'],'cancelled',5))
                self.assertEqual(self.service.get_scan(scan['id']),historical)
                self.assertEqual(json.loads(self.scalar('SELECT receipt_json FROM source_pages WHERE scan_id=:id',id=scan['id'])),receipt)
                self.assertEqual(self.scalar('SELECT count(*) FROM source_observations WHERE scan_id=:id',id=scan['id']),1)
                self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='source.download' AND state='queued'"),
                                 {'cancelled':1,'exhausted':2,'completed':3}[terminal])
                with self.assertRaises(LostLease):
                    self.service.commit_page(lease,expected_cursor=5,next_cursor=10,observations=[],done=True)
                resumed=self.service.start_scan(source,**dict(request,request_key='continue'))
                self.assertEqual((resumed['state'],resumed['cursor']),('active',5))
                self.assertNotEqual(resumed['id'],scan['id'])
                current=self.jobs.claim('scan-worker',kind='source.scan')
                self.assertEqual(current.job_id,resumed['job_id'])
                self.service.commit_page(current,expected_cursor=5,next_cursor=10,observations=[],done=True)
                self.service.finish_scan(current)
                self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources WHERE id=:id',id=source),10)

    def test_retry_wait_scan_keeps_active_slot(self):
        scan,lease=self.begin('retryable')
        self.jobs.fail(lease,'synthetic_retry')
        request=dict(config_version=1,mode='incremental',upper_bound=10)
        self.assertEqual(self.service.start_scan(self.source,request_key='retryable',**request)['state'],'active')
        with self.assertRaises(SourceError):
            self.service.start_scan(self.source,request_key='another',**request)
        self.clock.advance(1000)
        resumed=self.jobs.claim('retry-worker',kind='source.scan')
        self.assertEqual(resumed.job_id,scan['job_id'])
        self.service.commit_page(resumed,expected_cursor=0,next_cursor=10,observations=[],done=True)
        self.service.finish_scan(resumed)

    def test_page_replay_conflict_and_old_generation_rejected(self):
        scan,old=self.begin('resume')
        args=dict(expected_cursor=0,next_cursor=5,observations=[attachment(5)])
        first=self.service.commit_page(old,**args)
        self.assertEqual(self.service.commit_page(old,**args),first)
        with self.assertRaises(SourceError):
            self.service.commit_page(old,expected_cursor=0,next_cursor=5,observations=[attachment(4)])
        self.clock.advance(1000)
        current=self.jobs.claim('replacement',kind='source.scan')
        with self.assertRaises(LostLease):
            self.service.commit_page(old,expected_cursor=5,next_cursor=10,observations=[],done=True)
        self.assertEqual(self.service.get_scan(scan['id'])['cursor'],5)
        self.service.commit_page(current,expected_cursor=5,next_cursor=10,observations=[],done=True)
        self.service.finish_scan(current)
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'),10)

    def test_scan_request_idempotency_and_two_connection_serialization(self):
        barrier=threading.Barrier(2)
        second=database_engine(self.path/'synthetic.sqlite3')
        try:
            auth=AuthService(second,clock=lambda:self.clock.wall//1000)
            service=SourceService(auth,JobService(second,clock=self.clock))
            def start(svc):
                barrier.wait()
                return svc.start_scan(self.source,config_version=1,request_key='same',mode='incremental',upper_bound=9)
            with ThreadPoolExecutor(max_workers=2) as pool:
                results=list(pool.map(start,[self.service,service]))
            self.assertEqual(results[0]['id'],results[1]['id'])
            self.assertEqual(self.scalar('SELECT count(*) FROM source_scans'),1)
        finally:
            second.dispose()
        with self.assertRaises(SourceError):
            self.service.start_scan(self.source,config_version=1,request_key='same',mode='incremental',upper_bound=10)

    def test_whole_history_reconciliation_rejects_recent_window_and_waits_for_bytes(self):
        self.discover([attachment(1),attachment(5000)],upper=6000)
        while (lease:=self.jobs.claim('download',kind='source.download')) is not None:
            self.downloaded(lease)
        scan,lease=self.begin('full',mode='reconcile')
        page=self.service.reconciliation_page(lease)
        self.assertEqual(page['message_ids'],[1,5000])
        with self.assertRaises(SourceError):
            self.service.commit_page(lease,expected_cursor=0,next_cursor=5000,observations=[attachment(5000)],done=True)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=5000,observations=[attachment(1),attachment(5000)],done=True)
        self.service.finish_scan(lease)
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'),6000)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'),'RECONCILING')
        while (lease:=self.jobs.claim('revalidate',kind='source.download')) is not None:
            self.downloaded(lease)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'),'CURRENT')
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'),2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'),4)

    def test_reconciliation_resumes_every_page_after_process_recreation(self):
        scan,lease=self.begin('many',upper=151)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=100,
                                 observations=[missing(i,'no_media') for i in range(1,101)])
        self.service.commit_page(lease,expected_cursor=100,next_cursor=151,
                                 observations=[missing(i,'no_media') for i in range(101,152)],done=True)
        self.service.finish_scan(lease)
        scan,lease=self.begin('whole',mode='reconcile')
        self.service.commit_page(lease,expected_cursor=0,next_cursor=100,
                                 observations=[missing(i) for i in range(1,101)])
        self.clock.advance(1000)
        recreated=SourceService(self.auth,JobService(self.engine,clock=self.clock))
        current=recreated.jobs.claim('resumed',kind='source.scan')
        self.assertEqual(recreated.reconciliation_page(current)['message_ids'],list(range(101,152)))
        recreated.commit_page(current,expected_cursor=100,next_cursor=151,
                               observations=[missing(i) for i in range(101,152)],done=True)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'),'CURRENT')

    def test_gap_during_scan_and_inaccessible_never_mark_current(self):
        self.discover([missing(5,'no_media')])
        _scan,lease=self.begin('gap-pass',mode='reconcile')
        self.service.mark_event_gap(self.source,config_version=1)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=5,observations=[missing(5)],done=True)
        self.service.finish_scan(lease)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'),'RECONCILING')
        _scan,lease=self.begin('access-pass',mode='reconcile')
        self.service.commit_page(lease,expected_cursor=0,next_cursor=5,observations=[missing(5,'inaccessible')],done=True)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'),'RECONCILING')

    def test_same_metadata_new_sha_old_objects_retained_and_event_cursor_separate(self):
        self.discover()
        lease=self.jobs.claim('download',kind='source.download')
        original=self.downloaded(lease)
        _scan,lease=self.begin('edits',mode='events',upper=2)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=1,observations=[attachment(5)])
        self.service.commit_page(lease,expected_cursor=1,next_cursor=2,observations=[attachment(5)],done=True)
        self.service.finish_scan(lease)
        one=self.jobs.claim('changed',kind='source.download');changed=self.downloaded(one,b'xyz')
        two=self.jobs.claim('again',kind='source.download');same=self.downloaded(two,b'xyz')
        self.assertNotEqual(original,changed);self.assertEqual(changed,same)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'),2)
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'),10)
        self.assertEqual(self.scalar('SELECT event_cursor FROM ingestion_sources'),2)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'),0)

    def test_filters_no_secrets_and_download_configuration_fence(self):
        self.configure(expected_version=1,max_bytes=4,filename_filter='book')
        scan=self.service.start_scan(self.source,config_version=2,request_key='filter',mode='incremental',upper_bound=4)
        lease=self.jobs.claim('scan',kind='source.scan')
        bad=attachment(1);bad['metadata']['file_reference']='forbidden-synthetic-field'
        with self.assertRaises(ValueError):
            self.service.commit_page(lease,expected_cursor=0,next_cursor=4,observations=[bad],done=True)
        self.service.commit_page(lease,expected_cursor=0,next_cursor=4,
                                 observations=[attachment(1),attachment(2,size=5),attachment(3,name='other.txt'),attachment(4,name='Book.exe')],done=True)
        self.assertEqual(self.scalar("SELECT count(*) FROM source_observations WHERE admission='FILTERED'"),3)
        download=self.jobs.claim('download',kind='source.download')
        self.assertEqual(self.service.download_input(download)['config_version'],2)
        self.configure(expected_version=2,enabled=False)
        with self.assertRaises(SourceError):
            self.downloaded(download)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'),0)

    def test_content_receipt_conflicts_and_rollback_preserve_actual_digest(self):
        self.discover();lease=self.jobs.claim('download',kind='source.download')
        with self.jobs.ownership(lease) as conn:
            first=self.service.content_in(conn,lease,sha256='a'*64,byte_size=3)
            self.assertEqual(self.service.content_in(conn,lease,sha256='a'*64,byte_size=3),first)
        for sha,size in [('b'*64,3),('a'*64,4)]:
            with self.assertRaises(SourceError):
                with self.jobs.completion(lease,result={}) as conn:
                    self.service.content_in(conn,lease,sha256=sha,byte_size=size)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'),1)
        with self.assertRaises(Exception):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE source_content_versions SET sha256=:sha"),dict(sha='c'*64))
        self.assertEqual(self.scalar('SELECT sha256 FROM source_content_versions'),'a'*64)


if __name__=='__main__':
    unittest.main()
