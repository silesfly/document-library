"""Fixed tickets with real disposable SQLite, originals and ASGI send receipts."""
import asyncio
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import multiprocessing
from pathlib import Path
import threading
import unittest
import zipfile
from unittest.mock import patch

from sqlalchemy import event, text
from document_library.auth import AuthError, AuthService
from document_library.database import database_engine, write_transaction
from document_library.download_service import DownloadService, DownloadError
from document_library.jobs import JobService, ClockBlocked
from document_library.library_contracts import AssetSelection
from document_library.permissions import PermissionService
from document_library.publication import PublicationService, object_path
from document_library.storage import StorageRoot, StorageBusy
from document_library.streaming import StreamAdmission, AdmissionError
from document_library.transfer_owner import OWNER_DIRECTORY
from document_library.transfer_owner import stopped_owner
from library_helpers import LibraryFixture
from test_jobs import FakeClock
from test_storage import SyntheticVolume
from test_publication import option


async def transmit(response, *, fail=None, observe=None):
    messages, accepted = [], 0
    forever=asyncio.Event()
    async def receive():
        await forever.wait()
        return dict(type='http.disconnect')
    async def send(message):
        nonlocal accepted
        if fail is not None and fail(message,accepted):
            raise OSError('synthetic ASGI send failure')
        if observe is not None:
            observe(message,accepted)
        if message['type']=='http.response.body':
            accepted += len(message.get('body',b''))
        messages.append(message)
    error=None
    try:
        await response(dict(type='http',method='GET'),receive,send)
    except BaseException as caught:
        error=caught
    if response.cleanup_task is not None:
        await response.cleanup_task
    return messages,accepted,error


def claim_child(path, ticket_id, token, pipe):
    clock=FakeClock()
    with StorageRoot(Path(path),SyntheticVolume()) as root:
        engine=database_engine(Path(path)/'database/test.sqlite3')
        try:
            jobs=JobService(engine,clock=clock)
            auth=AuthService(engine,clock=lambda:clock.wall//1000)
            publisher=PublicationService(root,jobs,PermissionService(auth))
            service=DownloadService(publisher)
            initial=service._load(token,ticket_id)
            with service.owner.admitting():
                claimed=service._claim(token,initial)
            pipe.send(claimed.attempt_id)
            pipe.recv()
            service.close()
        finally:
            engine.dispose()


class DownloadServiceAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.admission=StreamAdmission(soft_limit=100000,count_fds=lambda:10)
        self.service=DownloadService(self.publisher,admission=self.admission)
        self.addCleanup(self.cleanup_service)

    def cleanup_service(self):
        # A synthetic clock fault intentionally retains ownership; reconcile it
        # only after test assertions, then settle its real retained payload.
        status=self.h.jobs.clock_status()
        if status and status['blocked']:
            self.h.jobs.reconcile_clock(expected_fence=status['fence'],reason='synthetic_test_cleanup')
        self.service.retry_pending()
        self.assertTrue(self.service.close())

    def selections(self, revision):
        with self.engine.connect() as conn:
            options=conn.execute(text('SELECT id FROM revision_options WHERE edition_revision_id=:id ORDER BY id'),dict(id=revision)).scalars().all()
        return tuple(AssetSelection(revision,option_id=ident,work_id='w',edition_id='e') for ident in options)

    def issue(self, revision):
        return self.service.issue(self.token,self.csrf,'w',self.selections(revision))

    def ticket_state(self, ticket):
        return self.scalar('SELECT state FROM download_tickets WHERE id=:id',id=ticket.ticket_id)

    def pins(self, ticket):
        return self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket' AND reference_id=:id",id=ticket.ticket_id)

    def event_row(self):
        with self.engine.connect() as conn:
            return dict(conn.execute(text('SELECT * FROM download_events')).mappings().one())

    def response(self, ticket, **kwargs):
        return self.service.ticket_response(self.token,ticket.ticket_id,**kwargs)

    def send(self, response, **kwargs):
        return asyncio.run(transmit(response,**kwargs))

    def assert_closed(self,response):
        self.assertTrue(response.handle.closed.is_set())
        self.assertFalse(response.handle.thread.is_alive())
        self.assertEqual(self.admission.usage()['producers'],0)

    def test_issue_fixed_120s_atomic_pins_digest_csrf_and_selection(self):
        revision,_lease,_asset=self.first()
        with self.assertRaises(AuthError):
            self.service.issue(self.token,'bad-csrf','w',self.selections(revision))
        for selection in ((),self.selections(revision)*2,(AssetSelection(revision,positions=(0,),work_id='w',edition_id='e'),)):
            with self.assertRaises(DownloadError):
                self.service.issue(self.token,self.csrf,'w',selection)
        ticket=self.issue(revision)
        self.assertEqual(ticket.expires_at_ms-ticket.issued_at_ms,120000)
        self.assertEqual(ticket.snapshot_digest,ticket.digest())
        self.assertEqual(self.pins(ticket),1)
        self.assertEqual(self.ticket_state(ticket),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)

    def test_subsecond_issue_immediate_head_get_and_exact_expiry(self):
        revision,_lease,_asset=self.first()
        self.h.clock.advance(123)
        ticket=self.issue(revision)
        self.assertEqual(ticket.issued_at_ms%1000,123)
        _messages,sent,error=self.send(self.response(ticket,method='HEAD'))
        self.assertIsNone(error)
        self.assertEqual(sent,0)
        self.assertEqual(self.ticket_state(ticket),'issued')
        self.h.clock.advance(119999)
        self.assertIsNone(self.send(self.response(ticket))[2])
        self.assertEqual(self.ticket_state(ticket),'completed')
        other=self.issue(revision)
        self.h.clock.advance(120000)
        for method in ('GET','HEAD'):
            with self.assertRaises(AuthError):
                self.response(other,method=method)
        self.assertTrue(self.service.expire(other.ticket_id))
        self.assertFalse(self.service.expire(other.ticket_id))
        self.assertEqual(self.pins(other),0)

    def test_head_no_consume_and_whole_zip_decodes_with_exact_audit(self):
        revision,_lease,asset=self.first()
        ticket=self.issue(revision)
        _messages,sent,error=self.send(self.response(ticket,method='HEAD'))
        self.assertIsNone(error)
        self.assertEqual(sent,0)
        self.assertEqual(self.ticket_state(ticket),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        response=self.response(ticket)
        messages,sent,error=self.send(response)
        self.assertIsNone(error)
        raw=b''.join(message.get('body',b'') for message in messages)
        self.assertEqual(len(raw),ticket.archive_bytes)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(hashlib.sha256(archive.read(archive.namelist()[0])).hexdigest(),asset['sha256'])
            self.assertIsNone(archive.testzip())
        row=self.event_row()
        self.assertEqual((row['status'],row['bytes_sent']),('completed',sent))
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_event_assets'),asset['byte_size'])
        self.assertEqual(self.pins(ticket),0)
        self.assert_closed(response)

    def test_double_get_one_cas_one_attempt_and_one_event(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        barrier=threading.Barrier(2)
        def create(_index):
            barrier.wait()
            try:
                return self.response(ticket)
            except AuthError as error:
                return error
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(create,range(2)))
        winners=[result for result in results if not isinstance(result,AuthError)]
        self.assertEqual(len(winners),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),1)
        self.assertIsNone(self.send(winners[0])[2])
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)
        with self.assertRaises(AuthError):
            self.response(ticket)

    def test_ticket_same_session_and_credential_current(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        other=self.session('synthetic-second-session')
        with self.assertRaises(AuthError):
            self.service.ticket_response(other,ticket.ticket_id)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET credential_version=credential_version+1 WHERE id='u'"))
        with self.assertRaises(AuthError):
            self.response(ticket)
        self.assertEqual(self.ticket_state(ticket),'issued')
        self.assertGreater(self.pins(ticket),0)

    def test_logout_and_suspension_before_headers_fail_without_body(self):
        revision,_lease,_asset=self.first()
        for action in ('logout','suspend'):
            with self.subTest(action=action):
                ticket=self.issue(revision)
                response=self.response(ticket)
                self.assertTrue(response.handle.prepared.wait(10))
                with write_transaction(self.engine) as conn:
                    if action=='logout':
                        conn.execute(text('DELETE FROM auth_sessions'))
                    else:
                        conn.execute(text("UPDATE users SET status='suspended'"))
                messages,sent,error=self.send(response)
                self.assertIsInstance(error,AuthError)
                self.assertEqual((messages,sent),([],0))
                self.assertEqual(self.ticket_state(ticket),'failed')
                self.assertEqual(self.pins(ticket),0)
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE users SET status='active'"))
                if action=='logout':
                    self.session(self.token)

    def test_source_revoke_before_headers_and_during_stream(self):
        revision,lease,_asset=self.publish(payload=b'x'*(512*1024))
        ticket=self.issue(revision)
        response=self.response(ticket)
        self.assertTrue(response.handle.prepared.wait(10))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=lease.job_id))
        messages,sent,error=self.send(response)
        self.assertIsInstance(error,AuthError)
        self.assertEqual((messages,sent),([],0))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed' WHERE job_id=:id"),dict(id=lease.job_id))
        other=self.issue(revision)
        changed=False
        def revoke(message,accepted):
            nonlocal changed
            if message['type']=='http.response.body' and accepted>65536 and not changed:
                changed=True
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=lease.job_id))
        _messages,sent,error=self.send(self.response(other),observe=revoke)
        self.assertTrue(changed)
        self.assertIsNotNone(error)
        self.assertLess(sent,other.archive_bytes)
        self.assertEqual(self.ticket_state(other),'failed')
        self.assertEqual(self.pins(other),0)

    def test_fixed_revision_gc_and_streaming_expiry(self):
        old,_lease,asset=self.first()
        ticket=self.issue(old)
        self.publish(payload=b'new version',previous=old)
        self.admin()
        self.service.lifecycle.release_revision(self.token,self.csrf,old,reason='synthetic_supersession')
        self.assertEqual(self.pins(ticket),1)
        response=self.response(ticket)
        self.assertTrue(response.handle.prepared.wait(10))
        with self.assertRaises(StorageBusy),self.publisher.locks.mutation([asset['sha256']],blocking=False):
            pass
        self.h.clock.advance(120000)
        self.assertFalse(self.service.expire(ticket.ticket_id))
        self.assertGreater(self.pins(ticket),0)
        messages,_sent,error=self.send(response)
        self.assertIsNone(error)
        raw=b''.join(message.get('body',b'') for message in messages)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(hashlib.sha256(archive.read(archive.namelist()[0])).hexdigest(),asset['sha256'])

    def test_missing_corrupt_member_fails_whole_ticket_before_headers(self):
        old,_lease,_asset=self.first()
        revision,_lease,_asset=self.publish(payload=b'original second',format='epub',previous=old)
        ticket=self.issue(revision)
        path=self.h.path/object_path(ticket.assets[-1].object_sha)
        original=path.read_bytes()
        for kind in ('corrupt','missing'):
            with self.subTest(kind=kind):
                current=ticket if kind=='corrupt' else self.issue(revision)
                if kind=='corrupt':
                    path.write_bytes(b'x'*len(original))
                else:
                    path.unlink()
                messages,sent,error=self.send(self.response(current))
                self.assertIsNotNone(error)
                self.assertEqual((messages,sent),([],0))
                self.assertEqual(self.ticket_state(current),'failed')
                self.assertEqual(self.pins(current),0)
                path.write_bytes(original)
                path.chmod(0o600)

    def test_cap_includes_overhead_and_explicit_individual_fallback(self):
        revision,_lease,_asset=self.first()
        limited=DownloadService(self.publisher,admission=self.admission,zip_max_bytes=98)
        self.addCleanup(limited.close)
        with self.assertRaises(DownloadError) as caught:
            limited.issue(self.token,self.csrf,'w',self.selections(revision))
        self.assertEqual(caught.exception.status,413)
        self.assertEqual(caught.exception.individual_options[0]['revision_id'],revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)

    def test_partial_final_and_header_send_failure_audit(self):
        revision,_lease,_asset=self.publish(payload=b'x'*(256*1024))
        for where in ('partial','final','headers'):
            with self.subTest(where=where):
                ticket=self.issue(revision)
                layout=self.service._layout(ticket)
                def fail(message,accepted):
                    if where=='headers':
                        return message['type']=='http.response.start'
                    return message['type']=='http.response.body' and ((where=='final' and not message['more_body']) or (where=='partial' and accepted>65536))
                response=self.response(ticket)
                _messages,sent,error=self.send(response,fail=fail)
                self.assertIsInstance(error,OSError)
                self.assertEqual(self.ticket_state(ticket),'failed')
                with self.engine.connect() as conn:
                    row=conn.execute(text('SELECT * FROM download_events WHERE ticket_id=:id'),dict(id=ticket.ticket_id)).mappings().one()
                    asset_bytes=conn.execute(text('SELECT bytes_sent FROM download_event_assets WHERE event_id=:id'),dict(id=row['id'])).scalars().all()
                self.assertEqual(row['bytes_sent'],sent)
                self.assertEqual(asset_bytes,list(layout.source_bytes(sent)))
                self.assertEqual(row['failure_code'],'TRANSPORT_INCOMPLETE')
                self.assertEqual(self.pins(ticket),0)
                self.assert_closed(response)

    def test_terminal_atomic_fault_keeps_pins_then_recovery_is_idempotent(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        response=self.response(ticket)
        def reject(_conn,_cursor,statement,_parameters,_context,_many):
            if statement.startswith('DELETE FROM object_pins'):
                raise RuntimeError('synthetic atomic terminal fault')
        event.listen(self.engine,'before_cursor_execute',reject)
        try:
            with self.assertRaisesRegex(RuntimeError,'atomic terminal fault'):
                self.send(response)
        finally:
            event.remove(self.engine,'before_cursor_execute',reject)
        self.assertEqual(self.ticket_state(ticket),'streaming')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
        self.assertEqual(self.pins(ticket),1)
        attempt=self.scalar('SELECT id FROM download_attempts')
        self.assertFalse(self.service.recover(attempt))
        result=self.service.retry_pending()
        self.assertEqual(result,dict(pending=0,retried=1,failed=0))
        self.assertFalse(self.service.recover(attempt))
        row=self.event_row()
        self.assertEqual(row['status'],'completed')
        self.assertEqual(row['bytes_sent'],ticket.archive_bytes)
        self.assertIn('successful_asgi_calls',row['stop_evidence'])
        self.assertEqual(self.pins(ticket),0)

    def test_clock_rollback_fence_survives_failed_issue_and_restart(self):
        revision,_lease,_asset=self.first()
        self.h.clock.advance(-1)
        with self.assertRaises(ClockBlocked):
            self.issue(revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)
        self.assertEqual(self.h.jobs.clock_status()['blocked'],1)
        replacement=JobService(self.engine,clock=self.h.clock)
        with self.assertRaises(ClockBlocked),replacement._writer():
            pass

    def test_invalid_clock_get_admission_persists_fence_without_claim(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        original=self.h.clock.wall
        self.h.clock.wall=True
        try:
            with self.assertRaises(ClockBlocked):
                self.response(ticket)
        finally:
            self.h.clock.wall=original
        self.assertEqual(self.h.jobs.clock_status()['blocked'],1)
        self.assertEqual(self.ticket_state(ticket),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)
        self.assertEqual(self.pins(ticket),1)

    def test_clock_rollback_at_terminal_preserves_pin(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        response=self.response(ticket)
        self.assertTrue(response.handle.prepared.wait(10))
        self.h.clock.advance(-1)
        with self.assertRaises(ClockBlocked):
            self.send(response)
        self.assertEqual(self.ticket_state(ticket),'streaming')
        self.assertGreater(self.pins(ticket),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
        self.assertEqual(self.h.jobs.clock_status()['blocked'],1)

    def test_issue_transaction_fault_rolls_back_draft_and_pins(self):
        revision,_lease,_asset=self.first()
        with patch.object(self.service.policy,'ticket_digest_in',side_effect=RuntimeError('synthetic issue fault')):
            with self.assertRaises(RuntimeError):
                self.issue(revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)

    def test_prelaunch_handle_and_registration_failure_prove_never_started(self):
        revision,_lease,_asset=self.first()
        for target in ('handle','registration'):
            with self.subTest(target=target):
                ticket=self.issue(revision)
                replacement=patch('document_library.streaming.TransferHandle',side_effect=RuntimeError('synthetic handle constructor')) if target=='handle' else patch.object(self.service.owner,'track',side_effect=RuntimeError('synthetic owner registration'))
                with replacement,self.assertRaises(RuntimeError):
                    self.response(ticket)
                self.assertEqual(self.ticket_state(ticket),'failed')
                self.assertEqual(self.pins(ticket),0)
                self.assertEqual(self.admission.usage()['producers'],0)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),2)

    def test_native_start_exception_keeps_owner_until_delayed_actual_launch_exit(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        native_start=threading.Thread.start
        launch=threading.Event()
        captured=[]
        def launch_later():
            launch.wait()
            native_start(captured[0])
        launcher=threading.Thread(target=launch_later)
        launcher.start()
        def interrupted_start(thread):
            captured.append(thread)
            raise KeyboardInterrupt('synthetic interrupted native bootstrap')
        try:
            with patch('document_library.streaming.threading.Thread.start',interrupted_start):
                with self.assertRaisesRegex(KeyboardInterrupt,'native bootstrap'):
                    self.response(ticket)
            self.assertFalse(captured[0].is_alive())
            self.assertEqual(self.ticket_state(ticket),'streaming')
            self.assertEqual(self.scalar('SELECT state FROM download_attempts'),'cancelling')
            self.assertEqual(self.pins(ticket),1)
            self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
            self.assertFalse(self.service.close(wait=False))
            with stopped_owner(self.root,self.service.owner.descriptor) as proof:
                self.assertIsNone(proof)
        finally:
            launch.set()
            launcher.join()
            self.service.close()
        self.assertEqual(self.ticket_state(ticket),'failed')
        self.assertEqual(self.pins(ticket),0)
        self.assertEqual(self.event_row()['bytes_sent'],0)
        self.assertEqual(self.event_row()['failure_code'],'PRODUCER_START_FAILED')
        self.assertEqual(self.admission.usage()['producers'],0)

    def test_response_constructor_failure_keeps_recoverable_owner_then_fails_exact_zero(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        with patch('document_library.download_service.OwnedResponse',side_effect=RuntimeError('synthetic response constructor')):
            with self.assertRaisesRegex(RuntimeError,'response constructor'):
                self.response(ticket)
        self.service.close()
        self.assertEqual(self.ticket_state(ticket),'failed')
        self.assertEqual(self.pins(ticket),0)
        self.assertEqual(self.event_row()['bytes_sent'],0)
        self.assertEqual(self.event_row()['failure_code'],'RESPONSE_CONSTRUCTION_FAILED')

    def test_recovery_preserves_durable_constructor_failure_with_unknown_bytes(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        release=threading.Event()
        original=self.publisher.read_selected
        @contextmanager
        def blocked(*args,**kwargs):
            release.wait()
            with original(*args,**kwargs) as opened:
                yield opened
        try:
            with patch.object(self.publisher,'read_selected',blocked),patch('document_library.download_service.OwnedResponse',side_effect=RuntimeError('synthetic response constructor')):
                with self.assertRaises(RuntimeError):
                    self.response(ticket)
            self.assertEqual(self.pins(ticket),1)
        finally:
            release.set()
        self.service.owner.close()
        attempt=self.scalar('SELECT id FROM download_attempts')
        self.assertTrue(self.service.recover(attempt))
        row=self.event_row()
        self.assertEqual(row['failure_code'],'RESPONSE_CONSTRUCTION_FAILED')
        self.assertIn('unknown_lower_bound',row['stop_evidence'])
        self.assertEqual(self.pins(ticket),0)

    def test_two_live_retry_calls_do_not_repeat_inflight_finalization(self):
        revision,_lease,_asset=self.publish(payload=b'x'*131072)
        ticket=self.issue(revision)
        response=self.response(ticket)
        with patch.object(self.service,'_finish',side_effect=RuntimeError('synthetic retry fault')):
            with self.assertRaises(RuntimeError):
                self.send(response)
        self.assertEqual(self.service.pending_finalizations(),1)
        entered,release=threading.Event(),threading.Event()
        original=self.service._finish
        def blocked(*args,**kwargs):
            entered.set()
            release.wait()
            return original(*args,**kwargs)
        results=[]
        with patch.object(self.service,'_finish',blocked):
            worker=threading.Thread(target=lambda:results.append(self.service.retry_pending()))
            worker.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(self.service.retry_pending(),dict(pending=1,retried=0,failed=0))
                self.assertFalse(self.service.close(wait=False))
                with stopped_owner(self.root,self.service.owner.descriptor) as proof:
                    self.assertIsNone(proof)
            finally:
                release.set()
                worker.join()
        self.assertEqual(results,[dict(pending=0,retried=1,failed=0)])
        self.assertEqual(self.event_row()['bytes_sent'],ticket.archive_bytes)
        self.assertEqual(self.event_row()['status'],'completed')
        self.assertTrue(self.service.close())

    def test_cancelled_finalizer_awaiter_keeps_actual_worker_owned(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        response=self.response(ticket)
        entered,release,finished=(threading.Event() for _ in range(3))
        original=self.service._finish
        original_run=self.service._run_pending
        def blocked(*args,**kwargs):
            entered.set()
            release.wait()
            return original(*args,**kwargs)
        def run(record):
            try:
                return original_run(record)
            finally:
                # Ignore retry/close calls that merely observe running=True.
                if release.is_set():
                    finished.set()
        async def scenario():
            task=asyncio.create_task(transmit(response))
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait,5))
                response.cleanup_task.cancel()
                await asyncio.sleep(0)
                self.assertEqual(self.service.pending_finalizations(),1)
                self.assertEqual(self.service.retry_pending()['retried'],0)
                self.assertFalse(self.service.close(wait=False))
                self.assertFalse(finished.is_set())
            finally:
                release.set()
                self.assertTrue(await asyncio.to_thread(finished.wait,5))
                await asyncio.gather(task,return_exceptions=True)
        with patch.object(self.service,'_finish',blocked),patch.object(self.service,'_run_pending',run):
            asyncio.run(scenario())
        self.assertEqual(self.service.pending_finalizations(),0)
        self.assertEqual(self.event_row()['status'],'completed')
        self.assertEqual(self.event_row()['bytes_sent'],ticket.archive_bytes)
        self.assertTrue(self.service.close())

    def test_response_final_send_is_a_barrier_after_producer_exit(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        response=self.response(ticket)
        async def scenario():
            at_final,release=asyncio.Event(),asyncio.Event()
            async def receive():
                await asyncio.Event().wait()
            async def send(message):
                if message['type']=='http.response.body' and not message['more_body']:
                    at_final.set()
                    await release.wait()
            task=asyncio.create_task(response(dict(type='http'),receive,send))
            try:
                await at_final.wait()
                self.assertTrue(response.handle.wait_exited(0))
                self.assertEqual(self.service.pending_finalizations(),1)
                self.assertFalse(self.service.close(wait=False))
                self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
            finally:
                release.set()
                await task
        asyncio.run(scenario())
        self.assertEqual(self.event_row()['status'],'completed')
        self.assertEqual(self.service.pending_finalizations(),0)
        self.assertTrue(self.service.close())

    def test_original_full_payload_retry_after_commit_error_is_idempotent(self):
        revision,_lease,_asset=self.first()
        original=self.service._original_event
        def committed_then_error(*args):
            original(*args)
            raise RuntimeError('synthetic after commit')
        selection=(AssetSelection(revision,positions=(0,),work_id='w',edition_id='e'),)
        with patch.object(self.service,'_original_event',committed_then_error):
            with self.assertRaisesRegex(RuntimeError,'after commit'):
                self.send(self.service.original_download_response(self.token,selection,range_value='bytes=1-3'))
        self.assertEqual(self.service.pending_finalizations(),1)
        self.assertEqual(self.service.retry_pending(),dict(pending=0,retried=1,failed=0))
        self.assertEqual(self.event_row()['bytes_sent'],3)
        self.assertEqual(self.event_row()['status'],'completed')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)

    def test_two_recoverers_create_one_immutable_event(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        with self.service.owner.admitting():
            claimed=self.service._claim(self.token,ticket)
        self.service.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.service.recover(claimed.attempt_id),range(2)))
        self.assertEqual(sum(results),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)
        self.assertEqual(self.pins(ticket),0)

    def test_actual_spawn_owner_death_and_missing_replaced_inode(self):
        revision,_lease,_asset=self.first()
        ticket=self.issue(revision)
        context=multiprocessing.get_context('spawn')
        parent,child=context.Pipe()
        process=context.Process(target=claim_child,args=(str(self.h.path),ticket.ticket_id,self.token,child))
        process.start()
        child.close()
        def cleanup_process():
            if process.is_alive():
                process.kill()
            process.join()
            parent.close()
        self.addCleanup(cleanup_process)
        self.assertTrue(parent.poll(15))
        attempt=parent.recv()
        self.assertFalse(self.service.recover(attempt))
        self.h.clock.advance(120000)
        self.assertFalse(self.service.expire(ticket.ticket_id))
        self.assertEqual(self.pins(ticket),1)
        process.kill()
        process.join(10)
        self.assertTrue(self.service.recover(attempt))
        self.assertEqual(self.ticket_state(ticket),'failed')
        self.assertEqual(self.event_row()['failure_code'],'OWNER_STOPPED_BYTES_UNKNOWN')
        self.assertEqual(self.pins(ticket),0)
        other=self.issue(revision)
        with self.service.owner.admitting():
            claimed=self.service._claim(self.token,other)
        descriptor=json.loads(self.scalar('SELECT owner_instance FROM download_attempts WHERE id=:id',id=claimed.attempt_id))
        self.service.close()
        lock=self.h.path/OWNER_DIRECTORY/(descriptor['uuid']+'.lock')
        import os
        with lock.open('rb') as old_lock:
            retained = os.fstat(old_lock.fileno())
            self.assertEqual((retained.st_dev,retained.st_ino),(descriptor['dev'],descriptor['ino']))
            lock.unlink()
            self.assertFalse(self.service.recover(claimed.attempt_id))
            self.assertFalse(lock.exists())
            lock.write_bytes(b'synthetic replacement')
            lock.chmod(0o600)
            replacement = lock.stat()
            self.assertNotEqual((replacement.st_dev,replacement.st_ino),(descriptor['dev'],descriptor['ino']))
        self.assertFalse(self.service.recover(claimed.attempt_id))
        self.assertEqual(self.pins(other),1)

    def test_issue_final_clock_fence_rolls_back_all_draft_rows(self):
        revision,_lease,_asset=self.first()
        def reverse(_conn,_cursor,statement,_parameters,_context,_many):
            if statement.startswith('UPDATE download_tickets SET snapshot_digest'):
                self.h.clock.advance(-1)
        event.listen(self.engine,'before_cursor_execute',reverse)
        try:
            with self.assertRaises(ClockBlocked):
                self.issue(revision)
        finally:
            event.remove(self.engine,'before_cursor_execute',reverse)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),0)
        self.assertEqual(self.h.jobs.clock_status()['blocked'],1)

    def test_individual_range_audit_and_head_no_event(self):
        revision,_lease,_asset=self.first()
        selection=(AssetSelection(revision,positions=(0,),work_id='w',edition_id='e'),)
        head=self.service.original_download_response(self.token,selection,method='HEAD',range_value='bytes=1-3')
        self.assertIsNone(self.send(head)[2])
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)
        response=self.service.original_download_response(self.token,selection,range_value='bytes=1-3')
        messages,sent,error=self.send(response)
        self.assertIsNone(error)
        self.assertEqual((messages[0]['status'],sent),(206,3))
        self.assertEqual(self.event_row()['status'],'completed')
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_event_assets'),3)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)

    def test_asgi_cancellation_waits_actual_exit_and_audits_sent_prefix(self):
        revision,_lease,_asset=self.publish(payload=b'x'*(256*1024))
        ticket=self.issue(revision)
        response=self.response(ticket)
        def cancel(message,accepted):
            if message['type']=='http.response.body' and accepted>65536:
                raise asyncio.CancelledError('synthetic caller cancellation')
            return False
        _messages,sent,error=self.send(response,fail=cancel)
        self.assertIsInstance(error,asyncio.CancelledError)
        self.assertEqual(self.event_row()['status'],'failed')
        self.assertEqual(self.event_row()['bytes_sent'],sent)
        self.assertEqual(self.event_row()['failure_code'],'TRANSPORT_INCOMPLETE')
        self.assertEqual(self.pins(ticket),0)
        self.assert_closed(response)

    def test_full_200_asset_bundle_has_one_shared_fd_and_distinct_entries(self):
        old,_lease,asset=self.first()
        lease=self.h.make_job(old)
        reused=dict(asset,prepared_relpath=None)
        coverage=dict(unit='episode',intervals=[[1,200]],gaps=[],confidence='verified')
        self.h.plan(lease,[reused]*200,[option(0),dict(format='txt',delivery_kind='bundle',positions=list(range(200)),coverage=coverage)],previous=old)
        revision=self.publisher.publish(lease)
        bundle=self.scalar("SELECT id FROM revision_options WHERE edition_revision_id=:id AND delivery_kind='bundle'",id=revision)
        ticket=self.service.issue(self.token,self.csrf,'w',(AssetSelection(revision,option_id=bundle,work_id='w',edition_id='e'),))
        observed=[]
        original=self.publisher.read_selected
        @contextmanager
        def inspect(*args,**kwargs):
            with original(*args,**kwargs) as opened:
                observed.append((len(opened._descriptors),threading.get_ident()))
                yield opened
        with patch.object(self.publisher,'read_selected',inspect):
            response=self.response(ticket)
            messages,_sent,error=self.send(response)
        self.assertIsNone(error)
        raw=b''.join(message.get('body',b'') for message in messages)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(len(archive.namelist()),200)
            self.assertIsNone(archive.testzip())
        self.assertEqual(self.scalar('SELECT count(*) FROM download_event_assets'),200)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)
        self.assertEqual(self.pins(ticket),0)
        self.assert_closed(response)
        self.assertEqual(observed,[(1,response.handle.ack.owner_thread)])

    def test_shared_sha_bundle_members_remain_distinct(self):
        old,_lease,asset=self.first()
        lease=self.h.make_job(old)
        reused=dict(asset,prepared_relpath=None)
        coverage=dict(unit='episode',intervals=[[1,2]],gaps=[],confidence='verified')
        self.h.plan(lease,[reused,reused],[option(0),dict(format='txt',delivery_kind='bundle',positions=[0,1],coverage=coverage)],previous=old)
        revision=self.publisher.publish(lease)
        bundle=self.scalar("SELECT id FROM revision_options WHERE edition_revision_id=:id AND delivery_kind='bundle'",id=revision)
        ticket=self.service.issue(self.token,self.csrf,'w',(AssetSelection(revision,option_id=bundle,work_id='w',edition_id='e'),))
        self.assertEqual(len(ticket.assets),2)
        self.assertEqual(self.pins(ticket),1)
        messages,_sent,error=self.send(self.response(ticket))
        self.assertIsNone(error)
        with zipfile.ZipFile(io.BytesIO(b''.join(message.get('body',b'') for message in messages))) as archive:
            self.assertEqual(len(archive.namelist()),2)
            self.assertEqual(archive.read(archive.namelist()[0]),archive.read(archive.namelist()[1]))
        self.assertEqual(self.scalar('SELECT count(*) FROM download_event_assets'),2)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),1)


    def test_configured_quota_issue_get_head_fail_send_and_finalization_retry_counts(self):
        from document_library.request_quota import AccountQuota
        quota = AccountQuota(self.auth)
        self.service.quota = quota
        revision,_,_ = self.first()
        selection = (AssetSelection(revision,positions=(0,),work_id='w',edition_id='e'),)
        original = self.service._original_event
        def committed_then_error(*args):
            original(*args)
            raise RuntimeError('synthetic configured after commit')
        with patch.object(quota,'admit',wraps=quota.admit) as charged:
            ticket = self.issue(revision)
            self.assertEqual(charged.call_count,1)
            self.assertEqual(charged.call_args.kwargs,dict(action='download',csrf=self.csrf))
            self.assertIsNone(self.send(self.response(ticket,method='HEAD'))[2])
            self.assertIsNone(self.send(self.service.original_download_response(self.token,selection,method='HEAD'))[2])
            self.assertEqual(charged.call_count,1)
            _,_,error = self.send(self.response(ticket),fail=lambda message,accepted:message['type']=='http.response.start')
            self.assertIsInstance(error,OSError)
            self.assertEqual(charged.call_count,2)
            with patch.object(self.service,'_original_event',committed_then_error):
                with self.assertRaisesRegex(RuntimeError,'configured after commit'):
                    self.send(self.service.original_download_response(self.token,selection,range_value='bytes=1-3'))
            self.assertEqual(charged.call_count,3)
            self.assertEqual(self.service.pending_finalizations(),1)
            self.assertEqual(self.service.retry_pending(),dict(pending=0,retried=1,failed=0))
            self.assertEqual(charged.call_count,3)
        self.assertEqual(self.scalar("SELECT attempts FROM auth_limits WHERE bucket LIKE 'g3:download:%'"),3)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),2)
        self.assertEqual(self.pins(ticket),0)

    def test_configured_download_validation_zero_and_downstream_failure_keeps_admission(self):
        from document_library.request_quota import AccountQuota
        quota = AccountQuota(self.auth)
        self.service.quota = quota
        revision,_,_ = self.first()
        selection = self.selections(revision)
        with patch.object(quota,'admit',wraps=quota.admit) as charged:
            for invoke in (lambda:self.service.ticket_response(self.token,'missing',method='POST'),
                    lambda:self.service.original_download_response(self.token,selection,method='POST'),
                    lambda:self.service.issue(self.token,'','w',selection),
                    lambda:self.service.issue(self.token,self.csrf,'w',())):
                with self.assertRaises(AuthError):
                    invoke()
            charged.assert_not_called()
            with patch.object(self.service,'_load',side_effect=DownloadError('TICKET_UNAVAILABLE',409)):
                with self.assertRaises(DownloadError):
                    self.service.ticket_response(self.token,'missing')
            self.assertEqual(charged.call_count,1)
            with patch.object(self.service,'_resolve',side_effect=AuthError('authentication_required')):
                with self.assertRaises(AuthError):
                    self.service.original_download_response(self.token,selection)
            self.assertEqual(charged.call_count,2)
            with patch.object(self.service,'_entries',side_effect=OSError('synthetic post-admission layout failure')):
                with self.assertRaises(OSError):
                    self.issue(revision)
            self.assertEqual(charged.call_count,3)
        self.assertEqual(self.scalar("SELECT attempts FROM auth_limits WHERE bucket LIKE 'g3:download:%'"),3)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),0)


if __name__=='__main__':
    unittest.main()
