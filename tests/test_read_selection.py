"""Authoritative opener races, logical aliases, owning threads and actual close."""
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import os
import threading
import unittest
from unittest.mock import patch

from sqlalchemy import text
from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.library_contracts import AssetSelection
from document_library.publication import PublicationError,object_path
from document_library.storage import StorageBusy
from document_library.streaming import OwnedProducer,OwnedResponse,PreparedTransfer,StreamAdmission,_fd_count
from library_helpers import LibraryFixture
from test_streaming import run_response
from test_publication import option


class SelectedReadAcceptance(LibraryFixture):
    def test_enter_body_exit_on_one_thread_and_parallel_readers(self):
        revision,_,_ = self.first()
        fresh = lambda:self.resolve(revision)
        events,release = [],threading.Event()
        @contextmanager
        def opened():
            with self.publisher.read_selected(fresh) as selected:
                events.append(('enter',threading.get_ident()))
                try:
                    yield selected
                finally:
                    events.append(('exit',threading.get_ident()))
        def prepare(selected):
            events.append(('body',threading.get_ident()))
            return PreparedTransfer(200,())
        gate = StreamAdmission(max_producers=2)
        handles = [OwnedProducer.start(open_selected=opened,prepare=prepare,produce=lambda *_:iter(()),
                   unique_originals=1,admission=gate) for _ in range(2)]
        for handle in handles:
            self.assertTrue(handle.prepared.wait(3))
        owner_ids = {i for stage,i in events if stage == 'enter'}
        self.assertEqual(len(owner_ids),2)
        for handle in handles:
            run_response(OwnedResponse(handle))
        for owner in owner_ids:
            self.assertEqual([stage for stage,i in events if i == owner],['enter','body','exit'])

    def test_revision_change_before_opener_and_retained_modes(self):
        revision,_,_ = self.first();expected = self.resolve(revision)
        newer,_,_ = self.publish(previous=revision)
        with self.assertRaises(AuthError):
            with self.publisher.read_selected(lambda:self.resolve(revision),expected=expected):
                self.fail('superseded current selection opened')
        with self.assertRaises(AuthError):
            self.resolve(revision,policy='admin_retained')
        self.admin()
        with self.publisher.read_selected(lambda:self.resolve(revision,policy='admin_retained')) as opened:
            self.assertEqual(opened.snapshot.assets[0].revision_id,revision)

    def test_revocation_during_hash_uses_fresh_transaction(self):
        revision,_,_ = self.first();original = self.publisher._hash_fd
        def revoke(fd,asset):
            value = original(fd,asset)
            with write_transaction(self.engine) as conn:
                conn.execute(text('UPDATE users SET credential_version=credential_version+1'))
            return value
        with patch.object(self.publisher,'_hash_fd',side_effect=revoke),self.assertRaises(AuthError):
            with self.publisher.read_selected(lambda:self.resolve(revision)):
                self.fail('revoked credential passed post-hash checkpoint')

    def test_revoke_after_prepared_before_proceed_no_headers(self):
        revision,_,_ = self.first()
        handle = OwnedProducer.start(open_selected=lambda:self.publisher.read_selected(lambda:self.resolve(revision)),
            prepare=lambda _:PreparedTransfer(200,()),produce=lambda *_:iter([b'private']),unique_originals=1)
        self.assertTrue(handle.prepared.wait(3))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked'"))
        with self.assertRaises(AuthError):
            run_response(OwnedResponse(handle))
        self.assertEqual(handle.ack.produced_bytes,0)
        self.assertFalse(handle.thread.is_alive())

    def test_frozen_expected_associations_must_match_actual_resolver(self):
        revision,_,_ = self.first();snapshot = self.resolve(revision)
        for field,value in [('work_id','other'),('edition_id','other'),('revision_id','other'),
                            ('file_format','pdf'),('detected_mime','application/pdf'),('asset_position',99),('ordinal',99)]:
            with self.subTest(field=field),self.assertRaises(PublicationError):
                altered = replace(snapshot,assets=(replace(snapshot.assets[0],**{field:value}),))
                with self.publisher.read_selected(lambda:self.resolve(revision),expected=altered):
                    self.fail('mismatched immutable expected snapshot opened')

    def _assert_last_failure(self, corruption):
        lease = self.h.make_job()
        assets = [self.h.asset(lease,b'first '+corruption.encode()),self.h.asset(lease,b'last '+corruption.encode())]
        self.h.plan(lease,assets,[dict(option(0),delivery_kind='bundle',positions=[0,1])])
        revision = self.publisher.publish(lease)
        path = self.h.path/object_path(assets[1]['sha256'])
        if corruption == 'missing':
            path.unlink()
        else:
            path.write_bytes(b'x'*len(path.read_bytes()))
        before = _fd_count()
        with self.assertRaises((OSError,PublicationError)):
            with self.publisher.read_selected(lambda:self.resolve(revision,positions=(0,1))):
                self.fail('last failed original exposed earlier body')
        self.assertLessEqual(_fd_count(),before+1)

    def test_last_missing_original_closes_partial_fds(self):
        self._assert_last_failure('missing')

    def test_last_same_size_corrupt_original_closes_partial_fds(self):
        self._assert_last_failure('same_size')

    def test_shared_sha_preserves_logical_order_and_pread_offsets(self):
        lease = self.h.make_job();asset = self.h.asset(lease,b'0123456789')
        self.h.plan(lease,[asset,dict(asset)],[dict(option(0),delivery_kind='bundle',positions=[1,0])])
        revision = self.publisher.publish(lease)
        option_id = self.scalar('SELECT id FROM revision_options')
        with self.publisher.read_selected(lambda:self.resolve(revision,positions=None,option_id=option_id)) as selected:
            left,right = selected.snapshot.assets
            self.assertEqual((left.asset_position,right.asset_position),(1,0))
            self.assertEqual(selected.fd_for(left),selected.fd_for(right))
            first = selected.chunks(left,start=0,length=6,block_size=3)
            second = selected.chunks(right,start=7,length=3)
            self.assertEqual(next(first),b'012');self.assertEqual(next(second),b'789');self.assertEqual(next(first),b'345')

    def test_gc_busy_after_reference_release_then_actual_reader_close(self):
        revision,lease,asset = self.first()
        with self.publisher.read_selected(lambda:self.resolve(revision)) as selected:
            with write_transaction(self.engine) as conn:
                conn.execute(text('UPDATE edition_revisions SET is_active=0'))
                conn.execute(text('UPDATE revision_seals SET retained=0'))
                conn.execute(text("UPDATE jobs SET state='cancelled' WHERE state IN ('queued','retry_wait')"))
            busy = []
            def gc_attempt():
                try:
                    with self.publisher.locks.mutation([asset['sha256']],blocking=False):
                        busy.append(False)
                except StorageBusy:
                    busy.append(True)
            thread = threading.Thread(target=gc_attempt);thread.start();thread.join(3)
            self.assertEqual(busy,[True])
        with self.publisher.locks.mutation([asset['sha256']],blocking=False):
            pass
        with self.assertRaises(PublicationError):
            selected.fd_for(selected.snapshot.assets[0])

    def test_cross_thread_reader_use_is_rejected(self):
        revision,_,_ = self.first();errors = []
        with self.publisher.read_selected(lambda:self.resolve(revision)) as selected:
            def wrong_thread():
                try:
                    selected.fd_for(selected.snapshot.assets[0])
                except PublicationError as error:
                    errors.append(error)
            worker = threading.Thread(target=wrong_thread);worker.start();worker.join(3)
        self.assertEqual(len(errors),1)

    def test_body_failure_closes_and_resets_actual_storage_operation(self):
        revision,_,_ = self.first()
        fresh = lambda:self.resolve(revision)
        release,probes,reports = threading.Event(),[],[]
        def produce(*_):
            yield b'first'
            release.wait(5)
            raise OSError('synthetic provider body failure')
        def after_close():
            # A new actual storage operation in the same producer thread proves
            # the previous read scope, FDs and thread-local lock guard exited.
            with self.publisher.read_selected(fresh) as selected:
                probes.append(selected._owner)
        async def sent(message):
            if message.get('body') == b'first':
                release.set()
        async def closed(ack,success,bytes_sent):
            reports.append((ack,success,bytes_sent))
        gate = StreamAdmission(max_producers=1)
        handle = OwnedProducer.start(open_selected=lambda:self.publisher.read_selected(fresh),
            prepare=lambda _:PreparedTransfer(200,()),produce=produce,unique_originals=1,
            admission=gate,after_close=after_close)
        with self.assertRaises(OSError):
            run_response(OwnedResponse(handle,on_closed=closed),send_hook=sent)
        self.assertEqual(probes,[handle.ack.owner_thread])
        self.assertEqual((reports[0][1],reports[0][2]),(False,len(b'first')))
        self.assertEqual(gate.usage(),dict(producers=0,reserved_fds=0))

    def test_fixed_ticket_source_rechecked_inside_opener(self):
        revision,_,_ = self.first();ticket = self.ticket(revision)
        with self.engine.connect() as conn:
            selections = self.policy.selections_for_ticket(ticket)
        def fresh():
            with self.engine.connect() as conn:
                return self.policy.resolve_selection_in(conn,self.token,selections,policy='ticket',ticket_snapshot=ticket)
        expected = fresh()
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM object_pins WHERE kind='ticket'"))
        with self.assertRaises(AuthError):
            with self.publisher.read_selected(fresh,expected=expected):
                self.fail('lost ticket pin still admitted')
