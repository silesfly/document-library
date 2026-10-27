"""Synthetic staged transport -> parser/cache -> sealed revision transactions."""
import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from PIL import Image
from sqlalchemy import text

from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.ingestion import IngestionError, IngestionReviewRequired, IngestionService, IngestionWorker
from document_library.jobs import JobService, LostLease
from document_library.permissions import PermissionService
from document_library.publication import PublicationError, PublicationService, object_path
from document_library.sources import SourceService
from document_library.storage import StorageRoot
from test_jobs import FakeClock
from test_sources import attachment
from test_storage import SyntheticVolume
from upload_helpers import UploadFixture


TEXT = '1화\n첫 본문입니다.\n3화\n다음 본문입니다.\n'.encode()


class IngestionAcceptance(UploadFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.root = StorageRoot(self.path, SyntheticVolume())
        self.addCleanup(self.root.close)
        self.clock = FakeClock()
        self.jobs = JobService(self.engine, clock=self.clock)
        self.permissions = PermissionService(self.auth)
        self.publisher = PublicationService(self.root, self.jobs, self.permissions)
        self.sources = SourceService(self.auth, self.jobs)
        self.ingest = IngestionService(self.publisher, self.uploads, self.sources)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('w','novel','Synthetic','synthetic','test','test')"))
            for ident, representation in [('e', 'text'), ('images', 'single_image')]:
                conn.execute(text('''INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at)
                    VALUES(:id,'w',:id,:representation,:id,'test')'''), dict(id=ident, representation=representation))

    def staged(self, payload=TEXT, *, filename='Book.txt', actor=None):
        actor = actor or self.uploader
        ident = self.reserve(actor, length=len(payload), total=len(payload), filename=filename)['upload_id']
        tus = uuid.uuid4().hex
        with self.root.file('uploads/tus/' + tus, create=True) as fd:
            self.ingest._write(fd, payload)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE upload_sessions SET status='staged',tus_id=:tus,received_size=declared_size WHERE id=:id"), dict(tus=tus, id=ident))
        return ident

    def queued(self, payload=TEXT, *, filename='Book.txt', edition='e', previous=None):
        upload = self.staged(payload, filename=filename)
        job = self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload,
                                         edition_id=edition, expected_revision_id=previous)
        self.ingest.review_input(self.admin.token, self.admin.csrf, job, rights_status='allowed', rights_receipt='synthetic-rights')
        lease = self.jobs.claim('ingestion-test', kind='ingest_upload', lease_ms=1000)
        self.assertEqual(job, lease.job_id)
        return upload, lease

    def source_job(self, *, key='initial', payload=TEXT, previous=None, mode='incremental'):
        if not hasattr(self, 'source_id'):
            self.source_id = self.sources.configure(self.admin.token, self.admin.csrf, account_scope='synthetic', chat_id=-100, extensions=['txt'])
        if mode == 'reconcile' and not self.scalar('SELECT count(*) FROM source_messages'):
            with write_transaction(self.engine) as conn:
                conn.execute(text('INSERT INTO source_messages VALUES(:id,1)'), dict(id=self.source_id))
        scan = self.sources.start_scan(self.source_id, config_version=1, request_key=key, mode=mode,
                                       upper_bound=None if mode == 'reconcile' else 1)
        scan_lease = self.jobs.claim('scan-test', kind='source.scan')
        self.sources.commit_page(scan_lease, expected_cursor=scan['cursor'], next_cursor=1,
                                  observations=[attachment(1, size=len(payload))], done=True)
        self.sources.finish_scan(scan_lease)
        lease = self.jobs.claim('source-test', kind='source.download', lease_ms=1000)
        self.ingest.bind_source(self.admin.token, self.admin.csrf, lease, edition_id='e', expected_revision_id=previous)
        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id, rights_status='allowed', rights_receipt='source-rights')
        return lease

    def source_process(self, lease, payload=TEXT):
        class Adapter:
            async def download_to(_self, _lease, fd):
                self.ingest._write(fd, payload)
        return asyncio.run(self.ingest.process_source(lease, Adapter()))

    def assert_no_publication(self):
        for table in ('edition_revisions', 'revision_seals', 'source_content_receipts', 'source_object_sources', 'object_sources'):
            self.assertEqual(self.scalar('SELECT count(*) FROM ' + table), 0, table)

    def test_enqueue_is_atomic_current_owner_csrf_and_admin_review(self):
        upload = self.staged()
        for actor, csrf in [(self.uploader, None), (self.member, self.member.csrf), (self.uploader, 'wrong')]:
            with self.assertRaises(AuthError):
                self.ingest.enqueue_upload(actor.token, csrf, upload_id=upload, edition_id='e')
        with patch.object(self.jobs, 'enqueue_in', side_effect=RuntimeError('synthetic DB failure')):
            with self.assertRaises(RuntimeError):
                self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e')
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_inputs'), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'), 0)
        job = self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e')
        self.assertEqual(job, self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e'))
        lease = self.jobs.claim('review-test', kind='ingest.review_upload')
        with self.assertRaises(IngestionError):
            self.ingest.process_upload(lease)
        with self.assertRaises(AuthError):
            self.ingest.review_input(self.uploader.token, self.uploader.csrf, job, rights_status='allowed', rights_receipt='not-admin')
        self.assert_no_publication()

    def test_delayed_rights_review_does_not_burn_attempts_or_duplicate_enqueue(self):
        upload = self.staged()
        job = self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e')
        worker = IngestionWorker(self.ingest, owner='review-waiting', lease_ms=1000)
        for _ in range(12):
            self.assertIsNone(asyncio.run(worker.run_once()))
            self.clock.advance(1000)
        self.assertEqual(self.jobs.get(job)['attempts'], 0)
        self.ingest.review_input(self.admin.token, self.admin.csrf, job, rights_status='allowed', rights_receipt='late-review')
        self.assertEqual(job, self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e'))
        revision = asyncio.run(worker.run_once())
        self.assertEqual(self.jobs.get(job)['attempts'], 1)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions'), revision)
        with self.assertRaises(AuthError):
            self.ingest.enqueue_upload(self.uploader.token, self.uploader.csrf, upload_id=upload, edition_id='e')
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_inputs'), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE idempotency_key=:key", key='ingest-upload:' + upload), 1)

    def test_upload_full_preview_hash_coverage_and_separate_cache(self):
        payload = TEXT + ('추가 본문 ' * 15000).encode()
        upload, lease = self.queued(payload)
        revision = self.ingest.process_upload(lease)
        result = json.loads(self.jobs.get(lease.job_id)['result_json'])
        self.assertEqual(result['revision_id'], revision)
        self.assertGreater(result['preview']['text_chars'], 65536)
        self.assertEqual(result['preview']['source_sha256'], hashlib.sha256(payload).hexdigest())
        self.assertTrue((self.path / result['preview']['bundle_relpath']).is_file())
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id', id=upload), 'published')
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_assets'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_options'), 1)
        coverage = json.loads(self.scalar('SELECT coverage_json FROM revision_options'))
        self.assertEqual(coverage['intervals'], [[1, 1], [3, 3]])
        self.assertEqual(coverage['gaps'], [[2, 2]])
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_revision'"), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_runs'), 1)

    def test_same_bytes_physical_reuse_retains_individual_upload_provenance(self):
        _upload, first = self.queued()
        previous = self.ingest.process_upload(first)
        _upload, second = self.queued(previous=previous)
        revision = self.ingest.process_upload(second)
        self.assertEqual(previous, revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_options WHERE edition_revision_id=:id', id=revision), 1)
        self.assertEqual(len(list((self.path / 'objects').rglob(hashlib.sha256(TEXT).hexdigest()))), 1)

    def test_rights_and_member_revocation_after_install_prevent_commit(self):
        for revoke in ('member', 'rights'):
            with self.subTest(revoke=revoke):
                # Independent fixtures avoid one scenario's durable revocation.
                _upload, lease = self.queued()
                real = self.publisher._install
                def installed_then_revoke(asset, verified):
                    real(asset, verified)
                    if revoke == 'member':
                        self.revoke()
                    else:
                        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                                 rights_status='blocked', rights_receipt='revoked')
                with patch.object(self.publisher, '_install', side_effect=installed_then_revoke):
                    with self.assertRaises((AuthError, PublicationError)):
                        self.ingest.process_upload(lease)
                self.assert_no_publication()
                if revoke == 'member':
                    with write_transaction(self.engine) as conn:
                        conn.execute(text('INSERT INTO user_permissions VALUES(:id,\'content.upload\',:admin,\'test\')'), dict(id=self.uploader.id, admin=self.admin.id))

    def test_transport_delete_reserved_after_install_prevents_publication(self):
        upload, lease = self.queued()
        install = self.publisher._install
        def reserve_delete(asset, verified):
            install(asset, verified)
            self.uploads.begin(self.uploader.token, self.uploader.csrf, upload,
                               method='DELETE', backend_id=str(uuid.uuid4()))
        with patch.object(self.publisher, '_install', side_effect=reserve_delete):
            with self.assertRaises(PublicationError):
                self.ingest.process_upload(lease)
        self.assert_no_publication()
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE state='reserved'"), 1)

    def test_superset_cannot_drop_or_change_current_preamble(self):
        original = 'unique preface\n1화\nbody1\n2화\nbody2\n'.encode()
        _upload, first = self.queued(original)
        previous = self.ingest.process_upload(first)
        for preface in ('', 'different preface\n'):
            with self.subTest(preface=preface):
                newer = (preface + '1화\nbody1\n2화\nbody2\n3화\nbody3\n').encode()
                _upload, lease = self.queued(newer, previous=previous)
                with self.assertRaises(IngestionReviewRequired) as caught:
                    self.ingest.process_upload(lease)
                self.assertEqual(caught.exception.reason, 'overlap_conflict')
                self.jobs.cancel(lease.job_id)
                self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        _upload, accepted = self.queued(original + '3화\nbody3\n'.encode(), previous=previous)
        self.assertNotEqual(self.ingest.process_upload(accepted), previous)

    def test_parser_changed_since_plan_requires_fresh_generation(self):
        _upload, lease = self.queued()
        old_pin = '0' * 64
        with (patch('document_library.ingestion.parser_fingerprint', return_value=old_pin),
              patch('document_library.ingest_formats.parser_fingerprint', return_value=old_pin),
              patch.object(self.publisher, 'publish', side_effect=RuntimeError('synthetic before publication'))):
            with self.assertRaises(RuntimeError):
                self.ingest.process_upload(lease)
        self.assertEqual(json.loads(self.scalar('SELECT payload_json FROM publication_plans'))['reconciliation']['parser_sha256'], old_pin)
        with self.assertRaisesRegex(PublicationError, 'parser changed'):
            self.ingest.resume_publication(lease)
        self.assert_no_publication()
        self.clock.advance(1000)
        fresh = self.jobs.claim('fresh-parser', kind='ingest_upload', lease_ms=1000)
        self.assertGreater(fresh.generation, lease.generation)
        self.ingest.process_upload(fresh)
        self.assertEqual(self.jobs.get(fresh.job_id)['state'], 'completed')

    def test_exhausted_review_cannot_rearm_but_revocation_and_completed_rights_remain(self):
        _upload, lease = self.queued()
        maximum = self.jobs.get(lease.job_id)['max_attempts']
        for attempt in range(1, maximum + 1):
            self.assertEqual(self.jobs.get(lease.job_id)['attempts'], attempt)
            self.ingest.park_review(lease, IngestionReviewRequired('synthetic_review'))
            if attempt < maximum:
                self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                         rights_status='allowed', rights_receipt=f'approval-{attempt}')
                lease = self.jobs.claim('review-again', kind='ingest_upload', lease_ms=1000)
        before = self.scalar('SELECT rights_receipt FROM ingestion_inputs WHERE job_id=:id', id=lease.job_id)
        with self.assertRaisesRegex(IngestionError, 'attempts exhausted'):
            self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                     rights_status='allowed', rights_receipt='exhausted-approval')
        self.assertEqual(self.jobs.get(lease.job_id)['kind'], 'ingest.validation_review')
        self.assertEqual(self.scalar('SELECT rights_receipt FROM ingestion_inputs WHERE job_id=:id', id=lease.job_id), before)
        for status in ('blocked', 'review'):
            self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                     rights_status=status, rights_receipt='exhausted-' + status)
        self.assertIsNone(self.jobs.claim('no-exhausted-job', kind='ingest_upload'))
        _upload, complete = self.queued()
        # The last already-running attempt may still finish normally.
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE jobs SET attempts=max_attempts WHERE id=:id'), dict(id=complete.job_id))
        self.ingest.review_input(self.admin.token, self.admin.csrf, complete.job_id,
                                 rights_status='allowed', rights_receipt='running-last')
        self.ingest.process_upload(complete)
        for status in ('blocked', 'allowed'):
            self.ingest.review_input(self.admin.token, self.admin.csrf, complete.job_id,
                                     rights_status=status, rights_receipt='completed-' + status)
        self.assertEqual(self.jobs.get(complete.job_id)['state'], 'completed')

    def test_worker_restart_new_generation_and_same_generation_resume(self):
        _upload, old = self.queued()
        real = self.publisher._install
        def interruption(asset, verified):
            real(asset, verified)
            raise RuntimeError('synthetic worker termination after rename')
        with patch.object(self.publisher, '_install', side_effect=interruption):
            with self.assertRaises(RuntimeError):
                self.ingest.process_upload(old)
        self.assert_no_publication()
        # Same live generation resumes its immutable plan without another parse.
        with patch('document_library.ingestion.inspect_fd', side_effect=AssertionError('must not parse again')):
            revision = self.ingest.process_upload(old)
        self.assertEqual(self.jobs.get(old.job_id)['state'], 'completed')
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions'), revision)
        _upload, interrupted = self.queued(previous=revision)
        with patch.object(self.publisher, '_install', side_effect=RuntimeError('synthetic restart')):
            with self.assertRaises(RuntimeError):
                self.ingest.process_upload(interrupted)
        self.clock.advance(1000)
        fresh = self.jobs.claim('replacement', kind='ingest_upload', lease_ms=1000)
        self.assertEqual(fresh.job_id, interrupted.job_id)
        self.assertGreater(fresh.generation, interrupted.generation)
        with self.assertRaises(LostLease):
            self.publisher.publish(interrupted)
        self.ingest.process_upload(fresh)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)

    def test_source_receipt_and_revision_commit_atomically_after_failure(self):
        lease = self.source_job(mode='reconcile')
        real = self.publisher._write_revision
        def after_rows(*args):
            real(*args)
            raise RuntimeError('synthetic failure before final CAS')
        with patch.object(self.publisher, '_write_revision', side_effect=after_rows):
            with self.assertRaises(RuntimeError):
                self.source_process(lease)
        self.assert_no_publication()
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'), 'RECONCILING')
        revision = self.ingest.resume_publication(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 0)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'), 'CURRENT')
        self.assertEqual(self.scalar('SELECT source_job_id FROM revision_seals WHERE revision_id=:id', id=revision), lease.job_id)

    def test_source_same_metadata_new_sha_and_same_sha_receipt_reuse(self):
        payload = '1화\n첫 본문입니다.\n'.encode()
        first = self.source_job(payload=payload)
        previous = self.source_process(first, payload)
        changed = '2화\n새 본문입니다.\n'.encode()
        self.assertEqual(len(changed), len(payload))
        second = self.source_job(key='history-2', payload=changed, previous=previous, mode='reconcile')
        previous = self.source_process(second, changed)
        third = self.source_job(key='history-3', payload=changed, previous=previous, mode='reconcile')
        self.source_process(third, changed)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 3)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 3)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 2)

    def test_source_epoch_change_after_install_is_not_a_member_upload(self):
        lease = self.source_job()
        real = self.publisher._install
        def changed_epoch(asset, verified):
            real(asset, verified)
            self.sources.mark_event_gap(self.source_id, config_version=1)
        with patch.object(self.publisher, '_install', side_effect=changed_epoch):
            with self.assertRaises(PublicationError):
                self.source_process(lease)
        self.assert_no_publication()
        self.assertEqual(self.scalar('SELECT count(*) FROM upload_sessions'), 0)

    def test_source_worker_waits_for_review_then_uses_injected_sdk_end_to_end(self):
        from document_library.source_adapter import SourceAdapter
        from test_source_adapter import FakeSDK, message
        lease = self.source_job()
        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                 rights_status='review', rights_receipt='waiting-for-decision')
        self.jobs.fail(lease, 'catalog_wait', retry_delay_ms=0)
        client = FakeSDK()
        client.messages[1] = message(1, TEXT)
        client.messages[1].document.id = 10
        adapter = SourceAdapter(self.sources, client, account_scope='synthetic')
        worker = IngestionWorker(self.ingest, owner='sdk-ingestion', lease_ms=1000)
        for _ in range(12):
            self.assertIsNone(asyncio.run(worker.run_once(kind='source.download', source_adapter=adapter)))
            self.clock.advance(1000)
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'], 1)
        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                 rights_status='allowed', rights_receipt='source-late-decision')
        revision = asyncio.run(worker.run_once(kind='source.download', source_adapter=adapter))
        self.assertEqual(client.download_calls, 1)
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'], 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions'), revision)

    def test_text_image_coexistence_and_parser_review_do_not_replace_editions(self):
        _upload, text_lease = self.queued()
        text_revision = self.ingest.process_upload(text_lease)
        image = io.BytesIO()
        Image.new('RGB', (3, 4), 'white').save(image, 'PNG')
        _upload, image_lease = self.queued(image.getvalue(), filename='Book.png', edition='images')
        self.ingest.process_upload(image_lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions WHERE is_active=1'), 2)
        self.assertEqual(self.scalar("SELECT id FROM edition_revisions WHERE edition_id='e' AND is_active=1"), text_revision)
        _upload, unsafe = self.queued(b'{\\rtf1 unsupported}', filename='Book.rtf', previous=text_revision)
        with self.assertRaises(IngestionError):
            self.ingest.process_upload(unsafe)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 2)

    def test_current_rights_revoke_denies_already_published_stream(self):
        _upload, lease = self.queued()
        revision = self.ingest.process_upload(lease)
        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                 rights_status='blocked', rights_receipt='review-revoked')
        with self.assertRaises(PublicationError):
            with self.publisher.read_revision(self.member.token, revision, [0]):
                self.fail('revoked source cannot stream')

    def test_validated_scanned_pdf_retains_original_with_unavailable_text_preview(self):
        from pypdf import PdfWriter
        output = io.BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=100, height=100)
        writer.write(output)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('pdf','w','pdf','document','PDF','test')"))
        _upload, lease = self.queued(output.getvalue(), filename='Scan.pdf', edition='pdf')
        revision = self.ingest.process_upload(lease)
        result = json.loads(self.jobs.get(lease.job_id)['result_json'])
        self.assertEqual(result['preview']['kind'], 'unavailable')
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_assets WHERE edition_revision_id=:id', id=revision), 1)

    def real_clock_worker_job(self):
        # This test's isolated DB has not yet observed a synthetic job clock.
        self.jobs = JobService(self.engine)
        self.publisher.jobs = self.ingest.jobs = self.sources.jobs = self.jobs
        _upload, lease = self.queued()
        self.jobs.fail(lease, 'synthetic_worker_handoff', retry_delay_ms=0)
        return lease.job_id

    def test_worker_heartbeats_real_clock_during_long_parse_boundary(self):
        job = self.real_clock_worker_job()
        real = self.ingest.process_upload
        samples = []
        def slower(lease):
            samples.append(self.jobs.get(job)['lease_until'])
            time.sleep(1.1)
            samples.append(self.jobs.get(job)['lease_until'])
            return real(lease)
        worker = IngestionWorker(self.ingest, owner='live-heartbeat', lease_ms=1000)
        with patch.object(self.ingest, 'process_upload', side_effect=slower):
            revision = asyncio.run(worker.run_once())
        self.assertGreater(samples[1], samples[0])
        self.assertEqual(self.jobs.get(job)['state'], 'completed')
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions'), revision)
        self.assertFalse(any(t.name == 'ingestion-lease' for t in threading.enumerate()))

    def test_parser_review_is_parked_without_automatic_attempt_consumption(self):
        _upload, lease = self.queued(b'{\\rtf1 unsupported}', filename='manual.rtf')
        self.jobs.fail(lease, 'synthetic-review-handoff', retry_delay_ms=0)
        worker = IngestionWorker(self.ingest, owner='validation-review', lease_ms=1000)
        with self.assertRaises(IngestionReviewRequired) as raised:
            asyncio.run(worker.run_once())
        job = self.jobs.get(lease.job_id)
        self.assertEqual(job['kind'], 'ingest.validation_review')
        self.assertEqual(job['state'], 'queued')
        self.assertIsNone(job['lease_owner'])
        self.assertIsNone(job['lease_until'])
        self.assertEqual(json.loads(job['checkpoint_json'])['reason'], raised.exception.reason)
        attempts = job['attempts']
        for _ in range(12):
            self.clock.advance(1000)
            self.assertIsNone(asyncio.run(worker.run_once()))
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'], attempts)
        self.ingest.review_input(self.admin.token, self.admin.csrf, lease.job_id,
                                 rights_status='allowed', rights_receipt='explicit-format-recheck')
        # Rights review permits one explicit reinspection; it cannot validate
        # unsupported bytes or make them publishable by itself.
        with self.assertRaises(IngestionReviewRequired):
            asyncio.run(worker.run_once())
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'], attempts + 1)
        self.assert_no_publication()

    def test_actual_worker_process_death_after_rename_recovers_new_generation(self):
        job = self.real_clock_worker_job()
        script = '''
import asyncio, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[2])
sys.path.insert(0, sys.argv[3])
os.environ['PYTHONPATH'] = sys.argv[3]
from document_library.auth import AuthService
from document_library.database import database_engine
from document_library.ingestion import IngestionService, IngestionWorker
from document_library.jobs import JobService
from document_library.permissions import PermissionService
from document_library.publication import PublicationService
from document_library.sources import SourceService
from document_library.storage import StorageRoot
from document_library.uploads import UploadService
from test_storage import SyntheticVolume
root = StorageRoot(Path(sys.argv[1]), SyntheticVolume())
engine = database_engine(Path(sys.argv[1]) / 'synthetic.sqlite3')
jobs = JobService(engine)
auth = AuthService(engine, clock=lambda: 1800000000)
publisher = PublicationService(root, jobs, PermissionService(auth))
service = IngestionService(publisher, UploadService(auth), SourceService(auth, jobs))
install = publisher._install
def stop_after_rename(asset, verified):
    install(asset, verified)
    os._exit(42)
publisher._install = stop_after_rename
asyncio.run(IngestionWorker(service, owner='child-process', lease_ms=1000).run_once())
'''
        test_directory = Path(__file__).resolve().parent
        core_directory = test_directory.parent / 'packages/core'
        child = subprocess.run([sys.executable, '-B', '-c', script, str(self.path),
            str(test_directory), str(core_directory)], cwd=self.path, capture_output=True, timeout=30)
        self.assertEqual(child.returncode, 42, child.stderr.decode())
        old = self.jobs.get(job)
        self.assertEqual(old['state'], 'running')
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_seals'), 0)
        self.assertTrue((self.path / object_path(hashlib.sha256(TEXT).hexdigest())).is_file())
        remaining = max(0, old['lease_until'] - self.jobs._sample()[1])
        if remaining:
            self.assertIsNone(self.jobs.claim('before-expiry', kind='ingest_upload'))
            time.sleep(remaining / 1000 + 0.05)
        revision = asyncio.run(IngestionWorker(self.ingest, owner='replacement-process', lease_ms=1000).run_once())
        fresh = self.jobs.get(job)
        self.assertEqual(fresh['state'], 'completed')
        self.assertGreater(fresh['lease_generation'], old['lease_generation'])
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions'), revision)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_runs'), 2)

    def publish_chapters(self, first, last, *, previous=None, changed=None):
        from test_reconciliation import chapters
        _upload, lease = self.queued(chapters(first, last, changed=changed), previous=previous)
        return self.ingest.process_upload(lease), lease

    def active_options(self, revision):
        with self.engine.connect() as conn:
            return [dict(row) for row in conn.execute(text('SELECT * FROM revision_options WHERE edition_revision_id=:id ORDER BY format'), dict(id=revision)).mappings()]

    def test_reconcile_superset_150_to_160_replaces_latest_and_retains_original(self):
        previous, old = self.publish_chapters(1, 150)
        revision, new = self.publish_chapters(1, 160, previous=previous)
        self.assertNotEqual(previous, revision)
        self.assertEqual(len(self.active_options(revision)), 1)
        self.assertEqual(json.loads(self.active_options(revision)[0]['coverage_json'])['intervals'], [[1, 160]])
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 2)
        self.assertEqual(self.scalar('SELECT retained FROM revision_seals WHERE revision_id=:id', id=previous), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_assets WHERE edition_revision_id=:id', id=previous), 1)
        self.assertEqual(json.loads(self.jobs.get(new.job_id)['result_json'])['reconciliation']['outcome'], 'replace_superset')
        with self.publisher.read_revision(self.member.token, previous, [0]) as fds:
            self.assertIn(b'150', os.read(fds[0], 100000))

    def test_reconcile_preserves_epub_actual_range_and_image_edition(self):
        from test_ingest_formats import archive_bytes, image_bytes
        previous, _old = self.publish_chapters(1, 150)
        epub = archive_bytes([
            ('mimetype', b'application/epub+zip'),
            ('META-INF/container.xml', b'<container><rootfiles><rootfile full-path="OPS/book.opf"/></rootfiles></container>'),
            ('OPS/book.opf', b'<package><manifest><item id="a" href="one.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="a"/></spine></package>'),
            ('OPS/one.xhtml', ('<html><body>' + ''.join(f'<p>{n}화</p><p>epub {n}</p>' for n in range(1, 151)) + '</body></html>').encode())])
        _u, ebook = self.queued(epub, filename='Book.epub', previous=previous)
        previous = self.ingest.process_upload(ebook)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('packs','w','packs','imagepack','Packs','test')"))
        _u, image = self.queued(archive_bytes([('1.png', image_bytes())]), filename='Book.cbz', edition='packs')
        image_revision = self.ingest.process_upload(image)
        revision, _new = self.publish_chapters(1, 160, previous=previous)
        options = self.active_options(revision)
        self.assertEqual([option['format'] for option in options], ['epub', 'txt'])
        self.assertEqual([json.loads(option['coverage_json'])['intervals'] for option in options], [[[1, 150]], [[1, 160]]])
        self.assertEqual(self.scalar("SELECT id FROM edition_revisions WHERE edition_id='packs' AND is_active=1"), image_revision)

    def test_reconcile_151_bundle_152_restart_and_duplicate_replay(self):
        from test_reconciliation import chapters
        previous, _old = self.publish_chapters(1, 150)
        revision, _one = self.publish_chapters(151, 151, previous=previous)
        option = self.active_options(revision)[0]
        self.assertEqual(option['delivery_kind'], 'bundle')
        self.assertEqual(json.loads(option['coverage_json'])['intervals'], [[1, 151]])
        _upload, next_lease = self.queued(chapters(152, 152), previous=revision)
        install = self.publisher._install
        def interrupted(asset, verified):
            install(asset, verified)
            raise RuntimeError('synthetic bundle restart')
        with patch.object(self.publisher, '_install', side_effect=interrupted):
            with self.assertRaises(RuntimeError):
                self.ingest.process_upload(next_lease)
        latest = self.ingest.process_upload(next_lease)
        with self.engine.connect() as conn:
            members = conn.execute(text('''SELECT a.object_sha FROM option_assets o JOIN revision_assets a
                ON a.edition_revision_id=o.edition_revision_id AND a.asset_position=o.asset_position
                WHERE o.option_id=:id ORDER BY ordinal'''), dict(id=self.active_options(latest)[0]['id'])).scalars().all()
        self.assertEqual(members, [hashlib.sha256(chapters(a, b)).hexdigest() for a, b in [(1, 150), (151, 151), (152, 152)]])
        same, duplicate = self.publish_chapters(152, 152, previous=latest)
        self.assertEqual(same, latest)
        self.assertEqual(len(self.active_options(same)), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 3)
        with self.assertRaises(LostLease):
            self.ingest.process_upload(next_lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 4)

    def test_reconcile_conflicting_self_valid_content_is_durable_review(self):
        from test_reconciliation import chapters
        previous, _old = self.publish_chapters(1, 150)
        _upload, lease = self.queued(chapters(1, 150, changed=77), previous=previous)
        self.jobs.fail(lease, 'test_handoff', retry_delay_ms=0)
        worker = IngestionWorker(self.ingest, owner='conflict-review', lease_ms=1000)
        with self.assertRaises(IngestionReviewRequired) as caught:
            asyncio.run(worker.run_once())
        self.assertEqual(caught.exception.reason, 'overlap_conflict')
        job = self.jobs.get(lease.job_id)
        self.assertEqual(job['kind'], 'ingest.validation_review')
        attempts = job['attempts']
        for _ in range(12):
            self.clock.advance(1000)
            self.assertIsNone(asyncio.run(worker.run_once()))
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'], attempts)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 1)

    def test_reconcile_missing_changed_and_stale_old_original_fail_closed(self):
        from test_reconciliation import chapters
        previous, old = self.publish_chapters(1, 2)
        sha = hashlib.sha256(chapters(1, 2)).hexdigest()
        _upload, lease = self.queued(chapters(1, 3), previous=previous)
        with (self.path / object_path(sha)).open('r+b') as stream:
            stream.write(b'X')
        with self.assertRaises(IngestionReviewRequired):
            self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        (self.path / object_path(sha)).unlink()
        with self.assertRaises(IngestionReviewRequired):
            self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)

    def test_reconcile_old_mutation_after_report_and_active_revision_race(self):
        from test_reconciliation import chapters
        previous, _old = self.publish_chapters(1, 2)
        _upload, lease = self.queued(chapters(1, 3), previous=previous)
        preview = self.ingest._preview
        def mutate_old(*args):
            result = preview(*args)
            with (self.path / object_path(hashlib.sha256(chapters(1, 2)).hexdigest())).open('r+b') as stream:
                stream.write(b'X')
            return result
        with patch.object(self.ingest, '_preview', side_effect=mutate_old):
            with self.assertRaises(PublicationError):
                self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)

    def test_reconcile_duplicate_provenance_final_cas_and_atomic_backup(self):
        previous, first = self.publish_chapters(1, 2)
        from test_reconciliation import chapters
        upload, lease = self.queued(chapters(1, 2), previous=previous)
        record = self.publisher._record_input_source
        def expire_after_rows(*args):
            record(*args)
            self.clock.advance(1000)
        with patch.object(self.publisher, '_record_input_source', side_effect=expire_after_rows):
            with self.assertRaises(LostLease):
                self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 0)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id', id=upload), 'staged')
        fresh = self.jobs.claim('duplicate-replacement', kind='ingest_upload', lease_ms=1000)
        self.assertEqual(self.ingest.process_upload(fresh), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 2)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM job_outbox WHERE kind='provenance_added'"), 1)

    def test_reconcile_replaced_original_source_revocation_is_not_reauthorized(self):
        from test_reconciliation import chapters
        previous, original = self.publish_chapters(1, 2)
        _upload, lease = self.queued(chapters(1, 3), previous=previous)
        install = self.publisher._install
        def revoke_base(asset, verified):
            install(asset, verified)
            self.ingest.review_input(self.admin.token, self.admin.csrf, original.job_id, rights_status='blocked', rights_receipt='old-revoked')
        with patch.object(self.publisher, '_install', side_effect=revoke_base):
            with self.assertRaises(PublicationError):
                self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 1)

    def test_reconcile_source_duplicate_receipt_failure_rolls_back_and_replays(self):
        lease = self.source_job()
        previous = self.source_process(lease)
        second = self.source_job(key='same-2', previous=previous, mode='reconcile')
        record = self.publisher._record_input_source
        def fail_after_receipt(*args):
            record(*args)
            raise RuntimeError('synthetic duplicate receipt transaction failure')
        with patch.object(self.publisher, '_record_input_source', side_effect=fail_after_receipt):
            with self.assertRaises(RuntimeError):
                self.source_process(second)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.ingest.resume_publication(second), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)

    def test_reconcile_source_same_metadata_changed_sha_is_fact_and_review(self):
        from test_reconciliation import chapters
        payload, changed = chapters(1, 2), chapters(1, 2).replace(b'body 2', b'edit 2')
        first = self.source_job(payload=payload)
        previous = self.source_process(first, payload)
        lease = self.source_job(key='changed-body', payload=changed, previous=previous, mode='reconcile')
        self.jobs.fail(lease, 'test_handoff', retry_delay_ms=0)
        class Adapter:
            async def download_to(_self, _lease, fd):
                self.ingest._write(fd, changed)
        with self.assertRaises(IngestionReviewRequired):
            asyncio.run(IngestionWorker(self.ingest, owner='source-conflict', lease_ms=1000).run_once(kind='source.download', source_adapter=Adapter()))
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)

    def test_reconcile_imagepack_logical_duplicate_parks_without_deleting_original(self):
        from test_ingest_formats import archive_bytes, image_bytes
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('packs','w','packs','imagepack','Packs','test')"))
        page = image_bytes()
        old = archive_bytes([('1.png', page)])
        fresh = archive_bytes([('renamed.png', page)])
        _upload, one = self.queued(old, filename='Book.cbz', edition='packs')
        previous = self.ingest.process_upload(one)
        _upload, second = self.queued(fresh, filename='Book.cbz', edition='packs', previous=previous)
        self.jobs.fail(second, 'test_handoff', retry_delay_ms=0)
        with self.assertRaises(IngestionReviewRequired) as caught:
            asyncio.run(IngestionWorker(self.ingest, owner='image-logical-review', lease_ms=1000).run_once())
        self.assertEqual(caught.exception.reason, 'logical_image_duplicate')
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        self.assertTrue((self.path / object_path(hashlib.sha256(old).hexdigest())).is_file())
        self.assertEqual(self.jobs.get(second.job_id)['kind'], 'ingest.validation_review')

    def test_reconcile_normalized_equal_raw_different_never_dedupes_originals(self):
        from test_reconciliation import chapters
        previous, _old = self.publish_chapters(1, 2)
        payload = chapters(1, 2, newline='\r\n')
        _upload, lease = self.queued(payload, previous=previous)
        with self.assertRaises(IngestionReviewRequired) as caught:
            self.ingest.process_upload(lease)
        self.assertEqual(caught.exception.reason, 'same_content_different_original')
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)
        self.assertTrue(list((self.path / lease.staging_key).glob('*.part')))


    def legacy_options(self, payloads, *, option_count=None):
        """Trusted pre-reconciliation publication fixture with actual raw originals."""
        from document_library.ingest_formats import inspect_bytes
        upload = self.staged(payloads[0])
        job = self.jobs.enqueue(kind='ingest_upload', idempotency_key='legacy:' + str(uuid.uuid4()), payload={},
                                work_id='w', upload_session_id=upload)
        lease = self.jobs.claim('legacy-fixture', kind='ingest_upload', lease_ms=1000)
        self.assertEqual(lease.job_id, job)
        assets, options = [], []
        for payload in payloads:
            report = inspect_bytes(payload, 'Book.txt')
            with self.root.stage(lease) as (path, fd):
                self.ingest._write(fd, payload)
            assets.append(self.publisher.prepare(lease, path, sha256=report['sha256'], byte_size=len(payload), detected_mime='text/plain', file_format='txt'))
            options.append(dict(format='txt', delivery_kind='single_file', positions=[len(assets)-1], coverage=report['metadata']['coverage']))
        if option_count:
            options = options * option_count
        self.publisher.register_validated_upload(lease, edition_id='e', expected_revision_id=None, assets=assets, options=options,
                                                 validation_receipt='synthetic-reviewed-legacy', rights_status='allowed')
        return self.publisher.publish(lease)

    def test_reconcile_parallel_conflicting_old_options_cannot_hide_behind_exact_sha(self):
        from test_reconciliation import chapters
        old = chapters(1, 2)
        previous = self.legacy_options([old, chapters(1, 2, changed=2)])
        _upload, lease = self.queued(old, previous=previous)
        with self.assertRaises(IngestionReviewRequired) as caught:
            self.ingest.process_upload(lease)
        self.assertEqual(caught.exception.reason, 'overlap_conflict')
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        self.assertEqual(len(self.active_options(previous)), 2)

    def test_reconcile_multiple_actual_old_options_get_one_superset_receipt(self):
        from test_reconciliation import chapters
        previous = self.legacy_options([chapters(1, 2), chapters(1, 3)])
        revision, lease = self.publish_chapters(1, 4, previous=previous)
        self.assertEqual(len(self.active_options(revision)), 1)
        payload = json.loads(self.scalar('SELECT payload_json FROM publication_plans WHERE job_id=:id', id=lease.job_id))
        self.assertEqual(len(payload['options'][0]['replaces_option_ids']), 2)
        self.assertEqual(payload['options'][0]['replacement_receipt'], 'overlap:' + payload['reconciliation']['proof_digest'])
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_assets WHERE edition_revision_id=:id', id=previous), 2)

    def test_reconcile_64_option_limit_is_review_without_dropping_formats(self):
        from test_reconciliation import chapters
        previous = self.legacy_options([chapters(1, 2)], option_count=64)
        self.assertEqual(len(self.active_options(previous)), 64)
        _upload, lease = self.queued('<h1>1화</h1><p>body</p>'.encode(), filename='Book.html', previous=previous)
        with self.assertRaises(IngestionReviewRequired) as caught:
            self.ingest.process_upload(lease)
        self.assertEqual(caught.exception.reason, 'reconciliation_plan_limit')
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)
        self.assertEqual(len(self.active_options(previous)), 64)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)

    def test_reconcile_200_asset_boundary_and_json_limit_remain_enforced(self):
        from test_reconciliation import chapters
        _upload, lease = self.queued(chapters(1, 2))
        with self.root.stage(lease) as (path, fd):
            self.ingest._write(fd, chapters(1, 2))
        asset = self.publisher.prepare(lease, path, sha256=hashlib.sha256(chapters(1, 2)).hexdigest(), byte_size=len(chapters(1, 2)), detected_mime='text/plain', file_format='txt')
        coverage = dict(unit='episode', intervals=[[1, 2]], gaps=[], confidence='verified')
        plan = dict(assets=[asset] * 200, options=[dict(format='txt', delivery_kind='bundle', positions=list(range(200)), coverage=coverage)])
        self.publisher._validate_plan(lease, plan)
        with self.assertRaises(PublicationError):
            self.publisher._validate_plan(lease, dict(plan, assets=[asset] * 201))
        from document_library.jobs import json_object
        with self.assertRaises(ValueError):
            json_object(dict(proof='x' * 65536))
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 0)

    def test_reconcile_source_duplicate_epoch_change_rolls_back_metadata(self):
        first = self.source_job()
        previous = self.source_process(first)
        lease = self.source_job(key='same-epoch', previous=previous, mode='reconcile')
        install = self.publisher._install
        def gap_after_install(asset, verified):
            install(asset, verified)
            self.sources.mark_event_gap(self.source_id, config_version=1)
        with patch.object(self.publisher, '_install', side_effect=gap_after_install):
            with self.assertRaises(PublicationError):
                self.source_process(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 1)

    def test_reconcile_duplicate_final_upload_authority_failure_rolls_back_all_effects(self):
        from test_reconciliation import chapters
        previous, _first = self.publish_chapters(1, 2)
        upload, lease = self.queued(chapters(1, 2), previous=previous)
        finalize = self.publisher.permissions.finish_publication_in
        def revoke_in_same_transaction(conn, upload_id):
            conn.execute(text("DELETE FROM user_permissions WHERE user_id=:id AND permission='content.upload'"), dict(id=self.uploader.id))
            return finalize(conn, upload_id)
        with patch.object(self.publisher.permissions, 'finish_publication_in', side_effect=revoke_in_same_transaction):
            with self.assertRaises(AuthError):
                self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 1)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id', id=upload), 'staged')
        self.assertEqual(self.scalar("SELECT count(*) FROM job_outbox WHERE kind='provenance_added'"), 0)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 0)
        self.assertEqual(self.ingest.resume_publication(lease), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM object_sources'), 2)

    def test_reconcile_active_revision_race_fails_without_overwriting_newer_revision(self):
        from test_reconciliation import chapters
        previous, _first = self.publish_chapters(1, 2)
        _upload, lease = self.queued(chapters(1, 3), previous=previous)
        preview = self.ingest._preview
        newer = []
        def concurrent_publication(*args):
            result = preview(*args)
            with patch.object(self.ingest, '_preview', side_effect=preview):
                revision, _job = self.publish_chapters(1, 4, previous=previous)
            newer.append(revision)
            return result
        with patch.object(self.ingest, '_preview', side_effect=concurrent_publication):
            with self.assertRaises(PublicationError):
                self.ingest.process_upload(lease)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), newer[0])
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'), 2)


    def test_reconcile_review_parking_expired_and_stale_cas_rolls_back_source_facts(self):
        from test_reconciliation import chapters
        payload = chapters(1, 2)
        changed = payload.replace(b'body 2', b'edit 2')
        first = self.source_job(payload=payload)
        previous = self.source_process(first, payload)
        lease = self.source_job(key='park-cas', payload=changed, previous=previous, mode='reconcile')
        with self.assertRaises(IngestionReviewRequired) as caught:
            self.source_process(lease, changed)
        enqueue = self.jobs.enqueue_in
        def expire_after_backup(conn, **request):
            result = enqueue(conn, **request)
            self.clock.advance(1000)
            return result
        with patch.object(self.jobs, 'enqueue_in', side_effect=expire_after_backup):
            with self.assertRaises(LostLease):
                self.ingest.park_review(lease, caught.exception)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_versions'), 1)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 0)
        fresh = self.jobs.claim('fresh-review', kind='ingest_source', lease_ms=1000)
        self.assertGreater(fresh.generation, lease.generation)
        with self.assertRaises(LostLease):
            self.ingest.park_review(lease, caught.exception)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.ingest.park_review(fresh, caught.exception)
        self.assertEqual(self.jobs.get(fresh.job_id)['kind'], 'ingest.validation_review')
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 2)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='backup_catalog'"), 1)


    def test_reconcile_same_sha_in_another_explicit_edition_keeps_its_own_revision(self):
        from test_reconciliation import chapters
        previous, _first = self.publish_chapters(1, 2)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('other','w','other','text','Other','test')"))
        _upload, lease = self.queued(chapters(1, 2), edition='other')
        revision = self.ingest.process_upload(lease)
        self.assertNotEqual(revision, previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM objects'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions WHERE is_active=1'), 2)
        self.assertEqual(len(self.active_options(revision)), 1)
        self.assertEqual(self.scalar("SELECT id FROM edition_revisions WHERE edition_id='e' AND is_active=1"), previous)

    def test_reconcile_bundle_source_config_changed_after_install_is_fenced(self):
        from document_library.sources import SourceError
        from test_reconciliation import chapters
        payload = chapters(1, 2)
        first = self.source_job(payload=payload)
        previous = self.source_process(first, payload)
        fresh = chapters(3, 3)
        lease = self.source_job(key='bundle-config', payload=fresh, previous=previous, mode='reconcile')
        install = self.publisher._install
        changed = []
        def config_after_install(asset, verified):
            install(asset, verified)
            if not changed:
                self.sources.configure(self.admin.token, self.admin.csrf, account_scope='synthetic', chat_id=-100,
                                       extensions=['txt'], enabled=False, expected_version=1)
                changed.append(True)
        with patch.object(self.publisher, '_install', side_effect=config_after_install):
            with self.assertRaises((SourceError, PublicationError)):
                self.source_process(lease, fresh)
        self.assertEqual(self.scalar('SELECT id FROM edition_revisions WHERE is_active=1'), previous)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_object_sources'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM revision_options'), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'), 1)

    def test_admin_upload_binding_checks_owner_full_transfer_operations_revision_and_rollback(self):
        upload=self.staged()
        with self.assertRaises(AuthError):self.ingest.bind_upload(self.uploader.token,self.uploader.csrf,upload_id=upload,edition_id='e')
        with self.assertRaises(AuthError):self.ingest.bind_upload(self.admin.token,'',upload_id=upload,edition_id='e')
        operation,_=self.uploads.begin(self.uploader.token,self.uploader.csrf,upload,method='HEAD',backend_id=str(uuid.uuid4()))
        with self.assertRaises(IngestionError):self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e')
        self.uploads.interrupted(operation)
        with write_transaction(self.engine) as conn:conn.execute(text('UPDATE upload_sessions SET received_size=0 WHERE id=:id'),dict(id=upload))
        with self.assertRaises(AuthError):self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e')
        with write_transaction(self.engine) as conn:conn.execute(text('UPDATE upload_sessions SET received_size=declared_size WHERE id=:id'),dict(id=upload))
        with patch.object(self.jobs,'enqueue_in',side_effect=RuntimeError('synthetic writer failure')):
            with self.assertRaises(RuntimeError):self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e')
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_inputs'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),0)
        job=self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e')
        self.assertEqual(job,self.ingest.enqueue_upload(self.uploader.token,self.uploader.csrf,upload_id=upload,edition_id='e'))
        self.assertEqual(self.scalar('SELECT rights_status FROM ingestion_inputs'),'review')
        self.assertEqual(self.scalar('SELECT decision_actor FROM ingestion_inputs'),self.uploader.id)

    def test_admin_bind_stale_active_revision_fails_before_new_job(self):
        _first,lease=self.queued();revision=self.ingest.process_upload(lease)
        upload=self.staged()
        before=self.scalar('SELECT count(*) FROM ingestion_inputs')
        with self.assertRaises(IngestionError):self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e',expected_revision_id=None)
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_inputs'),before)
        self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e',expected_revision_id=revision)
        self.assertEqual(self.scalar('SELECT expected_revision_id FROM ingestion_inputs WHERE upload_id=:id',id=upload),revision)

    def test_admin_and_original_owner_checks_share_writer_barrier_and_revalidate_after_revoke(self):
        upload=self.staged();entered=threading.Event();release=threading.Event();attempting=threading.Event();revoked=threading.Event()
        seen=[];results=[];errors=[]
        real_admin=self.ingest._admin;real_owner=self.permissions.check_upload_in
        def admin(conn,*args):
            actor=real_admin(conn,*args);seen.append(conn);entered.set()
            if not release.wait(5):raise RuntimeError('synthetic barrier release absent')
            return actor
        def owner(conn,ident):
            self.assertIs(conn,seen[0]);self.assertTrue(conn.in_transaction());return real_owner(conn,ident)
        def bind():
            try:results.append(self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e'))
            except BaseException as exc:errors.append(exc)
        def revoke():
            attempting.set()
            try:self.revoke();revoked.set()
            except BaseException as exc:errors.append(exc)
        with patch.object(self.ingest,'_admin',side_effect=admin),patch.object(self.permissions,'check_upload_in',side_effect=owner):
            a=threading.Thread(target=bind);b=threading.Thread(target=revoke)
            try:
                a.start();self.assertTrue(entered.wait(5));b.start();self.assertTrue(attempting.wait(5))
                self.assertFalse(revoked.wait(.05));release.set()
            finally:
                release.set();a.join(10)
                if b.ident is not None:b.join(10)
            self.assertFalse(a.is_alive());self.assertFalse(b.is_alive())
        self.assertEqual(errors,[]);self.assertEqual(len(results),1);self.assertTrue(revoked.is_set())
        with self.assertRaises(AuthError):self.ingest.bind_upload(self.admin.token,self.admin.csrf,upload_id=upload,edition_id='e')
        self.assertEqual(self.scalar('SELECT count(*) FROM ingestion_inputs'),1)



    def test_legacy_explicit_same_receipt_review_rearms_parser_without_http_retry_effects(self):
        _upload,lease=self.queued(b'{\\rtf1 unsupported}',filename='manual.rtf')
        self.jobs.fail(lease,'synthetic-review-handoff',retry_delay_ms=0)
        worker=IngestionWorker(self.ingest,owner='legacy-review-reinspection',lease_ms=1000)
        with self.assertRaises(IngestionReviewRequired):asyncio.run(worker.run_once())
        before=self.jobs.get(lease.job_id)
        self.assertEqual(before['kind'],'ingest.validation_review')
        self.ingest.review_input(self.admin.token,self.admin.csrf,lease.job_id,rights_status='allowed',rights_receipt='synthetic-rights')
        self.assertEqual(self.jobs.get(lease.job_id)['kind'],'ingest_upload')
        with self.assertRaises(IngestionReviewRequired):asyncio.run(worker.run_once())
        self.assertEqual(self.jobs.get(lease.job_id)['attempts'],before['attempts']+1)
        self.assert_no_publication()


if __name__ == '__main__':
    unittest.main()
