"""Reviewed input bindings, generation-owned copies and publication orchestration.

These services do not log in, start workers or publish on import. Caller supplies
the fixed local storage and already authorized SDK adapter.
"""
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import os
import re

from sqlalchemy import text

from .auth import AuthError, digest
from .ingest_formats import FormatError, Limits, inspect_fd, parser_fingerprint
from .reconciliation import ReconciliationRequired, reconcile, report_digest
from .jobs import JobError, identifier, json_object
from .publication import PublicationError, object_path
from .uploads import ACTIVE_OPS, upload_uuid


class IngestionError(JobError):
    pass


class IngestionReviewRequired(IngestionError):
    def __init__(self, reason, *, report=None):
        self.report = report
        self.reason = identifier(reason, 80)
        super().__init__('Manual ingestion review required: ' + reason)


class TusPartReader:
    """One trusted StorageRoot prefix; the stored server ID is the sole filename."""
    def __init__(self, root, prefix='uploads/tus'):
        self.root, self.prefix = root, prefix

    @contextmanager
    def __call__(self, item):
        if not isinstance(item['tus_id'], str) or not re.fullmatch('[0-9a-f]{32}', item['tus_id']):
            raise IngestionError('Invalid stored tus identity')
        with self.root.file(self.prefix + '/' + item['tus_id']) as fd:
            yield fd


class IngestionService:
    def __init__(self, publisher, uploads, sources, *, upload_reader=None, preview_builder=None):
        if publisher.engine is not uploads.engine or publisher.engine is not sources.engine:
            raise ValueError('One ingestion transaction engine required')
        self.publisher, self.uploads, self.sources = publisher, uploads, sources
        self.engine, self.jobs, self.root = publisher.engine, publisher.jobs, publisher.root
        self.auth = uploads.auth
        self.upload_reader = upload_reader or TusPartReader(self.root)
        self.preview_builder = preview_builder
        publisher.sources = sources

    @staticmethod
    def _edition(conn, edition_id, expected_revision_id):
        edition = conn.execute(text('SELECT * FROM work_editions WHERE id=:id'), dict(id=edition_id)).mappings().first()
        active = conn.execute(text('SELECT id FROM edition_revisions WHERE edition_id=:id AND is_active=1'), dict(id=edition_id)).scalar()
        if not edition or active != expected_revision_id:
            raise IngestionError('Explicit edition or active revision differs')
        return edition

    def enqueue_upload(self, token, csrf, *, upload_id, edition_id, expected_revision_id=None):
        """Member chooses an existing edition; rights remain review until admin decision."""
        upload_uuid(upload_id)
        with self.jobs._writer() as (conn, _now):
            item = self.uploads._upload(conn, token, csrf, upload_id)
            return self._enqueue_upload_in(conn, item, upload_id, edition_id, expected_revision_id)

    def bind_upload(self, token, csrf, *, upload_id, edition_id, expected_revision_id=None):
        """Explicit administrator binding, preserving the original owner's authority."""
        upload_uuid(upload_id)
        with self.jobs._writer() as (conn, _now):
            self._admin(conn, token, csrf)
            item = conn.execute(text('SELECT * FROM upload_sessions WHERE id=:id'),dict(id=upload_id)).mappings().first()
            if item is None:
                raise AuthError('RESOURCE_NOT_FOUND',404)
            return self._enqueue_upload_in(conn, item, upload_id, edition_id, expected_revision_id)

    def _enqueue_upload_in(self, conn, item, upload_id, edition_id, expected_revision_id):
        self.publisher.permissions.check_upload_in(conn, upload_id)
        owner = conn.execute(text('SELECT uploader_id FROM upload_batches WHERE id=:id'),dict(id=item['batch_id'])).scalar()
        if owner != item['owner_id']:
            raise IngestionError('Upload batch ownership differs')
        edition = self._edition(conn, edition_id, expected_revision_id)
        reservation = conn.execute(text('SELECT display_filename FROM upload_reservations WHERE upload_id=:id'), dict(id=upload_id)).first()
        if (not reservation or not item['tus_id'] or conn.execute(text(
                'SELECT 1 FROM upload_transport_ops WHERE upload_id=:id AND state IN ' + ACTIVE_OPS), dict(id=upload_id)).first()):
            raise IngestionError('Transfer must be fully staged without outstanding operations')
        payload = dict(upload_id=upload_id, edition_id=edition_id, expected_revision_id=expected_revision_id)
        fingerprint = digest(json_object(payload))
        old = conn.execute(text('SELECT job_id,request_digest FROM ingestion_inputs WHERE upload_id=:id'), dict(id=upload_id)).mappings().first()
        if old:
            if old['request_digest'] != fingerprint:
                raise IngestionError('Upload catalog binding conflict')
            return old['job_id']
        job = self.jobs.enqueue_in(conn, kind='ingest.review_upload', idempotency_key='ingest-upload:' + upload_id,
                                   payload=payload, work_id=edition['work_id'], input_revision_id=expected_revision_id,
                                   upload_session_id=upload_id, max_attempts=10)
        values = dict(job=job, edition=edition_id, previous=expected_revision_id, upload=upload_id,
                      filename=reservation.display_filename, size=item['declared_size'], tus=item['tus_id'],
                      actor=item['owner_id'], fingerprint=fingerprint)
        conn.execute(text('''INSERT INTO ingestion_inputs(job_id,edition_id,expected_revision_id,upload_id,
            filename,byte_size,tus_id,rights_status,rights_receipt,decision_actor,request_digest)
            VALUES(:job,:edition,:previous,:upload,:filename,:size,:tus,'review','pending',:actor,:fingerprint)
            ON CONFLICT(job_id) DO NOTHING'''), values)
        return job

    def bind_source(self, token, csrf, lease, *, edition_id, expected_revision_id=None):
        """Explicit administrator catalog choice for one already observed attachment."""
        with self.jobs.ownership(lease) as conn:
            actor = self._admin(conn, token, csrf)
            self._edition(conn, edition_id, expected_revision_id)
            item = self.sources._download(conn, lease)
            scan = conn.execute(text('SELECT event_epoch FROM source_scans WHERE id=:id'), dict(id=item['scan_id'])).one()
            metadata = json.loads(item['metadata_json'])
            values = dict(job=lease.job_id, edition=edition_id, previous=expected_revision_id, observation=item['id'],
                          version=item['config_version'], epoch=scan.event_epoch, filename=metadata['filename'],
                          size=metadata['byte_size'], actor=actor['id'])
            fingerprint = digest(json_object(values))
            old = conn.execute(text('SELECT request_digest FROM ingestion_inputs WHERE job_id=:job'), values).scalar()
            if old and old != fingerprint:
                raise IngestionError('Catalog binding conflict')
            conn.execute(text('''INSERT INTO ingestion_inputs(job_id,edition_id,expected_revision_id,observation_id,
                source_config_version,source_event_epoch,filename,byte_size,rights_status,rights_receipt,decision_actor,request_digest)
                VALUES(:job,:edition,:previous,:observation,:version,:epoch,:filename,:size,'review','pending',:actor,:fingerprint)
                ON CONFLICT(job_id) DO NOTHING'''), dict(values, fingerprint=fingerprint))
            if not old:
                conn.execute(text("UPDATE jobs SET kind='ingest.review_source' WHERE id=:id"), dict(id=lease.job_id))
            return lease.job_id

    def _admin(self, conn, token, csrf):
        if not isinstance(csrf, str) or not csrf:
            raise AuthError('csrf_rejected', 403)
        return self.auth.current(conn, token, csrf=csrf, admin=True)

    def review_input(self, token, csrf, job_id, *, rights_status, rights_receipt):
        if rights_status not in {'allowed', 'review', 'blocked'}:
            raise ValueError('Explicit rights decision required')
        identifier(rights_receipt, 200)
        with self.jobs._writer() as (conn, _now):
            actor = self._admin(conn, token, csrf)
            self._review_input_in(conn, actor, job_id, rights_status, rights_receipt)

    def review_upload(self, token, csrf, upload_id, *, rights_status, rights_receipt):
        upload_uuid(upload_id)
        if rights_status not in {'allowed', 'review', 'blocked'}:
            raise ValueError('Explicit rights decision required')
        identifier(rights_receipt, 200)
        with self.jobs._writer() as (conn, _now):
            actor = self._admin(conn, token, csrf)
            binding = conn.execute(text("""SELECT i.job_id FROM ingestion_inputs i JOIN jobs j ON j.id=i.job_id
                AND j.upload_session_id=i.upload_id WHERE i.upload_id=:id
                AND j.kind IN ('ingest_upload','ingest.review_upload','ingest.validation_review')"""),dict(id=upload_id)).first()
            if binding is None:
                raise IngestionError('Unknown upload ingestion binding')
            previous = conn.execute(text('SELECT rights_status,rights_receipt,decision_actor FROM ingestion_inputs WHERE job_id=:id'),dict(id=binding.job_id)).first()
            if (previous.rights_status,previous.rights_receipt,previous.decision_actor) == (rights_status,rights_receipt,actor['id']):
                return
            self._review_input_in(conn, actor, binding.job_id, rights_status, rights_receipt)

    def _review_input_in(self, conn, actor, job_id, rights_status, rights_receipt):
        job = conn.execute(text('SELECT kind,state,attempts,max_attempts FROM jobs WHERE id=:id'),
                           dict(id=job_id)).mappings().first()
        if (rights_status == 'allowed' and job and job['state'] in {'queued', 'retry_wait'}
                and job['attempts'] >= job['max_attempts']):
            raise IngestionError('Ingestion attempts exhausted; new execution cannot be authorized')
        changed = conn.execute(text('''UPDATE ingestion_inputs SET rights_status=:status,
            rights_receipt=:receipt,decision_actor=:actor WHERE job_id=:job'''),
            dict(job=job_id, status=rights_status, receipt=rights_receipt, actor=actor['id']))
        if changed.rowcount != 1:
            raise IngestionError('Unknown ingestion binding')
        conn.execute(text('UPDATE publication_plans SET rights_status=:status WHERE job_id=:job'),
                     dict(job=job_id, status=rights_status))
        upload = conn.execute(text('SELECT upload_id FROM ingestion_inputs WHERE job_id=:id'), dict(id=job_id)).scalar()
        ready = 'ingest_upload' if upload else 'ingest_source'
        waiting = 'ingest.review_upload' if upload else 'ingest.review_source'
        conn.execute(text('UPDATE jobs SET kind=:kind WHERE id=:id'),
                     dict(id=job_id, kind=ready if rights_status == 'allowed' else waiting))
        self.auth.audit(conn, actor['id'], 'ingestion.rights', details=dict(job_id=job_id, status=rights_status, receipt=rights_receipt))

    def input(self, lease):
        with self.jobs.ownership(lease) as conn:
            row = conn.execute(text('SELECT * FROM ingestion_inputs WHERE job_id=:id'), dict(id=lease.job_id)).mappings().first()
            if not row or row['rights_status'] != 'allowed':
                raise IngestionError('Current reviewed input required')
            item = dict(row)
            self._edition(conn, item['edition_id'], item['expected_revision_id'])
            if item['upload_id']:
                self.publisher.permissions.check_upload_in(conn, item['upload_id'])
                upload = conn.execute(text('SELECT tus_id,declared_size FROM upload_sessions WHERE id=:id'), dict(id=item['upload_id'])).one()
                if upload.tus_id != item['tus_id'] or upload.declared_size != item['byte_size']:
                    raise IngestionError('Staged upload identity changed')
            else:
                current = self.sources._download(conn, lease)
                source = self.sources._source(conn, current['source_id'], item['source_config_version'])
                if current['id'] != item['observation_id'] or source['event_epoch'] != item['source_event_epoch']:
                    raise IngestionError('Source observation epoch changed')
            return item

    def resume_publication(self, lease):
        self.input(lease)
        return self.publisher.publish(lease)

    def _has_plan(self, lease):
        with self.jobs.ownership(lease) as conn:
            return conn.execute(text('SELECT 1 FROM publication_plans WHERE job_id=:id AND generation=:generation'),
                                dict(id=lease.job_id, generation=lease.generation)).first() is not None

    def process_upload(self, lease):
        item = self.input(lease)
        if self._has_plan(lease):
            return self.resume_publication(lease)
        if not item['upload_id']:
            raise IngestionError('Upload job required')
        with self.upload_reader(item) as source, self.root.stage(lease) as (path, target):
            before = self.publisher._fingerprint(source)
            copied = 0
            while chunk := os.read(source, 1024**2):
                copied += len(chunk)
                if copied > item['byte_size']:
                    raise IngestionError('Staged source exceeds declared size')
                self._write(target, chunk)
            if copied != item['byte_size'] or self.publisher._fingerprint(source) != before:
                raise IngestionError('Staged source changed while copying')
        return self.process_stage(lease, path)

    @staticmethod
    def _write(fd, data):
        view = memoryview(data)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise IngestionError('Incomplete staging write')
            view = view[count:]

    async def process_source(self, lease, adapter):
        item = self.input(lease)
        if self._has_plan(lease):
            return self.resume_publication(lease)
        if not item['observation_id']:
            raise IngestionError('Source job required')
        with self.root.stage(lease) as (path, target):
            await adapter.download_to(lease, target)
        return self.process_stage(lease, path)

    def _preview(self, lease, stage_path, item, report):
        builder = self.preview_builder
        if builder is None:
            from .previews import build_preview
            builder = build_preview
        manifest = builder(self.root, lease, stage_path, item['filename'], report=report)
        if (not isinstance(manifest, dict) or manifest.get('source_sha256') != report['sha256']
                or manifest.get('source_byte_size') != report['byte_size']):
            raise IngestionError('Preview source binding differs')
        if manifest.get('kind') == 'unavailable':
            if any(key.startswith('bundle_') for key in manifest):
                raise IngestionError('Unavailable preview cannot claim cache bytes')
            json_object(manifest)
            return manifest
        path = manifest.get('bundle_relpath')
        if not isinstance(path, str) or not path.startswith(lease.staging_key + '/'):
            raise IngestionError('Preview belongs to another execution')
        object_path(manifest.get('bundle_sha256'))
        size = manifest.get('bundle_size')
        if type(size) is not int or not 0 < size <= 128 * 1024**2:
            raise IngestionError('Invalid preview cache size')
        self.publisher._verify_file(path, dict(sha256=manifest['bundle_sha256'], byte_size=size))
        json_object(manifest)
        return manifest

    def _preserved(self, conn, previous):
        assets, options = [], []
        if previous is None:
            return assets, options
        mapping = {}
        for row in conn.execute(text('''SELECT a.*,o.byte_size,o.detected_mime FROM revision_assets a
                JOIN objects o ON o.sha256=a.object_sha WHERE a.edition_revision_id=:id ORDER BY asset_position'''), dict(id=previous)).mappings():
            mapping[row['asset_position']] = len(assets)
            assets.append(dict(sha256=row['object_sha'], byte_size=row['byte_size'], detected_mime=row['detected_mime'],
                               file_format=row['file_format'], asset_role=row['asset_role'], prepared_relpath=None))
        for row in conn.execute(text('SELECT * FROM revision_options WHERE edition_revision_id=:id ORDER BY id'), dict(id=previous)).mappings():
            positions = conn.execute(text('SELECT asset_position FROM option_assets WHERE option_id=:id ORDER BY ordinal'), dict(id=row['id'])).scalars()
            options.append(dict(id=row['id'], format=row['format'], delivery_kind=row['delivery_kind'], positions=[mapping[p] for p in positions],
                                coverage=json.loads(row['coverage_json'])))
        return assets, options

    def _old_reports(self, item, assets, options):
        reports, cache = {}, {}
        needed = {p for option in options if option['format'] == item['format'] for p in option['positions']}
        for position in sorted(needed):
            asset = assets[position]
            key = (asset['sha256'], asset['file_format'])
            if key in cache:
                reports[position] = cache[key]
                continue
            def opened(stack):
                with self.engine.connect() as conn:
                    self._edition(conn, item['edition_id'], item['expected_revision_id'])
                    self.publisher._asset_sources(conn, None, dict(assets=[asset], expected_revision_id=item['expected_revision_id']))
                    row = conn.execute(text("SELECT state,local_relpath FROM objects WHERE sha256=:sha"), dict(sha=asset['sha256'])).first()
                    if not row or row.state != 'LIVE' or row.local_relpath != object_path(asset['sha256']):
                        raise IngestionReviewRequired('old_original_unavailable')
                return stack.enter_context(self.root.file(object_path(asset['sha256'])))
            try:
                # Open and duplicate under the reader lock on this thread. Drop
                # locks BEFORE starting the parser subprocess; final publication
                # hashes the original again under its own non-nested lock scope.
                with self.publisher.locks.readers([asset['sha256']], opened) as source:
                    fd = os.dup(source)
                try:
                    report = inspect_fd(fd, 'original.' + asset['file_format'])
                finally:
                    os.close(fd)
            except (FileNotFoundError, FormatError) as error:
                raise IngestionReviewRequired('old_original_unavailable') from error
            if (report['sha256'] != asset['sha256'] or report['byte_size'] != asset['byte_size']
                    or report['status'] != 'validated' or report.get('metadata', {}).get('review_reason')):
                raise IngestionReviewRequired('old_original_not_validated')
            reports[position] = cache[key] = report
        return reports

    def park_review(self, lease, error):
        with self.jobs._writer() as (conn, _now):
            self.jobs._cas(conn, lease, "lease_generation=lease_generation", {})
            checkpoint = dict(review_required=True, reason=error.reason)
            if error.report is not None:
                item = conn.execute(text('SELECT * FROM ingestion_inputs WHERE job_id=:id'), dict(id=lease.job_id)).mappings().one()
                if item['rights_status'] != 'allowed' or error.report['byte_size'] != item['byte_size']:
                    raise IngestionError('Review input authority changed')
                self._edition(conn, item['edition_id'], item['expected_revision_id'])
                if item['observation_id']:
                    current = self.sources._download(conn, lease)
                    source = self.sources._source(conn, current['source_id'], item['source_config_version'])
                    if current['id'] != item['observation_id'] or source['event_epoch'] != item['source_event_epoch']:
                        raise IngestionError('Review source epoch changed')
                    self.sources.content_in(conn, lease, sha256=error.report['sha256'], byte_size=error.report['byte_size'])
                    self.jobs.enqueue_in(conn, kind='backup_catalog', idempotency_key=f'backup-review:{lease.job_id}:{lease.generation}',
                                         payload={'ingestion_job_id': lease.job_id}, input_revision_id=item['expected_revision_id'])
                    conn.execute(text('UPDATE storage_sequence SET value=value+1 WHERE id=1'))
                else:
                    self.publisher.permissions.check_upload_in(conn, item['upload_id'])
                checkpoint.update(sha256=error.report['sha256'], byte_size=error.report['byte_size'])
            self.jobs._cas(conn, lease,
                "kind='ingest.validation_review',state='queued',lease_owner=NULL,lease_until=NULL,"
                "error_code='manual_review_required',checkpoint_json=:checkpoint",
                {'checkpoint': json_object(checkpoint)})

    def process_stage(self, lease, stage_path):
        item = self.input(lease)
        if not stage_path.startswith(lease.staging_key + '/'):
            raise IngestionError('Stage belongs to another execution')
        with self.root.file(stage_path) as fd:
            report = inspect_fd(fd, item['filename'])
        if report['byte_size'] != item['byte_size']:
            raise IngestionError('Actual input size differs')
        if report.get('status') == 'review':
            raise IngestionReviewRequired(report.get('reason') or 'unvalidated_format')
        if report.get('status') != 'validated':
            raise IngestionError('Validated actual input required')
        if report.get('metadata', {}).get('review_reason'):
            raise IngestionReviewRequired(report['metadata']['review_reason'])
        with self.engine.connect() as conn:
            edition = self._edition(conn, item['edition_id'], item['expected_revision_id'])
            if edition['representation'] != report['representation']:
                raise IngestionReviewRequired('edition_representation_mismatch')
        with self.engine.connect() as conn:
            assets, old_options = self._preserved(conn, item['expected_revision_id'])
        try:
            reports = self._old_reports(dict(item, format=report['format']), assets, old_options)
            decision = reconcile(report, old_options, reports)
        except ReconciliationRequired as error:
            raise IngestionReviewRequired(error.reason, report=report) from error
        envelope = dict(outcome=decision['outcome'], proof_digest=report_digest(dict(new=report, old=reports, decision=decision)),
                        parser_sha256=parser_fingerprint(),
                        limits=asdict(Limits()))
        comparison_assets = list(assets)
        preview = self._preview(lease, stage_path, item, report)
        asset = self.publisher.prepare(lease, stage_path, sha256=report['sha256'], byte_size=report['byte_size'],
                                       detected_mime=self._mime(report['format']), file_format=report['format'])
        with self.jobs.ownership(lease) as conn:
            old = conn.execute(text('SELECT * FROM ingestion_runs WHERE job_id=:id AND generation=:generation'), dict(id=lease.job_id, generation=lease.generation)).mappings().first()
            if old and (old['sha256'] != report['sha256'] or old['byte_size'] != report['byte_size']):
                raise IngestionError('Actual input changed within one execution')
            if old:
                raise IngestionError('Execution already prepared; resume its publication plan')
            conn.execute(text('INSERT INTO ingestion_runs VALUES(:id,:generation,:sha,:size,:preview)'),
                         dict(id=lease.job_id, generation=lease.generation, sha=report['sha256'], size=report['byte_size'], preview=json_object(preview)))
        new_position = len(assets)
        assets.append(asset)
        options = [{key: value for key, value in option.items() if key != 'id'}
                   for option in old_options if option['id'] not in decision['replaces']]
        selected = [new_position if p == 'new' else p for p in decision['positions']]
        option = dict(format=report['format'], delivery_kind='bundle' if len(selected) > 1 else 'single_file',
                      positions=selected, coverage=decision['coverage'])
        if decision['replaces']:
            option.update(replaces_option_ids=decision['replaces'], replacement_receipt='overlap:' + envelope['proof_digest'])
        if decision['outcome'] == 'exact_duplicate':
            assets, options = [asset], [dict(option, positions=[0])]
        else:
            options.append(option)
            used = sorted({p for option in options for p in option['positions']})
            mapping = {position: index for index, position in enumerate(used)}
            assets = [assets[p] for p in used]
            options = [dict(option, positions=[mapping[p] for p in option['positions']]) for option in options]
        register = self.publisher.register_validated_source if item['observation_id'] else self.publisher.register_validated_upload
        if len(assets) > 200 or len(options) > 64:
            raise IngestionReviewRequired('reconciliation_plan_limit', report=report)
        try:
            json_object(dict(assets=assets, options=options, reconciliation=envelope, comparison_assets=comparison_assets,
                             edition_id=item['edition_id'], expected_revision_id=item['expected_revision_id'], revision_id='x' * 36))
        except ValueError as error:
            raise IngestionReviewRequired('reconciliation_plan_limit', report=report) from error
        register(lease, edition_id=item['edition_id'], expected_revision_id=item['expected_revision_id'], assets=assets,
                 options=options, validation_receipt='sha256:' + report['sha256'], rights_status='allowed',
                 reconciliation=envelope, comparison_assets=comparison_assets)
        return self.publisher.publish(lease)

    @staticmethod
    def _mime(file_format):
        # Parser-validated formats only; one stable type for each physical SHA.
        if file_format in {'txt', 'md', 'markdown', 'csv', 'html', 'htm', 'fb2'}:
            return 'text/plain'
        if file_format in {'zip', 'cbz', 'epub', 'docx', 'odt'}:
            return 'application/zip'
        if file_format == 'pdf':
            return 'application/pdf'
        return {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'tif': 'image/tiff',
                'tiff': 'image/tiff'}.get(file_format, 'image/' + file_format)


class IngestionWorker:
    """Explicit one-job runner with process-local heartbeat and durable lease fencing.

    No loop, service activation or external login is implicit. A killed process
    loses its heartbeat; a replacement claims a new generation after expiry.
    Parser subprocesses and SDK I/O never own the SQLite writer.
    """
    def __init__(self, service, *, owner, lease_ms=300000):
        identifier(owner)
        service.jobs._duration(lease_ms)
        self.service, self.owner, self.lease_ms = service, owner, lease_ms

    @contextmanager
    def _heartbeat(self, lease):
        import threading
        stopped = threading.Event()
        failures = []
        def maintain():
            while not stopped.wait(self.lease_ms / 3000):
                try:
                    self.service.jobs.heartbeat(lease, lease_ms=self.lease_ms)
                except Exception as error:
                    failures.append(type(error).__name__)
                    return
        thread = threading.Thread(target=maintain, name='ingestion-lease', daemon=True)
        thread.start()
        try:
            yield
        finally:
            stopped.set()
            thread.join()
        # Publication's final live CAS is authoritative even if the heartbeat
        # observed completion just before shutdown. Unfinished work fails closed.
        job = self.service.jobs.get(lease.job_id)
        if failures and job['state'] == 'running' and job['lease_generation'] == lease.generation:
            raise IngestionError('Worker heartbeat failed; publication requires a fresh live lease')

    async def run_once(self, *, kind='ingest_upload', source_adapter=None):
        if kind not in {'ingest_upload', 'source.download'}:
            raise ValueError('Explicit ingestion job kind required')
        if kind == 'source.download' and source_adapter is None:
            raise ValueError('Injected source adapter required')
        claim_kind = 'ingest_source' if kind == 'source.download' else kind
        lease = self.service.jobs.claim(self.owner, kind=claim_kind, lease_ms=self.lease_ms)
        if lease is None:
            return None
        try:
            with self._heartbeat(lease):
                if kind == 'ingest_upload':
                    return self.service.process_upload(lease)
                return await self.service.process_source(lease, source_adapter)
        except IngestionReviewRequired as error:
            self.service.park_review(lease, error)
            raise
        except Exception:
            from .jobs import LostLease
            job = self.service.jobs.get(lease.job_id)
            if job['state'] == 'running' and job['lease_generation'] == lease.generation:
                try:
                    self.service.jobs.fail(lease, 'ingestion_failed')
                except LostLease:
                    pass
            raise
