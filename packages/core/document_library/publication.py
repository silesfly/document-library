"""Filesystem installation plus atomic upload/revision/job publication.

The ingestion validator supplies a reviewed plan; no unvalidated HTTP payload is
accepted here. Backup delivery and GC policy consume the durable records later.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import threading
import uuid

from sqlalchemy import text

from .auth import digest, stamp
from .database import write_transaction
from .jobs import identifier, json_object
from .storage import StorageError, StorageLocks, sync_file
from .uploads import ACTIVE_OPS


class PublicationError(StorageError):
    pass


class OpenedSelection:
    """Private, thread-owned originals. Logical members may share one descriptor."""
    def __init__(self, snapshot, descriptors, stack, resolve):
        self.snapshot, self._descriptors, self._stack = snapshot, descriptors, stack
        self._resolve, self._owner = resolve, threading.get_ident()
        self._closed, self._fingerprints = False, {}

    def _check(self):
        if self._closed or threading.get_ident() != self._owner:
            raise PublicationError('Original reader belongs to its live owning thread')

    def fd_for(self, asset):
        self._check()
        if asset not in self.snapshot.assets:
            raise PublicationError('Asset is outside the fixed selection')
        return self._descriptors[asset.object_sha]

    def enter_context(self, context):
        self._check()
        return self._stack.enter_context(context)

    def revalidate(self):
        self._check()
        current = self._resolve()  # caller opens a fresh, short read transaction
        if current != self.snapshot:
            raise PublicationError('Selected authority or immutable inputs changed')
        for sha, fd in self._descriptors.items():
            if PublicationService._fingerprint(fd) != self._fingerprints[sha]:
                raise PublicationError('Selected original changed after hashing')
        return current

    def chunks(self, asset, *, start=0, length=None, block_size=65536):
        self._check()
        fd = self.fd_for(asset)
        length = asset.byte_size - start if length is None else length
        if (type(start) is not int or type(length) is not int or start < 0 or length < 0
                or start + length > asset.byte_size or type(block_size) is not int
                or not 1 <= block_size <= 65536):
            raise PublicationError('Bounded original byte interval required')
        end = start + length
        while start < end:
            self._check()
            data = os.pread(fd, min(block_size, end-start), start)
            if not data:
                raise PublicationError('Selected original truncated during transfer')
            start += len(data)
            yield data


def object_path(sha):
    if not isinstance(sha, str) or not re.fullmatch('[0-9a-f]{64}', sha):
        raise PublicationError('Invalid object SHA-256')
    return f'objects/{sha[:2]}/{sha[2:4]}/{sha}'


def _coverage(value):
    if not isinstance(value, dict) or set(value) != {'unit','intervals','gaps','confidence'}:
        raise PublicationError('Explicit coverage fields required')
    if value['unit'] not in {'episode','volume','page','unknown'} or value['confidence'] not in {'verified','inferred','unknown'}:
        raise PublicationError('Invalid coverage type')
    for key in ('intervals','gaps'):
        previous = 0
        if not isinstance(value[key], list):
            raise PublicationError('Coverage ranges required')
        for interval in value[key]:
            if (not isinstance(interval, list) or len(interval) != 2
                    or any(type(n) is not int for n in interval) or not previous < interval[0] <= interval[1]):
                raise PublicationError('Sorted, disjoint positive coverage ranges required')
            previous = interval[1]
    if any(a <= d and c <= b for a,b in value['intervals'] for c,d in value['gaps']):
        raise PublicationError('Known intervals cannot also be gaps')
    if value['unit'] == 'unknown' and (value['intervals'] or value['gaps']):
        raise PublicationError('Unknown coverage cannot claim ranges')


class PublicationService:
    def __init__(self, root, jobs, permissions):
        self.root, self.jobs, self.permissions = root, jobs, permissions
        self.sources = None
        if jobs.engine is not permissions.engine:
            raise ValueError('One database engine required for publication')
        self.engine = jobs.engine
        self.locks = StorageLocks(root)

    def _prepared_prefix(self, lease):
        return 'quarantine/prepared/' + lease.staging_key + '/'

    def prepare(self, lease, stage_path, *, sha256, byte_size, detected_mime, file_format):
        """Copy worker-owned bytes to a new publisher file; an old fd cannot alter it."""
        object_path(sha256)
        if not stage_path.startswith(lease.staging_key + '/') or type(byte_size) is not int or byte_size < 0:
            raise PublicationError('Stage must belong to this generation with an explicit size')
        identifier(file_format, 32)
        if not isinstance(detected_mime, str) or not re.fullmatch(r'[a-z0-9.+-]+/[a-z0-9.+-]+', detected_mime):
            raise PublicationError('Validated MIME required')
        prepared = self._prepared_prefix(lease) + uuid.uuid4().hex + '.bin'
        hasher, total = hashlib.sha256(), 0
        with self.root.file(stage_path) as source, self.root.file(prepared, create=True) as target:
            with os.fdopen(os.dup(target), 'wb') as output:
                while chunk := os.read(source, 1024 * 1024):
                    total += len(chunk)
                    if total > byte_size:
                        raise PublicationError('Stage exceeds validated size')
                    hasher.update(chunk)
                    output.write(chunk)
        if total != byte_size or hasher.hexdigest() != sha256:
            raise PublicationError('Stage differs from validated bytes')
        return dict(sha256=sha256, byte_size=byte_size, detected_mime=detected_mime,
                    file_format=file_format, prepared_relpath=prepared, asset_role='content')

    def _validate_plan(self, lease, plan):
        if plan.get('schema') == 'metadata.cover.v1':
            raise PublicationError('Metadata cover plan cannot publish an original revision')
        assets, options = plan['assets'], plan['options']
        if not isinstance(assets,list) or not 1 <= len(assets) <= 200 or not isinstance(options,list) or not 1 <= len(options) <= 64:
            raise PublicationError('Bounded nonempty assets and options required')
        comparisons = plan.get('comparison_assets', [])
        if (not isinstance(comparisons, list) or len(comparisons) > 200
                or any(a.get('prepared_relpath') is not None for a in comparisons)):
            raise PublicationError('Bounded existing comparison originals required')
        decision = plan.get('reconciliation')
        if decision is not None:
            if (set(decision) != {'outcome', 'proof_digest', 'parser_sha256', 'limits'}
                    or decision['outcome'] not in {'initial', 'add_format', 'exact_duplicate', 'replace_superset', 'append_bundle'}):
                raise PublicationError('Invalid reconciliation decision')
            object_path(decision['proof_digest'])
            object_path(decision['parser_sha256'])
            from .ingest_formats import parser_fingerprint
            if decision['parser_sha256'] != parser_fingerprint():
                raise PublicationError('Reconciliation parser changed; a fresh execution is required')
            if not isinstance(decision['limits'], dict) or not decision['limits']:
                raise PublicationError('Explicit parser limits required')
        elif comparisons:
            raise PublicationError('Comparison originals require a decision')
        metadata = {}
        for asset in assets + comparisons:
            if set(asset) != {'sha256','byte_size','detected_mime','file_format','prepared_relpath','asset_role'}:
                raise PublicationError('Unexpected asset fields')
            object_path(asset['sha256'])
            identifier(asset['file_format'],32)
            if (type(asset['byte_size']) is not int or asset['byte_size'] < 0
                    or not re.fullmatch(r'[a-z0-9.+-]+/[a-z0-9.+-]+',asset['detected_mime'])
                    or asset['asset_role'] not in {'content','ocr'}):
                raise PublicationError('Invalid asset metadata')
            if asset['prepared_relpath'] is not None and not asset['prepared_relpath'].startswith(self._prepared_prefix(lease)):
                raise PublicationError('Prepared asset belongs to another execution')
            pair = (asset['byte_size'],asset['detected_mime'])
            if asset['sha256'] in metadata and metadata[asset['sha256']] != pair:
                raise PublicationError('One object cannot have conflicting size/MIME metadata')
            metadata[asset['sha256']] = pair
        used = set()
        for option in options:
            if not {'format','delivery_kind','positions','coverage'} <= set(option) or set(option) - {'format','delivery_kind','positions','coverage','replaces_option_id','replaces_option_ids','replacement_receipt'}:
                raise PublicationError('Unexpected option fields')
            identifier(option['format'],32)
            positions = option['positions']
            if (option['delivery_kind'] not in {'single_file','bundle'} or not isinstance(positions,list) or not positions
                    or any(type(p) is not int or not 0 <= p < len(assets) for p in positions)
                    or len(set(positions)) != len(positions)
                    or (option['delivery_kind'] == 'single_file' and len(positions) != 1)):
                raise PublicationError('Invalid ordered option assets')
            replacements = option.get('replaces_option_ids')
            if replacements is not None:
                if (not isinstance(replacements, list) or not 1 <= len(replacements) <= 64
                        or any(not isinstance(value, str) for value in replacements) or len(set(replacements)) != len(replacements)
                        or decision is None or decision['outcome'] not in {'replace_superset', 'append_bundle'}
                        or option.get('replacement_receipt') != 'overlap:' + decision['proof_digest']):
                    raise PublicationError('Replacement identities require the actual comparison receipt')
            _coverage(option['coverage'])
            used.update(positions)
        if used != set(range(len(assets))):
            raise PublicationError('Every asset must belong to an option')
        json_object(plan)

    def _binding(self, conn, lease, plan):
        job = conn.execute(text('SELECT * FROM jobs WHERE id=:id'),dict(id=lease.job_id)).mappings().one()
        if job['kind'].startswith('metadata.') or conn.execute(text('SELECT 1 FROM metadata_job_inputs WHERE job_id=:id'),dict(id=lease.job_id)).first():
            raise PublicationError('Metadata job cannot publish an original revision')
        edition = conn.execute(text('SELECT work_id FROM work_editions WHERE id=:id'),dict(id=plan['edition_id'])).first()
        binding = conn.execute(text('SELECT * FROM ingestion_inputs WHERE job_id=:id'),
                               dict(id=lease.job_id)).mappings().first()
        source_job = binding is not None and binding['observation_id'] is not None
        if (not edition or (not source_job and (not job['upload_session_id']
                or job['work_id'] != edition.work_id or job['input_revision_id'] != plan['expected_revision_id']))):
            raise PublicationError('Job input/upload/work binding differs')
        if 'reconciliation' in plan and not binding:
            raise PublicationError('Reconciliation requires an explicit ingestion binding')
        if binding:
            self._ingestion_binding(conn, lease, plan, job, binding)
        current = conn.execute(text('SELECT id FROM edition_revisions WHERE edition_id=:id AND is_active=1'),dict(id=plan['edition_id'])).scalar()
        if current != plan['expected_revision_id']:
            raise PublicationError('Active input revision changed')
        if not source_job:
            self.permissions.check_upload_in(conn, job['upload_session_id'])
        self._asset_sources(conn,lease,plan)
        self._comparison_binding(conn, lease, plan)
        return job

    @staticmethod
    def _duplicate(plan):
        return plan.get('reconciliation', {}).get('outcome') == 'exact_duplicate'

    def _comparison_binding(self, conn, lease, plan):
        if 'reconciliation' not in plan:
            return
        previous = plan['expected_revision_id']
        expected = []
        if previous is not None:
            if not conn.execute(text('SELECT 1 FROM revision_seals WHERE revision_id=:id'), dict(id=previous)).first():
                raise PublicationError('Comparison revision must be sealed')
            for row in conn.execute(text("""SELECT a.*,o.byte_size,o.detected_mime,o.state,o.local_relpath FROM revision_assets a
                    JOIN objects o ON o.sha256=a.object_sha WHERE a.edition_revision_id=:id ORDER BY asset_position"""), dict(id=previous)).mappings():
                if row['state'] != 'LIVE' or row['local_relpath'] != object_path(row['object_sha']):
                    raise PublicationError('Comparison original is no longer LIVE')
                expected.append(dict(sha256=row['object_sha'], byte_size=row['byte_size'], detected_mime=row['detected_mime'],
                                     file_format=row['file_format'], asset_role=row['asset_role'], prepared_relpath=None))
        if plan.get('comparison_assets') != expected:
            raise PublicationError('Comparison original snapshot differs')
        self._asset_sources(conn, lease, dict(assets=expected, expected_revision_id=previous))
        formats = dict(conn.execute(text('SELECT id,format FROM revision_options WHERE edition_revision_id=:id'), dict(id=previous)).all())
        for option in plan['options']:
            if any(formats.get(ident) != option['format'] for ident in option.get('replaces_option_ids', [])):
                raise PublicationError('Replacement identities must name old options of the same format')
        if self._duplicate(plan):
            fresh = [a for a in plan['assets'] if a['prepared_relpath'] is not None]
            if (previous is None or plan['revision_id'] != previous or len(fresh) != 1
                    or not any(all(old[key] == fresh[0][key] for key in ('sha256', 'byte_size', 'file_format', 'asset_role')) for old in expected)):
                raise PublicationError('Exact duplicate must reference an existing original')

    def _ingestion_binding(self, conn, lease, plan, job, binding):
        if (binding['edition_id'] != plan['edition_id']
                or binding['expected_revision_id'] != plan['expected_revision_id']
                or binding['rights_status'] != 'allowed'):
            raise PublicationError('Current catalog/rights decision differs')
        run = conn.execute(text('SELECT * FROM ingestion_runs WHERE job_id=:id AND generation=:generation'),
                           dict(id=lease.job_id, generation=lease.generation)).mappings().first()
        fresh = [a for a in plan['assets'] if a['prepared_relpath'] is not None]
        if (not run or len(fresh) != 1 or fresh[0]['sha256'] != run['sha256']
                or fresh[0]['byte_size'] != run['byte_size'] or run['byte_size'] != binding['byte_size']):
            raise PublicationError('Actual input digest/generation binding differs')
        if binding['upload_id']:
            upload = conn.execute(text('SELECT tus_id,declared_size FROM upload_sessions WHERE id=:id'),
                                  dict(id=binding['upload_id'])).first()
            if (job['kind'] != 'ingest_upload' or job['upload_session_id'] != binding['upload_id'] or not upload
                    or upload.tus_id != binding['tus_id'] or upload.declared_size != binding['byte_size']
                    or conn.execute(text('SELECT 1 FROM upload_transport_ops WHERE upload_id=:id AND state IN ' + ACTIVE_OPS),
                                    dict(id=binding['upload_id'])).first()):
                raise PublicationError('Stored upload identity differs')
        else:
            if self.sources is None or job['kind'] != 'ingest_source' or job['upload_session_id'] is not None:
                raise PublicationError('Explicit source worker required')
            item = self.sources._download(conn, lease)
            source = self.sources._source(conn, item['source_id'], binding['source_config_version'])
            if (item['id'] != binding['observation_id'] or source['event_epoch'] != binding['source_event_epoch']
                    or json.loads(item['metadata_json'])['byte_size'] != run['byte_size']):
                raise PublicationError('Source observation/configuration/epoch differs')

    def register_validated_source(self, lease, **plan):
        with self.engine.connect() as conn:
            if not conn.execute(text('SELECT 1 FROM ingestion_inputs WHERE job_id=:id AND observation_id IS NOT NULL'),
                                dict(id=lease.job_id)).first():
                raise PublicationError('Explicit source observation binding required')
        return self.register_validated_upload(lease, **plan)

    def _finish_ingestion_in(self, conn, lease, plan):
        current, _receipt = self._plan(conn, lease)
        active = conn.execute(text('SELECT id FROM edition_revisions WHERE edition_id=:id AND is_active=1'),
                              dict(id=plan['edition_id'])).scalar()
        if current != plan or active != plan['revision_id']:
            raise PublicationError('Newly sealed active revision differs before commit')
        job = conn.execute(text('SELECT * FROM jobs WHERE id=:id'), dict(id=lease.job_id)).mappings().one()
        binding = conn.execute(text('SELECT * FROM ingestion_inputs WHERE job_id=:id'), dict(id=lease.job_id)).mappings().first()
        if binding:
            self._ingestion_binding(conn, lease, plan, job, binding)
        self._asset_sources(conn, lease, plan)
        self._comparison_binding(conn, lease, plan)
        if job['upload_session_id'] is not None:
            self.permissions.finish_publication_in(conn, job['upload_session_id'])

    def _asset_sources(self, conn, lease, plan):
        """Inherited bytes keep their source approvals, never a new blanket grant."""
        sources = []
        for asset in plan['assets']:
            if asset['prepared_relpath'] is not None:
                sources.append([(lease.job_id,lease.generation)])
                continue
            inherited = conn.execute(text('''SELECT DISTINCT s.source_job_id,s.source_generation,p.rights_status
                FROM revision_assets a JOIN revision_asset_sources s
                    ON s.edition_revision_id=a.edition_revision_id AND s.asset_position=a.asset_position
                JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
                WHERE a.edition_revision_id=:revision AND a.object_sha=:sha
                    AND a.file_format=:format AND a.asset_role=:role'''),
                dict(revision=plan['expected_revision_id'],sha=asset['sha256'],format=asset['file_format'],role=asset['asset_role'])).all()
            if not inherited or any(row.rights_status != 'allowed' for row in inherited):
                raise PublicationError('Inherited asset source approval is absent')
            sources.append([(row.source_job_id,row.source_generation) for row in inherited])
        return sources

    def _preserve_options(self, conn, plan):
        previous = plan['expected_revision_id']
        if previous is None:
            return
        if not conn.execute(text('SELECT 1 FROM revision_seals WHERE revision_id=:id'),dict(id=previous)).first():
            raise PublicationError('Previous revision must be sealed')
        old_options = conn.execute(text('SELECT * FROM revision_options WHERE edition_revision_id=:id'),dict(id=previous)).mappings()
        for old in old_options:
            members = conn.execute(text('''SELECT a.object_sha,a.file_format,a.asset_role FROM option_assets o JOIN revision_assets a
                ON a.edition_revision_id=o.edition_revision_id AND a.asset_position=o.asset_position
                WHERE o.option_id=:id ORDER BY o.ordinal'''),dict(id=old['id'])).all()
            coverage = json.loads(old['coverage_json'])
            for new in plan['options']:
                if new['format'] != old['format']:
                    continue
                new_members = [(plan['assets'][p]['sha256'],plan['assets'][p]['file_format'],plan['assets'][p]['asset_role']) for p in new['positions']]
                if new_members == members and new['coverage'] == coverage and new['delivery_kind'] == old['delivery_kind']:
                    break
                replacements = new.get('replaces_option_ids', [])
                if (not isinstance(replacements, list) or len(replacements) > 64
                        or any(not isinstance(value, str) for value in replacements)):
                    raise PublicationError('Bounded replacement option identities required')
                if (new.get('replaces_option_id') == old['id'] or old['id'] in replacements) and new.get('replacement_receipt'):
                    identifier(new['replacement_receipt'],200)
                    current = new['coverage']
                    if (coverage['confidence'] == current['confidence'] == 'verified'
                            and coverage['unit'] == current['unit'] != 'unknown'
                            and coverage['intervals'] and current['intervals']
                            and all(any(c <= a <= b <= d for c,d in current['intervals']) for a,b in coverage['intervals'])):
                        break
            else:
                raise PublicationError('An old format/coverage option would be silently lost')

    def register_validated_upload(self, lease, *, edition_id, expected_revision_id, assets, options,
                                  validation_receipt, rights_status='review', reconciliation=None, comparison_assets=()):
        """Trusted ingestion validator only; real parser/review adapter belongs to G1."""
        identifier(validation_receipt,200)
        if rights_status not in {'allowed','review','blocked'}:
            raise PublicationError('Explicit rights decision required')
        plan = dict(edition_id=edition_id, expected_revision_id=expected_revision_id, assets=assets, options=options,
                    revision_id=str(uuid.uuid5(uuid.UUID(lease.job_id),f'revision:{lease.generation}')))
        if reconciliation is not None:
            plan.update(reconciliation=reconciliation, comparison_assets=list(comparison_assets))
            if self._duplicate(plan):
                plan['revision_id'] = expected_revision_id
        self._validate_plan(lease,plan)
        encoded = json_object(plan)
        with self.locks.mutation([a['sha256'] for a in assets]):
            with self.jobs.ownership(lease) as conn:
                self._binding(conn,lease,plan)
                if not self._duplicate(plan):
                    self._preserve_options(conn,plan)
                old = conn.execute(text('SELECT * FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).mappings().first()
                if old:
                    if old['payload_digest'] != digest(encoded) or old['validation_receipt'] != validation_receipt or old['rights_status'] != rights_status:
                        raise PublicationError('Publication plan conflict')
                    return plan['revision_id']
                conn.execute(text('INSERT INTO publication_plans VALUES(:id,:generation,:payload,:digest,:receipt,:rights,:now)'),
                             dict(id=lease.job_id,generation=lease.generation,payload=encoded,digest=digest(encoded),receipt=validation_receipt,rights=rights_status,now=self.jobs._sample()[1]))
        return plan['revision_id']

    def _plan(self, conn, lease):
        row = conn.execute(text('SELECT * FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).mappings().first()
        if not row or row['rights_status'] != 'allowed' or digest(row['payload_json']) != row['payload_digest']:
            raise PublicationError('Current source/rights approval is absent')
        plan = json.loads(row['payload_json'])
        self._validate_plan(lease,plan)
        return plan, row['validation_receipt']

    def _verify_file(self, path, asset):
        with self.root.file(path) as fd:
            return self._hash_fd(fd,asset)

    @staticmethod
    def _fingerprint(fd):
        info = os.fstat(fd)
        return info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns

    @staticmethod
    def _hash_fd(fd, asset):
        before = PublicationService._fingerprint(fd)
        hasher, total = hashlib.sha256(), 0
        while chunk := os.read(fd,1024*1024):
            total += len(chunk)
            if total > asset['byte_size']:
                raise PublicationError('Object size differs')
            hasher.update(chunk)
        if total != asset['byte_size'] or hasher.hexdigest() != asset['sha256']:
            raise PublicationError('Object hash/size differs')
        if PublicationService._fingerprint(fd) != before:
            raise PublicationError('File changed during validation')
        return before

    def _validated_files(self, assets):
        def open_files(stack):
            opened = []
            for asset in assets:
                path = object_path(asset['sha256'])
                try:
                    fd = stack.enter_context(self.root.file(path))
                except FileNotFoundError:
                    path = asset['prepared_relpath']
                    if path is None:
                        raise PublicationError('Referenced object is missing')
                    fd = stack.enter_context(self.root.file(path))
                opened.append((path,fd,asset))
            return opened
        # Only shared object locks remain while hashing; no global or DB writer.
        with self.locks.readers([a['sha256'] for a in assets],open_files) as opened:
            return {path:self._hash_fd(fd,asset) for path,fd,asset in opened}

    def _unchanged(self, path, verified):
        with self.root.file(path) as fd:
            if path not in verified or self._fingerprint(fd) != verified[path]:
                raise PublicationError('Validated file changed; a fresh validation is required')

    def _install(self, asset, verified):
        target = object_path(asset['sha256'])
        try:
            self._unchanged(target,verified)
            # A prior rename may have survived a failed directory flush. Reuse
            # must establish durability again before committing a revision.
            with self.root.file(target) as fd:
                sync_file(fd)
            with self.root.directory(str(PurePosixPath(target).parent)) as parent:
                os.fsync(parent)
            return
        except FileNotFoundError:
            if asset['prepared_relpath'] is None:
                raise PublicationError('Referenced object is missing')
        source = asset['prepared_relpath']
        self._unchanged(source,verified)
        self.root.move(source,target)

    def publish(self, lease):
        with self.engine.connect() as conn:
            plan, _receipt = self._plan(conn,lease)
            preview = conn.execute(text('SELECT preview_json FROM ingestion_runs WHERE job_id=:id AND generation=:generation'),
                                   dict(id=lease.job_id, generation=lease.generation)).scalar()
        result = {'revision_id': plan['revision_id']}
        if preview is not None:
            result['preview'] = json.loads(preview)
        checked_assets = plan['assets'] + plan.get('comparison_assets', [])
        shas = sorted({a['sha256'] for a in checked_assets})
        revision = plan['revision_id']
        verified = self._validated_files(checked_assets)
        with self.locks.mutation(shas):
            with self.jobs.ownership(lease) as conn:
                current, receipt = self._plan(conn,lease)
                if current != plan:
                    raise PublicationError('Plan changed before installation')
                job = self._binding(conn,lease,plan)
                if not self._duplicate(plan):
                    self._preserve_options(conn,plan)
                for asset in plan['assets']:
                    old = conn.execute(text('SELECT * FROM objects WHERE sha256=:sha'),dict(sha=asset['sha256'])).mappings().first()
                    if old and (old['state'] != 'LIVE' or old['byte_size'] != asset['byte_size'] or old['detected_mime'] != asset['detected_mime'] or old['local_relpath'] != object_path(asset['sha256'])):
                        raise PublicationError('Existing object is unavailable or metadata differs')
                    if not old and asset['prepared_relpath'] is None:
                        raise PublicationError('Cannot invent an existing object reference')
                    if asset['prepared_relpath'] is not None:
                        conn.execute(text('''INSERT INTO storage_intents VALUES(:id,:job,:generation,:sha,:size,:mime,:prepared,:target,'prepared',:now)
                            ON CONFLICT(job_id,generation,object_sha) DO NOTHING'''),dict(id=str(uuid.uuid5(uuid.UUID(lease.job_id),f'{lease.generation}:{asset["sha256"]}')),job=lease.job_id,generation=lease.generation,sha=asset['sha256'],size=asset['byte_size'],mime=asset['detected_mime'],prepared=asset['prepared_relpath'],target=object_path(asset['sha256']),now=self.jobs._sample()[1]))
            install_assets = {}
            for asset in plan['assets']:
                install_assets.setdefault(asset['sha256'],asset)
            for asset in install_assets.values():
                self._install(asset,verified)
            duplicate = self._duplicate(plan)
            if 'reconciliation' in plan:
                result['reconciliation'] = plan['reconciliation']
            if duplicate:
                followups = [dict(kind='backup_catalog', idempotency_key='backup-ingestion:' + lease.job_id,
                                  payload={'revision_id': revision, 'ingestion_job_id': lease.job_id}, input_revision_id=revision)]
                event = dict(kind='provenance_added', dedupe_key='provenance:' + lease.job_id, payload={'revision_id': revision})
            else:
                followups = [dict(kind='backup_revision',idempotency_key=f'backup:{revision}',payload={'revision_id':revision},input_revision_id=revision)]
                event = dict(kind='revision_published',dedupe_key=f'published:{revision}',payload={'revision_id':revision})
            with self.jobs.completion(lease,result=result,followups=followups,
                                     finalize=lambda conn:self._finish_ingestion_in(conn,lease,plan),
                                     outbox=[event]) as conn:
                current, receipt = self._plan(conn,lease)
                if current != plan:
                    raise PublicationError('Plan changed before commit')
                job = self._binding(conn,lease,plan)
                for asset in plan.get('comparison_assets', []):
                    self._unchanged(object_path(asset['sha256']), verified)
                if duplicate:
                    self._record_input_source(conn, lease, plan, receipt, job['upload_session_id'])
                    conn.execute(text("UPDATE storage_intents SET state='committed' WHERE job_id=:id AND generation=:generation"), dict(id=lease.job_id, generation=lease.generation))
                    conn.execute(text('UPDATE storage_sequence SET value=value+1 WHERE id=1'))
                else:
                    self._write_revision(conn,lease,plan,receipt,job['upload_session_id'])
        return revision

    def _record_input_source(self, conn, lease, plan, receipt, upload):
        fresh = [a for a in plan['assets'] if a['prepared_relpath'] is not None]
        source_version = None
        if upload is None:
            run = conn.execute(text('SELECT sha256,byte_size FROM ingestion_runs WHERE job_id=:id AND generation=:generation'),
                               dict(id=lease.job_id, generation=lease.generation)).mappings().one()
            source_version = self.sources.content_in(conn, lease, sha256=run['sha256'], byte_size=run['byte_size'])
        for asset in fresh:
            # Normal publication inserts objects before this method. A duplicate
            # references an existing LIVE object and never mutates its old seal.
            if upload is not None:
                conn.execute(text('INSERT INTO object_sources VALUES(:sha,:job,:upload,:receipt,:now) ON CONFLICT(object_sha,job_id) DO NOTHING'),
                             dict(sha=asset['sha256'],job=lease.job_id,upload=upload,receipt=receipt,now=self.jobs._sample()[1]))
            else:
                observation = conn.execute(text('SELECT observation_id FROM ingestion_inputs WHERE job_id=:id'), dict(id=lease.job_id)).scalar_one()
                conn.execute(text('INSERT INTO source_object_sources VALUES(:sha,:job,:observation,:version,:receipt)'),
                             dict(sha=asset['sha256'], job=lease.job_id, observation=observation, version=source_version, receipt=receipt))

    def _write_revision(self, conn, lease, plan, receipt, upload):
        now = self.jobs._sample()[1]
        revision = plan['revision_id']
        sources = self._asset_sources(conn,lease,plan)
        number = conn.execute(text('SELECT coalesce(max(revision_no),0)+1 FROM edition_revisions WHERE edition_id=:id'),dict(id=plan['edition_id'])).scalar_one()
        conn.execute(text('INSERT INTO edition_revisions VALUES(:id,:edition,:number,0,:stamp)'),dict(id=revision,edition=plan['edition_id'],number=number,stamp=stamp(now//1000)))
        for position, asset in enumerate(plan['assets']):
            conn.execute(text('''INSERT INTO objects(sha256,byte_size,detected_mime,local_relpath,created_at)
                VALUES(:sha,:size,:mime,:path,:stamp) ON CONFLICT(sha256) DO NOTHING'''),dict(sha=asset['sha256'],size=asset['byte_size'],mime=asset['detected_mime'],path=object_path(asset['sha256']),stamp=stamp(now//1000)))
            conn.execute(text('''INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format,asset_role)
                VALUES(:revision,:position,:sha,:format,:role)'''),dict(revision=revision,position=position,sha=asset['sha256'],format=asset['file_format'],role=asset['asset_role']))
            for source_job, generation in sources[position]:
                conn.execute(text('INSERT INTO revision_asset_sources VALUES(:revision,:position,:job,:generation)'),
                             dict(revision=revision,position=position,job=source_job,generation=generation))
        self._record_input_source(conn, lease, plan, receipt, upload)
        for index, option in enumerate(plan['options']):
            ident = str(uuid.uuid5(uuid.UUID(revision),f'option:{index}'))
            conn.execute(text('INSERT INTO revision_options VALUES(:id,:revision,:format,:delivery,:coverage,:confidence)'),dict(id=ident,revision=revision,format=option['format'],delivery=option['delivery_kind'],coverage=json_object(option['coverage']),confidence=option['coverage']['confidence']))
            for ordinal, position in enumerate(option['positions']):
                conn.execute(text('INSERT INTO option_assets VALUES(:id,:revision,:position,:ordinal)'),dict(id=ident,revision=revision,position=position,ordinal=ordinal))
        conn.execute(text('INSERT INTO revision_seals VALUES(:id,:job,:generation,:now,1)'),dict(id=revision,job=lease.job_id,generation=lease.generation,now=now))
        conn.execute(text('UPDATE edition_revisions SET is_active=0 WHERE edition_id=:id AND is_active=1'),dict(id=plan['edition_id']))
        conn.execute(text('UPDATE edition_revisions SET is_active=1 WHERE id=:id'),dict(id=revision))
        conn.execute(text("UPDATE storage_intents SET state='committed' WHERE job_id=:id AND generation=:generation"),dict(id=lease.job_id,generation=lease.generation))
        conn.execute(text('UPDATE storage_sequence SET value=value+1 WHERE id=1'))

    def inspect_intent(self, intent_id):
        """Reconcile evidence under the mutation lock; never delete an uncertain orphan."""
        with self.engine.connect() as conn:
            row = conn.execute(text('SELECT * FROM storage_intents WHERE id=:id'),dict(id=intent_id)).mappings().one()
        with self.locks.mutation([row['object_sha']]):
            with self.engine.connect() as conn:
                row = dict(conn.execute(text('SELECT * FROM storage_intents WHERE id=:id'),dict(id=intent_id)).mappings().one())
            asset = dict(sha256=row['object_sha'],byte_size=row['byte_size'])
            for path, status in [(row['target_relpath'],'installed_uncommitted'),(row['prepared_relpath'],'prepared')]:
                try:
                    self._verify_file(path,asset)
                except FileNotFoundError:
                    continue
                if row['state'] == 'committed' and status == 'installed_uncommitted':
                    status = 'committed'
                return dict(id=intent_id, state=status, durable_state=row['state'])
            return dict(id=intent_id,state='missing',durable_state=row['state'])

    def publish_cover(self, lease, *, metadata):
        """One typed metadata collaborator; reuse the existing physical install path."""
        from .metadata_service import MetadataService
        if type(metadata) is not MetadataService or metadata.publisher is not self or metadata.engine is not self.engine:
            raise PublicationError('This publisher metadata service is required')
        with self.engine.connect() as conn:
            plan=metadata._cover_plan_in(conn,lease)
        asset=plan['asset'];sha=asset['sha256'];verified=self._validated_files([asset])
        with self.locks.mutation([sha]):
            with self.jobs.ownership(lease) as conn:
                if metadata._cover_plan_in(conn,lease)!=plan:
                    raise PublicationError('Cover plan changed before installation')
                old=conn.execute(text('SELECT * FROM objects WHERE sha256=:sha'),dict(sha=sha)).mappings().first()
                if old and (old['state']!='LIVE' or old['byte_size']!=asset['byte_size'] or old['detected_mime']!=asset['detected_mime'] or old['local_relpath']!=object_path(sha)):
                    raise PublicationError('Existing cover object identity differs')
                conn.execute(text("""INSERT INTO storage_intents VALUES(:id,:job,:generation,:sha,:size,:mime,:prepared,:target,'prepared',:now)
                    ON CONFLICT(job_id,generation,object_sha) DO NOTHING"""),dict(id=str(uuid.uuid5(uuid.UUID(lease.job_id),f'{lease.generation}:{sha}')),job=lease.job_id,generation=lease.generation,sha=sha,size=asset['byte_size'],mime=asset['detected_mime'],prepared=asset['prepared_relpath'],target=object_path(sha),now=self.jobs._sample()[1]))
            self._install(asset,verified)
            result={'candidate_id':plan['candidate_id']}
            with self.jobs.completion(lease,result=result,
                    followups=[dict(kind='backup_catalog',idempotency_key='metadata-cover:'+plan['candidate_id'],payload=result)],
                    outbox=[dict(kind='metadata_published',dedupe_key='metadata:'+plan['candidate_id'],payload=result)],
                    finalize=lambda conn:metadata._finalize_cover_in(conn,lease,plan,receipt)) as conn:
                if metadata._cover_plan_in(conn,lease)!=plan:
                    raise PublicationError('Cover plan changed before commit')
                # _validated_files and _install already verified the owned immutable bytes.
                conn.execute(text('''INSERT INTO objects(sha256,byte_size,detected_mime,local_relpath,created_at)
                    VALUES(:sha,:size,:mime,:path,:stamp) ON CONFLICT(sha256) DO NOTHING'''),dict(sha=sha,size=asset['byte_size'],mime=asset['detected_mime'],path=object_path(sha),stamp=stamp(self.jobs._sample()[1]//1000)))
                old=conn.execute(text('SELECT * FROM objects WHERE sha256=:sha'),dict(sha=sha)).mappings().one()
                if old['state']!='LIVE' or old['byte_size']!=asset['byte_size'] or old['detected_mime']!=asset['detected_mime'] or old['local_relpath']!=object_path(sha):
                    raise PublicationError('Installed cover object identity differs')
                receipt=metadata._commit_cover_in(conn,lease,plan)
                conn.execute(text("UPDATE storage_intents SET state='committed' WHERE job_id=:id AND generation=:generation"),dict(id=lease.job_id,generation=lease.generation))
        return receipt

    def reference_sources(self, conn, sha):
        """Truth query for future GC; historical asset rows alone are not pins."""
        from .metadata_contracts import COVER_REFERENCES_V1
        object_path(sha)
        return conn.execute(text('''SELECT 'revision' AS kind,r.id AS reference_id FROM revision_assets a
            JOIN edition_revisions r ON r.id=a.edition_revision_id LEFT JOIN revision_seals s ON s.revision_id=r.id
            WHERE a.object_sha=:sha AND (r.is_active=1 OR s.retained=1)
            UNION SELECT 'job',j.id FROM jobs j WHERE j.state IN ('queued','running','retry_wait')
                AND (j.object_sha=:sha OR EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=j.input_revision_id AND a.object_sha=:sha))
            UNION SELECT 'resource',s.name FROM resource_slots s JOIN jobs j ON j.id=s.job_id
                WHERE j.object_sha=:sha OR EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=j.input_revision_id AND a.object_sha=:sha)
            UNION SELECT kind,reference_id FROM object_pins WHERE object_sha=:sha
            UNION SELECT 'intent',id FROM storage_intents WHERE object_sha=:sha AND state='prepared'
            UNION SELECT 'metadata_cover',candidate_id FROM ('''+COVER_REFERENCES_V1+''') WHERE object_sha=:sha
            ORDER BY kind,reference_id'''),dict(sha=sha)).all()

    def pin(self, kind, reference_id, shas):
        if kind not in {'ticket','backup','restore'} or not shas:
            raise PublicationError('Explicit supported retention reference required')
        identifier(reference_id,200)
        assets = []
        with self.engine.connect() as conn:
            for sha in sorted(set(shas)):
                row = conn.execute(text("SELECT byte_size FROM objects WHERE sha256=:sha AND state='LIVE'"),dict(sha=sha)).first()
                if not row:
                    raise PublicationError('New pins require LIVE objects')
                assets.append(dict(sha256=sha,byte_size=row.byte_size,prepared_relpath=None))
        verified = self._validated_files(assets)
        with self.locks.mutation(shas):
            with write_transaction(self.engine) as conn:
                for sha in sorted(set(shas)):
                    row = conn.execute(text("SELECT * FROM objects WHERE sha256=:sha AND state='LIVE'"),dict(sha=sha)).mappings().first()
                    if not row or row['local_relpath'] != object_path(sha):
                        raise PublicationError('New pins require LIVE objects')
                    self._unchanged(object_path(sha),verified)
                    conn.execute(text('INSERT INTO object_pins VALUES(:kind,:id,:sha,:now) ON CONFLICT DO NOTHING'),dict(kind=kind,id=reference_id,sha=sha,now=self.jobs._sample()[1]))

    @contextmanager
    def read_selected(self, resolve_fresh, *, expected=None, blocking=True):
        """Trusted DB resolver + one sorted reader scope, entirely on this thread.

        Resolver callbacks must finish a new short transaction on each call.
        All originals hash before yield; revalidate again after PROCEED before
        success headers. This does not replace read_revision's legacy contract.
        """
        initial = resolve_fresh()
        if (not 1 <= len(initial.assets) <= 200 or len(initial.options) > 64
                or (expected is not None and initial != expected)):
            raise PublicationError('Explicit bounded fixed original selection required')
        shas = [asset.object_sha for asset in initial.assets]
        def opener(stack):
            current = resolve_fresh()
            if current != initial:
                raise PublicationError('Selection changed before original opener')
            descriptors = {}
            for asset in current.assets:
                path = object_path(asset.object_sha)
                if asset.object_sha in descriptors:
                    if os.fstat(descriptors[asset.object_sha]).st_size != asset.byte_size:
                        raise PublicationError('Aliased original size differs')
                    continue
                fd = stack.enter_context(self.root.file(path))
                if os.fstat(fd).st_size != asset.byte_size:
                    raise PublicationError('Selected original size changed')
                descriptors[asset.object_sha] = fd
            return OpenedSelection(current, descriptors, stack, resolve_fresh)
        with self.locks.readers(shas, opener, blocking=blocking) as opened:
            try:
                for asset in opened.snapshot.assets:
                    if asset.object_sha in opened._fingerprints:
                        continue
                    fd = opened.fd_for(asset)
                    os.lseek(fd, 0, os.SEEK_SET)
                    opened._fingerprints[asset.object_sha] = self._hash_fd(fd,
                        dict(sha256=asset.object_sha, byte_size=asset.byte_size))
                    os.lseek(fd, 0, os.SEEK_SET)
                opened.revalidate()
                yield opened
            finally:
                opened._closed = True

    @contextmanager
    def read_revision(self, token, revision_id, positions):
        if not isinstance(positions,list) or not 1 <= len(positions) <= 200 or any(type(p) is not int or p < 0 for p in positions) or len(set(positions)) != len(positions):
            raise PublicationError('Explicit bounded asset positions required')
        def rows(conn):
            return conn.execute(text('''SELECT a.asset_position,a.object_sha,o.byte_size,o.local_relpath,o.state
                FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
                WHERE a.edition_revision_id=:id AND a.asset_position IN (SELECT value FROM json_each(:positions))
                ORDER BY a.asset_position'''),dict(id=revision_id,positions=json.dumps(positions))).mappings().all()
        with self.engine.connect() as conn:
            initial = rows(conn)
        def open_files(stack):
            with self.engine.connect() as conn:
                self.permissions.auth.current(conn,token)
                allowed = conn.execute(text('''SELECT 1 FROM edition_revisions r JOIN revision_seals s ON s.revision_id=r.id
                    JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
                    WHERE r.id=:id AND (r.is_active=1 OR s.retained=1) AND p.rights_status='allowed' ''') ,dict(id=revision_id)).first()
                current = rows(conn)
                if not allowed or len(current) != len(positions) or current != initial or any(r['state'] != 'LIVE' for r in current):
                    raise PublicationError('Revision assets are no longer available')
                for position in positions:
                    approvals = conn.execute(text('''SELECT p.rights_status FROM revision_asset_sources s
                        JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
                        WHERE s.edition_revision_id=:id AND s.asset_position=:position'''),dict(id=revision_id,position=position)).scalars().all()
                    if not approvals or any(status != 'allowed' for status in approvals):
                        raise PublicationError('Current asset source approval is absent')
                result = {}
                for row in current:
                    if row['local_relpath'] != object_path(row['object_sha']):
                        raise PublicationError('Unexpected object location')
                    fd = stack.enter_context(self.root.file(row['local_relpath']))
                    if os.fstat(fd).st_size != row['byte_size']:
                        raise PublicationError('Object size changed')
                    result[row['asset_position']] = fd
                return result
        with self.locks.readers([r['object_sha'] for r in initial],open_files) as opened:
            yield opened
