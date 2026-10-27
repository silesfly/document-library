"""Explicit administrator metadata admission and trusted worker publication.

No provider, model, HTTP adapter or implicit ingestion authority lives here.
"""
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import os
import re
import threading
import time
import uuid

from sqlalchemy import text as sql

from .auth import AuthError, digest
from .database import write_transaction
from .ingest_formats import inspect_fd, Limits, IMAGES, HEADING, FormatError, parse_metadata
from .ingestion import IngestionService
from .jobs import identifier
from .library_contracts import canonical_json
from .metadata_contracts import COVER_SCHEMA, LOCAL_ORIGINS, ORIGINS, PRIORITY
from .metadata_summary import (ChapterSpan, MAX_SOURCE_CHARS, SummaryOutput,
    assess_summary_rules, parse_summary_output, select_summary_input)
from .metadata_generated_contracts import (AUXILIARY_SCHEMA, SCHEMA_VERSION as GENERATED_SCHEMA,
    GeneratedContractError, MAX_REVIEW_BYTES, canonical_binding,
    generated_review_unit_id, generated_review_units,
    review_context_digest as generated_review_context_digest, sha256_text,
    validate_auxiliary, validate_ranges, valid_sha)
from .publication import PublicationError, PublicationService, object_path, _coverage


class MetadataError(PublicationError):
    pass


def _json(value, maximum):
    if not isinstance(value,dict):
        raise MetadataError('Explicit metadata object required')
    try:
        result=canonical_json(value)
    except (ValueError,TypeError):
        raise MetadataError('Finite metadata JSON required') from None
    if len(result.encode())>maximum:
        raise MetadataError('Metadata object exceeds limit')
    return result


def _positive(value, *, zero=False):
    if type(value) is not int or not (0 if zero else 1)<=value<=9223372036854775806:
        raise MetadataError('Explicit metadata version required')
    return value


def _provenance(value):
    if not isinstance(value,dict) or set(value)-{'source_url','model_id','model_sha256','prompt_sha256','tokenizer_id','input_ranges','quality','source_facts'}:
        raise MetadataError('Only bounded source facts and model hashes are admitted')
    if any(isinstance(v,str) and len(v)>2048 for v in value.values()):
        raise MetadataError('Provenance text exceeds limit')
    for key in ('model_sha256','prompt_sha256'):
        if key in value:object_path(value[key])
    if 'source_url' in value and (not isinstance(value['source_url'],str) or not value['source_url'].startswith(('https://','http://'))):
        raise MetadataError('Explicit source URL required')
    for key in ('source_facts','quality'):
        if key in value and (not isinstance(value[key],dict) or set(value[key])-{'provider','external_id','title','author','publisher','language','passed','reason_code','score'}):
            raise MetadataError('Provenance facts cannot contain body or raw request fields')
    return _json(value,65536)


def _cover_busy_predicate(job, generation):
    # Call sites supply fixed SQL expressions, never values from a request.
    return f"EXISTS(SELECT 1 FROM storage_intents WHERE job_id={job} AND generation={generation} AND state='prepared' UNION ALL SELECT 1 FROM resource_slots WHERE job_id={job} AND generation={generation} LIMIT 1)"


@dataclass(frozen=True)
class SourceTextRead:
    text: str = field(repr=False)
    job_id: str
    generation: int
    work_id: str
    source_revision_id: str
    asset_position: int
    original_sha256: str
    original_byte_size: int
    file_format: str
    text_sha256: str
    text_codepoints: int
    expected_head_version: int
    rights_id: str
    rights_version: int
    coverage: str = field(default='asset_only', init=False)


def _summary_chapters(parts, offsets, *, document_kind, coverage):
    """Build gapless original-codepoint chapters for an ordered option."""
    review = False
    try:
        for part in parts:
            metadata = parse_metadata("source.txt", part, max_entries=5000)
            if metadata['review_reason'] is not None:
                review = True
    except (MetadataError, FormatError, ValueError, TypeError, RecursionError):
        review = True
    if review:
        raise MetadataError("Summary chapters require review")

    heading_records = []
    heading_count = 0
    all_have_headings = True
    any_have_headings = False

    for part_index, part in enumerate(parts):
        part_records = []
        line_start = 0
        length = len(part)
        i = 0
        while i <= length:
            if i == length:
                line_end = i
            elif part[i] == "\r":
                if i + 1 < length and part[i + 1] == "\n":
                    line_end = i
                    i += 1
                else:
                    line_end = i
            elif part[i] == "\n":
                line_end = i
            else:
                i += 1
                continue

            line = part[line_start:line_end]
            match = HEADING.fullmatch(line)
            if match is not None:
                heading_count += 1
                if heading_count > 5000:
                    raise MetadataError("Summary chapters require review")

                number = int(match.group(1))
                unit_mark = match.group(2)
                if number < 1 or number > 10000000:
                    raise MetadataError("Summary chapters require review")

                semantic_unit = "volume" if unit_mark == "권" else "episode"
                part_records.append((number, line_start, semantic_unit))

            line_start = i + 1
            i += 1

        if part_records:
            any_have_headings = True
        else:
            all_have_headings = False
        heading_records.extend((part_index, record) for record in part_records)

    if not any_have_headings:
        if coverage['confidence'] == 'verified':
            raise MetadataError('VERIFIED_COVERAGE_UNPROVEN: summary input requires review')
        return None

    previous_number = None
    actual_unit = None
    for _part_index, (number, _line_start, semantic_unit) in heading_records:
        if actual_unit is None:
            actual_unit = semantic_unit
        elif semantic_unit != actual_unit:
            raise MetadataError("Summary chapters require review")

        if previous_number is not None and number != previous_number + 1:
            raise MetadataError("Summary chapters require review")
        previous_number = number

    if (coverage.get("confidence") == "verified"
            and coverage.get("unit") in ("episode", "volume")):
        coverage_unit = coverage.get("unit")
        if coverage_unit != actual_unit:
            raise MetadataError("Summary chapters require review")

        intervals = coverage.get("intervals")
        gaps = coverage.get("gaps")
        first_number = heading_records[0][1][0]
        last_number = heading_records[-1][1][0]
        if gaps != [] or intervals != [[first_number, last_number]]:
            raise MetadataError("Summary chapters require review")

    if not all_have_headings:
        if document_kind == "novel":
            raise MetadataError("Summary chapters require review")
        return None

    spans = []
    for index, (part_index, (number, line_start, _semantic_unit)) in enumerate(heading_records):
        if index == 0:
            start = 0
        else:
            previous_part_index, (previous_number, _previous_line_start, _previous_unit) = heading_records[index - 1]
            if part_index == previous_part_index:
                start = offsets[part_index] + line_start
            else:
                start = offsets[part_index] - 1

        if index + 1 < len(heading_records):
            next_part_index, (next_number, next_line_start, next_unit) = heading_records[index + 1]
            if next_part_index == part_index:
                end = offsets[next_part_index] + next_line_start
            else:
                end = offsets[next_part_index] - 1
        else:
            final_part_index = len(parts) - 1
            end = offsets[final_part_index] + len(parts[final_part_index])

        spans.append(ChapterSpan(ordinal=number, start=start, end=end))

    return tuple(spans)


def _summary_text_digest(body):
    failed = False
    result = None
    try:
        result = hashlib.sha256(body.encode('utf-8')).hexdigest()
    except UnicodeError:
        failed = True
    if failed:
        raise MetadataError('Summary source Unicode requires review')
    return result


class _PreparedSummaryInput:
    """Transient, thread-bound input. Its hashes are not durable authority."""

    def __init__(self, service, lease, option_id, snapshot, body, chapters, receipt,
                 max_source_chars):
        self._service, self._lease, self._option_id = service, lease, option_id
        self._snapshot, self._body, self._chapters = snapshot, body, chapters
        self._receipt = canonical_json(receipt)
        self._max_source_chars = max_source_chars
        self._thread_id = threading.get_ident()
        self._closed = False

    @property
    def receipt(self):
        return json.loads(self._receipt)

    @property
    def context_digest(self):
        return digest(self._snapshot)

    @property
    def input_digest(self):
        return digest(self._receipt)

    def _revalidate(self):
        if self._closed or threading.get_ident() != self._thread_id:
            raise MetadataError('Summary input context is closed or thread changed')
        if self._service._summary_snapshot(self._lease, self._option_id) != self._snapshot:
            raise MetadataError('Fixed summary option input changed')

    def select(self):
        """Fence every use; the returned Python string remains transient."""
        self._revalidate()
        snapshot = json.loads(self._snapshot)
        receipt = json.loads(self._receipt)
        return select_summary_input(
            self._body, source_revision_id=snapshot['source_revision_id'],
            document_kind=snapshot['item_type'], chapters=self._chapters,
            max_source_chars=self._max_source_chars, source_complete=True,
            expected_source_chars=receipt['text_codepoints'],
            expected_source_sha256=receipt['text_sha256'])

    def _close(self):
        self._body = None
        self._closed = True


class MetadataService:
    COVER_LIMITS=Limits(file_bytes=8*1024**2,parse_bytes=8*1024**2,expanded_bytes=8*1024**2,
                        member_bytes=8*1024**2,pixels=16_000_000,frames=1)

    def __init__(self,publisher):
        if not isinstance(publisher,PublicationService) or publisher.jobs.engine is not publisher.permissions.auth.engine:
            raise ValueError('One publisher and authority engine required')
        self.publisher,self.engine,self.jobs=publisher,publisher.engine,publisher.jobs
        self.auth=publisher.permissions.auth

    def _now(self):
        return self.jobs._sample()[1]

    @staticmethod
    def _head(conn,work,expected=None):
        row=conn.execute(sql('SELECT * FROM metadata_heads WHERE work_id=:id'),dict(id=work)).mappings().first()
        if row is None or (expected is not None and row['version']!=_positive(expected,zero=True)):
            raise MetadataError('Metadata head conflict')
        return dict(row)

    @staticmethod
    def _receipt(conn,work):
        return dict(conn.execute(sql('SELECT work_id,description_id,cover_id,version FROM metadata_heads WHERE work_id=:work'),dict(work=work)).mappings().one())

    def _backup(self,conn,work):
        sequence=conn.execute(sql('SELECT value FROM storage_sequence WHERE id=1')).scalar_one()
        self.jobs.enqueue_in(conn,kind='backup_catalog',idempotency_key=f'metadata:{work}:{sequence}',payload={'work_id':work,'storage_sequence':sequence},work_id=work)

    @staticmethod
    def _decision(value):
        keys={'state','officiality','match_state','display_allowed','cache_allowed','derivative_allowed','cache_until_ms','retention_policy','evidence'}
        if not isinstance(value,dict) or set(value)!=keys or any(type(value[k]) is not str for k in ('state','officiality','match_state')) or value['state'] not in {'allowed','review','blocked'} or value['officiality'] not in {'verified','unknown','not_applicable'} or value['match_state'] not in {'exact','review','not_applicable'}:
            raise MetadataError('Exact explicit rights decision required')
        if any(type(value[k]) is not bool for k in ('display_allowed','cache_allowed','derivative_allowed')):
            raise MetadataError('Rights flags must be booleans')
        if value['cache_until_ms'] is not None:_positive(value['cache_until_ms'],zero=True)
        policy=value['retention_policy']
        if (not isinstance(policy,dict) or set(policy)-{'backup_allowed','all_copies_deletion_required','policy_id'}
                or type(policy.get('backup_allowed')) is not bool or type(policy.get('all_copies_deletion_required')) is not bool):
            raise MetadataError('Explicit supported retention obligations required')
        if value['cache_allowed'] and (not policy['backup_allowed'] or policy['all_copies_deletion_required']):
            raise MetadataError('All-copy deletion route is not implemented; caching blocked')
        result={k:value[k] for k in ('state','officiality','match_state','cache_until_ms')}
        result.update({k:int(value[k]) for k in ('display_allowed','cache_allowed','derivative_allowed')})
        result.update(evidence_json=_json(value['evidence'],12000),retention_policy_json=_json(policy,4096))
        _json(result,16384)
        return result

    def review_rights(self,token,csrf,*,rights_id,work_id,kind,expected_version,request_id,decision):
        for value in (rights_id,work_id,request_id):identifier(value,200)
        if kind not in {'description','cover'}:raise MetadataError('Invalid metadata kind')
        expected_version=_positive(expected_version,zero=True);values=self._decision(decision)
        with write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True)
            replay=conn.execute(sql('SELECT * FROM metadata_rights_history WHERE request_id=:id'),dict(id=request_id)).mappings().first()
            if replay:
                identity=conn.execute(sql('SELECT work_id,kind FROM metadata_rights WHERE id=:id'),dict(id=rights_id)).first()
                if not identity or identity.work_id!=work_id or identity.kind!=kind or replay['rights_id']!=rights_id or replay['version']!=expected_version+1 or json.loads(replay['decision_json'])!=values:
                    raise MetadataError('Rights request replay conflicts')
                return dict(rights_id=rights_id,version=replay['version'])
            self._head(conn,work_id)
            row=conn.execute(sql('SELECT * FROM metadata_rights WHERE id=:id'),dict(id=rights_id)).mappings().first()
            params=dict(values,id=rights_id,work=work_id,kind=kind,version=expected_version+1,actor=actor['id'],request=request_id,now=self._now(),old=expected_version)
            if row is None:
                if expected_version:raise MetadataError('Rights version conflict')
                conn.execute(sql('''INSERT INTO metadata_rights VALUES(:id,:work,:kind,:version,:state,:match_state,:officiality,
                  :display_allowed,:cache_allowed,:derivative_allowed,:evidence_json,:retention_policy_json,:cache_until_ms,:actor,:request,:now)'''),params)
            else:
                if row['work_id']!=work_id or row['kind']!=kind or row['version']!=expected_version:raise MetadataError('Rights identity/version conflict')
                count=conn.execute(sql('''UPDATE metadata_rights SET version=:version,state=:state,officiality=:officiality,
                  match_state=:match_state,display_allowed=:display_allowed,cache_allowed=:cache_allowed,derivative_allowed=:derivative_allowed,
                  evidence_json=:evidence_json,retention_policy_json=:retention_policy_json,cache_until_ms=:cache_until_ms,
                  decision_actor=:actor,request_id=:request,updated_at_ms=:now WHERE id=:id AND version=:old'''),params).rowcount
                if count!=1:raise MetadataError('Rights version conflict')
            self.auth.audit(conn,actor['id'],'metadata_rights',details={'rights_id':rights_id,'version':expected_version+1})
            self._backup(conn,work_id)
        return dict(rights_id=rights_id,version=expected_version+1)

    def _rights(self,conn,rights_id,work,kind,version,origin):
        row=conn.execute(sql('SELECT * FROM metadata_rights WHERE id=:id'),dict(id=rights_id)).mappings().first()
        if not row or row['work_id']!=work or row['kind']!=kind or row['version']!=version or row['state']!='allowed' or not row['cache_allowed'] or not row['display_allowed']:
            raise MetadataError('Current explicit metadata rights are absent')
        if origin=='PUBLISHER_OFFICIAL' and (row['officiality']!='verified' or row['match_state']!='exact'):
            raise MetadataError('Verified publisher identity and exact match required')
        if origin in LOCAL_ORIGINS and not row['derivative_allowed']:
            raise MetadataError('Derivative permission is absent')
        if row['cache_until_ms'] is not None and row['cache_until_ms']<=self._now():
            raise MetadataError('Metadata cache permission expired')
        policy=json.loads(row['retention_policy_json'])
        if not policy.get('backup_allowed') or policy.get('all_copies_deletion_required'):
            raise MetadataError('Retention obligations are unsupported')
        return dict(row)

    @staticmethod
    def _source(conn,work,revision,*,current=True):
        row=conn.execute(sql('''SELECT r.is_active,e.work_id FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id WHERE r.id=:id'''),dict(id=revision)).first()
        if not row or row.work_id!=work or (current and not row.is_active):raise MetadataError('Current same-work metadata input required')
        assets=[dict(r) for r in conn.execute(sql('''SELECT a.asset_position,a.object_sha,a.file_format,o.byte_size,o.detected_mime,o.local_relpath,o.state
          FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha WHERE a.edition_revision_id=:id ORDER BY a.asset_position'''),dict(id=revision)).mappings()]
        if not assets or len(assets)>200 or any(a['state']!='LIVE' or a['local_relpath']!=object_path(a['object_sha']) for a in assets):
            raise MetadataError('Bounded LIVE original metadata input required')
        proofs=[dict(r) for r in conn.execute(sql('SELECT * FROM metadata_actual_sources_v1 WHERE revision_id=:id ORDER BY proof_role,asset_position,source_job_id,source_generation'),dict(id=revision)).mappings()]
        if len(proofs)>2000 or sum(p['proof_role']=='root' for p in proofs)!=1 or any(not any(p['proof_role']=='asset' and p['asset_position']==a['asset_position'] for p in proofs) for a in assets):
            raise MetadataError('Complete root and asset source proof required')
        for proof in proofs:
            plan=conn.execute(sql('''SELECT p.*,i.rights_status AS input_rights FROM publication_plans p LEFT JOIN ingestion_inputs i ON i.job_id=p.job_id
              WHERE p.job_id=:job AND p.generation=:generation'''),dict(job=proof['source_job_id'],generation=proof['source_generation'])).mappings().one()
            if plan['rights_status']!='allowed' or (plan['input_rights'] is not None and plan['input_rights']!='allowed') or digest(plan['payload_json'])!=proof['payload_digest'] or json.loads(plan['payload_json']).get('schema')==COVER_SCHEMA:
                raise MetadataError('Original input source permission changed')
        return dict(revision_id=revision,assets=assets,proofs=proofs)

    def enqueue(self,token,csrf,*,work_id,origin,rights_id,expected_rights_version,expected_head_version,source_revision_id,request_id):
        if origin not in ORIGINS:raise MetadataError('Explicit metadata origin required')
        for value in (work_id,rights_id,request_id):identifier(value,200)
        _positive(expected_rights_version);_positive(expected_head_version,zero=True)
        with write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True)
            rights=conn.execute(sql('SELECT kind FROM metadata_rights WHERE id=:id'),dict(id=rights_id)).scalar()
            if rights is None:raise MetadataError('Metadata rights are absent')
            payload=dict(work_id=work_id,origin=origin,rights_id=rights_id,rights_version=expected_rights_version,head_version=expected_head_version,source_revision_id=source_revision_id)
            encoded=_json(payload,65536);request_digest=digest(encoded)
            replay=conn.execute(sql('''SELECT i.* FROM metadata_job_inputs i JOIN jobs j ON j.id=i.job_id WHERE j.idempotency_key=:key'''),dict(key='metadata:'+request_id)).mappings().first()
            if replay:
                if replay['request_digest']!=request_digest:raise MetadataError('Metadata enqueue replay conflicts')
                return replay['job_id']
            self._head(conn,work_id,expected_head_version);self._rights(conn,rights_id,work_id,rights,expected_rights_version,origin)
            if origin in LOCAL_ORIGINS:
                binding=self._source(conn,work_id,source_revision_id)
            else:
                if source_revision_id is not None:raise MetadataError('Independent metadata cannot borrow an original source')
                binding={}
            ident=self.jobs.enqueue_in(conn,kind='metadata.'+rights+'.publish',idempotency_key='metadata:'+request_id,
                payload=payload,work_id=work_id,input_revision_id=source_revision_id)
            conn.execute(sql('INSERT INTO metadata_job_inputs VALUES(:job,:work,:rights,:rv,:hv,:origin,:revision,:source,:digest,:actor,:av,:now)'),
              dict(job=ident,work=work_id,rights=rights_id,rv=expected_rights_version,hv=expected_head_version,origin=origin,revision=source_revision_id,
                   source=_json(binding,65536),digest=request_digest,actor=actor['id'],av=actor['authz_version'],now=self._now()))
            self.auth.audit(conn,actor['id'],'metadata_enqueued',details={'job_id':ident,'work_id':work_id})
            return ident

    def _input(self,conn,lease,kind,*,head_version=None):
        row=conn.execute(sql('''SELECT i.*,j.kind AS job_kind,j.work_id AS job_work,j.input_revision_id AS job_revision FROM metadata_job_inputs i JOIN jobs j ON j.id=i.job_id WHERE i.job_id=:id'''),dict(id=lease.job_id)).mappings().first()
        if not row or row['job_kind']!='metadata.'+kind+'.publish' or row['job_work']!=row['work_id'] or row['job_revision']!=row['source_revision_id']:
            raise MetadataError('Explicit typed metadata job input required')
        actor=conn.execute(sql("SELECT 1 FROM users WHERE id=:id AND role='admin' AND status='active' AND authz_version=:version"),dict(id=row['actor_id'],version=row['actor_authz_version'])).first()
        if not actor:raise MetadataError('Metadata request actor authority changed')
        self._head(conn,row['work_id'],row['expected_head_version'] if head_version is None else head_version)
        self._rights(conn,row['rights_id'],row['work_id'],kind,row['expected_rights_version'],row['origin'])
        if row['origin'] in LOCAL_ORIGINS and json.loads(row['source_binding_json'])!=self._source(conn,row['work_id'],row['source_revision_id']):
            raise MetadataError('Fixed metadata source input changed')
        return dict(row)

    def read_source_text(self, lease, *, asset_position):
        """Read transient parser text for one currently authorized original asset.

        Asset-only text does not attest whole-source completeness or grant
        durable permission; later use and publication must fence again.
        """
        if type(asset_position) is not int or asset_position < 0:
            raise MetadataError('Unknown source asset')

        with self.jobs.ownership(lease) as conn:
            initial_input = self._input(conn, lease, 'description')
            if (initial_input['origin'] != 'QWEN_TEXT'
                    or not initial_input['source_revision_id']):
                raise MetadataError('Local text description input required')
            binding = json.loads(initial_input['source_binding_json'])
            matches = [asset for asset in binding['assets']
                       if asset['asset_position'] == asset_position]
            if len(matches) != 1:
                raise MetadataError('Unknown source asset')
            asset = matches[0]

        def revalidate():
            with self.jobs.ownership(lease) as conn:
                if self._input(conn, lease, 'description') != initial_input:
                    raise MetadataError('Fixed metadata source input changed')

        text, text_sha256, text_codepoints = self._extract_source_text(asset, revalidate)
        return SourceTextRead(
            text=text,
            job_id=lease.job_id,
            generation=lease.generation,
            work_id=initial_input['work_id'],
            source_revision_id=initial_input['source_revision_id'],
            asset_position=asset_position,
            original_sha256=asset['object_sha'],
            original_byte_size=asset['byte_size'],
            file_format=asset['file_format'],
            text_sha256=text_sha256,
            text_codepoints=text_codepoints,
            expected_head_version=initial_input['expected_head_version'],
            rights_id=initial_input['rights_id'],
            rights_version=initial_input['expected_rights_version'],
        )

    def _extract_source_text(self, asset, revalidate):
        """Shared locked extraction primitive for a lease or current admin authority.

        The callback is run while opening the reader and again after parsing.
        No path, body or callback exception detail is retained in errors.
        """
        thread_id = threading.get_ident()

        def open_bound_asset(stack):
            if threading.get_ident() != thread_id:
                raise MetadataError('Source reader thread changed')
            revalidate()
            return stack.enter_context(
                self.publisher.root.file(object_path(asset['object_sha'])))

        duplicated_fd = None
        try:
            with self.publisher.locks.readers([asset['object_sha']], open_bound_asset) as fd:
                duplicated_fd = os.dup(fd)
            inspection_failed = False
            text_value = None
            text_sha256 = None
            text_codepoints = None
            try:
                report = inspect_fd(duplicated_fd, 'original.' + asset['file_format'],
                                    Limits(), full_text=True)
                if (type(report) is not dict
                        or report.get('status') != 'validated'
                        or report.get('sha256') != asset['object_sha']
                        or type(report.get('byte_size')) is not int
                        or report['byte_size'] != asset['byte_size']
                        or report.get('format') != asset['file_format']
                        or type(report.get('full_text')) is not str
                        or not report['full_text']
                        or report.get('review_reason') is not None
                        or type(report.get('metadata')) is not dict
                        or report['metadata'].get('review_reason') is not None):
                    inspection_failed = True
                else:
                    text_value = report['full_text']
                    text_sha256 = hashlib.sha256(text_value.encode('utf-8')).hexdigest()
                    text_codepoints = len(text_value)
            except Exception:
                inspection_failed = True
            if inspection_failed:
                raise MetadataError('Source text inspection failed')
            revalidate()
            return text_value, text_sha256, text_codepoints
        finally:
            if duplicated_fd is not None:
                os.close(duplicated_fd)

    def _summary_snapshot_in(self, conn, lease, option_id):
        initial = self._input(conn, lease, 'description')
        revision = initial['source_revision_id']
        if initial['origin'] != 'QWEN_TEXT' or not revision:
            raise MetadataError('Local text description input required')
        option = conn.execute(sql('SELECT * FROM revision_options WHERE id=:id'),
                              dict(id=option_id)).mappings().first()
        if not option or option['edition_revision_id'] != revision:
            raise MetadataError('Explicit current source option required')
        if option['format'] not in {'txt', 'md', 'markdown', 'csv', 'html', 'htm',
                                    'fb2', 'epub', 'docx', 'odt'}:
            raise MetadataError('Summary source format requires review')
        members = [dict(row) for row in conn.execute(sql('''
            SELECT m.ordinal,m.edition_revision_id AS revision_id,m.asset_position,
                a.object_sha,o.byte_size,a.file_format,a.asset_role,o.local_relpath,o.state
            FROM option_assets m LEFT JOIN revision_assets a
                ON a.edition_revision_id=m.edition_revision_id AND a.asset_position=m.asset_position
            LEFT JOIN objects o ON o.sha256=a.object_sha
            WHERE m.option_id=:id ORDER BY m.ordinal'''), dict(id=option_id)).mappings()]
        delivery = option['delivery_kind']
        if ((delivery == 'single_file' and len(members) != 1)
                or (delivery == 'bundle' and not 2 <= len(members) <= 200)
                or delivery not in {'single_file', 'bundle'}
                or tuple(m['ordinal'] for m in members) != tuple(range(len(members)))
                or len({m['asset_position'] for m in members}) != len(members)):
            raise MetadataError('Complete ordered summary option required')
        binding = json.loads(initial['source_binding_json'])
        bound_assets = {a['asset_position']: a for a in binding['assets']}
        total_bytes, seen_objects = 0, set()
        for member in members:
            bound = bound_assets.get(member['asset_position'])
            if (member['revision_id'] != revision or member['asset_role'] != 'content'
                    or type(member['asset_position']) is not int or member['asset_position'] < 0
                    or member['file_format'] != option['format'] or member['state'] != 'LIVE'
                    or not bound or any(member[k] != bound[k] for k in
                        ('object_sha', 'byte_size', 'file_format', 'local_relpath', 'state'))
                    or member['local_relpath'] != object_path(member['object_sha'])
                    or type(member['byte_size']) is not int
                    or not 0 <= member['byte_size'] <= Limits().parse_bytes):
                raise MetadataError('Bounded original summary option required')
            if member['object_sha'] in seen_objects:
                raise MetadataError('Duplicate summary source requires review')
            seen_objects.add(member['object_sha'])
            total_bytes += member['byte_size']
        if total_bytes > 64 * 1024**2:
            raise MetadataError('Summary original byte limit exceeded')
        work = conn.execute(sql('SELECT item_type FROM works WHERE id=:id'),
                            dict(id=initial['work_id'])).mappings().first()
        classification = conn.execute(sql('''SELECT * FROM classification_history
            WHERE work_id=:id AND edition_id IS NULL ORDER BY rowid DESC LIMIT 1'''),
            dict(id=initial['work_id'])).mappings().first()
        if (not work or work['item_type'] not in {'novel', 'document'} or not classification
                or not classification['evidence'].strip()):
            raise MetadataError('Summary classification requires review')
        invalid = False
        try:
            event = dict(classification)
            event['before_json'] = json.loads(event['before_json'])
            event['after_json'] = json.loads(event['after_json'])
            coverage = json.loads(option['coverage_json'])
            _coverage(coverage)
            invalid = (type(event['after_json']) is not dict
                       or event['after_json'].get('item_type') != work['item_type']
                       or coverage['confidence'] != option['coverage_confidence'])
            event_digest = digest(canonical_json(event))
        except (ValueError, TypeError, KeyError, RecursionError, PublicationError):
            invalid = True
        if invalid:
            raise MetadataError('Summary classification or coverage requires review')
        edition = conn.execute(sql('SELECT edition_id FROM edition_revisions WHERE id=:id'),
                               dict(id=revision)).scalar_one()
        return canonical_json(dict(
            schema='metadata.summary.context.v1', job_id=lease.job_id,
            generation=lease.generation, owner=lease.owner, work_id=initial['work_id'],
            source_revision_id=revision, edition_id=edition, actor_id=initial['actor_id'],
            actor_authz_version=initial['actor_authz_version'],
            expected_head_version=initial['expected_head_version'], rights_id=initial['rights_id'],
            rights_version=initial['expected_rights_version'], request_digest=initial['request_digest'],
            source_binding=binding, option_id=option_id, format=option['format'],
            delivery_kind=delivery, coverage=coverage, coverage_confidence=option['coverage_confidence'],
            members=members, item_type=work['item_type'], classification_id=classification['id'],
            classification_digest=event_digest))

    def _summary_snapshot(self, lease, option_id):
        with self.jobs.ownership(lease) as conn:
            return self._summary_snapshot_in(conn, lease, option_id)

    @staticmethod
    def _generated_option_snapshot(context):
        members = [{key: member[key] for key in (
            'ordinal', 'revision_id', 'asset_position', 'object_sha', 'byte_size',
            'file_format', 'asset_role', 'local_relpath', 'state')}
            for member in context['members']]
        return dict(schema='metadata.generated.option.v1', option_id=context['option_id'],
            format=context['format'], delivery_kind=context['delivery_kind'],
            coverage=context['coverage'], coverage_confidence=context['coverage_confidence'],
            members=members)

    def generation_context(self, lease, *, option_id):
        """Return safe immutable context fields for a typed generation binding."""
        snapshot = self._summary_snapshot(lease, option_id)
        context = json.loads(snapshot)
        option_snapshot = self._generated_option_snapshot(context)
        return dict(context_digest=digest(snapshot), option_snapshot=option_snapshot,
            option_digest=digest(canonical_json(option_snapshot)),
            source_binding_digest=digest(canonical_json(context['source_binding'])),
            request_digest=context['request_digest'], work_id=context['work_id'],
            source_revision_id=context['source_revision_id'], option_id=option_id,
            classification_event_id=context['classification_id'],
            classification_digest=context['classification_digest'],
            item_type=context['item_type'],
            generation_head_version=context['expected_head_version'],
            rights_id=context['rights_id'],
            generation_rights_version=context['rights_version'])

    @staticmethod
    def _current_option_snapshot(conn, revision, option_id):
        option = conn.execute(sql('SELECT * FROM revision_options WHERE id=:id'),
                              dict(id=option_id)).mappings().first()
        if not option or option['edition_revision_id'] != revision:
            raise MetadataError('Generated option changed')
        members = [dict(row) for row in conn.execute(sql('''
            SELECT m.ordinal,m.edition_revision_id AS revision_id,m.asset_position,
              a.object_sha,o.byte_size,a.file_format,a.asset_role,o.local_relpath,o.state
            FROM option_assets m LEFT JOIN revision_assets a
              ON a.edition_revision_id=m.edition_revision_id AND a.asset_position=m.asset_position
            LEFT JOIN objects o ON o.sha256=a.object_sha
            WHERE m.option_id=:id ORDER BY m.ordinal'''),dict(id=option_id)).mappings()]
        return dict(schema='metadata.generated.option.v1', option_id=option_id,
            format=option['format'], delivery_kind=option['delivery_kind'],
            coverage=json.loads(option['coverage_json']),
            coverage_confidence=option['coverage_confidence'], members=members)

    def _generated_current_in(self, conn, candidate_id, *, at_ms=None):
        """Revalidate completed generated provenance against current source/rights."""
        row = conn.execute(sql('''SELECT c.*,k.version AS control_version,k.review_status,
              k.minimum_rights_version,k.audit_context_json,p.text_value,p.text_sha256,
              a.auxiliary_json,a.auxiliary_digest,b.binding_json,b.binding_digest,
              b.review_unit_id,b.summary_codepoints,
              i.request_digest,i.source_binding_json,j.state AS producer_state
            FROM metadata_candidates c
            JOIN metadata_candidate_control k ON k.candidate_id=c.id
            JOIN metadata_payloads p ON p.candidate_id=c.id
            JOIN metadata_generated_bindings b ON b.candidate_id=c.id
            JOIN metadata_generated_payloads a ON a.candidate_id=c.id
            JOIN metadata_job_inputs i ON i.job_id=c.producing_job_id
            JOIN jobs j ON j.id=c.producing_job_id
            WHERE c.id=:id'''),dict(id=candidate_id)).mappings().first()
        if (not row or row['kind'] != 'description' or row['origin'] != 'QWEN_TEXT'
                or row['producer_state'] != 'completed' or row['text_sha256'] != row['content_sha256']
                or row['producing_job_id'] is None or row['generation'] is None):
            raise MetadataError('Generated candidate is unavailable')
        try:
            binding = json.loads(row['binding_json'])
            canonical, binding_digest = canonical_binding(binding)
            auxiliary = json.loads(row['auxiliary_json'])
            validate_auxiliary(auxiliary, content_sha256=row['content_sha256'])
            auxiliary_digest = digest(canonical_json(auxiliary))
            if (canonical != row['binding_json'] or binding_digest != row['binding_digest']
                    or auxiliary_digest != row['auxiliary_digest']
                    or binding.get('final_output_sha256') != row['content_sha256']
                    or binding.get('candidate_id') != candidate_id
                    or binding.get('job_id') != row['producing_job_id']
                    or binding.get('generation') != row['generation']
                    or binding.get('request_digest') != row['request_digest']):
                raise ValueError
            text_value = row['text_value']
            if (sha256_text(text_value) != row['content_sha256']
                    or len(text_value) != row['summary_codepoints']
                    or generated_review_unit_id(candidate_id, row['binding_digest'],
                        row['content_sha256'], row['summary_codepoints']) != row['review_unit_id']):
                raise ValueError
            option = self._current_option_snapshot(conn, row['source_revision_id'], binding['option_id'])
            if canonical_json(option) != canonical_json(binding['option_snapshot']):
                raise ValueError
            source = self._source(conn, row['work_id'], row['source_revision_id'], current=True)
            if canonical_json(source) != row['source_binding_json']:
                raise ValueError
            rights_row = conn.execute(sql('SELECT * FROM metadata_rights WHERE id=:id'),
                                      dict(id=row['rights_id'])).mappings().first()
            if not rights_row:
                raise ValueError
            now = self._now() if at_ms is None else at_ms
            rights = self._rights(conn, row['rights_id'], row['work_id'], 'description',
                                  rights_row['version'], 'QWEN_TEXT')
            if rights['version'] < row['minimum_rights_version']:
                raise ValueError
            work = conn.execute(sql('SELECT item_type FROM works WHERE id=:id'),
                                dict(id=row['work_id'])).mappings().first()
            classification = conn.execute(sql('''SELECT * FROM classification_history
                WHERE work_id=:id AND edition_id IS NULL ORDER BY rowid DESC LIMIT 1'''),
                dict(id=row['work_id'])).mappings().first()
            if not work or not classification or work['item_type'] != binding['item_type']:
                raise ValueError
            event = dict(classification)
            event['before_json'] = json.loads(event['before_json'])
            event['after_json'] = json.loads(event['after_json'])
            current_classification = digest(canonical_json(event))
            if (classification['id'] != binding['classification_event_id']
                    or current_classification != binding['classification_digest']):
                raise ValueError
            head = self._head(conn, row['work_id'])
            range_hashes = []
            return dict(row=dict(row), binding=binding, binding_digest=binding_digest,
                auxiliary=auxiliary, text=text_value, option_snapshot=option,
                source_binding=source, source_binding_digest=digest(canonical_json(source)),
                classification_digest=current_classification, rights_version=rights['version'],
                head_version=head['version'], head=head, now_ms=now, range_hashes=range_hashes)
        except (KeyError, TypeError, ValueError, RecursionError, GeneratedContractError):
            raise MetadataError('Generated source, rights, or provenance changed') from None

    @staticmethod
    def _generated_review_context(state, ranges):
        row,binding=state['row'],state['binding']
        return dict(schema='metadata.generated.review-context.v1',candidate_id=row['id'],
            binding_digest=state['binding_digest'],summary_sha256=row['content_sha256'],
            source_revision_id=row['source_revision_id'],source_sha256=binding['full_text_sha256'],
            option_digest=binding['option_digest'],classification_digest=state['classification_digest'],
            rights_id=row['rights_id'],rights_version=state['rights_version'],
            head_version=state['head_version'],control_version=row['control_version'],
            ranges=[dict(start=item['start'],end=item['end'],sha256=item['sha256']) for item in ranges])

    def generated_review_detail(self, token, *, candidate_id):
        identifier(candidate_id, 200)
        with self.engine.connect() as conn:
            self.auth.current(conn, token, admin=True)
            row = conn.execute(sql('''SELECT c.id,c.work_id,c.kind,c.origin,c.source_revision_id,
                c.rights_id,c.rights_version_at_creation,c.content_sha256,c.producing_job_id,c.generation,
              k.version AS control_version,k.review_status,h.version AS head_version,
                p.text_value,p.text_sha256,b.binding_json,b.binding_digest,b.review_unit_id,
                b.summary_codepoints,a.auxiliary_json,a.auxiliary_digest
              FROM metadata_candidates c JOIN metadata_candidate_control k ON k.candidate_id=c.id
              JOIN metadata_heads h ON h.work_id=c.work_id
              JOIN metadata_payloads p ON p.candidate_id=c.id
              JOIN metadata_generated_bindings b ON b.candidate_id=c.id
              JOIN metadata_generated_payloads a ON a.candidate_id=c.id WHERE c.id=:id'''),
              dict(id=candidate_id)).mappings().first()
            if not row:
                raise AuthError('RESOURCE_NOT_FOUND',404)
            try:
                binding=json.loads(row['binding_json']);auxiliary=json.loads(row['auxiliary_json'])
                canonical,binding_digest=canonical_binding(binding)
                validate_auxiliary(auxiliary,content_sha256=row['content_sha256'])
                if (canonical!=row['binding_json'] or binding_digest!=row['binding_digest']
                        or sha256_text(row['text_value'])!=row['content_sha256']
                        or len(row['text_value'])!=row['summary_codepoints']
                        or generated_review_unit_id(candidate_id,binding_digest,row['content_sha256'],
                            row['summary_codepoints'])!=row['review_unit_id']):
                    raise ValueError
            except (ValueError,TypeError,GeneratedContractError):
                raise MetadataError('Generated evidence integrity failed') from None
            current=None
            try:
                state=self._generated_current_in(conn,candidate_id)
                current=dict(revalidatable=True,rights_version=state['rights_version'],
                    head_version=state['head_version'],source_current=True,option_current=True,
                    classification_current=True)
            except MetadataError:
                current=dict(revalidatable=False,rights_version=None,
                    head_version=row['head_version'],source_current=False,
                    option_current=False,classification_current=False)
            return dict(candidate_id=candidate_id,work_id=row['work_id'],summary=row['text_value'],
                summary_sha256=row['text_sha256'],item_type=binding['item_type'],
                uncertainties=auxiliary['uncertainties'],
                review_units=list(generated_review_units(candidate_id,row['binding_digest'],row['text_value'])),
                spoiler_risk=auxiliary['spoiler_risk'],qa_status=binding['qa_status'],
                qa_codes=binding['qa_codes'],source_revision_id=row['source_revision_id'],
                option_snapshot=binding['option_snapshot'],selected_ranges=binding['selected_ranges'],
                excluded_tail_range=binding['excluded_tail_range'],full_text_codepoints=binding['full_text_codepoints'],
                full_text_sha256=binding['full_text_sha256'],generation=dict(job_id=row['producing_job_id'],
                generation=row['generation'],head_version=binding['generation_head_version'],
                rights_id=row['rights_id'],rights_version=row['rights_version_at_creation'],
                binding_digest=row['binding_digest'],profile_digest=binding['profile_digest'],
                prompt_policy_digest=binding['prompt_policy_digest'],request_manifest=binding['request_manifest']),
                control=dict(version=row['control_version'],review_status=row['review_status']),
                current=current)

    def read_generated_evidence(self, token, *, candidate_id, ranges):
        identifier(candidate_id, 200)
        try:
            with self.engine.connect() as conn:
                self.auth.current(conn,token,admin=True)
                state=self._generated_current_in(conn,candidate_id)
                binding=state['binding']
                checked=validate_ranges(ranges,source_chars=binding['full_text_codepoints'])
                source_assets=state['source_binding']['assets']
                source_revision=state['row']['source_revision_id']
                option=state['option_snapshot']
                if len(source_assets)>200:
                    raise MetadataError('Generated evidence option exceeds limit')
        except GeneratedContractError:
            raise MetadataError('Generated evidence ranges are invalid') from None
        thread_id=threading.get_ident()

        def revalidate():
            if threading.get_ident()!=thread_id:
                raise MetadataError('Generated evidence reader thread changed')
            with self.engine.connect() as conn:
                self.auth.current(conn,token,admin=True)
                self._generated_current_in(conn,candidate_id)

        assets_by_position={asset['asset_position']:asset for asset in source_assets}
        ordered_assets=[]
        for member in option['members']:
            asset=assets_by_position.get(member['asset_position'])
            if not asset or asset['object_sha']!=member['object_sha']:
                raise MetadataError('Generated evidence option changed')
            ordered_assets.append(asset)
        parts=[]
        for asset in ordered_assets:
            text_value,_,_=self._extract_source_text(asset,revalidate)
            parts.append(text_value)
        source_text='\n'.join(parts)
        if (len(source_text)!=binding['full_text_codepoints']
                or sha256_text(source_text)!=binding['full_text_sha256']):
            raise MetadataError('Generated source text changed')
        selected_end=binding['excluded_tail_range']['start']
        excerpts=[]
        for item in checked:
            excerpt=source_text[item['start']:item['end']]
            excerpts.append(dict(start=item['start'],end=item['end'],text=excerpt,
                sha256=sha256_text(excerpt),tail=item['end']>selected_end))
        try:
            context=self._generated_review_context(state,excerpts)
            digest_value=generated_review_context_digest(context)
            excerpt_bytes=sum(len(item['text'].encode('utf-8')) for item in excerpts)
            if excerpt_bytes>65_536:
                raise GeneratedContractError('EVIDENCE_RESPONSE_TOO_LARGE')
        except GeneratedContractError:
            raise MetadataError('Generated review context exceeds limit') from None
        return dict(candidate_id=candidate_id,review_context_digest=digest_value,
            expected_control_version=state['row']['control_version'],
            expected_head_version=state['head_version'],expected_rights_version=state['rights_version'],
            ranges=excerpts)

    def review_generated_candidate(self,token,csrf,*,candidate_id,expected_control_version,
            expected_head_version,expected_rights_version,request_id,review_context_digest,
            ranges,review_units,grounding_disposition,spoiler_disposition,flag_resolutions,
            decision,select):
        """Record a current-admin, range-grounded generated review and optional selection."""
        identifier(candidate_id,200);identifier(request_id,200)
        _positive(expected_control_version);_positive(expected_head_version,zero=True)
        _positive(expected_rights_version)
        if (type(review_context_digest) is not str or not valid_sha(review_context_digest)
                or type(decision) is not str or decision not in {'approved','rejected'}
                or type(select) is not bool or (select and decision!='approved')):
            raise MetadataError('Explicit generated review decision required')
        if grounding_disposition not in {'grounded','insufficient'}:
            raise MetadataError('Explicit grounding disposition required')
        if spoiler_disposition not in {'acceptable','reject','not_applicable'}:
            raise MetadataError('Explicit spoiler disposition required')
        try:
            ranges_checked=validate_ranges(ranges,source_chars=16_000_000)
        except GeneratedContractError:
            raise MetadataError('Generated evidence ranges are invalid') from None
        if type(review_units) is not list or len(review_units)>1:
            raise MetadataError('Bounded generated review-unit coverage is required')
        normalized_units=[];unit_ids=set()
        for unit in review_units:
            if type(unit) is not dict or set(unit)!={'unit_id','range_indexes'}:
                raise MetadataError('Typed generated review-unit evidence required')
            unit_id=unit['unit_id'];indexes=unit['range_indexes']
            if (type(unit_id) is not str or not re.fullmatch(r'summary-v1:[0-9a-f]{64}',unit_id)
                    or unit_id in unit_ids or type(indexes) is not list or not indexes or len(indexes)>16
                    or any(type(i) is not int or not 0<=i<len(ranges_checked) for i in indexes)
                    or len(set(indexes))!=len(indexes)):
                raise MetadataError('Generated review-unit evidence references are invalid')
            unit_ids.add(unit_id);normalized_units.append(dict(unit_id=unit_id,range_indexes=list(indexes)))
        if type(flag_resolutions) is not list or len(flag_resolutions)>64:
            raise MetadataError('Bounded generated flag decisions are required')
        normalized_flags=[];flag_codes=set()
        for flag in flag_resolutions:
            if type(flag) is not dict or set(flag)!={'code','resolution','range_indexes'}:
                raise MetadataError('Typed generated flag decision required')
            code,resolution,indexes=flag['code'],flag['resolution'],flag['range_indexes']
            if (type(code) is not str or not 1<=len(code)<=256 or code in flag_codes
                    or resolution not in {'accept_with_evidence','reject','fix_required'}
                    or type(indexes) is not list or len(indexes)>16
                    or any(type(i) is not int or not 0<=i<len(ranges_checked) for i in indexes)
                    or len(set(indexes))!=len(indexes)
                    or (resolution=='accept_with_evidence' and not indexes)):
                raise MetadataError('Generated flag decision is invalid')
            flag_codes.add(code);normalized_flags.append(dict(code=code,resolution=resolution,
                range_indexes=list(indexes)))
        submitted_ranges=[dict(start=item['start'],end=item['end']) for item in ranges_checked]

        def replay_receipt(conn):
            replay=conn.execute(sql("SELECT after_json FROM metadata_control_events WHERE candidate_id=:id AND request_id=:request AND action='admin'"),
                dict(id=candidate_id,request=request_id)).scalar()
            if not replay:return None
            try:
                after=json.loads(replay)
                if type(after) is not dict or type(after.get('audit_context_json')) is not str:
                    raise ValueError
                context=json.loads(after['audit_context_json'])
                if type(context) is not dict:
                    raise ValueError
            except (ValueError,TypeError,KeyError,RecursionError):
                raise MetadataError('Generated review replay is unavailable') from None
            stored_ranges=context.get('ranges')
            if (type(stored_ranges) is not list or any(
                    type(item) is not dict
                    or set(item)!={'start','end','sha256','tail'}
                    or type(item['start']) is not int or type(item['end']) is not int
                    or not valid_sha(item['sha256']) or type(item['tail']) is not bool
                    for item in stored_ranges)
                    or [dict(start=item['start'],end=item['end']) for item in stored_ranges]
                       !=submitted_ranges):
                raise MetadataError('Generated review request replay conflicts')
            operation_value=dict(candidate_id=candidate_id,
                expected_control_version=expected_control_version,expected_head_version=expected_head_version,
                expected_rights_version=expected_rights_version,review_context_digest=review_context_digest,
                ranges=stored_ranges,review_units=normalized_units,grounding=grounding_disposition,
                spoiler=spoiler_disposition,flags=normalized_flags,decision=decision,select=select)
            if (context.get('operation')!='generated_human_review_v1'
                    or context.get('operation_digest')!=digest(canonical_json(operation_value))
                    or context.get('review_context_digest')!=review_context_digest):
                raise MetadataError('Generated review request replay conflicts')
            return context.get('receipt')

        with self.engine.connect() as conn:
            self.auth.current(conn,token,csrf=csrf,admin=True)
            replayed=replay_receipt(conn)
            if replayed is not None:return replayed
        # Read current administrator evidence through the same locked source
        # reader used by the display endpoint; snippets stay transient.
        evidence=self.read_generated_evidence(token,candidate_id=candidate_id,ranges=ranges)
        evidence_ranges=[dict(start=item['start'],end=item['end'],sha256=item['sha256'],tail=item['tail'])
                         for item in evidence['ranges']]
        with write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True)
            replayed=replay_receipt(conn)
            if replayed is not None:return replayed
            state=self._generated_current_in(conn,candidate_id)
            row=state['row'];binding=state['binding']
            if (row['control_version']!=expected_control_version or state['head_version']!=expected_head_version
                    or state['rights_version']!=expected_rights_version
                    or state['head_version']!=evidence['expected_head_version']
                    or row['control_version']!=evidence['expected_control_version']
                    or state['rights_version']!=evidence['expected_rights_version']):
                raise MetadataError('Generated review context changed')
            context_digest=generated_review_context_digest(self._generated_review_context(state,evidence_ranges))
            if context_digest!=review_context_digest or context_digest!=evidence['review_context_digest']:
                raise MetadataError('Generated review evidence changed')
            qa_codes=binding['qa_codes']
            expected_units=generated_review_units(candidate_id,state['binding_digest'],row['text_value'])
            expected_unit_ids={item['unit_id'] for item in expected_units}
            if not unit_ids.issubset(expected_unit_ids):
                raise MetadataError('Generated review-unit id was not issued for this summary')
            required_flags=set(qa_codes)-{'SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'}
            if flag_codes!=required_flags:
                raise MetadataError('Every generated risk and uncertainty flag needs a decision')
            if decision=='approved':
                tail=binding['excluded_tail_range']
                for item in evidence_ranges:
                    if (item['end']>binding['full_text_codepoints']
                            or (item['tail'] and not (item['start']>=tail['start']
                                and item['end']<=tail['end']))
                            or (not item['tail'] and item['end']>tail['start'])):
                        raise MetadataError('Generated review evidence is outside its selected source ranges')
                if (unit_ids!=expected_unit_ids or grounding_disposition!='grounded'
                        or any(evidence_ranges[index]['tail'] for unit in normalized_units
                               for index in unit['range_indexes'])
                        or any(flag['resolution']!='accept_with_evidence'
                               or not flag['range_indexes'] for flag in normalized_flags)):
                    raise MetadataError('Generated review-unit coverage or findings remain unresolved')
                if binding['item_type']=='novel':
                    if spoiler_disposition!='acceptable' or not any(item['tail'] for item in evidence_ranges):
                        raise MetadataError('Novel ending evidence must be reviewed')
                elif spoiler_disposition not in {'acceptable','not_applicable'}:
                    raise MetadataError('Document spoiler disposition is invalid')
            elif select:
                raise MetadataError('Rejected generated candidate cannot be selected')
            head=self._head(conn,row['work_id'],expected_head_version)
            decision_record=dict(schema='metadata.generated.decision.v1',candidate_id=candidate_id,
                binding_digest=state['binding_digest'],review_context_digest=context_digest,
                expected_control_version=expected_control_version,expected_head_version=expected_head_version,
                expected_rights_version=expected_rights_version,grounding=grounding_disposition,
                spoiler=spoiler_disposition,review_units=normalized_units,ranges=evidence_ranges,
                flags=normalized_flags,decision=decision,select=select)
            decision_json=_json(decision_record,MAX_REVIEW_BYTES)
            decision_digest=digest(decision_json)
            operation_value=dict(candidate_id=candidate_id,
                expected_control_version=expected_control_version,expected_head_version=expected_head_version,
                expected_rights_version=expected_rights_version,review_context_digest=context_digest,
                ranges=evidence_ranges,review_units=normalized_units,grounding=grounding_disposition,
                spoiler=spoiler_disposition,flags=normalized_flags,decision=decision,select=select)
            operation_digest=digest(canonical_json(operation_value))
            audit=dict(operation='generated_human_review_v1',candidate_id=candidate_id,
                binding_digest=state['binding_digest'],review_context_digest=context_digest,
                decision_digest=decision_digest,operation_digest=operation_digest,
                expected_control_version=expected_control_version,expected_head_version=expected_head_version,
                expected_rights_version=expected_rights_version,decision=decision,
                select=select,semantic_review='HUMAN_ADMIN_ATTESTED',
                grounding=grounding_disposition,spoiler=spoiler_disposition,
                review_units=normalized_units,ranges=evidence_ranges,flags=normalized_flags)
            # Probe `_select` inside a rollback-only savepoint so the exact
            # resulting receipt can be frozen in the single durable control event.
            if decision=='approved' and select:
                conn.exec_driver_sql('SAVEPOINT generated_review_receipt_probe')
                try:
                    probe_context=_json(audit|dict(receipt={}),4096)
                    changed=conn.execute(sql("UPDATE metadata_candidate_control SET version=version+1,review_status='approved',actor_kind='admin',actor_id=:actor,source_snapshot_sha256=NULL,request_id=:request,updated_at_ms=:now,audit_context_json=:context WHERE candidate_id=:id AND version=:version"),
                        dict(actor=actor['id'],request=request_id,now=self._now(),context=probe_context,
                             id=candidate_id,version=expected_control_version)).rowcount
                    if changed!=1:raise MetadataError('Generated review control conflict')
                    probe_head=self._head(conn,row['work_id'])
                    predicted=self._select(conn,dict(row),probe_head['version'],force=False)
                finally:
                    conn.exec_driver_sql('ROLLBACK TO SAVEPOINT generated_review_receipt_probe')
                    conn.exec_driver_sql('RELEASE SAVEPOINT generated_review_receipt_probe')
            else:
                predicted=self._receipt(conn,row['work_id'])
                if head['description_id']==candidate_id:
                    predicted=dict(predicted,version=predicted['version']+1)
            receipt=dict(predicted,candidate_id=candidate_id,review_status=decision,
                control_version=expected_control_version+1,selected=predicted['description_id']==candidate_id)
            audit_json=_json(audit|dict(receipt=receipt),4096)
            changed=conn.execute(sql("UPDATE metadata_candidate_control SET version=version+1,review_status=:status,actor_kind='admin',actor_id=:actor,source_snapshot_sha256=NULL,request_id=:request,updated_at_ms=:now,audit_context_json=:context WHERE candidate_id=:id AND version=:version"),
                dict(status=decision,actor=actor['id'],request=request_id,now=self._now(),context=audit_json,
                     id=candidate_id,version=expected_control_version)).rowcount
            if changed!=1:raise MetadataError('Generated review control conflict')
            if decision=='approved' and select:
                actual_head=self._head(conn,row['work_id'])
                actual=self._select(conn,dict(row),actual_head['version'],force=False)
                if actual!=predicted:raise MetadataError('Generated selection receipt changed')
            self._backup(conn,row['work_id'])
            return receipt

    @contextmanager
    def prepare_summary_input(self, lease, *, option_id, max_source_chars=MAX_SOURCE_CHARS):
        """Prepare all parser text of one selected option, without inference.

        PDF and OCR completeness are not admitted. Aggregate parser expansion and
        CPU accounting remain NOT_VERIFIED. No DB/file lock spans assembly or use.
        """
        identifier(option_id, 200)
        if type(max_source_chars) is not int or not 0 < max_source_chars <= MAX_SOURCE_CHARS:
            raise MetadataError('Bounded summary source characters required')
        snapshot = self._summary_snapshot(lease, option_id)
        fixed = json.loads(snapshot)
        deadline = time.monotonic() + 120
        parts, offsets, assets, separators, seen_texts = [], [], [], [], set()
        total_chars, total_headings = 0, 0

        def fence():
            if self._summary_snapshot(lease, option_id) != snapshot:
                raise MetadataError('Fixed summary option input changed')

        for member in fixed['members']:
            fence()
            if time.monotonic() + Limits().wall_seconds > deadline:
                raise MetadataError('Summary parser wall admission exceeded')
            result = self.read_source_text(lease, asset_position=member['asset_position'])
            fence()
            if time.monotonic() > deadline:
                raise MetadataError('Summary parser wall admission exceeded')
            if (type(result) is not SourceTextRead or type(result.text) is not str or not result.text
                    or (result.job_id, result.generation, result.work_id, result.source_revision_id,
                        result.asset_position, result.original_sha256, result.original_byte_size,
                        result.file_format, result.expected_head_version, result.rights_id,
                        result.rights_version, result.coverage) !=
                    (lease.job_id, lease.generation, fixed['work_id'], fixed['source_revision_id'],
                        member['asset_position'], member['object_sha'], member['byte_size'],
                        member['file_format'], fixed['expected_head_version'], fixed['rights_id'],
                        fixed['rights_version'], 'asset_only')
                    or type(result.text_codepoints) is not int
                    or result.text_codepoints != len(result.text)
                    or result.text_codepoints > Limits().text_chars
                    or _summary_text_digest(result.text) != result.text_sha256):
                raise MetadataError('Summary reader identity requires review')
            if result.text_sha256 in seen_texts:
                raise MetadataError('Duplicate summary parser text requires review')
            seen_texts.add(result.text_sha256)
            start = total_chars + bool(parts)
            total_chars = start + result.text_codepoints
            if total_chars > max_source_chars:
                raise MetadataError('Summary source character limit exceeded')
            # Match only parser logical newlines; splitlines also splits Unicode separators.
            for line in re.finditer(r'[^\r\n]*(?:\r\n|\r|\n|$)', result.text):
                if HEADING.fullmatch(line.group().rstrip('\r\n')):
                    total_headings += 1
                    if total_headings > 5000:
                        raise MetadataError('Summary heading limit exceeded')
            if parts:
                separators.append(dict(start=start - 1, end=start))
            offsets.append(start)
            parts.append(result.text)
            assets.append(dict(ordinal=member['ordinal'], revision_id=member['revision_id'],
                asset_position=member['asset_position'], original_sha256=result.original_sha256,
                original_byte_size=result.original_byte_size, file_format=result.file_format,
                text_sha256=result.text_sha256, text_codepoints=result.text_codepoints,
                start=start, end=total_chars))
        if not total_headings and fixed['coverage']['confidence'] == 'verified':
            raise MetadataError('VERIFIED_COVERAGE_UNPROVEN: summary input requires review')
        chapters = _summary_chapters(tuple(parts), tuple(offsets),
            document_kind=fixed['item_type'], coverage=fixed['coverage'])
        body = '\n'.join(parts)
        receipt = dict(schema='metadata.summary.assembly.v1',
            coverage='whole_selected_option_parser_text_v1', context_digest=digest(snapshot),
            option_id=option_id, assets=assets, separator='LF_between_assets_v1',
            separators=separators, text_sha256=_summary_text_digest(body),
            text_codepoints=total_chars, chapter_count=total_headings,
            chapter_map_status='complete' if chapters is not None else 'unavailable',
            visual_text_completeness='NOT_VERIFIED', aggregate_expansion_cpu='NOT_VERIFIED')
        fence()
        if time.monotonic() > deadline:
            raise MetadataError('Summary parser wall admission exceeded')
        prepared = _PreparedSummaryInput(self, lease, option_id, snapshot, body, chapters,
                                         receipt, max_source_chars)
        # Do not keep assembly's additional body references alive across caller code.
        parts.clear()
        result = body = line = None
        try:
            yield prepared
            prepared._revalidate()
        finally:
            prepared._close()

    def _control(self,conn,candidate,actor,request,*,retained=False,context=None):
        conn.execute(sql('''INSERT INTO metadata_candidate_control VALUES(:id,1,'approved',:retained,:rv,'admin',:actor,:request,:now,NULL,:context)'''),
          dict(id=candidate['id'],retained=int(retained),rv=candidate['rights_version_at_creation'],actor=actor,request=request,now=self._now(),context=_json(context or {},4096)))

    def _candidate(self,conn,*,ident,work,kind,origin,rights,rv,revision,sha,provenance,job=None,generation=None):
        parsed=json.loads(provenance)
        candidate=dict(id=ident,work_id=work,kind=kind,origin=origin,rights_id=rights,rights_version_at_creation=rv,
                       source_revision_id=revision,object_sha=sha if kind=='cover' else None,source_url=parsed.get('source_url'),content_sha256=sha,
                       provenance_json=provenance,producing_job_id=job,generation=generation,created_at_ms=self._now())
        conn.execute(sql('''INSERT INTO metadata_candidates VALUES(:id,:work_id,:kind,:origin,:rights_id,:rights_version_at_creation,:source_revision_id,:object_sha,:source_url,:content_sha256,:provenance_json,:producing_job_id,:generation,:created_at_ms)'''),candidate)
        if origin in LOCAL_ORIGINS:
            binding=json.loads(conn.execute(sql('SELECT source_binding_json FROM metadata_job_inputs WHERE job_id=:id'),dict(id=job)).scalar_one())
            for proof in binding['proofs']:
                conn.execute(sql('INSERT INTO metadata_candidate_sources VALUES(:candidate,:revision,:role,:position,:job,:generation,:digest)'),dict(candidate=ident,revision=revision,role=proof['proof_role'],position=proof['asset_position'],job=proof['source_job_id'],generation=proof['source_generation'],digest=proof['payload_digest']))
        return candidate

    def _select(self,conn,candidate,expected,*,force=False):
        head=self._head(conn,candidate['work_id'],expected);column=candidate['kind']+'_id'
        now=self._now()
        eligible=sql('SELECT 1 FROM metadata_eligible_candidates_v1 c JOIN metadata_rights r ON r.id=c.rights_id WHERE c.id=:id AND (r.cache_until_ms IS NULL OR r.cache_until_ms>:now)')
        if not conn.execute(eligible,dict(id=candidate['id'],now=now)).first():raise MetadataError('Candidate is not eligible')
        old=head[column]
        if old is not None and not force:
            old_eligible=conn.execute(eligible,dict(id=old,now=now)).first()
            if old_eligible is not None:
                origin=conn.execute(sql('SELECT origin FROM metadata_candidates WHERE id=:id'),dict(id=old)).scalar_one()
                if candidate['kind']=='cover':
                    cover_priority={'EDITORIAL':0,'EMBEDDED':1,'PUBLISHER_OFFICIAL':2,'FIRST_PAGE':3}
                    old_priority=cover_priority[origin] if origin in cover_priority else (PRIORITY[origin]+4 if origin in ('QWEN_TEXT','QWEN_OCR') else PRIORITY[origin])
                    new_origin=candidate['origin']
                    candidate_priority=cover_priority[new_origin] if new_origin in cover_priority else (PRIORITY[new_origin]+4 if new_origin in ('QWEN_TEXT','QWEN_OCR') else PRIORITY[new_origin])
                else:
                    old_priority=PRIORITY[origin];candidate_priority=PRIORITY[candidate['origin']]
                if old_priority<candidate_priority:return self._receipt(conn,candidate['work_id'])
        if old!=candidate['id']:
            count=conn.execute(sql(f'UPDATE metadata_heads SET {column}=:id,version=version+1,updated_at_ms=:now WHERE work_id=:work AND version=:version'),dict(id=candidate['id'],work=candidate['work_id'],version=expected,now=now)).rowcount
            if count!=1:raise MetadataError('Metadata selection CAS conflict')
        return self._receipt(conn,candidate['work_id'])

    def edit_description(self,token,csrf,*,work_id,text,rights_id,expected_rights_version,expected_head_version,request_id):
        if not isinstance(text,str) or not 1<=len(text)<=16384 or not text.strip():raise MetadataError('Bounded nonempty description required')
        for value in (work_id,rights_id,request_id):identifier(value,200)
        sha=hashlib.sha256(text.encode()).hexdigest();ident=str(uuid.uuid5(uuid.NAMESPACE_URL,'metadata-editorial:'+request_id))
        operation=digest(_json(dict(work_id=work_id,text_sha256=sha,rights_id=rights_id,rights_version=expected_rights_version,head_version=expected_head_version),4096))
        with write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True)
            replay=conn.execute(sql("SELECT after_json FROM metadata_control_events WHERE candidate_id=:id AND request_id=:request AND action='admin'"),dict(id=ident,request=request_id)).scalar()
            if replay:
                context=json.loads(json.loads(replay)['audit_context_json'])
                if context.get('operation_digest')!=operation:raise MetadataError('Description request replay conflicts')
                return context['receipt']
            self._head(conn,work_id,expected_head_version);self._rights(conn,rights_id,work_id,'description',expected_rights_version,'EDITORIAL')
            candidate=self._candidate(conn,ident=ident,work=work_id,kind='description',origin='EDITORIAL',rights=rights_id,rv=expected_rights_version,revision=None,sha=sha,provenance='{}')
            conn.execute(sql('INSERT INTO metadata_payloads VALUES(:id,:value,:sha)'),dict(id=ident,value=text,sha=sha))
            head=self._head(conn,work_id);receipt=dict(work_id=work_id,description_id=ident,cover_id=head['cover_id'],version=head['version']+1)
            self._control(conn,candidate,actor['id'],request_id,context=dict(operation_digest=operation,receipt=receipt))
            result=self._select(conn,candidate,expected_head_version,force=True);self._backup(conn,work_id)
            self.auth.audit(conn,actor['id'],'metadata_description',details={'candidate_id':ident,'content_sha256':sha})
            return result

    def publish_description(self,lease,*,text,provenance):
        if not isinstance(text,str) or not 1<=len(text)<=16384 or not text.strip():raise MetadataError('Bounded nonempty description required')
        with self.jobs.ownership(lease) as conn:
            initial=self._input(conn,lease,'description')
            if initial['origin'] in {'QWEN_TEXT','QWEN_OCR'}:
                raise MetadataError('Generated descriptions require pending human review')
        evidence=_provenance(provenance);sha=hashlib.sha256(text.encode()).hexdigest();ident=str(uuid.uuid5(uuid.UUID(lease.job_id),f'metadata:{lease.generation}'))
        quality=json.loads(evidence).get('quality',{})
        if 'passed' in quality and quality['passed'] is not True:
            raise MetadataError('Description quality review did not pass')
        with self.jobs.completion(lease,result={'candidate_id':ident},followups=[dict(kind='backup_catalog',idempotency_key='metadata-published:'+ident,payload={'candidate_id':ident})],
             outbox=[dict(kind='metadata_published',dedupe_key='metadata:'+ident,payload={'candidate_id':ident})],finalize=lambda conn:self._finalize_description_in(conn,lease,ident,receipt)) as conn:
            source=self._input(conn,lease,'description')
            candidate=self._candidate(conn,ident=ident,work=source['work_id'],kind='description',origin=source['origin'],rights=source['rights_id'],rv=source['expected_rights_version'],revision=source['source_revision_id'],sha=sha,provenance=evidence,job=lease.job_id,generation=lease.generation)
            conn.execute(sql('INSERT INTO metadata_payloads VALUES(:id,:value,:sha)'),dict(id=ident,value=text,sha=sha))
            self._control(conn,candidate,source['actor_id'],'publish:'+ident)
            receipt=self._select(conn,candidate,source['expected_head_version']);self._backup(conn,source['work_id'])
        return receipt

    def _finalize_description_in(self,conn,lease,candidate_id,receipt):
        row=conn.execute(sql('SELECT * FROM metadata_candidates WHERE id=:id'),dict(id=candidate_id)).mappings().one()
        head=self._head(conn,row['work_id']);
        if self._receipt(conn,row['work_id'])!=receipt:raise MetadataError('Description head drifted before commit')
        source=self._input(conn,lease,'description',head_version=head['version'])
        if row['producing_job_id']!=lease.job_id or row['generation']!=lease.generation or not conn.execute(sql('SELECT 1 FROM metadata_eligible_candidates_v1 WHERE id=:id'),dict(id=candidate_id)).first():
            raise MetadataError('Final metadata description differs')
        if head['description_id']==candidate_id and head['version']!=source['expected_head_version']+1:
            raise MetadataError('New metadata head drifted before commit')

    def store_generated_description(self, lease, *, output, binding, auxiliary):
        """Atomically finish one typed generated result as pending human review."""
        if type(output) is not SummaryOutput:
            raise MetadataError('Parsed generated output required')
        try:
            output = parse_summary_output(canonical_json(dict(summary=output.summary,
                uncertainties=list(output.uncertainties),spoiler_risk=output.spoiler_risk)),
                max_response_bytes=65_536)
            auxiliary_value=dict(schema=AUXILIARY_SCHEMA,
                uncertainties=list(output.uncertainties),spoiler_risk=output.spoiler_risk)
            if auxiliary != auxiliary_value:
                raise GeneratedContractError('AUXILIARY_OUTPUT_MISMATCH')
            validate_auxiliary(auxiliary_value,content_sha256=sha256_text(output.summary))
            if type(binding) is not dict:
                raise GeneratedContractError('INVALID_BINDING')
            binding=dict(binding)
            binding['created_at_ms']=self._now()
            binding_json,binding_digest=canonical_binding(binding)
            if binding['final_output_sha256']!=sha256_text(output.summary):
                raise GeneratedContractError('OUTPUT_BINDING_MISMATCH')
            aux_json=canonical_json(auxiliary_value)
            if len(aux_json.encode('utf-8'))>32768:
                raise GeneratedContractError('AUXILIARY_TOO_LARGE')
            aux_digest=digest(aux_json)
        except (GeneratedContractError,ValueError,TypeError,UnicodeError,RecursionError):
            raise MetadataError('Generated result contract failed') from None
        ident=str(uuid.uuid5(uuid.UUID(lease.job_id),f'metadata:{lease.generation}'))
        if binding.get('candidate_id')!=ident or binding.get('job_id')!=lease.job_id or binding.get('generation')!=lease.generation:
            raise MetadataError('Generated result producer binding differs')
        if binding.get('qa_status') not in {'reject','review_required'}:
            raise MetadataError('Generated output cannot self-approve')
        manifest=binding['request_manifest']
        try:
            if not 1<=len(manifest)<=1025 or any(item.get('terminal') is not True or item.get('fully_drained') is not True or item.get('finish_reason')=='length' for item in manifest):
                raise ValueError
        except (AttributeError,TypeError,ValueError):
            raise MetadataError('Generated terminal request receipt is incomplete') from None
        context_digest=binding['context_digest']

        def finalize(conn):
            snapshot=self._summary_snapshot_in(conn,lease,binding['option_id'])
            parsed=json.loads(snapshot)
            option_snapshot=self._generated_option_snapshot(parsed)
            if (digest(snapshot)!=context_digest
                    or digest(canonical_json(option_snapshot))!=binding['option_digest']
                    or canonical_json(option_snapshot)!=canonical_json(binding['option_snapshot'])
                    or digest(canonical_json(parsed['source_binding']))!=binding['source_binding_digest']
                    or parsed['classification_id']!=binding['classification_event_id']
                    or parsed['classification_digest']!=binding['classification_digest']
                    or parsed['work_id']!=binding['work_id']
                    or parsed['source_revision_id']!=binding['source_revision_id']
                    or parsed['item_type']!=binding['item_type']
                    or parsed['expected_head_version']!=binding['generation_head_version']
                    or parsed['rights_id']!=binding['rights_id']
                    or parsed['rights_version']!=binding['generation_rights_version']
                    or parsed['request_digest']!=binding['request_digest']):
                raise MetadataError('Generated source or job context drifted before storage')
            source=self._input(conn,lease,'description')
            candidate=self._candidate(conn,ident=ident,work=source['work_id'],kind='description',
                origin='QWEN_TEXT',rights=source['rights_id'],rv=source['expected_rights_version'],
                revision=source['source_revision_id'],sha=sha256_text(output.summary),
                provenance=_provenance({'quality':{'provider':'local','reason_code':'human_review_required'}}),
                job=lease.job_id,generation=lease.generation)
            conn.execute(sql('INSERT INTO metadata_payloads VALUES(:id,:value,:sha)'),
                dict(id=ident,value=output.summary,sha=candidate['content_sha256']))
            summary_codepoints=len(output.summary)
            review_unit_id=generated_review_unit_id(
                ident,binding_digest,binding['final_output_sha256'],summary_codepoints)
            conn.execute(sql('''INSERT INTO metadata_generated_bindings
                (candidate_id,schema_version,binding_json,binding_digest,review_unit_id,
                 summary_codepoints,created_at_ms)
                VALUES(:id,:schema,:json,:digest,:unit,:codepoints,:now)'''),
                dict(id=ident,schema=GENERATED_SCHEMA,json=binding_json,digest=binding_digest,
                     unit=review_unit_id,codepoints=summary_codepoints,now=binding['created_at_ms']))
            conn.execute(sql('INSERT INTO metadata_generated_payloads VALUES(:id,:sha,:json,:digest,:now)'),
                dict(id=ident,sha=candidate['content_sha256'],json=aux_json,digest=aux_digest,now=binding['created_at_ms']))
            pending_context=_json(dict(operation='generated_pending_from_admin_request',candidate_id=ident,
                binding_digest=binding_digest,semantic_review='NOT_PERFORMED',qa_status=binding['qa_status']),4096)
            conn.execute(sql('''INSERT INTO metadata_candidate_control
                (candidate_id,version,review_status,retained,minimum_rights_version,actor_kind,actor_id,
                 request_id,updated_at_ms,source_snapshot_sha256,audit_context_json)
                VALUES(:id,1,'pending',0,:rv,'admin',:actor,:request,:now,NULL,:context)'''),
                dict(id=ident,rv=source['expected_rights_version'],actor=source['actor_id'],
                     request='generated-pending:'+ident,now=self._now(),context=pending_context))
            self._backup(conn,source['work_id'])
            return candidate

        try:
            with self.jobs.completion(lease,result={'candidate_id':ident,'state':'pending_review',
                'binding_digest':binding_digest},
                followups=[dict(kind='backup_catalog',idempotency_key='metadata-generated:'+ident,
                    payload={'candidate_id':ident})],
                outbox=[dict(kind='metadata_review_pending',dedupe_key='metadata-review:'+ident,
                    payload={'candidate_id':ident})],finalize=finalize):
                pass
        except MetadataError:
            raise
        except Exception:
            raise MetadataError('Generated pending storage transaction failed') from None
        with self.engine.connect() as conn:
            return self._receipt(conn,binding['work_id']) | dict(candidate_id=ident,
                state='pending_review',binding_digest=binding_digest)

    def register_cover(self,lease,*,stage_path,filename,provenance):
        if not isinstance(filename,str) or filename.rsplit('.',1)[-1].lower() not in IMAGES:
            raise MetadataError('A single raster cover is required')
        if not isinstance(stage_path,str) or not stage_path.startswith(lease.staging_key+'/'):
            raise MetadataError('Cover stage belongs to another execution')
        evidence=_provenance(provenance)
        with self.engine.connect() as conn:
            self._input(conn,lease,'cover')
            job=conn.execute(sql('SELECT state,lease_generation,lease_owner,lease_until FROM jobs WHERE id=:id'),dict(id=lease.job_id)).mappings().one()
            if job['state']!='running' or job['lease_generation']!=lease.generation or job['lease_owner']!=lease.owner or job['lease_until']<=self._now():
                raise MetadataError('Current cover lease required before inspecting bytes')
            old=conn.execute(sql('SELECT 1 FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).first()
            prior=self._cover_plan_in(conn,lease) if old else None
        with self.publisher.root.file(stage_path) as fd:report=inspect_fd(fd,filename,self.COVER_LIMITS)
        if report.get('representation')!='single_image' or report.get('status')!='validated':
            raise MetadataError('Validated single raster cover required')
        if prior:
            asset=prior['asset']
            if asset['sha256']!=report['sha256'] or asset['byte_size']!=report['byte_size'] or asset['file_format']!=report['format']:
                raise MetadataError('Cover registration bytes changed')
        else:
            asset=self.publisher.prepare(lease,stage_path,sha256=report['sha256'],byte_size=report['byte_size'],
                detected_mime=IngestionService._mime(report['format']),file_format=report['format'])
            asset.pop('asset_role')
        try:
            with self.publisher.root.file(asset['prepared_relpath']) as fd:prepared=inspect_fd(fd,filename,self.COVER_LIMITS)
        except FileNotFoundError:
            if prior is None:raise
            with self.publisher.root.file(object_path(asset['sha256'])) as fd:prepared=inspect_fd(fd,filename,self.COVER_LIMITS)
        if any(prepared.get(key)!=report.get(key) for key in ('sha256','byte_size','format','image')):
            raise MetadataError('Prepared cover differs from validated raster')
        with self.publisher.locks.mutation([asset['sha256']]),self.jobs.ownership(lease) as conn:
            source=self._input(conn,lease,'cover')
            plan=dict(schema=COVER_SCHEMA,work_id=source['work_id'],candidate_id=str(uuid.uuid5(uuid.UUID(lease.job_id),f'metadata:{lease.generation}')),
                origin=source['origin'],rights_id=source['rights_id'],rights_version=source['expected_rights_version'],expected_head_version=source['expected_head_version'],
                input_binding_digest=source['request_digest'],candidate_spec_digest=digest(evidence),asset=asset)
            encoded=_json(plan,65536);receipt=_json(dict(provenance=json.loads(evidence),filename=filename),65536)
            old=conn.execute(sql('SELECT * FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).mappings().first()
            if old:
                if old['rights_status']!='allowed' or old['payload_json']!=encoded or old['payload_digest']!=digest(encoded) or old['validation_receipt']!=receipt:
                    raise MetadataError('Cover plan replay conflicts')
                return plan
            conn.execute(sql("INSERT INTO publication_plans VALUES(:id,:generation,:payload,:digest,:receipt,'allowed',:now)"),dict(id=lease.job_id,generation=lease.generation,payload=encoded,digest=digest(encoded),receipt=receipt,now=self._now()))
            return plan

    def _cover_plan_in(self,conn,lease,*,head_version=None):
        source=self._input(conn,lease,'cover',head_version=head_version)
        row=conn.execute(sql('SELECT * FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).mappings().first()
        if not row or row['rights_status']!='allowed' or digest(row['payload_json'])!=row['payload_digest']:raise MetadataError('Cover plan authority changed')
        plan=json.loads(row['payload_json']);keys={'schema','work_id','candidate_id','origin','rights_id','rights_version','expected_head_version','input_binding_digest','candidate_spec_digest','asset'}
        if set(plan)!=keys or plan['schema']!=COVER_SCHEMA or any(plan[k]!=v for k,v in dict(work_id=source['work_id'],origin=source['origin'],rights_id=source['rights_id'],rights_version=source['expected_rights_version'],expected_head_version=source['expected_head_version'],input_binding_digest=source['request_digest']).items()):raise MetadataError('Typed cover plan binding differs')
        evidence=json.loads(row['validation_receipt']);_provenance(evidence['provenance'])
        if digest(canonical_json(evidence['provenance']))!=plan['candidate_spec_digest']:raise MetadataError('Cover evidence digest differs')
        asset=plan['asset']
        if set(asset)!={'sha256','byte_size','detected_mime','file_format','prepared_relpath'} or not asset['prepared_relpath'].startswith(self.publisher._prepared_prefix(lease)):raise MetadataError('Typed cover asset fields differ')
        object_path(asset['sha256'])
        if (type(asset['byte_size']) is not int or not 1<=asset['byte_size']<=self.COVER_LIMITS.file_bytes
                or asset['file_format'] not in IMAGES or asset['detected_mime']!=IngestionService._mime(asset['file_format'])
                or plan['candidate_id']!=str(uuid.uuid5(uuid.UUID(lease.job_id),f'metadata:{lease.generation}'))):raise MetadataError('Validated cover asset identity differs')
        return plan

    def publish_cover(self,lease):
        return self.publisher.publish_cover(lease,metadata=self)

    def _commit_cover_in(self,conn,lease,plan):
        source=self._input(conn,lease,'cover');evidence=json.loads(conn.execute(sql('SELECT validation_receipt FROM publication_plans WHERE job_id=:id AND generation=:generation'),dict(id=lease.job_id,generation=lease.generation)).scalar_one())
        candidate=self._candidate(conn,ident=plan['candidate_id'],work=source['work_id'],kind='cover',origin=source['origin'],rights=source['rights_id'],rv=source['expected_rights_version'],revision=source['source_revision_id'],sha=plan['asset']['sha256'],provenance=_provenance(evidence['provenance']),job=lease.job_id,generation=lease.generation)
        self._control(conn,candidate,source['actor_id'],'publish:'+candidate['id'],retained=True)
        receipt=self._select(conn,candidate,source['expected_head_version']);self._backup(conn,source['work_id'])
        return receipt

    def _finalize_cover_in(self,conn,lease,plan,receipt):
        current=self._cover_plan_in(conn,lease,head_version=receipt['version'])
        if current!=plan or self._receipt(conn,plan['work_id'])!=receipt or not conn.execute(sql('SELECT 1 FROM metadata_eligible_candidates_v1 WHERE id=:id'),dict(id=plan['candidate_id'])).first():raise MetadataError('Final cover publication changed')

    def review_candidate(self,token,csrf,*,candidate_id,expected_control_version,expected_head_version,request_id,review_status,select):
        if type(review_status) is not str or review_status not in {'pending','approved','rejected'} or type(select) is not bool:raise MetadataError('Explicit candidate review required')
        for value in (candidate_id,request_id):identifier(value,200)
        _positive(expected_control_version);_positive(expected_head_version,zero=True)
        with write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True)
            row=conn.execute(sql('SELECT * FROM metadata_candidates WHERE id=:id'),dict(id=candidate_id)).mappings().first()
            if row is None:raise AuthError('RESOURCE_NOT_FOUND',404)
            if row['origin'] in {'QWEN_TEXT','QWEN_OCR'} and (review_status=='approved' or select):
                raise MetadataError('Generated approval requires the typed human-review route')
            self._head(conn,row['work_id'],expected_head_version)
            count=conn.execute(sql("UPDATE metadata_candidate_control SET version=version+1,review_status=:status,actor_kind='admin',actor_id=:actor,source_snapshot_sha256=NULL,request_id=:request,updated_at_ms=:now,audit_context_json='{}' WHERE candidate_id=:id AND version=:version"),dict(status=review_status,actor=actor['id'],request=request_id,now=self._now(),id=candidate_id,version=expected_control_version)).rowcount
            if count!=1:raise MetadataError('Candidate control conflict')
            if select:self._select(conn,dict(row),self._head(conn,row['work_id'])['version'],force=row['origin']=='EDITORIAL')
            self._backup(conn,row['work_id']);return self._receipt(conn,row['work_id'])

    def release_cover(self,token,csrf,*,candidate_id,expected_control_version,expected_head_version,request_id,reason):
        identifier(request_id,200)
        if not isinstance(reason,str) or not 1<=len(reason)<=200:raise MetadataError('Bounded release reason required')
        with self.engine.connect() as conn:row=dict(conn.execute(sql('SELECT * FROM metadata_candidates WHERE id=:id'),dict(id=candidate_id)).mappings().one())
        if row['kind']!='cover':raise MetadataError('Cover candidate required')
        with self.publisher.locks.mutation([row['object_sha']]),write_transaction(self.engine) as conn:
            actor=self.auth.current(conn,token,csrf=csrf,admin=True);head=self._head(conn,row['work_id'],expected_head_version)
            if row['kind']!='cover' or head['cover_id']==candidate_id or self._cover_busy(conn,row):raise MetadataError('Only a nonselected, stopped cover can release retention')
            count=conn.execute(sql("UPDATE metadata_candidate_control SET retained=0,version=version+1,actor_kind='admin',actor_id=:actor,source_snapshot_sha256=NULL,request_id=:request,updated_at_ms=:now,audit_context_json=:context WHERE candidate_id=:id AND version=:version"),dict(actor=actor['id'],request=request_id,now=self._now(),context=_json({'reason':reason},4096),id=candidate_id,version=expected_control_version)).rowcount
            if count!=1:raise MetadataError('Cover release control conflict')
            self._backup(conn,row['work_id']);return dict(candidate_id=candidate_id,version=expected_control_version+1,retained=False)

    @staticmethod
    def _cover_busy(conn,row):
        return bool(conn.execute(sql('SELECT '+_cover_busy_predicate(':id',':generation')),dict(id=row['producing_job_id'],generation=row['generation'])).scalar_one())

    def reconcile_retention(self,*,limit=100):
        if type(limit) is not int or not 1<=limit<=100:raise MetadataError('Bounded retention reconciliation required')
        result=dict(local_payload_removed=0,local_object_pending=0,backup_obligation_pending=0,conflicts=0,released=0)
        with write_transaction(self.engine) as conn:
            due=conn.execute(sql("SELECT c.id,r.version,r.retention_policy_json,CASE WHEN r.cache_until_ms IS NOT NULL AND r.cache_until_ms<=:now THEN 'cache_expired' ELSE 'cache_revoked' END AS reason FROM metadata_candidates c JOIN metadata_rights r ON r.id=c.rights_id WHERE (r.state<>'allowed' OR r.cache_allowed=0 OR (r.cache_until_ms IS NOT NULL AND r.cache_until_ms<=:now)) AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id) ORDER BY c.id LIMIT :limit"),dict(now=self._now(),limit=limit)).mappings().all()
            for row in due:
                result['local_payload_removed']+=conn.execute(sql('SELECT count(*) FROM metadata_payloads WHERE candidate_id=:id'),dict(id=row['id'])).scalar_one()
                conn.execute(sql("INSERT INTO metadata_retention_tombstones VALUES(:id,:version,:reason,:now,(SELECT value FROM storage_sequence WHERE id=1),:obligation)"),dict(id=row['id'],version=row['version'],reason=row['reason'],now=self._now(),obligation=row['retention_policy_json']))
            busy=_cover_busy_predicate('c.producing_job_id','c.generation')
            # An uncertain-use prefix cannot hide independently releasable rows.
            rows=[dict(row) for row in conn.execute(sql("SELECT c.*,k.version AS control_version,h.version AS head_version FROM metadata_candidates c JOIN metadata_retention_tombstones t ON t.candidate_id=c.id JOIN metadata_candidate_control k ON k.candidate_id=c.id JOIN metadata_heads h ON h.work_id=c.work_id WHERE c.kind='cover' AND k.retained=1 ORDER BY "+busy+",c.id LIMIT :limit"),dict(limit=limit)).mappings()]
        for row in rows:
            with self.publisher.locks.mutation([row['object_sha']]),write_transaction(self.engine) as conn:
                head=self._head(conn,row['work_id']);control=conn.execute(sql('SELECT * FROM metadata_candidate_control WHERE candidate_id=:id'),dict(id=row['id'])).mappings().one()
                if head['version']!=row['head_version'] or control['version']!=row['control_version']:
                    result['conflicts']+=1;continue
                if not conn.execute(sql('SELECT 1 FROM metadata_retention_tombstones WHERE candidate_id=:id'),dict(id=row['id'])).first():raise MetadataError('Retention obligation disappeared')
                before=dict(head)
                if head['cover_id']==row['id']:
                    count=conn.execute(sql('UPDATE metadata_heads SET cover_id=NULL,version=version+1,updated_at_ms=:now WHERE work_id=:work AND cover_id=:id AND version=:version'),dict(now=self._now(),work=row['work_id'],id=row['id'],version=head['version'])).rowcount
                    if count!=1:raise MetadataError('Retention head conflict')
                busy=self._cover_busy(conn,row);after=self._head(conn,row['work_id']);request=str(uuid.uuid5(uuid.NAMESPACE_URL,f"retention:{row['id']}:{control['version']}:{head['version']}"))
                count=conn.execute(sql("UPDATE metadata_candidate_control SET retained=:retained,version=version+1,actor_kind='system_retention',actor_id=NULL,source_snapshot_sha256=NULL,request_id=:request,updated_at_ms=:now,audit_context_json=:context WHERE candidate_id=:id AND version=:version"),dict(retained=int(busy),request=request,now=self._now(),context=_json({'head_before':before,'head_after':after,'physical_release_pending':busy},4096),id=row['id'],version=control['version'])).rowcount
                if count!=1:raise MetadataError('Retention control conflict')
                result['local_object_pending']+=int(busy);result['released']+=int(not busy);self._backup(conn,row['work_id'])
        with self.engine.connect() as conn:
            result['backup_obligation_pending']=conn.execute(sql('SELECT count(*) FROM metadata_retention_tombstones')).scalar_one()
            result['local_object_pending']=conn.execute(sql("SELECT count(*) FROM metadata_candidates c JOIN metadata_retention_tombstones t ON t.candidate_id=c.id JOIN metadata_candidate_control k ON k.candidate_id=c.id WHERE c.kind='cover' AND k.retained=1 AND "+_cover_busy_predicate('c.producing_job_id','c.generation'))).scalar_one()
        return result

    @staticmethod
    def _public_rights(row, now):
        value=dict(row)
        value['rights_id']=value.pop('id')
        for name in ('display_allowed','cache_allowed','derivative_allowed'):
            value[name]=bool(value[name])
        value['retention_policy']=json.loads(value.pop('retention_policy_json'))
        value['evidence']=json.loads(value.pop('evidence_json'))
        policy=value['retention_policy']
        value['editorial_eligible']=(value['kind']=='description' and value['state']=='allowed'
            and value['display_allowed'] and value['cache_allowed']
            and (value['cache_until_ms'] is None or value['cache_until_ms']>now)
            and policy.get('backup_allowed') is True and not policy.get('all_copies_deletion_required'))
        return value

    def review_queue(self,token,*,limit=50):
        if type(limit) is not int or not 1<=limit<=100:
            raise MetadataError('Bounded metadata review queue required')
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            now=self._now()
            candidates=[dict(row) for row in conn.execute(sql("""
                SELECT c.id AS candidate_id,c.work_id,c.kind,c.origin,
                       k.version AS control_version,k.retained AS control_retained,
                       h.version AS head_version,w.title_display AS title
                FROM metadata_candidates c
                JOIN metadata_candidate_control k ON k.candidate_id=c.id
                JOIN metadata_heads h ON h.work_id=c.work_id
                JOIN works w ON w.id=c.work_id
                WHERE k.review_status='pending'
                ORDER BY c.id LIMIT :limit"""),dict(limit=limit)).mappings()]
            for item in candidates:
                preview=conn.execute(sql('SELECT text_value FROM metadata_payloads WHERE candidate_id=:id LIMIT 1'),dict(id=item['candidate_id'])).scalar() if item['kind']=='description' else None
                item['text_preview']=preview[:200] if preview else None
            rights=[self._public_rights(row,now) for row in conn.execute(sql("""
                SELECT r.id,r.work_id,r.kind,r.version,r.state,r.officiality,r.match_state,
                       r.display_allowed,r.cache_allowed,r.derivative_allowed,r.cache_until_ms,
                       r.retention_policy_json,r.evidence_json,
                       w.title_display AS title
                FROM metadata_rights r
                JOIN works w ON w.id=r.work_id
                WHERE r.state='review'
                ORDER BY r.id LIMIT :limit"""),dict(limit=limit)).mappings()]
        return dict(candidates=candidates,rights=rights,now_ms=now)

    def work_metadata_state(self, token, *, work_id):
        """Return a work's current head, rights and pending candidates for admin editing.

        Gives the G4 admin UI the optimistic-lock versions (head + rights) needed to author a
        new EDITORIAL description through :meth:`edit_description`.
        """
        with self.engine.connect() as conn:
            self.auth.current(conn, token, admin=True)
            now=self._now()
            head = conn.execute(sql('SELECT version,description_id,cover_id FROM metadata_heads WHERE work_id=:id'), dict(id=work_id)).mappings().first()
            if head is None:
                raise MetadataError('Metadata head conflict')
            rights = conn.execute(sql('SELECT id,work_id,kind,state,version,officiality,match_state,display_allowed,cache_allowed,derivative_allowed,cache_until_ms,retention_policy_json,evidence_json FROM metadata_rights WHERE work_id=:id ORDER BY kind,id'), dict(id=work_id)).mappings().all()
            pending = conn.execute(sql("SELECT c.id,c.kind,c.origin,k.version AS control_version FROM metadata_candidates c "
                "JOIN metadata_candidate_control k ON k.candidate_id=c.id WHERE c.work_id=:id AND k.review_status='pending' ORDER BY c.kind"), dict(id=work_id)).mappings().all()
            return dict(work_id=work_id,now_ms=now,
                head=dict(version=head['version'], description_id=head['description_id'], cover_id=head['cover_id']),
                rights=[self._public_rights(row,now) for row in rights],
                pending=[dict(candidate_id=row['id'], kind=row['kind'], origin=row['origin'], control_version=row['control_version']) for row in pending])
