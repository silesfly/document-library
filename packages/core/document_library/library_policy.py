"""DB-only authoritative read selection; no HTTP, file or streaming authority.

Call in the storage reader's opener and again after hashes/before response
headers, then compare immutable snapshots. A check outside that scope alone
does not close an active-revision or rights race. Existing publication's
retained-revision primitive is deliberately not used as public policy.
"""
import json
import re
import time

from sqlalchemy import text

from .auth import AuthError, digest
from .library_contracts import (AssetSelection, AssetSnapshot, OptionSnapshot,
    PreviewProof, ReadPrincipal, ResolvedSelection, SourceProof, TicketAsset,
    TicketOption, TicketSnapshot, READ_POLICIES, canonical_json)
from .publication import object_path
from .storage import _parts, StorageError


class LibraryPolicyError(AuthError):
    pass


class LibraryReadPolicy:
    def __init__(self, auth, *, now_ms=None):
        self.auth,self.engine=auth,auth.engine
        if now_ms is not None and not callable(now_ms):
            raise TypeError('Trusted millisecond clock callable required')
        self.now_ms=now_ms if now_ms is not None else lambda:time.time_ns()//1_000_000

    def _principal_in(self, conn, token):
        if conn.engine is not self.engine:
            raise ValueError('One library authorization engine required')
        actor=self.auth.current(conn,token)
        return ReadPrincipal(actor['id'],digest(token),actor['credential_version'],actor['role'])

    @staticmethod
    def _ticket_rows(conn, ticket_id, *, attempt_id=None):
        row=conn.execute(text('SELECT * FROM download_tickets WHERE id=:id'),dict(id=ticket_id)).mappings().first()
        if not row:
            raise LibraryPolicyError('TICKET_UNAVAILABLE',404)
        options=tuple(TicketOption(r['selection_order'],r['edition_id'],r['revision_id'],r['option_id'],
                      r['format'],r['delivery_kind'],canonical_json(json.loads(r['coverage_json'])))
                      for r in conn.execute(text('SELECT * FROM download_ticket_options WHERE ticket_id=:id ORDER BY selection_order'),dict(id=ticket_id)).mappings())
        assets=tuple(TicketAsset(r['selection_order'],r['ordinal'],r['revision_id'],r['asset_position'],r['object_sha'],
                     r['byte_size'],r['detected_mime'],r['file_format'],r['asset_role'],r['entry_name'])
                     for r in conn.execute(text('SELECT * FROM download_ticket_assets WHERE ticket_id=:id ORDER BY selection_order,ordinal'),dict(id=ticket_id)).mappings())
        snapshot=TicketSnapshot(row['id'],row['user_id'],row['session_token_hash'],row['credential_version'],row['work_id'],
                   row['issued_at_ms'],row['expires_at_ms'],row['archive_bytes'],row['snapshot_digest'],options,assets,row['state'],attempt_id)
        return row,snapshot

    def ticket_digest_in(self, conn, ticket_id):
        """Trusted draft writer computes this before atomic issue/pin commit."""
        return self._ticket_rows(conn,ticket_id)[1].digest()

    def load_ticket_snapshot_in(self, conn, token, ticket_id, *, attempt_id=None):
        principal=self._principal_in(conn,token)
        row,snapshot=self._ticket_rows(conn,ticket_id,attempt_id=attempt_id)
        if (row['invalidated'] or snapshot.user_id!=principal.user_id
                or snapshot.session_hash!=principal.session_hash
                or snapshot.credential_version!=principal.credential_version):
            raise LibraryPolicyError('TICKET_ACCESS_DENIED',403)
        if not snapshot.options or not snapshot.assets or snapshot.digest()!=snapshot.snapshot_digest:
            raise LibraryPolicyError('TICKET_SNAPSHOT_CONFLICT',409)
        if snapshot.state=='issued':
            now=self.now_ms()
            if type(now) is not int or now<0:
                raise LibraryPolicyError('CLOCK_UNAVAILABLE',503)
            if attempt_id is not None or not snapshot.issued_at_ms<=now<snapshot.expires_at_ms:
                raise LibraryPolicyError('TICKET_EXPIRED_OR_USED',409)
        elif snapshot.state=='streaming':
            # Expiration is an admission deadline, never stream-stop evidence.
            attempt=conn.execute(text('SELECT * FROM download_attempts WHERE id=:id AND ticket_id=:ticket'),
                                 dict(id=attempt_id,ticket=snapshot.ticket_id)).mappings().first()
            if not attempt or attempt['invalidated'] or attempt['state']!='running':
                raise LibraryPolicyError('TICKET_EXPIRED_OR_USED',409)
        else:
            raise LibraryPolicyError('TICKET_EXPIRED_OR_USED',409)
        missing=conn.execute(text("""SELECT 1 FROM download_ticket_assets a
            LEFT JOIN object_pins p ON p.kind='ticket' AND p.reference_id=a.ticket_id AND p.object_sha=a.object_sha
            WHERE a.ticket_id=:id AND p.object_sha IS NULL LIMIT 1"""),dict(id=ticket_id)).first()
        if missing:
            raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
        return snapshot

    @staticmethod
    def selections_for_ticket(snapshot):
        return tuple(AssetSelection(option.revision_id,option_id=option.option_id,
                                    work_id=snapshot.work_id,edition_id=option.edition_id)
                     for option in snapshot.options)

    @staticmethod
    def _preview(row, proof, asset):
        if row['run_sha']!=asset['object_sha'] or row['run_size']!=asset['byte_size']:
            return None
        try:
            plan=json.loads(row['payload_json'])
            fresh=[item for item in plan['assets'] if item['prepared_relpath'] is not None]
            if (len(fresh)!=1 or row['input_edition']!=plan['edition_id']
                    or row['input_previous']!=plan['expected_revision_id'] or row['input_size']!=row['run_size']):
                return None
            item=fresh[0]
            if any(item[key]!=asset[target] for key,target in (
                    ('sha256','object_sha'),('byte_size','byte_size'),('file_format','file_format'),('asset_role','asset_role'))):
                return None
            prepared=_parts(item['prepared_relpath'])
            expected=['quarantine','prepared','jobs',proof.job_id,f'generation-{proof.generation}']
            if prepared[:5]!=expected or len(prepared)<7 or not re.fullmatch('[0-9a-f]{16}',prepared[5]):
                return None
            manifest=json.loads(row['preview_json'])
            if not isinstance(manifest,dict):
                return None
            if (manifest.get('source_sha256')!=asset['object_sha'] or manifest.get('source_byte_size')!=asset['byte_size']
                    or manifest.get('kind') not in {'text','images'}):
                return None
            path=_parts(manifest['bundle_relpath'])
            if (path[:4]!=prepared[2:6] or len(path)<5 or type(manifest.get('bundle_size')) is not int
                    or not 0<manifest['bundle_size']<=128*1024**2
                    or not isinstance(manifest.get('bundle_sha256'),str)
                    or not re.fullmatch('[0-9a-f]{64}',manifest['bundle_sha256'])):
                return None
            return PreviewProof(proof,asset['object_sha'],asset['byte_size'],canonical_json(manifest))
        except (KeyError,TypeError,ValueError,StorageError):
            return None

    def _sources(self, conn, revision_id, asset):
        rows=conn.execute(text('''SELECT s.source_job_id,s.source_generation,p.payload_digest,p.payload_json,p.rights_status,
            run.sha256 AS run_sha,run.byte_size AS run_size,run.preview_json,
            i.edition_id AS input_edition,i.expected_revision_id AS input_previous,i.byte_size AS input_size
            FROM revision_asset_sources s JOIN publication_plans p
                ON p.job_id=s.source_job_id AND p.generation=s.source_generation
            LEFT JOIN ingestion_runs run ON run.job_id=s.source_job_id AND run.generation=s.source_generation
            LEFT JOIN ingestion_inputs i ON i.job_id=s.source_job_id
            WHERE s.edition_revision_id=:revision AND s.asset_position=:position
            ORDER BY s.source_job_id,s.source_generation'''),dict(revision=revision_id,position=asset['asset_position'])).mappings().all()
        if not rows or any(row['rights_status']!='allowed' or digest(row['payload_json'])!=row['payload_digest'] for row in rows):
            raise LibraryPolicyError('ASSET_ACCESS_DENIED',403)
        proofs=tuple(SourceProof(row['source_job_id'],row['source_generation'],row['payload_digest']) for row in rows)
        previews=tuple(preview for row,proof in zip(rows,proofs) if (preview:=self._preview(row,proof,asset)) is not None)
        return proofs,previews

    def resolve_selection_in(self, conn, token, selection, *, policy='current', ticket_snapshot=None):
        """Return an immutable DB snapshot; caller owns short transaction scope."""
        principal=self._principal_in(conn,token)
        if policy not in READ_POLICIES or not isinstance(selection,(tuple,list)) or not 1<=len(selection)<=128:
            raise LibraryPolicyError('INVALID_SELECTION',400)
        if any(not isinstance(request,AssetSelection) for request in selection):
            raise LibraryPolicyError('INVALID_SELECTION',400)
        if policy=='admin_retained' and principal.role!='admin':
            raise LibraryPolicyError('PERMISSION_DENIED',403)
        ticket=None
        if policy=='ticket':
            if not isinstance(ticket_snapshot,TicketSnapshot):
                raise LibraryPolicyError('TICKET_AUTHORITY_REQUIRED',403)
            ticket=self.load_ticket_snapshot_in(conn,token,ticket_snapshot.ticket_id,attempt_id=ticket_snapshot.attempt_id)
            if ticket.payload()!=ticket_snapshot.payload():
                raise LibraryPolicyError('TICKET_SNAPSHOT_CONFLICT',409)
        elif ticket_snapshot is not None:
            raise LibraryPolicyError('INVALID_SELECTION',400)
        assets,options=[],[]
        for index,request in enumerate(selection):
            revision=conn.execute(text('''SELECT r.edition_id,r.is_active,e.work_id,s.retained,p.rights_status,p.payload_json,p.payload_digest
                FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id
                JOIN revision_seals s ON s.revision_id=r.id
                JOIN publication_plans p ON p.job_id=s.source_job_id AND p.generation=s.source_generation
                WHERE r.id=:id'''),dict(id=request.revision_id)).mappings().first()
            if (not revision or (policy=='current' and not revision['is_active'])
                    or (policy=='admin_retained' and not (revision['is_active'] or revision['retained']))
                    or (request.work_id is not None and request.work_id!=revision['work_id'])
                    or (request.edition_id is not None and request.edition_id!=revision['edition_id'])):
                raise LibraryPolicyError('RESOURCE_NOT_FOUND',404)
            if revision['rights_status']!='allowed' or digest(revision['payload_json'])!=revision['payload_digest']:
                raise LibraryPolicyError('ASSET_ACCESS_DENIED',403)
            positions=request.positions
            ordinals={}
            option=None
            if request.option_id is not None:
                option=conn.execute(text('SELECT * FROM revision_options WHERE id=:id AND edition_revision_id=:revision'),
                                    dict(id=request.option_id,revision=request.revision_id)).mappings().first()
                members=conn.execute(text('SELECT asset_position,ordinal FROM option_assets WHERE option_id=:id ORDER BY ordinal'),dict(id=request.option_id)).all()
                if not option or not members:
                    raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
                canonical=tuple(member.asset_position for member in members)
                if positions is None:
                    positions=canonical
                elif positions!=tuple(position for position in canonical if position in set(positions)):
                    raise LibraryPolicyError('INVALID_SELECTION',400)
                ordinals={member.asset_position:member.ordinal for member in members}
                options.append(OptionSnapshot(index,revision['work_id'],revision['edition_id'],request.revision_id,option['id'],
                    option['format'],option['delivery_kind'],canonical_json(json.loads(option['coverage_json'])),option['coverage_confidence'],positions))
            rows=conn.execute(text('''SELECT a.asset_position,a.object_sha,a.file_format,a.asset_role,o.byte_size,o.detected_mime,o.local_relpath,o.state
                FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
                WHERE a.edition_revision_id=:revision AND a.asset_position IN (SELECT value FROM json_each(:positions))'''),
                dict(revision=request.revision_id,positions=json.dumps(positions))).mappings().all()
            by_position={row['asset_position']:row for row in rows}
            if len(rows)!=len(positions):
                raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
            for ordinal,position in enumerate(positions):
                row=by_position[position]
                if option is not None and row['file_format']!=option['format']:
                    raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
                if row['state']!='LIVE' or row['local_relpath']!=object_path(row['object_sha']):
                    raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
                if not conn.execute(text('SELECT 1 FROM option_assets oa JOIN revision_options ro ON ro.id=oa.option_id WHERE oa.edition_revision_id=:revision AND oa.asset_position=:position AND ro.format=:format'),
                                    dict(revision=request.revision_id,position=position,format=row['file_format'])).first():
                    raise LibraryPolicyError('ASSET_UNAVAILABLE',409)
                sources,previews=self._sources(conn,request.revision_id,row)
                assets.append(AssetSnapshot(index,ordinals.get(position,ordinal),revision['work_id'],revision['edition_id'],request.revision_id,
                    request.option_id,position,row['object_sha'],row['byte_size'],row['detected_mime'],row['file_format'],row['asset_role'],sources,previews))
        if ticket is not None:
            actual_options=tuple((o.selection_index,o.edition_id,o.revision_id,o.option_id,o.format,o.delivery_kind,o.coverage_json) for o in options)
            expected_options=tuple((o.selection_order,o.edition_id,o.revision_id,o.option_id,o.format,o.delivery_kind,o.coverage_json) for o in ticket.options)
            actual_assets=tuple((a.selection_index,a.ordinal,a.revision_id,a.asset_position,a.object_sha,a.byte_size,a.detected_mime,a.file_format,a.asset_role) for a in assets)
            expected_assets=tuple((a.selection_order,a.ordinal,a.revision_id,a.asset_position,a.object_sha,a.byte_size,a.detected_mime,a.file_format,a.asset_role) for a in ticket.assets)
            if actual_options!=expected_options or actual_assets!=expected_assets or any(a.work_id!=ticket.work_id for a in assets):
                raise LibraryPolicyError('TICKET_SNAPSHOT_CONFLICT',409)
        return ResolvedSelection(principal,tuple(assets),tuple(options))
