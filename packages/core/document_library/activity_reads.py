"""Owner/admin activity reads. No transport, publication or audit mutations."""
from datetime import datetime, timezone
import unicodedata

from sqlalchemy import text

from .auth import AuthError
from .cursors import CursorError, decode_cursor, encode_cursor
from .library_contracts import _ident
from .library_projection import VALID_OPTIONS_V1
from .permissions import PermissionService
from .uploads import upload_uuid, display_filename, ACTIVE_OPS

OUTCOMES = ('initial','add_format','exact_duplicate','replace_superset','append_bundle')
PERMISSION_ACTIONS = ('permission_changed','permission_batch')
INGESTION_JOB = "j.kind IN ('ingest_upload','ingest.review_upload','ingest.validation_review')"
STAGE = """CASE WHEN i.job_id IS NULL THEN 'unqueued'
 WHEN j.id IS NULL OR NOT ("""+INGESTION_JOB+""") THEN 'unavailable'
 WHEN j.state='completed' THEN 'completed'
 WHEN i.rights_status='blocked' THEN 'blocked'
 WHEN i.rights_status='review' OR j.kind='ingest.review_upload' THEN 'review_pending'
 WHEN j.kind='ingest.validation_review' THEN 'validation_review' ELSE j.state END"""
JOINS = """ FROM upload_sessions s
 LEFT JOIN ingestion_inputs i ON i.upload_id=s.id
 LEFT JOIN jobs j ON j.id=i.job_id AND j.upload_session_id=s.id """
DECISION = """CASE WHEN """+INGESTION_JOB+""" AND j.state='completed' AND json_extract(j.result_json,'$.reconciliation.outcome')
 IN ('initial','add_format','exact_duplicate','replace_superset','append_bundle')
 THEN json_extract(j.result_json,'$.reconciliation.outcome') END"""


def _input():
    return AuthError('INVALID_INPUT',422)


def _id(value):
    try:
        return _ident(value)
    except ValueError:
        raise _input() from None


def _safe_text(value, limit):
    if (not isinstance(value,str) or not 1 <= len(value) <= limit
            or any(unicodedata.category(c).startswith('C') for c in value)):
        return None
    return value


def _stamp(value, *, milliseconds=False):
    try:
        if type(value) is int:
            date = datetime.fromtimestamp(value/1000 if milliseconds else value,timezone.utc)
        elif isinstance(value,str) and len(value)<=64:
            date = datetime.fromisoformat(value.replace('Z','+00:00'))
            if date.tzinfo is None:
                return None
        else:
            return None
        return date.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
    except (ValueError,OverflowError,OSError):
        return None


def _page(limit, cursor):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise _input()
    if cursor is not None and (not isinstance(cursor,str) or not 1 <= len(cursor) <= 4096):
        raise CursorError()


class ActivityReads:
    def __init__(self, auth, *, permissions=None):
        self.auth,self.engine = auth,auth.engine
        self.permissions = permissions or PermissionService(auth)
        if self.permissions.auth is not auth or self.permissions.engine is not self.engine:
            raise ValueError('One activity read authority required')

    @staticmethod
    def _counts(conn, where, params, *, expected_owner=None):
        owner_check = '0' if expected_owner is None else 'sum(CASE WHEN s.owner_id=:expected_owner THEN 0 ELSE 1 END)'
        params = dict(params,expected_owner=expected_owner)
        rows = conn.execute(text('SELECT '+owner_check+' AS owner_mismatch,s.status AS transport_status,'+STAGE+' AS ingestion_state,'+
            DECISION+' AS decision,count(*) AS count,coalesce(sum(s.declared_size),0) AS declared,'+
            'coalesce(sum(s.received_size),0) AS received'+JOINS+' WHERE '+where+
            ' GROUP BY s.status,ingestion_state,decision'),params).mappings()
        result = dict(file_count=0,reserved_bytes=0,received_bytes=0,transport_counts={},ingestion_counts={},decision_counts={})
        for row in rows:
            if row['owner_mismatch']:
                raise AuthError('ASSET_UNAVAILABLE',409)
            result['file_count'] += row['count']
            result['reserved_bytes'] += row['declared']
            result['received_bytes'] += row['received']
            for key,column in (('transport_counts','transport_status'),('ingestion_counts','ingestion_state'),('decision_counts','decision')):
                value = row[column]
                if value is not None:
                    result[key][value] = result[key].get(value,0)+row['count']
        return result

    def batch(self, token, batch_id, *, cursor=None, limit=20):
        batch_id = upload_uuid(batch_id)
        _page(limit,cursor)
        # database_engine's autocommit=False holds one read snapshot through
        # authority, all-file aggregates and the bounded returned file page.
        with self.engine.connect() as conn:
            actor = self.auth.current(conn,token)
            batch = conn.execute(text('SELECT * FROM upload_batches WHERE id=:batch'),dict(batch=batch_id)).mappings().first()
            if batch is None or (batch['uploader_id'] != actor['id'] and actor['role'] != 'admin'):
                raise AuthError('RESOURCE_NOT_FOUND',404)
            params = dict(batch=batch_id,owner=batch['uploader_id'],limit=limit+1)
            seek = ''
            if cursor is not None:
                data = decode_cursor(cursor,expected_kind='upload.files')
                if set(data) != {'batch_id','last_id'} or data['batch_id'] != batch_id:
                    raise CursorError()
                try:
                    last = upload_uuid(data['last_id'])
                except AuthError:
                    raise CursorError() from None
                if not conn.execute(text('SELECT 1 FROM upload_sessions WHERE batch_id=:batch AND id=:last'),dict(batch=batch_id,last=last)).first():
                    raise CursorError()
                params['last'] = last
                seek = ' AND s.id>:last'
            counts = self._counts(conn,'s.batch_id=:batch',params,expected_owner=batch['uploader_id'])
            rows = conn.execute(text('WITH '+VALID_OPTIONS_V1+''', page AS (
                SELECT s.* FROM upload_sessions s WHERE s.batch_id=:batch AND s.owner_id=:owner'''+seek+''' ORDER BY s.id LIMIT :limit)
                SELECT s.id,s.status,s.declared_size,s.received_size,s.expires_at,
                coalesce(res.display_filename,i.filename) AS display_filename,
                i.rights_status,j.state AS job_state,j.updated_at AS processing_updated_at,
                '''+STAGE+' AS ingestion_state,'+DECISION+''' AS decision,
                (SELECT op.state FROM upload_transport_ops op WHERE op.upload_id=s.id AND op.state IN '''+ACTIVE_OPS+''' ORDER BY op.created_at DESC,op.id DESC LIMIT 1) AS transport_operation,
                e.work_id,e.id AS edition_id,r.id AS revision_id,seal.revision_id AS sealed_revision_id,
                EXISTS(SELECT 1 FROM valid_options v JOIN option_assets oa ON oa.option_id=v.option_id
                    AND oa.edition_revision_id=v.revision_id
                    JOIN revision_assets a ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
                    WHERE v.work_id=e.work_id AND v.edition_id=e.id AND v.revision_id=r.id
                    AND a.object_sha=run.sha256) AS readable
                FROM page s LEFT JOIN upload_reservations res ON res.upload_id=s.id
                LEFT JOIN ingestion_inputs i ON i.upload_id=s.id
                LEFT JOIN jobs j ON j.id=i.job_id AND j.upload_session_id=s.id
                LEFT JOIN ingestion_runs run ON run.job_id=j.id AND run.generation=j.lease_generation
                LEFT JOIN edition_revisions r ON '''+INGESTION_JOB+''' AND j.state='completed' AND json_type(j.result_json,'$.revision_id')='text' AND r.id=json_extract(j.result_json,'$.revision_id') AND r.edition_id=i.edition_id
                LEFT JOIN work_editions e ON e.id=r.edition_id AND e.work_id=j.work_id
                LEFT JOIN revision_seals seal ON seal.revision_id=r.id
                ORDER BY s.id'''),params).mappings().all()
            more = len(rows)>limit
            rows = rows[:limit]
            files = []
            for row in rows:
                try:
                    name = display_filename(row['display_filename'])
                except AuthError:
                    name = None
                published = None
                if row['readable'] and row['work_id'] is not None:
                    published = {key:_id(row[key]) for key in ('work_id','edition_id','revision_id')}
                files.append(dict(upload_id=row['id'],display_filename=name,filename_state='ready' if name else 'unavailable',
                    declared_size=row['declared_size'],received_size=row['received_size'],transport_status=row['status'],
                    transport_operation=row['transport_operation'],reservation_expires_at=_stamp(row['expires_at']),
                    ingestion_state=row['ingestion_state'],job_state=row['job_state'],rights_state=row['rights_status'],
                    processing_updated_at=_stamp(row['processing_updated_at'],milliseconds=True),decision=row['decision'],
                    publication_state='published' if row['status']=='published' or (row['sealed_revision_id'] is not None and row['work_id'] is not None) else 'not_published',
                    published=published))
            next_cursor = encode_cursor('upload.files',dict(batch_id=batch_id,last_id=rows[-1]['id'])) if more else None
            return dict(batch_id=batch_id,status=batch['status'],declared_total_bytes=batch['declared_total_bytes'],
                created_at=_stamp(batch['created_at']),**counts,files=files,next_cursor=next_cursor)

    def user(self, token, user_id):
        user_id = _id(user_id)
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            user = conn.execute(text('SELECT id,status,credential_version FROM users WHERE id=:user'),dict(user=user_id)).mappings().first()
            if user is None:
                raise AuthError('RESOURCE_NOT_FOUND',404)
            counts = self._counts(conn,'s.owner_id=:user',dict(user=user_id))
            # Count the existing permission-preview predicate without building
            # its full list of affected IDs. The preview API keeps its contract.
            active = conn.execute(text("""SELECT count(*) FROM upload_sessions WHERE owner_id=:user
                AND status IN ('receiving','staged') AND expires_at>:now"""),
                dict(user=user_id,now=self.auth.now())).scalar_one()
            sessions = conn.execute(text('''SELECT count(*) FROM auth_sessions s JOIN users u ON u.id=s.user_id
                WHERE u.id=:user AND u.status='active' AND s.credential_version=u.credential_version
                AND s.expires_at>:now'''),dict(user=user_id,now=self.auth.now())).scalar_one()
            transport = conn.execute(text('''SELECT count(*) FROM upload_transport_ops op
                JOIN upload_sessions s ON s.id=op.upload_id WHERE s.owner_id=:user AND op.state IN '''+ACTIVE_OPS),dict(user=user_id)).scalar_one()
            return dict(user_id=user_id,**counts,active_upload_count=active,
                active_upload_count_definition='permission_preview_affected_sessions',
                inflight_transport_count=transport,current_session_count=sessions)

    def permission_audit(self, token, *, cursor=None, limit=50):
        _page(limit,cursor)
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            params = dict(limit=limit+1)
            where = "action IN ('permission_changed','permission_batch')"
            if cursor is not None:
                data = decode_cursor(cursor,expected_kind='permission.audit')
                if set(data) != {'last_id'}:
                    raise CursorError()
                last = conn.execute(text('SELECT id,created_at FROM audit_events WHERE id=:id AND '+where),dict(id=_id(data['last_id']))).mappings().first()
                if last is None:
                    raise CursorError()
                params.update(last=last['id'],time=last['created_at'])
                where += ' AND (created_at<:time OR (created_at=:time AND id<:last))'
            rows = conn.execute(text('''SELECT id,actor_id,target_id,action,created_at,
                CASE WHEN json_type(details_json,'$.reason')='text' THEN json_extract(details_json,'$.reason') END AS reason,
                json_extract(details_json,'$.old.can_upload') AS old_capability,
                json_extract(details_json,'$.new.can_upload') AS new_capability
                FROM audit_events WHERE '''+where+' ORDER BY created_at DESC,id DESC LIMIT :limit'),params).mappings().all()
            more = len(rows)>limit
            rows = rows[:limit]
            items = []
            for row in rows:
                def capability(value):
                    return bool(value) if type(value) is int and value in (0,1) else None
                items.append(dict(event_id=_id(row['id']),action=row['action'],
                    actor_id=_id(row['actor_id']),target_id=_id(row['target_id']) if row['target_id'] is not None else None,
                    created_at=_stamp(row['created_at']),reason=_safe_text(row['reason'],500),
                    old_capability=capability(row['old_capability']),new_capability=capability(row['new_capability'])))
            next_cursor = encode_cursor('permission.audit',dict(last_id=rows[-1]['id'])) if more else None
            return dict(items=items,next_cursor=next_cursor,actions=list(PERMISSION_ACTIONS))
