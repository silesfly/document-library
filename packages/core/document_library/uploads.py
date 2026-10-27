"""Current upload authority, atomic quota reservations and transport receipts.

No client filename grants filesystem authority. Transport occupancy has no lease
or timeout: an unobserved backend completion retains its slot across restarts.
"""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re
import stat
import unicodedata
import uuid

from sqlalchemy import text

from .auth import AuthError, digest
from .database import write_transaction
from .ingest_formats import KNOWN
from .permissions import canonical

FILE_LIMIT = 2 * 1024**3
BATCH_LIMIT = 10 * 1024**3
CHUNK_LIMIT = 8 * 1024**2
ACTIVE_OPS = "('reserved','sent','uncertain')"


def upload_uuid(value):
    try:
        if not isinstance(value,str) or str(uuid.UUID(value)) != value:
            raise ValueError()
    except (ValueError,AttributeError):
        raise AuthError('invalid_upload_id',404) from None
    return value


def validate_request_key(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}',value):
        raise AuthError('invalid_request_key',400)
    return value


def size(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise AuthError('invalid_upload_size',400)
    return value


def display_filename(value):
    if not isinstance(value,str) or not 1 <= len(value) <= 255:
        raise AuthError('invalid_filename',400)
    name=unicodedata.normalize('NFKC',value)
    if (not name or len(name)>255 or name!=name.strip() or any(c in name for c in '/\\:')
            or name in {'.','..'} or name.startswith('.') or name.endswith('.')
            or any(unicodedata.category(c).startswith('C') for c in name)):
        raise AuthError('invalid_filename',400)
    if '.' not in name or name.rsplit('.',1)[1].lower() not in KNOWN:
        raise AuthError('unsupported_extension',400)
    return name


class UploadService:
    def __init__(self, auth):
        self.auth,self.engine=auth,auth.engine
        self._database_path=Path(self.engine.url.database)
        if not self._database_path.is_absolute() or self._database_path.resolve()!=self._database_path:
            raise ValueError('Stable absolute database path required')
        info=self._database_path.lstat()
        self._database_identity=(info.st_dev,info.st_ino)

    @contextmanager
    def ingress_guard(self, *, reconcile=False):
        """Advisory ingress ownership, separate from SQLite's transaction locks.

        Acquire before reserving and hold through handler cleanup. Independent
        open descriptions make this guard effective across threads/processes;
        process death releases it. Reconciliation may retire reserved rows only
        while it exclusively owns the same stable database inode. A busy guard
        retains every reserved row until its handler exits or a later explicit
        startup reconciliation finds all ingress quiescent. No lease is used.
        """
        try:
            fd=os.open(self._database_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        except OSError:
            raise AuthError('upload_ingress_unverified',503) from None
        try:
            info=os.fstat(fd)
            current=self._database_path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.getuid()
                    or (info.st_dev,info.st_ino)!=self._database_identity
                    or (current.st_dev,current.st_ino)!=self._database_identity
                    or self._database_path.resolve()!=self._database_path):
                raise AuthError('upload_ingress_unverified',503)
            try:
                fcntl.flock(fd,(fcntl.LOCK_EX if reconcile else fcntl.LOCK_SH)|fcntl.LOCK_NB)
            except BlockingIOError:
                if not reconcile:
                    raise AuthError('upload_ingress_reconciling',503) from None
                yield False
            else:
                yield True
        finally:
            os.close(fd)

    def _actor(self, conn, token, csrf):
        if not isinstance(csrf,str) or not csrf:
            raise AuthError('csrf_rejected',403)
        if not isinstance(token,str) or not token:
            raise AuthError('authentication_required')
        return self.auth.current(conn,token,csrf=csrf,upload=True)

    def create_batch(self, token, csrf, *, declared_total_bytes, request_key):
        size(declared_total_bytes,BATCH_LIMIT)
        key=validate_request_key(request_key)
        fingerprint=digest(canonical({'total':declared_total_bytes}))
        with write_transaction(self.engine) as conn:
            actor=self._actor(conn,token,csrf)
            old=conn.execute(text('''SELECT b.*,r.request_digest,r.authz_version
                FROM upload_batch_requests r JOIN upload_batches b ON b.id=r.batch_id
                WHERE r.owner_id=:owner AND r.request_key=:key'''),dict(owner=actor['id'],key=key)).mappings().first()
            if old:
                if old['request_digest']!=fingerprint:
                    raise AuthError('idempotency_conflict',409)
                if old['status']!='open' or old['authz_version']!=actor['authz_version']:
                    raise AuthError('upload_permission_revoked',403)
                return {'batch_id':old['id'],'declared_total_bytes':old['declared_total_bytes']}
            ident=str(uuid.uuid4())
            conn.execute(text("INSERT INTO upload_batches VALUES(:id,:owner,:total,'open',:now)"),
                         dict(id=ident,owner=actor['id'],total=declared_total_bytes,now=self.auth.now()))
            conn.execute(text('INSERT INTO upload_batch_requests VALUES(:id,:owner,:key,:digest,:version)'),
                         dict(id=ident,owner=actor['id'],key=key,digest=fingerprint,version=actor['authz_version']))
            return {'batch_id':ident,'declared_total_bytes':declared_total_bytes}

    def reserve_file(self, token, csrf, *, batch_id, filename, declared_size, request_key):
        upload_uuid(batch_id);size(declared_size,FILE_LIMIT)
        name=display_filename(filename);key=validate_request_key(request_key)
        fingerprint=digest(canonical(dict(name=name,size=declared_size)))
        with write_transaction(self.engine) as conn:
            actor=self._actor(conn,token,csrf)
            batch=conn.execute(text('''SELECT b.*,r.authz_version FROM upload_batches b
                JOIN upload_batch_requests r ON r.batch_id=b.id WHERE b.id=:id'''),dict(id=batch_id)).mappings().first()
            if (not batch or batch['uploader_id']!=actor['id'] or batch['status']!='open'
                    or batch['authz_version']!=actor['authz_version']):
                raise AuthError('upload_permission_revoked',403)
            old=conn.execute(text('''SELECT r.request_digest,s.* FROM upload_reservations r
                JOIN upload_sessions s ON s.id=r.upload_id WHERE r.batch_id=:batch AND r.request_key=:key'''),
                dict(batch=batch_id,key=key)).mappings().first()
            if old:
                if old['request_digest']!=fingerprint:
                    raise AuthError('idempotency_conflict',409)
                self._upload(conn,token,csrf,old['id'])
                return self._reservation(old)
            # Expiration retires the user's reservation, never a transport slot.
            conn.execute(text("UPDATE upload_sessions SET status='expired' WHERE owner_id=:owner AND status='receiving' AND expires_at<=:now"),
                         dict(owner=actor['id'],now=self.auth.now()))
            if conn.execute(text("SELECT count(*) FROM upload_sessions WHERE owner_id=:owner AND status='receiving'"),
                            dict(owner=actor['id'])).scalar_one()>=2:
                raise AuthError('uploader_limit',429)
            reserved=conn.execute(text("SELECT coalesce(sum(declared_size),0) FROM upload_sessions WHERE batch_id=:batch AND status NOT IN ('expired','revoked')"),dict(batch=batch_id)).scalar_one()
            if reserved+declared_size>batch['declared_total_bytes']:
                raise AuthError('batch_limit',413)
            ident=str(uuid.uuid4())
            conn.execute(text("INSERT INTO upload_sessions VALUES(:id,:batch,:owner,NULL,:version,:size,0,:expiry,'receiving')"),
                dict(id=ident,batch=batch_id,owner=actor['id'],version=actor['authz_version'],size=declared_size,expiry=self.auth.now()+86400))
            conn.execute(text('INSERT INTO upload_reservations VALUES(:id,:batch,:key,:digest,:name)'),
                dict(id=ident,batch=batch_id,key=key,digest=fingerprint,name=name))
            return self._reservation({'id':ident,'declared_size':declared_size,'received_size':0,'status':'receiving'})

    @staticmethod
    def _reservation(row):
        return dict(upload_id=row['id'],url='/tus/'+row['id'],declared_size=row['declared_size'],
                    received_size=row['received_size'],status=row['status'])

    def _upload(self, conn, token, csrf, upload_id):
        actor=self._actor(conn,token,csrf)
        row=conn.execute(text('''SELECT s.*,b.status AS batch_status FROM upload_sessions s
            JOIN upload_batches b ON b.id=s.batch_id WHERE s.id=:id'''),dict(id=upload_id)).mappings().first()
        if (not row or row['owner_id']!=actor['id'] or row['authz_version']!=actor['authz_version']
                or row['batch_status']!='open' or row['status'] not in {'receiving','staged'}
                or row['expires_at']<=self.auth.now()):
            raise AuthError('upload_permission_revoked',403)
        return dict(row)

    def authorize(self, token, csrf, upload_id):
        upload_uuid(upload_id)
        with self.engine.connect() as conn:
            return self._upload(conn,token,csrf,upload_id)

    def begin(self, token, csrf, upload_id, *, method, backend_id, offset=None, length=0):
        upload_uuid(upload_id);upload_uuid(backend_id)
        if method not in {'POST','HEAD','PATCH','DELETE'}:
            raise AuthError('method_rejected',405)
        with write_transaction(self.engine) as conn:
            row=self._upload(conn,token,csrf,upload_id)
            if conn.execute(text('SELECT 1 FROM upload_transport_ops WHERE upload_id=:id AND state IN '+ACTIVE_OPS),dict(id=upload_id)).first():
                raise AuthError('upload_busy',423)
            if method=='POST' and row['tus_id']:
                return None,row
            if method!='POST' and not row['tus_id']:
                raise AuthError('upload_not_created',409)
            if method=='PATCH':
                if row['status']!='receiving' or type(offset) is not int or offset!=row['received_size']:
                    raise AuthError('offset_conflict',409)
                if not 1 <= length <= CHUNK_LIMIT or offset+length>row['declared_size']:
                    raise AuthError('chunk_size',413)
            if conn.execute(text('SELECT count(*) FROM upload_transport_ops WHERE state IN '+ACTIVE_OPS)).scalar_one()>=2:
                raise AuthError('system_upload_limit',429)
            ident=str(uuid.uuid4())
            conn.execute(text("INSERT INTO upload_transport_ops VALUES(:id,:upload,:backend,:method,'reserved',:now,NULL)"),
                dict(id=ident,upload=upload_id,backend=backend_id,method=method,now=self.auth.now()))
            return ident,row

    def sent(self, operation):
        with write_transaction(self.engine) as conn:
            if conn.execute(text("UPDATE upload_transport_ops SET state='sent' WHERE id=:id AND state='reserved' AND stop_evidence IS NULL"),dict(id=operation)).rowcount!=1:
                raise AuthError('upload_operation_stopped',409)

    def interrupted(self, operation):
        # A queued thread cancelled before sent() cannot reach the upstream.
        # Once sent, cancellation/timeout supplies no evidence of backend stop.
        with write_transaction(self.engine) as conn:
            conn.execute(text("""UPDATE upload_transport_ops SET state=CASE WHEN state='reserved' THEN 'stopped' ELSE 'uncertain' END
                WHERE id=:id AND state IN ('reserved','sent')"""),dict(id=operation))

    def response(self, token, csrf, operation, *, tus_id=None, offset=None, deleted=False):
        denied=None
        with write_transaction(self.engine) as conn:
            op=conn.execute(text('SELECT * FROM upload_transport_ops WHERE id=:id'),dict(id=operation)).mappings().one()
            if op['state'] not in {'sent','uncertain'}:
                raise AuthError('upload_operation_stopped',409)
            conn.execute(text("UPDATE upload_transport_ops SET state='complete' WHERE id=:id"),dict(id=operation))
            if tus_id is not None:
                conn.execute(text('UPDATE upload_sessions SET tus_id=:tus WHERE id=:id AND tus_id IS NULL'),dict(tus=tus_id,id=op['upload_id']))
            if offset is not None:
                conn.execute(text('UPDATE upload_sessions SET received_size=:offset WHERE id=:id'),dict(offset=offset,id=op['upload_id']))
            try:
                row=self._upload(conn,token,csrf,op['upload_id'])
            except AuthError as exc:
                denied=exc
            else:
                if deleted:
                    conn.execute(text("UPDATE upload_sessions SET status='expired' WHERE id=:id"),dict(id=row['id']))
                elif row['received_size']==row['declared_size']:
                    conn.execute(text("UPDATE upload_sessions SET status='staged' WHERE id=:id"),dict(id=row['id']))
        if denied:
            raise denied

    def _backend_stopped(self, backend_id, evidence):
        """Called only by the gateway's verified owned-process controller.

        No HTTP route exposes this operation. Existing staging bytes survive.
        Unbound POSTs are retired rather than recreated with a guessed ID.
        """
        with self.ingress_guard(reconcile=True) as quiescent, write_transaction(self.engine) as conn:
            conn.execute(text("""UPDATE upload_sessions SET status='expired' WHERE tus_id IS NULL
                AND status='receiving' AND id IN (SELECT upload_id FROM upload_transport_ops
                    WHERE backend_id=:backend AND method='POST' AND state IN ('sent','uncertain'))"""),dict(backend=backend_id))
            # Backend death fences every operation, but does not prove that an
            # application handler has stopped receiving its request body.
            conn.execute(text("""UPDATE upload_transport_ops SET
                state=CASE WHEN state='reserved' AND :quiescent=0 THEN state ELSE 'stopped' END,
                stop_evidence=:evidence WHERE backend_id=:backend AND state IN """+ACTIVE_OPS),
                         dict(backend=backend_id,evidence=evidence,quiescent=int(quiescent)))
