"""Transactional discovery. SDK access and byte publication are separate adapters.

Only configure/list_sources are user-facing. Remaining methods are trusted worker
operations. No SDK session, access hash, file reference or arbitrary metadata is
accepted. Cursors prove durable registration, not completed file publication.
"""
import json
from pathlib import PurePosixPath
import re
import uuid

from sqlalchemy import text

from .auth import AuthError, digest
from .database import write_transaction
from .ingest_formats import KNOWN
from .jobs import JobError, identifier, json_object


class SourceError(JobError):
    pass


def _integer(value, low=0, high=2**63-1):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid source integer')
    return value


def _observations(items):
    if not isinstance(items, list) or len(items) > 1000:
        raise ValueError('Bounded observation page required')
    result, identities, messages = [], set(), {}
    for item in items:
        if not isinstance(item, dict) or set(item) != {'message_id','media_index','state','metadata'}:
            raise ValueError('Unexpected observation fields')
        msg = _integer(item['message_id'], 1)
        index = _integer(item['media_index'], -1, 999)
        state, metadata = item['state'], item['metadata']
        if state not in {'attached','no_media','deleted','inaccessible'} or (state == 'attached') != (index >= 0):
            raise ValueError('Invalid media state')
        if not isinstance(metadata, dict):
            raise ValueError('Metadata object required')
        if state == 'attached':
            if set(metadata) != {'filename','byte_size','document_id','edited_at','grouped_id'}:
                raise ValueError('Only sanitized SDK metadata accepted')
            name = metadata['filename']
            if not isinstance(name, str) or not 1 <= len(name) <= 255 or any(ord(c) < 32 for c in name):
                raise ValueError('Invalid display filename')
            _integer(metadata['byte_size'], 1)
            _integer(metadata['document_id'], 1)
            _integer(metadata['edited_at'])
            if metadata['grouped_id'] is not None:
                _integer(metadata['grouped_id'], 1)
        elif metadata:
            raise ValueError('Unavailable observation has no attachment metadata')
        key = (msg,index)
        if key in identities or (msg in messages and (messages[msg] != 'attached' or state != 'attached')):
            raise ValueError('Ambiguous message state')
        identities.add(key)
        messages[msg] = state
        result.append(dict(message_id=msg,media_index=index,state=state,metadata=dict(metadata)))
    if len(messages) > 100:
        raise ValueError('At most 100 messages per page')
    # json_object enforces a bounded wire representation before DB changes.
    json_object({'observations':result})
    return sorted(result, key=lambda item:(item['message_id'],item['media_index']))


class SourceService:
    def __init__(self, auth, jobs):
        if auth.engine is not jobs.engine:
            raise ValueError('Shared transaction engine required')
        self.auth, self.jobs, self.engine = auth, jobs, jobs.engine

    def configure(self, token, csrf, *, account_scope, chat_id, extensions,
                  max_bytes=2*1024**3, filename_filter='', enabled=True, expected_version=None):
        if not isinstance(csrf,str) or not csrf:
            raise AuthError('csrf_rejected',403)
        identifier(account_scope, 64)
        _integer(chat_id, -(2**63)+1)
        if chat_id == 0 or type(enabled) is not bool:
            raise ValueError('Invalid source configuration')
        if (not isinstance(extensions,list) or not extensions or len(extensions)>len(KNOWN)
                or any(not isinstance(x,str) or x not in KNOWN for x in extensions)):
            raise ValueError('Unsupported source extensions')
        if not isinstance(filename_filter,str) or len(filename_filter)>100 or any(ord(c)<32 for c in filename_filter):
            raise ValueError('Invalid filename filter')
        _integer(max_bytes,1,2*1024**3)
        if expected_version is not None:
            _integer(expected_version,1)
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn,token,csrf=csrf,admin=True)
            old = conn.execute(text('SELECT * FROM ingestion_sources WHERE account_scope=:scope AND chat_id=:chat'),
                               dict(scope=account_scope,chat=chat_id)).mappings().first()
            if (old is None and expected_version is not None) or (old is not None and expected_version != old['config_version']):
                raise SourceError('Source configuration changed')
            ident = old['id'] if old else str(uuid.uuid4())
            values = dict(id=ident,scope=account_scope,chat=chat_id,extensions=json.dumps(sorted(set(extensions))),
                          maximum=max_bytes,filter=filename_filter,enabled=int(enabled))
            if old:
                conn.execute(text("""UPDATE ingestion_sources SET config_version=config_version+1,
                    enabled=:enabled,extensions_json=:extensions,max_bytes=:maximum,filename_filter=:filter,
                    event_epoch=event_epoch+1,recovery_state='RECONCILING' WHERE id=:id"""),values)
                conn.execute(text("UPDATE source_scans SET state='cancelled' WHERE source_id=:id AND state='active'"),values)
            else:
                conn.execute(text("""INSERT INTO ingestion_sources(id,account_scope,chat_id,enabled,extensions_json,max_bytes,filename_filter)
                    VALUES(:id,:scope,:chat,:enabled,:extensions,:maximum,:filter)"""),values)
            self.auth.audit(conn,actor['id'],'source.configure',details={'source_id':ident,'version':old['config_version']+1 if old else 1})
            return ident

    def list_sources(self, token):
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            return [dict(r) for r in conn.execute(text('SELECT * FROM ingestion_sources ORDER BY id')).mappings()]

    @staticmethod
    def _source(conn, source_id, version=None):
        row = conn.execute(text('SELECT * FROM ingestion_sources WHERE id=:id'),dict(id=source_id)).mappings().first()
        if not row or not row['enabled'] or (version is not None and row['config_version'] != version):
            raise SourceError('Source disabled or configuration changed')
        return row

    def mark_event_gap(self, source_id, *, config_version):
        """Trusted adapter after missing/unproven event recovery; never auto-login."""
        with self.jobs._writer() as (conn,_now):
            self._source(conn,source_id,config_version)
            conn.execute(text("UPDATE ingestion_sources SET event_epoch=event_epoch+1,recovery_state='RECONCILING' WHERE id=:id"),dict(id=source_id))

    def start_scan(self, source_id, *, config_version, request_key, mode, upper_bound=None):
        identifier(request_key,100)
        _integer(config_version,1)
        if mode not in {'incremental','events','reconcile'}:
            raise ValueError('Unknown scan mode')
        if upper_bound is not None:
            _integer(upper_bound)
        if (mode == 'reconcile') != (upper_bound is None):
            raise ValueError('Reconciliation snapshots known messages; other scans need explicit upper bound')
        fingerprint=digest(json_object(dict(version=config_version,mode=mode,upper=upper_bound)))
        with self.jobs._writer() as (conn,_now):
            source=self._source(conn,source_id,config_version)
            # A terminal job cannot own an active source scan. Keep its page
            # evidence and cursor intact; a fresh request may resume discovery.
            conn.execute(text("""UPDATE source_scans SET state='cancelled'
                WHERE source_id=:source AND state='active' AND job_id IN
                    (SELECT id FROM jobs WHERE state IN ('failed','cancelled','completed'))"""),
                dict(source=source_id))
            old=conn.execute(text('SELECT * FROM source_scans WHERE source_id=:source AND request_key=:key'),
                             dict(source=source_id,key=request_key)).mappings().first()
            if old:
                if old['request_digest'] != fingerprint:
                    raise SourceError('Scan request conflict')
                return dict(old)
            if conn.execute(text("SELECT 1 FROM source_scans WHERE source_id=:id AND state='active'"),dict(id=source_id)).first():
                raise SourceError('Source scan already active')
            cursor=0 if mode=='reconcile' else source['high_water' if mode=='incremental' else 'event_cursor']
            upper=(conn.execute(text('SELECT coalesce(max(message_id),0) FROM source_messages WHERE source_id=:id'),dict(id=source_id)).scalar_one()
                   if mode=='reconcile' else upper_bound)
            if upper < cursor:
                raise SourceError('Cursor cannot move backward')
            scan_id=str(uuid.uuid4())
            job=self.jobs.enqueue_in(conn,kind='source.scan',idempotency_key='source-scan:'+scan_id,
                                     payload={'scan_id':scan_id},priority=10,max_attempts=100)
            conn.execute(text("""INSERT INTO source_scans VALUES(:id,:source,:version,:key,:digest,:mode,:upper,:cursor,:epoch,:job,'active')"""),
                         dict(id=scan_id,source=source_id,version=config_version,key=request_key,digest=fingerprint,
                              mode=mode,upper=upper,cursor=cursor,epoch=source['event_epoch'],job=job))
            return dict(conn.execute(text('SELECT * FROM source_scans WHERE id=:id'),dict(id=scan_id)).mappings().one())

    def get_scan(self, scan_id):
        with self.engine.connect() as conn:
            row=conn.execute(text('SELECT * FROM source_scans WHERE id=:id'),dict(id=scan_id)).mappings().first()
            return dict(row) if row else None

    def _scan(self, conn, lease):
        row=conn.execute(text('SELECT * FROM source_scans WHERE job_id=:id'),dict(id=lease.job_id)).mappings().first()
        if not row or row['state']!='active':
            raise SourceError('Scan is not active')
        return row,self._source(conn,row['source_id'],row['config_version'])

    @staticmethod
    def _known_page(conn, scan):
        return list(conn.execute(text('''SELECT message_id FROM source_messages WHERE source_id=:source
            AND message_id>:cursor AND message_id<=:upper ORDER BY message_id LIMIT 100'''),
            dict(source=scan['source_id'],cursor=scan['cursor'],upper=scan['upper_bound'])).scalars())

    def reconciliation_page(self, lease):
        with self.jobs.ownership(lease) as conn:
            scan,_source=self._scan(conn,lease)
            if scan['mode']!='reconcile':
                raise SourceError('Not a reconciliation scan')
            return {'cursor':scan['cursor'],'message_ids':self._known_page(conn,scan),'upper_bound':scan['upper_bound']}

    def commit_page(self, lease, *, expected_cursor, next_cursor, observations, done=False):
        _integer(expected_cursor);_integer(next_cursor)
        if type(done) is not bool:
            raise ValueError('Boolean completion required')
        items=_observations(observations)
        fingerprint=digest(json_object(dict(expected=expected_cursor,next=next_cursor,observations=items,done=done)))
        with self.jobs.ownership(lease) as conn:
            scan,source=self._scan(conn,lease)
            old=conn.execute(text('SELECT * FROM source_pages WHERE scan_id=:id AND cursor_before=:cursor'),
                             dict(id=scan['id'],cursor=expected_cursor)).mappings().first()
            if old:
                if old['request_digest']!=fingerprint:
                    raise SourceError('Page replay differs')
                return json.loads(old['receipt_json'])
            upper=scan['upper_bound']
            if (scan['cursor']!=expected_cursor or not expected_cursor<=next_cursor<=upper
                    or (next_cursor==expected_cursor and not (done and next_cursor==upper))
                    or done != (next_cursor==upper)):
                raise SourceError('Invalid cursor boundary')
            message_ids=sorted({item['message_id'] for item in items})
            if scan['mode']=='reconcile':
                expected=self._known_page(conn,scan)
                if message_ids!=expected or next_cursor!=(expected[-1] if expected else upper):
                    raise SourceError('Reconciliation must inspect the entire next known page')
            elif scan['mode']=='incremental' and any(not expected_cursor<x<=next_cursor for x in message_ids):
                raise SourceError('Message outside incremental page')
            observation_ids=[]
            for item in items:
                metadata=item['metadata']
                admission='UNAVAILABLE'
                if item['state']=='attached':
                    suffix=PurePosixPath(metadata['filename']).suffix.lower().lstrip('.')
                    admission=('DISCOVERED' if suffix in json.loads(source['extensions_json'])
                               and metadata['byte_size']<=source['max_bytes']
                               and source['filename_filter'].casefold() in metadata['filename'].casefold() else 'FILTERED')
                conn.execute(text('INSERT INTO source_messages VALUES(:source,:msg) ON CONFLICT DO NOTHING'),
                             dict(source=source['id'],msg=item['message_id']))
                ident=str(uuid.uuid4());job=None
                if admission=='DISCOVERED':
                    job=self.jobs.enqueue_in(conn,kind='source.download',idempotency_key='source-download:'+ident,
                                             payload={'observation_id':ident},priority=10,max_attempts=10)
                conn.execute(text('''INSERT INTO source_observations VALUES(:id,:scan,:page,:source,:msg,:media,:state,:metadata,:admission,:job)'''),
                             dict(id=ident,scan=scan['id'],page=expected_cursor,source=source['id'],msg=item['message_id'],media=item['media_index'],
                                  state=item['state'],metadata=json_object(metadata),admission=admission,job=job))
                observation_ids.append(ident)
            receipt={'observation_ids':observation_ids,'cursor':next_cursor,'done':done}
            conn.execute(text('INSERT INTO source_pages VALUES(:scan,:before,:after,:digest,:receipt)'),
                         dict(scan=scan['id'],before=expected_cursor,after=next_cursor,digest=fingerprint,receipt=json_object(receipt)))
            conn.execute(text("UPDATE source_scans SET cursor=:cursor,state=:state WHERE id=:id"),
                         dict(cursor=next_cursor,state='complete' if done else 'active',id=scan['id']))
            if scan['mode'] in {'incremental','events'}:
                column='high_water' if scan['mode']=='incremental' else 'event_cursor'
                conn.execute(text(f'UPDATE ingestion_sources SET {column}=:cursor WHERE id=:id'),dict(cursor=next_cursor,id=source['id']))
            elif done:
                self._reconciled_in(conn,source['id'])
            if done:
                # ownership() performs another live CAS on exit, so complete only afterward.
                self.jobs._cas(conn,lease,'checkpoint_json=:checkpoint',{'checkpoint':json_object({'scan_complete':True})})
            return receipt

    @staticmethod
    def _reconciled_in(conn, source_id):
        # Registration alone cannot prove content revalidation. Only a complete
        # pass at the current epoch with actual byte receipts clears the gap.
        conn.execute(text("""UPDATE ingestion_sources SET recovery_state='CURRENT'
            WHERE id=:id AND EXISTS(SELECT 1 FROM source_scans s WHERE s.source_id=:id
                AND s.mode='reconcile' AND s.state='complete'
                AND s.config_version=ingestion_sources.config_version
                AND s.event_epoch=ingestion_sources.event_epoch
                AND NOT EXISTS(SELECT 1 FROM source_observations o
                    WHERE o.scan_id=s.id AND (o.state='inaccessible' OR
                    (o.admission='DISCOVERED' AND NOT EXISTS(SELECT 1 FROM source_content_receipts r
                        WHERE r.observation_id=o.id)))))"""),dict(id=source_id))

    def finish_scan(self, lease):
        """Idempotency after a lost final-page reply is read through get_scan()."""
        with self.jobs.completion(lease,result={'scan_complete':True}) as conn:
            scan=conn.execute(text('SELECT * FROM source_scans WHERE job_id=:id'),dict(id=lease.job_id)).mappings().one()
            self._source(conn,scan['source_id'],scan['config_version'])
            if scan['state']!='complete':
                raise SourceError('Scan not complete')

    def download_input(self, lease):
        with self.jobs.ownership(lease) as conn:
            return self._download(conn,lease)

    def _download(self, conn, lease):
        row=conn.execute(text('''SELECT o.*,s.config_version FROM source_observations o
            JOIN source_scans s ON s.id=o.scan_id WHERE o.download_job_id=:job'''),dict(job=lease.job_id)).mappings().first()
        if not row or row['admission']!='DISCOVERED':
            raise SourceError('Not a discovered download')
        source=self._source(conn,row['source_id'],row['config_version'])
        return dict(row,account_scope=source['account_scope'],chat_id=source['chat_id'])

    def content_in(self, conn, lease, *, sha256, byte_size):
        """G1-D calls inside its owned publication txn after hashing actual bytes.

        This is provenance only: no object or active revision is created here.
        A SHA receipt does not prove an installed file or authorize publication.
        """
        if not isinstance(sha256,str) or not re.fullmatch('[0-9a-f]{64}',sha256):
            raise ValueError('Invalid actual content digest')
        _integer(byte_size,1,2*1024**3)
        self.jobs._cas(conn,lease,'lease_generation=lease_generation',{})
        item=self._download(conn,lease)
        if byte_size!=json.loads(item['metadata_json'])['byte_size']:
            raise SourceError('Downloaded size changed; observe again')
        old=conn.execute(text('''SELECT v.* FROM source_content_receipts r JOIN source_content_versions v
            ON v.id=r.version_id WHERE r.observation_id=:id'''),dict(id=item['id'])).mappings().first()
        if old:
            if old['sha256']!=sha256 or old['byte_size']!=byte_size:
                raise SourceError('Observation content conflict; observe again')
            return old['id']
        values=dict(source=item['source_id'],msg=item['message_id'],media=item['media_index'],sha=sha256,size=byte_size)
        version=conn.execute(text('''SELECT id,byte_size FROM source_content_versions
            WHERE source_id=:source AND message_id=:msg AND media_index=:media AND sha256=:sha'''),values).mappings().first()
        if version and version['byte_size']!=byte_size:
            raise SourceError('Content hash size conflict')
        ident=version['id'] if version else str(uuid.uuid4())
        if not version:
            conn.execute(text('INSERT INTO source_content_versions VALUES(:id,:source,:msg,:media,:sha,:size)'),dict(values,id=ident))
        conn.execute(text('INSERT INTO source_content_receipts VALUES(:obs,:version)'),dict(obs=item['id'],version=ident))
        self._reconciled_in(conn,item['source_id'])
        return ident
