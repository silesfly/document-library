"""Fixed session tickets, physical transfer audit and conservative owner recovery.

HTTP adapters must call response factories inside their actual ASGI invocation.
This service does not create routes, background workers or complete ZIP files.
"""
import asyncio
from dataclasses import dataclass
import json
import re
import threading
import uuid

from sqlalchemy import text

from .auth import AuthError
from .jobs import ClockBlocked
from .library_contracts import AssetSelection, TICKET_TTL_MS, ZIP_LIMIT
from .library_policy import LibraryPolicyError, LibraryReadPolicy
from .lifecycle import LifecycleService
from .publication import PublicationError, object_path
from .storage import StorageBusy, StorageError
from .streaming import (DEFAULT_ADMISSION, OwnedProducer, OwnedResponse,
                        PreparedTransfer, original_response)
from .transfer_owner import TransferOwner, stopped_owner
from .zip_stream import ZipEntry, archive_layout, zip_chunks


class DownloadError(LibraryPolicyError):
    def __init__(self, code, status=409, *, individual_options=()):
        super().__init__(code,status)
        self.individual_options = tuple(individual_options)


@dataclass(eq=False)
class _Finalization:
    handle: object
    kind: str
    payload: object
    key: object
    running: bool = False


class DownloadService:
    def __init__(self, publisher, *, policy=None, lifecycle=None, owner=None,
                 admission=DEFAULT_ADMISSION, zip_max_bytes=ZIP_LIMIT, quota=None):
        self.publisher, self.jobs, self.engine = publisher, publisher.jobs, publisher.engine
        self.root, self.locks = publisher.root, publisher.locks
        self.auth = publisher.permissions.auth
        self.policy = policy or LibraryReadPolicy(self.auth,now_ms=lambda:self.jobs._sample()[1])
        self.lifecycle = lifecycle or LifecycleService(publisher)
        if self.policy.engine is not self.engine or self.lifecycle.engine is not self.engine:
            raise ValueError('One authoritative download engine required')
        if type(zip_max_bytes) is not int or not 98 <= zip_max_bytes <= ZIP_LIMIT:
            raise ValueError('Archive cap must fit the fixed schema maximum')
        self.admission, self.zip_max_bytes = admission, zip_max_bytes
        if quota is not None and (quota.auth is not self.auth or quota.engine is not self.engine):
            raise ValueError('Shared download quota authority required')
        self.quota = quota
        self.owner = owner or TransferOwner(self.root)
        self._pending, self._pending_mutex = {}, threading.Lock()

    def _clock_auth_in(self, conn, now, token, *, csrf=None):
        # Auth currently resolves seconds; its wall time must agree with the
        # durable job clock, and ticket admission additionally uses exact ms.
        if abs(self.auth.now()*1000-now) >= 1000:
            raise ClockBlocked('download_auth_clock_mismatch')
        return self.auth.current(conn,token,csrf=csrf)

    def _resolve(self, token, selection, *, snapshot=None):
        with self.engine.connect() as conn, conn.begin():
            return self.policy.resolve_selection_in(conn,token,selection,
                policy='ticket' if snapshot else 'current',ticket_snapshot=snapshot)

    def _registration(self):
        created = []
        def on_created(handle):
            created.append(handle)
            self.owner.track(handle)
            self._retain(handle,'response',None)
        return created,on_created

    def _retain(self, handle, kind, payload):
        """Register before submitting a worker; a live response is also a barrier."""
        with self._pending_mutex:
            record = self._pending.get(handle) if handle is not None else None
            if record is None:
                key = handle if handle is not None else object()
                record = _Finalization(handle,kind,payload,key)
                self._pending[key] = record
            elif record.payload is not None and (record.kind,record.payload) != (kind,payload):
                raise DownloadError('FINALIZATION_PAYLOAD_CONFLICT',409)
            else:
                record.kind,record.payload = kind,payload
            return record

    def _run_pending(self, record):
        # Only the actual worker changes running. Cancelling its async awaiter
        # cannot make the still executing DB operation appear idle.
        with self._pending_mutex:
            if (self._pending.get(record.key) is not record or record.running
                    or record.payload is None or
                    (record.handle is not None and not self.owner._stopped(record.handle))):
                return None
            record.running = True
        settled = False
        try:
            if record.kind == 'ticket':
                snapshot,sent,completed,evidence,code,unknown = record.payload
                self._finish(snapshot,sent,completed,evidence,code,unknown=unknown)
            elif record.kind == 'original':
                self._original_event(*record.payload)
            elif record.kind != 'discard':
                raise DownloadError('INVALID_FINALIZATION_KIND',409)
            if record.handle is not None:
                self.owner.forget(record.handle)
            settled = True
            return True
        finally:
            with self._pending_mutex:
                record.running = False
                if settled and self._pending.get(record.key) is record:
                    self._pending.pop(record.key)

    def pending_finalizations(self):
        """Count awaiting responses, retained payloads and actual worker operations."""
        with self._pending_mutex:
            return len(self._pending)

    def retry_pending(self):
        """Try each ready record once; retried counts attempts, including discards."""
        self.owner._assert_process()
        with self._pending_mutex:
            records = tuple(self._pending.values())
        retried = failed = 0
        for record in records:
            try:
                attempted = self._run_pending(record)
            except BaseException:
                retried += 1
                failed += 1
            else:
                if attempted is not None:
                    retried += 1
        return dict(pending=self.pending_finalizations(),retried=retried,failed=failed)

    @staticmethod
    def _request(work_id, selection):
        if (not isinstance(selection,(tuple,list)) or not 1 <= len(selection) <= 64
                or any(not isinstance(item,AssetSelection) or item.positions is not None
                       or item.option_id is None or item.work_id != work_id
                       or item.edition_id is None for item in selection)
                or len({item.option_id for item in selection}) != len(selection)):
            raise DownloadError('INVALID_SELECTION',400)
        return tuple(selection)

    @staticmethod
    def _entries(selected):
        result = []
        for index, asset in enumerate(selected.assets):
            if not re.fullmatch('[a-z0-9]{1,16}',asset.file_format):
                raise DownloadError('INVALID_ARCHIVE_FORMAT',409)
            name = f'edition-{asset.selection_index+1:03d}-asset-{index+1:03d}.{asset.file_format}'
            result.append(ZipEntry(name,asset.byte_size))
        return tuple(result)

    @staticmethod
    def _layout(snapshot):
        layout = archive_layout(ZipEntry(asset.entry_name,asset.byte_size) for asset in snapshot.assets)
        if layout.total_bytes != snapshot.archive_bytes:
            raise DownloadError('TICKET_ARCHIVE_CONFLICT',409)
        return layout

    def issue(self, token, csrf, work_id, selection):
        if not isinstance(csrf,str) or not csrf:
            raise DownloadError('csrf_rejected',403)
        selection = self._request(work_id,selection)
        if self.quota is not None:
            self.quota.admit(token,action='download',csrf=csrf)
        with self.engine.connect() as conn, conn.begin():
            self.auth.current(conn,token,csrf=csrf)
            selected = self.policy.resolve_selection_in(conn,token,selection)
        if not 1 <= len(selected.assets) <= 200:
            raise DownloadError('INVALID_SELECTION',400)
        entries = self._entries(selected)
        layout = archive_layout(entries)
        if layout.total_bytes > self.zip_max_bytes:
            raise DownloadError('ZIP_TOO_LARGE',413,
                                individual_options=[option.public() for option in selected.options])
        shas = sorted({asset.object_sha for asset in selected.assets})
        # Hashing may hold many FDs; reserve before opening them, including issue.
        reservation = self.admission.reserve(unique_originals=len(shas))
        try:
            unique = {asset.object_sha:asset for asset in selected.assets}
            verified = self.publisher._validated_files([
                dict(sha256=asset.object_sha,byte_size=asset.byte_size,prepared_relpath=None)
                for asset in unique.values()])
        finally:
            reservation.acknowledge_closed()
            reservation.release_after_exit()
        ident = str(uuid.uuid4())
        with self.owner.admitting(), self.locks.mutation(shas), self.jobs._writer() as (conn,now):
            self._clock_auth_in(conn,now,token,csrf=csrf)
            if self.policy.resolve_selection_in(conn,token,selection) != selected:
                raise DownloadError('SELECTION_CHANGED',409)
            for sha in shas:
                self.publisher._unchanged(object_path(sha),verified)
            principal = selected.principal
            conn.execute(text('''INSERT INTO download_tickets
                (id,user_id,session_token_hash,credential_version,work_id,issued_at_ms,expires_at_ms,archive_bytes)
                VALUES(:id,:user,:session,:version,:work,:now,:expiry,:size)'''),
                dict(id=ident,user=principal.user_id,session=principal.session_hash,
                     version=principal.credential_version,work=work_id,now=now,
                     expiry=now+TICKET_TTL_MS,size=layout.total_bytes))
            for option in selected.options:
                conn.execute(text('INSERT INTO download_ticket_options VALUES(:ticket,:ordering,:edition,:revision,:option,:format,:delivery,:coverage)'),
                    dict(ticket=ident,ordering=option.selection_index,edition=option.edition_id,
                         revision=option.revision_id,option=option.option_id,format=option.format,
                         delivery=option.delivery_kind,coverage=option.coverage_json))
            for asset,entry in zip(selected.assets,entries):
                conn.execute(text('INSERT INTO download_ticket_assets VALUES(:ticket,:ordering,:ordinal,:revision,:position,:sha,:size,:mime,:format,:role,:name)'),
                    dict(ticket=ident,ordering=asset.selection_index,ordinal=asset.ordinal,revision=asset.revision_id,
                         position=asset.asset_position,sha=asset.object_sha,size=asset.byte_size,
                         mime=asset.detected_mime,format=asset.file_format,role=asset.asset_role,name=entry.name))
            for sha in shas:
                conn.execute(text("INSERT INTO object_pins VALUES('ticket',:id,:sha,:now)"),dict(id=ident,sha=sha,now=now))
            conn.execute(text("UPDATE download_tickets SET snapshot_digest=:digest,state='issued' WHERE id=:id"),
                         dict(id=ident,digest=self.policy.ticket_digest_in(conn,ident)))
            # Do not open a second transaction inside the writer for the return value.
            snapshot = self.policy._ticket_rows(conn,ident)[1]
        return snapshot

    def _load(self, token, ticket_id):
        # Public admission observes the durable clock before a policy callback
        # can reject an invalid sample, preserving the fence even on GET/HEAD.
        with self.jobs._writer() as (conn,now):
            self._clock_auth_in(conn,now,token)
            snapshot = self.policy.load_ticket_snapshot_in(conn,token,ticket_id)
            if not snapshot.issued_at_ms <= now < snapshot.expires_at_ms:
                raise DownloadError('TICKET_EXPIRED_OR_USED',409)
            return snapshot

    def _claim(self, token, initial):
        shas = [asset.object_sha for asset in initial.assets]
        attempt_id = str(uuid.uuid4())
        with self.locks.mutation(shas,blocking=False), self.jobs._writer() as (conn,now):
            self.owner.validate()
            self._clock_auth_in(conn,now,token)
            snapshot = self.policy.load_ticket_snapshot_in(conn,token,initial.ticket_id)
            if snapshot != initial or not snapshot.issued_at_ms <= now < snapshot.expires_at_ms:
                raise DownloadError('TICKET_EXPIRED_OR_USED',409)
            self.policy.resolve_selection_in(conn,token,self.policy.selections_for_ticket(snapshot),
                                             policy='ticket',ticket_snapshot=snapshot)
            conn.execute(text('''INSERT INTO download_attempts(id,ticket_id,owner_instance,started_at_ms)
                VALUES(:id,:ticket,:owner,:now)'''),
                dict(id=attempt_id,ticket=snapshot.ticket_id,owner=self.owner.descriptor,now=now))
            changed = conn.execute(text("UPDATE download_tickets SET state='streaming',used_at_ms=:now WHERE id=:id AND state='issued' AND invalidated=0"),
                                   dict(id=snapshot.ticket_id,now=now)).rowcount
            if changed != 1:
                raise DownloadError('TICKET_EXPIRED_OR_USED',409)
            return self.policy.load_ticket_snapshot_in(conn,token,snapshot.ticket_id,attempt_id=attempt_id)

    def ticket_response(self, token, ticket_id, *, method='GET'):
        """Factory: invoke only from E's actual ASGI __call__, without an await gap."""
        if method not in {'GET','HEAD'}:
            raise DownloadError('METHOD_NOT_ALLOWED',405)
        if method == 'GET' and self.quota is not None:
            self.quota.admit(token,action='download')
        initial = self._load(token,ticket_id)
        layout = self._layout(initial)
        with self.owner.admitting():
            try:
                if method == 'GET':
                    snapshot = self._claim(token,initial)
                else:
                    with self.jobs._writer() as (conn,now):
                        self._clock_auth_in(conn,now,token)
                        snapshot = self.policy.load_ticket_snapshot_in(conn,token,ticket_id)
                        if snapshot != initial or not snapshot.issued_at_ms <= now < snapshot.expires_at_ms:
                            raise DownloadError('TICKET_EXPIRED_OR_USED',409)
            except StorageBusy as error:
                raise DownloadError('TICKET_BUSY',409) from error
            selection = self.policy.selections_for_ticket(snapshot)
            resolve = lambda:self._resolve(token,selection,snapshot=snapshot)
            created,on_created = self._registration()
            try:
                expected = resolve()
                def prepare(opened):
                    return PreparedTransfer(200,((b'content-type',b'application/zip'),
                        (b'content-length',str(layout.total_bytes).encode()),
                        (b'content-disposition',b'attachment; filename="download.zip"'),
                        (b'cache-control',b'no-store'),(b'referrer-policy',b'no-referrer'),
                        (b'x-content-type-options',b'nosniff')))
                def produce(opened, transfer):
                    if method == 'HEAD':
                        return
                    checked, since_check = False, 0
                    def reader(index):
                        nonlocal checked, since_check
                        for data in opened.chunks(expected.assets[index]):
                            # One fresh full-selection checkpoint before the
                            # first source bytes and before every further 64KiB.
                            # Tiny bundle members share this byte budget.
                            if not checked or since_check+len(data) > 65536:
                                opened.revalidate()
                                checked, since_check = True, 0
                            since_check += len(data)
                            yield data
                    yield from zip_chunks(layout,reader)
                handle = OwnedProducer.start(open_selected=lambda:self.publisher.read_selected(resolve,expected=expected),
                    prepare=prepare,produce=produce,unique_originals=len({a.object_sha for a in expected.assets}),
                    admission=self.admission,on_created=on_created)
            except BaseException as error:
                if method == 'GET':
                    self._factory_failure(snapshot,created,error,'PRODUCER_START_FAILED')
                elif created:
                    created[0].request_stop('head_start_failed')
                    self._retain(created[0],'discard',())
                raise
            async def closed(ack, success, sent):
                if method == 'GET':
                    record = self._retain(handle,'ticket',(snapshot,sent,
                        success and ack.error is None and not ack.stopped and sent == layout.total_bytes,
                        'PHYSICAL_CLOSE_AND_THREAD_EXIT',self._failure(ack.error,'TRANSPORT_INCOMPLETE'),False))
                else:
                    record = self._retain(handle,'discard',())
                await asyncio.to_thread(self._run_pending,record)
            try:
                return OwnedResponse(handle,on_closed=closed,on_preflight_error=self._preflight_error)
            except BaseException as error:
                # A producer exists: stopping is only a request. Keep it owned;
                # explicit shutdown awaits actual exit, then recovery can finalize.
                handle.request_stop('response_construction_failed')
                if method == 'GET':
                    self._factory_failure(snapshot,created,error,'RESPONSE_CONSTRUCTION_FAILED')
                else:
                    self._retain(handle,'discard',())
                raise

    def _factory_failure(self, snapshot, created, error, code):
        """Preserve the original exception and never finalize ambiguous bootstrap."""
        handle = created[0] if created else None
        evidence = 'NO_PRODUCER_STARTED' if handle is None or handle.never_started.is_set() else 'PHYSICAL_CLOSE_AND_THREAD_EXIT'
        record = self._retain(handle,'ticket',(snapshot,0,False,evidence,code,False))
        try:
            if handle is None or self.owner._stopped(handle):
                self._run_pending(record)
            else:
                handle.request_stop('factory_failed')
                with self.jobs._writer() as (conn,_now):
                    conn.execute(text("UPDATE download_attempts SET state='cancelling',failure_code=:code WHERE id=:id AND state='running'"),
                                 dict(id=snapshot.attempt_id,code=code))
        except BaseException:
            # No false terminal/pin release. Durable recovery remains possible;
            # even clock-fence failure must not replace an original KI/error.
            error.add_note('Download failure finalization incomplete; pins retained for verified recovery')

    @staticmethod
    async def _preflight_error(error):
        if isinstance(error,(PublicationError,OSError)):
            return DownloadError('ASSET_UNAVAILABLE',409)
        return error

    @staticmethod
    def _failure(error, default):
        if isinstance(error,AuthError):
            return error.code
        if isinstance(error,(PublicationError,OSError)):
            return 'ASSET_UNAVAILABLE'
        return default

    def _finish(self, snapshot, sent, completed, evidence, failure_code, *, unknown=False):
        layout = self._layout(snapshot)
        asset_sent = layout.source_bytes(sent)
        shas = sorted({asset.object_sha for asset in snapshot.assets})
        with self.locks.mutation(shas), self.jobs._writer() as (conn,now):
            attempt = conn.execute(text('SELECT * FROM download_attempts WHERE id=:id AND ticket_id=:ticket'),
                dict(id=snapshot.attempt_id,ticket=snapshot.ticket_id)).mappings().one()
            if unknown:
                # The prefetch in recover can be arbitrarily stale. Preserve
                # the cause and lower bound committed in this writer's row.
                failure_code = attempt['failure_code'] or failure_code
                sent = attempt['bytes_sent']
                asset_sent = layout.source_bytes(sent)
            if attempt['state'] in {'completed','failed','cancelled'}:
                self._consistent_terminal(conn,snapshot,attempt)
                if not unknown and (attempt['state'] != ('completed' if completed else 'failed')
                        or attempt['bytes_sent'] != sent
                        or attempt['failure_code'] != (None if completed else failure_code)):
                    raise DownloadError('FINALIZATION_CONFLICT',409)
                return False
            if completed and (unknown or attempt['state'] != 'running' or attempt['invalidated']):
                raise DownloadError('INVALID_COMPLETION_EVIDENCE',409)
            status = 'completed' if completed else 'failed'
            ended = max(now,attempt['started_at_ms'])
            stop = json.dumps(dict(kind=evidence,byte_accounting='unknown_lower_bound' if unknown else 'successful_asgi_calls'),sort_keys=True)
            params = dict(id=attempt['id'],status=status,ended=ended,sent=sent,stop=stop,
                          failure=None if completed else failure_code)
            conn.execute(text('''UPDATE download_attempts SET state=:status,ended_at_ms=:ended,
                bytes_sent=:sent,stop_evidence=:stop,failure_code=:failure WHERE id=:id'''),params)
            conn.execute(text('UPDATE download_tickets SET state=:status WHERE id=:ticket'),dict(status=status,ticket=snapshot.ticket_id))
            event_id = str(uuid.uuid4())
            conn.execute(text('''INSERT INTO download_events VALUES(:event,:logical,:user,:work,:ticket,:attempt,:started,:ended,:status,:sent,:stop,:failure)'''),
                dict(event=event_id,logical=snapshot.ticket_id,user=snapshot.user_id,work=snapshot.work_id,
                     ticket=snapshot.ticket_id,attempt=attempt['id'],started=attempt['started_at_ms'],
                     ended=ended,status=status,sent=sent,stop=stop,failure=params['failure']))
            editions = {option.selection_order:option.edition_id for option in snapshot.options}
            for index,(asset,count) in enumerate(zip(snapshot.assets,asset_sent)):
                conn.execute(text('INSERT INTO download_event_assets VALUES(:event,:ordinal,:edition,:revision,:position,:sha,:sent)'),
                    dict(event=event_id,ordinal=index,edition=editions[asset.selection_order],revision=asset.revision_id,
                         position=asset.asset_position,sha=asset.object_sha,sent=count))
            current = conn.execute(text("SELECT object_sha FROM object_pins WHERE kind='ticket' AND reference_id=:id"),dict(id=snapshot.ticket_id)).scalars().all()
            if set(current) != set(shas):
                raise DownloadError('TICKET_PIN_CONFLICT',409)
            conn.execute(text("DELETE FROM object_pins WHERE kind='ticket' AND reference_id=:id"),dict(id=snapshot.ticket_id))
            self.lifecycle._reset_grace(conn,shas,now)
            return True

    def _consistent_terminal(self, conn, snapshot, attempt):
        event = conn.execute(text('SELECT * FROM download_events WHERE attempt_id=:id'),dict(id=attempt['id'])).mappings().first()
        expected = (snapshot.user_id,snapshot.work_id,snapshot.ticket_id,attempt['id'],attempt['started_at_ms'],
                    attempt['ended_at_ms'],attempt['state'],attempt['bytes_sent'],attempt['stop_evidence'],attempt['failure_code'])
        keys = ('user_id','work_id','ticket_id','attempt_id','started_at_ms','ended_at_ms','status','bytes_sent','stop_evidence','failure_code')
        if not event or tuple(event[key] for key in keys) != expected:
            raise DownloadError('TERMINAL_AUDIT_CONFLICT',409)
        editions = {option.selection_order:option.edition_id for option in snapshot.options}
        expected_assets = [(i,editions[a.selection_order],a.revision_id,a.asset_position,a.object_sha,count)
                           for i,(a,count) in enumerate(zip(snapshot.assets,self._layout(snapshot).source_bytes(attempt['bytes_sent'])))]
        actual = conn.execute(text('SELECT ordinal,edition_id,revision_id,asset_position,object_sha,bytes_sent FROM download_event_assets WHERE event_id=:id ORDER BY ordinal'),dict(id=event['id'])).all()
        state = conn.execute(text('SELECT state FROM download_tickets WHERE id=:id'),dict(id=snapshot.ticket_id)).scalar_one()
        if actual != expected_assets or state != attempt['state']:
            raise DownloadError('TERMINAL_AUDIT_CONFLICT',409)

    def expire(self, ticket_id):
        """Only issued→expired CAS proves no physical attempt ever started."""
        with self.engine.connect() as conn:
            initial = self.policy._ticket_rows(conn,ticket_id)[1]
        if initial.state != 'issued':
            return False
        shas = [asset.object_sha for asset in initial.assets]
        with self.locks.mutation(shas), self.jobs._writer() as (conn,now):
            row,snapshot = self.policy._ticket_rows(conn,ticket_id)
            if row['state'] != 'issued' or now < snapshot.expires_at_ms:
                return False
            if conn.execute(text('SELECT 1 FROM download_attempts WHERE ticket_id=:id'),dict(id=ticket_id)).first():
                raise DownloadError('TICKET_ATTEMPT_CONFLICT',409)
            changed = conn.execute(text("UPDATE download_tickets SET state='expired' WHERE id=:id AND state='issued'"),dict(id=ticket_id)).rowcount
            if changed:
                conn.execute(text("DELETE FROM object_pins WHERE kind='ticket' AND reference_id=:id"),dict(id=ticket_id))
                self.lifecycle._reset_grace(conn,shas,now)
            return bool(changed)

    def recover(self, attempt_id):
        """One bounded attempt. Busy/missing/replaced owner preserves all pins."""
        with self.engine.connect() as conn:
            attempt = conn.execute(text('SELECT * FROM download_attempts WHERE id=:id'),dict(id=attempt_id)).mappings().first()
            if not attempt or attempt['state'] in {'completed','failed','cancelled'}:
                return False
            snapshot = self.policy._ticket_rows(conn,attempt['ticket_id'],attempt_id=attempt_id)[1]
        with stopped_owner(self.root,attempt['owner_instance']) as stopped:
            if stopped is None:
                return False
            return self._finish(snapshot,attempt['bytes_sent'],False,'EXCLUSIVE_UNCHANGED_OWNER_INODE',
                                'OWNER_STOPPED_BYTES_UNKNOWN',unknown=True)

    def original_download_response(self, token, selection, *, method='GET', range_value=None, if_range=None):
        """Individual attachment audit; Range completion means requested bytes only."""
        if method not in {'GET','HEAD'}:
            raise DownloadError('METHOD_NOT_ALLOWED',405)
        if method == 'GET' and self.quota is not None:
            self.quota.admit(token,action='download')
        resolve = lambda:self._resolve(token,tuple(selection))
        expected = resolve()
        if len(expected.assets) != 1:
            raise DownloadError('SINGLE_ORIGINAL_REQUIRED',400)
        logical_id = str(uuid.uuid4())
        with self.owner.admitting():
            with self.jobs._writer() as (conn,started):
                self._clock_auth_in(conn,started,token)
            async def closed(ack, success, sent):
                transfer = response.handle.transfer
                if method == 'GET' and transfer is not None and transfer.payload is not None:
                    record = self._retain(response.handle,'original',(expected,logical_id,started,sent,
                        success and ack.error is None and not ack.stopped and sent == transfer.payload.length,
                        self._failure(ack.error,'TRANSPORT_INCOMPLETE')))
                else:
                    record = self._retain(response.handle,'discard',())
                await asyncio.to_thread(self._run_pending,record)
            created,on_created = self._registration()
            try:
                response = original_response(self.publisher,resolve,method=method,range_value=range_value,
                    if_range=if_range,admission=self.admission,on_closed=closed,on_created=on_created)
            except BaseException:
                if created:
                    handle = created[0]
                    handle.request_stop('original_factory_failed')
                    if method == 'GET':
                        self._retain(handle,'original',(expected,logical_id,started,0,False,'RESPONSE_CONSTRUCTION_FAILED'))
                    else:
                        self._retain(handle,'discard',())
                raise
            return response

    def _original_event(self, expected, logical_id, started, sent, completed, failure):
        asset, principal = expected.assets[0], expected.principal
        with self.jobs._writer() as (conn,now):
            existing = conn.execute(text('SELECT * FROM download_events WHERE user_id=:user AND logical_download_id=:logical'),
                dict(user=principal.user_id,logical=logical_id)).mappings().first()
            if existing:
                fields = ('work_id','started_at_ms','status','bytes_sent','failure_code','ticket_id','attempt_id','stop_evidence')
                values = (asset.work_id,started,'completed' if completed else 'failed',sent,None if completed else failure,
                          None,None,'PHYSICAL_CLOSE_AND_THREAD_EXIT;successful_asgi_calls')
                members = conn.execute(text('SELECT edition_id,revision_id,asset_position,object_sha,bytes_sent FROM download_event_assets WHERE event_id=:id'),dict(id=existing['id'])).all()
                if tuple(existing[key] for key in fields) != values or members != [(asset.edition_id,asset.revision_id,asset.asset_position,asset.object_sha,sent)]:
                    raise DownloadError('FINALIZATION_CONFLICT',409)
                return False
            event_id = str(uuid.uuid4())
            conn.execute(text('''INSERT INTO download_events VALUES(:id,:logical,:user,:work,NULL,NULL,:started,:ended,:status,:sent,:stop,:failure)'''),
                dict(id=event_id,logical=logical_id,user=principal.user_id,work=asset.work_id,
                     started=started,ended=max(now,started),status='completed' if completed else 'failed',sent=sent,
                     stop='PHYSICAL_CLOSE_AND_THREAD_EXIT;successful_asgi_calls',failure=None if completed else failure))
            conn.execute(text('INSERT INTO download_event_assets VALUES(:id,0,:edition,:revision,:position,:sha,:sent)'),
                dict(id=event_id,edition=asset.edition_id,revision=asset.revision_id,position=asset.asset_position,
                     sha=asset.object_sha,sent=sent))

    def close(self, *, wait=True):
        self.owner._assert_process()
        error = None
        try:
            stopped = self.owner.close(wait=wait,release=False)
        except Exception as caught:
            error = caught
            stopped = False
        result = self.retry_pending()
        if result['pending']:
            if error is not None:
                raise error
            return False
        # No response/worker/payload barrier remains. The owner independently
        # rechecks actual local stop before releasing its retained descriptors.
        if stopped or error is not None:
            return self.owner.close(wait=wait)
        return False
