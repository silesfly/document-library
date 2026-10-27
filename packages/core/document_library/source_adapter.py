"""Bounded Telethon-compatible adapter for an injected, already authorized client.

No connect/start/login is attempted; catch_up is an explicit recovery operation. SDK objects remain
ephemeral; only SourceService's allowlisted observation metadata reaches SQLite.
"""
import json

from sqlalchemy import text
from telethon.errors import FloodWaitError, FloodPremiumWaitError

from .sources import SourceError, _observations


class SourceWait(SourceError):
    pass


class _DownloadSink:
    def __init__(self, fd, limit):
        self.fd, self.limit, self.size = fd, limit, 0

    def write(self, data):
        from .ingestion import IngestionService
        if not isinstance(data, (bytes, bytearray, memoryview)) or self.size + len(data) > self.limit:
            raise SourceError('Downloaded attachment exceeds observed size')
        IngestionService._write(self.fd, data)
        self.size += len(data)
        return len(data)

    def flush(self):
        pass


class SourceAdapter:
    def __init__(self, service, client, *, account_scope, flood_wait_types=()):
        self.service, self.client = service, client
        self.account_scope = account_scope
        self.flood_wait_types = tuple(dict.fromkeys((FloodWaitError, FloodPremiumWaitError) + tuple(flood_wait_types)))
        if any(not isinstance(t, type) or not issubclass(t, Exception) for t in self.flood_wait_types):
            raise ValueError('Explicit exception classes required')
        self._sdk_policy()

    def _sdk_policy(self):
        # Telethon 1.45 exposes flood_sleep_threshold publicly; its configured
        # request-retry count has only this nonsecret stored attribute. Fake
        # clients must declare the same bounded policy. Never inspect a session.
        retries = getattr(self.client, '_request_retries', None)
        threshold = getattr(self.client, 'flood_sleep_threshold', None)
        if type(retries) is not int or retries != 0 or type(threshold) is not int or threshold != 0:
            raise SourceError('Injected SDK requires request_retries=0 and flood_sleep_threshold=0')

    async def _ready(self, scope):
        self._sdk_policy()
        if scope != self.account_scope or not self.client.is_connected() or not await self.client.is_user_authorized():
            raise SourceError('Configured connected authorized client required; no automatic login')

    @staticmethod
    def _observation(message, message_id=None):
        if message is None:
            return dict(message_id=message_id, media_index=-1, state='deleted', metadata={})
        document = getattr(message, 'document', None)
        if document is None:
            return dict(message_id=message.id, media_index=-1, state='no_media', metadata={})
        names = [a.file_name for a in document.attributes if isinstance(getattr(a, 'file_name', None), str)]
        if len(names) != 1:
            raise SourceError('One explicit attachment filename required')
        edited = getattr(message, 'edit_date', None)
        metadata = dict(filename=names[0], byte_size=document.size, document_id=document.id,
                        edited_at=int(edited.timestamp()) if edited else 0,
                        grouped_id=getattr(message, 'grouped_id', None))
        result = dict(message_id=message.id, media_index=0, state='attached', metadata=metadata)
        _observations([result])
        return result

    def _scan(self, lease):
        with self.service.jobs.ownership(lease) as conn:
            scan, source = self.service._scan(conn, lease)
            return dict(scan), dict(source)

    def _flood_wait(self, lease, error):
        seconds = getattr(error, 'seconds', None)
        if type(seconds) is not int or seconds < 0:
            raise SourceError('Unrecognized source rate limit') from None
        if seconds <= 86400:
            self.service.jobs.fail(lease, 'source_flood_wait', retry_delay_ms=seconds * 1000)
        else:
            with self.service.jobs._writer() as (conn, _now):
                self.service.jobs._cas(conn, lease,
                    "state='cancelled',lease_owner=NULL,lease_until=NULL,error_code='source_wait_requires_operator'", {})
        raise SourceWait('Source rate limit durably deferred; no in-process retry') from None

    async def scan_page(self, lease):
        with self.service.jobs.ownership(lease) as conn:
            completed = conn.execute(text("SELECT id FROM source_scans WHERE job_id=:id AND state='complete'"),
                                     dict(id=lease.job_id)).scalar()
            receipt = conn.execute(text('SELECT receipt_json FROM source_pages WHERE scan_id=:id ORDER BY cursor_before DESC LIMIT 1'),
                                   dict(id=completed)).scalar() if completed else None
        if completed:
            self.service.finish_scan(lease)
            return json.loads(receipt)
        scan, source = self._scan(lease)
        try:
            await self._ready(source['account_scope'])
            if scan['mode'] == 'events':
                raise SourceError('Event stream requires explicit event cursor; use commit_events')
            if scan['mode'] == 'reconcile':
                page = self.service.reconciliation_page(lease)
                ids = page['message_ids']
                messages = await self.client.get_messages(source['chat_id'], ids=ids) if ids else []
                if len(messages) != len(ids):
                    raise SourceError('SDK returned an incomplete known-history page')
                observations = []
                for ident, message in zip(ids, messages):
                    if message is not None and message.id != ident:
                        raise SourceError('SDK known-history identity mismatch')
                    observations.append(self._observation(message, ident))
                cursor = ids[-1] if ids else scan['upper_bound']
            else:
                observations, last = [], scan['cursor']
                async for message in self.client.iter_messages(source['chat_id'], min_id=scan['cursor'],
                        max_id=scan['upper_bound'] + 1, reverse=True, limit=100):
                    if len(observations) >= 100 or not last < message.id <= scan['upper_bound']:
                        raise SourceError('SDK incremental page boundary mismatch')
                    observations.append(self._observation(message))
                    last = message.id
                cursor = last if len(observations) == 100 else scan['upper_bound']
            receipt = self.service.commit_page(lease, expected_cursor=scan['cursor'], next_cursor=cursor,
                                               observations=observations, done=cursor == scan['upper_bound'])
            if receipt['done']:
                self.service.finish_scan(lease)
            return receipt
        except self.flood_wait_types as error:
            self._flood_wait(lease, error)

    async def commit_events(self, lease, *, next_cursor, observations, done):
        scan, source = self._scan(lease)
        try:
            await self._ready(source['account_scope'])
            if scan['mode'] != 'events':
                raise SourceError('Explicit events scan required')
            receipt = self.service.commit_page(lease, expected_cursor=scan['cursor'], next_cursor=next_cursor,
                                               observations=observations, done=done)
            if done:
                self.service.finish_scan(lease)
            return receipt
        except self.flood_wait_types as error:
            self._flood_wait(lease, error)

    def mark_unproven_recovery(self, source_id, *, config_version):
        # A reconnect or catch_up return alone is not proof of complete recovery.
        self.service.mark_event_gap(source_id, config_version=config_version)

    async def recover_events(self, source_id, *, config_version):
        with self.service.engine.connect() as conn:
            source = self.service._source(conn, source_id, config_version)
        self.mark_unproven_recovery(source_id, config_version=config_version)
        try:
            await self._ready(source['account_scope'])
            await self.client.catch_up()
        except self.flood_wait_types:
            raise SourceWait('Recovery rate limited; reconciliation required, no automatic retry') from None
        # Telethon returning from catch_up does not certify our durable event
        # cursor or every historical attachment. Full reconciliation remains due.
        return {'recovery_state': 'RECONCILING'}

    async def download_to(self, lease, fd):
        item = self.service.download_input(lease)
        expected = json.loads(item['metadata_json'])
        try:
            await self._ready(item['account_scope'])
            messages = await self.client.get_messages(item['chat_id'], ids=[item['message_id']])
            if len(messages) != 1:
                raise SourceError('Observed attachment unavailable')
            message = messages[0]
            actual = self._observation(message, item['message_id'])
            if actual['message_id'] != item['message_id'] or actual['state'] != 'attached' or actual['metadata'] != expected:
                raise SourceError('Attachment identity changed; observe again')
            sink = _DownloadSink(fd, expected['byte_size'])
            await self.client.download_media(message, file=sink)
            after = await self.client.get_messages(item['chat_id'], ids=[item['message_id']])
            if (len(after) != 1 or self._observation(after[0], item['message_id']) != actual
                    or sink.size != expected['byte_size']):
                raise SourceError('Attachment changed during download; observe again')
            self.service.download_input(lease)
            return sink.size
        except self.flood_wait_types as error:
            self._flood_wait(lease, error)
