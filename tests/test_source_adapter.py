"""Injected SDK integration: complete history, restart, identity and FloodWait."""
from collections import defaultdict
from datetime import datetime, timezone
import inspect
import logging
import os
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from telethon.client.telegrambaseclient import TelegramBaseClient
from telethon.client.users import UserMethods
from telethon.errors import FloodWaitError
from telethon.tl.functions.help import GetConfigRequest

from document_library.jobs import JobService, LostLease
from document_library.source_adapter import SourceAdapter, SourceWait
from document_library.sources import SourceError, SourceService
from test_jobs import FakeClock
from upload_helpers import UploadFixture


class SyntheticFloodWait(Exception):
    def __init__(self, seconds):
        self.seconds = seconds


def message(ident, payload=b'abc'):
    return SimpleNamespace(id=ident, grouped_id=None, edit_date=None, payload=payload,
        document=SimpleNamespace(id=ident + 10000, size=len(payload), attributes=[SimpleNamespace(file_name='Book.txt')]))


class FakeSDK:
    def __init__(self, count=1):
        self._request_retries = 0
        self.flood_sleep_threshold = 0
        self.messages = {i: message(i) for i in range(1, count + 1)}
        self.authorized, self.connected = True, True
        self.iter_calls, self.download_calls = 0, 0
        self.wait = None
        self.change_after_download = False
        self.catch_up_calls = 0

    def is_connected(self):
        return self.connected

    async def is_user_authorized(self):
        return self.authorized

    async def start(self, *_args, **_kwargs):
        raise AssertionError('Adapter must never start or log in')

    async def iter_messages(self, chat, *, min_id, max_id, reverse, limit):
        assert chat == -100 and reverse and limit == 100
        self.iter_calls += 1
        if self.wait is not None:
            raise SyntheticFloodWait(self.wait)
        for ident in sorted(i for i in self.messages if min_id < i < max_id)[:limit]:
            yield self.messages[ident]

    async def get_messages(self, chat, *, ids):
        assert chat == -100
        return [self.messages.get(i) for i in ids]

    async def catch_up(self):
        self.catch_up_calls += 1

    async def download_media(self, item, *, file):
        self.download_calls += 1
        file.write(item.payload)
        if self.change_after_download:
            item.edit_date = datetime(2026, 10, 23, tzinfo=timezone.utc)


class OfflineRpcSDK(FakeSDK, UserMethods):
    """Exercise installed RPC handling with synthetic sender/state, no account."""
    def __init__(self):
        super().__init__()
        self._loop = None
        self._raise_last_call_error = True
        self._flood_waited_requests = {}
        self._no_updates = False
        self._log = defaultdict(lambda: logging.getLogger('synthetic-sdk-rpc'))
        self.session = SimpleNamespace(process_entities=lambda _result: None)
        self.rpc_calls = 0

    def send(self, request, ordered=False):
        self.rpc_calls += 1
        async def response():
            raise FloodWaitError(request=request, capture=7)
        return response()

    async def iter_messages(self, *_args, **_kwargs):
        await self._call(self, GetConfigRequest())
        yield message(1)


class SourceAdapterAcceptance(UploadFixture, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.jobs = JobService(self.engine, clock=self.clock)
        self.sources = SourceService(self.auth, self.jobs)
        self.source = self.sources.configure(self.admin.token, self.admin.csrf,
            account_scope='synthetic', chat_id=-100, extensions=['txt'])
        self.client = FakeSDK()
        self.adapter = SourceAdapter(self.sources, self.client, account_scope='synthetic',
                                     flood_wait_types=(SyntheticFloodWait,))

    def scan(self, key='initial', *, mode='incremental', upper=1):
        scan = self.sources.start_scan(self.source, config_version=1, request_key=key,
            mode=mode, upper_bound=None if mode == 'reconcile' else upper)
        lease = self.jobs.claim('sdk-test', kind='source.scan', lease_ms=1000)
        self.assertEqual(lease.job_id, scan['job_id'])
        return scan, lease

    async def discovered_download(self):
        _scan, lease = self.scan()
        await self.adapter.scan_page(lease)
        return self.jobs.claim('sdk-download-test', kind='source.download', lease_ms=1000)

    async def test_1005_messages_page_restart_and_full_history_revalidation(self):
        self.client = FakeSDK(1005)
        self.adapter.client = self.client
        scan, old = self.scan(upper=1005)
        for _ in range(3):
            self.assertFalse((await self.adapter.scan_page(old))['done'])
        self.assertEqual(self.sources.get_scan(scan['id'])['cursor'], 300)
        self.clock.advance(1000)
        fresh = self.jobs.claim('sdk-restarted', kind='source.scan', lease_ms=1000)
        with self.assertRaises(LostLease):
            await self.adapter.scan_page(old)
        while not (await self.adapter.scan_page(fresh))['done']:
            pass
        self.assertEqual(self.scalar('SELECT high_water FROM ingestion_sources'), 1005)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_observations'), 1005)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='source.download'"), 1005)
        # The first historical message is changed and the last one deleted.
        self.client.messages[1] = message(1, b'xyz')
        del self.client.messages[1005]
        self.adapter.mark_unproven_recovery(self.source, config_version=1)
        _scan, lease = self.scan('history', mode='reconcile')
        while not (await self.adapter.scan_page(lease))['done']:
            pass
        self.assertEqual(self.scalar('SELECT count(*) FROM source_observations'), 2010)
        self.assertEqual(self.scalar("SELECT count(*) FROM source_observations WHERE state='deleted'"), 1)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'), 'RECONCILING')

    async def test_flood_wait_is_durable_without_retry_or_cursor_progress(self):
        scan, lease = self.scan()
        self.client.wait = 7
        with self.assertRaises(SourceWait):
            await self.adapter.scan_page(lease)
        job = self.jobs.get(lease.job_id)
        self.assertEqual(job['state'], 'retry_wait')
        self.assertEqual(job['next_run_at'], self.clock.wall + 7000)
        self.assertEqual(self.client.iter_calls, 1)
        self.assertEqual(self.sources.get_scan(scan['id'])['cursor'], 0)
        self.assertIsNone(self.jobs.claim('too-early', kind='source.scan'))
        self.clock.advance(7000)
        self.client.wait = None
        resumed = self.jobs.claim('after-wait', kind='source.scan')
        self.assertTrue((await self.adapter.scan_page(resumed))['done'])

    async def test_default_sdk_wait_policy_is_rejected_before_any_rpc(self):
        client = OfflineRpcSDK()
        defaults = inspect.signature(TelegramBaseClient.__init__).parameters
        client._request_retries = defaults['request_retries'].default
        client.flood_sleep_threshold = defaults['flood_sleep_threshold'].default
        with self.assertRaisesRegex(SourceError, 'request_retries=0'):
            SourceAdapter(self.sources, client, account_scope='synthetic')
        self.assertEqual(client.rpc_calls, 0)

    async def test_actual_sdk_floodwait_uses_durable_delay_without_sdk_sleep(self):
        scan, lease = self.scan()
        client = OfflineRpcSDK()
        adapter = SourceAdapter(self.sources, client, account_scope='synthetic')
        with patch('telethon.client.users.asyncio.sleep', side_effect=AssertionError('SDK must not sleep for FloodWait')):
            with self.assertRaises(SourceWait):
                await adapter.scan_page(lease)
        self.assertEqual(client.rpc_calls, 1)
        self.assertEqual(self.sources.get_scan(scan['id'])['cursor'], 0)
        self.assertEqual(self.jobs.get(lease.job_id)['state'], 'retry_wait')
        self.assertEqual(self.jobs.get(lease.job_id)['next_run_at'], self.clock.wall + 7000)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_pages'), 0)
        self.assertIsNone(self.jobs.claim('rpc-too-early', kind='source.scan'))
        self.clock.advance(7000)
        resumed = self.jobs.claim('rpc-after-wait', kind='source.scan')
        self.assertEqual(resumed.job_id, lease.job_id)
        self.assertGreater(resumed.generation, lease.generation)

    async def test_policy_mutation_is_rejected_before_authorization_rpc(self):
        _scan, lease = self.scan()
        self.client.flood_sleep_threshold = 60
        with patch.object(self.client, 'is_user_authorized', side_effect=AssertionError('policy must be checked before SDK')):
            with self.assertRaises(SourceError):
                await self.adapter.scan_page(lease)
        self.assertEqual(self.client.iter_calls, 0)

    async def test_actual_floodwait_during_authorization_is_durably_deferred(self):
        scan, lease = self.scan()
        adapter = SourceAdapter(self.sources, self.client, account_scope='synthetic')
        async def wait_on_authorization():
            raise FloodWaitError(request=None, capture=9)
        self.client.is_user_authorized = wait_on_authorization
        with self.assertRaises(SourceWait):
            await adapter.scan_page(lease)
        self.assertEqual(self.jobs.get(lease.job_id)['next_run_at'], self.clock.wall + 9000)
        self.assertEqual(self.sources.get_scan(scan['id'])['cursor'], 0)

    async def test_missing_authorization_and_wrong_account_never_auto_login(self):
        _scan, lease = self.scan()
        self.client.authorized = False
        with self.assertRaises(SourceError):
            await self.adapter.scan_page(lease)
        self.client.authorized = True
        self.adapter.account_scope = 'different-account'
        with self.assertRaises(SourceError):
            await self.adapter.scan_page(lease)
        self.assertEqual(self.client.iter_calls, 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_pages'), 0)

    async def test_lost_last_page_reply_resumes_finish_without_new_network(self):
        _scan, lease = self.scan()
        with patch.object(self.sources, 'finish_scan', side_effect=RuntimeError('synthetic worker stop')):
            with self.assertRaises(RuntimeError):
                await self.adapter.scan_page(lease)
        self.clock.advance(1000)
        resumed = self.jobs.claim('finish-resume', kind='source.scan')
        self.assertTrue((await self.adapter.scan_page(resumed))['done'])
        self.assertEqual(self.client.iter_calls, 1)
        self.assertEqual(self.jobs.get(lease.job_id)['state'], 'completed')

    async def test_download_is_bounded_and_rechecks_observed_identity(self):
        lease = await self.discovered_download()
        with tempfile.TemporaryFile() as output:
            self.assertEqual(await self.adapter.download_to(lease, output.fileno()), 3)
            output.seek(0)
            self.assertEqual(output.read(), b'abc')
        self.client.messages[1].document.id += 1
        with tempfile.TemporaryFile() as output:
            with self.assertRaises(SourceError):
                await self.adapter.download_to(lease, output.fileno())
            self.assertEqual(os.fstat(output.fileno()).st_size, 0)
        self.assertEqual(self.client.download_calls, 1)

    async def test_download_change_after_io_and_oversize_leave_no_receipt(self):
        lease = await self.discovered_download()
        self.client.change_after_download = True
        with tempfile.TemporaryFile() as output:
            with self.assertRaises(SourceError):
                await self.adapter.download_to(lease, output.fileno())
        self.client.change_after_download = False
        self.client.messages[1] = message(1, b'abc')
        self.client.messages[1].payload = b'oversize'
        with tempfile.TemporaryFile() as output:
            with self.assertRaises(SourceError):
                await self.adapter.download_to(lease, output.fileno())
            self.assertEqual(os.fstat(output.fileno()).st_size, 0)
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 0)

    async def test_explicit_catch_up_preserves_reconciliation_until_content_proof(self):
        result = await self.adapter.recover_events(self.source, config_version=1)
        self.assertEqual(result['recovery_state'], 'RECONCILING')
        self.assertEqual(self.client.catch_up_calls, 1)
        self.assertEqual(self.scalar('SELECT event_epoch FROM ingestion_sources'), 1)
        self.assertEqual(self.scalar('SELECT recovery_state FROM ingestion_sources'), 'RECONCILING')

    async def test_download_floodwait_does_not_claim_content_or_retry(self):
        lease = await self.discovered_download()
        async def limited(*_args, **_kwargs):
            raise SyntheticFloodWait(12)
        self.client.get_messages = limited
        with tempfile.TemporaryFile() as output:
            with self.assertRaises(SourceWait):
                await self.adapter.download_to(lease, output.fileno())
        self.assertEqual(self.jobs.get(lease.job_id)['state'], 'retry_wait')
        self.assertEqual(self.scalar('SELECT count(*) FROM source_content_receipts'), 0)


if __name__ == '__main__':
    unittest.main()
