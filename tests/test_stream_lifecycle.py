"""Actual lazy ASGI invocation, native launch and observable cleanup failures."""
import asyncio
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from document_library.library_api import error_response
from document_library.stream_lifecycle import LazyOwnedResponse, StreamSupervisor
from document_library.streaming import OwnedProducer, OwnedResponse, PreparedTransfer, StreamAdmission


class _Downloads:
    def __init__(self):
        self.calls = 0
    def pending_finalizations(self):
        return 0
    def retry_pending(self):
        return dict(pending=0,retried=0,failed=0)
    def close(self, *, wait):
        assert wait is False
        self.calls += 1
        return True


class LazyLifetimeAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='g2e-owned-response-')
        self.path = Path(self.temp.name)/'synthetic.bin'
        self.path.write_bytes(b'x'*65536*8)
        self.closed = threading.Event()
        self.fds, self.created = [], []
        self.gate = StreamAdmission(max_producers=4,reserve_fds=1,transient_fds=1,count_fds=lambda:4,soft_limit=4096)
        self.downloads = _Downloads()
        self.supervisor = StreamSupervisor(downloads=self.downloads,admissions=(self.gate,),settle_rounds=8)

    async def asyncTearDown(self):
        for handle in self.created:
            handle.request_stop('test_cleanup')
        for _ in range(200):
            if all(self.supervisor.stopped(handle) for handle in self.created):
                break
            await asyncio.sleep(0.002)
        self.assertTrue(all(self.supervisor.stopped(handle) for handle in self.created))
        for response in tuple(self.supervisor._responses):
            self.supervisor.observe(response)
        tasks = tuple(self.supervisor._tasks)
        if tasks:
            await asyncio.gather(*tasks,return_exceptions=True)
        self.temp.cleanup()
        self.assertFalse(self.path.exists())

    def factory(self, *, closed=None, fail_after_start=False):
        @contextmanager
        def opened():
            fd = os.open(self.path,os.O_RDONLY)
            self.fds.append(fd)
            class Selected:
                def revalidate(self):
                    pass
            try:
                yield Selected()
            finally:
                os.close(fd)
                self.closed.set()
        def produce(selected, transfer):
            for _ in range(8):
                yield b'x'*65536
        def register(handle):
            self.created.append(handle)
            self.supervisor.track(handle)
        handle = OwnedProducer.start(open_selected=opened,prepare=lambda _:PreparedTransfer(200,()),
            produce=produce,unique_originals=1,admission=self.gate,on_created=register)
        if fail_after_start:
            raise RuntimeError('synthetic factory error after launch')
        return OwnedResponse(handle,on_closed=closed)

    def lazy(self, **kwargs):
        return LazyOwnedResponse(lambda:self.factory(**kwargs),supervisor=self.supervisor,error_response=error_response)

    def held_factory(self, entered, release, **kwargs):
        response = self.factory(**kwargs)
        entered.set()
        release.wait(2)
        return response

    async def call(self, response, *, send=None, receive=None):
        messages = []
        async def output(message):
            if send:
                await send(message)
            messages.append(message)
        async def never():
            await asyncio.Event().wait()
        await response({'type':'http','method':'GET'},receive or never,output)
        return messages

    async def test_uncalled_and_cancelled_before_call_create_no_handle_or_fd(self):
        response = self.lazy()
        self.assertEqual(self.created,[])
        async def route():
            await asyncio.sleep(10)
            await self.call(response)
        task = asyncio.create_task(route())
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.created,[])
        self.assertEqual(self.gate.usage()['producers'],0)
        self.assertTrue((await self.supervisor.shutdown())['clean'])

    async def test_first_await_cancellation_closes_actual_reader(self):
        original = self.factory
        loop = asyncio.get_running_loop()
        def cancelled_factory():
            response = original()
            loop.call_soon_threadsafe(task.cancel)
            return response
        response = LazyOwnedResponse(cancelled_factory,supervisor=self.supervisor,error_response=error_response)
        task = asyncio.create_task(self.call(response))
        with self.assertRaises(asyncio.CancelledError):
            await task
        result = await self.supervisor.shutdown()
        self.assertTrue(result['clean'],result)
        self.assertTrue(self.closed.is_set())
        self.assertEqual(self.gate.usage()['producers'],0)

    async def test_blocking_factory_keeps_event_loop_responsive(self):
        entered,release,returned = threading.Event(),threading.Event(),threading.Event()
        thread_ids = []
        def blocked_factory():
            thread_ids.append(threading.get_ident())
            entered.set()
            release.wait(2)
            returned.set()
            return self.factory()
        response = LazyOwnedResponse(blocked_factory,supervisor=self.supervisor,error_response=error_response)
        task = asyncio.create_task(self.call(response))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            await asyncio.sleep(0)
            self.assertFalse(returned.is_set())
            self.assertNotEqual(thread_ids,[threading.get_ident()])
            self.assertEqual(self.supervisor.health()['inflight_operations'],1)
        finally:
            release.set()
            messages = await task
        self.assertEqual(messages[0]['status'],200)
        self.assertTrue((await self.supervisor.shutdown())['clean'])

    async def test_cancelled_factory_late_response_finalizes_before_owner_close(self):
        entered,release = threading.Event(),threading.Event()
        finalized = asyncio.Event()
        receipts,messages = [],[]
        async def cleanup(ack,success,sent):
            receipts.append((ack,success,sent,self.closed.is_set(),self.created[0].wait_exited(0)))
            finalized.set()
        async def send(message):
            messages.append(message)
        task = asyncio.create_task(self.call(LazyOwnedResponse(lambda:self.held_factory(entered,release,closed=cleanup),
            supervisor=self.supervisor,error_response=error_response),send=send))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            result = await self.supervisor.shutdown()
            self.assertFalse(result['clean'])
            self.assertFalse(result['owner_closed'])
            self.assertEqual(result['inflight_operations'],1)
            self.assertEqual(self.downloads.calls,0)
        finally:
            release.set()
        await asyncio.wait_for(finalized.wait(),2)
        self.assertEqual(messages,[])
        self.assertEqual(len(receipts),1)
        self.assertEqual(receipts[0][1:],(False,0,True,True))
        self.assertTrue(receipts[0][0].stopped)
        self.assertTrue((await self.supervisor.shutdown())['clean'])
        self.assertEqual(self.downloads.calls,1)
        self.assertEqual(self.gate.usage()['producers'],0)

    async def test_cancelled_factory_later_launch_error_keeps_created_handle_owned(self):
        entered,release = threading.Event(),threading.Event()
        def blocked_factory():
            entered.set()
            release.wait(2)
            return self.factory(fail_after_start=True)
        task = asyncio.create_task(self.call(LazyOwnedResponse(blocked_factory,
            supervisor=self.supervisor,error_response=error_response)))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.created,[])
            self.assertEqual(self.supervisor.health()['inflight_operations'],1)
        finally:
            release.set()
        for _ in range(200):
            if self.supervisor.health()['inflight_operations']==0:
                break
            await asyncio.sleep(0.002)
        self.assertEqual(len(self.created),1)
        self.assertTrue(self.created[0].stop.is_set())
        self.assertTrue((await self.supervisor.shutdown())['clean'])
        self.assertTrue(self.closed.is_set())

    async def test_late_response_cleanup_failure_is_observed(self):
        entered,release = threading.Event(),threading.Event()
        async def cleanup(*_):
            raise RuntimeError('synthetic late cleanup failure')
        task = asyncio.create_task(self.call(LazyOwnedResponse(lambda:self.held_factory(entered,release,closed=cleanup),
            supervisor=self.supervisor,error_response=error_response)))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        finally:
            release.set()
        for _ in range(200):
            state = self.supervisor.health()
            if not any(state[key] for key in ('inflight_operations','cleanup_tasks','unfinalized_responses')):
                break
            await asyncio.sleep(0.002)
        result = await self.supervisor.shutdown()
        self.assertFalse(result['clean'])
        self.assertEqual(result['cleanup_failures'],1)
        self.assertEqual(result['unresolved_handles'],0)

    async def test_after_launch_factory_error_remains_owned_and_stopped(self):
        messages = await self.call(self.lazy(fail_after_start=True))
        self.assertEqual(messages[0]['status'],500)
        self.assertTrue(self.created[0].stop.is_set())
        self.assertTrue((await self.supervisor.shutdown())['clean'])
        self.assertTrue(self.closed.is_set())

    async def test_disconnect_when_queue_full_closes_reader(self):
        waiting = asyncio.Event()
        async def send(message):
            if message['type']=='http.response.start':
                for _ in range(200):
                    if self.created[0].queue_size==2:
                        break
                    await asyncio.sleep(0.002)
                self.assertEqual(self.created[0].queue_size,2)
                waiting.set()
                await asyncio.Event().wait()
        async def receive():
            await waiting.wait()
            return {'type':'http.disconnect'}
        task = asyncio.create_task(self.call(self.lazy(),send=send,receive=receive))
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue((await self.supervisor.shutdown())['clean'])
        self.assertTrue(self.closed.is_set())

    async def test_cleanup_failure_is_observed_and_primary_send_error_survives(self):
        async def cleanup(*_):
            raise RuntimeError('synthetic cleanup exception')
        primary = OSError('synthetic transport exception')
        async def send(message):
            if message['type']=='http.response.body':
                raise primary
        with self.assertRaises(OSError) as failure:
            await self.call(self.lazy(closed=cleanup),send=send)
        self.assertIs(failure.exception,primary)
        await asyncio.sleep(0)
        result = await self.supervisor.shutdown()
        self.assertFalse(result['clean'])
        self.assertEqual(result['cleanup_failures'],1)
        self.assertEqual(result['unresolved_handles'],0)

    async def test_second_cancel_keeps_detached_cleanup_strongly_observed(self):
        entered,release = asyncio.Event(),asyncio.Event()
        async def cleanup(*_):
            entered.set()
            await release.wait()
            raise RuntimeError('synthetic detached cleanup exception')
        loop = asyncio.get_running_loop()
        previous, detached_errors = loop.get_exception_handler(), []
        loop.set_exception_handler(lambda _loop,context:detached_errors.append(context))
        try:
            task = asyncio.create_task(self.call(self.lazy(closed=cleanup)))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.supervisor.health()['cleanup_tasks'],1)
            release.set()
            await asyncio.gather(*tuple(self.supervisor._tasks),return_exceptions=True)
            await asyncio.sleep(0)
            self.assertEqual(detached_errors,[])
            self.assertEqual(self.supervisor.health()['cleanup_failures'],1)
            self.assertFalse((await self.supervisor.shutdown())['clean'])
        finally:
            release.set()
            loop.set_exception_handler(previous)

    async def test_unknown_native_launch_false_alive_is_unresolved_until_real_exit(self):
        pending = []
        real_start = threading.Thread.start
        def unknown(thread):
            if thread.name != 'library-original-producer':
                return real_start(thread)
            pending.append(thread)
            raise RuntimeError('synthetic native launch ambiguity')
        with patch.object(threading.Thread,'start',unknown):
            messages = await self.call(self.lazy())
        self.assertEqual(messages[0]['status'],500)
        handle = self.created[0]
        self.assertFalse(handle.thread.is_alive())
        self.assertIsNone(handle.thread.ident)
        self.assertFalse(handle.never_started.is_set())
        self.assertEqual(self.supervisor.health()['unresolved_handles'],1)
        result = await self.supervisor.shutdown()
        self.assertFalse(result['clean'])
        self.assertEqual(result['unresolved_handles'],1)
        self.assertEqual(self.gate.usage()['producers'],1)
        # Fixture-only ground truth: launch delayed native thread, never fake ACK.
        real_start(pending[0])
        for _ in range(200):
            if self.supervisor.health()['unresolved_handles']==0:
                break
            await asyncio.sleep(0.002)
        self.assertEqual(self.supervisor.health()['unresolved_handles'],0)

    async def test_no_second_headers_after_post_start_producer_failure(self):
        @contextmanager
        def opened():
            class Selected:
                def revalidate(self):
                    pass
            yield Selected()
        def produce(*_):
            yield b'first'
            raise RuntimeError('synthetic midstream failure')
        def factory():
            handle = OwnedProducer.start(open_selected=opened,prepare=lambda _:PreparedTransfer(200,()),produce=produce,
                unique_originals=1,admission=self.gate,on_created=self.supervisor.track)
            self.created.append(handle)
            return OwnedResponse(handle)
        messages=[]
        async def send(message):
            messages.append(message)
        with self.assertRaises(RuntimeError):
            await self.call(LazyOwnedResponse(factory,supervisor=self.supervisor,error_response=error_response),send=send)
        self.assertEqual(sum(item['type']=='http.response.start' for item in messages),1)
        self.assertEqual(messages[0]['status'],200)
        self.assertTrue((await self.supervisor.shutdown())['clean'])

    async def test_shutdown_waits_for_actual_callback_before_owner_close(self):
        entered,release = asyncio.Event(),asyncio.Event()
        async def cleanup(*_):
            entered.set()
            await release.wait()
        task = asyncio.create_task(self.call(self.lazy(closed=cleanup)))
        await entered.wait()
        try:
            result = await self.supervisor.shutdown()
            self.assertFalse(result['clean'])
            self.assertFalse(result['owner_closed'])
            self.assertEqual(self.downloads.calls,0)
            self.assertEqual(result['inflight_operations'],1)
        finally:
            release.set()
        await task
        self.assertTrue((await self.supervisor.shutdown())['clean'])
        self.assertEqual(self.downloads.calls,1)

    async def test_cancelled_retry_awaiter_retains_actual_worker_until_done(self):
        entered,release = threading.Event(),threading.Event()
        def retry():
            entered.set()
            release.wait()
            return dict(pending=0,retried=0,failed=0)
        self.downloads.retry_pending = retry
        task = asyncio.create_task(self.supervisor.shutdown())
        try:
            for _ in range(200):
                if entered.is_set():
                    break
                await asyncio.sleep(0.002)
            self.assertTrue(entered.is_set())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(self.supervisor.health()['inflight_operations'],1)
            self.assertEqual(self.downloads.calls,0)
        finally:
            release.set()
        for _ in range(200):
            if self.supervisor.health()['inflight_operations']==0:
                break
            await asyncio.sleep(0.002)
        self.assertEqual(self.supervisor.health()['inflight_operations'],0)
        self.assertTrue((await self.supervisor.shutdown())['clean'])

    async def test_missing_finalization_seam_cannot_report_clean_or_close_owner(self):
        with patch.object(self.downloads,'retry_pending',None):
            result = await self.supervisor.shutdown()
        self.assertFalse(result['clean'])
        self.assertFalse(result['finalization_seam_ready'])
        self.assertEqual(self.downloads.calls,0)
