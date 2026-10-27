"""Actual owning threads, ASGI barriers, FD budgets and bounded original bodies."""
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

from document_library.streaming import (AdmissionError, OwnedProducer, OwnedResponse,
    PreparedTransfer, RangeError, StreamAdmission, StreamError, NeverStartedAck,
    original_response, parse_single_range, _fd_count)
from library_helpers import LibraryFixture


async def call_response(response, *, send_hook=None):
    messages = []
    async def send(message):
        if send_hook:
            await send_hook(message)
        messages.append(message)
    async def receive():
        await asyncio.Event().wait()
    await response({'type':'http','asgi':{'spec_version':'2.0'}},receive,send)
    return messages


def run_response(response, **kwargs):
    return asyncio.run(call_response(response,**kwargs))


class RangeAcceptance(unittest.TestCase):
    def test_closed_open_suffix_and_clamping(self):
        for raw,start,length in [('bytes=0-0',0,1),('bytes=9-',9,1),('bytes=3-99',3,7),
                                  ('bytes=-3',7,3),('bytes=-99',0,10)]:
            with self.subTest(raw=raw):
                got = parse_single_range('GET',raw,None,10,'"abc"')
                self.assertEqual((got.status,got.start,got.length),(206,start,length))
                self.assertEqual(got.content_range,f'bytes {start}-{start+length-1}/10')

    def test_invalid_multiple_duplicate_and_unsatisfiable(self):
        for raw,status,size in [('bytes=10-',416,10),('bytes=-0',416,10),('bytes=0-',416,0),
                                ('bytes=2-1',400,10),('bytes=-',400,10),('bytes=0-1,4-5',400,10),
                                (['bytes=0-1','bytes=2-3'],400,10),('bytes=one-two',400,10),
                                ('bytes='+'1'*100+'-',400,10)]:
            with self.subTest(raw=raw):
                with self.assertRaises(RangeError) as failure:
                    parse_single_range('GET',raw,None,size,'"abc"')
                self.assertEqual(failure.exception.status,status)

    def test_head_unknown_unit_and_strong_if_range(self):
        for method,raw,condition,status in [('HEAD','bytes=broken','bad',200),('GET','items=bad',None,200),
            ('GET','bytes=1-2','"abc"',206),('GET','bytes=1-2','W/"abc"',200),
            ('GET','bytes=1-2','"other"',200),('GET','bytes=1-2','Wed, 21 Oct 2015 07:28:00 GMT',200),
            ('GET',None,'"abc"',200)]:
            with self.subTest(method=method,raw=raw,condition=condition):
                self.assertEqual(parse_single_range(method,raw,condition,10,'"abc"').status,status)


class ProducerAcceptance(unittest.TestCase):
    def gate(self, **kwargs):
        return StreamAdmission(count_fds=lambda:10,soft_limit=4096,**kwargs)

    def producer(self, chunks, *, gate=None, prepare=None, after_close=None, open_selected=None, produce=None,
                 on_created=None):
        @contextmanager
        def opened():
            class Selected:
                def revalidate(self):
                    pass
            yield Selected()
        return OwnedProducer.start(open_selected=open_selected or opened,
            prepare=prepare or (lambda _:PreparedTransfer(200,())),produce=produce or (lambda *_:iter(chunks)),
            unique_originals=1,admission=gate or self.gate(),after_close=after_close,on_created=on_created)

    def test_registered_callback_failure_is_explicitly_never_started(self):
        gate,registered = self.gate(max_producers=1),[]
        def register_then_fail(handle):
            registered.append(handle)
            raise MemoryError('synthetic registration failure after tracking')
        with self.assertRaises(MemoryError):
            self.producer([],gate=gate,on_created=register_then_fail)
        handle = registered[0]
        self.assertTrue(handle.never_started.is_set())
        self.assertFalse(handle.closed.is_set())
        self.assertIsInstance(handle.wait_closed(0),NeverStartedAck)
        self.assertTrue(handle.wait_exited(0))
        self.assertEqual(gate.usage(),dict(producers=0,reserved_fds=0))
        self.assertEqual(gate.failed_start_handles(),())
        next_handle = self.producer([],gate=gate)
        next_handle.request_stop()
        self.assertIsNotNone(next_handle.wait_closed(2))
        self.assertTrue(next_handle.wait_exited(2))

    def test_unknown_start_keeps_handle_when_native_thread_is_not_alive_yet(self):
        gate,registered = self.gate(max_producers=1),[]
        real_start = threading.Thread.start
        # The product cannot know whether this call launched. Only this fixture
        # knows its injected function never called real_start, allowing recovery.
        def uncertain_start(_thread):
            raise KeyboardInterrupt('synthetic indeterminate native start')
        try:
            with patch.object(threading.Thread,'start',uncertain_start):
                with self.assertRaises(KeyboardInterrupt):
                    self.producer([],gate=gate,on_created=registered.append)
            handle = registered[0]
            self.assertFalse(handle.thread.is_alive())
            self.assertIsNone(handle.thread.ident)
            self.assertTrue(handle.stop.is_set())
            self.assertFalse(handle.never_started.is_set())
            self.assertFalse(handle.closed.is_set())
            self.assertFalse(handle.wait_exited(0))
            self.assertIsNone(handle.wait_closed(0))
            self.assertEqual(gate.failed_start_handles(),(handle,))
            self.assertEqual(gate.usage()['producers'],1)
            self.assertGreater(gate.usage()['reserved_fds'],0)
            with self.assertRaises(AdmissionError):
                gate.reserve(unique_originals=1)
        finally:
            if registered:
                handle = registered[0]
                # Fixture-only ground truth: real Thread.start has not run.
                # Production never retries an indeterminate start this way.
                real_start(handle.thread)
                self.assertIsNotNone(handle.wait_closed(2))
                self.assertTrue(handle.wait_exited(2))
        self.assertEqual(gate.failed_start_handles(),())
        self.assertEqual(gate.usage(),dict(producers=0,reserved_fds=0))

    def test_two_slots_backpressure_and_full_queue_stop(self):
        gate = self.gate(max_producers=1)
        handle = self.producer([b'x'*65536]*8,gate=gate)
        self.assertTrue(handle.prepared.wait(2)); handle.proceed()
        for _ in range(200):
            if handle.queue_size == 2:
                break
            time.sleep(.005)
        self.assertEqual(handle.queue_size,2)
        self.assertFalse(handle.closed.is_set())
        handle.request_stop()
        self.assertIsNotNone(handle.wait_closed(2));self.assertTrue(handle.wait_exited(2))
        self.assertLessEqual(handle.ack.produced_bytes,3*65536)
        self.assertEqual(gate.usage()['producers'],0)

    def test_cancel_during_blocked_io_keeps_charge_until_real_close(self):
        gate,entered,release = self.gate(max_producers=1),threading.Event(),threading.Event()
        @contextmanager
        def opened():
            entered.set();release.wait(5)
            class Selected:
                def revalidate(self):
                    pass
            yield Selected()
        handle = self.producer([],gate=gate,open_selected=opened)
        self.assertTrue(entered.wait(2));handle.request_stop()
        self.assertIsNone(handle.wait_closed(.02))
        self.assertGreater(gate.usage()['reserved_fds'],0)
        with self.assertRaises(AdmissionError):
            gate.reserve(unique_originals=1)
        release.set();self.assertIsNotNone(handle.wait_closed(2));self.assertTrue(handle.wait_exited(2))
        self.assertEqual(gate.usage()['reserved_fds'],0)

    def test_close_ack_does_not_release_live_thread_slot(self):
        gate,epilogue,release = self.gate(max_producers=1),threading.Event(),threading.Event()
        def after_close():
            epilogue.set();release.wait(5)
        handle = self.producer([],gate=gate,after_close=after_close)
        handle.proceed();self.assertTrue(epilogue.wait(2))
        self.assertTrue(handle.closed.is_set());self.assertTrue(handle.thread.is_alive())
        self.assertEqual(gate.usage()['reserved_fds'],0)
        with self.assertRaises(AdmissionError):
            gate.reserve(unique_originals=1)
        release.set();self.assertTrue(handle.wait_exited(2))
        self.assertEqual(gate.usage()['producers'],0)

    def test_last_put_after_empty_get_is_drained_before_eof(self):
        release = threading.Event()
        def produce(*_):
            release.wait(5)
            yield b'last packet'
        handle = self.producer((),produce=produce)
        original_take,first = handle.take_nowait,True
        def race():
            nonlocal first
            got = original_take()
            if first and got is None:
                first = False
                release.set()
                self.assertTrue(handle.closed.wait(2))
                return None
            return got
        handle.take_nowait = race
        messages = run_response(OwnedResponse(handle))
        self.assertEqual(b''.join(m.get('body',b'') for m in messages),b'last packet')
        self.assertFalse(messages[-1]['more_body'])

    def test_send_failure_counts_only_successful_asgi_accepts(self):
        reports = []
        async def closed(ack,success,bytes_sent):
            reports.append((ack,success,bytes_sent))
        accepted = 0
        async def send(message):
            nonlocal accepted
            if message.get('body'):
                accepted += 1
                if accepted == 2:
                    raise OSError('synthetic disconnected socket')
        handle = self.producer([b'first',b'second',b'third'])
        with self.assertRaises(OSError):
            run_response(OwnedResponse(handle,on_closed=closed),send_hook=send)
        ack,success,amount = reports[0]
        self.assertFalse(success);self.assertEqual(amount,len(b'first'))
        self.assertGreaterEqual(ack.produced_bytes,amount)
        self.assertFalse(handle.thread.is_alive())

    def test_disconnect_and_coroutine_cancellation_observe_final_close(self):
        async def exercise(disconnect):
            handle = self.producer([b'x'*65536]*100)
            response = OwnedResponse(handle)
            entered = asyncio.Event()
            async def receive():
                await entered.wait()
                if disconnect:
                    return {'type':'http.disconnect'}
                await asyncio.Event().wait()
            async def send(message):
                if message.get('body'):
                    entered.set()
                    await asyncio.Event().wait()
            task = asyncio.create_task(response({'type':'http'},receive,send))
            await entered.wait()
            if not disconnect:
                task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            if response.cleanup_task:
                await response.cleanup_task
            self.assertTrue(handle.closed.is_set());self.assertFalse(handle.thread.is_alive())
        for disconnect in (True,False):
            asyncio.run(exercise(disconnect))

    def test_preflight_and_context_exit_errors_send_no_headers(self):
        for stage in ('open','prepare','exit'):
            owner = []
            @contextmanager
            def opened():
                owner.append(threading.get_ident())
                if stage == 'open':
                    raise OSError('synthetic setup failure')
                class Selected:
                    def revalidate(self):
                        pass
                try:
                    yield Selected()
                finally:
                    owner.append(threading.get_ident())
                    if stage == 'exit':
                        raise OSError('synthetic exit failure')
            def prepare(_):
                if stage == 'prepare':
                    raise OSError('synthetic preflight failure')
                return PreparedTransfer(200,())
            handle = self.producer([],prepare=prepare,open_selected=opened)
            if stage == 'exit':
                handle.proceed();self.assertIsNotNone(handle.wait_closed(2))
            with self.subTest(stage=stage), self.assertRaises(OSError):
                run_response(OwnedResponse(handle))
            self.assertEqual(len(set(owner)),1)
            self.assertFalse(handle.thread.is_alive())


class OriginalHttpAcceptance(LibraryFixture):
    def test_original_response_registers_ownership_before_native_launch(self):
        revision,_,_ = self.first()
        registered = []
        def register(handle):
            self.assertFalse(handle.thread.is_alive())
            self.assertFalse(handle.never_started.is_set())
            registered.append(handle)
        response = original_response(self.publisher,lambda:self.resolve(revision),on_created=register)
        self.assertEqual(registered,[response.handle])
        messages = run_response(response)
        self.assertEqual(messages[0]['status'],200)
        self.assertFalse(response.handle.thread.is_alive())

    def test_full_head_range_and_attachment_pdf_preview_purpose(self):
        previous = None
        for position,format in enumerate(('txt','html','pdf')):
            revision,_,asset = self.publish(b'0123456789',format=format,previous=previous)
            previous = revision
            fresh = lambda:self.resolve(revision,positions=(position,))
            for method,raw,status,body in [('GET',None,200,b'0123456789'),('GET','bytes=2-4',206,b'234'),
                                          ('HEAD','bytes=garbage',200,b''),('GET','bytes=99-',416,b''),('GET','bytes=1-2,3-4',400,b'')]:
                with self.subTest(format=format,method=method,raw=raw):
                    messages = run_response(original_response(self.publisher,fresh,method=method,range_value=raw))
                    self.assertEqual(messages[0]['status'],status)
                    self.assertEqual(b''.join(m.get('body',b'') for m in messages),body)
                    headers = dict(messages[0]['headers'])
                    if status in (200,206):
                        self.assertTrue(headers[b'content-disposition'].startswith(b'attachment;'))
                        self.assertEqual(headers[b'etag'],('"'+asset['sha256']+'"').encode())
                        self.assertEqual(headers[b'content-length'],b'10' if method == 'HEAD' else str(len(body)).encode())
            messages = run_response(original_response(self.publisher,fresh,inline=True))
            self.assertEqual(dict(messages[0]['headers'])[b'content-disposition'].startswith(b'inline;'),format == 'pdf')

    def test_unauthorized_range_preflight_has_no_headers(self):
        revision,_,_ = self.first()
        with self.assertRaises(Exception):
            original_response(self.publisher,lambda:self.resolve(revision,token='synthetic-invalid'),range_value='bytes=999-')

    def test_ticket_head_does_not_consume_get(self):
        revision,_,_ = self.first()
        ticket = self.ticket(revision)
        def fresh():
            with self.engine.connect() as conn:
                return self.policy.resolve_selection_in(conn,self.token,self.policy.selections_for_ticket(ticket),policy='ticket',ticket_snapshot=ticket)
        messages = run_response(original_response(self.publisher,fresh,method='HEAD'))
        self.assertEqual(messages[0]['status'],200)
        self.assertEqual(self.scalar('SELECT state FROM download_tickets'),'issued')
        self.assertEqual(self.scalar('SELECT count(*) FROM download_attempts'),0)


class PhysicalProcessAcceptance(unittest.TestCase):
    def run_child(self, mode):
        child = Path(__file__).with_name('test_streaming.py')
        result = subprocess.run([sys.executable,str(child),'--physical',mode],capture_output=True,text=True,timeout=180)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        data = json.loads(result.stdout.splitlines()[-1])
        print('PHYSICAL_METRIC '+json.dumps(dict(mode=mode,**data)),flush=True)
        return data

    def test_actual_1024_budget_and_partial_emfile_cleanup(self):
        data = self.run_child('1024')
        self.assertGreaterEqual(data['highwater_delta'],800)
        self.assertLess(data['highwater_delta'],850)
        self.assertTrue(data['second_rejected'])
        error = self.run_child('emfile')
        self.assertEqual(error['errno'],24)
        self.assertLessEqual(error['after'],error['before']+1)

    def test_actual_4096_aggregate_and_thread_count(self):
        data = self.run_child('4096')
        self.assertEqual(data['producers'],2)
        self.assertTrue(data['third_rejected'])
        self.assertGreaterEqual(data['highwater_delta'],1600)

    def test_one_gib_sparse_original_get_and_head_bounded_memory(self):
        data = self.run_child('gib')
        self.assertEqual(data['bytes'],1024**3)
        self.assertEqual(data['head_body'],0)
        self.assertLess(data['peak_bytes'],16*1024**2)
        self.assertLess(data['rss_growth_bytes'],64*1024**2)


def physical(mode):
    from dataclasses import replace
    import resource,tempfile,tracemalloc
    from document_library.library_contracts import AssetSnapshot,ReadPrincipal,ResolvedSelection
    from document_library.publication import PublicationService,object_path
    from document_library.storage import StorageRoot,StorageLocks
    from test_storage import SyntheticVolume
    with tempfile.TemporaryDirectory(prefix='library-physical-stream-') as temporary:
        root = StorageRoot(Path(temporary).resolve(),SyntheticVolume())
        count = _fd_count
        publisher = PublicationService.__new__(PublicationService)
        publisher.root,publisher.locks = root,StorageLocks(root)
        assets = []
        number = 1 if mode == 'gib' else 200
        size = 1024**3 if mode == 'gib' else None
        for i in range(number):
            if mode == 'gib':
                hasher = hashlib.sha256();zero = b'\0'*65536
                for _ in range(size//len(zero)):
                    hasher.update(zero)
                sha,payload = hasher.hexdigest(),None
            else:
                payload = str(i).encode();sha = hashlib.sha256(payload).hexdigest()
            with root.file(object_path(sha),create=True) as fd:
                if payload is None:
                    os.ftruncate(fd,size)
                else:
                    os.write(fd,payload)
            assets.append(AssetSnapshot(0,i,'w','e','r',None,i,sha,size if payload is None else len(payload),'text/plain','txt','content',(),()))
        snapshot = ResolvedSelection(ReadPrincipal('u','synthetic',1,'user'),tuple(assets),())
        resolve = lambda:snapshot
        if mode == 'gib':
            async def body(response):
                amount,hasher = 0,hashlib.sha256()
                async def send(message):
                    nonlocal amount
                    chunk = message.get('body',b'');amount += len(chunk);hasher.update(chunk)
                async def receive():
                    await asyncio.Event().wait()
                await response({'type':'http'},receive,send)
                return amount,hasher.hexdigest()
            rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            tracemalloc.start()
            amount,got = asyncio.run(body(original_response(publisher,resolve)))
            head,_ = asyncio.run(body(original_response(publisher,resolve,method='HEAD')))
            _,peak = tracemalloc.get_traced_memory();tracemalloc.stop()
            assert got == assets[0].object_sha
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_unit = 1 if sys.platform == 'darwin' else 1024
            data = dict(bytes=amount,head_body=head,peak_bytes=peak,
                        rss_growth_bytes=max(0,rss_after-rss_before)*rss_unit)
        else:
            resource.setrlimit(resource.RLIMIT_NOFILE,(64 if mode == 'emfile' else int(mode),resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
            before = count()
            gate = StreamAdmission(max_producers=2,count_fds=count,soft_limit=4096 if mode == 'emfile' else None)
            handles = []
            def start():
                return OwnedProducer.start(open_selected=lambda:publisher.read_selected(resolve),prepare=lambda _:PreparedTransfer(200,()),
                    produce=lambda *_:iter(()),unique_originals=200,admission=gate)
            try:
                first = start();handles.append(first)
                assert first.prepared.wait(10)
                if mode == 'emfile':
                    assert isinstance(first.error,OSError)
                    first.wait_exited(5)
                    data = dict(errno=first.error.errno,before=before,after=count())
                elif mode == '1024':
                    rejected = False
                    try:
                        start()
                    except AdmissionError:
                        rejected = True
                    data = dict(highwater_delta=count()-before,second_rejected=rejected)
                else:
                    second = start();handles.append(second);assert second.prepared.wait(10)
                    rejected = False
                    try:
                        start()
                    except AdmissionError:
                        rejected = True
                    data = dict(highwater_delta=count()-before,third_rejected=rejected,producers=gate.usage()['producers'])
            finally:
                for handle in handles:
                    handle.request_stop();assert handle.wait_closed(5);assert handle.wait_exited(5)
        root.close()
        print(json.dumps(data))


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--physical':
        physical(sys.argv[2])
    else:
        unittest.main()
