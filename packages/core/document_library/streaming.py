"""Bounded same-thread producers; signal cancellation is never a close receipt."""
import asyncio
from dataclasses import dataclass
import os
import queue
import re
import resource
import threading

from starlette.responses import Response


class StreamError(RuntimeError):
    pass


class AdmissionError(StreamError):
    pass


class RangeError(StreamError):
    def __init__(self, status, size):
        self.status, self.size = status, size
        super().__init__('INVALID_RANGE' if status == 400 else 'RANGE_NOT_SATISFIABLE')


@dataclass(frozen=True)
class TransferRange:
    status: int
    start: int
    length: int
    total: int

    @property
    def content_range(self):
        if self.status == 206:
            return f'bytes {self.start}-{self.start+self.length-1}/{self.total}'
        return None


def parse_single_range(method, range_value, if_range, size, strong_etag):
    """Call only after authority. HEAD ignores all Range/If-Range input."""
    if type(size) is not int or size < 0:
        raise ValueError('Explicit representation size required')
    full = TransferRange(200, 0, size, size)
    if method != 'GET' or range_value is None:
        return full
    if isinstance(range_value, (tuple,list)):
        if len(range_value) != 1:
            raise RangeError(400, size)
        range_value = range_value[0]
    if not isinstance(range_value,str) or len(range_value) > 1024 or '=' not in range_value:
        raise RangeError(400, size)
    unit, value = range_value.split('=', 1)
    if unit.strip().lower() != 'bytes':
        return full
    if ',' in value or not re.fullmatch(r'\d*-\d*',value.strip()):
        raise RangeError(400, size)
    first, last = value.strip().split('-')
    if not first and not last:
        raise RangeError(400, size)
    # Reject long decimal input before int conversion (including Python's cap).
    if len(first) > 20 or len(last) > 20:
        raise RangeError(400, size)
    if if_range is not None and if_range != strong_etag:
        return full
    if first:
        start = int(first)
        end = min(int(last),size-1) if last else size-1
        if last and int(last) < start:
            raise RangeError(400, size)
    else:
        suffix = int(last)
        start, end = max(0,size-suffix), size-1
        if suffix == 0:
            raise RangeError(416, size)
    if not size or start >= size:
        raise RangeError(416, size)
    return TransferRange(206, start, end-start+1, size)


def _fd_count():
    # Count entries only. No descriptor targets or authentication material read.
    for path in ('/proc/self/fd','/dev/fd'):
        try:
            return len(os.listdir(path))
        except FileNotFoundError:
            continue
    raise AdmissionError('FD_COUNT_UNAVAILABLE')


class StreamAdmission:
    """Inject one process-shared gate across services, independent of app routes.

    Pending reservations are conservatively added to actual open FDs, including
    already materialized reservations. This may reject load early; it never
    promises control over unrelated code opening FDs after admission.
    """
    def __init__(self, *, max_producers=4, reserve_fds=64, transient_fds=16,
                 count_fds=_fd_count, soft_limit=None):
        if any(type(v) is not int or v < 1 for v in (max_producers,reserve_fds,transient_fds)):
            raise ValueError('Explicit positive process budgets required')
        self.max_producers, self.reserve_fds, self.transient_fds = max_producers,reserve_fds,transient_fds
        self._count, self._soft = count_fds,soft_limit
        self._lock, self._reservations = threading.Lock(),set()

    def _reap_in(self):
        for item in tuple(self._reservations):
            if item.never_started:
                self._reservations.remove(item)
            elif item.thread is not None and item.closed and not item.thread.is_alive():
                item.thread.join(0)
                self._reservations.remove(item)

    def reserve(self, *, unique_originals, cache_fds=0, provider_extra=0):
        if (type(unique_originals) is not int or not 1 <= unique_originals <= 200
                or any(type(v) is not int or v < 0 for v in (cache_fds,provider_extra))):
            raise ValueError('Bounded explicit FD request required')
        charge = 4*unique_originals + 2*cache_fds + provider_extra + self.transient_fds
        with self._lock:
            self._reap_in()
            if len(self._reservations) >= self.max_producers:
                raise AdmissionError('PRODUCER_CAPACITY')
            soft = self._soft if self._soft is not None else resource.getrlimit(resource.RLIMIT_NOFILE)[0]
            used = self._count() + sum(r.charge for r in self._reservations if not r.closed)
            if soft != resource.RLIM_INFINITY and used+charge+self.reserve_fds > soft:
                raise AdmissionError('FD_BUDGET')
            item = _Reservation(self,charge)
            self._reservations.add(item)
            return item

    def usage(self):
        with self._lock:
            self._reap_in()
            return dict(producers=len(self._reservations),
                        reserved_fds=sum(r.charge for r in self._reservations if not r.closed))

    def failed_start_handles(self):
        """Strongly owned unresolved startups for the application supervisor.

        A start exception is not evidence that the OS thread never launched.
        No timeout, absent ident or early is_alive result releases these slots.
        """
        with self._lock:
            self._reap_in()
            return tuple(r.handle for r in self._reservations
                         if r.handle is not None and r.handle.start_error is not None)


class _Reservation:
    def __init__(self, gate, charge):
        self.gate,self.charge,self.thread,self.closed = gate,charge,None,False
        self.handle,self.never_started = None,False

    def acknowledge_closed(self):
        with self.gate._lock:
            self.closed = True

    def acknowledge_never_started(self):
        # Called only by the controlled phase before Thread.start is invoked.
        with self.gate._lock:
            self.never_started = True

    def release_after_exit(self):
        with self.gate._lock:
            if (not self.never_started
                    and (not self.closed or (self.thread is not None and self.thread.is_alive()))):
                raise StreamError('ACTUAL_PRODUCER_EXIT_REQUIRED')
            self.gate._reservations.discard(self)


# All callers using the default share the same ledger in this process.
DEFAULT_ADMISSION = StreamAdmission()


@dataclass(frozen=True)
class PreparedTransfer:
    status: int
    headers: tuple
    payload: object = None
    validate: object = None


@dataclass(frozen=True)
class ClosedAck:
    owner_thread: int
    produced_bytes: int
    error: object
    stopped: bool


@dataclass(frozen=True)
class NeverStartedAck:
    """Explicit pre-invocation rollback; this is not a producer close ACK."""
    error: object
    owner_thread: None = None
    produced_bytes: int = 0
    stopped: bool = True


class TransferHandle:
    def __init__(self, reservation):
        self._reservation = reservation
        self._data = queue.Queue(maxsize=2)
        self.prepared,self.ready,self.closed = threading.Event(),threading.Event(),threading.Event()
        self.never_started,self._completed = threading.Event(),threading.Event()
        self._proceed,self.stop = threading.Event(),threading.Event()
        self.transfer,self.error,self.ack,self.thread = None,None,None,None
        self.start_error = None

    def proceed(self):
        self._proceed.set()

    def request_stop(self, reason='cancelled'):
        self.stop.set()
        self._proceed.set()

    def take_nowait(self):
        try:
            return self._data.get_nowait()
        except queue.Empty:
            return None

    def wait_closed(self, timeout=None):
        if not self._completed.wait(timeout):
            return None
        return self.ack

    def wait_exited(self, timeout=None):
        # join before start raises RuntimeError; is_alive False during unknown
        # bootstrap does not prove a failed start. Wait for our own evidence.
        if not self._completed.wait(timeout):
            return False
        if self.never_started.is_set():
            self._reservation.release_after_exit()
            return True
        self.thread.join(timeout)
        if self.thread.is_alive():
            return False
        self._reservation.release_after_exit()
        return True

    def _rollback_before_start(self, error):
        self.start_error,self.error = error,error
        self.request_stop('never_started')
        self.ack = NeverStartedAck(error)
        self.never_started.set()
        self._reservation.acknowledge_never_started()
        self._reservation.release_after_exit()
        self.prepared.set()
        self._completed.set()

    @property
    def queue_size(self):
        return self._data.qsize()


class OwnedProducer:
    @staticmethod
    def start(*, open_selected, prepare, produce, unique_originals,
              admission=DEFAULT_ADMISSION, cache_fds=0, provider_extra=0,
              after_close=None, on_created=None):
        """Register before launch, preserving original startup exceptions.

        on_created is an application ownership callback, not permission to start
        the Thread. A caller with durable pins must register here. Failed starts
        also remain owned by admission.failed_start_handles until real closure
        and exit; an indeterminate native start may need process-level recovery.
        """
        reservation = admission.reserve(unique_originals=unique_originals,
                                         cache_fds=cache_fds,provider_extra=provider_extra)
        handle, launch_attempted = None, False
        try:
            handle = TransferHandle(reservation)
            reservation.handle = handle
            def run():
                produced = 0
                try:
                    with open_selected() as opened:
                        if not handle.stop.is_set():
                            handle.transfer = prepare(opened)
                            handle.prepared.set()
                            handle._proceed.wait()
                            if not handle.stop.is_set():
                                opened.revalidate()
                                if handle.transfer.validate is not None:
                                    handle.transfer.validate()
                                handle.ready.set()
                                for data in produce(opened,handle.transfer):
                                    if not isinstance(data,bytes) or not 0 < len(data) <= 65536:
                                        raise StreamError('BOUNDED_BINARY_CHUNK_REQUIRED')
                                    while not handle.stop.is_set():
                                        try:
                                            handle._data.put(data,timeout=0.05)
                                            produced += len(data)
                                            break
                                        except queue.Full:
                                            pass
                                    if handle.stop.is_set():
                                        break
                except BaseException as error:
                    handle.error = error
                finally:
                    # Every opened context exits in this OS thread before the ACK.
                    handle.ack = ClosedAck(threading.get_ident(),produced,handle.error,handle.stop.is_set())
                    reservation.acknowledge_closed()
                    handle.closed.set()
                    handle._completed.set()
                    handle.prepared.set()
                    if after_close is not None:
                        after_close()
            thread = threading.Thread(target=run,name='library-original-producer',daemon=True)
            handle.thread = reservation.thread = thread
            if on_created is not None:
                on_created(handle)
            launch_attempted = True
            thread.start()
        except BaseException as error:
            if not launch_attempted:
                if handle is None:
                    reservation.acknowledge_never_started()
                    reservation.release_after_exit()
                else:
                    handle._rollback_before_start(error)
            else:
                # An exception may follow launch. Keep the real thread/handle
                # and ledger, even when this caller receives no handle.
                handle.start_error = error
                handle.request_stop('startup_exception')
            raise
        return handle


class OwnedResponse(Response):
    """ASGI ready barrier; no path reopen or sync-iterator threadpool ownership.

    on_closed(ack, transport_success, bytes_sent) runs after physical close and
    observed thread exit, or explicit never-started rollback. bytes_sent counts body bytes accepted by successful
    ASGI send calls; it is not proof of client receipt. transport_success also
    requires the final ASGI send to return successfully.
    G2D owns durable attempt/audit effects, not this reusable response.
    """
    def __init__(self, handle, *, on_closed=None, on_preflight_error=None):
        super().__init__(content=b'')
        self.handle,self.on_closed,self.cleanup_task = handle,on_closed,None
        self.on_preflight_error = on_preflight_error

    async def __call__(self, scope, receive, send):
        handle = self.handle
        sender = asyncio.current_task()
        success, start_attempted, bytes_sent, failure = False, False, 0, None
        async def disconnect():
            while True:
                message = await receive()
                if message['type'] == 'http.disconnect':
                    handle.request_stop('disconnect')
                    sender.cancel()
                    return
        watcher = asyncio.create_task(disconnect())
        async def cleanup():
            ack = await asyncio.to_thread(handle.wait_closed)
            await asyncio.to_thread(handle.wait_exited)
            if self.on_closed is not None:
                await self.on_closed(ack,success,bytes_sent)
        try:
            while not handle.prepared.is_set():
                await asyncio.sleep(0.002)
            if handle.error is not None:
                raise handle.error
            if handle.stop.is_set() or handle.transfer is None:
                raise StreamError('TRANSFER_STOPPED_BEFORE_HEADERS')
            handle.proceed()
            while not handle.ready.is_set():
                if handle.closed.is_set():
                    raise handle.error or StreamError('TRANSFER_STOPPED_BEFORE_HEADERS')
                await asyncio.sleep(0.002)
            transfer = handle.transfer
            start_attempted = True
            await send(dict(type='http.response.start',status=transfer.status,headers=list(transfer.headers)))
            while True:
                data = handle.take_nowait()
                if data is not None:
                    await send(dict(type='http.response.body',body=data,more_body=True))
                    bytes_sent += len(data)
                    continue
                if handle.closed.is_set():
                    # CLOSED means there can be no future puts. The first empty
                    # get may have raced the final put, so drain once more.
                    data = handle.take_nowait()
                    if data is not None:
                        await send(dict(type='http.response.body',body=data,more_body=True))
                        bytes_sent += len(data)
                        continue
                    if handle.error is not None:
                        raise handle.error
                    if handle.stop.is_set():
                        raise StreamError('TRANSFER_STOPPED')
                    break
                await asyncio.sleep(0.002)
            await send(dict(type='http.response.body',body=b'',more_body=False))
            success = True
        except BaseException as error:
            failure = error
        finally:
            watcher.cancel()
            if not success:
                handle.request_stop('response_ended')
            # Shielded cleanup remains alive if ASGI cancels this coroutine.
            self.cleanup_task = asyncio.create_task(cleanup())
            await asyncio.shield(self.cleanup_task)
        if failure is not None:
            if (not start_attempted and not isinstance(failure,asyncio.CancelledError)
                    and self.on_preflight_error is not None):
                failure = await self.on_preflight_error(failure)
            raise failure


def original_response(publisher, resolve_fresh, *, method='GET', range_value=None,
                      if_range=None, inline=False, admission=DEFAULT_ADMISSION, on_closed=None,
                      on_created=None):
    expected = resolve_fresh()
    if len(expected.assets) != 1:
        raise StreamError('SINGLE_ORIGINAL_REQUIRED')
    asset = expected.assets[0]
    def prepare(opened):
        etag = '"'+asset.object_sha+'"'
        try:
            selected = parse_single_range(method,range_value,if_range,asset.byte_size,etag)
        except RangeError as error:
            headers = [(b'content-length',b'0')]
            if error.status == 416:
                headers.append((b'content-range',f'bytes */{asset.byte_size}'.encode()))
            return PreparedTransfer(error.status,tuple(headers))
        headers = [(b'content-length',str(selected.length).encode()),(b'etag',etag.encode()),
                   (b'accept-ranges',b'bytes'),(b'content-type',asset.detected_mime.encode('ascii')),
                   (b'x-content-type-options',b'nosniff')]
        disposition = b'inline' if inline and asset.file_format == 'pdf' else b'attachment'
        headers.append((b'content-disposition',disposition+b'; filename="original.'+asset.file_format.encode('ascii')+b'"'))
        if selected.content_range:
            headers.append((b'content-range',selected.content_range.encode()))
        return PreparedTransfer(selected.status,tuple(headers),selected)
    def produce(opened, transfer):
        if method != 'HEAD' and transfer.payload is not None:
            yield from opened.chunks(asset,start=transfer.payload.start,length=transfer.payload.length)
    handle = OwnedProducer.start(open_selected=lambda:publisher.read_selected(resolve_fresh,expected=expected),
        prepare=prepare,produce=produce,unique_originals=1,admission=admission,on_created=on_created)
    return OwnedResponse(handle,on_closed=on_closed)
