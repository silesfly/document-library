"""Lazy ASGI factories and process-local physical cleanup supervision.

An uncalled Response owns only a callable. A stop request, time limit or false
is_alive value is never physical exit evidence. Unresolved resources remain
strongly owned and shutdown exposes them for explicit recovery.
"""
import asyncio
from contextvars import ContextVar
from contextlib import asynccontextmanager
import threading

from starlette.responses import Response

from .streaming import ClosedAck, NeverStartedAck, OwnedResponse


_CREATED = ContextVar('library_factory_handles',default=None)


class _Work:
    def __init__(self):
        self.finished = threading.Event()
        self.task = self.value = self.error = None


class StreamSupervisor:
    def __init__(self, *, downloads, admissions=(), settle_rounds=50):
        self.downloads, self.admissions = downloads, tuple(dict.fromkeys(admissions))
        self.accepting = True
        self._handles, self._responses, self._tasks = set(), set(), set()
        self._work = set()
        self._mutex = threading.RLock()
        self.failures = []
        self.shutdown_result = None
        self.settle_rounds = settle_rounds

    def admit(self):
        if not self.accepting:
            from .auth import AuthError
            raise AuthError('LIBRARY_SHUTTING_DOWN',409)

    def track(self, handle):
        # This callback executes before native Thread.start, including failures.
        with self._mutex:
            self.admit()
            self._handles.add(handle)
            created = _CREATED.get()
            if created is not None:
                created.append(handle)

    def attach(self, response):
        if not isinstance(response, OwnedResponse):
            raise TypeError('An owned ASGI response is required')
        with self._mutex:
            self._handles.add(response.handle)
            self._responses.add(response)
            response.lifecycle_cleanup_error = None
            callback = response.on_closed
            if callback is not None:
                async def observed_closed(*args):
                    work = self._async_work(lambda:callback(*args),'CLEANUP_CALLBACK_FAILED',
                        failed=lambda error:setattr(response,'lifecycle_cleanup_error',error))
                    await asyncio.shield(work.task)
                response.on_closed = observed_closed
            preflight = response.on_preflight_error
            if preflight is not None:
                async def observed_preflight(error):
                    work = self._async_work(lambda:preflight(error),'PREVIEW_FINALIZATION_FAILED')
                    await asyncio.shield(work.task)
                    return work.error if work.error is not None else work.value
                response.on_preflight_error = observed_preflight

    def _work_done(self, work):
        task = work.task
        if task.cancelled():
            self._failure('SUPERVISED_WAITER_CANCELLED')
        elif task.exception() is not None:
            self._failure('SUPERVISED_WAITER_FAILED')
        self._reap_work()

    def _reap_work(self):
        with self._mutex:
            for work in tuple(self._work):
                # A cancelled await does not prove the actual worker stopped.
                if work.finished.is_set() and work.task is not None and work.task.done():
                    self._work.remove(work)

    def _async_work(self, callback, code, *, failed=None):
        work = _Work()
        async def run():
            cancelled = False
            try:
                work.value = await callback()
            except BaseException as error:
                work.error = error
                cancelled = isinstance(error,asyncio.CancelledError)
                self._failure(code)
                if failed is not None:
                    failed(error)
            finally:
                # External cancellation can detach a callback's own to_thread
                # worker. Preserve uncertainty rather than inventing its exit.
                if not cancelled:
                    work.finished.set()
        with self._mutex:
            self._work.add(work)
            work.task = asyncio.create_task(run())
            work.task.add_done_callback(lambda _:self._work_done(work))
        return work

    def _start_worker(self, callback, code=None, *, completed=None):
        work = _Work()
        def run():
            try:
                work.value = callback()
            except BaseException as error:
                work.error = error
                if code is not None:
                    self._failure(code)
            finally:
                # Set by the actual synchronous worker, never its awaiter.
                work.finished.set()
        async def wait():
            await asyncio.to_thread(run)
            if completed is not None:
                try:
                    completed(work)
                except BaseException as error:
                    work.error = error
        with self._mutex:
            self._work.add(work)
            work.task = asyncio.create_task(wait())
            work.task.add_done_callback(lambda _:self._work_done(work))
        return work

    async def _worker(self, callback, code=None):
        work = self._start_worker(callback,code)
        await asyncio.shield(work.task)
        return work

    async def run_call(self, callback):
        """Track an admitted sync request through actual worker completion."""
        self.admit()
        def admitted():
            self.admit()
            return callback()
        work = await self._worker(admitted)
        if work.error is not None:
            raise work.error
        return work.value

    def _failure(self, code):
        with self._mutex:
            self.failures.append(code)

    def _task_done(self, task):
        try:
            if task.cancelled():
                self._failure('CLEANUP_CANCELLED')
            elif task.exception() is not None:
                self._failure('CLEANUP_FAILED')
        finally:
            with self._mutex:
                self._tasks.discard(task)

    def observe(self, response):
        # OwnedResponse publishes its shielded task even on a second cancel.
        with self._mutex:
            task = response.cleanup_task
            if task is not None and task not in self._tasks:
                self._tasks.add(task)
                task.add_done_callback(self._task_done)
            if task is not None:
                self._responses.discard(response)

    def discard(self, response):
        """Finalize a factory result which never entered its ASGI response."""
        response.handle.request_stop('lazy_response_ended')
        if response.cleanup_task is not None:
            return
        async def cleanup():
            ack = await asyncio.to_thread(response.handle.wait_closed)
            await asyncio.to_thread(response.handle.wait_exited)
            if response.on_closed is not None:
                await response.on_closed(ack,False,0)
        response.cleanup_task = asyncio.create_task(cleanup())
        self.observe(response)

    @staticmethod
    def stopped(handle):
        ack = handle.ack
        if isinstance(ack, NeverStartedAck) and handle.never_started.is_set():
            return handle.wait_exited(0)
        if isinstance(ack, ClosedAck) and handle.closed.is_set():
            return handle.wait_exited(0)
        return False

    def reap(self):
        self._reap_work()
        with self._mutex:
            for admission in self.admissions:
                self._handles.update(admission.failed_start_handles())
            for response in tuple(self._responses):
                if response.cleanup_task is not None:
                    self.observe(response)
            for handle in tuple(self._handles):
                try:
                    if self.stopped(handle):
                        self._handles.remove(handle)
                except BaseException:
                    self._failure('EXIT_OBSERVATION_FAILED')

    def health(self):
        self.reap()
        with self._mutex:
            return dict(accepting=self.accepting, unresolved_handles=len(self._handles),
                        cleanup_tasks=len(self._tasks), unfinalized_responses=len(self._responses),
                        cleanup_failures=len(self.failures),inflight_operations=len(self._work))

    def _shutdown_state(self, owner_closed, *, pending=None, seam_ready=True):
        state = self.health()
        self.shutdown_result = dict(state,owner_closed=bool(owner_closed),
            pending_finalizations=pending,finalization_seam_ready=seam_ready,
            clean=bool(owner_closed and seam_ready and pending == 0 and not any(state[key] for key in
                ('unresolved_handles','cleanup_tasks','unfinalized_responses','cleanup_failures','inflight_operations'))))
        return self.shutdown_result

    async def shutdown(self):
        self.accepting = False
        self._shutdown_state(False)
        self.reap()
        with self._mutex:
            for handle in self._handles:
                handle.request_stop('application_shutdown')
        # Drain actual callback/worker completion before owner release. A
        # bounded observation ends with unresolved state, not an invented ACK.
        for _ in range(self.settle_rounds):
            state = self.health()
            if not any(state[key] for key in ('cleanup_tasks','unfinalized_responses','inflight_operations')):
                break
            await asyncio.sleep(0.002)
        state = self.health()
        if any(state[key] for key in ('cleanup_tasks','unfinalized_responses','inflight_operations')):
            return self._shutdown_state(False)
        retry = getattr(self.downloads,'retry_pending',None)
        pending = getattr(self.downloads,'pending_finalizations',None)
        if not callable(retry) or not callable(pending):
            return self._shutdown_state(False,seam_ready=False)
        result = await self._worker(retry,'DOWNLOAD_RETRY_FAILED')
        if result.error is not None:
            return self._shutdown_state(False)
        value = result.value
        if (not isinstance(value,dict) or set(value) != {'pending','retried','failed'}
                or any(type(value[key]) is not int or value[key] < 0 for key in value)):
            self._failure('INVALID_FINALIZATION_RECEIPT')
            return self._shutdown_state(False)
        count = await self._worker(pending,'PENDING_STATUS_FAILED')
        if count.error is not None:
            return self._shutdown_state(False)
        remaining = count.value
        if type(remaining) is not int or remaining < 0:
            self._failure('INVALID_PENDING_FINALIZATIONS')
            return self._shutdown_state(False)
        if value['pending'] or remaining or value['failed']:
            return self._shutdown_state(False,pending=remaining)
        closed = await self._worker(lambda:self.downloads.close(wait=False),'DOWNLOAD_CLOSE_FAILED')
        count = await self._worker(pending,'PENDING_STATUS_FAILED')
        remaining = count.value if count.error is None and type(count.value) is int and count.value >= 0 else None
        return self._shutdown_state(closed.error is None and closed.value is True,pending=remaining)

    @asynccontextmanager
    async def lifespan(self, app):
        try:
            yield
        finally:
            await self.shutdown()


class LazyOwnedResponse(Response):
    """Invoke the synchronous factory only when ASGI actually calls the response."""
    def __init__(self, factory, *, supervisor, error_response):
        super().__init__(content=b'')
        self.factory, self.supervisor, self.error_response = factory, supervisor, error_response

    async def __call__(self, scope, receive, send):
        response, started, primary, abandoned = None, False, None, False
        created = []
        def abandon():
            for handle in created:
                handle.request_stop('lazy_factory_ended')
            if response is not None:
                self.supervisor.discard(response)
        def factory():
            capture = _CREATED.set(created)
            try:
                self.supervisor.admit()
                return self.factory()
            finally:
                _CREATED.reset(capture)
        def completed(work):
            nonlocal response
            try:
                if work.error is None:
                    if not isinstance(work.value,OwnedResponse):
                        raise TypeError('An owned ASGI response is required')
                    response = work.value
                    self.supervisor.attach(response)
            finally:
                # The request can leave before the actual factory worker. A
                # late response still owns its physical-close callback.
                if abandoned or not self.supervisor.accepting or work.error is not None:
                    abandon()
        async def observed_send(message):
            nonlocal started, primary
            if message['type'] == 'http.response.start':
                # An attempted header send may already have reached the peer.
                started = True
            try:
                await send(message)
            except BaseException as error:
                primary = error
                raise
        try:
            self.supervisor.admit()
            work = self.supervisor._start_worker(factory,completed=completed)
            await asyncio.shield(work.task)
            if work.error is not None:
                raise work.error
            self.supervisor.admit()
            # Completion attaches ownership before waking this ASGI awaiter.
            await response(scope,receive,observed_send)
            if response.lifecycle_cleanup_error is not None:
                raise response.lifecycle_cleanup_error
        except BaseException as error:
            abandoned = True
            abandon()
            if primary is not None:
                raise primary from error
            if asyncio.current_task().cancelling():
                raise asyncio.CancelledError() from error
            if not isinstance(error,Exception) or started:
                raise
            if response is not None and response.cleanup_task is not None and response.cleanup_task.done():
                # Cleanup failure is observable independently of the primary
                # pre-header producer failure; never expose exception strings.
                task = response.cleanup_task
                if not task.cancelled() and task.exception() is not None and response.handle.error is not None:
                    error = response.handle.error
            await self.error_response(error)(scope,receive,send)
        finally:
            if response is not None:
                self.supervisor.observe(response)
            self.supervisor.reap()
