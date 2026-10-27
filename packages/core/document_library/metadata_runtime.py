"""Bounded offline tokenizer ownership and exact, single-exchange raw transport.

Profiles/envelopes stay UNQUALIFIED. Frozen trials admit one exact request; a
pilot additionally needs frozen, parent-reconciled evidence. This module owns no
JobService hold and never treats socket closure or cancellation as server stop.
Runtime receipts are transient; persistent events contain hashes/counters only.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import re
import selectors
import socket
import struct
import subprocess
import threading
import time

from . import _metadata_tokenizer_worker as worker
from . import metadata_requests as requests
from .metadata_requests import PromptPolicy, RequestEnvelope, RequestProfile, render_summary_prompt


class CodecFailure(ValueError):
    """Stable code, without helper output, source or underlying exceptions."""


_DIAGNOSTIC_STAGES = frozenset({"content_type", "utf8", "json", "keys", "field", "http", "transport", "internal"})
_DIAGNOSTIC_REASONS = frozenset({"invalid_content_type", "invalid_utf8", "invalid_json", "duplicate_key", "nonfinite_number", "missing_key", "unknown_key", "invalid_field", "invalid_body", "invalid_framing", "unspecified"})


def _safe_diagnostic(value):
    stage = "internal"
    reason = "unspecified"
    body_complete = False
    response_sha256 = None
    response_bytes_len = None
    if type(value) is dict:
        try:
            stage_value = value.get("stage")
            if type(stage_value) is str and stage_value in _DIAGNOSTIC_STAGES:
                stage = stage_value
            reason_value = value.get("reason")
            if type(reason_value) is str and reason_value in _DIAGNOSTIC_REASONS:
                reason = reason_value
            if value.get("body_complete") is True:
                body_hash = value.get("response_sha256")
                body_len = value.get("response_bytes_len")
                if (type(body_hash) is str and requests._is_digest(body_hash)
                        and type(body_len) is int
                        and 0 <= body_len <= 1048576):
                    body_complete = True
                    response_sha256 = body_hash
                    response_bytes_len = body_len
        except Exception:
            stage = "internal"
            reason = "unspecified"
            body_complete = False
            response_sha256 = None
            response_bytes_len = None
    return {
        "stage": stage,
        "reason": reason,
        "body_complete": body_complete,
        "response_sha256": response_sha256,
        "response_bytes_len": response_bytes_len,
    }


class _PayloadError(ValueError):
    def __init__(self, code, *, stage="internal", reason="unspecified"):
        if type(stage) is not str or stage not in _DIAGNOSTIC_STAGES:
            stage = "internal"
        if type(reason) is not str or reason not in _DIAGNOSTIC_REASONS:
            reason = "unspecified"
        super().__init__(code)
        self.stage = stage
        self.reason = reason


@dataclass(frozen=True)
class RuntimeStopProof:
    request_id: str
    wire_sha256: str
    profile_digest: str
    disposition: str
    confirmation: str
    terminal: bool
    fully_drained: bool

    def __post_init__(self):
        _require(type(self.request_id) is str and bool(self.request_id)
                 and requests._is_digest(self.wire_sha256)
                 and requests._is_digest(self.profile_digest)
                 and requests._is_digest(self.confirmation)
                 and type(self.disposition) is str and self.disposition in ("not_started", "completed")
                 and self.terminal is (self.disposition == "completed")
                 and self.fully_drained is (self.disposition == "completed"))


class RuntimeFailure(Exception):
    """Stop proof for exactly one owned request; never carries provider text."""
    def __init__(self, code: str, *, request_id: str, envelope: RequestEnvelope,
                 stop_state: str = "uncertain", response_sha256: str | None = None,
                 diagnostic: dict | None = None):
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code):
            code = "RUNTIME_FAILED"
        self.code = code
        self.diagnostic = _safe_diagnostic(diagnostic)
        self.stop_proof = None
        if stop_state in ("not_started", "terminal"):
            terminal = stop_state == "terminal"
            if terminal:
                _require(requests._is_digest(response_sha256))
            confirmation = _digest({"request_id": request_id, "wire_sha256": envelope.wire_sha256,
                                    "profile_digest": envelope.profile_digest, "code": code,
                                    "disposition": "completed" if terminal else "not_started",
                                    "response_sha256": response_sha256})
            self.stop_proof = RuntimeStopProof(request_id, envelope.wire_sha256,
                                               envelope.profile_digest,
                                               "completed" if terminal else "not_started",
                                               confirmation, terminal, terminal)
        super().__init__(code)


def _require(condition: bool, code: str = "INVALID_RUNTIME_BINDING") -> None:
    if not condition:
        raise CodecFailure(code)


def _digest(value) -> str:
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _file_hash(path: str | Path, cap: int = 134_217_728) -> str:
    """Exact explicitly admitted implementation file, no directory inventory."""
    digest = sha256()
    with open(path, "rb") as source:
        before = os.fstat(source.fileno())
        _require(0 < before.st_size <= cap)
        remaining = before.st_size
        while remaining:
            part = source.read(min(65_536, remaining))
            _require(bool(part))
            digest.update(part)
            remaining -= len(part)
        _require(source.read(1) == b"")
        after = os.fstat(source.fileno())
        _require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                  before.st_ctime_ns) ==
                 (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                  after.st_ctime_ns))
    return digest.hexdigest()


_LOADED_SOURCE_HASHES = (_file_hash(requests.__file__), _file_hash(__file__),
                         _file_hash(worker.__file__))


@dataclass(frozen=True)
class ArtifactSpec:
    name: str
    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class ArtifactBinding:
    root: str
    manifest_path: str
    manifest_size: int
    manifest_sha256: str
    artifacts: tuple[ArtifactSpec, ...]

    def payload(self, profile: RequestProfile) -> dict:
        _require(type(self) is ArtifactBinding and Path(self.root).is_absolute())
        _require(type(self.manifest_size) is int and 0 < self.manifest_size <= 1_048_576
                 and self.manifest_sha256 == profile.manifest_sha256)
        expected = {"config.json": profile.model_config_sha256,
                    "tokenizer.json": profile.tokenizer_sha256,
                    "tokenizer_config.json": profile.tokenizer_config_sha256}
        _require(type(self.artifacts) is tuple and len(self.artifacts) == 3
                 and {a.name for a in self.artifacts} == set(expected))
        for item in self.artifacts:
            _require(type(item) is ArtifactSpec and type(item.size) is int
                     and 0 < item.size <= 33_554_432
                     and item.sha256 == expected[item.name]
                     and item.path == "blobs/sha256-" + item.sha256)
        return {"root": self.root,
                "manifest": {"path": self.manifest_path, "size": self.manifest_size,
                             "sha256": self.manifest_sha256},
                "artifacts": [asdict(a) for a in self.artifacts]}


@dataclass(frozen=True)
class ImplementationFile:
    role: str
    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class HelperSpec:
    interpreter: str
    interpreter_sha256: str
    worker_path: str
    worker_sha256: str
    implementation_files: tuple[ImplementationFile, ...]


@dataclass(frozen=True)
class CodecLimits:
    cpu_seconds: int = 60
    address_bytes: int = 1_073_741_824
    context_wall_seconds: float = 120
    encode_wall_seconds: float = 10
    startup_wall_seconds: float = 30
    max_codec_calls: int = 10_000

    def validate(self):
        _require(type(self.cpu_seconds) is int and 1 <= self.cpu_seconds <= 60)
        _require(type(self.address_bytes) is int
                 and 67_108_864 <= self.address_bytes <= 1_073_741_824)
        for value, cap in ((self.context_wall_seconds, 120),
                           (self.encode_wall_seconds, 10), (self.startup_wall_seconds, 30)):
            _require(type(value) in (int, float) and math.isfinite(value) and 0 < value <= cap)
        _require(type(self.max_codec_calls) is int and 2 <= self.max_codec_calls <= 10_000)


class OwnedTokenizerCodec:
    """One child per preparation context; selector deadlines interrupt pipe waits."""
    def __init__(self, profile: RequestProfile, binding: ArtifactBinding,
                 helper: HelperSpec, limits: CodecLimits = CodecLimits()):
        self.profile = profile
        self.binding = binding
        self.helper = helper
        self.limits = limits
        self.profile_digest = profile.digest
        self.observation = None
        self.identity_sha256 = None
        self.cleanup_state = "NOT_STARTED"
        self.returncode = None
        self._process = None
        self._closed = False
        self._sequence = 0
        self._calls = 0
        self._gate = threading.Lock()
        self._cleanup_lock = threading.Lock()
        self._watchdog_stop = threading.Event()
        self._watchdog = None

    def __enter__(self):
        failed = False
        with self._cleanup_lock:
            try:
                requests._validate_profile(self.profile)
                self.limits.validate()
                _require(self.profile.codec_kind == "artifact")
                _require(self._process is None and not self._closed)
                payload = self.binding.payload(self.profile)
                h = self.helper
                _require(type(h) is HelperSpec and Path(h.interpreter).is_absolute()
                         and Path(h.worker_path).is_absolute())
                _require(_file_hash(h.interpreter) == h.interpreter_sha256
                         and _file_hash(h.worker_path) == h.worker_sha256)
                _require(type(h.implementation_files) is tuple and len(h.implementation_files) == 2
                         and {f.role for f in h.implementation_files}
                         == {"python_module", "native_extension"})
                payload["implementation_files"] = [asdict(f) for f in h.implementation_files]
                self._deadline = time.monotonic() + self.limits.context_wall_seconds
                self._process = subprocess.Popen(
                    [h.interpreter, "-I", "-B", h.worker_path], stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
                    env={"PATH": "/usr/bin:/bin", "TOKENIZERS_PARALLELISM": "false"},
                    close_fds=True,
                )
                self.cleanup_state = "OWNED"
                os.set_blocking(self._process.stdin.fileno(), False)
                os.set_blocking(self._process.stdout.fileno(), False)
                ready = self._exchange({"op": "init", "seq": 0,
                                        "profile_digest": self.profile_digest,
                                        "binding": payload, "profile": asdict(self.profile),
                                        "cpu_seconds": self.limits.cpu_seconds,
                                        "address_bytes": self.limits.address_bytes},
                                       self.limits.startup_wall_seconds)
                _require(type(ready) is dict and ready.get("op") == "ready"
                         and type(ready.get("seq")) is int and ready["seq"] == 0
                         and ready.get("profile_digest") == self.profile_digest)
                inventory = ready["inventory"]
                _require(inventory["tokenizers_version"] == "0.22.1"
                         and inventory["from_str"] == "OBSERVED"
                         and inventory["padding"] is None and inventory["truncation"] is None
                         and inventory["special_tokens"] == [list(t) for t in self.profile.special_tokens]
                         and inventory["token_id_ranges"] == [list(t) for t in self.profile.token_id_ranges])
                observed_limits = ready["limits"]
                _require(observed_limits["cpu_seconds"] == self.limits.cpu_seconds
                         and observed_limits["cpu_status"] == "CONFIGURED_OBSERVED"
                         and observed_limits["address_bytes"] == self.limits.address_bytes
                         and observed_limits["address_status"] in ("CONFIGURED_OBSERVED", "NOT_VERIFIED"))
                self.observation = ready
                self.identity_sha256 = _digest({"helper": asdict(h), "observation": ready,
                                                "artifacts": payload})
                self._watchdog = threading.Thread(target=self._watch_deadline,
                                                   name="novellibrary-tokenizer-deadline")
                self._watchdog.start()
            except Exception:
                failed = True
        if failed:
            self.close()
            raise CodecFailure("CODEC_START_FAILED")
        return self

    @property
    def pid(self):
        return None if self._process is None else self._process.pid

    def _pipe(self, descriptor, event, deadline, *, data=None, size=0):
        position = 0
        result = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(descriptor, event)
            while position < (len(data) if data is not None else size):
                remaining = deadline - time.monotonic()
                _require(remaining > 0 and bool(selector.select(remaining)), "CODEC_DEADLINE")
                try:
                    if data is not None:
                        amount = os.write(descriptor, memoryview(data)[position:position + 65_536])
                    else:
                        part = os.read(descriptor, min(65_536, size - position))
                        amount = len(part)
                        result.extend(part)
                except BlockingIOError:
                    continue
                _require(amount > 0, "CODEC_PIPE_CLOSED")
                position += amount
        return bytes(result)

    def _exchange(self, value, wall_seconds):
        deadline = min(self._deadline, time.monotonic() + wall_seconds)
        data = json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                          allow_nan=False).encode("utf-8")
        _require(0 < len(data) <= worker.MAX_FRAME)
        self._pipe(self._process.stdin.fileno(), selectors.EVENT_WRITE, deadline,
                   data=struct.pack("!I", len(data)) + data)
        header = self._pipe(self._process.stdout.fileno(), selectors.EVENT_READ, deadline, size=4)
        size, = struct.unpack("!I", header)
        _require(0 < size <= worker.MAX_FRAME, "CODEC_REPLY_TOO_LARGE")
        return worker.decode(self._pipe(self._process.stdout.fileno(), selectors.EVENT_READ,
                                        deadline, size=size))

    def _begin_call(self):
        _require(not self._closed and self._process is not None and self.observation is not None
                 and self._process.poll() is None and self.profile.digest == self.profile_digest,
                 "CODEC_NOT_ACTIVE")
        _require(self._calls < self.limits.max_codec_calls, "CODEC_CALL_LIMIT")
        _require(time.monotonic() < self._deadline, "CODEC_DEADLINE")
        self._calls += 1

    def render(self, source_text: str, frozen_prompt_policy: PromptPolicy) -> str:
        code = None
        try:
            self._begin_call()
        except CodecFailure as error:
            code = str(error)
        if code is not None:
            self.close()
            raise CodecFailure(code)
        return render_summary_prompt(source_text, frozen_prompt_policy)

    def encode(self, rendered_prompt: str) -> tuple[int, ...]:
        _require(self._gate.acquire(blocking=False), "CONCURRENT_CODEC_CALL")
        failure = False
        result = None
        try:
            self._begin_call()
            _require(type(rendered_prompt) is str and 0 < len(rendered_prompt) <= worker.MAX_PROMPT)
            raw = rendered_prompt.encode("utf-8")
            _require(len(raw) <= worker.MAX_PROMPT)
            prompt_hash = sha256(raw).hexdigest()
            self._sequence += 1
            reply = self._exchange({"op": "encode", "seq": self._sequence,
                                    "profile_digest": self.profile_digest,
                                    "prompt_sha256": prompt_hash, "prompt": rendered_prompt},
                                   self.limits.encode_wall_seconds)
            _require(type(reply) is dict and reply.get("op") == "encoded"
                     and type(reply.get("seq")) is int and reply["seq"] == self._sequence
                     and reply.get("profile_digest") == self.profile_digest
                     and reply.get("prompt_sha256") == prompt_hash)
            ids = reply["ids"]
            _require(type(ids) is list and 0 < len(ids) <= worker.MAX_IDS
                     and all(requests._valid_id(value, self.profile) for value in ids))
            result = tuple(ids)
            _require(not self._closed and time.monotonic() < self._deadline, "CODEC_DEADLINE")
        except Exception:
            failure = True
        finally:
            self._gate.release()
        if failure:
            self.close()
            raise CodecFailure("CODEC_ENCODE_FAILED")
        return result

    def _watch_deadline(self):
        remaining = max(0, self._deadline - time.monotonic())
        if not self._watchdog_stop.wait(remaining):
            self.close()

    def close(self):
        self._watchdog_stop.set()
        # One cleanup owner. Active encode releases _gate before calling close;
        # its pipe wait already has the same context deadline as the watchdog.
        with self._cleanup_lock:
            if not self._closed:
                self._closed = True
                with self._gate:
                    p = self._process
                    if p is None:
                        self.cleanup_state = "NOT_STARTED"
                    else:
                        try:
                            p.stdin.close()
                        except OSError:
                            pass
                        for stop in (None, p.terminate, p.kill):
                            try:
                                if stop is not None and p.poll() is None:
                                    stop()
                                self.returncode = p.wait(timeout=0.5 if stop is None else 1)
                                self.cleanup_state = "REAPED"
                                break
                            except subprocess.TimeoutExpired:
                                continue
                            except OSError:
                                continue
                        else:
                            self.cleanup_state = "STOP_UNCONFIRMED"
                        p.stdout.close()
        watcher = self._watchdog
        if (watcher is not None and watcher is not threading.current_thread()
                and watcher.ident is not None):
            watcher.join()

    def __exit__(self, *_):
        self.close()


def load_current_codec(binding: ArtifactBinding, helper: HelperSpec, *, profile: RequestProfile,
                       limits: CodecLimits = CodecLimits()) -> OwnedTokenizerCodec:
    """Explicit context manager, using only the admitted interpreter and artifacts."""
    return OwnedTokenizerCodec(profile, binding, helper, limits)


@dataclass(frozen=True)
class RuntimeBinding:
    task_id: str
    host_id: str
    profile_digest: str
    request_source_sha256: str
    runtime_source_sha256: str
    helper_source_sha256: str
    codec_identity_sha256: str
    listener_identity_sha256: str
    prompt_policy_digest: str
    model: str
    manifest_sha256: str
    tokenizer_sha256: str
    tokenizer_config_sha256: str
    model_config_sha256: str
    interpreter_sha256: str
    tokenizer_implementation_sha256: str
    special_token_inventory_digest: str
    target_host: str = "127.0.0.1"
    target_port: int = 11434
    wire_profile: str = requests.WIRE_PROFILE
    renderer_sha256: str = requests.RENDERER_SHA256

    @property
    def digest(self):
        return _digest(asdict(self))


def make_runtime_binding(profile: RequestProfile, codec: OwnedTokenizerCodec, *, task_id: str,
                         listener_identity_sha256: str, prompt_policy: PromptPolicy,
                         target_port: int = 11434) -> RuntimeBinding:
    _require(codec.profile_digest == profile.digest and codec.identity_sha256 is not None)
    _require((_file_hash(requests.__file__), _file_hash(__file__), _file_hash(worker.__file__))
             == _LOADED_SOURCE_HASHES, "RUNTIME_SOURCE_DRIFT")
    return RuntimeBinding(task_id, socket.gethostname(), profile.digest,
                          _file_hash(requests.__file__), _file_hash(__file__),
                          _file_hash(worker.__file__), codec.identity_sha256,
                          listener_identity_sha256, prompt_policy.digest,
                          profile.model, profile.manifest_sha256, profile.tokenizer_sha256,
                          profile.tokenizer_config_sha256, profile.model_config_sha256,
                          codec.helper.interpreter_sha256,
                          _digest([asdict(f) for f in codec.helper.implementation_files]),
                          profile.special_token_inventory_digest,
                          target_port=target_port)


@dataclass(frozen=True)
class HttpLimits:
    connect_seconds: float = 5
    exchange_seconds: float = 180
    max_response_bytes: int = 1_048_576
    max_output_bytes: int = 65_536

    def validate(self):
        for value, cap in ((self.connect_seconds, 5), (self.exchange_seconds, 180)):
            _require(type(value) in (float, int) and math.isfinite(value) and 0 < value <= cap)
        _require(type(self.max_response_bytes) is int and 0 < self.max_response_bytes <= 1_048_576
                 and type(self.max_output_bytes) is int and 0 < self.max_output_bytes <= 65_536)


@dataclass(frozen=True)
class RuntimeReceipt:
    response_bytes: bytes = field(repr=False)
    response_sha256: str
    output_text: str = field(repr=False)
    request_id: str
    wire_sha256: str
    profile_digest: str
    model: str
    prompt_eval_count: int
    eval_count: int
    done_reason: str
    terminal: bool = True
    fully_drained: bool = True


class _SocketReader:
    def __init__(self, connection, deadline):
        self.connection = connection
        self.deadline = deadline
        self.buffer = bytearray()

    def receive(self, cap):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise _PayloadError("EXCHANGE_DEADLINE")
        self.connection.settimeout(remaining)
        return self.connection.recv(min(cap, 65_536))

    def line(self, cap=16_384):
        while True:
            found = self.buffer.find(b"\r\n")
            if found >= 0:
                if found + 2 > cap:
                    raise _PayloadError("INVALID_HTTP_FRAMING")
                result = bytes(self.buffer[:found])
                del self.buffer[:found + 2]
                return result
            if len(self.buffer) >= cap:
                raise _PayloadError("INVALID_HTTP_FRAMING")
            part = self.receive(cap - len(self.buffer))
            if not part:
                raise _PayloadError("TRUNCATED_HTTP_RESPONSE")
            self.buffer.extend(part)

    def exact(self, size):
        result = bytearray()
        if self.buffer:
            amount = min(size, len(self.buffer))
            result.extend(self.buffer[:amount])
            del self.buffer[:amount]
        while len(result) < size:
            part = self.receive(size - len(result))
            if not part:
                raise _PayloadError("TRUNCATED_HTTP_RESPONSE")
            result.extend(part)
        return bytes(result)

    def body(self, headers, cap):
        length = headers.get(b"content-length")
        encoding = headers.get(b"transfer-encoding")
        if length is not None and encoding is not None:
            raise _PayloadError("INVALID_HTTP_FRAMING")
        if length is not None:
            if not re.fullmatch(rb"[0-9]{1,10}", length) or int(length) > cap:
                raise _PayloadError("RESPONSE_TOO_LARGE")
            body = self.exact(int(length))
            if self.buffer:
                raise _PayloadError("INVALID_HTTP_FRAMING")
            return body
        body = bytearray()
        if encoding is not None:
            if encoding.lower() != b"chunked":
                raise _PayloadError("INVALID_HTTP_FRAMING")
            for _ in range(8192):
                line = self.line(128)
                if not re.fullmatch(rb"[0-9a-fA-F]{1,8}", line):
                    raise _PayloadError("INVALID_HTTP_FRAMING")
                size = int(line, 16)
                if size == 0:
                    trailer_bytes = 0
                    trailer_names = set()
                    for _ in range(128):
                        trailer = self.line()
                        trailer_bytes += len(trailer) + 2
                        if trailer_bytes > 16_384:
                            raise _PayloadError("INVALID_HTTP_FRAMING")
                        if not trailer:
                            if self.buffer:
                                raise _PayloadError("INVALID_HTTP_FRAMING")
                            return bytes(body)
                        if b":" not in trailer:
                            raise _PayloadError("INVALID_HTTP_FRAMING")
                        name, value = trailer.split(b":", 1)
                        name = name.lower()
                        if (not re.fullmatch(rb"[a-z0-9!#$%&'*+.^_`|~-]+", name)
                                or name in trailer_names
                                or name in (b"content-length", b"transfer-encoding")):
                            raise _PayloadError("INVALID_HTTP_FRAMING")
                        value = value.strip(b" \t")
                        if any(n < 32 and n != 9 or n == 127 for n in value):
                            raise _PayloadError("INVALID_HTTP_FRAMING")
                        trailer_names.add(name)
                    raise _PayloadError("INVALID_HTTP_FRAMING")
                if size > cap - len(body):
                    raise _PayloadError("RESPONSE_TOO_LARGE")
                body.extend(self.exact(size))
                if self.exact(2) != b"\r\n":
                    raise _PayloadError("INVALID_HTTP_FRAMING")
            raise _PayloadError("INVALID_HTTP_FRAMING")
        if len(self.buffer) > cap:
            raise _PayloadError("RESPONSE_TOO_LARGE")
        body.extend(self.buffer)
        self.buffer.clear()
        while len(body) <= cap:
            part = self.receive(cap - len(body) + 1)
            if not part:
                return bytes(body)
            body.extend(part)
        raise _PayloadError("RESPONSE_TOO_LARGE")


class RawHttpTransport:
    """No proxy, DNS, redirects, retry, cancel endpoint or keep_alive override."""
    def __init__(self, limits: HttpLimits = HttpLimits(), *, mock_port: int | None = None,
                 connection_factory=None):
        limits.validate()
        _require(mock_port is None or type(mock_port) is int and 0 < mock_port < 65_536
                 and mock_port != 11434)
        _require(connection_factory is None or mock_port is not None)
        self.limits = limits
        self.port = 11434 if mock_port is None else mock_port
        self.is_mock = mock_port is not None
        self._connection_factory = connection_factory

    def _connect(self, timeout):
        if self._connection_factory is not None:
            return self._connection_factory(timeout)
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            connection.settimeout(timeout)
            connection.connect(("127.0.0.1", self.port))
            return connection
        except BaseException:
            connection.close()
            raise

    def _exchange(self, envelope, *, request_id, model, cancelled, before_connect):
        failure = None
        stage = "internal"
        reason = "unspecified"
        body_complete = False
        response_hash = None
        response_len = None
        connection = None
        receipt = None
        state = "not_started"
        try:
            if cancelled.is_set():
                raise _PayloadError("CANCELLED")
            before_connect()
            # Socket creation/connect itself has started: later failure is uncertain.
            state = "uncertain"
            deadline = time.monotonic() + self.limits.exchange_seconds
            connection = self._connect(min(self.limits.connect_seconds,
                                           self.limits.exchange_seconds))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise _PayloadError("EXCHANGE_DEADLINE")
            connection.settimeout(remaining)
            if cancelled.is_set():
                raise _PayloadError("CANCELLED")
            header = ("POST /api/generate HTTP/1.1\r\nHost: 127.0.0.1:" + str(self.port)
                      + "\r\nContent-Type: application/json\r\nAccept: application/json"
                      + "\r\nConnection: close\r\nContent-Length: " + str(len(envelope.body))
                      + "\r\n\r\n").encode("ascii")
            connection.sendall(header + envelope.body)
            reader = _SocketReader(connection, deadline)
            status = reader.line(256)
            if status not in (b"HTTP/1.1 200 OK", b"HTTP/1.0 200 OK"):
                stage, reason = "http", "invalid_framing"
                raise _PayloadError("HTTP_STATUS_UNVERIFIED", stage=stage, reason=reason)
            headers = {}
            header_bytes = len(status) + 2
            for _ in range(128):
                line = reader.line()
                header_bytes += len(line) + 2
                if header_bytes > 16_384:
                    stage, reason = "http", "invalid_framing"
                    raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
                if not line:
                    break
                if b":" not in line:
                    stage, reason = "http", "invalid_framing"
                    raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
                name, value = line.split(b":", 1)
                name = name.lower()
                if not re.fullmatch(rb"[a-z0-9!#$%&'*+.^_`|~-]+", name) or name in headers:
                    stage, reason = "http", "invalid_framing"
                    raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
                value = value.strip(b" \t")
                if any(n < 32 and n != 9 or n == 127 for n in value):
                    stage, reason = "http", "invalid_framing"
                    raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
                headers[name] = value
            else:
                stage, reason = "http", "invalid_framing"
                raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
            if headers.get(b"content-type", b"").split(b";", 1)[0].lower() != b"application/json":
                stage, reason = "content_type", "invalid_content_type"
                raise _PayloadError("INVALID_PROVIDER_RESPONSE", stage=stage, reason=reason)
            if headers.get(b"content-encoding", b"identity").lower() != b"identity":
                stage, reason = "http", "invalid_framing"
                raise _PayloadError("INVALID_HTTP_FRAMING", stage=stage, reason=reason)
            body = reader.body(headers, self.limits.max_response_bytes)
            body_complete = True
            response_len = len(body)
            response_hash = sha256(body).hexdigest()
            provider = _decode_provider_terminal(body, model)
            state = "terminal"
            if time.monotonic() >= deadline:
                stage, reason = "transport", "unspecified"
                raise _PayloadError("EXCHANGE_DEADLINE", stage=stage, reason=reason)
            if provider["prompt_eval_count"] != envelope.request_tokens:
                stage, reason = "field", "invalid_field"
                raise _PayloadError("TOKEN_PARITY_MISMATCH", stage=stage, reason=reason)
            if provider["eval_count"] > envelope.output_reserve_tokens:
                stage, reason = "field", "invalid_field"
                raise _PayloadError("OUTPUT_TOKEN_CAP_EXCEEDED", stage=stage, reason=reason)
            output_failed = False
            try:
                text_bytes = provider["response"].encode("utf-8")
            except UnicodeError:
                output_failed = True
            if output_failed:
                stage, reason = "field", "invalid_field"
                raise _PayloadError("INVALID_OUTPUT_TEXT", stage=stage, reason=reason)
            if len(text_bytes) > self.limits.max_output_bytes:
                stage, reason = "field", "invalid_field"
                raise _PayloadError("OUTPUT_TEXT_TOO_LARGE", stage=stage, reason=reason)
            receipt = RuntimeReceipt(body, sha256(body).hexdigest(), provider["response"],
                                     request_id, envelope.wire_sha256, envelope.profile_digest,
                                     model, provider["prompt_eval_count"], provider["eval_count"],
                                     provider["done_reason"])
        except _PayloadError as error:
            failure = error.args[0]
            stage, reason = error.stage, error.reason
        except Exception:
            failure = "TRANSPORT_FAILED"
            stage, reason = "transport", "unspecified"
        finally:
            if connection is not None:
                connection.close()
        if failure is not None:
            diagnostic = {"stage": stage, "reason": reason,
                          "body_complete": body_complete,
                          "response_sha256": response_hash,
                          "response_bytes_len": response_len}
            raise RuntimeFailure(failure, request_id=request_id, envelope=envelope, stop_state=state,
                                 response_sha256=response_hash, diagnostic=diagnostic)
        return receipt


def _envelope_digest(envelope):
    values = asdict(envelope)
    del values["body"]
    return _digest(values)


def _validate_envelope(envelope, profile, binding, transport):
    _require(type(envelope) is RequestEnvelope and type(profile) is RequestProfile)
    requests._validate_profile(profile)
    _require(profile.codec_kind == "artifact" and type(binding) is RuntimeBinding)
    _require(type(binding.task_id) is str and 0 < len(binding.task_id) <= 256
             and binding.profile_digest == profile.digest == envelope.profile_digest
             and envelope.qualification_status == "UNQUALIFIED"
             and binding.host_id == socket.gethostname()
             and binding.target_host == "127.0.0.1" and binding.target_port == transport.port
             and binding.wire_profile == requests.WIRE_PROFILE
             and binding.renderer_sha256 == requests.RENDERER_SHA256
             and binding.prompt_policy_digest == envelope.prompt_policy_digest)
    _require(binding.model == profile.model
             and binding.manifest_sha256 == profile.manifest_sha256
             and binding.tokenizer_sha256 == profile.tokenizer_sha256
             and binding.tokenizer_config_sha256 == profile.tokenizer_config_sha256
             and binding.model_config_sha256 == profile.model_config_sha256
             and binding.special_token_inventory_digest == profile.special_token_inventory_digest)
    for name in ("profile_digest", "request_source_sha256", "runtime_source_sha256",
                 "helper_source_sha256", "codec_identity_sha256", "listener_identity_sha256",
                 "prompt_policy_digest", "manifest_sha256", "tokenizer_sha256",
                 "tokenizer_config_sha256", "model_config_sha256", "interpreter_sha256",
                 "tokenizer_implementation_sha256", "special_token_inventory_digest"):
        _require(requests._is_digest(getattr(binding, name)))
    _require(binding.request_source_sha256 == _file_hash(requests.__file__)
             and binding.runtime_source_sha256 == _file_hash(__file__)
             and binding.helper_source_sha256 == _file_hash(worker.__file__)
             and (binding.request_source_sha256, binding.runtime_source_sha256,
                  binding.helper_source_sha256) == _LOADED_SOURCE_HASHES, "RUNTIME_SOURCE_DRIFT")
    _require(type(envelope.body) is bytes and 0 < len(envelope.body) <= 1_048_576
             and type(envelope.wire_bytes) is int and envelope.wire_bytes == len(envelope.body)
             and sha256(envelope.body).hexdigest() == envelope.wire_sha256)
    wire = worker.decode(envelope.body)
    _require(type(wire) is dict and set(wire) == {"model", "prompt", "raw", "stream", "think", "options"}
             and wire["model"] == profile.model and wire["raw"] is True
             and wire["stream"] is False and wire["think"] is False
             and type(wire["prompt"]) is str
             and sha256(wire["prompt"].encode("utf-8")).hexdigest() == envelope.prompt_sha256)
    _require(type(wire["options"]) is dict and set(wire["options"]) == {"num_ctx", "num_predict", "temperature"}
             and type(wire["options"]["num_ctx"]) is int
             and wire["options"]["num_ctx"] == envelope.requested_context_tokens
             and type(wire["options"]["num_predict"]) is int
             and wire["options"]["num_predict"] == envelope.output_reserve_tokens
             and type(wire["options"]["temperature"]) is int and wire["options"]["temperature"] == 0)
    _require(type(envelope.request_tokens) is int and 0 < envelope.request_tokens <= 1_000_000
             and type(envelope.output_reserve_tokens) is int
             and 0 < envelope.output_reserve_tokens <= profile.max_output_tokens
             and type(envelope.requested_context_tokens) is int
             and envelope.request_tokens + envelope.output_reserve_tokens
             <= envelope.requested_context_tokens <= profile.max_context_tokens
             and envelope.special_token_inventory_digest == profile.special_token_inventory_digest)


@dataclass(frozen=True)
class FrozenTrialPermit:
    """Explicit one-request bootstrap authority, never backend qualification."""
    binding: RuntimeBinding
    request_id: str
    envelope_digest: str
    wire_sha256: str
    prompt_sha256: str
    token_ids_sha256: str
    request_tokens: int
    context_tokens: int
    output_tokens: int
    max_response_bytes: int = 1_048_576
    max_output_bytes: int = 65_536

    @classmethod
    def freeze(cls, binding, envelope, *, request_id):
        _require(envelope.request_tokens <= 512
                 and envelope.requested_context_tokens <= 4096
                 and envelope.output_reserve_tokens <= 1024)
        return cls(binding, request_id, _envelope_digest(envelope), envelope.wire_sha256,
                   envelope.prompt_sha256, envelope.token_ids_sha256, envelope.request_tokens,
                   envelope.requested_context_tokens, envelope.output_reserve_tokens)


def _read_evidence(path, expected_sha256):
    p = Path(path)
    _require(p.is_absolute() and requests._is_digest(expected_sha256))
    parent = worker.open_directory(str(p.parent))
    try:
        info = os.stat(p.name, dir_fd=parent, follow_symlinks=False)
        _require(0 < info.st_size <= 65_536)
        data = worker.read_verified(parent, p.name, info.st_size, expected_sha256)
    finally:
        os.close(parent)
    return worker.decode(data)


@dataclass(frozen=True, init=False)
class RuntimeQualification:
    binding: RuntimeBinding
    qualification_sha256: str
    reconciliation_sha256: str
    trial_receipt_sha256: str
    max_prompt_tokens: int
    max_context_tokens: int
    max_output_tokens: int
    max_requests: int

    @classmethod
    def load(cls, qualification_path, reconciliation_path, trial_receipt_path, *,
             expected_reconciliation_sha256: str, current_binding: RuntimeBinding):
        """Load exact three-way evidence, not a status flag or file-presence check."""
        failed = False
        result = None
        try:
            result = cls._load(qualification_path, reconciliation_path, trial_receipt_path,
                               expected_reconciliation_sha256=expected_reconciliation_sha256,
                               current_binding=current_binding)
        except Exception:
            failed = True
        if failed:
            raise CodecFailure("INVALID_QUALIFICATION_EVIDENCE")
        return result

    @classmethod
    def _load(cls, qualification_path, reconciliation_path, trial_receipt_path, *,
              expected_reconciliation_sha256, current_binding):
        reconciliation = _read_evidence(reconciliation_path, expected_reconciliation_sha256)
        _require(reconciliation["schema"] == "novellibrary.runtime.pilot-reconciliation.v1"
                 and reconciliation["decision"] == "PILOT_ADMITTED"
                 and type(reconciliation["reviewer"]) is str and bool(reconciliation["reviewer"].strip())
                 and reconciliation["binding_digest"] == current_binding.digest
                 and type(reconciliation["remaining_uncertainties"]) is list
                 and 0 < len(reconciliation["remaining_uncertainties"]) <= 32)
        qualification_hash = reconciliation["qualification_sha256"]
        qualification = _read_evidence(qualification_path, qualification_hash)
        _require(qualification["schema"] == "novellibrary.runtime.qualification.v1"
                 and qualification["binding"] == asdict(current_binding)
                 and qualification["capabilities"] == ["normal_raw_single_trial"])
        trial_hash = qualification["trial_receipt_sha256"]
        trial = _read_evidence(trial_receipt_path, trial_hash)
        _require(trial["state"] == "CAPTURED" and trial["cancelled"] is False
                 and trial["terminal"] is True
                 and trial["fully_drained"] is True
                 and trial["binding_digest"] == current_binding.digest
                 and trial["profile_digest"] == current_binding.profile_digest
                 and trial["model"] == current_binding.model
                 and trial["request_id"] == qualification["trial_request_id"]
                 and trial["wire_sha256"] == qualification["trial_wire_sha256"]
                 and trial["response_sha256"] == qualification["trial_response_sha256"]
                 and requests._is_digest(trial["response_sha256"])
                 and requests._is_digest(trial["wire_sha256"])
                 and type(trial["prompt_eval_count"]) is int and trial["prompt_eval_count"] > 0
                 and trial["prompt_eval_count"] == qualification["local_prompt_tokens"]
                 and type(trial["eval_count"]) is int and trial["eval_count"] >= 0
                 and trial["done_reason"] in ("stop", "length"))
        limits = reconciliation["limits"]
        for name, cap in (("max_prompt_tokens", 512), ("max_context_tokens", 4096),
                          ("max_output_tokens", 1024), ("max_requests", 16)):
            _require(type(limits[name]) is int and 0 < limits[name] <= cap)
        result = object.__new__(cls)
        values = {"binding": current_binding, "qualification_sha256": qualification_hash,
                  "reconciliation_sha256": expected_reconciliation_sha256,
                  "trial_receipt_sha256": trial_hash, **limits}
        _require(set(values) == set(cls.__dataclass_fields__))
        for name, value in values.items():
            object.__setattr__(result, name, value)
        return result


class _AdmittedAdapter:
    def __init__(self, *, profile, binding, transport, evidence_dir, max_requests):
        self.profile = profile
        self.binding = binding
        self.transport = transport
        self.evidence_dir = str(evidence_dir)
        self._max_requests = max_requests
        self._used = 0
        self._gate = threading.Lock()
        self._owners = set()

    def validate_envelope(self, envelope):
        failed = False
        try:
            _validate_envelope(envelope, self.profile, self.binding, self.transport)
            self._validate_scope(envelope)
        except Exception:
            failed = True
        if failed:
            raise CodecFailure("RUNTIME_ADMISSION_REJECTED")

    def _event(self, directory, request_id, state, envelope, **fields):
        name = sha256(request_id.encode("utf-8")).hexdigest() + "." + state.lower() + ".json"
        record = {"schema": "novellibrary.runtime.exchange.v1", "state": state,
                  "request_id": request_id, "wire_sha256": envelope.wire_sha256,
                  "profile_digest": envelope.profile_digest, "binding_digest": self.binding.digest,
                  **fields}
        data = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=directory)
        try:
            position = 0
            while position < len(data):
                amount = os.write(fd, data[position:])
                _require(amount > 0)
                position += amount
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(directory)

    def _claim_budget(self, directory, request_id, envelope):
        """Pilot admission's call cap survives adapter/process replacement."""
        if not isinstance(self, QualifiedRuntimeAdapter):
            return
        prefix = self.qualification.reconciliation_sha256
        for number in range(self._max_requests):
            name = prefix + ".claim." + str(number) + ".json"
            try:
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                             | os.O_CLOEXEC, 0o600, dir_fd=directory)
            except FileExistsError:
                continue
            try:
                data = json.dumps({"request_id": request_id,
                    "wire_sha256": envelope.wire_sha256, "binding_digest": self.binding.digest},
                    separators=(",", ":")).encode("utf-8")
                position = 0
                while position < len(data):
                    amount = os.write(fd, data[position:])
                    _require(amount > 0)
                    position += amount
                os.fsync(fd)
            finally:
                os.close(fd)
            os.fsync(directory)
            return
        raise CodecFailure("PILOT_CALL_CAP_EXHAUSTED")

    async def generate(self, envelope, *, request_id):
        failure = None
        try:
            self.validate_envelope(envelope)
            _require(type(request_id) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", request_id))
            self._validate_request_id(request_id)
        except Exception:
            failure = "RUNTIME_ADMISSION_REJECTED"
        if failure:
            raise RuntimeFailure(failure, request_id=request_id, envelope=envelope, stop_state="not_started")
        if not self._gate.acquire(blocking=False):
            raise RuntimeFailure("CONCURRENT_EXCHANGE", request_id=request_id, envelope=envelope,
                                 stop_state="not_started")
        directory = None
        receipt = None
        caught = None
        cancelled = threading.Event()
        try:
            preparation_failed = False
            try:
                _require(self._used < self._max_requests)
                directory = worker.open_directory(self.evidence_dir)
                self._event(directory, request_id, "PREPARED", envelope)
                self._claim_budget(directory, request_id, envelope)
                self._used += 1
            except Exception:
                preparation_failed = True
            if preparation_failed:
                raise RuntimeFailure("DUPLICATE_OR_INTENT_FAILED", request_id=request_id,
                                     envelope=envelope, stop_state="not_started")

            def before_connect():
                self._event(directory, request_id, "SEND_ATTEMPTED", envelope)

            owner = Future()
            # This result belongs to one joined request thread, independent of
            # asyncio waiters and the application's shared default executor.
            owner.set_running_or_notify_cancel()
            entry_lock = threading.Lock()
            entry_state = "pending"
            request_thread = None
            was_cancelled = False
            start_failed = False

            def exchange_outcome():
                nonlocal entry_state
                with entry_lock:
                    if entry_state == "sealed":
                        return
                    entry_state = "started"
                outcome = None
                try:
                    outcome = self.transport._exchange(envelope, request_id=request_id,
                        model=self.profile.model, cancelled=cancelled, before_connect=before_connect)
                except RuntimeFailure as error:
                    outcome = error.with_traceback(None)
                except BaseException:
                    outcome = RuntimeFailure("OWNER_FAILED", request_id=request_id, envelope=envelope)
                with entry_lock:
                    entry_state = "completed"
                    owner.set_result(outcome)

            self._owners.add(owner)
            # A cancellation already queued before physical launch has a proven
            # no-socket outcome. No thread exists on this path.
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                was_cancelled = True
                cancelled.set()
            if was_cancelled:
                entry_state = "sealed"
                owner.set_result(RuntimeFailure("CANCELLED", request_id=request_id,
                                                envelope=envelope, stop_state="not_started"))
            else:
                request_thread = threading.Thread(target=exchange_outcome,
                    name="novellibrary-raw-request", daemon=False)
                try:
                    request_thread.start()
                except Exception:
                    start_failed = True
                    with entry_lock:
                        if entry_state == "pending":
                            # Seal entry before establishing no-start proof. A
                            # late target can only return without touching transport.
                            entry_state = "sealed"
                            owner.set_result(RuntimeFailure("OWNER_START_FAILED", request_id=request_id,
                                                            envelope=envelope, stop_state="not_started"))
                        else:
                            # Launch failure does not imply no work: a thread may
                            # already own the exchange. Preserve its actual result.
                            cancelled.set()
            waiter = asyncio.wrap_future(owner)
            while True:
                try:
                    outcome = await asyncio.shield(waiter)
                    if type(outcome) is RuntimeFailure:
                        caught = outcome
                    else:
                        receipt = outcome
                    break
                except asyncio.CancelledError:
                    was_cancelled = True
                    cancelled.set()
                    if waiter.cancelled():
                        waiter = asyncio.wrap_future(owner)
                except Exception:
                    caught = RuntimeFailure("OWNER_FAILED", request_id=request_id, envelope=envelope)
                    break
            if request_thread is not None and request_thread.ident is not None:
                request_thread.join()
            self._owners.discard(owner)
            if receipt is not None:
                event_failed = False
                try:
                    self._event(directory, request_id, "CAPTURED", envelope,
                                response_sha256=receipt.response_sha256, model=receipt.model,
                                prompt_eval_count=receipt.prompt_eval_count, eval_count=receipt.eval_count,
                                done_reason=receipt.done_reason, terminal=True, fully_drained=True,
                                cancelled=was_cancelled or start_failed)
                except Exception:
                    event_failed = True
                if was_cancelled or start_failed or event_failed:
                    code = ("OWNER_START_FAILED" if start_failed else
                            "CANCELLED" if was_cancelled else "RECEIPT_WRITE_FAILED")
                    caught = RuntimeFailure(code,
                                            request_id=request_id, envelope=envelope, stop_state="terminal",
                                            response_sha256=receipt.response_sha256)
            elif caught is not None:
                # Failure events do not upgrade the independently established stop state.
                try:
                    safe_diag = _safe_diagnostic(getattr(caught, "diagnostic", None))
                    try:
                        self._event(directory, request_id, "FAILED", envelope, code=caught.code,
                                    stop_proof=None if caught.stop_proof is None else asdict(caught.stop_proof),
                                    diagnostic=safe_diag)
                    except Exception:
                        pass
                except Exception:
                    pass
        finally:
            if directory is not None:
                os.close(directory)
            self._gate.release()
        if caught is not None:
            raise caught
        return receipt

    def _validate_request_id(self, request_id):
        pass


class FrozenTrialAdapter(_AdmittedAdapter):
    def __init__(self, permit: FrozenTrialPermit, *, profile: RequestProfile,
                 transport: RawHttpTransport, evidence_dir: str | Path):
        _require(type(permit) is FrozenTrialPermit)
        self.permit = permit
        super().__init__(profile=profile, binding=permit.binding, transport=transport,
                         evidence_dir=evidence_dir, max_requests=1)

    def _validate_scope(self, envelope):
        p = self.permit
        _require(_envelope_digest(envelope) == p.envelope_digest
                 and envelope.wire_sha256 == p.wire_sha256
                 and envelope.prompt_sha256 == p.prompt_sha256
                 and envelope.token_ids_sha256 == p.token_ids_sha256
                 and envelope.request_tokens == p.request_tokens <= 512
                 and envelope.requested_context_tokens == p.context_tokens <= 4096
                 and envelope.output_reserve_tokens == p.output_tokens <= 1024
                 and self.transport.limits.max_response_bytes <= p.max_response_bytes <= 1_048_576
                 and self.transport.limits.max_output_bytes <= p.max_output_bytes <= 65_536)

    def _validate_request_id(self, request_id):
        _require(request_id == self.permit.request_id)


class QualifiedRuntimeAdapter(_AdmittedAdapter):
    """Only the reconciled, explicitly bounded pilot scope, not blanket approval."""
    def __init__(self, qualification: RuntimeQualification, *, profile: RequestProfile,
                 transport: RawHttpTransport, evidence_dir: str | Path):
        _require(type(qualification) is RuntimeQualification)
        self.qualification = qualification
        super().__init__(profile=profile, binding=qualification.binding, transport=transport,
                         evidence_dir=evidence_dir, max_requests=qualification.max_requests)

    def _validate_scope(self, envelope):
        q = self.qualification
        _require(envelope.request_tokens <= q.max_prompt_tokens
                 and envelope.requested_context_tokens <= q.max_context_tokens
                 and envelope.output_reserve_tokens <= q.max_output_tokens)


def _decode_provider_terminal(data: bytes, model: str) -> dict:
    """Decode one bounded, fully drained provider body; raise _PayloadError(code)."""
    def _object_pairs(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='json',
                                    reason='duplicate_key')
            obj[key] = value
        return obj

    def _nonfinite(_value):
        raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='json',
                            reason='nonfinite_number')

    try:
        text = data.decode('utf-8', errors='strict')
    except UnicodeError:
        raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='utf8',
                            reason='invalid_utf8') from None
    try:
        parsed = json.loads(text, object_pairs_hook=_object_pairs,
                            parse_constant=_nonfinite)
    except _PayloadError:
        raise
    except Exception:
        raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='json',
                            reason='invalid_json') from None

    if type(parsed) is not dict:
        raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='field',
                            reason='invalid_body')

    allowed = {
        'model',
        'created_at',
        'response',
        'thinking',
        'done',
        'done_reason',
        'context',
        'total_duration',
        'load_duration',
        'prompt_eval_count',
        'prompt_eval_cached_count',
        'prompt_eval_duration',
        'eval_count',
        'eval_duration',
    }
    required = {
        'model',
        'response',
        'done',
        'done_reason',
        'prompt_eval_count',
        'eval_count',
    }

    for key in parsed:
        if key not in allowed:
            raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='keys',
                                reason='unknown_key')
    for key in required:
        if key not in parsed:
            raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='keys',
                                reason='missing_key')

    def invalid():
        raise _PayloadError('INVALID_PROVIDER_RESPONSE', stage='field',
                            reason='invalid_field')

    if type(parsed['model']) is not str or parsed['model'] != model:
        invalid()
    if type(parsed['response']) is not str:
        invalid()
    if parsed['done'] is not True:
        invalid()
    if type(parsed['done_reason']) is not str or parsed['done_reason'] not in ('stop', 'length'):
        invalid()

    prompt_eval_count = parsed['prompt_eval_count']
    if type(prompt_eval_count) is not int or prompt_eval_count <= 0:
        invalid()
    eval_count = parsed['eval_count']
    if type(eval_count) is not int or eval_count < 0:
        invalid()

    if 'prompt_eval_cached_count' in parsed:
        cached = parsed['prompt_eval_cached_count']
        if type(cached) is not int or cached < 0 or cached > prompt_eval_count:
            invalid()

    if 'thinking' in parsed:
        thinking = parsed['thinking']
        if type(thinking) is not str or thinking != '':
            invalid()

    if 'created_at' in parsed:
        created_at = parsed['created_at']
        if type(created_at) is not str or not created_at or len(created_at) > 128:
            invalid()

    for key in ('total_duration', 'load_duration', 'prompt_eval_duration', 'eval_duration'):
        if key in parsed:
            value = parsed[key]
            if type(value) is not int or value < 0:
                invalid()

    if 'context' in parsed:
        context = parsed['context']
        if type(context) is not list or len(context) > 1000000:
            invalid()
        for value in context:
            if type(value) is not int or value < 0 or value > 4294967295:
                invalid()

    return parsed
