"""One bounded public HTTPS hop; no scheduler, publication or activation authority.

The mandatory trusted outer supervisor must own a killable fetch child and enforce
the total deadline, including blocking DNS and TLS/HTTP internals. Socket timeouts
and this protocol are not that implementation. B3b must supply durable rate and
inflight admission before this adapter can be activated.
"""
from dataclasses import dataclass, field
import fcntl
import functools
import hashlib
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import ssl
import stat
import threading
import time
from typing import Protocol
from urllib.parse import parse_qsl, urlsplit, urlunsplit, urljoin


METADATA_BYTES = 1024**2
COVER_BYTES = 8 * 1024**2
SEARCH_FIELDS = 'key,title,author_name,isbn,edition_key,language,publisher,cover_i'


class FetchError(ValueError):
    """Codes only: never retain response bytes, credentials or exception text."""
    def __init__(self, code, *, send_attempted=False, bytes_written=0):
        super().__init__(code)
        self.code = code
        self.send_attempted = send_attempted
        self.bytes_written = bytes_written


class OwnedSupervisor(Protocol):
    def assert_owned_deadline(self, deadline: float) -> None:
        """Require active owned-child hard deadline, including blocking DNS."""


@dataclass(frozen=True)
class FetchRequest:
    url: str
    kind: str = 'metadata'

    @property
    def digest(self):
        return hashlib.sha256((self.kind + '\n' + self.url).encode()).hexdigest()


@dataclass(frozen=True)
class PublicTarget:
    url: str
    host: str
    path_query: str
    purpose: str


@dataclass(frozen=True)
class OpenLibraryPolicy:
    """Fixed server endpoints; caller URLs cannot add hosts or query options."""
    provider_id: str = field(default='openlibrary', init=False)

    def validate(self, request):
        value = request.url
        if (not isinstance(value, str) or not 1 <= len(value) <= 2048
                or not value.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in value)
                or '\\' in value or '#' in value):
            raise FetchError('url_rejected')
        try:
            parts = urlsplit(value)
            host = parts.hostname
            if (parts.scheme != 'https' or host not in {'openlibrary.org', 'covers.openlibrary.org'}
                    or parts.netloc != host or parts.username or parts.password or parts.fragment):
                raise ValueError
            if re.search(r'%(?![0-9A-Fa-f]{2})', parts.query):
                raise ValueError
            pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True, errors='strict')
            if len(pairs) > 6 or len(dict(pairs)) != len(pairs):
                raise ValueError
            query = dict(pairs)
            if any(any(ord(c) < 32 or ord(c) == 127 for c in v) or '\\' in v
                   or re.search(r'https?://', v, re.I) for _, v in pairs):
                raise ValueError
            if request.kind == 'metadata' and host == 'openlibrary.org':
                if parts.path == '/search.json':
                    if (set(query) - {'title', 'author', 'isbn', 'limit', 'fields'}
                            or query.get('limit') != '5' or query.get('fields') != SEARCH_FIELDS
                            or not any(query.get(k) for k in ('title', 'isbn'))
                            or any(len(query.get(k, '')) > 512 for k in ('title', 'author', 'isbn'))):
                        raise ValueError
                    purpose = 'search'
                elif re.fullmatch(r'/(?:books/OL[1-9][0-9]*M|works/OL[1-9][0-9]*W)\.json', parts.path) and not query:
                    purpose = 'detail'
                else:
                    raise ValueError
            elif (request.kind == 'cover' and host == 'covers.openlibrary.org'
                  and re.fullmatch(r'/b/(?:id/[1-9][0-9]*|olid/OL[1-9][0-9]*M)-L\.jpg', parts.path)
                  and query == {'default': 'false'}):
                purpose = 'cover'
            else:
                raise ValueError
        except (ValueError, UnicodeError):
            raise FetchError('url_rejected') from None
        canonical = urlunsplit(('https', host, parts.path, parts.query, ''))
        return PublicTarget(canonical, host, parts.path + ('?' + parts.query if parts.query else ''), purpose)

    def redirect(self, previous, location, kind):
        if not isinstance(location, str) or location.startswith('//'):
            raise FetchError('redirect_rejected')
        target = self.validate(FetchRequest(urljoin(previous.url, location), kind))
        if target.host != previous.host or target.purpose != previous.purpose:
            raise FetchError('redirect_rejected')
        return target.url


@dataclass
class LookupBudget:
    """Per-lookup caps only; durable cross-worker rate admission is B3b."""
    started_at: float
    exchanges: int = field(default=0, init=False)
    redirects: int = field(default=0, init=False)
    _purposes: set = field(default_factory=set, init=False, repr=False)
    _redirect: str | None = field(default=None, init=False, repr=False)
    _active: bool = field(default=False, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.started_at, (int, float)) or not math.isfinite(self.started_at):
            raise FetchError('invalid_deadline')

    @property
    def deadline(self):
        return self.started_at + 20

    def admit(self, target, now):
        with self._lock:
            if now >= self.deadline or self.exchanges >= 6 or self._active:
                raise FetchError('lookup_budget')
            if self._redirect is not None:
                if target.url != self._redirect or self.redirects >= 3:
                    raise FetchError('redirect_budget')
                self.redirects += 1
                self._redirect = None
            elif target.purpose in self._purposes:
                raise FetchError('duplicate_lookup_stage')
            else:
                self._purposes.add(target.purpose)
            self.exchanges += 1
            self._active = True


@dataclass(frozen=True)
class StorageGrant:
    """Trusted B3b prefetch permission seam, never evidence minted by HTTP."""
    resource_url: str
    valid_until: float
    policy_digest: str

    def valid(self, url, now):
        return (self.resource_url == url and math.isfinite(self.valid_until) and now < self.valid_until
                and isinstance(self.policy_digest, str) and re.fullmatch('[0-9a-f]{64}', self.policy_digest))


@dataclass
class FetchPermit:
    """Single-use hop token supplied by trusted rate writer, not an auth token.

    Constructing this object does not prove durable provider rate or rights. B3b
    must bind its request digest and lifetime to an actual owned writer receipt.
    """
    request_digest: str
    lookup: LookupBudget
    not_before: float
    expires_at: float
    storage: StorageGrant | None = None
    _used: bool = field(default=False, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def consume(self, request, target, now):
        with self._lock:
            if (self._used or self.request_digest != request.digest
                    or not all(math.isfinite(v) for v in (self.not_before, self.expires_at))
                    or not self.not_before <= now < self.expires_at <= self.lookup.deadline):
                raise FetchError('invalid_rate_permit')
            self._used = True
            self.lookup.admit(target, now)


def _public_address(value):
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise FetchError('dns_rejected') from None
    if (not address.is_global or address.is_reserved or address.is_multicast
            or address.is_unspecified or address.is_loopback or address.is_link_local
            or getattr(address, 'is_site_local', False)
            or '%' in value or getattr(address, 'ipv4_mapped', None) is not None):
        raise FetchError('dns_rejected')
    return address


class _FramingBudget:
    """Limits stdlib line reads; http.client remains the HTTP framing parser."""
    def __init__(self, stream, wire_limit):
        self.stream, self.line_bytes = stream, 0
        self.headers_pending, self.wire_bytes, self.wire_limit = True, 0, wire_limit
        self.require_trailer_crlf = False

    def _count(self, value):
        if not self.headers_pending:
            self.wire_bytes += len(value)
            if self.wire_bytes > self.wire_limit:
                raise FetchError('encoded_body_limit')
        return value

    def _amount(self, amount):
        cap = 65537 if self.headers_pending else self.wire_limit - self.wire_bytes + 1
        return min(amount, cap) if amount >= 0 else cap

    def read(self, amount=-1):
        return self._count(self.stream.read(self._amount(amount)))

    def readinto(self, buffer):
        value = self.read(len(buffer))
        buffer[:len(value)] = value
        return len(value)

    def readline(self, limit=-1):
        value = self._count(self.stream.readline(self._amount(min(8193, limit) if limit >= 0 else 8193)))
        self.line_bytes += len(value)
        if len(value) > 8192 or self.line_bytes > 65536 or value.startswith((b' ', b'\t')):
            raise FetchError('header_or_framing_limit')
        if not value.endswith(b'\r\n'):
            raise FetchError('incomplete_chunk_trailer' if self.require_trailer_crlf
                             else 'incomplete_http_framing_line')
        return value

    def __getattr__(self, name):
        return getattr(self.stream, name)


class _BoundedResponse(http.client.HTTPResponse):
    def __init__(self, *args, wire_limit, **kwargs):
        super().__init__(*args, **kwargs)
        self.framing_budget = self.fp = _FramingBudget(self.fp, wire_limit)

    def begin(self):
        super().begin()
        self.framing_budget.headers_pending = False

    def _safe_read(self, amount):
        # Python3.14's chunk loop consumes the data terminator through this
        # method with chunk_left==0. Keep its size/extension/payload parser;
        # only tighten the unchecked CRLF discard, not payload bytes of size2.
        value = super()._safe_read(amount)
        if self.chunked and self.chunk_left == 0 and (amount != 2 or value != b'\r\n'):
            raise FetchError('invalid_chunk_separator')
        return value

    def _read_and_discard_trailer(self):
        # Stdlib permits EOF here. Require each trailer line, including the
        # terminal blank line, to end in CRLF at the same bounded read seam.
        self.framing_budget.require_trailer_crlf = True
        try:
            return super()._read_and_discard_trailer()
        finally:
            self.framing_budget.require_trailer_crlf = False


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, target, address, family, *, timeout, context, socket_factory, wire_limit):
        super().__init__(target.host, 443, timeout=timeout, context=context)
        self._address, self._family, self._socket_factory = address, family, socket_factory
        self.response_class = functools.partial(_BoundedResponse, wire_limit=wire_limit)

    def connect(self):
        raw = self._socket_factory(self._family, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        wrapped = None
        try:
            raw.settimeout(self.timeout)
            numeric = str(self._address)
            raw.connect((numeric, 443) if self._family == socket.AF_INET else (numeric, 443, 0, 0))
            if ipaddress.ip_address(raw.getpeername()[0]) != self._address:
                raise FetchError('peer_mismatch')
            wrapped = self._context.wrap_socket(raw, server_hostname=self.host, suppress_ragged_eofs=False)
            if ipaddress.ip_address(wrapped.getpeername()[0]) != self._address:
                raise FetchError('peer_mismatch')
            self.sock = wrapped
        except BaseException:
            if wrapped is not None:
                wrapped.close()
            raw.close()
            raise


def decode_metadata_json(body):
    """One strict bounded transient decoder shared by fetch and fixture parsers."""
    if not isinstance(body, bytes) or len(body) > METADATA_BYTES:
        raise FetchError('json_bytes')

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise FetchError('duplicate_json_key')
            result[key] = value
        return result

    def constant(_):
        raise FetchError('nonfinite_json')

    try:
        value = json.loads(body.decode('utf-8'), object_pairs_hook=pairs, parse_constant=constant)
        todo, nodes = [(value, 0)], 0
        while todo:
            item, depth = todo.pop()
            nodes += 1
            if nodes > 10000 or depth > 24:
                raise FetchError('json_shape')
            if isinstance(item, dict):
                if len(item) > 1000 or any(len(k) > 256 for k in item):
                    raise FetchError('json_shape')
                todo.extend((v, depth + 1) for v in item.values())
            elif isinstance(item, list):
                if len(item) > 100:
                    raise FetchError('json_shape')
                todo.extend((v, depth + 1) for v in item)
            elif isinstance(item, str) and len(item) > 8192:
                raise FetchError('json_string')
            elif isinstance(item, float) and not math.isfinite(item):
                raise FetchError('nonfinite_json')
        if not isinstance(value, dict):
            raise FetchError('json_object')
        return value
    except (UnicodeError, ValueError, RecursionError) as error:
        if isinstance(error, FetchError):
            raise
    # Raise outside the handler: 'from None' hides display, but retains context.
    raise FetchError('invalid_json') from None


@dataclass(frozen=True)
class FetchHopReceipt:
    url: str
    request_digest: str
    status: int
    selected_ip: str
    headers: tuple
    byte_count: int
    encoded_byte_count: int
    body_sha256: str
    response_complete: bool
    redirect_url: str | None
    ephemeral_json: dict | None = field(default=None, repr=False, compare=False)
    local_http_closed: bool = True

    def durable_facts(self):
        """No body/description/image/header values, even on unknown rights/errors."""
        return {k: getattr(self, k) for k in ('url', 'request_digest', 'status', 'selected_ip',
                                             'byte_count', 'encoded_byte_count', 'body_sha256', 'response_complete',
                                             'redirect_url', 'local_http_closed')}


class SafePublicFetcher:
    def __init__(self, *, supervisor: OwnedSupervisor, resolver=socket.getaddrinfo,
                 socket_factory=socket.socket, context_factory=ssl.create_default_context,
                 clock=time.monotonic):
        if not callable(getattr(supervisor, 'assert_owned_deadline', None)):
            raise TypeError('Trusted outer owned supervisor required')
        self.supervisor, self.resolver = supervisor, resolver
        self.socket_factory, self.context_factory, self.clock = socket_factory, context_factory, clock

    def fetch_hop(self, policy, request, *, permit, output_fd=None):
        if type(policy) is not OpenLibraryPolicy or not isinstance(permit, FetchPermit):
            raise FetchError('server_policy_and_rate_permit_required')
        target = policy.validate(request)
        deadline = permit.lookup.deadline

        def remaining():
            self.supervisor.assert_owned_deadline(deadline)
            value = deadline - self.clock()
            if value <= 0:
                raise FetchError('lookup_deadline')
            return min(5, value)

        remaining()
        if output_fd is not None:
            if request.kind != 'cover' or permit.storage is None or not permit.storage.valid(target.url, self.clock()):
                raise FetchError('prefetch_storage_permission_required')
            s = os.fstat(output_fd)
            flags = fcntl.fcntl(output_fd, fcntl.F_GETFL)
            if (not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_size != 0
                    or flags & os.O_ACCMODE != os.O_WRONLY or flags & os.O_APPEND
                    or os.lseek(output_fd, 0, os.SEEK_CUR) != 0):
                raise FetchError('empty_owned_write_fd_required')
        elif request.kind == 'cover':
            raise FetchError('cover_stage_fd_required')
        permit.consume(request, target, self.clock())
        connection = response = None
        attempted, written = False, 0
        body_failed = False
        failure_code = None
        try:
            # DNS may block: external owned deadline is a mandatory dependency.
            answers = self.resolver(target.host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP)
            remaining()
            if not answers or len(answers) > 16:
                raise FetchError('dns_answer_limit')
            vetted = []
            for family, kind, proto, _, sockaddr in answers:
                if family not in {socket.AF_INET, socket.AF_INET6} or kind != socket.SOCK_STREAM or proto != socket.IPPROTO_TCP:
                    raise FetchError('dns_rejected')
                address = _public_address(sockaddr[0])
                if (address.version == 4) != (family == socket.AF_INET) or sockaddr[1] != 443:
                    raise FetchError('dns_rejected')
                vetted.append((family, address))
            family, address = vetted[0]
            context = self.context_factory()
            if context.verify_mode != ssl.CERT_REQUIRED or context.check_hostname is not True:
                raise FetchError('verified_tls_required')
            connection = _PinnedHTTPSConnection(target, address, family, timeout=remaining(),
                                                context=context, socket_factory=self.socket_factory,
                                                wire_limit=COVER_BYTES if request.kind == 'cover' else METADATA_BYTES)
            attempted = True
            connection.request('GET', target.path_query, headers={
                'Host': target.host, 'User-Agent': 'NovelLibrary/0.1', 'Accept-Encoding': 'identity',
                'Accept': 'application/json' if request.kind == 'metadata' else 'image/jpeg,image/png,image/webp',
                'Connection': 'close'})
            response = connection.getresponse()
            headers = response.getheaders()
            if response.msg.defects or len(headers) > 100:
                raise FetchError('invalid_headers')
            values = {}
            for name, value in headers:
                if (not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name)
                        or any(ord(c) < 32 and c != '\t' or ord(c) == 127 for c in value)):
                    raise FetchError('invalid_headers')
                values.setdefault(name.lower(), []).append(value.strip())
            if any(len(values.get(k, [])) > 1 for k in ('content-length', 'transfer-encoding', 'content-type', 'location', 'content-encoding')):
                raise FetchError('ambiguous_headers')
            one = {k: v[0] for k, v in values.items()}
            if (one.get('content-encoding', 'identity').lower() != 'identity'
                    or 'transfer-encoding' in one and one['transfer-encoding'].lower() != 'chunked'
                    or 'content-length' in one and 'transfer-encoding' in one):
                raise FetchError('unsupported_framing')
            if response.chunked != (one.get('transfer-encoding', '').lower() == 'chunked'):
                raise FetchError('parser_framing_mismatch')
            cap = COVER_BYTES if request.kind == 'cover' else METADATA_BYTES
            declared = one.get('content-length')
            if declared is not None and (not re.fullmatch('[0-9]+', declared) or len(declared) > 20):
                raise FetchError('body_limit')
            status = response.status
            if not 200 <= status <= 599:
                raise FetchError('invalid_status')
            redirect = status in {301, 302, 303, 307, 308}
            redirect_url = policy.redirect(target, one.get('location'), request.kind) if redirect else None
            ok = status == 200
            if ok and declared is not None and int(declared) > cap:
                raise FetchError('body_limit')
            content_type = one.get('content-type', '').split(';', 1)[0].lower()
            if ok and content_type not in ({'application/json'} if request.kind == 'metadata' else {'image/jpeg', 'image/png', 'image/webp'}):
                raise FetchError('content_type')
            read_cap = cap if ok else 4096
            captured, hasher, count, complete = bytearray(), hashlib.sha256(), 0, False
            while True:
                if not ok and count == read_cap:
                    complete = response.length == 0
                    break
                connection.sock.settimeout(remaining()) if connection.sock is not None else None
                chunk = response.read(min(65536, read_cap + (1 if ok else 0) - count))
                if not chunk:
                    complete = True
                    break
                count += len(chunk)
                if count > read_cap:
                    if ok:
                        raise FetchError('body_limit')
                    break
                hasher.update(chunk)
                if ok and output_fd is not None:
                    if not permit.storage.valid(target.url, self.clock()):
                        raise FetchError('storage_permission_expired')
                    view = memoryview(chunk)
                    while view:
                        n = os.write(output_fd, view)
                        if n <= 0:
                            raise FetchError('stage_write_failed')
                        written += n
                        view = view[n:]
                elif ok:
                    captured.extend(chunk)
            remaining()
            if complete and declared is not None and count != int(declared):
                raise FetchError('incomplete_body')
            ephemeral = decode_metadata_json(bytes(captured)) if ok and request.kind == 'metadata' else None
            allowed = {'content-type', 'content-length', 'location', 'retry-after', 'cache-control', 'expires', 'etag', 'last-modified'}
            selected = tuple((k, v) for k, v in one.items() if k in allowed and len(v) <= 2048)
            if redirect_url is not None:
                permit.lookup._redirect = redirect_url
            receipt = FetchHopReceipt(target.url, request.digest, status, str(address), selected,
                                   count, response.framing_budget.wire_bytes, hasher.hexdigest(), complete, redirect_url, ephemeral)
        except (OSError, http.client.HTTPException, ValueError) as error:
            body_failed = True
            failure_code = error.code if isinstance(error, FetchError) else 'transport_failed'
        except BaseException:
            body_failed = True
            raise
        finally:
            try:
                try:
                    if response is not None:
                        response.close()
                finally:
                    try:
                        if connection is not None:
                            connection.close()
                    finally:
                        with permit.lookup._lock:
                            permit.lookup._active = False
            except Exception:
                if not body_failed:
                    failure_code = 'cleanup_failed'
        if failure_code is not None:
            raise FetchError(failure_code, send_attempted=attempted, bytes_written=written) from None
        return receipt
