"""Controlled stdlib wire/socket/TLS fixtures; never a real DNS/TLS request."""
import dataclasses
import io
import json
import os
from pathlib import Path
import socket
import ssl
import tempfile
import unittest
from unittest.mock import patch
import document_library.safe_public_fetch as fetch_module

from document_library.safe_public_fetch import (
    COVER_BYTES, METADATA_BYTES, FetchError, FetchPermit, FetchRequest,
    LookupBudget, OpenLibraryPolicy, SafePublicFetcher, StorageGrant,
    decode_metadata_json,
)


DETAIL = 'https://openlibrary.org/books/OL1M.json'
COVER = 'https://covers.openlibrary.org/b/id/1-L.jpg?default=false'
PUBLIC = '93.184.216.34'


class Supervisor:
    def __init__(self):
        self.calls = []

    def assert_owned_deadline(self, deadline):
        self.calls.append(deadline)


class WireSocket:
    def __init__(self, wire, peer=PUBLIC):
        self.wire, self.peer, self.closed = wire, peer, False
        self.connected, self.sent, self.streams, self.timeouts = [], [], [], []

    def settimeout(self, value):
        self.timeouts.append(value)

    def connect(self, target):
        self.connected.append(target)

    def getpeername(self):
        return (self.peer, 443)

    def sendall(self, value):
        self.sent.append(bytes(value))

    def makefile(self, mode):
        stream = io.BytesIO(self.wire)
        self.streams.append(stream)
        return stream

    def close(self):
        self.closed = True


class TLSContext:
    verify_mode = ssl.CERT_REQUIRED
    check_hostname = True

    def __init__(self, *, error=None, changed_peer=None):
        self.names, self.error, self.changed_peer = [], error, changed_peer
        self.eof_options = []

    def wrap_socket(self, raw, *, server_hostname, suppress_ragged_eofs=True):
        self.names.append(server_hostname)
        self.eof_options.append(suppress_ragged_eofs)
        if self.error:
            raise self.error
        if self.changed_peer:
            raw.peer = self.changed_peer
        return raw


def wire(body=b'{}', *, status=200, headers=(), length=True):
    fields = [('Content-Type', 'application/json'), *headers]
    if length:
        fields.append(('Content-Length', str(len(body))))
    return (f'HTTP/1.1 {status} Fixture\r\n'.encode()
            + b''.join(f'{k}: {v}\r\n'.encode() for k, v in fields) + b'\r\n' + body)


class FetchTests(unittest.TestCase):
    def setUp(self):
        self.supervisor, self.dns, self.sockets = Supervisor(), [], []
        self.now, self.answers, self.context = 10.0, [PUBLIC], TLSContext()
        self.responses = [wire()]

        def resolve(*args):
            self.dns.append(args)
            return [(socket.AF_INET6 if ':' in a else socket.AF_INET, socket.SOCK_STREAM,
                     socket.IPPROTO_TCP, '', (a, 443)) for a in self.answers]

        def make(*args):
            result = WireSocket(self.responses.pop(0))
            self.sockets.append(result)
            return result

        self.fetcher = SafePublicFetcher(supervisor=self.supervisor, resolver=resolve,
            socket_factory=make, context_factory=lambda: self.context, clock=lambda: self.now)
        self.policy = OpenLibraryPolicy()

    def permit(self, request, *, budget=None, storage=None):
        return FetchPermit(request.digest, budget or LookupBudget(self.now), self.now, self.now+20, storage)

    def fetch(self, request=None, *, permit=None, output_fd=None):
        request = request or FetchRequest(DETAIL)
        return self.fetcher.fetch_hop(self.policy, request, permit=permit or self.permit(request), output_fd=output_fd)

    def assert_closed(self):
        for s in self.sockets:
            self.assertTrue(s.closed)
            self.assertTrue(all(f.closed for f in s.streams))

    def test_numeric_connect_original_host_sni_no_ambient_proxy_or_cookie(self):
        self.responses = [wire(b'{"description":"PRIVATE-PROSE"}')]
        with patch.dict(os.environ, {'HTTPS_PROXY':'http://127.0.0.1:9','HTTP_PROXY':'http://127.0.0.1:9'}):
            receipt = self.fetch()
        self.assertEqual(len(self.dns), 1)
        self.assertEqual(self.sockets[0].connected, [(PUBLIC, 443)])
        self.assertEqual(self.context.names, ['openlibrary.org'])
        sent = b''.join(self.sockets[0].sent)
        self.assertIn(b'Host: openlibrary.org\r\n', sent)
        self.assertIn(b'Accept-Encoding: identity\r\n', sent)
        self.assertNotIn(b'Cookie', sent)
        self.assertNotIn(b'Authorization', sent)
        self.assertNotIn('PRIVATE-PROSE', repr(receipt))
        self.assertNotIn('PRIVATE-PROSE', json.dumps(receipt.durable_facts()))
        self.assertTrue(receipt.response_complete)
        self.assertTrue(receipt.local_http_closed)
        self.assertTrue(self.supervisor.calls)
        self.assert_closed()

    def test_url_ambiguities_fail_before_dns(self):
        bad = ['http://openlibrary.org/books/OL1M.json', 'https://127.0.0.1/x',
            'https://openlibrary.org.:443/books/OL1M.json', 'https://openlibrary.org:444/books/OL1M.json',
            'https://user@openlibrary.org/books/OL1M.json', 'https://OPENLIBRARY.org/books/OL1M.json',
            'https://openlibrary.org/books/%4fL1M.json', DETAIL+'#x', DETAIL+'?url=https%3A%2F%2Fevil.test',
            'https://openlibrary.org\\@evil.test/books/OL1M.json', DETAIL+'\nX: y',
            'https://openlіbrary.org/books/OL1M.json', 'https://[fe80::1%25en0]/x']
        for value in bad:
            with self.subTest(value=value), self.assertRaises(FetchError):
                self.fetch(FetchRequest(value))
        self.assertFalse(self.dns)

    def test_all_dns_answers_must_be_public_not_just_selected(self):
        denied = ['.'.join(map(str, octets)) for octets in
                  ((127,0,0,1),(10,0,0,1),(172,16,0,1),(192,168,2,1),(169,254,1,1),
                   (100,64,0,1),(192,0,2,1),(224,1,1,1),(0,0,0,0))]
        denied += ['::1','fe80::1','fc00::1','2001:db8::1','ff02::1',
                   '::ffff:'+'.'.join(map(str,(127,0,0,1)))]
        for address in denied:
            with self.subTest(address=address), self.assertRaises(FetchError):
                self.answers = [PUBLIC, address]
                self.fetch()
        self.assertFalse(self.sockets)

    def test_ipv6_site_local_range_and_public_first_mix_rejected_before_socket(self):
        for address in ('fec0::','fec0::1','feff:ffff::1','feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'):
            for answers in ([address],[PUBLIC,address]):
                self.answers = answers
                with self.subTest(answers=answers), self.assertRaisesRegex(FetchError,'dns_rejected'):
                    self.fetch()
                self.assertFalse(self.sockets)

    def test_TE_parser_disagreement_rejected_before_metadata_or_cover_body(self):
        for suffix in (' ','\t'):
            self.responses = [wire(b'{}',headers=[('Transfer-Encoding','chunked'+suffix)],length=False)]
            with self.subTest(suffix=suffix), self.assertRaisesRegex(FetchError,'parser_framing_mismatch'):
                self.fetch()
            # Both an individually oversized extension and cumulative framing
            # overflow must not be staged as opaque cover bytes.
            for chunked in ((b'1;'+b'x'*8000+b'\r\nx\r\n')*9+b'0\r\n\r\n',
                            b'1;'+b'x'*8193+b'\r\nx\r\n0\r\n\r\n'):
                self.responses = [wire(chunked,headers=[('Transfer-Encoding','chunked'+suffix)],length=False)
                                  .replace(b'Content-Type: application/json',b'Content-Type: image/jpeg')]
                request = FetchRequest(COVER,'cover')
                with tempfile.TemporaryDirectory() as root:
                    fd = os.open(Path(root)/'stage',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
                    try:
                        with self.assertRaisesRegex(FetchError,'parser_framing_mismatch'):
                            self.fetch(request,permit=self.permit(request,storage=StorageGrant(COVER,30,'a'*64)),output_fd=fd)
                        self.assertEqual(os.fstat(fd).st_size,0)
                    finally:
                        os.close(fd)
        self.assert_closed()

    def test_header_EOF_and_unterminated_zero_chunk_line_rejected(self):
        for value in (b'HTTP/1.1 404 Not Found\r\n',
                      b'HTTP/1.1 404 Not Found\r\nX: unfinished',
                      wire(b'2\r\n{}\r\n0',headers=[('Transfer-Encoding','chunked')],length=False)):
            self.responses = [value]
            with self.subTest(value=value), self.assertRaises(FetchError):
                self.fetch()
            self.assert_closed()

    def test_TLS_ragged_EOF_option_and_controlled_close_delimited_failure(self):
        self.fetch()
        self.assertEqual(self.context.eof_options,[False])
        self.assertEqual(self.context.names,['openlibrary.org'])
        context = self.context

        class RaggedStream(io.BytesIO):
            def read(self, amount=-1):
                if self.tell() == len(self.getvalue()) and context.eof_options[-1] is False:
                    raise ssl.SSLEOFError('synthetic missing TLS close_notify')
                return super().read(amount)

        class RaggedSocket(WireSocket):
            def makefile(self, mode):
                stream = RaggedStream(self.wire)
                self.streams.append(stream)
                return stream

        def make(*_):
            result = RaggedSocket(wire(b'{}',length=False))
            self.sockets.append(result)
            return result

        self.fetcher.socket_factory = make
        with self.assertRaisesRegex(FetchError,'transport_failed'):
            self.fetch()
        self.assert_closed()

    def test_nested_close_failures_still_close_connection_and_release_lookup(self):
        response_close = fetch_module._BoundedResponse.close
        connection_close = fetch_module._PinnedHTTPSConnection.close
        for fail_response,fail_connection,invalid_body in (
                (a,b,c) for a,b in ((True,False),(False,True),(True,True)) for c in (False,True)):
            calls = []

            def close_response(response):
                if fail_response and not getattr(response,'_fixture_close_failed',False):
                    response._fixture_close_failed = True
                    raise OSError('synthetic response close failure')
                return response_close(response)

            def close_connection(connection):
                calls.append('connection_close')
                connection_close(connection)
                if fail_connection:
                    raise OSError('synthetic connection close failure')

            self.responses = [wire(b'not-json') if invalid_body else wire(b'x'*5000,status=500)]
            request,budget = FetchRequest(DETAIL),LookupBudget(10)
            with (patch.object(fetch_module._BoundedResponse,'close',close_response),
                  patch.object(fetch_module._PinnedHTTPSConnection,'close',close_connection)):
                with self.assertRaisesRegex(FetchError,'invalid_json' if invalid_body else 'cleanup_failed') as caught:
                    self.fetch(request,permit=self.permit(request,budget=budget))
                self.assertTrue(caught.exception.send_attempted)
                self.assertEqual(caught.exception.bytes_written,0)
                self.assertNotIn('synthetic',str(caught.exception))
                self.assertIsNone(caught.exception.__context__)
                self.assertIsNone(caught.exception.__cause__)
            self.assertTrue(calls)
            self.assertFalse(budget._active)
            self.assert_closed()

    def test_bounded_dns_set_and_family_consistency(self):
        for addresses in ([], [PUBLIC]*17):
            self.answers = addresses
            with self.assertRaises(FetchError):
                self.fetch()
        self.assertFalse(self.sockets)

    def test_peer_mismatch_and_certificate_failure_close_without_body(self):
        for context in [TLSContext(error=ssl.SSLCertVerificationError('synthetic cert mismatch')),
                        TLSContext(changed_peer='8.8.8.8')]:
            with self.subTest(context=context), self.assertRaises(FetchError):
                self.context = context
                self.responses = [wire()]
                self.fetch()
        self.assert_closed()

    def test_unverified_tls_context_rejected_before_connect(self):
        self.context.check_hostname = False
        with self.assertRaisesRegex(FetchError, 'verified_tls_required'):
            self.fetch()
        self.assertFalse(self.sockets)

    def test_mandatory_supervisor_and_expired_or_mismatched_single_use_permit(self):
        with self.assertRaises(TypeError):
            SafePublicFetcher(supervisor=None)
        request = FetchRequest(DETAIL)
        for permit in [FetchPermit('0'*64, LookupBudget(10), 10, 30),
                       FetchPermit(request.digest, LookupBudget(10), 11, 30)]:
            with self.assertRaises(FetchError):
                self.fetch(request, permit=permit)
        permit = self.permit(request)
        self.fetch(request, permit=permit)
        with self.assertRaisesRegex(FetchError, 'invalid_rate_permit'):
            self.fetch(request, permit=permit)
        self.assertEqual(len(self.dns), 1)

    def test_deadline_expired_during_dns_prevents_socket_creation(self):
        def resolver(*_):
            self.now = 31
            return [(socket.AF_INET,socket.SOCK_STREAM,socket.IPPROTO_TCP,'',(PUBLIC,443))]
        self.fetcher.resolver = resolver
        with self.assertRaisesRegex(FetchError, 'lookup_deadline'):
            self.fetch()
        self.assertFalse(self.sockets)
        # This is a returned-DNS deadline check, not proof of interruptible DNS.

    def test_redirect_is_visible_and_requires_fresh_permit_and_bounded_hops(self):
        self.responses = [wire(status=302, headers=[('Location','/books/OL2M.json')])]
        request = FetchRequest(DETAIL)
        budget, permit = LookupBudget(10), None
        first = self.fetch(request, permit=self.permit(request, budget=budget))
        self.assertEqual(first.redirect_url, 'https://openlibrary.org/books/OL2M.json')
        self.assertEqual(len(self.dns), 1)
        for number in (2,3,4):
            request = FetchRequest(f'https://openlibrary.org/books/OL{number}M.json')
            self.responses = [wire(status=302, headers=[('Location',f'/books/OL{number+1}M.json')])]
            self.fetch(request, permit=self.permit(request, budget=budget))
        request = FetchRequest('https://openlibrary.org/books/OL5M.json')
        with self.assertRaisesRegex(FetchError, 'redirect_budget'):
            self.fetch(request, permit=self.permit(request, budget=budget))
        self.assertEqual(len(self.dns), 4)
        self.assert_closed()

    def test_redirect_off_policy_or_protocol_relative_never_followed(self):
        for location in ['https://127.0.0.1/x','https://evil.test/x','//openlibrary.org/books/OL2M.json',
                         'http://openlibrary.org/books/OL2M.json','https://openlibrary.org:444/books/OL2M.json',
                         '/search.json','https://u@openlibrary.org/books/OL2M.json']:
            self.responses = [wire(status=302, headers=[('Location',location)])]
            with self.subTest(location=location), self.assertRaises(FetchError):
                self.fetch()
        self.assertEqual(len(self.dns), 7)
        self.assert_closed()

    def test_six_exchange_cap_and_one_of_each_logical_stage(self):
        budget = LookupBudget(10)
        request = FetchRequest(DETAIL)
        self.fetch(request, permit=self.permit(request, budget=budget))
        with self.assertRaisesRegex(FetchError, 'duplicate_lookup_stage'):
            self.fetch(FetchRequest('https://openlibrary.org/books/OL2M.json'),
                       permit=self.permit(FetchRequest('https://openlibrary.org/books/OL2M.json'), budget=budget))
        capped = LookupBudget(10)
        capped.exchanges = 6
        with self.assertRaisesRegex(FetchError, 'lookup_budget'):
            self.fetch(request, permit=self.permit(request, budget=capped))

    def test_stream_caps_absent_chunked_and_overdeclared_lengths(self):
        cases = [wire(b'x'*(METADATA_BYTES+1), length=False),
                 wire(b'', headers=[('Content-Length',str(METADATA_BYTES+1))], length=False),
                 wire(b'{}', headers=[('Content-Length','50')], length=False),
                 wire(b'100001\r\n'+b'x'*(METADATA_BYTES+1)+b'\r\n0\r\n\r\n',
                      headers=[('Transfer-Encoding','chunked')], length=False)]
        for value in cases:
            self.responses = [value]
            with self.subTest(size=len(value)), self.assertRaises(FetchError):
                self.fetch()
        self.assert_closed()

    def test_header_compression_and_ambiguous_framing_bounds(self):
        cases = [wire(headers=[('Content-Encoding','gzip')]),
                 wire(headers=[('Content-Length','2')]),
                 wire(headers=[('Transfer-Encoding','chunked')]),
                 wire(headers=[('X-Big','a'*8193)]),
                 wire(headers=[(f'X-{i}','a'*1000) for i in range(70)]),
                 b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n X: folded\r\n\r\n{}',
                 wire(headers=[(f'X-{i}','x') for i in range(101)])]
        for value in cases:
            self.responses = [value]
            with self.subTest(size=len(value)), self.assertRaises(FetchError):
                self.fetch()
        self.assert_closed()

    def test_interim_100_and_final_headers_share_read_boundary_budget(self):
        interim = b'HTTP/1.1 100 Continue\r\nX-Pad: '+b'a'*1000+b'\r\n\r\n'
        self.responses = [interim*70+wire()]
        with self.assertRaisesRegex(FetchError,'header_or_framing_limit'):
            self.fetch()
        self.assert_closed()

    def test_chunked_wire_budget_includes_framing_and_trailers(self):
        entity = b'x'*65536
        chunked = (b'10000\r\n'+entity+b'\r\n')*16+b'0\r\n\r\n'
        self.responses = [wire(chunked,headers=[('Transfer-Encoding','chunked')],length=False)]
        with self.assertRaisesRegex(FetchError,'encoded_body_limit'):
            self.fetch()
        self.assert_closed()
        self.responses = [wire(b'2\r\n{}\r\n0\r\n'+b'X-Trailer: '+b'x'*8193+b'\r\n\r\n',
                              headers=[('Transfer-Encoding','chunked')],length=False)]
        with self.assertRaisesRegex(FetchError,'header_or_framing_limit'):
            self.fetch()
        self.assert_closed()

    def test_chunked_success_counts_wire_separately_and_error_huge_CL_preserves_429(self):
        self.responses = [wire(b'2\r\n{}\r\n0\r\n\r\n',headers=[('Transfer-Encoding','chunked')],length=False)]
        receipt = self.fetch()
        self.assertEqual(receipt.byte_count,2)
        self.assertEqual(receipt.encoded_byte_count,12)
        self.responses = [wire(b'x'*4096,status=429,headers=[('Content-Length','999999999'),('Retry-After','3600')],length=False)]
        receipt = self.fetch()
        self.assertEqual(receipt.status,429)
        self.assertFalse(receipt.response_complete)
        self.assertEqual(receipt.byte_count,4096)

    def test_malformed_chunk_data_separator_and_terminal_trailer_fail_closed(self):
        cases = [b'2\r\n{}XX0\r\n\r\n', b'2\r\n{}\r\n0\r\n',
                 b'2\r\n{}\r\n0\r\nX-Trailer: unfinished']
        for chunked in cases:
            self.responses = [wire(chunked,headers=[('Transfer-Encoding','chunked')],length=False)]
            with self.subTest(chunked=chunked), self.assertRaises(FetchError):
                self.fetch()
            self.assert_closed()

    def test_valid_chunk_extensions_and_complete_trailers_keep_stdlib_framing(self):
        # The payload itself contains CRLF and has size2; strict checking must
        # distinguish payload reads from the chunk-data separator read.
        chunked = b'2;fixture=yes\r\n{}\r\n2\r\n\r\n\r\n0\r\nX-Trailer: complete\r\n\r\n'
        self.responses = [wire(chunked,headers=[('Transfer-Encoding','chunked')],length=False)]
        receipt = self.fetch()
        self.assertEqual(receipt.ephemeral_json,{})
        self.assertEqual(receipt.byte_count,4)
        self.assertEqual(receipt.encoded_byte_count,len(chunked))
        self.assertTrue(receipt.response_complete)
        self.assert_closed()

    def test_decoder_and_fetch_errors_drop_implicit_raw_body_context(self):
        for body in (b'{"synthetic":"context-sentinel", invalid}', b'\xffcontext-sentinel'):
            with self.subTest(body=body):
                with self.assertRaisesRegex(FetchError, 'invalid_json') as decoded:
                    decode_metadata_json(body)
                self.assertIsNone(decoded.exception.__context__)
                self.assertIsNone(decoded.exception.__cause__)
                self.responses = [wire(body)]
                with self.assertRaisesRegex(FetchError, 'invalid_json') as fetched:
                    self.fetch()
                self.assertIsNone(fetched.exception.__context__)
                self.assertIsNone(fetched.exception.__cause__)
                self.assertTrue(fetched.exception.send_attempted)
                self.assertEqual(fetched.exception.bytes_written, 0)
        self.assert_closed()

    def test_json_duplicate_depth_array_nonfinite_html_are_rejected(self):
        for body in [b'{"a":1,"a":2}',b'{"x":NaN}',b'{"x":1e999}',b'<html>no</html>',
                     b'{"x":'+b'['*25+b'0'+b']'*25+b'}',json.dumps({'x':[0]*101}).encode()]:
            self.responses = [wire(body)]
            with self.subTest(body=body[:30]), self.assertRaises(FetchError):
                self.fetch()
        self.assert_closed()

    def test_429_error_body_bounded_discarded_retry_after_retained(self):
        self.responses = [wire(b'SECRET-PROSE'*1000, status=429, headers=[('Retry-After','999999999')])]
        receipt = self.fetch()
        self.assertEqual(receipt.status, 429)
        self.assertEqual(receipt.byte_count, 4096)
        self.assertFalse(receipt.response_complete)
        self.assertIsNone(receipt.ephemeral_json)
        self.assertIn(('retry-after','999999999'), receipt.headers)
        self.assertNotIn('SECRET-PROSE', repr(receipt))
        self.assert_closed()

    def test_cover_storage_grant_and_empty_owned_fd_are_required(self):
        request = FetchRequest(COVER, 'cover')
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'stage'
            fd = os.open(path, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
            try:
                with self.assertRaises(FetchError):
                    self.fetch(request, output_fd=fd)
                grant = StorageGrant(COVER,30,'a'*64)
                self.responses = [b'HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: 3\r\n\r\nabc']
                result = self.fetch(request, permit=self.permit(request, storage=grant), output_fd=fd)
                self.assertEqual(path.read_bytes(), b'abc')
                self.assertEqual(result.byte_count, 3)
                self.assertIsNone(result.ephemeral_json)
                with self.assertRaises(FetchError):
                    self.fetch(request, permit=self.permit(request, storage=grant), output_fd=fd)
            finally:
                os.close(fd)
        self.assert_closed()

    def test_cover_8mib_boundary_and_partial_write_report_no_implicit_install(self):
        request = FetchRequest(COVER, 'cover')
        with tempfile.TemporaryFile() as tmp:
            with self.assertRaises(FetchError):
                self.fetch(request, permit=self.permit(request, storage=StorageGrant(COVER,30,'a'*64)), output_fd=tmp.fileno())
        with tempfile.TemporaryDirectory() as root:
            fd = os.open(Path(root)/'stage', os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
            try:
                self.responses = [b'HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n\r\n'+b'x'*(COVER_BYTES+1)]
                with self.assertRaises(FetchError) as error:
                    self.fetch(request, permit=self.permit(request, storage=StorageGrant(COVER,30,'a'*64)), output_fd=fd)
                self.assertEqual(error.exception.bytes_written, COVER_BYTES)
                self.assertEqual(os.fstat(fd).st_size, COVER_BYTES)
                self.assertTrue(error.exception.send_attempted)
            finally:
                os.close(fd)
        self.assert_closed()


if __name__ == '__main__':
    unittest.main()
