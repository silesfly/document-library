"""Synthetic child/process and disposable literal-loopback transport contracts.

The child runs the real worker with an explicit fake tokenizers implementation.
No real tokenizer package, model, production DB, SSH or external endpoint is used.
"""
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

if __name__ == '__main__' and '--shutdown-probe' in sys.argv:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'packages/core'))

from document_library import metadata_runtime as runtime
from document_library import _metadata_tokenizer_worker as worker
from document_library.metadata_requests import PromptPolicy, RequestProfile, prepare_summary_requests
from document_library.metadata_summary import SelectedInput, SourceRange


def stop_state(error):
    proof = error.stop_proof
    return 'uncertain' if proof is None else ('terminal' if proof.disposition == 'completed' else 'not_started')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


class MockEncoder:
    def __init__(self, profile):
        self.profile_digest = profile.digest

    def render(self, text, policy):
        return runtime.render_summary_prompt(text, policy)

    def encode(self, prompt):
        return (1, 3, 4, 2)


class LoopbackServer:
    """One owned connection, bounded reader and mandatory close/join."""
    def __init__(self, reply):
        self.reply = reply
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(1)
        self.listener.settimeout(3)
        self.port = self.listener.getsockname()[1]
        self.requests = []
        self.error = None
        self.received = threading.Event()
        self.finished = threading.Event()
        self.thread = threading.Thread(target=self.serve)

    def serve(self):
        try:
            with self.listener.accept()[0] as connection:
                connection.settimeout(3)
                request = bytearray()
                while b'\r\n\r\n' not in request:
                    part = connection.recv(4096)
                    if not part or len(request) > 16384:
                        raise AssertionError('missing bounded request headers')
                    request.extend(part)
                header, body = bytes(request).split(b'\r\n\r\n', 1)
                length = int(next(line.split(b':', 1)[1] for line in header.split(b'\r\n')
                                  if line.lower().startswith(b'content-length:')))
                if length > 1_048_576:
                    raise AssertionError('request too large')
                while len(body) < length:
                    part = connection.recv(min(4096, length - len(body)))
                    if not part:
                        raise AssertionError('request truncated')
                    body += part
                self.requests.append((header, body))
                self.received.set()
                if callable(self.reply):
                    self.reply(connection)
                else:
                    connection.sendall(self.reply)
        except BaseException as error:
            self.error = error
        finally:
            self.finished.set()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.listener.close()
        self.thread.join(4)
        if self.thread.is_alive():
            raise AssertionError('owned mock server did not stop')
        if self.error is not None:
            raise self.error


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='novellibrary-runtime125-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.events = self.root / 'events'
        self.events.mkdir()
        definition = {
            'added_tokens': [{'id': 1, 'content': '<|im_start|>', 'special': True},
                             {'id': 2, 'content': '<|im_end|>', 'special': True}],
            'model': {'vocab': {'x': 0, '<|im_start|>': 1, '<|im_end|>': 2, 'a': 3, 'b': 4}},
        }
        values = {'config.json': {'model_type': 'synthetic'},
                  'tokenizer.json': definition,
                  'tokenizer_config.json': {'eos_token': '<|im_end|>'}}
        layers = []
        artifacts = []
        (self.root / 'blobs').mkdir()
        for name, value in values.items():
            data = encoded(value)
            digest = sha256(data).hexdigest()
            relative = 'blobs/sha256-' + digest
            (self.root / relative).write_bytes(data)
            layers.append({'name': name, 'mediaType': 'application/vnd.ollama.image.json',
                           'digest': 'sha256:' + digest, 'size': len(data)})
            artifacts.append(runtime.ArtifactSpec(name, relative, len(data), digest))
        # Nonexistent weights ensure successful construction never opens a weight layer.
        layers.append({'name': 'weights.safetensors', 'digest': 'sha256:' + 'f' * 64,
                       'size': 9_000_000_000, 'mediaType': 'application/octet-stream'})
        manifest = encoded({'layers': layers})
        (self.root / 'manifest.json').write_bytes(manifest)
        hashes = {a.name: a.sha256 for a in artifacts}
        self.profile = RequestProfile(
            model='synthetic-runtime-model', manifest_sha256=sha256(manifest).hexdigest(),
            tokenizer_sha256=hashes['tokenizer.json'],
            tokenizer_config_sha256=hashes['tokenizer_config.json'],
            model_config_sha256=hashes['config.json'],
            tokenizer_implementation='synthetic-tokenizers-0.22.1-fixture',
            renderer_implementation='product.raw.v1', token_id_ranges=((0, 5),),
            special_tokens=(('<|im_start|>', 1), ('<|im_end|>', 2)),
            max_context_tokens=4096, max_output_tokens=1024, codec_kind='artifact')
        self.artifacts = runtime.ArtifactBinding(str(self.root), 'manifest.json', len(manifest),
                                                self.profile.manifest_sha256, tuple(artifacts))
        self.policy = PromptPolicy(control_tokens=tuple(n for n, _ in self.profile.special_tokens))
        source = '합성 자료'
        selection = SelectedInput('synthetic-r1', sha256(source.encode()).hexdigest(), len(source),
                                  'document', (SourceRange(0, len(source)),),
                                  SourceRange(len(source), len(source)), source)
        self.envelope, = prepare_summary_requests(
            selection, option_digest='a' * 64, input_digest='b' * 64, context_digest='c' * 64,
            profile=self.profile, prompt_policy=self.policy, codec=MockEncoder(self.profile),
            context_tokens=4096, output_reserve_tokens=1024)
        self.entry = self.root / 'synthetic_child.py'
        self.entry.write_text('''import importlib.util,json,sys,time,types
from pathlib import Path
fixture=Path(__file__).resolve()
fake=types.ModuleType('tokenizers');fake.__version__='0.22.1';fake.__file__=str(fixture)
native=types.ModuleType('tokenizers.tokenizers');native.__file__=str(fixture)
class Tokenizer:
    @classmethod
    def from_str(cls, data):
        instance=cls();instance.definition=json.loads(data)
        instance.padding={'synthetic':True};instance.truncation={'synthetic':True}
        return instance
    def no_padding(self):self.padding=None
    def no_truncation(self):self.truncation=None
    def get_vocab(self,with_added_tokens=True):
        assert with_added_tokens is True
        return self.definition['model']['vocab']
    def token_to_id(self,name):return self.get_vocab()[name]
    def encode(self,prompt,add_special_tokens=True):
        assert add_special_tokens is False
        if prompt=='HANG':time.sleep(30)
        if prompt=='CPU':
            while True:pass
        if prompt=='BAD':raise ValueError('PRIVATE_HELPER_PAYLOAD')
        return types.SimpleNamespace(ids=[1,3,4,2])
fake.Tokenizer=Tokenizer
sys.modules['tokenizers']=fake;sys.modules['tokenizers.tokenizers']=native
spec=importlib.util.spec_from_file_location('owned_worker',WORKER_PATH)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
if __name__=='__main__':
    status=1
    try:status=module.main()
    except BaseException:pass
    raise SystemExit(status)
'''.replace('WORKER_PATH', repr(str(Path(worker.__file__).resolve()))))
        self.helper = self.helper_for(self.entry)

    def helper_for(self, entry):
        digest = runtime._file_hash(entry)
        files = tuple(runtime.ImplementationFile(role, str(entry), entry.stat().st_size, digest)
                      for role in ('python_module', 'native_extension'))
        return runtime.HelperSpec(str(Path(sys.executable).absolute()), runtime._file_hash(sys.executable),
                                  str(entry), digest, files)

    def binding(self, port=11434):
        p = self.profile
        return runtime.RuntimeBinding(
            'NOVELLIBRARY-W20261022-001', socket.gethostname(), p.digest,
            runtime._file_hash(runtime.requests.__file__), runtime._file_hash(runtime.__file__),
            runtime._file_hash(worker.__file__), '1' * 64, '2' * 64, self.policy.digest,
            p.model, p.manifest_sha256, p.tokenizer_sha256, p.tokenizer_config_sha256,
            p.model_config_sha256, self.helper.interpreter_sha256, '3' * 64,
            p.special_token_inventory_digest, target_port=port)

    def provider(self, **changes):
        value = {'model': self.profile.model, 'response': '{}', 'done': True,
                 'done_reason': 'stop', 'prompt_eval_count': 4, 'eval_count': 2}
        value.update(changes)
        return value

    def http(self, payload=None, *, framing='length', headers=b''):
        body = encoded(self.provider()) if payload is None else payload
        start = b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n' + headers
        if framing == 'length':
            return start + b'Content-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body
        if framing == 'chunked':
            a, b = body[:7], body[7:]
            return (start + b'Transfer-Encoding: chunked\r\n\r\n' + hex(len(a))[2:].encode() + b'\r\n'
                    + a + b'\r\n' + hex(len(b))[2:].encode() + b'\r\n' + b
                    + b'\r\n0\r\nX-Synthetic-Trailer: yes\r\n\r\n')
        return start + b'\r\n' + body

    def adapter(self, server, *, limits=None, directory=None):
        transport = runtime.RawHttpTransport(limits or runtime.HttpLimits(exchange_seconds=2),
                                              mock_port=server.port)
        permit = runtime.FrozenTrialPermit.freeze(self.binding(server.port), self.envelope,
                                                   request_id='synthetic-request-1')
        return runtime.FrozenTrialAdapter(permit, profile=self.profile, transport=transport,
                                          evidence_dir=directory or self.events)

    def generate(self, adapter):
        return asyncio.run(adapter.generate(self.envelope, request_id='synthetic-request-1'))

    def test_real_owned_child_from_verified_bytes_and_eof_reap(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile)
        with codec:
            self.assertEqual(codec.encode('한글 e\u0301\r\n😀'), (1, 3, 4, 2))
            self.assertEqual(codec.render('자료', self.policy),
                             runtime.render_summary_prompt('자료', self.policy))
            self.assertEqual(codec.observation['inventory']['vocabulary_count'], 5)
            self.assertEqual(codec.observation['inventory']['tokenizers_version'], '0.22.1')
            self.assertEqual(codec.observation['limits']['cpu_status'], 'CONFIGURED_OBSERVED')
            if sys.platform.startswith('linux'):
                self.assertEqual(codec.observation['limits']['address_status'], 'CONFIGURED_OBSERVED')
            binding = runtime.make_runtime_binding(
                self.profile, codec, task_id='NOVELLIBRARY-W20261022-001',
                listener_identity_sha256='8' * 64, prompt_policy=self.policy)
            self.assertEqual(binding.codec_identity_sha256, codec.identity_sha256)
        self.assertEqual(codec.cleanup_state, 'REAPED')
        self.assertEqual(codec.returncode, 0)
        self.assertIsNotNone(codec.pid)
        self.assertRaises(ProcessLookupError, os.kill, codec.pid, 0)
        self.assertEqual(self.profile.qualification_status, 'UNQUALIFIED')

    def test_child_deadline_terminates_and_reaps_without_replacement(self):
        limits = runtime.CodecLimits(encode_wall_seconds=0.1)
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile, limits=limits)
        with codec:
            pid = codec.pid
            started = time.monotonic()
            with self.assertRaises(runtime.CodecFailure):
                codec.encode('HANG')
            self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(codec.pid, pid)
            self.assertEqual(codec.cleanup_state, 'REAPED')
            self.assertIn(codec.returncode, (-signal.SIGTERM, -signal.SIGKILL))
            with self.assertRaises(runtime.CodecFailure):
                codec.encode('again')
        self.assertRaises(ProcessLookupError, os.kill, pid, 0)

    def test_child_cpu_limit_observed_by_actual_busy_child(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile,
                                           limits=runtime.CodecLimits(cpu_seconds=1, encode_wall_seconds=5))
        with codec:
            with self.assertRaises(runtime.CodecFailure):
                codec.encode('CPU')
            self.assertEqual(codec.cleanup_state, 'REAPED')
            self.assertEqual(codec.returncode, -signal.SIGKILL)

    def test_child_errors_drop_raw_exception_and_helper_stops(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile)
        with codec:
            with self.assertRaises(runtime.CodecFailure) as caught:
                codec.encode('BAD')
            self.assertNotIn('PRIVATE_HELPER_PAYLOAD', str(caught.exception))
            self.assertIsNone(caught.exception.__context__)
            self.assertEqual(codec.cleanup_state, 'REAPED')

    def test_artifact_hash_and_symlink_drift_rejected_before_ready(self):
        artifact = self.artifacts.artifacts[0]
        path = self.root / artifact.path
        original = path.read_bytes()
        for mode in ('hash', 'file-symlink', 'root-symlink'):
            with self.subTest(mode=mode):
                path.unlink(missing_ok=True)
                path.write_bytes(original)
                binding = self.artifacts
                if mode == 'hash':
                    path.write_bytes(original.replace(b'synthetic', b'synthetiX'))
                elif mode == 'file-symlink':
                    target = self.root / 'copy.json'
                    target.write_bytes(original)
                    path.unlink()
                    path.symlink_to(target)
                else:
                    link = self.root / 'root-link'
                    link.symlink_to(self.root, target_is_directory=True)
                    binding = replace(binding, root=str(link))
                codec = runtime.load_current_codec(binding, self.helper, profile=self.profile)
                with self.assertRaises(runtime.CodecFailure):
                    with codec:
                        self.fail('drift admitted')
                self.assertEqual(codec.cleanup_state, 'REAPED')
                self.assertIsNone(codec.observation)

    def test_midread_identity_change_is_rejected(self):
        artifact = self.artifacts.artifacts[0]
        directory = worker.open_directory(str(self.root))
        original_read = worker.os.read
        replaced = False
        def replacing_read(fd, size):
            nonlocal replaced
            data = original_read(fd, size)
            if data and not replaced:
                replaced = True
                path = self.root / artifact.path
                path.rename(self.root / 'old-open-file')
                path.write_bytes(b'ATTACKER_REPLACEMENT')
            return data
        try:
            with patch.object(worker.os, 'read', replacing_read):
                with self.assertRaises(worker.WorkerError):
                    worker.read_verified(directory, artifact.path, artifact.size, artifact.sha256)
        finally:
            os.close(directory)

    def test_from_str_uses_verified_bytes_after_path_replacement(self):
        artifact = next(a for a in self.artifacts.artifacts if a.name == 'tokenizer.json')
        before_main = self.entry.read_text().split("if __name__=='__main__':", 1)[0]
        injected = """
original_read=module.read_verified
def replace_after_verification(root_fd, relative, size, digest):
    data=original_read(root_fd, relative, size, digest)
    if relative==TOKENIZER_RELATIVE:
        target=fixture.parent/relative
        target.rename(fixture.parent/'verified-original')
        target.write_bytes(b'ATTACKER_INVALID_JSON')
    return data
module.read_verified=replace_after_verification
if __name__=='__main__':
    status=1
    try:status=module.main()
    except BaseException:pass
    raise SystemExit(status)
""".replace('TOKENIZER_RELATIVE', repr(artifact.path))
        script = self.root / 'replacement_child.py'
        script.write_text(before_main + injected)
        codec = runtime.load_current_codec(self.artifacts, self.helper_for(script), profile=self.profile)
        with codec:
            self.assertEqual(codec.encode('자료'), (1, 3, 4, 2))
        self.assertEqual((self.root / artifact.path).read_bytes(), b'ATTACKER_INVALID_JSON')
        self.assertEqual(codec.returncode, 0)

    def test_actual_child_protocol_sequence_hash_and_ids_fail_closed(self):
        before_main = self.entry.read_text().split("if __name__=='__main__':", 1)[0]
        for kind in ('sequence', 'profile', 'prompt', 'invalid-ids', 'truncated'):
            with self.subTest(kind=kind):
                script = self.root / (kind + '_child.py')
                injection = """
original_write=module.write_frame
def corrupt_reply(value):
    if value.get('op')=='encoded':
        if KIND=='sequence':value['seq']+=1
        if KIND=='profile':value['profile_digest']='0'*64
        if KIND=='prompt':value['prompt_sha256']='0'*64
        if KIND=='invalid-ids':value['ids']=[True]
        if KIND=='truncated':
            import struct
            sys.stdout.buffer.write(struct.pack('!I',100));sys.stdout.buffer.write(b'{}')
            sys.stdout.buffer.flush()
            raise SystemExit(0)
    original_write(value)
module.write_frame=corrupt_reply
if __name__=='__main__':
    status=1
    try:status=module.main()
    except SystemExit as error:status=error.code
    except BaseException:pass
    raise SystemExit(status)
""".replace('KIND', repr(kind))
                script.write_text(before_main + injection)
                codec = runtime.load_current_codec(self.artifacts, self.helper_for(script), profile=self.profile)
                with codec:
                    with self.assertRaises(runtime.CodecFailure):codec.encode('자료')
                    self.assertEqual(codec.cleanup_state, 'REAPED')

    def test_actual_child_announces_oversize_frame_and_is_reaped(self):
        script = self.root / 'oversize_child.py'
        # Forward real init/ready through the real child, then announce >8MiB only.
        script.write_text(self.entry.read_text().split("if __name__=='__main__':", 1)[0] + '''
init=module.read_frame()
limits=module.configure_limits(init['cpu_seconds'],init['address_bytes'])
tokenizer,inventory=module.load_tokenizer(init['binding'],init['profile'])
module.write_frame({'op':'ready','seq':0,'profile_digest':init['profile_digest'],
                    'inventory':inventory,'limits':limits})
module.read_frame()
import struct
sys.stdout.buffer.write(struct.pack('!I',8388609));sys.stdout.buffer.flush()
time.sleep(30)
''')
        helper = self.helper_for(script)
        codec = runtime.load_current_codec(self.artifacts, helper, profile=self.profile)
        with codec:
            with self.assertRaises(runtime.CodecFailure):codec.encode('자료')
            self.assertEqual(codec.cleanup_state, 'REAPED')

    def test_codec_concurrency_and_aggregate_call_cap(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile,
                                           limits=runtime.CodecLimits(max_codec_calls=2))
        with codec:
            codec._gate.acquire()
            try:
                with self.assertRaisesRegex(runtime.CodecFailure, 'CONCURRENT_CODEC_CALL'):
                    codec.encode('자료')
            finally:
                codec._gate.release()
            codec.render('자료', self.policy)
            codec.encode('자료')
            with self.assertRaisesRegex(runtime.CodecFailure, 'CODEC_CALL_LIMIT'):
                codec.render('자료', self.policy)

    def test_exact_single_wire_and_receipt_framing_variants(self):
        for framing in ('length', 'chunked', 'eof'):
            with self.subTest(framing=framing):
                directory = self.events / framing
                directory.mkdir()
                with LoopbackServer(self.http(framing=framing)) as server:
                    adapter = self.adapter(server, directory=directory)
                    receipt = self.generate(adapter)
                self.assertEqual(len(server.requests), 1)
                header, body = server.requests[0]
                self.assertTrue(header.startswith(b'POST /api/generate HTTP/1.1\r\n'))
                self.assertIn(b'Host: 127.0.0.1:', header)
                self.assertEqual(body, self.envelope.body)
                self.assertEqual(receipt.response_bytes, encoded(self.provider()))
                self.assertEqual(receipt.response_sha256, sha256(receipt.response_bytes).hexdigest())
                self.assertEqual(receipt.output_text, '{}')
                self.assertTrue(receipt.terminal and receipt.fully_drained)
                self.assertNotIn('response_bytes=', repr(receipt))
                records = [json.loads(p.read_bytes()) for p in directory.iterdir()]
                self.assertEqual({r['state'] for r in records}, {'PREPARED', 'SEND_ATTEMPTED', 'CAPTURED'})
                self.assertTrue(all('body' not in r and 'output_text' not in r for r in records))

    def test_length_terminal_kept_separate_from_summary_quality(self):
        with LoopbackServer(self.http(encoded(self.provider(done_reason='length', response='short invalid JSON')))) as server:
            receipt = self.generate(self.adapter(server))
        self.assertEqual(receipt.done_reason, 'length')
        self.assertEqual(receipt.output_text, 'short invalid JSON')
        self.assertTrue(receipt.terminal)

    def test_invalid_framing_never_claims_stop_from_done_fragment(self):
        body = encoded(self.provider())
        head = b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'
        cases = {
            'truncated-length': head + b'Content-Length: 999\r\n\r\n' + body,
            'missing-zero-chunk': head + b'Transfer-Encoding: chunked\r\n\r\n' + hex(len(body))[2:].encode() + b'\r\n' + body + b'\r\n',
            'cl-and-te': head + b'Transfer-Encoding: chunked\r\nContent-Length: 1\r\n\r\nX',
            'duplicate-cl': head + b'Content-Length: 1\r\nContent-Length: 1\r\n\r\nX',
            'chunk-truncated': head + b'Transfer-Encoding: chunked\r\n\r\n999\r\n' + body,
            'bad-chunk-size': head + b'Transfer-Encoding: chunked\r\n\r\n+2\r\n{}\r\n0\r\n\r\n',
            'overflow-length': head + b'Content-Length: 1048577\r\n\r\n',
            'error-status': b'HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n',
            'unsupported-content-coding': self.http(headers=b'Content-Encoding: gzip\r\n'),
        }
        for name, reply in cases.items():
            with self.subTest(name=name):
                directory = self.events / name
                directory.mkdir()
                with LoopbackServer(reply) as server:
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.generate(self.adapter(server, directory=directory))
                self.assertEqual(stop_state(caught.exception), 'uncertain')
                self.assertIsNone(caught.exception.stop_proof)
                self.assertIsNone(caught.exception.__context__)

    def test_outer_response_types_duplicates_model_and_thinking_are_uncertain(self):
        invalid = [encoded(self.provider(done=False)), encoded(self.provider(prompt_eval_count=True)),
                   encoded(self.provider(eval_count=False)), encoded(self.provider(model='other-model')),
                   encoded(self.provider(done_reason='unknown')), encoded(self.provider(thinking='secret')),
                   encoded(self.provider(extra='unknown')), b'{"model":"x","model":"y"}',
                   encoded(self.provider()).replace(b'"done":true', b'"done":true,"done":true'),
                   b'\xff', b'[]', encoded(self.provider()).replace(b'"eval_count":2', b'"eval_count":NaN')]
        for number, payload in enumerate(invalid):
            with self.subTest(number=number):
                directory = self.events / str(number)
                directory.mkdir()
                with LoopbackServer(self.http(payload)) as server:
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.generate(self.adapter(server, directory=directory))
                self.assertEqual(stop_state(caught.exception), 'uncertain')
                self.assertNotIn('secret', str(caught.exception))

    def test_parity_and_output_caps_preserve_confirmed_stop(self):
        cases = [self.provider(prompt_eval_count=5), self.provider(eval_count=1025),
                 self.provider(response='x' * 65_537), self.provider(response='\ud800')]
        for number, value in enumerate(cases):
            with self.subTest(number=number):
                directory = self.events / str(number)
                directory.mkdir()
                payload = json.dumps(value, ensure_ascii=True, separators=(',', ':')).encode()
                with LoopbackServer(self.http(payload)) as server:
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.generate(self.adapter(server, directory=directory))
                error = caught.exception
                self.assertEqual(stop_state(error), 'terminal')
                self.assertTrue(error.stop_proof.terminal and error.stop_proof.fully_drained)
                self.assertEqual((error.stop_proof.request_id, error.stop_proof.wire_sha256, error.stop_proof.profile_digest),
                                 ('synthetic-request-1', self.envelope.wire_sha256, self.profile.digest))

    def test_deadline_and_response_overflow_are_uncertain_and_one_send(self):
        def stalled(connection):
            connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\n\r\n')
            time.sleep(0.15)
        for name, reply, limits in (
            ('deadline', stalled, runtime.HttpLimits(exchange_seconds=0.05)),
            ('overflow-eof', self.http(b'x' * 33, framing='eof'), runtime.HttpLimits(max_response_bytes=32)),
        ):
            with self.subTest(name=name):
                directory = self.events / name
                directory.mkdir()
                with LoopbackServer(reply) as server:
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.generate(self.adapter(server, limits=limits, directory=directory))
                self.assertEqual(stop_state(caught.exception), 'uncertain')
                self.assertEqual(len(server.requests), 1)

    def test_no_network_on_drift_or_wrong_trial_id_and_duplicate_no_replay(self):
        with LoopbackServer(self.http()) as server:
            adapter = self.adapter(server)
            with self.assertRaises(runtime.RuntimeFailure) as wrong_id:
                asyncio.run(adapter.generate(self.envelope, request_id='not-frozen'))
            self.assertEqual(stop_state(wrong_id.exception), 'not_started')
            with self.assertRaises(runtime.RuntimeFailure) as drift:
                asyncio.run(adapter.generate(replace(self.envelope, body=b'{}'), request_id='synthetic-request-1'))
            self.assertEqual(stop_state(drift.exception), 'not_started')
            self.assertEqual(server.requests, [])
            self.generate(adapter)
            with self.assertRaises(runtime.RuntimeFailure) as duplicate:
                self.generate(self.adapter(server))
            self.assertEqual(stop_state(duplicate.exception), 'not_started')
        self.assertEqual(len(server.requests), 1)

    def test_async_cancel_retains_owner_until_late_terminal_and_discards_output(self):
        release = threading.Event()
        def late(connection):
            if not release.wait(3):raise AssertionError('test did not release mock response')
            connection.sendall(self.http())
        async def scenario(adapter, server):
            loop_errors = []
            asyncio.get_running_loop().set_exception_handler(lambda loop, context: loop_errors.append(context))
            pending = asyncio.create_task(adapter.generate(self.envelope, request_id='synthetic-request-1'))
            while not server.received.is_set():await asyncio.sleep(0.005)
            pending.cancel()
            await asyncio.sleep(0.02)
            self.assertFalse(pending.done())
            self.assertEqual(len(adapter._owners), 1)
            pending.cancel()
            release.set()
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                await pending
            self.assertEqual(caught.exception.code, 'CANCELLED')
            self.assertEqual(stop_state(caught.exception), 'terminal')
            self.assertEqual(adapter._owners, set())
            await asyncio.sleep(0)
            self.assertEqual(loop_errors, [])
        with LoopbackServer(late) as server:
            asyncio.run(scenario(self.adapter(server), server))
        self.assertEqual(len(server.requests), 1)
        capture = json.loads(next(self.events.glob('*.captured.json')).read_bytes())
        self.assertIs(capture['cancelled'], True)

    def test_cancelled_exchange_disconnect_retains_uncertainty(self):
        release = threading.Event()
        def lost(connection):
            if not release.wait(3):raise AssertionError('test did not release mock')
            connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\n\r\n{}')
        async def scenario(adapter, server):
            loop_errors = []
            asyncio.get_running_loop().set_exception_handler(lambda loop, context: loop_errors.append(context))
            pending = asyncio.create_task(adapter.generate(self.envelope, request_id='synthetic-request-1'))
            while not server.received.is_set():await asyncio.sleep(0.005)
            pending.cancel()
            release.set()
            with self.assertRaises(runtime.RuntimeFailure) as caught:await pending
            self.assertEqual(stop_state(caught.exception), 'uncertain')
            await asyncio.sleep(0)
            self.assertEqual(loop_errors, [])
            self.assertEqual(adapter._owners, set())
        with LoopbackServer(lost) as server:
            asyncio.run(scenario(self.adapter(server), server))

    def test_pre_cancel_transport_does_not_create_socket(self):
        transport = runtime.RawHttpTransport()
        cancelled = threading.Event()
        cancelled.set()
        with patch.object(transport, '_connect') as connect:
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                transport._exchange(self.envelope, request_id='synthetic-request-1',
                                   model=self.profile.model, cancelled=cancelled,
                                   before_connect=lambda: self.fail('intent send began'))
        self.assertEqual(stop_state(caught.exception), 'not_started')
        connect.assert_not_called()

    def test_parent_reconciled_three_way_evidence_and_bounded_pilot(self):
        with LoopbackServer(self.http()) as server:
            adapter = self.adapter(server)
            self.generate(adapter)
            binding = adapter.binding
        capture = next(self.events.glob('*.captured.json'))
        capture_value = json.loads(capture.read_bytes())
        qualification = {'schema': 'novellibrary.runtime.qualification.v1', 'binding': asdict(binding),
                         'capabilities': ['normal_raw_single_trial'],
                         'trial_receipt_sha256': sha256(capture.read_bytes()).hexdigest(),
                         'trial_request_id': capture_value['request_id'],
                         'trial_wire_sha256': capture_value['wire_sha256'],
                         'trial_response_sha256': capture_value['response_sha256'],
                         'local_prompt_tokens': 4}
        qpath = self.root / 'qualification.json'
        qpath.write_bytes(encoded(qualification))
        reconciliation = {'schema': 'novellibrary.runtime.pilot-reconciliation.v1',
                          'decision': 'PILOT_ADMITTED', 'reviewer': 'synthetic-independent-parent',
                          'binding_digest': binding.digest,
                          'remaining_uncertainties': ['synthetic only; actual serving NOT_VERIFIED'],
                          'qualification_sha256': sha256(qpath.read_bytes()).hexdigest(),
                          'limits': {'max_prompt_tokens': 4, 'max_context_tokens': 4096,
                                     'max_output_tokens': 1024, 'max_requests': 1}}
        rpath = self.root / 'reconciliation.json'
        rpath.write_bytes(encoded(reconciliation))
        rhash = sha256(rpath.read_bytes()).hexdigest()
        q = runtime.RuntimeQualification.load(qpath, rpath, capture,
                                             expected_reconciliation_sha256=rhash,
                                             current_binding=binding)
        pilot = runtime.QualifiedRuntimeAdapter(q, profile=self.profile,
                                                transport=adapter.transport, evidence_dir=self.events)
        pilot.validate_envelope(self.envelope)
        self.assertEqual(q.max_prompt_tokens, 4)
        # A terminal capture from a cancelled exchange is stop evidence, not a
        # qualifying normal trial, even if someone freezes its new file hash.
        cancelled_capture = dict(capture_value, cancelled=True)
        bad_capture = self.root / 'cancelled-capture.json'
        bad_capture.write_bytes(encoded(cancelled_capture))
        cancelled_q = dict(qualification, trial_receipt_sha256=sha256(bad_capture.read_bytes()).hexdigest())
        bad_qpath = self.root / 'cancelled-qualification.json'
        bad_qpath.write_bytes(encoded(cancelled_q))
        cancelled_reconciliation = dict(reconciliation, qualification_sha256=sha256(bad_qpath.read_bytes()).hexdigest())
        bad_rpath = self.root / 'cancelled-reconciliation.json'
        bad_rpath.write_bytes(encoded(cancelled_reconciliation))
        with self.assertRaisesRegex(runtime.CodecFailure, 'INVALID_QUALIFICATION_EVIDENCE'):
            runtime.RuntimeQualification.load(bad_qpath, bad_rpath, bad_capture,
                expected_reconciliation_sha256=sha256(bad_rpath.read_bytes()).hexdigest(),
                current_binding=binding)
        attempts = []
        def refused_connection(timeout):
            attempts.append(timeout)
            raise OSError('PRIVATE_SOCKET_DETAIL')
        transport = runtime.RawHttpTransport(mock_port=binding.target_port,
                                               connection_factory=refused_connection)
        first = runtime.QualifiedRuntimeAdapter(q, profile=self.profile, transport=transport,
                                                evidence_dir=self.events)
        with self.assertRaises(runtime.RuntimeFailure) as first_failure:
            asyncio.run(first.generate(self.envelope, request_id='pilot-request-one'))
        self.assertIsNone(first_failure.exception.stop_proof)
        self.assertIsNone(first_failure.exception.__context__)
        self.assertNotIn('PRIVATE_SOCKET_DETAIL', str(first_failure.exception))
        replacement = runtime.QualifiedRuntimeAdapter(q, profile=self.profile, transport=transport,
                                                      evidence_dir=self.events)
        with self.assertRaises(runtime.RuntimeFailure) as exhausted:
            asyncio.run(replacement.generate(self.envelope, request_id='pilot-request-two'))
        self.assertEqual(stop_state(exhausted.exception), 'not_started')
        self.assertEqual(len(attempts), 1)
        with self.assertRaises(runtime.CodecFailure):
            replacement.validate_envelope(replace(self.envelope, request_tokens=5))
        self.assertEqual(len(exhausted.exception.stop_proof.__dataclass_fields__), 7)
        self.assertFalse(hasattr(exhausted.exception, 'stop_state'))
        with self.assertRaises(AttributeError):
            exhausted.exception.stop_proof.terminal = True
        self.assertEqual(self.envelope.qualification_status, 'UNQUALIFIED')
        self.assertEqual(self.profile.qualification_status, 'UNQUALIFIED')
        with self.assertRaises(runtime.CodecFailure):
            runtime.RuntimeQualification.load(qpath, rpath, capture,
                                               expected_reconciliation_sha256='0' * 64,
                                               current_binding=binding)
        with self.assertRaises(runtime.CodecFailure):
            runtime.RuntimeQualification.load(qpath, rpath, capture,
                                               expected_reconciliation_sha256=rhash,
                                               current_binding=replace(binding, listener_identity_sha256='7' * 64))
        with self.assertRaises(TypeError):
            runtime.QualifiedRuntimeAdapter(qualified=True, profile=self.profile,
                                            transport=adapter.transport, evidence_dir=self.events)

    def test_idle_context_deadline_reaps_helper_and_joins_single_watchdog(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile,
            limits=runtime.CodecLimits(context_wall_seconds=0.25))
        with codec:
            watcher = codec._watchdog
            watcher.join(timeout=3)
            self.assertFalse(watcher.is_alive())
            self.assertEqual(codec.cleanup_state, 'REAPED')
            self.assertEqual(codec.returncode, 0)
            self.assertIsNotNone(codec._process.poll())
            self.assertRaises(ProcessLookupError, os.kill, codec.pid, 0)
            with self.assertRaises(runtime.CodecFailure):codec.render('자료', self.policy)
            with self.assertRaises(runtime.CodecFailure):codec.encode('자료')
            self.assertIs(codec._watchdog, watcher)
        self.assertFalse(watcher.is_alive())

    def test_watchdog_start_failure_reaps_the_owned_helper(self):
        codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile)
        with patch.object(threading.Thread, 'start', side_effect=RuntimeError('SYNTHETIC_WATCHER_FAILURE')):
            with self.assertRaisesRegex(runtime.CodecFailure, 'CODEC_START_FAILED') as caught:
                with codec:self.fail('helper without a deadline owner was admitted')
        self.assertIsNone(caught.exception.__context__)
        self.assertEqual(codec.cleanup_state, 'REAPED')
        self.assertEqual(codec.returncode, 0)
        self.assertIsNone(codec._watchdog.ident)
        self.assertRaises(ProcessLookupError, os.kill, codec.pid, 0)

    def test_context_deadline_races_encode_and_explicit_close_with_one_cleanup(self):
        for explicit_close in (False, True):
            with self.subTest(explicit_close=explicit_close):
                codec = runtime.load_current_codec(self.artifacts, self.helper, profile=self.profile,
                    limits=runtime.CodecLimits(context_wall_seconds=0.3))
                errors = []
                def encoding():
                    try:
                        codec.encode('HANG')
                    except runtime.CodecFailure as error:
                        errors.append(str(error))
                with codec:
                    watcher = codec._watchdog
                    encoder = threading.Thread(target=encoding)
                    encoder.start()
                    while not codec._gate.locked():time.sleep(0.002)
                    closer = threading.Thread(target=codec.close) if explicit_close else None
                    with patch.object(codec._process, 'terminate', wraps=codec._process.terminate) as terminate:
                        if closer is not None:closer.start()
                        encoder.join(timeout=4)
                        if closer is not None:closer.join(timeout=4)
                        self.assertFalse(encoder.is_alive())
                        self.assertTrue(closer is None or not closer.is_alive())
                        self.assertLessEqual(terminate.call_count, 1)
                    self.assertEqual(errors, ['CODEC_ENCODE_FAILED'])
                    self.assertEqual(codec.cleanup_state, 'REAPED')
                    self.assertEqual(codec.returncode, -signal.SIGTERM)
                    self.assertFalse(watcher.is_alive())
                    self.assertRaises(ProcessLookupError, os.kill, codec.pid, 0)

    def test_chunk_trailer_field_names_values_and_duplicate_rules(self):
        body = encoded(self.provider())
        prefix = (b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'
                  b'Transfer-Encoding: chunked\r\n\r\n' + format(len(body), 'x').encode()
                  + b'\r\n' + body + b'\r\n0\r\n')
        cases = (b'bad name: accepted', b': accepted', b'X-Probe: bad\x00value',
                 b'X-Probe: bad\x7fvalue', b'X-Probe: first\r\nx-probe: second',
                 b'Content-Length: 1', b'Transfer-Encoding: chunked',
                 b'X-Probe: ok\tvalue')
        for index, trailer in enumerate(cases):
            with self.subTest(trailer=trailer):
                directory = self.events / ('trailer-grammar-' + str(index))
                directory.mkdir()
                with LoopbackServer(prefix + trailer + b'\r\n\r\n') as server:
                    adapter = self.adapter(server, directory=directory)
                    if index == len(cases) - 1:
                        receipt = self.generate(adapter)
                        self.assertTrue(receipt.terminal and receipt.fully_drained)
                    else:
                        with self.assertRaises(runtime.RuntimeFailure) as caught:self.generate(adapter)
                        self.assertEqual(caught.exception.code, 'INVALID_HTTP_FRAMING')
                        self.assertIsNone(caught.exception.stop_proof)
                        self.assertIsNone(caught.exception.__context__)

    def test_asyncio_shutdown_cancels_caller_without_losing_physical_exchange(self):
        for mode in ('terminal', 'disconnect'):
            with self.subTest(mode=mode):
                completed = subprocess.run([sys.executable, '-I', '-B', str(Path(__file__).resolve()),
                    '--shutdown-probe', str(self.root), mode], capture_output=True, timeout=5,
                    env={'PATH': '/usr/bin:/bin'})
                self.assertEqual(completed.returncode, 0, completed.stderr.decode())
                self.assertEqual(completed.stderr, b'')
                report = json.loads(completed.stdout)
                self.assertEqual(report['owners'], 0)
                self.assertEqual(report['owner_cancel_results'], [False])
                self.assertEqual(report['sends'], 1)
                self.assertTrue(report['caller_done'])
                self.assertEqual(len(report['shutdown_notifications']), 1)
                notification = report['shutdown_notifications'][0]
                self.assertEqual(notification['message'], 'unhandled exception during asyncio.run() shutdown')
                self.assertEqual(notification['code'], 'CANCELLED' if mode == 'terminal'
                                 else 'TRUNCATED_HTTP_RESPONSE')
                proof = notification['stop_proof']
                if mode == 'terminal':
                    self.assertEqual(proof['disposition'], 'completed')
                    self.assertTrue(proof['terminal'] and proof['fully_drained'])
                    self.assertTrue(report['capture_cancelled'])
                else:
                    self.assertIsNone(proof)
                    self.assertIsNone(report['capture_cancelled'])

    def test_caller_cancelled_at_prelaunch_checkpoint_has_no_socket_or_thread(self):
        calls = []
        def connection(timeout):
            calls.append(timeout)
            raise AssertionError('prelaunch cancellation connected')
        transport = runtime.RawHttpTransport(mock_port=32104, connection_factory=connection)
        permit = runtime.FrozenTrialPermit.freeze(self.binding(32104), self.envelope,
            request_id='synthetic-request-1')
        adapter = runtime.FrozenTrialAdapter(permit, profile=self.profile, transport=transport,
            evidence_dir=self.events)
        async def scenario():
            pending = asyncio.create_task(adapter.generate(self.envelope, request_id='synthetic-request-1'))
            asyncio.get_running_loop().call_soon(pending.cancel)
            with self.assertRaises(runtime.RuntimeFailure) as caught:await pending
            self.assertEqual(stop_state(caught.exception), 'not_started')
            self.assertEqual(caught.exception.stop_proof.wire_sha256, self.envelope.wire_sha256)
        with patch.object(threading.Thread, 'start') as start:
            asyncio.run(scenario())
        start.assert_not_called()
        self.assertEqual(calls, [])
        self.assertEqual(adapter._owners, set())

    def test_request_launch_failure_seals_entry_even_if_os_thread_already_exists(self):
        for phase in ('before-thread', 'before-transport-entry'):
            with self.subTest(phase=phase):
                directory = self.events / phase
                directory.mkdir()
                calls = []
                def connection(timeout):
                    calls.append(timeout)
                    raise AssertionError('sealed launch connected')
                transport = runtime.RawHttpTransport(mock_port=32105, connection_factory=connection)
                permit = runtime.FrozenTrialPermit.freeze(self.binding(32105), self.envelope,
                    request_id='synthetic-request-1')
                adapter = runtime.FrozenTrialAdapter(permit, profile=self.profile, transport=transport,
                    evidence_dir=directory)
                owner = Future()
                release = threading.Event()
                parked = threading.Event()
                owner.add_done_callback(lambda result: release.set())
                original_start, original_run = threading.Thread.start, threading.Thread.run
                threads = []
                def paused_run(thread):
                    if thread.name == 'novellibrary-raw-request':
                        parked.set()
                        if not release.wait(3):raise AssertionError('sealed owner was never completed')
                    original_run(thread)
                def failing_start(thread):
                    threads.append(thread)
                    if phase == 'before-transport-entry':
                        original_start(thread)
                        self.assertTrue(parked.wait(2))
                    raise RuntimeError('SYNTHETIC_LAUNCH_FAILURE')
                with patch.object(runtime, 'Future', return_value=owner):
                    with patch.object(threading.Thread, 'run', paused_run):
                        with patch.object(threading.Thread, 'start', failing_start):
                            with self.assertRaises(runtime.RuntimeFailure) as caught:self.generate(adapter)
                self.assertEqual(caught.exception.code, 'OWNER_START_FAILED')
                self.assertEqual(stop_state(caught.exception), 'not_started')
                self.assertEqual(calls, [])
                self.assertEqual(adapter._owners, set())
                self.assertEqual(len(threads), 1)
                self.assertFalse(threads[0].is_alive())
                self.assertTrue(owner.done())

    def test_shared_executor_queue_teardown_does_not_cancel_owned_request_thread(self):
        response_release = threading.Event()
        blocker_release = threading.Event()
        blocker_started = threading.Event()
        threads = []
        def delayed(connection):
            if not response_release.wait(3):raise AssertionError('response was not released')
            connection.sendall(self.http())
        async def scenario(adapter, server):
            pool = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()
            loop.set_default_executor(pool)
            def occupy():
                blocker_started.set()
                return blocker_release.wait(3)
            blocking = loop.run_in_executor(None, occupy)
            while not blocker_started.is_set():await asyncio.sleep(0.002)
            queued_calls = []
            queued = loop.run_in_executor(None, lambda: queued_calls.append('ran'))
            original_start = threading.Thread.start
            def record_start(thread):
                if thread.name == 'novellibrary-raw-request':threads.append(thread)
                return original_start(thread)
            try:
                with patch.object(threading.Thread, 'start', record_start):
                    # The new mechanics must not submit this exchange to that pool.
                    with patch.object(loop, 'run_in_executor', side_effect=AssertionError('shared pool used')):
                        pending = asyncio.create_task(adapter.generate(self.envelope, request_id='synthetic-request-1'))
                        while not server.received.is_set():await asyncio.sleep(0.002)
                        pool.shutdown(wait=False, cancel_futures=True)
                        await asyncio.sleep(0.01)
                        self.assertTrue(queued.cancelled())
                        self.assertEqual(queued_calls, [])
                        self.assertFalse(pending.done())
                        response_release.set()
                        receipt = await pending
                self.assertTrue(receipt.terminal and receipt.fully_drained)
                self.assertEqual(adapter._owners, set())
            finally:
                blocker_release.set()
                response_release.set()
                await blocking
                pool.shutdown(wait=True)
        with LoopbackServer(delayed) as server:
            asyncio.run(scenario(self.adapter(server), server))
        self.assertEqual(len(server.requests), 1)
        self.assertEqual(len(threads), 1)
        self.assertFalse(threads[0].is_alive())

    def test_cancelled_asyncio_waiters_after_send_cannot_create_not_started_proof(self):
        for mode in ('terminal', 'disconnect'):
            with self.subTest(mode=mode):
                directory = self.events / mode
                directory.mkdir()
                release = threading.Event()
                waiters = []
                threads = []
                def delayed(connection):
                    if not release.wait(3):raise AssertionError('response was not released')
                    if mode == 'terminal':connection.sendall(self.http())
                    else:connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\n\r\n{}')
                original_wrap, original_start = asyncio.wrap_future, threading.Thread.start
                def record_wrap(future, *args, **kwargs):
                    waiter = original_wrap(future, *args, **kwargs)
                    waiters.append(waiter)
                    return waiter
                def record_start(thread):
                    if thread.name == 'novellibrary-raw-request':threads.append(thread)
                    return original_start(thread)
                async def scenario(adapter, server):
                    loop_errors = []
                    asyncio.get_running_loop().set_exception_handler(lambda loop, context: loop_errors.append(context))
                    pending = asyncio.create_task(adapter.generate(self.envelope, request_id='synthetic-request-1'))
                    while not server.received.is_set():await asyncio.sleep(0.002)
                    for _ in range(2):
                        waiters[-1].cancel()
                        await asyncio.sleep(0.01)
                        self.assertFalse(pending.done())
                        self.assertEqual(len(adapter._owners), 1)
                    pending.cancel()
                    release.set()
                    with self.assertRaises(runtime.RuntimeFailure) as caught:await pending
                    self.assertEqual(stop_state(caught.exception), 'terminal' if mode == 'terminal' else 'uncertain')
                    self.assertNotEqual(stop_state(caught.exception), 'not_started')
                    self.assertEqual(adapter._owners, set())
                    await asyncio.sleep(0)
                    self.assertEqual(loop_errors, [])
                with LoopbackServer(delayed) as server:
                    with patch.object(asyncio, 'wrap_future', record_wrap):
                        with patch.object(threading.Thread, 'start', record_start):
                            asyncio.run(scenario(self.adapter(server, directory=directory), server))
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(len(threads), 1)
                self.assertFalse(threads[0].is_alive())

    def test_launch_error_after_physical_send_preserves_actual_drain_proof(self):
        for mode in ('terminal', 'disconnect'):
            with self.subTest(mode=mode):
                directory = self.events / mode
                directory.mkdir()
                release = threading.Event()
                threads = []
                def delayed(connection):
                    if not release.wait(3):raise AssertionError('response was not released')
                    if mode == 'terminal':connection.sendall(self.http())
                    else:connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\n\r\n{}')
                with LoopbackServer(delayed) as server:
                    original_start = threading.Thread.start
                    def start_then_fail(thread):
                        threads.append(thread)
                        original_start(thread)
                        self.assertTrue(server.received.wait(2))
                        release.set()
                        raise RuntimeError('SYNTHETIC_ERROR_AFTER_ENTRY')
                    with patch.object(threading.Thread, 'start', start_then_fail):
                        with self.assertRaises(runtime.RuntimeFailure) as caught:
                            self.generate(self.adapter(server, directory=directory))
                self.assertEqual(stop_state(caught.exception), 'terminal' if mode == 'terminal' else 'uncertain')
                self.assertNotEqual(stop_state(caught.exception), 'not_started')
                if mode == 'terminal':
                    self.assertEqual(caught.exception.code, 'OWNER_START_FAILED')
                    capture = json.loads(next(directory.glob('*.captured.json')).read_bytes())
                    self.assertIs(capture['cancelled'], True)
                self.assertEqual(len(server.requests), 1)
                self.assertEqual(len(threads), 1)
                self.assertFalse(threads[0].is_alive())


def shutdown_probe(directory, mode):
    """File-based isolated runner; collect the real abandoned-caller notification."""
    tempfile.tempdir = directory
    fixture = RuntimeTests()
    fixture.setUp()
    notifications = []
    owners = []
    pending = []
    try:
        def delayed(connection):
            time.sleep(0.15)
            if mode == 'terminal':
                connection.sendall(fixture.http())
            else:
                connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'
                                   b'Content-Length: 999\r\n\r\n{}')
        with LoopbackServer(delayed) as server:
            adapter = fixture.adapter(server)
            async def main():
                def collect(loop, context):
                    error = context.get('exception')
                    notifications.append({'message': context.get('message'),
                        'code': getattr(error, 'code', None),
                        'stop_proof': None if getattr(error, 'stop_proof', None) is None
                        else asdict(error.stop_proof)})
                asyncio.get_running_loop().set_exception_handler(collect)
                pending.append(asyncio.create_task(adapter.generate(fixture.envelope,
                    request_id='synthetic-request-1')))
                while not server.received.is_set():await asyncio.sleep(0.002)
                owners.extend(owner.cancel() for owner in adapter._owners)
                # Returning triggers asyncio.run's real mass Task cancellation.
            asyncio.run(main())
        capture_paths = list(fixture.events.glob('*.captured.json'))
        capture = json.loads(capture_paths[0].read_bytes()) if capture_paths else None
        print(json.dumps({'owners': len(adapter._owners), 'owner_cancel_results': owners,
                          'caller_done': pending[0].done(), 'sends': len(server.requests),
                          'shutdown_notifications': notifications,
                          'capture_cancelled': None if capture is None else capture['cancelled']}))
    finally:
        fixture.doCleanups()


if __name__ == '__main__':
    if len(sys.argv) == 4 and sys.argv[1] == '--shutdown-probe':
        shutdown_probe(sys.argv[2], sys.argv[3])
    else:
        unittest.main()
