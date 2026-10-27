"""Synthetic boundary tests. These do not qualify a native OCR/raster engine."""
from dataclasses import asdict, replace
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

from document_library import metadata_ocr_runtime as runtime

HEADER = b'level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n'
PAGE = b'1\t1\t0\t0\t0\t0\t0\t0\t100\t50\t-1\t\n'
PARENTS = (b'2\t1\t1\t0\t0\t0\t0\t0\t100\t50\t-1\t\n'
           b'3\t1\t1\t1\t0\t0\t0\t0\t100\t50\t-1\t\n'
           b'4\t1\t1\t1\t1\t0\t0\t0\t100\t50\t-1\t\n')
WORD = '5\t1\t1\t1\t1\t1\t1\t2\t30\t10\t90.5\t가나다\n'.encode()
TSV = HEADER+PAGE+PARENTS+WORD


def pin(path, *, link=False):
    path = Path(path)
    data = path.read_bytes()
    return runtime.ArtifactPin(str(path),hashlib.sha256(data).hexdigest(),len(data),
                               str(path.resolve()) if link else None)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='ocr126-test-')
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.engine = self.base/'engine'
        self.engine.write_bytes(b'synthetic-not-an-engine')
        self.pack = self.base/'eng.traineddata'
        self.pack.write_bytes(b'synthetic-not-a-language-pack')
        self.library = self.base/'library'
        self.library.write_bytes(b'synthetic-not-a-native-library')
        executable = Path(os.path.abspath(sys.executable))
        self.profile = runtime.RuntimeProfile(
            interpreter=pin(executable,link=executable.is_symlink()),
            helper=pin(runtime.__file__),tesseract=pin(self.engine),
            traineddata=(pin(self.pack),),libraries=(pin(self.library),),languages=('eng',),
            versions=(('fixture','synthetic-v1'),),memory_policy='linux_rlimit_as',
            linux_address_space_bytes=256*1024**2,
            limits=runtime.RuntimeLimits(child_wall_seconds=0.3,term_grace_seconds=0.05,
                                         kill_grace_seconds=0.5))
        self.cancel = threading.Event()
        self.source_path = self.base/'source.png'
        self.source_path.write_bytes(b'not decoded in protocol fixtures')
        self.source = os.open(self.source_path,os.O_RDONLY)
        self.addCleanup(os.close,self.source)
        self.identity = runtime.SourceIdentity(hashlib.sha256(self.source_path.read_bytes()).hexdigest(),
                                               self.source_path.stat().st_size)
        self.raster = runtime.RasterIdentity(self.identity,100,50,'verified_full_page_preview')

    def error(self, code, call, *args, **kwargs):
        with self.assertRaises(runtime.OcrRuntimeError) as result:
            call(*args,**kwargs)
        self.assertEqual(result.exception.code,code)
        return result.exception

    def synthetic(self, code, *, limits=None, cancel=None):
        return runtime._run_owned([sys.executable,'-I','-B','-c',code],(),
            cancel or self.cancel,limits or self.profile.limits,
            (limits or self.profile.limits).child_wall_seconds)

    @staticmethod
    def receipt(*, confirmed=True, cpu=0., reason='completed'):
        return runtime.ChildReceipt('synthetic-owned-operation',None,0,reason,True,True,
                                    confirmed,confirmed,cpu,0,0)

    def protocol_child(self, data=TSV, metadata=None, cpu=0.):
        value = metadata if metadata is not None else dict(width=100,height=50,ink_fraction=0.)
        def child(argv, pass_fds, cancel, limits, wall):
            os.write(pass_fds[1],json.dumps(value).encode())
            return data,self.receipt(cpu=cpu)
        return child


class TsvTests(Fixture):
    def test_preserves_lines_and_character_weighted_quality_without_certification(self):
        tsv = TSV+b'5\t1\t1\t1\t1\t2\t40\t2\t10\t10\t0\tX\n'
        tsv += (b'4\t1\t1\t1\t2\t0\t0\t20\t100\t20\t-1\t\n'
                b'5\t1\t1\t1\t2\t1\t1\t20\t20\t10\t50\tZZ\n')
        result = runtime.parse_tsv(tsv,100,50)
        self.assertEqual(result['text'],'가나다 X\nZZ')
        self.assertEqual(result['confidence_bins'],(1,0,2,3))
        self.assertAlmostEqual(result['confidence_mean'],(90.5*3+50*2)/6)
        self.assertEqual(result['alnum_characters'],6)
        self.assertEqual(result['density'],1200.)
        self.assertTrue(result['order_uncertain'])
        self.assertEqual(result['status'],'review_required')
        self.assertEqual([w['word'] for w in result['words']],[1,2,1])

    def test_blank_and_empty_words_never_become_positive_evidence(self):
        blank = runtime.parse_tsv(HEADER+PAGE,100,50)
        self.assertEqual(blank['text'],'')
        self.assertIsNone(blank['confidence_mean'])
        self.assertEqual(blank['confidence_bins'],(0,0,0,0))
        self.assertEqual(blank['status'],'empty')
        empty_words = (HEADER+PAGE+PARENTS+
            b'5\t1\t1\t1\t1\t1\t0\t0\t0\t0\t100\t\n'+
            b'5\t1\t1\t1\t1\t2\t0\t0\t0\t0\t100\t\n')
        self.assertEqual(runtime.parse_tsv(empty_words,100,50)['text'],'')

    def test_malformed_numeric_hierarchy_order_bbox_and_text_are_rejected(self):
        cases = [TSV.replace(b'90.5',bad) for bad in (b'NaN',b'inf',b'1e999',b'-1',b'100.1')]
        cases += [TSV.replace(b'\t30\t10\t',b'\t999\t10\t'),
                  TSV.replace(b'5\t1\t1',b'5\t2\t1'),
                  TSV.replace(b'5\t1\t1\t1\t1\t1',b'5\t1\t1\t1\t1\t0'),
                  TSV.replace('가나다'.encode(),b'bad\0text'),
                  TSV.replace(WORD,WORD.replace(b'90.5',b'90\t5')),
                  TSV+WORD, HEADER+PAGE+WORD, TSV+b'\n',
                  TSV.replace(b'\t-1\t',b'\t100\t',1),
                  TSV.replace(b'5\t1',b'05\t1'),
                  TSV.replace(WORD,WORD.replace('가나다'.encode(),b'\xff'))]
        for candidate in cases:
            with self.subTest(candidate=candidate[-60:]):
                with self.assertRaises(runtime.OcrRuntimeError):
                    runtime.parse_tsv(candidate,100,50)
        reverse = TSV+b'5\t1\t1\t1\t1\t2\t40\t2\t10\t10\t50\tB\n'+WORD
        self.error('order_violation',runtime.parse_tsv,reverse,100,50)

    def test_size_limits_apply_without_truncation(self):
        self.error('data_too_large',runtime.parse_tsv,b'x'*(2*1024**2+1),100,50)
        self.error('too_many_rows',runtime.parse_tsv,TSV,100,50,max_rows=4)
        self.error('text_too_large',runtime.parse_tsv,TSV,100,50,max_text_chars=2)
        for args in ((True,50),(10001,1),(5000,5000),(0,50)):
            with self.assertRaises(runtime.OcrRuntimeError):
                runtime.parse_tsv(TSV,*args)


class OwnedProcessTests(Fixture):
    def assert_reaped(self, receipt):
        self.assertTrue(receipt.stop_confirmed)
        self.assertTrue(receipt.pipes_drained)
        self.assertTrue(receipt.group_absent)
        with self.assertRaises(ChildProcessError):
            os.waitpid(receipt.pid,os.WNOHANG)

    def test_idle_child_deadline_reaps_and_drains(self):
        before = time.monotonic()
        failure = self.error('deadline',self.synthetic,'import time; time.sleep(10)')
        self.assertLess(time.monotonic()-before,2.)
        self.assert_reaped(failure.receipt)

    def test_cancel_before_spawn_has_no_pid(self):
        self.cancel.set()
        with patch.object(runtime.subprocess,'Popen') as launch:
            failure = self.error('cancelled',self.synthetic,'raise RuntimeError')
        launch.assert_not_called()
        self.assertIsNone(failure.receipt.pid)
        self.assertTrue(failure.receipt.stop_confirmed)

    def test_cancel_during_sigterm_ignoring_child_escalates_and_reaps(self):
        marker = self.base/'ready'
        code = ('import signal,time,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                f'pathlib.Path({str(marker)!r}).write_text("ready"); time.sleep(10)')
        failures = []
        limits = replace(self.profile.limits,child_wall_seconds=3.)
        def own():
            try:
                self.synthetic(code,limits=limits)
            except BaseException as exc:
                failures.append(exc)
        thread = threading.Thread(target=own)
        thread.start()
        try:
            until = time.monotonic()+2
            while not marker.exists() and time.monotonic() < until:
                time.sleep(0.005)
            self.assertTrue(marker.exists())
            self.cancel.set()
        finally:
            self.cancel.set()
            thread.join(3.)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(failures),1)
        self.assertEqual(failures[0].code,'cancelled')
        self.assertEqual(failures[0].receipt.returncode,-signal.SIGKILL)
        self.assert_reaped(failures[0].receipt)

    def test_output_floods_are_bounded_and_do_not_block_reaping(self):
        for target,cap in ((1,self.profile.limits.result_bytes),(2,self.profile.limits.stderr_bytes)):
            with self.subTest(target=target):
                code = f'import os,time; os.write({target},b"x"*{cap+65536}); time.sleep(10)'
                failure = self.error('output_limit',self.synthetic,code)
                self.assert_reaped(failure.receipt)
                observed = failure.receipt.stdout_bytes if target == 1 else failure.receipt.stderr_bytes
                self.assertGreater(observed,cap)

    def test_owned_group_cleans_descendant_and_leader_reaps_it(self):
        code = '''import os,signal,time
pid=os.fork()
if pid == 0:
    time.sleep(10)
    os._exit(0)
def stop(sig,frame):
    os.waitpid(pid,0)
    os._exit(0)
signal.signal(signal.SIGTERM,stop)
print('ready',flush=True)
time.sleep(10)
'''
        failure = self.error('deadline',self.synthetic,code)
        self.assert_reaped(failure.receipt)

    def test_success_is_reaped_and_nonzero_is_failure(self):
        data,receipt = self.synthetic('import os; os.write(1,b"ok")')
        self.assertEqual(data,b'ok')
        self.assert_reaped(receipt)
        failure = self.error('child_failed',self.synthetic,'raise SystemExit(7)')
        self.assertEqual(failure.receipt.returncode,7)
        self.assert_reaped(failure.receipt)

    def test_group_uncertainty_never_returns_stop_proof(self):
        with patch.object(runtime,'_group_absent',return_value=False):
            failure = self.error('stop_unconfirmed',self.synthetic,'pass')
        self.assertTrue(failure.receipt.reaped)
        self.assertTrue(failure.receipt.pipes_drained)
        self.assertFalse(failure.receipt.stop_confirmed)

    def test_owner_exception_uses_owned_cleanup_once(self):
        select = runtime.selectors.DefaultSelector
        class Interrupted:
            def __init__(self):
                self.inner,self.first = select(),True
            def register(self,*args): return self.inner.register(*args)
            def unregister(self,*args): return self.inner.unregister(*args)
            def get_map(self): return self.inner.get_map()
            def close(self): return self.inner.close()
            def select(self,*args):
                if self.first:
                    self.first = False
                    raise KeyboardInterrupt('synthetic owner cancellation')
                return self.inner.select(*args)
        with patch.object(runtime.selectors,'DefaultSelector',Interrupted):
            failure = self.error('owner_interrupted',self.synthetic,'import time; time.sleep(10)')
        self.assertIsInstance(failure.__cause__,KeyboardInterrupt)
        self.assert_reaped(failure.receipt)


class SourceAndProfileTests(Fixture):
    def test_readonly_fd_snapshot_preserves_offset_and_survives_path_swap(self):
        os.lseek(self.source,4,os.SEEK_SET)
        renamed = self.base/'opened-original'
        self.source_path.rename(renamed)
        self.source_path.write_bytes(b'replaced pathname')
        snapshot = runtime._capture_source(self.source,self.identity,self.base/'captured',1024)
        try:
            self.assertEqual(os.read(snapshot,1024),renamed.read_bytes())
            self.assertEqual(os.lseek(self.source,0,os.SEEK_CUR),4)
            self.assertEqual(renamed.read_bytes(),b'not decoded in protocol fixtures')
        finally:
            os.close(snapshot)

    def test_writable_source_and_changed_bytes_rejected(self):
        writable = os.open(self.source_path,os.O_RDWR)
        self.addCleanup(os.close,writable)
        self.error('readonly_source_required',runtime._capture_source,writable,self.identity,
                   self.base/'captured',1024)
        self.source_path.write_bytes(b'x'*self.identity.byte_size)
        self.error('source_changed',runtime._capture_source,self.source,self.identity,
                   self.base/'changed',1024)

    def test_artifact_replacement_and_unobserved_symlink_are_rejected(self):
        original = pin(self.engine)
        self.engine.write_bytes(b'X'*original.byte_size)
        self.error('artifact_hash_changed',original.verify)
        link = self.base/'engine-link'
        link.symlink_to(self.engine)
        self.error('artifact_symlink',pin(link).verify)
        known = pin(link,link=True)
        known.verify()
        link.unlink()
        link.symlink_to(self.library)
        self.error('artifact_link_changed',known.verify)

    def test_fifo_artifact_and_swap_at_open_are_rejected_without_blocking(self):
        for swap in (False,True):
            with self.subTest(swap_at_open=swap):
                path = self.base/('fifo-race' if swap else 'fifo-before')
                # Equal zero sizes ensure that regular-file type is checked.
                path.write_bytes(b'')
                original = asdict(pin(path))
                code = '''import os,pathlib,runpy
m=runpy.run_path(MODULE)
target=pathlib.Path(TARGET)
artifact=m['ArtifactPin'](**PIN)
original_open,original_read=os.open,os.read
opened=[]
def swap_to_fifo():
    target.unlink()
    os.mkfifo(target,0o600)
if not SWAP:
    swap_to_fifo()
def observed_open(path,flags,*args,**kwargs):
    assert flags & os.O_NONBLOCK and flags & os.O_NOFOLLOW
    if SWAP:
        swap_to_fifo()
    fd=original_open(path,flags,*args,**kwargs)
    opened.append(fd)
    return fd
def forbidden_read(*args):
    raise AssertionError('special file read before regular fstat')
os.open,os.read=observed_open,forbidden_read
try:
    artifact.verify()
except m['OcrRuntimeError'] as exc:
    assert exc.code == 'artifact_size_changed',exc.code
else:
    raise AssertionError('FIFO artifact accepted')
finally:
    os.open,os.read=original_open,original_read
assert len(opened) == 1
try:
    os.fstat(opened[0])
except OSError:
    pass
else:
    raise AssertionError('artifact descriptor leaked')
os.write(1,b'rejected')
'''
                code = ('MODULE='+repr(runtime.__file__)+'\nTARGET='+repr(str(path))+
                        '\nPIN='+repr(original)+'\nSWAP='+repr(swap)+'\n'+code)
                data,receipt = self.synthetic(code,limits=replace(self.profile.limits,
                                                                 child_wall_seconds=3.))
                self.assertEqual(data,b'rejected')
                self.assert_reaped_if_needed(receipt)

    def test_exact_languages_orientation_and_limits_are_explicit(self):
        self.profile.verify()
        for change in ({'languages':()}, {'languages':('unknown',)}, {'languages':('eng','eng')},
                       {'languages':('jpn_vert',)}, {'orientation':'vertical'}, {'oem':3}, {'psm':12},
                       {'memory_policy':'darwin_as_unavailable'}):
            with self.subTest(change=change),self.assertRaises(runtime.OcrRuntimeError):
                replace(self.profile,**change)
        for change in ({'child_cpu_seconds':31},{'page_pixels':16000001},{'children':401},
                       {'whole_wall_seconds':float('inf')},{'pages':True}):
            with self.assertRaises(runtime.OcrRuntimeError):
                runtime.RuntimeLimits(**change)
        with self.assertRaises(runtime.OcrRuntimeError):
            runtime.RasterIdentity(self.identity,100,50,'thumbnail')
        restored = runtime._profile_from_wire(json.loads(json.dumps(asdict(self.profile))))
        self.assertEqual(restored.digest,self.profile.digest)
        self.assertEqual(restored.order_policy,'tsv_native_order_uncertain_v1')
        self.assertEqual(restored.quality_policy,'confidence_ink_density_review_only_v1')

    def test_import_does_not_load_native_packages(self):
        code = ('import runpy,sys; runpy.run_path('+repr(runtime.__file__)+'); '
                'assert "PIL" not in sys.modules and "pypdfium2" not in sys.modules')
        self.synthetic(code,limits=replace(self.profile.limits,child_wall_seconds=3.))

    def test_child_resource_settings_are_applied_in_disposable_process(self):
        code = ('import runpy,types,resource,json; m=runpy.run_path('+repr(runtime.__file__)+'); '
                'p=types.SimpleNamespace(limits=types.SimpleNamespace(child_cpu_seconds=1,'
                'source_bytes=1048576,png_bytes=1048576),'
                'memory_policy="linux_rlimit_as",linux_address_space_bytes=268435456); '
                'm["_child_limits"](p,1); print(json.dumps([resource.getrlimit(resource.RLIMIT_CPU),'
                'resource.getrlimit(resource.RLIMIT_FSIZE),resource.getrlimit(resource.RLIMIT_AS)]))')
        data,receipt = self.synthetic(code,limits=replace(self.profile.limits,child_wall_seconds=3.))
        self.assertEqual(json.loads(data),[[1,1],[1048576,1048576],[268435456,268435456]])
        self.assert_reaped_if_needed(receipt)

    def test_loaded_native_module_must_match_an_explicit_source_pin(self):
        original_runtime_sys = runtime.sys
        original_modules = sys.modules
        original_pil_modules = {
            name: module for name, module in tuple(sys.modules.items())
            if name.split('.', 1)[0] == 'PIL'
        }
        fake = types.SimpleNamespace(__file__=str(self.library))
        synthetic_modules = {'PIL.synthetic': fake}
        observation = types.SimpleNamespace(modules=synthetic_modules)
        with patch.object(runtime, 'sys', observation):
            runtime._verify_native_modules(self.profile)
            fake.__file__ = str(self.engine)
            self.error('unpinned_native_module', runtime._verify_native_modules, self.profile)
            fake.__file__ = str(self.library) + '.pyc'
            self.error('native_module_identity', runtime._verify_native_modules, self.profile)
        self.assertIs(runtime.sys, original_runtime_sys)
        self.assertIs(sys.modules, original_modules)
        self.assertEqual(
            {
                name: module for name, module in tuple(sys.modules.items())
                if name.split('.', 1)[0] == 'PIL'
            },
            original_pil_modules,
        )
        for name, module in original_pil_modules.items():
            self.assertIs(sys.modules[name], module)

    def assert_reaped_if_needed(self, receipt):
        self.assertTrue(receipt.reaped and receipt.stop_confirmed)


class RuntimeProtocolTests(Fixture):
    def test_ocr_protocol_empty_nonempty_and_no_accuracy_or_option_claim(self):
        for tsv,expected in ((TSV,'가나다'),(HEADER+PAGE,'')):
            with self.subTest(expected=expected):
                owner = runtime.OcrRuntime(self.profile)
                with patch.object(runtime,'_run_owned',side_effect=self.protocol_child(data=tsv)):
                    result = owner.ocr_image(self.source,self.raster,page_id='page-1',cancel=self.cancel)
                self.assertEqual(result.text,expected)
                receipt = result.receipt
                self.assertEqual(receipt['visual_accuracy'],'NOT_CERTIFIED')
                self.assertEqual(receipt['quality_qualification'],'NOT_RUN')
                self.assertEqual(receipt['blank_hallucination_check'],'NOT_QUALIFIED')
                self.assertFalse(receipt['complete_selected_option'])
                self.assertTrue(receipt['order_uncertain'])
                self.assertEqual(receipt['ink_fraction'],0.)
                self.assertEqual(owner.children,1)
                self.assertEqual(owner.characters,len(expected))

    def test_malformed_bounded_protocol_is_not_a_completed_page(self):
        for meta in ({'width':100,'height':50,'ink_fraction':float('nan')},
                     {'width':101,'height':50,'ink_fraction':0.},
                     {'width':100,'height':50,'ink_fraction':0.,'extra':1}):
            owner = runtime.OcrRuntime(self.profile)
            with patch.object(runtime,'_run_owned',side_effect=self.protocol_child(metadata=meta)):
                failure = self.error('protocol_nonfinite' if math.isnan(meta['ink_fraction']) else
                                     'invalid_raster_receipt',owner.ocr_image,self.source,self.raster,
                                     page_id='page-1',cancel=self.cancel)
            self.assertTrue(failure.receipt.stop_confirmed)
            self.assertEqual(owner.characters,0)
        for data in (b'{"x":1,"x":2}',b'{"x":NaN}',b'x'*8193,b'\xff'):
            with self.assertRaises(runtime.OcrRuntimeError):
                runtime._strict_json(data,8192)

    def test_stop_uncertainty_latches_owner_and_preserves_exact_stage(self):
        owner = runtime.OcrRuntime(self.profile)
        failure_receipt = self.receipt(confirmed=False,reason='stop_unconfirmed')
        with patch.object(runtime,'_run_owned',side_effect=runtime.OcrRuntimeError(
                'stop_unconfirmed',receipt=failure_receipt)) as run:
            failure = self.error('stop_unconfirmed',owner.ocr_image,self.source,self.raster,
                                 page_id='page-1',cancel=self.cancel)
            self.assertTrue(Path(failure.retained_stage).is_dir())
            try:
                self.error('previous_stop_unconfirmed',owner.ocr_image,self.source,self.raster,
                           page_id='page-2',cancel=self.cancel)
                self.assertEqual(run.call_count,1)
            finally:
                # Fixture returned no actual PID; this is only our retained fake stage.
                self.assertIsNone(failure_receipt.pid)
                import shutil
                shutil.rmtree(failure.retained_stage)

    def test_cpu_and_wall_budget_checked_again_before_next_child(self):
        limits = replace(self.profile.limits,whole_cpu_seconds=1.5,child_cpu_seconds=1)
        owner = runtime.OcrRuntime(replace(self.profile,limits=limits))
        # Isolate receipt bookkeeping from host hashing cost. Other tests retain
        # real artifact hashing and disposable-process resource-limit coverage.
        with patch.object(runtime.time,'thread_time',return_value=0.),\
             patch.object(runtime,'_run_owned',side_effect=self.protocol_child(cpu=0.6)) as run:
            result = owner.ocr_image(self.source,self.raster,page_id='first',cancel=self.cancel)
            self.assertEqual(result.receipt['resources']['cpu_limit_seconds'],1)
            self.error('preparation_budget_exhausted',owner.ocr_image,self.source,self.raster,
                       page_id='second',cancel=self.cancel)
            self.assertEqual(run.call_count,1)
        owner = runtime.OcrRuntime(self.profile)
        def expired_verification(*args):
            owner.started -= 1000
        with patch.object(runtime.RuntimeProfile,'verify',side_effect=expired_verification),\
             patch.object(runtime,'_run_owned') as run:
            self.error('preparation_budget_exhausted',owner.ocr_image,self.source,self.raster,
                       page_id='first',cancel=self.cancel)
            run.assert_not_called()

    def test_child_cpu_request_accounts_for_parent_preparation(self):
        clock = [0.]
        limits = replace(self.profile.limits,whole_cpu_seconds=2.5,child_cpu_seconds=2)
        owner = runtime.OcrRuntime(replace(self.profile,limits=limits))
        verify,child = runtime.RuntimeProfile.verify,self.protocol_child()
        def preparation(profile):
            verify(profile)
            clock[0] = 1.1
        def inspect_launch(argv,pass_fds,cancel,limits,wall):
            request = json.loads(os.pread(pass_fds[2],384*1024,0))
            self.assertEqual(request['cpu_seconds'],1)
            self.assertLessEqual(request['cpu_seconds'],owner._remaining_budget()[1])
            return child(argv,pass_fds,cancel,limits,wall)
        with patch.object(runtime.time,'thread_time',side_effect=lambda:clock[0]),\
             patch.object(runtime.RuntimeProfile,'verify',preparation),\
             patch.object(runtime,'_run_owned',side_effect=inspect_launch) as run:
            result = owner.ocr_image(self.source,self.raster,page_id='fresh',cancel=self.cancel)
        self.assertEqual(result.receipt['resources']['cpu_limit_seconds'],1)
        self.assertEqual(owner.parent_cpu_seconds,1.1)
        self.assertEqual(run.call_count,1)

    def test_serialization_cpu_boundary_rejects_before_launch_without_retry(self):
        clock,requests = [0.],[]
        limits = replace(self.profile.limits,whole_cpu_seconds=2.5,child_cpu_seconds=2)
        owner = runtime.OcrRuntime(replace(self.profile,limits=limits))
        encode,verify = runtime._json,runtime.RuntimeProfile.verify
        def preparation(profile):
            verify(profile)
            clock[0] = 0.3
        def serialization(value):
            data = encode(value)
            if type(value) is dict and 'cpu_seconds' in value and 'stage' in value:
                requests.append(value['cpu_seconds'])
                clock[0] += 0.3
            return data
        with patch.object(runtime.time,'thread_time',side_effect=lambda:clock[0]),\
             patch.object(runtime.RuntimeProfile,'verify',preparation),\
             patch.object(runtime,'_json',side_effect=serialization),\
             patch.object(runtime,'_run_owned') as run:
            self.error('preparation_budget_exhausted',owner.ocr_image,self.source,self.raster,
                       page_id='serialization',cancel=self.cancel)
            run.assert_not_called()
        self.assertEqual(requests,[2])
        self.assertEqual(owner.children,0)
        self.assertEqual(owner.parent_cpu_seconds,0.6)

    def test_post_child_artifact_drift_discards_output_but_preserves_stop_proof(self):
        owner = runtime.OcrRuntime(self.profile)
        def changed(*args):
            value = self.protocol_child()(*args)
            self.engine.write_bytes(b'X'*self.engine.stat().st_size)
            return value
        with patch.object(runtime,'_run_owned',side_effect=changed):
            failure = self.error('artifact_hash_changed',owner.ocr_image,self.source,self.raster,
                                 page_id='one',cancel=self.cancel)
        self.assertTrue(failure.receipt.stop_confirmed)
        self.assertEqual(owner.characters,0)

    def test_whole_text_limit_rejects_second_page_without_truncation(self):
        owner = runtime.OcrRuntime(replace(self.profile,limits=replace(self.profile.limits,
            whole_characters=4)))
        with patch.object(runtime,'_run_owned',side_effect=self.protocol_child()) as run:
            first = owner.ocr_image(self.source,self.raster,page_id='one',cancel=self.cancel)
            failure = self.error('text_too_large',owner.ocr_image,self.source,self.raster,
                                 page_id='two',cancel=self.cancel)
        self.assertEqual(first.text,'가나다')
        self.assertTrue(failure.receipt.stop_confirmed)
        self.assertEqual(owner.characters,3)
        self.assertEqual(run.call_count,2)

    def test_page_pixel_whole_text_and_single_child_limits(self):
        owner = runtime.OcrRuntime(replace(self.profile,limits=replace(self.profile.limits,
            total_pixels=6000,pages=2,whole_characters=3)))
        with patch.object(runtime,'_run_owned',side_effect=self.protocol_child()) as run:
            owner.ocr_image(self.source,self.raster,page_id='one',cancel=self.cancel)
            self.error('duplicate_page_operation',owner.ocr_image,self.source,self.raster,
                       page_id='one',cancel=self.cancel)
            self.error('aggregate_pixel_limit',owner.ocr_image,self.source,self.raster,
                       page_id='two',cancel=self.cancel)
            self.assertEqual(run.call_count,1)
        owner = runtime.OcrRuntime(self.profile)
        self.assertTrue(owner._lock.acquire(False))
        try:
            self.error('concurrent_child_forbidden',owner.ocr_image,self.source,self.raster,
                       page_id='one',cancel=self.cancel)
        finally:
            owner._lock.release()

    def test_cancel_after_reap_discards_output_with_matching_stop_receipt(self):
        owner = runtime.OcrRuntime(self.profile)
        def child(*args):
            output,receipt = self.protocol_child()(*args)
            self.cancel.set()
            return output,receipt
        with patch.object(runtime,'_run_owned',side_effect=child):
            failure = self.error('cancelled',owner.ocr_image,self.source,self.raster,
                                 page_id='one',cancel=self.cancel)
        self.assertTrue(failure.receipt.stop_confirmed)
        self.assertEqual(owner.characters,0)

    def test_pdf_preallocation_rejects_huge_page_without_rendering(self):
        page = types.SimpleNamespace(get_size=lambda:(1000000.,1000000.),close=lambda:None)
        rendered = []
        page.render = lambda **kw: rendered.append(kw)
        class Document:
            def __init__(self,*unused): pass
            def __enter__(self): return self
            def __exit__(self,*unused): pass
            def __len__(self): return 1
            def __getitem__(self,index): return page
        with patch.dict(sys.modules,{'pypdfium2':types.SimpleNamespace(PdfDocument=Document)}),\
             patch.object(runtime,'_verify_native_modules'):
            self.error('page_pixel_limit',runtime._child_pdf,self.source,-1,
                       {'page_count':1,'page_index':0,'remaining_pixels':400000000},self.profile)
        self.assertEqual(rendered,[])

    def test_pdf_result_is_readonly_descriptor_with_order_rotation_and_hash(self):
        owner = runtime.OcrRuntime(self.profile)
        inspection = runtime.PdfInspection(self.identity,2,'a'*64)
        png = b'synthetic-raster-bytes-parent-does-not-decode'
        def child(argv,pass_fds,cancel,limits,wall):
            request = json.loads(os.pread(pass_fds[2],384*1024,0))
            self.assertEqual(request['page_index'],1)
            self.assertEqual(request['page_count'],2)
            os.write(pass_fds[1],png)
            return json.dumps(dict(width=100,height=50,rotation=90,cropbox=[0,0,36,72])).encode(),self.receipt()
        with patch.object(runtime,'_run_owned',side_effect=child):
            with owner.render_pdf_page(self.source,inspection,1,page_id='pdf-2',cancel=self.cancel) as page:
                self.error('previous_raster_descriptor_open',owner.render_pdf_page,
                           self.source,inspection,0,page_id='pdf-1',cancel=self.cancel)
                self.assertEqual(os.read(page.fd,100),png)
                with self.assertRaises(OSError): os.write(page.fd,b'no')
                self.assertEqual(page.raster.source.sha256,hashlib.sha256(png).hexdigest())
                self.assertEqual(page.receipt['page_index'],1)
                self.assertEqual(page.receipt['rotation'],90)
                descriptor = page.fd
            with self.assertRaises(OSError): os.fstat(descriptor)
        self.error('pdf_page_limit',owner.render_pdf_page,self.source,inspection,2,
                   page_id='bad',cancel=self.cancel)

    def test_source_snapshot_runs_inside_owned_child_and_preserves_parent_fd(self):
        os.lseek(self.source,4,os.SEEK_SET)
        destination = self.base/'child-owned-copy'
        code = ('import runpy,os; m=runpy.run_path('+repr(runtime.__file__)+'); '
                'identity=m["SourceIdentity"]('+repr(self.identity.sha256)+','+
                str(self.identity.byte_size)+'); '
                'fd=m["_capture_source"]('+str(self.source)+',identity,'+
                repr(str(destination))+',1024); '
                'm["_child_verify_source"](fd,identity,1024); '
                'os.write(1,os.read(fd,1024)); os.close(fd)')
        data,receipt = runtime._run_owned([sys.executable,'-I','-B','-c',code],(self.source,),
            self.cancel,replace(self.profile.limits,child_wall_seconds=3.),3.)
        self.assertEqual(data,self.source_path.read_bytes())
        self.assertEqual(os.lseek(self.source,0,os.SEEK_CUR),4)
        self.assertTrue(receipt.stop_confirmed)


if __name__ == '__main__':
    unittest.main()
