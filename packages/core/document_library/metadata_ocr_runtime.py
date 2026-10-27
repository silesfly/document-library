"""Private, blocking, one-child OCR primitives; this module grants no read authority.

Call from an owned worker thread and signal its Event on lease/caller cancellation.
Awaiting a thread Future alone does not stop native work. The caller owns source,
option/rights checks, one local-ocr hold and whole-option completeness. No services,
Pillow or PDFium are imported in the parent. Native qualification is separate.
"""
from dataclasses import asdict, dataclass, field
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid


class OcrRuntimeError(Exception):
    def __init__(self, code, *, receipt=None, retained_stage=None):
        super().__init__(code)
        self.code, self.receipt, self.retained_stage = code, receipt, retained_stage


def _require(value, code):
    if not value:
        raise OcrRuntimeError(code)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


@dataclass(frozen=True)
class RuntimeLimits:
    source_bytes: int = 64 * 1024**2
    pages: int = 200
    page_pixels: int = 16000000
    edge_pixels: int = 10000
    total_pixels: int = 400000000
    stage_bytes: int = 256 * 1024**2
    png_bytes: int = 32 * 1024**2
    result_bytes: int = 2 * 1024**2
    stderr_bytes: int = 16 * 1024
    page_characters: int = 100000
    whole_characters: int = 2000000
    child_cpu_seconds: int = 30
    child_wall_seconds: float = 45.0
    whole_cpu_seconds: float = 600.0
    whole_wall_seconds: float = 900.0
    children: int = 400
    manifest_bytes: int = 512 * 1024
    term_grace_seconds: float = 0.25
    kill_grace_seconds: float = 1.0

    def __post_init__(self):
        ceilings = (64*1024**2, 200, 16000000, 10000, 400000000, 256*1024**2,
                    32*1024**2, 2*1024**2, 16*1024, 100000, 2000000, 30, 45,
                    600, 900, 400, 512*1024, 1, 2)
        for (name, value), cap in zip(asdict(self).items(), ceilings):
            _require(type(value) in (int, float) and math.isfinite(value)
                     and 0 < value <= cap, 'invalid_runtime_limits')
            if name not in {'child_wall_seconds', 'whole_cpu_seconds', 'whole_wall_seconds',
                            'term_grace_seconds', 'kill_grace_seconds'}:
                _require(type(value) is int, 'invalid_runtime_limits')


@dataclass(frozen=True)
class ArtifactPin:
    """An explicit installed file and, only when observed, its exact symlink target."""
    path: str
    sha256: str
    byte_size: int
    resolved_path: str | None = None

    def __post_init__(self):
        _require(type(self.path) is str and Path(self.path).is_absolute()
                 and '..' not in Path(self.path).parts, 'invalid_artifact_path')
        _require(type(self.sha256) is str and len(self.sha256) == 64
                 and all(c in '0123456789abcdef' for c in self.sha256), 'invalid_artifact_hash')
        _require(type(self.byte_size) is int and 0 <= self.byte_size <= 64*1024**2,
                 'invalid_artifact_size')
        if self.resolved_path is not None:
            _require(Path(self.resolved_path).is_absolute(), 'invalid_artifact_link')

    def verify(self):
        path = Path(self.path)
        if self.resolved_path is None:
            _require(not path.is_symlink(), 'artifact_symlink')
        else:
            _require(path.is_symlink() and str(path.resolve(strict=True)) == self.resolved_path,
                     'artifact_link_changed')
        actual = self.resolved_path or self.path
        fd = os.open(actual, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            _require(stat.S_ISREG(info.st_mode) and info.st_size == self.byte_size,
                     'artifact_size_changed')
            digest, total = hashlib.sha256(), 0
            while piece := os.read(fd, 1024**2):
                total += len(piece)
                _require(total <= self.byte_size, 'artifact_size_changed')
                digest.update(piece)
            _require(total == self.byte_size and digest.hexdigest() == self.sha256,
                     'artifact_hash_changed')
            _require(_identity(info) == _identity(os.fstat(fd)), 'artifact_changed')
            _require(_identity(info) == _identity(os.stat(actual, follow_symlinks=False)),
                     'artifact_replaced')
            if self.resolved_path is not None:
                _require(str(path.resolve(strict=True)) == self.resolved_path,
                         'artifact_link_changed')
        finally:
            os.close(fd)


@dataclass(frozen=True)
class RuntimeProfile:
    interpreter: ArtifactPin
    helper: ArtifactPin
    tesseract: ArtifactPin
    traineddata: tuple[ArtifactPin, ...]
    libraries: tuple[ArtifactPin, ...]
    languages: tuple[str, ...]
    versions: tuple[tuple[str, str], ...]
    psm: int = 3
    oem: int = 1
    dpi: int = 200
    preprocessing: str = 'rgb_full_resolution_v1'
    orientation: str = 'horizontal'
    order_policy: str = 'tsv_native_order_uncertain_v1'
    quality_policy: str = 'confidence_ink_density_review_only_v1'
    limits: RuntimeLimits = field(default_factory=RuntimeLimits)
    memory_policy: str = 'darwin_as_unavailable'
    linux_address_space_bytes: int | None = None

    def __post_init__(self):
        _require(type(self.languages) is tuple and 0 < len(self.languages) <= 6
                 and len(set(self.languages)) == len(self.languages)
                 and all(v in {'kor','jpn','jpn_vert','chi_sim','chi_tra','eng'}
                         for v in self.languages), 'explicit_languages_required')
        _require(type(self.traineddata) is tuple and len(self.traineddata) == len(self.languages),
                 'traineddata_profile')
        parents = {str(Path(p.path).parent) for p in self.traineddata}
        _require(len(parents) == 1 and tuple(Path(p.path).name for p in self.traineddata)
                 == tuple(v+'.traineddata' for v in self.languages), 'traineddata_profile')
        _require(self.orientation in {'horizontal','vertical'} and
                 (('jpn_vert' in self.languages) == (self.orientation == 'vertical')),
                 'orientation_profile')
        _require(type(self.psm) is int and self.psm in {3,5,6,11}
                 and type(self.oem) is int and self.oem == 1
                 and self.dpi == 200 and self.preprocessing == 'rgb_full_resolution_v1',
                 'unsupported_ocr_profile')
        _require(self.order_policy == 'tsv_native_order_uncertain_v1'
                 and self.quality_policy == 'confidence_ink_density_review_only_v1',
                 'unsupported_quality_policy')
        _require(type(self.libraries) is tuple and 0 < len(self.libraries) <= 256
                 and type(self.versions) is tuple and all(type(k) is str and type(v) is str
                     and 0 < len(k) <= 64 and 0 < len(v) <= 128 for k,v in self.versions),
                 'runtime_library_profile')
        _require(isinstance(self.limits, RuntimeLimits), 'invalid_runtime_limits')
        _require(self.memory_policy in {'darwin_as_unavailable','linux_rlimit_as'},
                 'unsupported_memory_policy')
        if self.memory_policy == 'linux_rlimit_as':
            _require(type(self.linux_address_space_bytes) is int and
                     64*1024**2 <= self.linux_address_space_bytes <= 2*1024**3,
                     'invalid_address_space_limit')
        else:
            _require(self.linux_address_space_bytes is None, 'darwin_hard_as_not_qualified')
        paths = [p.path for p in self.artifacts]
        _require(len(paths) == len(set(paths)), 'duplicate_artifact_pin')
        _require(len(_json(asdict(self))) <= 256*1024, 'profile_size_limit')

    @property
    def artifacts(self):
        return (self.interpreter, self.helper, self.tesseract, *self.traineddata, *self.libraries)

    @property
    def digest(self):
        return _sha(_json(asdict(self)))

    def verify(self):
        _require(str(Path(__file__).resolve()) == (self.helper.resolved_path or self.helper.path),
                 'helper_identity_mismatch')
        for pin in self.artifacts:
            pin.verify()
        if self.memory_policy == 'linux_rlimit_as':
            _require(sys.platform.startswith('linux'), 'memory_policy_host_mismatch')
        else:
            _require(sys.platform == 'darwin', 'memory_policy_host_mismatch')


def _identity(info):
    return tuple(getattr(info, key) for key in
                 ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns'))


def _dimensions(width, height, limits):
    _require(type(width) is int and type(height) is int and 0 < width <= limits.edge_pixels
             and 0 < height <= limits.edge_pixels and width*height <= limits.page_pixels,
             'page_pixel_limit')


@dataclass(frozen=True)
class SourceIdentity:
    sha256: str
    byte_size: int

    def __post_init__(self):
        _require(type(self.sha256) is str and len(self.sha256) == 64
                 and all(c in '0123456789abcdef' for c in self.sha256), 'invalid_source_hash')
        _require(type(self.byte_size) is int and 0 < self.byte_size <= 64*1024**2,
                 'source_byte_limit')


@dataclass(frozen=True)
class RasterIdentity:
    source: SourceIdentity
    width: int
    height: int
    origin: str
    exif_corrected: bool = True

    def __post_init__(self):
        _require(isinstance(self.source, SourceIdentity), 'raster_source_identity')
        _dimensions(self.width, self.height, RuntimeLimits())
        _require(self.origin in {'verified_full_page_preview','pdfium_raster'}
                 and self.exif_corrected is True, 'verified_full_page_required')


@dataclass
class RenderedPage:
    fd: int
    raster: RasterIdentity
    receipt: dict

    def close(self):
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


@dataclass(frozen=True)
class OcrPageResult:
    text: str
    words: tuple[dict, ...]
    receipt: dict


def _capture_source(fd, identity, destination, maximum):
    before = _source_stat(fd,identity,maximum)
    digest, position = hashlib.sha256(), 0
    with open(destination, 'xb', buffering=0) as target:
        while position < identity.byte_size:
            data = os.pread(fd, min(1024**2, identity.byte_size-position), position)
            _require(bool(data), 'source_changed')
            target.write(data)
            digest.update(data)
            position += len(data)
    _require(_identity(before) == _identity(os.fstat(fd)) and
             digest.hexdigest() == identity.sha256, 'source_changed')
    os.chmod(destination, 0o400)
    return os.open(destination, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)


def _source_stat(fd,identity,maximum):
    _require(type(fd) is int and fd >= 3 and isinstance(identity, SourceIdentity),
             'protected_fd_required')
    _require(fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY,
             'readonly_source_required')
    before = os.fstat(fd)
    _require(stat.S_ISREG(before.st_mode) and before.st_size == identity.byte_size
             and 0 < before.st_size <= maximum, 'source_byte_limit')
    return before


@dataclass(frozen=True)
class ChildReceipt:
    operation_id: str
    pid: int | None
    returncode: int | None
    reason: str
    reaped: bool
    pipes_drained: bool
    group_absent: bool
    stop_confirmed: bool
    cpu_seconds: float
    stdout_bytes: int
    stderr_bytes: int


def _group_absent(pid):
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def _run_owned(argv, pass_fds, cancel, limits, wall_seconds):
    """Private no-shell runner; retain the leader PID until all signals are sent.

    WNOWAIT keeps even an exited leader's PID reserved while its owned process
    group is signalled. We never signal the numerical group after reaping it.
    """
    operation = uuid.uuid4().hex
    if cancel.is_set():
        receipt = ChildReceipt(operation,None,None,'cancelled',True,True,True,True,0,0,0)
        raise OcrRuntimeError('cancelled',receipt=receipt)
    _require(type(wall_seconds) in (int,float) and 0 < wall_seconds <= limits.child_wall_seconds,
             'child_wall_budget')
    started = time.monotonic()
    process = subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,pass_fds=pass_fds,close_fds=True,start_new_session=True,
        env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','OMP_THREAD_LIMIT':'1',
             'PYTHONNOUSERSITE':'1'})
    selector,setup_complete = None,False
    output,counts = [bytearray(),bytearray()],[0,0]
    reason,phase,stop_at,owner_error = 'completed',None,None,None
    reaped,drained,cpu = False,False,0.
    owns_leader = True

    def signal_owned(sig):
        # All callers run before our single wait4. This PID cannot be recycled.
        if not owns_leader:
            return
        try:
            os.killpg(process.pid,sig)
        except OSError:
            pass  # A signal attempt is never itself stop evidence.

    def exited():
        nonlocal owns_leader
        try:
            value = os.waitid(os.P_PID,process.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
        except ChildProcessError:
            owns_leader = False
            raise
        return value is not None

    def drain(timeout,retain=True):
        for key,unused in selector.select(timeout):
            try:
                chunk = os.read(key.fd,65536)
            except BlockingIOError:
                continue
            if not chunk:
                selector.unregister(key.fileobj)
                key.fileobj.close()
                continue
            index = key.data
            counts[index] += len(chunk)
            if retain:
                cap = limits.result_bytes if index == 0 else limits.stderr_bytes
                output[index].extend(chunk[:max(0,cap-len(output[index]))])

    try:
        selector = selectors.DefaultSelector()
        for index,pipe in enumerate((process.stdout,process.stderr)):
            os.set_blocking(pipe.fileno(),False)
            selector.register(pipe,selectors.EVENT_READ,index)
        setup_complete = True
        while True:
            now = time.monotonic()
            overflow = counts[0] > limits.result_bytes or counts[1] > limits.stderr_bytes
            if phase is None and (cancel.is_set() or now-started >= wall_seconds or overflow):
                reason = 'cancelled' if cancel.is_set() else 'output_limit' if overflow else 'deadline'
                phase,stop_at = 'term',now
                signal_owned(signal.SIGTERM)
            if phase == 'term' and now-stop_at >= limits.term_grace_seconds:
                phase,stop_at = 'kill',now
                signal_owned(signal.SIGKILL)
            leader_exited = exited()
            if leader_exited and not selector.get_map():
                break
            if phase == 'kill' and now-stop_at >= limits.kill_grace_seconds:
                reason = 'stop_unconfirmed'
                break
            # Deadlines and cancellation are checked even when no pipe is ready.
            drain(0.02)
    except BaseException as exc:
        owner_error,reason = exc,'owner_interrupted'
        signal_owned(signal.SIGKILL)
        until = time.monotonic()+limits.kill_grace_seconds
        try:
            while setup_complete and time.monotonic() < until:
                drain(0.01,retain=False)
                if exited() and not selector.get_map():
                    break
        except BaseException:
            pass  # Unknown cleanup must produce an unconfirmed receipt.
    finally:
        # Close a leftover descendant group while the leader still reserves PID.
        signal_owned(signal.SIGKILL)
        try:
            waited,status,usage = os.wait4(process.pid,os.WNOHANG)
            if waited:
                reaped,cpu = True,usage.ru_utime+usage.ru_stime
                process.returncode = os.waitstatus_to_exitcode(status)
        except OSError:
            pass
        drained = setup_complete and not bool(selector.get_map())
        if selector is not None:
            selector.close()
        for pipe in (process.stdout,process.stderr):
            if not pipe.closed:
                pipe.close()
    absent = reaped and _group_absent(process.pid)
    receipt = ChildReceipt(operation,process.pid,process.returncode,reason,reaped,drained,absent,
                           reaped and drained and absent,cpu,counts[0],counts[1])
    if owner_error is not None:
        raise OcrRuntimeError('owner_interrupted',receipt=receipt) from owner_error
    if not receipt.stop_confirmed or reason != 'completed' or process.returncode:
        code = 'stop_unconfirmed' if not receipt.stop_confirmed else reason
        raise OcrRuntimeError(code if code != 'completed' else 'child_failed',receipt=receipt)
    return bytes(output[0]),receipt


def parse_tsv(data, width, height, *, max_rows=100000, max_text_chars=100000):
    """Parse one-page Tesseract TSV into bounded, uncertain quality evidence."""
    import math
    import re
    import unicodedata

    def fail(code):
        raise OcrRuntimeError(code)

    if type(data) is not bytes:
        fail("invalid_data_type")
    if len(data) > 2097152:
        fail("data_too_large")
    if type(width) is not int or type(height) is not int:
        fail("invalid_dimensions")
    if width <= 0 or height <= 0 or width > 10000 or height > 10000:
        fail("invalid_dimensions")
    if width * height > 16000000:
        fail("invalid_dimensions")
    if type(max_rows) is not int or type(max_text_chars) is not int:
        fail("invalid_limits")
    if max_rows <= 0 or max_rows > 100000 or max_text_chars <= 0 or max_text_chars > 100000:
        fail("invalid_limits")

    try:
        text_all = data.decode("utf-8", "strict")
    except UnicodeDecodeError:
        fail("invalid_utf8")

    header = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"
    if not text_all.startswith(header):
        fail("invalid_header")
    if len(text_all) == len(header):
        fail("missing_rows")
    if text_all[len(header)] not in "\r\n":
        fail("invalid_header")
    if text_all.startswith(header + "\r\n"):
        rest = text_all[len(header) + 2:]
    elif text_all.startswith(header + "\n"):
        rest = text_all[len(header) + 1:]
    elif text_all.startswith(header + "\r"):
        rest = text_all[len(header) + 1:]
    else:
        fail("invalid_header")

    # Normalize CRLF and lone CR line endings only after the header.  A final
    # newline is permitted, but any additional malformed/blank row is not.
    if "\r" in rest:
        if "\r\n" in rest:
            rest = rest.replace("\r\n", "\n")
        if "\r" in rest:
            rest = rest.replace("\r", "\n")

    if rest == "":
        fail("missing_rows")
    if rest.endswith("\n"):
        rest = rest[:-1]
        if rest == "":
            fail("missing_rows")
    lines = rest.split("\n")
    if len(lines) > max_rows:
        fail("too_many_rows")

    int_re = re.compile(r"(?:0|[1-9][0-9]*)\Z")
    # A finite decimal float.  No NaN/Inf spellings, whitespace, or signs
    # other than a leading minus are accepted here.
    float_re = re.compile(r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")

    def parse_uint(s):
        if int_re.fullmatch(s) is None:
            fail("invalid_integer")
        try:
            value = int(s)
        except (TypeError, ValueError):
            fail("invalid_integer")
        return value

    def parse_confidence(s):
        if s == "-1":
            return -1.0
        if float_re.fullmatch(s) is None:
            fail("invalid_confidence")
        try:
            value = float(s)
        except (TypeError, ValueError, OverflowError):
            fail("invalid_confidence")
        if not math.isfinite(value):
            fail("invalid_confidence")
        return value

    def bad_text(s):
        for ch in s:
            if ch == "\x00":
                return True
            cat = unicodedata.category(ch)
            if cat == "Cc" or cat == "Cs":
                return True
        return False

    prev_key = None
    seen_keys = set()
    parents = set()
    first = True
    saw_page = False
    line_parts = None
    last_word_line_key = None
    alnum_count = 0
    text_parts = []
    text_len = 0
    words = []
    weighted_conf_sum = 0.0
    weighted_char_count = 0
    bins = [0, 0, 0, 0]

    def flush_line():
        nonlocal line_parts, text_len
        if line_parts is None:
            return
        if line_parts:
            line_text = " ".join(line_parts)
            if text_parts:
                text_parts.append("\n")
                text_len += 1
                if text_len > max_text_chars:
                    fail("text_too_large")
            text_parts.append(line_text)
            text_len += len(line_text)
            if text_len > max_text_chars:
                fail("text_too_large")
        line_parts = None

    for line in lines:
        if line == "":
            fail("blank_row")
        fields = line.split("\t")
        if len(fields) != 12:
            fail("wrong_field_count")

        level_s, page_s, block_s, par_s, line_s, word_s, left_s, top_s, width_s, height_s, conf_s, text_value = fields

        level = parse_uint(level_s)
        page_num = parse_uint(page_s)
        block_num = parse_uint(block_s)
        par_num = parse_uint(par_s)
        line_num = parse_uint(line_s)
        word_num = parse_uint(word_s)
        left = parse_uint(left_s)
        top = parse_uint(top_s)
        box_width = parse_uint(width_s)
        box_height = parse_uint(height_s)
        conf = parse_confidence(conf_s)

        if bad_text(text_value):
            fail("invalid_text")

        if level < 1 or level > 5:
            fail("invalid_level")
        if page_num != 1:
            fail("invalid_page")

        if level == 1:
            if block_num != 0 or par_num != 0 or line_num != 0 or word_num != 0:
                fail("invalid_hierarchy")
        elif level == 2:
            if block_num <= 0 or par_num != 0 or line_num != 0 or word_num != 0:
                fail("invalid_hierarchy")
        elif level == 3:
            if block_num <= 0 or par_num <= 0 or line_num != 0 or word_num != 0:
                fail("invalid_hierarchy")
        elif level == 4:
            if block_num <= 0 or par_num <= 0 or line_num <= 0 or word_num != 0:
                fail("invalid_hierarchy")
        else:
            if block_num <= 0 or par_num <= 0 or line_num <= 0 or word_num <= 0:
                fail("invalid_hierarchy")

        if level <= 4:
            if conf_s != "-1" or text_value != "":
                fail("invalid_structure")
        else:
            if conf < 0 or conf > 100:
                fail("invalid_confidence")
            if text_value == "":
                # Empty word text is representable, but it contributes no
                # character weight and cannot be counted as a word.
                pass

        if left < 0 or top < 0 or box_width < 0 or box_height < 0:
            fail("invalid_bbox")
        if left + box_width > width or top + box_height > height:
            fail("invalid_bbox")

        key = (page_num, block_num, par_num, line_num, word_num)
        if prev_key is not None and key < prev_key:
            fail("order_violation")
        if key in seen_keys:
            fail("duplicate_identity")
        if level > 1:
            parent = (*key[:level-1], *([0] * (6-level)))
            if parent not in parents:
                fail("missing_parent_row")
        seen_keys.add(key)
        if level < 5:
            parents.add(key)
        prev_key = key

        if first:
            if level != 1:
                fail("invalid_first_row")
            if left != 0 or top != 0 or box_width != width or box_height != height:
                fail("invalid_first_row")
            first = False
        saw_page = True

        if level == 5:
            if block_num <= 0 or par_num <= 0 or line_num <= 0 or word_num <= 0:
                fail("invalid_hierarchy")
            word_line_key = (block_num, par_num, line_num)
            if last_word_line_key != word_line_key:
                flush_line()
                line_parts = []
                last_word_line_key = word_line_key
            elif line_parts is None:
                line_parts = []

            word_text = text_value
            if word_text:
                line_parts.append(word_text)
            if word_text:
                char_count = len(word_text)
                weighted_conf_sum += conf * char_count
                weighted_char_count += char_count
                for ch in word_text:
                    if ch.isalnum():
                        alnum_count += 1
                if conf < 25:
                    bins[0] += char_count
                elif conf < 50:
                    bins[1] += char_count
                elif conf < 75:
                    bins[2] += char_count
                else:
                    bins[3] += char_count

            words.append({
                "page": page_num,
                "block": block_num,
                "paragraph": par_num,
                "line": line_num,
                "word": word_num,
                "left": left,
                "top": top,
                "width": box_width,
                "height": box_height,
                "confidence": conf,
                "text": word_text,
            })
        else:
            if line_parts is not None:
                flush_line()
                last_word_line_key = None

    flush_line()

    if not saw_page:
        fail("missing_rows")

    text_value = "".join(text_parts)
    if len(text_value) > max_text_chars:
        fail("text_too_large")
    try:
        if len(text_value.encode("utf-8")) > 400000:
            fail("text_too_large")
    except UnicodeEncodeError:
        fail("invalid_text")

    confidence_mean = None
    if weighted_char_count:
        confidence_mean = weighted_conf_sum / weighted_char_count
        if not math.isfinite(confidence_mean):
            fail("invalid_confidence")

    pixels = width * height
    density = (alnum_count * 1000000.0) / pixels

    return {
        "text": text_value,
        "words": tuple(words),
        "confidence_mean": confidence_mean,
        "confidence_bins": tuple(bins),
        "alnum_characters": alnum_count,
        "density": density,
        "order_uncertain": True,
        "status": "empty" if text_value == "" else "review_required",
    }


@dataclass(frozen=True)
class PdfInspection:
    """Trusted caller's passive-PDF inspection, bound to exactly these FD bytes."""
    source: SourceIdentity
    page_count: int
    inspection_sha256: str
    policy: str = 'validated_passive_pdf_v1'

    def __post_init__(self):
        _require(isinstance(self.source, SourceIdentity) and type(self.page_count) is int
                 and 0 < self.page_count <= 200 and self.policy == 'validated_passive_pdf_v1',
                 'passive_pdf_inspection_required')
        _require(type(self.inspection_sha256) is str and len(self.inspection_sha256) == 64
                 and all(c in '0123456789abcdef' for c in self.inspection_sha256),
                 'passive_pdf_inspection_required')


class OcrRuntime:
    """One preparation budget, no scheduler/hold acquisition and no global worker.

    A stop-unconfirmed failure latches this instance and preserves its stage.
    Only exit/reap + EOF for both pipes + absence of its process group can prove
    stop. External hold/checkpoint cleanup must inspect that exact receipt.
    """
    def __init__(self, profile):
        _require(isinstance(profile, RuntimeProfile), 'explicit_profile_required')
        self.profile, self.limits = profile, profile.limits
        self.started, self.cpu_seconds, self.children, self.characters = time.monotonic(), 0., 0, 0
        self.parent_cpu_seconds, self._active_parent_cpu = 0., None
        self.pages, self.total_pixels = {}, 0
        self._lock, self._blocked = threading.Lock(), False
        self._raster_result = None

    def _remaining_budget(self):
        active = (time.thread_time()-self._active_parent_cpu
                  if self._active_parent_cpu is not None else 0.)
        return (self.limits.whole_wall_seconds-(time.monotonic()-self.started),
                self.limits.whole_cpu_seconds-self.cpu_seconds-self.parent_cpu_seconds-active)

    def _check_budget(self,for_child=False):
        wall,cpu = self._remaining_budget()
        _require(wall > 0 and cpu >= (1 if for_child else 0)
                 and (not for_child or self.children < self.limits.children),
                 'preparation_budget_exhausted')
        return wall,cpu

    def _finish_parent_cpu(self):
        self.parent_cpu_seconds += time.thread_time()-self._active_parent_cpu
        self._active_parent_cpu = None

    def _admit_page(self, page_id, operation, dimensions=None):
        _require(type(page_id) is str and 0 < len(page_id) <= 128
                 and all(ord(c) >= 32 for c in page_id), 'page_identity_required')
        if page_id not in self.pages:
            _require(len(self.pages) < self.limits.pages, 'page_count_limit')
            self.pages[page_id] = {'dimensions':None,'operations':set()}
        page = self.pages[page_id]
        _require(operation not in page['operations'], 'duplicate_page_operation')
        page['operations'].add(operation)
        if dimensions is not None:
            self._record_dimensions(page_id, dimensions)

    def _record_dimensions(self, page_id, dimensions):
        width, height = dimensions
        _dimensions(width,height,self.limits)
        prior = self.pages[page_id]['dimensions']
        if prior is not None:
            _require(prior == dimensions, 'page_dimensions_changed')
        else:
            _require(self.total_pixels+width*height <= self.limits.total_pixels,
                     'aggregate_pixel_limit')
            self.total_pixels += width*height
            self.pages[page_id]['dimensions'] = dimensions

    def _execute(self, source_fd, identity, request, cancel):
        _require(isinstance(cancel, threading.Event), 'cancellation_event_required')
        _require(not self._blocked, 'previous_stop_unconfirmed')
        self._check_budget(for_child=True)
        _require(not cancel.is_set(), 'cancelled')
        self.profile.verify()
        stage = Path(tempfile.mkdtemp(prefix='novellibrary-ocr-'))
        output = control = None
        confirmed = True
        receipt = None
        try:
            maximum = self.limits.source_bytes if request['operation'] == 'pdf' else self.limits.png_bytes
            retained_bytes = (self._raster_result.raster.source.byte_size
                              if self._raster_result is not None and self._raster_result.fd >= 0 else 0)
            _require(identity.byte_size+self.limits.png_bytes+512*1024+retained_bytes
                     <= self.limits.stage_bytes,
                     'owned_stage_limit')
            # The child copies/hashes under its CPU/wall owner. No source bytes
            # are read by the parent; only the protected readonly FD is passed.
            _source_stat(source_fd,identity,maximum)
            output = os.open(stage/'output.bin', os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_CLOEXEC,0o600)
            payload = dict(request, profile=asdict(self.profile), source=asdict(identity),
                           stage=str(stage))
            remaining_cpu = self._check_budget(for_child=True)[1]
            payload['cpu_seconds'] = min(self.limits.child_cpu_seconds, math.floor(remaining_cpu))
            encoded = _json(payload)
            _require(len(encoded) <= 384*1024, 'request_size_limit')
            (stage/'request.json').write_bytes(encoded)
            control = os.open(stage/'request.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
            # -B prevents writes, but existing pyc files would still be read.
            # A fresh private prefix also excludes unpinned cached bytecode.
            argv = [self.profile.interpreter.path,'-I','-B','-X',
                    'pycache_prefix='+str(stage/'bytecode'),self.profile.helper.path,
                    '--owned-child',str(source_fd),str(output),str(control)]
            remaining_wall,remaining_cpu = self._check_budget(for_child=True)
            _require(not cancel.is_set(),'cancelled')
            # Serialize once. If preparation crosses an integer CPU boundary,
            # reject before launch rather than admit a stale allowance or retry.
            _require(payload['cpu_seconds'] <= math.floor(remaining_cpu),
                     'preparation_budget_exhausted')
            self.children += 1
            try:
                data, receipt = _run_owned(argv,(source_fd,output,control),cancel,self.limits,
                                          min(remaining_wall,self.limits.child_wall_seconds))
            except OcrRuntimeError as exc:
                receipt = exc.receipt
                raise
            finally:
                if receipt is not None:
                    self.cpu_seconds += receipt.cpu_seconds
                    confirmed = receipt.stop_confirmed
                    self._blocked |= not confirmed
            # No output is accepted after interpreter/helper/library drift or cancellation.
            self.profile.verify()
            _require(not cancel.is_set(), 'cancelled')
            self._check_budget()
            _require(receipt.cpu_seconds <= payload['cpu_seconds'], 'child_cpu_limit')
            os.close(output)
            output = None
            result_fd = os.open(stage/'output.bin',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
            return data, result_fd, self._receipt(receipt,payload['cpu_seconds'])
        except BaseException as exc:
            if not confirmed:
                if isinstance(exc,OcrRuntimeError):
                    exc.retained_stage = str(stage)
                else:
                    raise OcrRuntimeError('stop_unconfirmed',receipt=receipt,
                                          retained_stage=str(stage)) from exc
            if isinstance(exc,OcrRuntimeError) and exc.receipt is None:
                exc.receipt = receipt
            raise
        finally:
            for descriptor in (output,control):
                if descriptor is not None:
                    os.close(descriptor)
            if confirmed:
                shutil.rmtree(stage)

    def _receipt(self, child, cpu_limit):
        return {'schema':1,'profile_sha256':self.profile.digest,'child':asdict(child),
                'resources':{'cpu_limit_seconds':cpu_limit,
                             'cpu_used_seconds':child.cpu_seconds,
                             'hard_address_space':self.profile.memory_policy == 'linux_rlimit_as',
                             'memory_policy':self.profile.memory_policy,
                             'memory_qualification':'NOT_RUN',
                             'dynamic_library_closure':'NOT_VERIFIED'},
                'quality_qualification':'NOT_RUN','visual_accuracy':'NOT_CERTIFIED',
                'reading_order':'UNVERIFIED','complete_selected_option':False}

    def ocr_image(self, source_fd, raster, *, page_id, cancel):
        _require(isinstance(raster,RasterIdentity), 'verified_full_page_required')
        _require(self._lock.acquire(blocking=False), 'concurrent_child_forbidden')
        receipt = None
        self._active_parent_cpu = time.thread_time()
        try:
            self._admit_page(page_id,'ocr',(raster.width,raster.height))
            remaining = self.limits.whole_characters-self.characters
            _require(remaining > 0, 'whole_text_limit')
            data, output, receipt = self._execute(source_fd,raster.source,
                dict(operation='ocr',raster=asdict(raster)),cancel)
            try:
                metadata = _strict_json_fd(output,8192)
            finally:
                os.close(output)
            _require(type(metadata) is dict and set(metadata) == {'width','height','ink_fraction'}
                     and metadata['width'] == raster.width and metadata['height'] == raster.height
                     and type(metadata['ink_fraction']) in (int,float)
                     and math.isfinite(metadata['ink_fraction'])
                     and 0 <= metadata['ink_fraction'] <= 1, 'invalid_raster_receipt')
            parsed = parse_tsv(data,raster.width,raster.height,
                               max_text_chars=min(self.limits.page_characters,remaining))
            self._check_budget()
            self.characters += len(parsed['text'])
            receipt.update(page_id=page_id,raster=asdict(raster),ink_fraction=metadata['ink_fraction'],
                           text_sha256=_sha(parsed['text'].encode()),characters=len(parsed['text']),
                           confidence_mean=parsed['confidence_mean'],
                           confidence_bins=parsed['confidence_bins'],density=parsed['density'],
                           order_uncertain=True,status=parsed['status'],
                           blank_hallucination_check='NOT_QUALIFIED')
            receipt['resources']['preparation_cpu_used_seconds'] = (
                self.limits.whole_cpu_seconds-self._remaining_budget()[1])
            _require(len(_json(receipt)) <= 16384, 'receipt_size_limit')
            return OcrPageResult(parsed['text'],parsed['words'],receipt)
        except OcrRuntimeError as exc:
            if exc.receipt is None and receipt is not None:
                exc.receipt = ChildReceipt(**receipt['child'])
            raise
        finally:
            self._finish_parent_cpu()
            self._lock.release()

    def render_pdf_page(self, source_fd, inspection, page_index, *, page_id, cancel):
        _require(isinstance(inspection,PdfInspection), 'passive_pdf_inspection_required')
        _require(type(page_index) is int and 0 <= page_index < inspection.page_count
                 and inspection.page_count <= self.limits.pages, 'pdf_page_limit')
        _require(self._lock.acquire(blocking=False), 'concurrent_child_forbidden')
        output = receipt = None
        self._active_parent_cpu = time.thread_time()
        try:
            _require(self._raster_result is None or self._raster_result.fd < 0,
                     'previous_raster_descriptor_open')
            self._admit_page(page_id,'pdf')
            data, output, receipt = self._execute(source_fd,inspection.source,
                dict(operation='pdf',page_index=page_index,page_count=inspection.page_count,
                     inspection_sha256=inspection.inspection_sha256,
                     remaining_pixels=self.limits.total_pixels-self.total_pixels),cancel)
            result = _strict_json(data,8192)
            _require(type(result) is dict and set(result) == {'width','height','rotation','cropbox'}
                     and type(result['rotation']) is int and result['rotation'] in {0,90,180,270}
                     and type(result['cropbox']) is list and len(result['cropbox']) == 4
                     and all(type(v) in (int,float) and math.isfinite(v) for v in result['cropbox']),
                     'invalid_pdf_raster_receipt')
            self._record_dimensions(page_id,(result['width'],result['height']))
            info = os.fstat(output)
            _require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= self.limits.png_bytes,
                     'png_byte_limit')
            digest, at = hashlib.sha256(), 0
            while piece := os.pread(output,min(1024**2,info.st_size-at),at):
                digest.update(piece)
                at += len(piece)
            _require(at == info.st_size, 'raster_changed')
            self._check_budget()
            raster = RasterIdentity(SourceIdentity(digest.hexdigest(),at),result['width'],
                                    result['height'],'pdfium_raster')
            receipt.update(page_id=page_id,source=asdict(inspection.source),page_index=page_index,
                           inspection_sha256=inspection.inspection_sha256,
                           raster=asdict(raster),rotation=result['rotation'],cropbox=result['cropbox'],
                           rotation_policy='pdfium_intrinsic_rotation_cropbox_default',dpi=200)
            returned = RenderedPage(output,raster,receipt)
            self._raster_result = returned
            output = None
            return returned
        except OcrRuntimeError as exc:
            if exc.receipt is None and receipt is not None:
                exc.receipt = ChildReceipt(**receipt['child'])
            raise
        finally:
            if output is not None:
                os.close(output)
            self._finish_parent_cpu()
            self._lock.release()


def _strict_json(data, maximum):
    _require(type(data) is bytes and len(data) <= maximum, 'protocol_size_limit')
    def pairs(items):
        value = {}
        for key,item in items:
            _require(key not in value, 'protocol_duplicate_key')
            value[key] = item
        return value
    def invalid(unused):
        raise OcrRuntimeError('protocol_nonfinite')
    try:
        return json.loads(data.decode('utf-8','strict'),object_pairs_hook=pairs,parse_constant=invalid)
    except (ValueError,UnicodeError,RecursionError) as exc:
        raise OcrRuntimeError('invalid_protocol') from exc


def _strict_json_fd(fd,maximum):
    info = os.fstat(fd)
    _require(stat.S_ISREG(info.st_mode) and 0 <= info.st_size <= maximum, 'protocol_size_limit')
    return _strict_json(os.pread(fd,maximum+1,0),maximum)


def _profile_from_wire(value):
    value = dict(value)
    for name in ('interpreter','helper','tesseract'):
        value[name] = ArtifactPin(**value[name])
    for name in ('traineddata','libraries'):
        value[name] = tuple(ArtifactPin(**v) for v in value[name])
    value['languages'] = tuple(value['languages'])
    value['versions'] = tuple(tuple(v) for v in value['versions'])
    value['limits'] = RuntimeLimits(**value['limits'])
    return RuntimeProfile(**value)


def _child_limits(profile,cpu_seconds):
    import resource
    _require(type(cpu_seconds) is int and 0 < cpu_seconds <= profile.limits.child_cpu_seconds,
             'invalid_cpu_limit')
    resource.setrlimit(resource.RLIMIT_CPU,(cpu_seconds,cpu_seconds))
    stage_file_limit = max(profile.limits.source_bytes,profile.limits.png_bytes)
    resource.setrlimit(resource.RLIMIT_FSIZE,(stage_file_limit,stage_file_limit))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    if profile.memory_policy == 'linux_rlimit_as':
        _require(sys.platform.startswith('linux'), 'memory_policy_host_mismatch')
        resource.setrlimit(resource.RLIMIT_AS,(profile.linux_address_space_bytes,)*2)
    else:
        # Actual Darwin AS setting is unqualified/unavailable. Never claim it.
        _require(sys.platform == 'darwin', 'memory_policy_host_mismatch')


def _child_verify_source(fd,identity,maximum):
    before = os.fstat(fd)
    _require(fcntl.fcntl(fd,fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY
             and stat.S_ISREG(before.st_mode) and before.st_size == identity.byte_size
             and 0 < before.st_size <= maximum, 'child_source_identity')
    digest,at = hashlib.sha256(),0
    while at < identity.byte_size:
        data = os.pread(fd,min(1024**2,identity.byte_size-at),at)
        _require(bool(data), 'child_source_changed')
        digest.update(data)
        at += len(data)
    _require(_identity(before) == _identity(os.fstat(fd)) and digest.hexdigest() == identity.sha256,
             'child_source_changed')


def _verify_native_modules(profile):
    """Every loaded package source/extension must be an explicitly pinned file.

    This checks Python module identities. OS/interpreter libraries loaded by the
    platform loader are a separate, unqualified closure; no sandbox is claimed.
    """
    pins = {p.resolved_path or p.path:p for p in profile.libraries}
    for name,module in tuple(sys.modules.items()):
        if name.split('.',1)[0] not in {'PIL','pypdfium2','pypdfium2_raw'}:
            continue
        path = getattr(module,'__file__',None)
        _require(type(path) is str and not path.endswith(('.pyc','.pyo')),
                 'native_module_identity')
        pin = pins.get(str(Path(path).resolve()))
        _require(pin is not None, 'unpinned_native_module')
        pin.verify()


def _child_ocr(fd,output,request,profile):
    from PIL import Image
    _verify_native_modules(profile)
    raster = request['raster']
    Image.MAX_IMAGE_PIXELS = profile.limits.page_pixels
    with os.fdopen(os.dup(fd),'rb') as source, Image.open(source) as image:
        _verify_native_modules(profile)
        _require(image.format == 'PNG' and getattr(image,'n_frames',1) == 1,
                 'single_full_page_png_required')
        width,height = image.size
        _dimensions(width,height,profile.limits)
        _require((width,height) == (raster['width'],raster['height'])
                 and image.getexif().get(274,1) == 1, 'raster_dimensions_or_orientation')
        image.load()
        with image.convert('RGB') as rgb:
            with rgb.convert('L') as gray:
                histogram = gray.histogram()
            ink = sum(histogram[:245])/(width*height)
            rgb.info.clear()
            normalized = Path(request['stage'])/'normalized.png'
            rgb.save(normalized,format='PNG')
    _require(0 < normalized.stat().st_size <= profile.limits.png_bytes, 'png_byte_limit')
    metadata = _json(dict(width=width,height=height,ink_fraction=ink))
    os.write(output,metadata)
    os.close(output)
    profile.verify()
    _verify_native_modules(profile)
    # Same PID/owned process group and inherited CPU/FSIZE limits, no shell/child.
    argv = [profile.tesseract.path,str(normalized),'stdout','--tessdata-dir',
            str(Path(profile.traineddata[0].path).parent),'-l','+'.join(profile.languages),
            '--psm',str(profile.psm),'--oem',str(profile.oem),'--dpi','200',
            '-c','tessedit_create_txt=0','-c','tessedit_create_tsv=1']
    os.execve(profile.tesseract.path,argv,{'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C',
                                         'OMP_THREAD_LIMIT':'1'})


def _child_pdf(fd,output,request,profile):
    import pypdfium2 as pdfium
    _verify_native_modules(profile)
    with os.fdopen(os.dup(fd),'rb') as source, pdfium.PdfDocument(source) as document:
        _require(len(document) == request['page_count'], 'pdf_page_count_changed')
        page = document[request['page_index']]
        try:
            width_points,height_points = page.get_size()
            _require(all(type(v) in (int,float) and math.isfinite(v) and v > 0
                         for v in (width_points,height_points)), 'pdf_dimensions')
            width,height = math.ceil(width_points*200/72),math.ceil(height_points*200/72)
            _dimensions(width,height,profile.limits)
            _require(width*height <= request['remaining_pixels'], 'aggregate_pixel_limit')
            rotation,cropbox = page.get_rotation(),list(page.get_cropbox())
            bitmap = page.render(scale=200/72,rotation=0)
            try:
                with bitmap.to_pil() as pil, pil.convert('RGB') as rgb:
                    _dimensions(*rgb.size,profile.limits)
                    _require(rgb.size == (width,height), 'pdf_raster_dimensions')
                    rgb.info.clear()
                    with os.fdopen(os.dup(output),'wb') as target:
                        rgb.save(target,format='PNG')
            finally:
                bitmap.close()
        finally:
            page.close()
    profile.verify()
    _verify_native_modules(profile)
    return dict(width=width,height=height,rotation=rotation,cropbox=cropbox)


def _child_main(source,output,control):
    request = _strict_json_fd(control,384*1024)
    os.close(control)
    profile = _profile_from_wire(request['profile'])
    _child_limits(profile,request['cpu_seconds'])
    profile.verify()
    _require(request['operation'] in {'ocr','pdf'}, 'invalid_child_operation')
    identity = SourceIdentity(**request['source'])
    maximum = profile.limits.source_bytes if request['operation']=='pdf' else profile.limits.png_bytes
    captured = _capture_source(source,identity,Path(request['stage'])/'source.bin',maximum)
    try:
        os.close(source)
        _child_verify_source(captured,identity,maximum)
        import resource
        resource.setrlimit(resource.RLIMIT_FSIZE,(profile.limits.png_bytes,)*2)
        if request['operation'] == 'ocr':
            _child_ocr(captured,output,request,profile)
        return _child_pdf(captured,output,request,profile)
    finally:
        os.close(captured)


if __name__ == '__main__':
    try:
        _require(len(sys.argv) == 5 and sys.argv[1] == '--owned-child', 'invalid_child_entrypoint')
        response = _child_main(*(int(v) for v in sys.argv[2:]))
        serialized = _json(response)
        _require(len(serialized) <= 8192, 'protocol_size_limit')
        os.write(1,serialized)
    except Exception:
        # No document text/path/traceback escapes on stderr or a malformed protocol.
        os._exit(70)
