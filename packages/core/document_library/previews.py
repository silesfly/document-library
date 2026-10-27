"""Bounded disposable preview bundles, separate from original content options.

Only generated entry names leave the parser. The child receives two descriptors,
never a storage path. A bundle is a derivative, not publication/read authority.
"""
from dataclasses import asdict
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
import unicodedata
import zipfile

from PIL import Image, ImageOps

from .ingest_formats import FormatError, Limits, _worker
from .library_contracts import canonical_json

BUNDLE_LIMIT = 128 * 1024**2
CHUNK_CHARS = 16384


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def preview_profile(limits):
    """One exact renderer/limits identity for generation, serving and repair."""
    if not isinstance(limits, Limits):
        raise ValueError('Explicit validated preview limits required')
    profile = dict(schema=2, renderer=2, limits=asdict(limits))
    return dict(profile, profile_sha256=_digest(canonical_json(profile).encode()))


class _BoundedOutput:
    def __init__(self, fd):
        self.fd = fd
        self.position = 0

    def write(self, data):
        if self.position + len(data) > BUNDLE_LIMIT:
            raise FormatError('preview_output_limit')
        remaining = memoryview(data)
        while remaining:
            count = os.write(self.fd, remaining)
            if count <= 0:
                raise OSError('preview output write failed')
            remaining = remaining[count:]
            self.position += count
        return len(data)

    def tell(self):
        return self.position

    def flush(self):
        pass


def _text_index(value):
    """Renderer v2 offsets refer only to these exact served UTF8 bytes."""
    from .ingest_formats import HEADING, parse_metadata
    chunks, char_start, byte_start, byte_offset = [], 0, 0, 0
    for position, character in enumerate(value):
        width = len(character.encode('utf-8'))
        if byte_offset - byte_start + width > 65536:
            chunks.append(dict(char_start=char_start, char_end=position,
                               byte_start=byte_start, byte_end=byte_offset))
            char_start, byte_start = position, byte_offset
        byte_offset += width
    if char_start < len(value):
        chunks.append(dict(char_start=char_start, char_end=len(value),
                           byte_start=byte_start, byte_end=byte_offset))
    metadata = parse_metadata('preview.txt', value)
    matches = list(HEADING.finditer(value)) if not metadata['review_reason'] else []
    targets = {match.start() for match in matches}
    targets.update(match.end() for match in matches)
    offsets, at = {}, 0
    for position, character in enumerate(value):
        if position in targets:
            offsets[position] = at
        at += len(character.encode('utf-8'))
    if len(value) in targets:
        offsets[len(value)] = at
    chapters = [dict(number=int(match.group(1)),
                     unit='volume' if match.group(2) == '권' else 'episode',
                     char_start=match.start(), char_end=match.end(),
                     byte_start=offsets[match.start()], byte_end=offsets[match.end()])
                for match in matches]
    return dict(schema=2, renderer=2, encoding='utf-8', characters=len(value),
                byte_size=byte_offset, chunks=chunks, chapters=chapters,
                chapter_review_reason=metadata['review_reason'])

def _render_bundle(fd, output_fd, request):
    import fcntl
    limits = Limits(**request['limits'])
    before_output = os.fstat(output_fd)
    if (not stat.S_ISREG(before_output.st_mode) or before_output.st_nlink != 1
            or before_output.st_size or os.lseek(output_fd, 0, os.SEEK_CUR)
            or fcntl.fcntl(output_fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_WRONLY):
        raise FormatError('empty_writeonly_output_required')
    source = os.fstat(fd)
    if (source.st_dev, source.st_ino) == (before_output.st_dev, before_output.st_ino):
        raise FormatError('distinct_preview_output_required')
    report = _worker(fd, dict(request, full_text=True))
    result = dict(preview_profile(limits), source_sha256=report['sha256'], source_byte_size=report['byte_size'],
                  kind='unavailable', text_chars=0, page_count=0)
    if report['status'] != 'validated':
        return result
    value = report.pop('full_text', '').replace('\r\n', '\n').replace('\r', '\n')
    image_kind = report['preview'] in {'image', 'imagepack'}
    if not value and not image_kind:
        return result
    # Re-read only bounded image sources and bind decoding to the inspected bytes.
    data = b''
    if image_kind:
        os.lseek(fd, 0, os.SEEK_SET)
        pieces, total = [], 0
        while chunk := os.read(fd, 1024**2):
            total += len(chunk)
            if total > limits.parse_bytes:
                raise FormatError('preview_source_limit')
            pieces.append(chunk)
        data = b''.join(pieces)
        if len(data) != report['byte_size'] or _digest(data) != report['sha256']:
            raise FormatError('preview_source_changed')
    entries = []
    result.update(kind='images' if image_kind else 'text', text_chars=len(value))
    with zipfile.ZipFile(_BoundedOutput(output_fd), 'w', compression=zipfile.ZIP_STORED) as bundle:
        def put(name, content):
            # Only caller-generated constants and numeric names reach this helper.
            bundle.writestr(name, content)
            entries.append(dict(name=name, byte_size=len(content), sha256=_digest(content)))
        if value:
            put('text.txt', value.encode('utf-8'))
            put('text-index.json', _json(_text_index(value)))
        if image_kind:
            def render(payload):
                with Image.open(io.BytesIO(payload)) as image:
                    for frame in range(getattr(image, 'n_frames', 1)):
                        if result['page_count'] >= limits.archive_frames:
                            raise FormatError('preview_page_limit')
                        image.seek(frame)
                        page = ImageOps.exif_transpose(image).convert('RGB')
                        page.info.clear()
                        result['page_count'] += 1
                        name = f"{result['page_count']:06d}.png"
                        output = io.BytesIO()
                        page.save(output, format='PNG')
                        put('pages/' + name, output.getvalue())
                        page.thumbnail((256, 256))
                        output = io.BytesIO()
                        page.save(output, format='PNG')
                        put('thumbnails/' + name, output.getvalue())
                        page.close()
            if report['preview'] == 'image':
                render(data)
            else:
                with zipfile.ZipFile(io.BytesIO(data)) as source_zip:
                    by_name = {unicodedata.normalize('NFKC', item.filename): item for item in source_zip.infolist()}
                    for page in report['pages']:
                        render(source_zip.read(by_name[page['name']]))
        put('manifest.json', _json(dict(result, entries=list(entries))))
    return result


def build_preview(root, lease, stage_path, filename, *, report=None, limits=Limits()):
    """Return a generation-owned derivative; caller must retain/recheck job authority."""
    if not stage_path.startswith(lease.staging_key + '/'):
        raise FormatError('preview_stage_generation')
    output_path = None
    try:
        with root.file(stage_path) as source, root.stage(lease) as (output_path, output):
            position = os.lseek(source, 0, os.SEEK_CUR)
            request = json.dumps(dict(filename=filename, limits=asdict(limits)))
            try:
                done = subprocess.run([sys.executable, '-B', '-m', __name__, str(source), str(output), request],
                                      pass_fds=(source, output), stdout=subprocess.PIPE,
                                      stderr=subprocess.DEVNULL, timeout=limits.wall_seconds)
            except subprocess.TimeoutExpired as error:
                raise FormatError('preview_timeout') from error
            finally:
                os.lseek(source, position, os.SEEK_SET)
            if done.returncode or len(done.stdout) > 16384:
                raise FormatError('preview_worker_failed')
            result = json.loads(done.stdout)
            if 'error' in result:
                raise FormatError(result['error'])
            if report is not None and (result['source_sha256'] != report['sha256']
                                       or result['source_byte_size'] != report['byte_size']):
                raise FormatError('preview_inspection_mismatch')
        if result['kind'] == 'unavailable':
            root.unlink(output_path)
            return result
        hasher, size = hashlib.sha256(), 0
        with root.file(output_path) as bundle_fd:
            while chunk := os.read(bundle_fd, 1024**2):
                size += len(chunk)
                if size > BUNDLE_LIMIT:
                    raise FormatError('preview_output_limit')
                hasher.update(chunk)
        result.update(bundle_relpath=output_path, bundle_sha256=hasher.hexdigest(), bundle_size=size)
        return result
    except BaseException:
        if output_path is not None:
            try:
                root.unlink(output_path)
            except FileNotFoundError:
                pass
        raise


if __name__ == '__main__':
    try:
        result = _render_bundle(int(sys.argv[1]), int(sys.argv[2]), json.loads(sys.argv[3]))
    except FormatError as error:
        result = {'error': str(error)}
    except Exception:
        result = {'error': 'invalid_preview'}
    print(json.dumps(result))
