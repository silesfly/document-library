"""Bounded, isolated ingestion inspection; never extraction or publication authority."""
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
import hashlib
import io
import json
import os
import posixpath
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import unicodedata
from urllib.parse import unquote, urlsplit
import warnings
import zipfile

from defusedxml import ElementTree
from PIL import Image
from pypdf import PdfReader, apply_configuration
from .catalog import title_match_key


class FormatError(ValueError):
    pass


@dataclass(frozen=True)
class Limits:
    file_bytes: int = 2 * 1024**3
    parse_bytes: int = 64 * 1024**2
    expanded_bytes: int = 256 * 1024**2
    member_bytes: int = 32 * 1024**2
    entries: int = 5000
    pixels: int = 40_000_000
    frames: int = 200
    archive_pixels: int = 400_000_000
    archive_frames: int = 5000
    ratio: int = 200
    text_chars: int = 2_000_000
    cpu_seconds: int = 30
    wall_seconds: int = 45

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in asdict(self).values()):
            raise ValueError('Positive integer limits required')
        if self.parse_bytes > self.file_bytes or self.member_bytes > self.expanded_bytes:
            raise ValueError('Inconsistent size limits')


TEXT = {'txt', 'md', 'markdown', 'csv'}
HTML = {'html', 'htm'}
IMAGES = {'jpg': 'JPEG', 'jpeg': 'JPEG', 'png': 'PNG', 'webp': 'WEBP',
          'gif': 'GIF', 'bmp': 'BMP', 'avif': 'AVIF', 'tif': 'TIFF', 'tiff': 'TIFF'}
ARCHIVES = {'zip', 'cbz', 'epub', 'docx', 'odt'}
OPTIONAL = {'rar', 'cbr', '7z', 'mobi', 'azw3', 'rtf'}
KNOWN = TEXT | HTML | set(IMAGES) | ARCHIVES | OPTIONAL | {'pdf', 'fb2'}
ACTIVE_TAGS = {'script', 'iframe', 'object', 'embed', 'form', 'foreignobject',
               'base', 'frame', 'frameset', 'applet', 'animate', 'set', 'animatemotion', 'animatetransform'}
COMPLETE = re.compile(r'[\[\(]?\s*(완결|完結|完本|連載中|연재중)\s*[\]\)]?$', re.I)
RANGE = re.compile(r'(?:제|第)?\s*(\d+)\s*(화|회|話|권)?\s*[~–-]\s*(?:제|第)?\s*(\d+)\s*(화|회|話|권)?$')
SINGLE = re.compile(r'(?:제|第)?\s*(\d+)\s*(화|회|話|권)$')
HEADING = re.compile(r'(?m)^[ \t]*(?:제|第)?[ \t]*(\d+)[ \t]*(화|회|話|章|권)(?:[ \t:.-]+[^\n]*)?[ \t]*$')


def parser_fingerprint():
    """Bind pending reconciliation plans to this parser source revision."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _display_name(filename):
    if (not isinstance(filename, str) or not filename or len(filename.encode()) > 1024
            or any(unicodedata.category(c).startswith('C') for c in filename)
            or '/' in filename or '\\' in filename):
        raise FormatError('invalid_filename')
    name = unicodedata.normalize('NFKC', filename)
    if '/' in name or '\\' in name or name in {'.', '..'}:
        raise FormatError('invalid_filename')
    return name


def _intervals(numbers):
    intervals = []
    for number in numbers:
        if intervals and number == intervals[-1][1] + 1:
            intervals[-1][1] = number
        else:
            intervals.append([number, number])
    gaps = [[left[1] + 1, right[0] - 1] for left, right in zip(intervals, intervals[1:])]
    return intervals, gaps


def parse_metadata(filename, text='', *, max_entries=5000):
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    name = _display_name(filename)
    title = PurePosixPath(name).stem.strip()
    marker = COMPLETE.search(title)
    completion = marker.group(1) if marker else None
    if marker:
        title = title[:marker.start()].rstrip()
    match = RANGE.search(title) or SINGLE.search(title)
    inferred = None
    if match:
        first = int(match.group(1))
        last = int(match.group(3)) if match.re is RANGE else first
        unit = 'volume' if '권' in match.group() else 'episode'
        units_agree = match.re is SINGLE or not match.group(2) or not match.group(4) or (match.group(2) == '권') == (match.group(4) == '권')
        if units_agree and first > 0 and first <= last <= 10_000_000:
            inferred = dict(unit=unit, intervals=[[first, last]], gaps=[], confidence='inferred')
            title = title[:match.start()].rstrip(' _-')
    unknown = dict(unit='unknown', intervals=[], gaps=[], confidence='unknown')
    coverage = inferred or unknown
    headings = list(HEADING.finditer(text))
    if len(headings) > max_entries:
        raise FormatError('episode_limit')
    episode_hashes, reason = {}, None
    if headings:
        numbers = [int(m.group(1)) for m in headings]
        units = {'volume' if m.group(2) == '권' else 'episode' for m in headings}
        if (len(units) != 1 or numbers[0] <= 0 or numbers[-1] > 10_000_000
                or any(a >= b for a, b in zip(numbers, numbers[1:]))):
            coverage, reason = unknown, 'ambiguous_headings'
        else:
            intervals, gaps = _intervals(numbers)
            coverage = dict(unit=units.pop(), intervals=intervals, gaps=gaps, confidence='verified')
            for index, match in enumerate(headings):
                end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
                start = 0 if index == 0 else match.start()
                body = text[start:end].replace('\r\n', '\n').strip()
                if not text[match.end():end].strip():
                    coverage, reason, episode_hashes = unknown, 'headings_without_body', {}
                    break
                episode_hashes[str(numbers[index])] = hashlib.sha256(body.encode()).hexdigest()
            if not reason and inferred and (inferred['unit'] != coverage['unit'] or inferred['intervals'] != intervals):
                reason = 'filename_content_disagree'
    return dict(title_display=title or name, title_match_key=title_match_key(title),
                coverage=coverage, episode_hashes=episode_hashes, review_reason=reason,
                publication_state='unknown', completion_marker=completion)


def _active_reference(value):
    value = unquote(value).strip()
    parsed = urlsplit(value)
    return (bool(parsed.scheme or parsed.netloc) or value.startswith(('/', '\\'))
            or any(ord(c) < 32 for c in value) or '\\' in value)


def _active_css(value):
    value = re.sub(r'/\*.*?\*/', '', value, flags=re.S)
    value = re.sub(r'\\(?:\r\n|\r|\n|\f)', '', value)
    def unescape(match):
        if match.group(1):
            number = int(match.group(1), 16)
            return chr(number) if 0 < number <= 0x10ffff else '\ufffd'
        return match.group(2)
    value = re.sub(r'\\([0-9a-fA-F]{1,6})[ \t\r\n\f]?|\\([^\r\n\f])', unescape, value)
    return bool(re.search(r'(?:url|image|image-set|-webkit-image-set|src|expression)\s*\(|@import', value, re.I))


def _attributes(tag, attrs):
    if tag.lower() in ACTIVE_TAGS:
        raise FormatError('active_document')
    for key, value in attrs:
        key, value = key.lower().split('}')[-1].split(':')[-1], value or ''
        if (key.startswith('on') or key in {'srcdoc', 'formaction', 'srcset', 'imagesrcset', 'http-equiv', 'ping'}
                or (key in {'href', 'src', 'action', 'data', 'target', 'poster', 'background',
                            'cite', 'longdesc', 'manifest', 'profile', 'usemap', 'dynsrc', 'lowsrc', 'base'}
                    and _active_reference(value))
                or (key == 'targetmode' and value.lower() == 'external')
                or _active_css(value)):
            raise FormatError('active_document')


class _PlainHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.in_style = False

    def handle_starttag(self, tag, attrs):
        _attributes(tag, attrs)
        if tag.lower() == 'style':
            self.in_style = True
        if tag.lower() in {'p', 'div', 'br', 'h1', 'h2', 'h3', 'li'}:
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag.lower() == 'style':
            self.in_style = False

    def handle_startendtag(self, tag, attrs):
        if tag.lower() == 'style':
            raise FormatError('ambiguous_style_element')
        super().handle_startendtag(tag, attrs)

    def handle_data(self, data):
        if self.in_style:
            if _active_css(data):
                raise FormatError('active_document')
            return
        self.parts.append(data)


def _decode(data):
    codecs = ['utf-8-sig', 'cp949']
    if data.startswith((b'\xff\xfe', b'\xfe\xff')):
        codecs = ['utf-16']
    for encoding in codecs:
        try:
            value = data.decode(encoding)
            if any(ord(c) < 32 and c not in '\t\r\n' for c in value):
                raise FormatError('binary_text')
            return value
        except UnicodeError:
            continue
    raise FormatError('unknown_text_encoding')


def _markup(data, suffix):
    if suffix in HTML:
        parser = _PlainHTML()
        parser.feed(_decode(data))
        parser.close()
        return ''.join(parser.parts)
    root = ElementTree.fromstring(data, forbid_dtd=True, forbid_entities=True, forbid_external=True)
    for element in root.iter():
        _attributes(element.tag.split('}')[-1], list(element.attrib.items()))
        if element.tag.split('}')[-1].lower() == 'style' and _active_css(''.join(element.itertext())):
            raise FormatError('active_document')
    parts = []
    def collect(element):
        if element.tag.split('}')[-1].lower() == 'style':
            return
        block = element.tag.split('}')[-1].lower() in {'p', 'div', 'br', 'h1', 'h2', 'h3', 'li', 'title', 'subtitle'}
        if block:
            parts.append('\n')
        parts.append(element.text or '')
        for child in element:
            collect(child)
            parts.append(child.tail or '')
        if block:
            parts.append('\n')
    collect(root)
    return ''.join(parts)


def _epub_text(members):
    """Use package spine order, not ZIP order or filename order."""
    def xml(name):
        if name not in members:
            raise FormatError('missing_epub_resource')
        return ElementTree.fromstring(members[name], forbid_dtd=True, forbid_entities=True, forbid_external=True)
    def local(base, href):
        if not href or _active_reference(href):
            raise FormatError('invalid_epub_reference')
        reference = urlsplit(unquote(href))
        if reference.query or reference.fragment:
            raise FormatError('invalid_epub_reference')
        name = posixpath.normpath(posixpath.join(posixpath.dirname(base), reference.path))
        if name.startswith('../') or name not in members:
            raise FormatError('missing_epub_resource')
        return name
    container = xml('META-INF/container.xml')
    roots = [n.get('full-path') for n in container.iter() if n.tag.split('}')[-1] == 'rootfile']
    if len(roots) != 1:
        raise FormatError('ambiguous_epub_package')
    package_name = local('', roots[0])
    package = xml(package_name)
    items, spine = {}, []
    for node in package.iter():
        tag = node.tag.split('}')[-1]
        if tag == 'item':
            ident = node.get('id')
            if not ident or ident in items:
                raise FormatError('ambiguous_epub_manifest')
            items[ident] = (local(package_name, node.get('href')), node.get('media-type'))
        elif tag == 'itemref':
            spine.append(node.get('idref'))
    if not spine or len(set(spine)) != len(spine):
        raise FormatError('ambiguous_epub_spine')
    chunks = []
    for ident in spine:
        if ident not in items or items[ident][1] != 'application/xhtml+xml':
            raise FormatError('unsupported_epub_spine')
        chunks.append(_markup(members[items[ident][0]], 'fb2'))
    return '\n'.join(chunks)


def _image(data, suffix, limits):
    with warnings.catch_warnings():
        warnings.simplefilter('error', Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(data)) as image:
            if image.format != IMAGES[suffix]:
                raise FormatError('image_extension_mismatch')
            frames = getattr(image, 'n_frames', 1)
            if frames > limits.frames:
                raise FormatError('image_frame_limit')
            dimensions, pixels = [], 0
            for index in range(frames):
                image.seek(index)
                width, height = image.size
                pixels += width * height
                if width <= 0 or height <= 0 or pixels > limits.pixels:
                    raise FormatError('image_pixel_limit')
                image.load()
                dimensions.append([width, height])
            return dict(dimensions=dimensions, frames=frames)


def _archive(data, suffix, limits):
    pages, chunks, spellings, expanded, members = [], [], {}, 0, {}
    total_pixels, total_frames = 0, 0
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        if len(entries) > limits.entries:
            raise FormatError('archive_entry_limit')
        for info in entries:
            original = info.orig_filename
            normalized = unicodedata.normalize('NFKC', original)
            name = normalized.rstrip('/') if info.is_dir() else normalized
            parts = PurePosixPath(name)
            kind = stat.S_IFMT(info.external_attr >> 16)
            if (not name or len(original.encode()) > 1024 or original.endswith('//')
                    or parts.is_absolute() or parts.as_posix() != name or '\\' in name
                    or any(p in {'.', '..'} or ':' in p for p in parts.parts)
                    or any(unicodedata.category(c).startswith('C') for c in original)
                    or kind not in {0, stat.S_IFREG, stat.S_IFDIR}
                    or (kind == stat.S_IFDIR and not info.is_dir())
                    or (kind == stat.S_IFREG and info.is_dir())
                    or info.flag_bits & 1):
                raise FormatError('unsafe_archive_member')
            for size in range(1, len(parts.parts) + 1):
                spelling = '/'.join(parts.parts[:size])
                key = spelling.casefold()
                if key in spellings and spellings[key] != spelling:
                    raise FormatError('archive_case_collision')
                spellings[key] = spelling
            key = name.casefold()
            marker = ('directory:' if info.is_dir() else 'file:') + key
            if marker in spellings or 'file:' + key in spellings or 'directory:' + key in spellings:
                raise FormatError('archive_duplicate_member')
            if any('file:' + '/'.join(parts.parts[:i]).casefold() in spellings for i in range(1, len(parts.parts))):
                raise FormatError('archive_prefix_collision')
            if not info.is_dir() and any(k.startswith('file:' + key + '/') or k.startswith('directory:' + key + '/') for k in spellings):
                raise FormatError('archive_prefix_collision')
            spellings[marker] = name
            if info.is_dir():
                continue
            expanded += info.file_size
            if (info.file_size > limits.member_bytes or expanded > limits.expanded_bytes
                    or info.file_size > max(1, info.compress_size) * limits.ratio
                    or info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}):
                raise FormatError('archive_size_or_compression_limit')
            with archive.open(info) as member:
                payload = member.read(limits.member_bytes + 1)
            if len(payload) != info.file_size or len(payload) > limits.member_bytes:
                raise FormatError('archive_size_mismatch')
            if suffix == 'epub':
                members[name] = payload
            ext = parts.suffix.lower().lstrip('.')
            if payload.startswith((b'PK\x03\x04', b'7z\xbc\xaf\x27\x1c', b'Rar!')):
                raise FormatError('nested_archive')
            if ext in IMAGES:
                details = _image(payload, ext, limits)
                total_pixels += sum(width * height for width, height in details['dimensions'])
                total_frames += details['frames']
                if total_pixels > limits.archive_pixels or total_frames > limits.archive_frames:
                    raise FormatError('archive_image_budget')
                pages.append(dict(name=name, sha256=hashlib.sha256(payload).hexdigest(), **details))
            elif suffix in {'epub', 'docx', 'odt'} and ext in {'xml', 'opf', 'ncx', 'xhtml', 'html', 'htm', 'rels'}:
                value = _markup(payload, 'fb2' if ext not in HTML else ext)
                if name in {'word/document.xml', 'content.xml'}:
                    chunks.append(value)
            elif suffix in {'epub', 'docx', 'odt'} and (name == 'mimetype' or ext in {'css', 'ttf', 'otf'}):
                if ext == 'css' and _active_css(_decode(payload)):
                    raise FormatError('external_stylesheet_reference')
            elif suffix in {'zip', 'cbz'} and ext == 'txt' and parts.name.casefold().startswith('readme'):
                _decode(payload)
            else:
                raise FormatError('unsupported_archive_member')
        if suffix == 'epub' and ('mimetype' not in archive.namelist() or archive.read('mimetype') != b'application/epub+zip'):
            raise FormatError('invalid_epub')
        if suffix == 'docx' and 'word/document.xml' not in archive.namelist():
            raise FormatError('invalid_docx')
        if suffix == 'odt' and ('mimetype' not in archive.namelist() or archive.read('mimetype') != b'application/vnd.oasis.opendocument.text'):
            raise FormatError('invalid_odt')
    pages.sort(key=lambda page: [(0, int(p)) if p.isdigit() else (1, p.casefold()) for p in re.split(r'(\d+)', page['name'])])
    text = _epub_text(members) if suffix == 'epub' else '\n'.join(chunks)
    if len(text) > limits.text_chars:
        raise FormatError('text_limit')
    return dict(pages=pages, text=text, imagepack=bool(pages) and suffix in {'zip', 'cbz'})


def _pdf(data, limits):
    if not data.startswith(b'%PDF-'):
        raise FormatError('pdf_magic')
    reader = PdfReader(io.BytesIO(data), strict=True)
    if reader.is_encrypted:
        raise FormatError('encrypted_pdf')
    forbidden = {'/JS', '/JavaScript', '/A', '/AA', '/OpenAction', '/Launch', '/EmbeddedFiles',
                 '/EF', '/FS', '/AF', '/FFilter', '/FDecodeParms',
                 '/RichMedia', '/XFA', '/SubmitForm', '/URI', '/AcroForm'}
    resource_maps = {'/Font', '/XObject', '/ExtGState', '/ColorSpace', '/Pattern', '/Shading', '/Properties'}
    pending, seen = [(reader.trailer, 'object')], set()
    while pending:
        node, context = pending.pop()
        ident = (id(node), context)
        if ident in seen:
            continue
        seen.add(ident)
        if len(seen) > limits.entries * 20:
            raise FormatError('pdf_object_limit')
        if hasattr(node, 'get_object'):
            resolved = node.get_object()
            if resolved is not node:
                pending.append((resolved, context))
                continue
        if isinstance(node, dict):
            if context == 'appearance_states':
                context = 'object' if hasattr(node, 'get_data') else 'names'
            # Font glyphs and resource names are arbitrary keys, not PDF actions.
            # Their values still undergo the complete object inspection.
            if context != 'names' and (forbidden.intersection(node)
                    or node.get('/Type') == '/Filespec' or node.get('/Subtype') == '/FileAttachment'
                    or ('/F' in node and not isinstance(node['/F'], int))
                    or (isinstance(node.get('/S'), str) and node['/S'] in
                        {'/GoToR', '/GoToE', '/Launch', '/JavaScript', '/SubmitForm', '/ImportData'})):
                raise FormatError('active_pdf')
            for key, value in node.items():
                child_context = 'object'
                if context != 'names':
                    if key in {'/Resources', '/DR'}:
                        child_context = 'resources'
                    elif ((context == 'resources' and key in resource_maps)
                          or key == '/Dests'
                          or (key == '/CharProcs' and node.get('/Subtype') == '/Type3')):
                        child_context = 'names'
                    elif key == '/AP':
                        child_context = 'appearances'
                    elif context == 'appearances' and key in {'/N', '/R', '/D'}:
                        child_context = 'appearance_states'
                pending.append((value, child_context))
        elif isinstance(node, (list, tuple)):
            pending.extend((value, 'object') for value in node)
    if len(reader.pages) > limits.entries:
        raise FormatError('pdf_page_limit')
    chunks, size = [], 0
    for page in reader.pages:
        content = page.get_contents()
        if content is not None and len(content.get_data()) > limits.member_bytes:
            raise FormatError('pdf_stream_limit')
        value = page.extract_text() or ''
        size += len(value)
        if size > limits.text_chars:
            raise FormatError('text_limit')
        chunks.append(value)
    return '\n'.join(chunks), len(reader.pages)


def _inspect_bytes(data, filename, limits, *, full_text=False):
    name = _display_name(filename)
    suffix = PurePosixPath(name).suffix.lower().lstrip('.')
    if suffix not in KNOWN or not data or len(data) > limits.file_bytes:
        raise FormatError('unsupported_or_oversize_input')
    report = dict(format=suffix, sha256=hashlib.sha256(data).hexdigest(), byte_size=len(data),
                  status='validated', preview='text', representation='text', pages=[], text='')
    if suffix in OPTIONAL or len(data) > limits.parse_bytes:
        report.update(status='review', preview='unavailable', representation='document', reason='decoder_or_parse_limit')
    elif suffix in TEXT:
        report['text'] = _decode(data)
    elif suffix in HTML | {'fb2'}:
        report['text'] = _markup(data, suffix)
    elif suffix in IMAGES:
        report.update(preview='image', representation='single_image', image=_image(data, suffix, limits))
    elif suffix in ARCHIVES:
        result = _archive(data, suffix, limits)
        report.update(text=result['text'], pages=result['pages'])
        if result['imagepack']:
            report.update(preview='imagepack', representation='imagepack')
        elif not result['text'].strip():
            report.update(status='review', preview='unavailable', representation='document', reason='no_readable_content')
    elif suffix == 'pdf':
        with apply_configuration(
            maximum_declared_stream_length=limits.member_bytes,
            array_based_stream_maximum_output_length=limits.member_bytes,
            zlib_maximum_output_length=limits.member_bytes,
            lzw_maximum_output_length=limits.member_bytes,
            run_length_maximum_output_length=limits.member_bytes,
            brotli_maximum_output_length=limits.member_bytes,
            jbig2_maximum_output_length=limits.member_bytes,
            stream_decoding_work_maximum_length=limits.expanded_bytes,
            page_tree_maximum_entries=limits.entries,
            outline_maximum_entries=limits.entries,
            array_maximum_element_count=limits.entries * 20,
            dictionary_maximum_element_count=limits.entries * 20,
            jbig2dec_binary=None,
        ):
            report['text'], report['page_count'] = _pdf(data, limits)
        report.update(preview='pdf', representation='document')
    if len(report['text']) > limits.text_chars:
        raise FormatError('text_limit')
    report['metadata'] = parse_metadata(name, report['text'], max_entries=limits.entries)
    value = report.pop('text')
    report['text_excerpt'] = value[:65536]
    if full_text:
        report['full_text'] = value
    return report


def inspect_bytes(data, filename, limits=Limits(), *, full_text=False):
    """In-worker byte inspection; use inspect_fd for untrusted production input."""
    try:
        return _inspect_bytes(data, filename, limits, full_text=full_text)
    except FormatError:
        raise
    except Exception as error:
        raise FormatError('invalid_content') from error


def inspect_fd(fd, filename, limits=Limits(), *, full_text=False):
    """Inspect a caller-owned descriptor; optionally include full parser text.

    The report dict otherwise stays unchanged and existing limits still apply.
    Full parser text does not prove source/read authority, OCR, whole-work
    completeness, or summary attestation. Review, missing, and empty results
    remain exactly as returned by the parser.
    """
    if type(full_text) is not bool:
        raise FormatError('invalid_full_text_flag')
    _display_name(filename)
    position = os.lseek(fd, 0, os.SEEK_CUR)
    timed_out = False
    result = None
    try:
        result = subprocess.run([sys.executable, '-B', '-m', __name__, '--worker', str(fd),
                                 json.dumps(dict(filename=filename, limits=asdict(limits),
                                                 full_text=full_text))],
                                pass_fds=(fd,), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                timeout=limits.wall_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
    finally:
        os.lseek(fd, position, os.SEEK_SET)
    if timed_out:
        raise FormatError('inspection_timeout')
    if result.returncode or len(result.stdout) > 2 * 1024**2:
        raise FormatError('inspection_worker_failed')
    decode_failed = False
    report = None
    try:
        report = json.loads(result.stdout)
    except (ValueError, RecursionError):
        decode_failed = True
    if decode_failed:
        raise FormatError('inspection_worker_failed')
    if type(report) is not dict:
        raise FormatError('inspection_worker_failed')
    if 'error' in report:
        error_code = report['error']
        if (type(error_code) is str and len(error_code) <= 64 and
                re.fullmatch(r'[a-z][a-z0-9]*(?:_[a-z0-9]+)*', error_code)):
            raise FormatError(error_code)
        raise FormatError('inspection_worker_failed')
    return report


def _worker(fd, request):
    import fcntl
    import resource
    limits = Limits(**request['limits'])
    resource.setrlimit(resource.RLIMIT_CPU, (limits.cpu_seconds, limits.cpu_seconds))
    if sys.platform.startswith('linux'):
        resource.setrlimit(resource.RLIMIT_AS, (2 * 1024**3, 2 * 1024**3))
    if fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
        raise FormatError('readonly_descriptor_required')
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or not 0 < before.st_size <= limits.file_bytes:
        raise FormatError('invalid_source')
    os.lseek(fd, 0, os.SEEK_SET)
    sha, captured, size = hashlib.sha256(), bytearray(), 0
    while chunk := os.read(fd, 1024**2):
        size += len(chunk)
        if size > limits.file_bytes:
            raise FormatError('source_grew')
        sha.update(chunk)
        if before.st_size <= limits.parse_bytes:
            captured.extend(chunk)
    after = os.fstat(fd)
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    if size != before.st_size or any(getattr(before, k) != getattr(after, k) for k in fields):
        raise FormatError('source_changed')
    if size > limits.parse_bytes:
        name = _display_name(request['filename'])
        if PurePosixPath(name).suffix.lower().lstrip('.') not in KNOWN:
            raise FormatError('unsupported_or_oversize_input')
        return dict(sha256=sha.hexdigest(), byte_size=size, status='review', preview='unavailable',
                    reason='parser_size_limit', metadata=parse_metadata(name))
    return inspect_bytes(bytes(captured), request['filename'], limits,
                         full_text=request.get('full_text', False))


if __name__ == '__main__':
    try:
        if len(sys.argv) != 4 or sys.argv[1] != '--worker':
            raise FormatError('invalid_worker_request')
        output = _worker(int(sys.argv[2]), json.loads(sys.argv[3]))
    except FormatError as error:
        output = {'error': str(error)}
    except Exception:
        output = {'error': 'invalid_content'}
    serialized = json.dumps(output, ensure_ascii=False)
    if len(serialized.encode()) > 2 * 1024**2:
        serialized = '{"error": "inspection_output_limit"}'
    print(serialized)
