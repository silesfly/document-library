"""Verified derivatives and fixed service repair, separate from original authority."""
from dataclasses import asdict, dataclass
import hashlib
import io
import json
import os
import re
import stat
import struct
import uuid
import zipfile

from sqlalchemy import text

from .auth import AuthError
from .cursors import CursorError, decode_cursor, encode_cursor
from .ingest_formats import Limits, HEADING
from .jobs import JobError, LostLease
from .library_contracts import AssetSnapshot, canonical_json
from .previews import BUNDLE_LIMIT, build_preview, preview_profile, _digest as _sha
from .publication import PublicationError, object_path
from .storage import StorageError, _parts
from .streaming import (DEFAULT_ADMISSION, OwnedProducer, OwnedResponse,
                        PreparedTransfer)

RENDERER = 2
CURSOR_KIND = 'preview.text.v2'
INDEX_LIMIT = 4 * 1024**2
ENTRY_LIMIT = 10003


class PreviewError(AuthError):
    pass


class PreviewNotReady(PreviewError):
    def __init__(self, *, state='unavailable', retry_after_seconds=None):
        super().__init__('PREVIEW_NOT_READY',409)
        self.state,self.retry_after_seconds = state,retry_after_seconds

    def public(self):
        value = dict(state=self.state)
        if self.retry_after_seconds is not None:
            value['retry_after_seconds'] = self.retry_after_seconds
        return value


class _Damaged(Exception):
    pass


class _Missing(PreviewNotReady):
    def __init__(self, expected, damaged_key, expected_head):
        super().__init__()
        self.expected,self.damaged_key,self.expected_head = expected,damaged_key,expected_head


def _decode(raw):
    def pairs(items):
        value = {}
        for key,item in items:
            if key in value:
                raise _Damaged('duplicate JSON key')
            value[key] = item
        return value
    try:
        return json.loads(raw,object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(_Damaged('invalid number')))
    except (ValueError,TypeError,UnicodeError,RecursionError) as error:
        raise _Damaged('invalid JSON') from error


def _integer(value, low, high):
    return type(value) is int and low <= value <= high


class _FDView(io.RawIOBase):
    """Borrow the thread-owned descriptor without another FD or shared offset."""
    def __init__(self, fd):
        self.fd,self.position,self.size = fd,0,os.fstat(fd).st_size

    def seekable(self):
        return True

    def readable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        self.position = offset if whence == 0 else self.position+offset if whence == 1 else self.size+offset
        if self.position < 0:
            raise _Damaged('invalid seek')
        return self.position

    def read(self, size=-1):
        size = self.size-self.position if size < 0 else size
        data = os.pread(self.fd,size,self.position)
        self.position += len(data)
        return data


@dataclass
class _VerifiedBundle:
    fd: int
    archive: object
    manifest: dict
    index: dict | None
    fingerprint: tuple

    def unchanged(self):
        from .publication import PublicationService
        if PublicationService._fingerprint(self.fd) != self.fingerprint:
            raise PreviewNotReady()

    def chunks(self, name, *, start=0, length=None):
        item = self.archive.getinfo(name)
        length = item.file_size-start if length is None else length
        if (type(start) is not int or type(length) is not int or start < 0
                or length < 0 or start+length > item.file_size):
            raise _Damaged('entry interval')
        # ZipFile checks local name, header consistency and overlapping entries.
        # ZIP_STORED bytes then use pread; ZipExtFile.seek can allocate/discard
        # a large prefix for every later logical text chunk.
        with self.archive.open(item):
            pass
        header = os.pread(self.fd,30,item.header_offset)
        if len(header) != 30 or header[:4] != b'PK\x03\x04':
            raise _Damaged('entry header')
        flags,compression = struct.unpack_from('<HH',header,6)
        if flags != item.flag_bits or compression != zipfile.ZIP_STORED:
            raise _Damaged('local entry metadata differs')
        name_size,extra_size = struct.unpack_from('<HH',header,26)
        position = item.header_offset+30+name_size+extra_size+start
        remaining = length
        while remaining:
            chunk = os.pread(self.fd,min(65536,remaining),position)
            if not chunk:
                raise _Damaged('truncated entry')
            position += len(chunk)
            remaining -= len(chunk)
            yield chunk


def _read_small(archive, name, limit):
    item = archive.getinfo(name)
    if item.file_size > limit:
        raise _Damaged('metadata limit')
    pieces = []
    with archive.open(item) as source:
        while chunk := source.read(65536):
            pieces.append(chunk)
    return b''.join(pieces)


def _verify_index(bundle):
    index = _decode(_read_small(bundle.archive,'text-index.json',INDEX_LIMIT))
    if (not isinstance(index,dict) or set(index) != {'schema','renderer','encoding','characters','byte_size','chunks','chapters','chapter_review_reason'}
            or index['schema'] != 2 or type(index['schema']) is not int
            or index['renderer'] != RENDERER or type(index['renderer']) is not int
            or index['encoding'] != 'utf-8' or not _integer(index['characters'],1,BUNDLE_LIMIT)
            or not _integer(index['byte_size'],1,BUNDLE_LIMIT)
            or index['byte_size'] != bundle.archive.getinfo('text.txt').file_size
            or not isinstance(index['chunks'],list) or not 1 <= len(index['chunks']) <= 4096
            or not isinstance(index['chapters'],list) or len(index['chapters']) > 5000
            or index['chapter_review_reason'] not in (None,'ambiguous_headings','headings_without_body')):
        raise _Damaged('invalid text index')
    byte_at,char_at = 0,0
    for part in index['chunks']:
        if (not isinstance(part,dict) or set(part) != {'char_start','char_end','byte_start','byte_end'}
                or any(type(v) is not int for v in part.values())
                or part['byte_start'] != byte_at or part['char_start'] != char_at
                or not byte_at < part['byte_end'] <= min(index['byte_size'],byte_at+65536)
                or not char_at < part['char_end'] <= index['characters']):
            raise _Damaged('noncontiguous UTF8 chunks')
        raw = b''.join(bundle.chunks('text.txt',start=byte_at,length=part['byte_end']-byte_at))
        try:
            if len(raw.decode('utf-8')) != part['char_end']-char_at:
                raise _Damaged('wrong character mapping')
        except UnicodeError as error:
            raise _Damaged('split UTF8 chunk') from error
        byte_at,char_at = part['byte_end'],part['char_end']
    if (byte_at,char_at) != (index['byte_size'],index['characters']):
        raise _Damaged('incomplete UTF8 index')
    previous = 0
    if index['chapter_review_reason'] and index['chapters']:
        raise _Damaged('ambiguous chapter jump')
    for chapter in index['chapters']:
        if (not isinstance(chapter,dict) or set(chapter) != {'number','unit','char_start','char_end','byte_start','byte_end'}
                or not _integer(chapter['number'],previous+1,10_000_000)
                or chapter['unit'] not in {'volume','episode'}
                or any(type(chapter[k]) is not int for k in ('char_start','char_end','byte_start','byte_end'))
                or not 0 <= chapter['byte_start'] < chapter['byte_end'] <= index['byte_size']
                or not 0 <= chapter['char_start'] < chapter['char_end'] <= index['characters']
                or chapter['byte_end']-chapter['byte_start'] > 65536):
            raise _Damaged('invalid chapter')
        raw = b''.join(bundle.chunks('text.txt',start=chapter['byte_start'],length=chapter['byte_end']-chapter['byte_start']))
        heading = HEADING.fullmatch(raw.decode('utf-8'))
        if (heading is None or int(heading.group(1)) != chapter['number']
                or len(raw.decode('utf-8')) != chapter['char_end']-chapter['char_start']
                or ('volume' if heading.group(2) == '권' else 'episode') != chapter['unit']):
            raise _Damaged('chapter bytes differ')
        # Map the chapter start through the exact verified chunk, not a client offset.
        part = next(p for p in index['chunks'] if p['byte_start'] <= chapter['byte_start'] < p['byte_end'])
        prefix = b''.join(bundle.chunks('text.txt',start=part['byte_start'],length=chapter['byte_start']-part['byte_start']))
        if part['char_start']+len(prefix.decode('utf-8')) != chapter['char_start']:
            raise _Damaged('chapter character offset differs')
        previous = chapter['number']
    return index


class PreviewService:
    def __init__(self, publisher, policy, *, limits=Limits(), admission=DEFAULT_ADMISSION):
        if publisher.engine is not policy.engine:
            raise ValueError('One preview authority engine required')
        self.publisher,self.policy,self.limits,self.admission = publisher,policy,limits,admission
        self.root,self.jobs,self.engine = publisher.root,publisher.jobs,publisher.engine
        preview_profile(limits)

    @property
    def profile(self):
        return preview_profile(self.limits)

    def _head_in(self, conn, asset, *, profile_sha256=None):
        profile_sha256 = self.profile['profile_sha256'] if profile_sha256 is None else profile_sha256
        row = conn.execute(text('''SELECT r.* FROM preview_cache_heads h JOIN preview_cache_receipts r ON r.id=h.receipt_id
            WHERE h.revision_id=:revision AND h.asset_position=:position AND h.renderer=2 AND h.profile_sha256=:profile'''),
            dict(revision=asset.revision_id,position=asset.asset_position,profile=profile_sha256)).mappings().first()
        return dict(row) if row else None

    def _candidates(self, asset):
        with self.engine.connect() as conn:
            head = self._head_in(conn,asset)
        if head is not None:
            # Heads are service evidence tied to this actual original, never the latest seal.
            if (head['object_sha'] != asset.object_sha or head['byte_size'] != asset.byte_size):
                raise _Damaged('head source differs')
            return [(head['manifest_json'],head['job_id'],head['generation'])],head['id']
        return [(p.manifest_json,p.source.job_id,p.source.generation) for p in asset.previews],None

    def _bundle(self, opened, asset, raw, job_id, generation, *, limits=None):
        if not isinstance(raw,str) or len(raw.encode()) > 16384:
            raise _Damaged('manifest limit')
        proof = _decode(raw)
        profile = preview_profile(self.limits if limits is None else limits)
        if (not isinstance(proof,dict) or proof.get('schema') != 2 or type(proof.get('schema')) is not int
                or proof.get('renderer') != RENDERER or type(proof.get('renderer')) is not int
                or proof.get('source_sha256') != asset.object_sha or type(proof.get('source_byte_size')) is not int
                or proof.get('source_byte_size') != asset.byte_size
                or proof.get('kind') not in {'text','images'} or not _integer(proof.get('bundle_size'),1,BUNDLE_LIMIT)
                or not isinstance(proof.get('bundle_sha256'),str) or not re.fullmatch('[0-9a-f]{64}',proof['bundle_sha256'])):
            raise _Damaged('unsupported or wrong source proof')
        if canonical_json({key:proof.get(key) for key in profile}) != canonical_json(profile):
            raise _Damaged('preview profile differs')
        parts = _parts(proof['bundle_relpath'])
        if (parts[:3] != ['jobs',job_id,f'generation-{generation}'] or len(parts) != 5
                or not re.fullmatch('[0-9a-f]{16}',parts[3]) or not re.fullmatch(r'[0-9a-f]{32}\.part',parts[4])):
            raise _Damaged('wrong generation path')
        fd = opened.enter_context(self.root.file(proof['bundle_relpath']))
        fingerprint = self.publisher._hash_fd(fd,dict(sha256=proof['bundle_sha256'],byte_size=proof['bundle_size']))
        # Bound central directory before ZipFile allocates it. Renderer output is
        # <=128MiB, single-disk, comment-free and needs no ZIP64 directory.
        size = os.fstat(fd).st_size
        tail = os.pread(fd,22,max(0,size-22))
        if len(tail) != 22 or tail[:4] != b'PK\x05\x06':
            raise _Damaged('zip trailer')
        _,disk,central_disk,on_disk,count,central_size,central_at,comment = struct.unpack('<4s4H2LH',tail)
        if (disk or central_disk or on_disk != count or not 1 <= count <= ENTRY_LIMIT
                or central_size > INDEX_LIMIT or central_at+central_size != size-22 or comment):
            raise _Damaged('zip directory limit')
        archive = opened.enter_context(zipfile.ZipFile(_FDView(fd)))
        infos = archive.infolist()
        names = [item.filename for item in infos]
        if len(infos) != count or len(set(names)) != count or 'manifest.json' not in names:
            raise _Damaged('duplicate or absent entry')
        total = 0
        for item in infos:
            if (not re.fullmatch(r'(?:text\.txt|text-index\.json|manifest\.json|(?:pages|thumbnails)/[0-9]{6}\.png)',item.filename)
                    or item.compress_type != zipfile.ZIP_STORED or item.flag_bits & ~(0x8|0x800)
                    or item.compress_size != item.file_size or item.file_size > BUNDLE_LIMIT
                    or item.is_dir() or stat.S_IFMT(item.external_attr >> 16) not in (0,stat.S_IFREG)):
                raise _Damaged('unsupported archive entry')
            total += item.file_size
        if total > BUNDLE_LIMIT:
            raise _Damaged('expanded bundle limit')
        embedded = _decode(_read_small(archive,'manifest.json',INDEX_LIMIT))
        base = {k:v for k,v in proof.items() if not k.startswith('bundle_')}
        if (not isinstance(embedded,dict)
                or canonical_json({k:v for k,v in embedded.items() if k != 'entries'}) != canonical_json(base)):
            raise _Damaged('inner source binding differs')
        entries = embedded.get('entries')
        if not isinstance(entries,list) or len(entries) != count-1:
            raise _Damaged('entry manifest differs')
        expected_names = set()
        bundle = _VerifiedBundle(fd,archive,proof,None,fingerprint)
        for entry in entries:
            if (not isinstance(entry,dict) or set(entry) != {'name','byte_size','sha256'}
                    or entry['name'] == 'manifest.json' or entry['name'] in expected_names
                    or entry['name'] not in names or type(entry['byte_size']) is not int
                    or archive.getinfo(entry['name']).file_size != entry['byte_size']):
                raise _Damaged('entry binding differs')
            hasher = hashlib.sha256()
            for chunk in bundle.chunks(entry['name']):
                hasher.update(chunk)
            if hasher.hexdigest() != entry['sha256']:
                raise _Damaged('entry SHA differs')
            expected_names.add(entry['name'])
        if expected_names != set(names)-{'manifest.json'}:
            raise _Damaged('extra archive entry')
        if proof['kind'] == 'text':
            if set(names) != {'text.txt','text-index.json','manifest.json'} or proof.get('page_count') != 0:
                raise _Damaged('text entries differ')
            bundle.index = _verify_index(bundle)
            if proof.get('text_chars') != bundle.index['characters']:
                raise _Damaged('text character count differs')
        else:
            pages = proof.get('page_count')
            if not _integer(pages,1,5000) or proof.get('text_chars') != 0:
                raise _Damaged('page count differs')
            if set(names) != {'manifest.json'} | {f'{prefix}/{i:06d}.png' for prefix in ('pages','thumbnails') for i in range(1,pages+1)}:
                raise _Damaged('page order differs')
        bundle.unchanged()
        return bundle

    def response(self, resolve_fresh, *, kind='text', cursor=None, chapter=None,
                 page=None, thumbnail=False, method='GET', on_created=None):
        if kind not in {'text','chapters','image'} or method not in {'GET','HEAD'}:
            raise PreviewError('INVALID_PREVIEW',400)
        expected = resolve_fresh()
        if len(expected.assets) != 1:
            raise PreviewError('SINGLE_ORIGINAL_REQUIRED',400)
        asset = expected.assets[0]
        def prepare(opened):
            candidates,head = self._candidates(asset)
            damaged = _sha(canonical_json(dict(head=head,candidates=candidates,renderer=RENDERER)).encode())
            bundle = None
            # Each candidate is opened on a separate nested ExitStack? The fixed
            # asset source list is normally one; bound fan-out protects FD budget.
            if len(candidates) > 1:
                candidates = candidates[:1]
            for raw,job,generation in candidates:
                try:
                    bundle = self._bundle(opened,asset,raw,job,generation)
                except (OSError,ValueError,KeyError,TypeError,StorageError,zipfile.BadZipFile,_Damaged):
                    bundle = None
            if bundle is None:
                raise _Missing(expected,damaged,head)
            headers = [(b'x-content-type-options',b'nosniff'),(b'cache-control',b'private, no-store')]
            if kind == 'chapters':
                if bundle.index is None:
                    raise PreviewError('PREVIEW_KIND_MISMATCH',400)
                payload = canonical_json(dict(chapters=bundle.index['chapters'],review_reason=bundle.index['chapter_review_reason'])).encode()
                headers += [(b'content-type',b'application/json'),(b'content-length',str(len(payload)).encode())]
                return PreparedTransfer(200,tuple(headers),('bytes',payload),bundle.unchanged)
            if kind == 'image':
                if bundle.manifest['kind'] != 'images' or not _integer(page,1,bundle.manifest['page_count']):
                    raise PreviewError('INVALID_PREVIEW_PAGE',400)
                if cursor is not None or chapter is not None:
                    raise CursorError()
                name = f'{"thumbnails" if thumbnail else "pages"}/{page:06d}.png'
                size = bundle.archive.getinfo(name).file_size
                headers += [(b'content-type',b'image/png'),(b'content-length',str(size).encode()),
                            (b'x-preview-page-count',str(bundle.manifest['page_count']).encode('ascii')),
                            (b'x-preview-page',str(page).encode('ascii'))]
                return PreparedTransfer(200,tuple(headers),('entry',bundle,name,0,size),bundle.unchanged)
            if bundle.index is None:
                raise PreviewError('PREVIEW_KIND_MISMATCH',400)
            chunks = bundle.index['chunks']
            binding = dict(revision_id=asset.revision_id,asset_position=asset.asset_position,
                           source_sha256=asset.object_sha,bundle_sha256=bundle.manifest['bundle_sha256'],renderer=RENDERER)
            ordinal = 0
            if cursor is not None:
                payload = decode_cursor(cursor,expected_kind=CURSOR_KIND)
                if (set(payload) != set(binding)|{'ordinal'}
                        or any(type(payload[k]) is not type(v) or payload[k] != v for k,v in binding.items())
                        or not _integer(payload['ordinal'],0,len(chunks)-1) or chapter is not None):
                    raise CursorError()
                ordinal = payload['ordinal']
            if chapter is not None:
                if not _integer(chapter,0,len(bundle.index['chapters'])-1):
                    raise PreviewError('INVALID_PREVIEW_CHAPTER',400)
                target = bundle.index['chapters'][chapter]['byte_start']
                ordinal = next(i for i,p in enumerate(chunks) if p['byte_start'] <= target < p['byte_end'])
            part = chunks[ordinal]
            headers += [(b'content-type',b'text/plain; charset=utf-8'),
                        (b'content-length',str(part['byte_end']-part['byte_start']).encode()),
                        (b'x-preview-chunk',str(ordinal).encode())]
            if ordinal+1 < len(chunks):
                headers.append((b'x-preview-next-cursor',encode_cursor(CURSOR_KIND,dict(binding,ordinal=ordinal+1)).encode()))
            return PreparedTransfer(200,tuple(headers),('entry',bundle,'text.txt',part['byte_start'],part['byte_end']-part['byte_start']),bundle.unchanged)
        def produce(opened, transfer):
            if method == 'HEAD':
                return
            value = transfer.payload
            if value[0] == 'bytes':
                for at in range(0,len(value[1]),65536):
                    yield value[1][at:at+65536]
            else:
                yield from value[1].chunks(value[2],start=value[3],length=value[4])
        async def preflight_error(error):
            if not isinstance(error,_Missing):
                return error
            import asyncio
            return await asyncio.to_thread(self.enqueue,resolve_fresh,error.expected,
                damaged_key=error.damaged_key,expected_head=error.expected_head)
        handle = OwnedProducer.start(open_selected=lambda:self.publisher.read_selected(resolve_fresh,expected=expected),
            prepare=prepare,produce=produce,unique_originals=1,cache_fds=1,admission=self.admission,on_created=on_created)
        return OwnedResponse(handle,on_preflight_error=preflight_error)

    def enqueue(self, resolve_fresh, expected, *, damaged_key, expected_head):
        """Called only after HTTP original/cache reader close and producer exit."""
        asset = expected.assets[0]
        with self.publisher.locks.mutation([asset.object_sha]):
            with self.jobs._writer() as (conn,now):
                current = resolve_fresh()
                if current != expected:
                    raise PreviewError('PREVIEW_SELECTION_CHANGED',409)
                head = self._head_in(conn,asset)
                if (head['id'] if head else None) != expected_head:
                    return PreviewNotReady()
                if head and head['kind'] == 'unavailable':
                    return PreviewNotReady()
                payload = dict(revision_id=asset.revision_id,asset_position=asset.asset_position,
                    object_sha=asset.object_sha,byte_size=asset.byte_size,file_format=asset.file_format,
                    asset_role=asset.asset_role,sources=[asdict(p) for p in asset.sources],renderer=RENDERER,
                    limits=asdict(self.limits),profile_sha256=self.profile['profile_sha256'],
                    damaged_key=damaged_key,expected_head=expected_head)
                request = canonical_json(payload)
                request_digest = _sha(request.encode())
                job = self.jobs.enqueue_in(conn,kind='preview.repair',idempotency_key='preview:'+request_digest,
                    payload=payload,work_id=asset.work_id,input_revision_id=asset.revision_id,object_sha=asset.object_sha)
                conn.execute(text('''INSERT INTO preview_repair_inputs VALUES(:job,:revision,:position,:sha,:size,:format,:role,
                    :sources,2,:limits,:profile,:damaged,:head,:actor,:digest) ON CONFLICT(job_id) DO NOTHING'''),
                    dict(job=job,revision=asset.revision_id,position=asset.asset_position,sha=asset.object_sha,
                        size=asset.byte_size,format=asset.file_format,role=asset.asset_role,sources=canonical_json(payload['sources']),
                        limits=canonical_json(payload['limits']),profile=payload['profile_sha256'],damaged=damaged_key,head=expected_head,
                        actor=current.principal.user_id,digest=request_digest))
                state = conn.execute(text('SELECT state FROM jobs WHERE id=:id'),dict(id=job)).scalar_one()
        return PreviewNotReady(state='repair_running' if state == 'running' else 'repair_queued' if state in {'queued','retry_wait'} else 'unavailable',
                               retry_after_seconds=1 if state in {'queued','running','retry_wait'} else None)

    def _input_in(self, conn, lease):
        row = conn.execute(text('''SELECT i.*,j.kind,j.payload_json,j.state,j.lease_generation,j.lease_owner,j.lease_until,
            j.input_revision_id AS job_revision,j.object_sha AS job_sha,j.work_id AS job_work
            FROM preview_repair_inputs i JOIN jobs j ON j.id=i.job_id WHERE i.job_id=:id'''),dict(id=lease.job_id)).mappings().first()
        if (not row or row['kind'] != 'preview.repair' or row['state'] != 'running'
                or row['lease_generation'] != lease.generation or row['lease_owner'] != lease.owner
                or row['lease_until'] <= self.jobs._sample()[1]):
            raise LostLease('Fixed preview repair lease required')
        if row['job_revision'] != row['revision_id'] or row['job_sha'] != row['object_sha']:
            raise JobError('Repair job original reference differs')
        payload = _decode(row['payload_json'])
        if _sha(canonical_json(payload).encode()) != row['request_digest']:
            raise JobError('Repair payload differs from admitted input')
        for field in ('revision_id','asset_position','object_sha','byte_size','file_format','asset_role','renderer','profile_sha256','damaged_key','expected_head'):
            if payload.get(field) != row[field] or type(payload.get(field)) is not type(row[field]):
                raise JobError('Repair binding differs')
        if payload.get('sources') != _decode(row['sources_json']) or payload.get('limits') != _decode(row['limits_json']):
            raise JobError('Repair source or limits differ')
        fixed_profile = preview_profile(Limits(**payload['limits']))
        if payload['renderer'] != fixed_profile['renderer'] or payload['profile_sha256'] != fixed_profile['profile_sha256']:
            raise JobError('Repair profile differs from fixed limits')
        payload['_job_work'] = row['job_work']
        return dict(row),payload

    def _asset_in(self, conn, binding):
        row = conn.execute(text('''SELECT a.*,o.byte_size,o.detected_mime,o.local_relpath,o.state,e.work_id,r.edition_id,
            r.is_active,s.retained,p.rights_status FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
            JOIN edition_revisions r ON r.id=a.edition_revision_id JOIN work_editions e ON e.id=r.edition_id
            JOIN revision_seals s ON s.revision_id=r.id JOIN publication_plans p
                ON p.job_id=s.source_job_id AND p.generation=s.source_generation
            WHERE a.edition_revision_id=:revision AND a.asset_position=:position'''),
            dict(revision=binding['revision_id'],position=binding['asset_position'])).mappings().first()
        if (not row or row['state'] != 'LIVE' or row['local_relpath'] != object_path(binding['object_sha'])
                or row['work_id'] != binding['_job_work']
                or not (row['is_active'] or row['retained']) or row['rights_status'] != 'allowed'
                or any(row[k] != binding[k] for k in ('object_sha','byte_size','file_format','asset_role'))):
            raise JobError('Repair original is no longer LIVE or permitted')
        sources,previews = self.policy._sources(conn,binding['revision_id'],row)
        if [asdict(p) for p in sources] != binding['sources']:
            raise JobError('Repair source proof changed')
        return AssetSnapshot(0,0,row['work_id'],row['edition_id'],binding['revision_id'],None,
            binding['asset_position'],row['object_sha'],row['byte_size'],row['detected_mime'],row['file_format'],row['asset_role'],sources,())

    def process(self, lease, *, preview_builder=build_preview):
        """Only a fixed DB repair input is admitted; no request session persists.

        Copy under the selected original reader, close it, then give the child
        only the generation-owned copy. A cancelled job is NOT an original pin;
        the live copy reader itself excludes GC until its physical close.
        """
        @dataclass(frozen=True)
        class RepairSelection:
            assets: tuple
            options: tuple = ()
        def resolve_service():
            with self.engine.connect() as conn:
                _,binding = self._input_in(conn,lease)
                return RepairSelection((self._asset_in(conn,binding),))
        with self.engine.connect() as conn:
            input_row,binding = self._input_in(conn,lease)
        fixed_limits = Limits(**binding['limits'])
        fixed_profile = preview_profile(fixed_limits)
        expected = resolve_service()
        source_path,bundle_path = None,None
        committed = False
        try:
            with self.publisher.read_selected(resolve_service,expected=expected) as opened:
                with self.root.stage(lease) as (source_path,output):
                    for chunk in opened.chunks(expected.assets[0]):
                        remaining = memoryview(chunk)
                        while remaining:
                            written = os.write(output,remaining)
                            if written <= 0:
                                raise OSError('repair copy write failed')
                            remaining = remaining[written:]
            # No original descriptor or shared lock crosses the child boundary.
            resolve_service()
            manifest = preview_builder(self.root,lease,source_path,'original.'+binding['file_format'],
                report=dict(sha256=binding['object_sha'],byte_size=binding['byte_size']),limits=fixed_limits)
            bundle_path = manifest.get('bundle_relpath')
            if (manifest.get('source_sha256') != binding['object_sha'] or manifest.get('source_byte_size') != binding['byte_size']
                    or manifest.get('renderer') != RENDERER or manifest.get('schema') != 2
                    or manifest.get('kind') not in {'text','images','unavailable'}):
                raise JobError('Repair renderer output differs')
            if canonical_json({key:manifest.get(key) for key in fixed_profile}) != canonical_json(fixed_profile):
                raise JobError('Repair output profile differs from fixed input')
            if manifest['kind'] != 'unavailable':
                # Validate every generated cache byte before durable evidence.
                with self.publisher.read_selected(resolve_service,expected=expected) as opened:
                    self._bundle(opened,expected.assets[0],canonical_json(manifest),lease.job_id,lease.generation,limits=fixed_limits)
            elif bundle_path is not None:
                raise JobError('Unavailable repair cannot claim a cache')
            receipt = str(uuid.uuid4())
            with self.publisher.locks.mutation([binding['object_sha']]):
                with self.jobs.completion(lease,result={'renderer':RENDERER,'kind':manifest['kind']}) as conn:
                    self._input_in(conn,lease)
                    self._asset_in(conn,binding)
                    head = self._head_in(conn,expected.assets[0],profile_sha256=binding['profile_sha256'])
                    if (head['id'] if head else None) != binding['expected_head']:
                        raise JobError('Repair head changed; stale completion denied')
                    conn.execute(text('''INSERT INTO preview_cache_receipts VALUES(:id,:job,:generation,:revision,:position,:sha,:size,
                        2,:profile,:kind,:reason,:manifest,:now)'''),dict(id=receipt,job=lease.job_id,generation=lease.generation,
                        revision=binding['revision_id'],position=binding['asset_position'],sha=binding['object_sha'],size=binding['byte_size'],
                        profile=binding['profile_sha256'],kind=manifest['kind'],reason='renderer_unavailable' if manifest['kind'] == 'unavailable' else None,
                        manifest=canonical_json(manifest),now=self.jobs._sample()[1]))
                    conn.execute(text('''INSERT INTO preview_cache_heads VALUES(:revision,:position,2,:profile,:receipt)
                        ON CONFLICT(revision_id,asset_position,renderer,profile_sha256) DO UPDATE SET receipt_id=excluded.receipt_id'''),
                        dict(revision=binding['revision_id'],position=binding['asset_position'],profile=binding['profile_sha256'],receipt=receipt))
            committed = True
            return manifest['kind']
        finally:
            # Never unlink admitted source/cache bytes or another generation.
            for path in (source_path,bundle_path if not committed else None):
                if path is not None:
                    if not path.startswith(lease.staging_key+'/'):
                        raise JobError('Repair cleanup path escaped its generation')
                    try:
                        self.root.unlink(path)
                    except FileNotFoundError:
                        pass
