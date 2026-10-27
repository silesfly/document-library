"""Current public covers through the existing protected reader and transfer owner."""
from dataclasses import dataclass
import json
import re
from urllib.parse import quote
import uuid

from sqlalchemy import text

from .auth import AuthError, digest
from .library_contracts import ReadPrincipal, canonical_json, _ident
from .library_projection import VALID_OPTIONS_V1
from .metadata_contracts import COVER_SCHEMA
from .publication import object_path
from .streaming import OwnedProducer, OwnedResponse, PreparedTransfer


@dataclass(frozen=True)
class CoverObjectSnapshot:
    object_sha: str
    byte_size: int
    detected_mime: str
    file_format: str


@dataclass(frozen=True)
class CoverSelection:
    principal: ReadPrincipal
    work_id: str
    head_version: int
    candidate_id: str
    rights_version: int
    control_version: int
    cache_until_ms: int | None
    plan_digest: str
    input_digest: str
    assets: tuple[CoverObjectSnapshot, ...]
    options: tuple = ()


def cover_url(work_id, version):
    return '/api/v1/items/'+quote(work_id,safe='')+'/cover?version='+str(version)


def _now(clock):
    value=clock()
    if type(value) is not int or not 0 <= value <= 9223372036854775807:
        raise AuthError('ASSET_UNAVAILABLE',409)
    return value


def _cover_asset(plan, row):
    """Return verified raster identity or None for an unavailable cover."""
    try:
        asset = plan["asset"]
        if type(asset) is not dict or set(asset) != {
            "sha256", "byte_size", "detected_mime", "file_format", "prepared_relpath"
        }:
            return None
        sha = asset["sha256"]
        size = asset["byte_size"]
        mime = asset["detected_mime"]
        fmt = asset["file_format"]
        relpath = asset["prepared_relpath"]
        if type(sha) is not str or re.fullmatch(r"[0-9a-f]{64}", sha) is None:
            return None
        if sha != row["object_sha"]:
            return None
        row_size = row["byte_size"]
        if type(size) is not int or type(row_size) is not int:
            return None
        if not (1 <= size <= 8 * 1024 ** 2) or size != row_size:
            return None
        if type(fmt) is not str or fmt not in {
            "jpg", "jpeg", "png", "webp", "gif", "bmp", "avif", "tif", "tiff"
        }:
            return None
        if fmt in ("jpg", "jpeg"):
            expected_mime = "image/jpeg"
        elif fmt in ("tif", "tiff"):
            expected_mime = "image/tiff"
        else:
            expected_mime = "image/" + fmt
        if type(mime) is not str or mime != expected_mime or mime != row["detected_mime"]:
            return None
        if row["local_relpath"] != object_path(sha) or row["object_state"] != "LIVE":
            return None
        if type(relpath) is not str or not relpath.startswith(row["prepared_prefix"]):
            return None
        if any(part in ("", ".", "..") for part in relpath.split("/")):
            return None
        return sha, size, mime, fmt
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def _validated_cover(row):
    """Published typed evidence is required in addition to current eligibility."""
    try:
        raw=row['payload_json']
        if not isinstance(raw,str) or len(raw.encode()) > 65536 or digest(raw) != row['payload_digest']:
            return None
        plan=json.loads(raw)
        keys={'schema','work_id','candidate_id','origin','rights_id','rights_version',
              'expected_head_version','input_binding_digest','candidate_spec_digest','asset'}
        if type(plan) is not dict or set(plan)!=keys or canonical_json(plan)!=raw or plan['schema']!=COVER_SCHEMA:
            return None
        binding=dict(work_id=row['work_id'],candidate_id=row['id'],origin=row['origin'],
            rights_id=row['rights_id'],rights_version=row['rights_version_at_creation'],
            expected_head_version=row['expected_head_version'],input_binding_digest=row['request_digest'])
        if any(type(plan[k]) is not type(v) or plan[k]!=v for k,v in binding.items()):
            return None
        if (plan['candidate_id'] != str(uuid.uuid5(uuid.UUID(row['producing_job_id']),f"metadata:{row['generation']}"))
                or type(plan['rights_version']) is not int or plan['rights_version']<1
                or type(plan['expected_head_version']) is not int or plan['expected_head_version']<0):
            return None
        evidence=json.loads(row['validation_receipt'])
        if (type(evidence) is not dict or set(evidence)!={'provenance','filename'}
                or canonical_json(evidence['provenance'])!=row['provenance_json']
                or digest(row['provenance_json'])!=plan['candidate_spec_digest']):
            return None
        identity=_cover_asset(plan,row)
        if identity is None:
            return None
        path=plan['asset']['prepared_relpath']
        if not re.fullmatch(re.escape(row['prepared_prefix'])+r'[0-9a-f]{16}/[0-9a-f]{32}\.bin',path):
            return None
        return CoverObjectSnapshot(*identity)
    except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):
        return None


def eligible_covers_in(conn, work_ids, *, now_ms):
    """One bounded batch for projections and fresh readers; no per-card query."""
    if type(now_ms) is not int or not 0 <= now_ms <= 9223372036854775807:
        raise AuthError('ASSET_UNAVAILABLE',409)
    if not work_ids:
        return {}
    rows=conn.execute(text("""SELECT c.*,h.version AS head_version,r.version AS rights_version,
        r.cache_until_ms,k.version AS control_version,o.byte_size,o.detected_mime,
        o.local_relpath,o.state AS object_state,p.payload_json,p.payload_digest,p.validation_receipt,
        i.expected_head_version,i.request_digest,
        'quarantine/prepared/jobs/'||c.producing_job_id||'/generation-'||c.generation||'/' AS prepared_prefix
        FROM json_each(:ids) requested JOIN metadata_heads h ON h.work_id=requested.value
        JOIN metadata_eligible_candidates_v1 c ON c.id=h.cover_id AND c.work_id=h.work_id AND c.kind='cover'
        JOIN metadata_rights r ON r.id=c.rights_id
        JOIN metadata_candidate_control k ON k.candidate_id=c.id
        JOIN objects o ON o.sha256=c.object_sha
        JOIN publication_plans p ON p.job_id=c.producing_job_id AND p.generation=c.generation
        JOIN metadata_job_inputs i ON i.job_id=c.producing_job_id
        JOIN jobs j ON j.id=i.job_id
        WHERE (r.cache_until_ms IS NULL OR r.cache_until_ms>:now)
        AND i.work_id=c.work_id AND i.origin=c.origin AND i.rights_id=c.rights_id
        AND i.expected_rights_version=c.rights_version_at_creation
        AND i.source_revision_id IS c.source_revision_id
        AND library_plan_valid_v1(j.payload_json,i.request_digest)=1"""),
        dict(ids=json.dumps(work_ids),now=now_ms)).mappings()
    result={}
    for row in rows:
        asset=_validated_cover(row)
        if asset is not None:
            result[row['work_id']]=(row,asset)
    return result


class CoverService:
    def __init__(self, publisher, queries, *, admission):
        if (publisher.engine is not queries.engine or publisher.permissions.auth is not queries.auth
                or not callable(queries.now_ms)):
            raise ValueError('One cover publisher, query clock and authority required')
        self.publisher,self.queries,self.admission=publisher,queries,admission
        self.engine,self.auth=queries.engine,queries.auth

    def resolve(self, token, work_id, version):
        _ident(work_id)
        if type(version) is not int or not 0 <= version <= 9223372036854775807:
            raise AuthError('INVALID_INPUT',422)
        with self.engine.connect() as conn, conn.begin():
            actor=self.auth.current(conn,token)
            if not conn.execute(text('WITH '+VALID_OPTIONS_V1+' SELECT 1 FROM valid_options WHERE work_id=:work LIMIT 1'),dict(work=work_id)).first():
                raise AuthError('RESOURCE_NOT_FOUND',404)
            current=eligible_covers_in(conn,[work_id],now_ms=_now(self.queries.now_ms)).get(work_id)
            if current is None or current[0]['head_version']!=version:
                raise AuthError('ASSET_UNAVAILABLE',409)
            row,asset=current
            return CoverSelection(ReadPrincipal(actor['id'],digest(token),actor['credential_version'],actor['role']),
                work_id,version,row['id'],row['rights_version'],row['control_version'],row['cache_until_ms'],
                row['payload_digest'],row['request_digest'],(asset,))

    def response(self, token, work_id, version, *, method='GET', on_created=None):
        if method not in {'GET','HEAD'}:
            raise AuthError('INVALID_INPUT',422)
        resolve=lambda:self.resolve(token,work_id,version)
        expected=resolve()
        asset=expected.assets[0]
        def prepare(opened):
            headers=((b'content-type',asset.detected_mime.encode('ascii')),
                (b'content-length',str(asset.byte_size).encode('ascii')),
                (b'content-disposition',b'inline'),(b'cache-control',b'private, no-store'),
                (b'x-content-type-options',b'nosniff'),(b'referrer-policy',b'no-referrer'))
            return PreparedTransfer(200,headers)
        def produce(opened, transfer):
            if method!='HEAD':
                yield from opened.chunks(asset)
        handle=OwnedProducer.start(open_selected=lambda:self.publisher.read_selected(resolve,expected=expected),
            prepare=prepare,produce=produce,unique_originals=1,admission=self.admission,on_created=on_created)
        return OwnedResponse(handle)
