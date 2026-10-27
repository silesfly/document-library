"""Immutable library read contracts and versioned SQLite normalization.

No session secret, filesystem path, job identity or source receipt is a public
DTO field. The v1 SQL functions are part of migration0013's frozen contract;
keep them available unchanged when adding a future normalization version.
"""
from dataclasses import dataclass, field
from typing import ClassVar
import hashlib
import json
import math
import unicodedata

from .catalog import CATEGORIES

SORTS = frozenset({'latest','updated','title','author','rating','votes','popularity','episode','size'})
REPRESENTATIONS = frozenset({'text','imagepack','document','single_image'})
PUBLICATION_STATES = frozenset({'completed','ongoing','unknown'})
READ_POLICIES = frozenset({'current','admin_retained','ticket'})
TICKET_TTL_MS = 120000
ZIP_LIMIT = 2 * 1024**3
DISABLED_TICKET_SESSION = '0' * 64


def search_key_v1(value):
    if value is None:
        return ''
    if not isinstance(value,str):
        raise TypeError('Search metadata must be text')
    return ' '.join(unicodedata.normalize('NFKC',value).casefold().split())


def archive_name_v1(value):
    if (not isinstance(value,str) or not value or len(value.encode('utf-8'))>1024
            or unicodedata.normalize('NFKC',value)!=value or '\\' in value or ':' in value
            or any(unicodedata.category(char).startswith('C') for char in value)):
        return False
    return all(part and part not in {'.','..'} and not part.startswith('.')
               and not part.endswith(('.', ' ')) and len(part.encode('utf-8'))<=255
               for part in value.split('/'))


def plan_valid_v1(payload, expected):
    """Frozen raw UTF-8 SHA contract; never normalize or reserialize JSON."""
    return int(isinstance(payload,str) and isinstance(expected,str)
               and hashlib.sha256(payload.encode('utf-8')).hexdigest()==expected)


def time_ms_v1(value):
    from datetime import datetime, timezone
    if not isinstance(value,str):
        return None
    try:
        dt=datetime.fromisoformat(value.replace('Z','+00:00'))
        if dt.tzinfo is None:
            return None
        delta=dt.astimezone(timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)
        return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000
    except (ValueError,OverflowError):
        return None


def coverage_value_v1(raw, gaps=False):
    # Late pure import avoids the publication -> database -> contracts cycle.
    from .publication import _coverage, PublicationError
    try:
        value=json.loads(raw);_coverage(value)
        if value['unit']!='episode' or not value['intervals']:
            return None
        return int(bool(value['gaps'])) if gaps else sum(b-a+1 for a,b in value['intervals'])
    except (TypeError,ValueError,KeyError,PublicationError):
        return None


def register_sqlite_functions(connection):
    # REPLACE must fire the displaced row's DELETE guards/OLD-work refresh.
    # Read first: repeated registration must not invalidate prepared statements
    # by toggling an already-correct connection setting.
    if connection.execute('PRAGMA recursive_triggers').fetchone()[0]!=1:
        connection.execute('PRAGMA recursive_triggers=ON')
    if connection.execute('PRAGMA recursive_triggers').fetchone()[0]!=1:
        raise RuntimeError('Recursive SQLite triggers are required')
    connection.create_function('library_search_key_v1',1,search_key_v1,deterministic=True)
    connection.create_function('library_archive_name_v1',1,lambda value:int(archive_name_v1(value)),deterministic=True)
    connection.create_function('library_plan_valid_v1',2,plan_valid_v1,deterministic=True)
    connection.create_function('library_time_ms_v1',1,time_ms_v1,deterministic=True)
    connection.create_function('library_episode_count_v1',1,coverage_value_v1,deterministic=True)
    connection.create_function('library_episode_gaps_v1',1,lambda raw:coverage_value_v1(raw,True),deterministic=True)


def canonical_json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _ident(value):
    if not isinstance(value,str) or not value or len(value)>200 or any(unicodedata.category(c).startswith('C') for c in value):
        raise ValueError('Explicit identifier required')
    return value


@dataclass(frozen=True)
class LibraryQuery:
    """Categories are OR; representations require ALL listed accessible types.

    In particular ('text','imagepack') means both currently authorized active
    representations. A blocked image edition cannot make text-only work match.
    """
    search: str = ''
    categories: tuple = ()
    states: tuple = ()
    formats: tuple = ()
    representations: tuple = ()
    genres: tuple = ()
    unrated: bool | None = None
    favorite: bool | None = None
    has_summary: bool | None = None
    downloaded: bool | None = None
    has_gaps: bool | None = None
    sort: str = 'updated'
    direction: str | None = None
    limit: int = 50
    cursor: str | None = None

    def __post_init__(self):
        if not isinstance(self.search,str) or len(self.search)>200 or self.sort not in SORTS:
            raise ValueError('Invalid library search or sort')
        if self.direction not in {None,'asc','desc'} or type(self.limit) is not int or not 1<=self.limit<=100:
            raise ValueError('Invalid library page')
        if self.cursor is not None and (not isinstance(self.cursor,str) or not 1<=len(self.cursor)<=4096):
            raise ValueError('Invalid opaque cursor')
        for name in ('categories','states','formats','representations','genres'):
            value=getattr(self,name)
            if not isinstance(value,tuple) or len(value)>64 or any(not isinstance(v,str) or not v or len(v)>128 for v in value):
                raise ValueError('Explicit bounded filter tuples required')
            object.__setattr__(self,name,tuple(sorted(set(value))))
        if (not set(self.categories)<=CATEGORIES or not set(self.states)<=PUBLICATION_STATES
                or not set(self.representations)<=REPRESENTATIONS):
            raise ValueError('Unknown library filter')
        for name in ('unrated','favorite','has_summary','downloaded','has_gaps'):
            if getattr(self,name) is not None and type(getattr(self,name)) is not bool:
                raise ValueError('Boolean or absent filter required')

    def fingerprint(self):
        # Cursor/limit are not search identity; changing a filter or sort is.
        payload={name:getattr(self,name) for name in self.__dataclass_fields__ if name not in {'cursor','limit'}}
        payload['search']=search_key_v1(self.search)
        return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


@dataclass(frozen=True, kw_only=True)
class RankingConfig:
    """Server-owned v1 ranking policy shared by all configured query paths."""
    m: float = 5.0
    MAX_M: ClassVar[float] = 1_000_000.0
    POPULAR_WEIGHTS: ClassVar[tuple] = (0.65, 0.20, 0.10, 0.05)
    MIN_OWN_RATINGS: ClassVar[int] = 5
    MIN_OVERLAP: ClassVar[int] = 2
    OVERLAP_SHRINK: ClassVar[int] = 5
    MAX_PEERS: ClassVar[int] = 50

    def __post_init__(self):
        if type(self.m) not in {int,float} or not math.isfinite(self.m) or not 0 < self.m <= self.MAX_M:
            raise ValueError('Finite bounded positive ranking m required')
        object.__setattr__(self,'m',float(self.m))

    @property
    def version(self):
        return 'ranking-v1-m'+format(self.m,'.17g')


@dataclass(frozen=True, kw_only=True)
class RecommendationQuery:
    """None means omitted; an explicit empty tuple clears one saved filter."""
    mode: str = 'personalized'
    limit: int = 20
    categories: tuple | None = None
    states: tuple | None = None
    formats: tuple | None = None
    representations: tuple | None = None
    genres: tuple | None = None
    exclude_rated: bool | None = None
    exclude_downloaded: bool | None = None

    def __post_init__(self):
        if not isinstance(self.mode,str) or self.mode not in {'top_rated','popular','personalized','unrated'} or type(self.limit) is not int or not 1<=self.limit<=50:
            raise ValueError('Invalid finite recommendation request')
        names=('categories','states','formats','representations','genres')
        if any(getattr(self,name) is not None and not isinstance(getattr(self,name),(tuple,list)) for name in names):
            raise ValueError('Bounded filter collections required')
        checked=LibraryQuery(**{name:tuple(getattr(self,name)) if getattr(self,name) is not None else () for name in names})
        for name in names:
            if getattr(self,name) is not None:
                object.__setattr__(self,name,getattr(checked,name))
        for name in ('exclude_rated','exclude_downloaded'):
            if getattr(self,name) is not None and type(getattr(self,name)) is not bool:
                raise ValueError('Boolean or absent exclusion required')


@dataclass(frozen=True)
class AssetSelection:
    revision_id: str
    positions: tuple | None = None
    option_id: str | None = None
    work_id: str | None = None
    edition_id: str | None = None

    def __post_init__(self):
        _ident(self.revision_id)
        for name in ('option_id','work_id','edition_id'):
            if getattr(self,name) is not None:
                _ident(getattr(self,name))
        if self.positions is not None and (not isinstance(self.positions,tuple) or not 1<=len(self.positions)<=200
                or any(type(value) is not int or value<0 for value in self.positions)
                or len(set(self.positions))!=len(self.positions)):
            raise ValueError('Explicit unique bounded positions required')
        if self.positions is None and self.option_id is None:
            raise ValueError('An option or explicit positions are required')


@dataclass(frozen=True)
class ReadPrincipal:
    user_id: str
    session_hash: str = field(repr=False)
    credential_version: int
    role: str


@dataclass(frozen=True)
class SourceProof:
    job_id: str = field(repr=False)
    generation: int
    payload_digest: str = field(repr=False)


@dataclass(frozen=True)
class PreviewProof:
    source: SourceProof
    source_sha256: str
    source_byte_size: int
    manifest_json: str = field(repr=False)


@dataclass(frozen=True)
class AssetSnapshot:
    selection_index: int
    ordinal: int
    work_id: str
    edition_id: str
    revision_id: str
    option_id: str | None
    asset_position: int
    object_sha: str
    byte_size: int
    detected_mime: str
    file_format: str
    asset_role: str
    sources: tuple = field(repr=False)
    previews: tuple = field(default=(),repr=False)

    def public(self):
        return {name:getattr(self,name) for name in ('work_id','edition_id','revision_id','option_id',
                'asset_position','object_sha','byte_size','detected_mime','file_format','asset_role')}


@dataclass(frozen=True)
class OptionSnapshot:
    selection_index: int
    work_id: str
    edition_id: str
    revision_id: str
    option_id: str
    format: str
    delivery_kind: str
    coverage_json: str
    coverage_confidence: str
    positions: tuple

    def public(self):
        return dict(work_id=self.work_id,edition_id=self.edition_id,revision_id=self.revision_id,
                    option_id=self.option_id,format=self.format,delivery_kind=self.delivery_kind,
                    coverage=json.loads(self.coverage_json),coverage_confidence=self.coverage_confidence,
                    asset_positions=list(self.positions))


@dataclass(frozen=True)
class ResolvedSelection:
    principal: ReadPrincipal
    assets: tuple
    options: tuple

    def public(self):
        return dict(assets=[asset.public() for asset in self.assets],options=[option.public() for option in self.options])


@dataclass(frozen=True)
class TicketOption:
    selection_order: int
    edition_id: str
    revision_id: str
    option_id: str
    format: str
    delivery_kind: str
    coverage_json: str


@dataclass(frozen=True)
class TicketAsset:
    selection_order: int
    ordinal: int
    revision_id: str
    asset_position: int
    object_sha: str
    byte_size: int
    detected_mime: str
    file_format: str
    asset_role: str
    entry_name: str


@dataclass(frozen=True)
class TicketSnapshot:
    ticket_id: str
    user_id: str
    session_hash: str = field(repr=False)
    credential_version: int
    work_id: str
    issued_at_ms: int
    expires_at_ms: int
    archive_bytes: int
    snapshot_digest: str
    options: tuple
    assets: tuple
    state: str
    attempt_id: str | None = None

    def payload(self):
        return dict(ticket_id=self.ticket_id,user_id=self.user_id,session_hash=self.session_hash,
                    credential_version=self.credential_version,work_id=self.work_id,
                    issued_at_ms=self.issued_at_ms,expires_at_ms=self.expires_at_ms,archive_bytes=self.archive_bytes,
                    options=[dict((name,getattr(option,name)) for name in option.__dataclass_fields__) for option in self.options],
                    assets=[dict((name,getattr(asset,name)) for name in asset.__dataclass_fields__) for asset in self.assets])

    def digest(self):
        return hashlib.sha256(canonical_json(self.payload()).encode()).hexdigest()


@dataclass(frozen=True)
class WorkReadDTO:
    work_id: str
    title: str
    author: str | None
    publication_state: str
    item_type: str = 'unknown'
    created_at: str | None = None
    updated_at: str | None = None
    genres: tuple = ()
    tags: tuple = ()
    classification_review_required: bool = False
    basis_option_id: str | None = None
    episode_count: int | None = None
    size_bytes: int | None = None
    weighted_rating: float | None = None
    rating_average: float | None = None
    rating_votes: int | None = None
    my_rating_half_steps: int | None = field(default=None,kw_only=True)
    popularity: float | None = None
    favorite: bool | None = None
    downloaded: bool | None = None
    description_state: str = 'pending'
    summary_state: str = 'pending'
    cover_state: str = 'pending'
    cover_url: str | None = field(default=None,kw_only=True)

    def __post_init__(self):
        _ident(self.work_id)
        if self.publication_state not in PUBLICATION_STATES:
            raise ValueError('Unknown publication state')
        if self.my_rating_half_steps is not None and (type(self.my_rating_half_steps) is not int or not 1<=self.my_rating_half_steps<=10):
            raise ValueError('Strict own half-step rating required')
        for name in ('episode_count','size_bytes','rating_votes'):
            value=getattr(self,name)
            if value is not None and (type(value) is not int or value<0):
                raise ValueError('Actual nonnegative count or unknown required')
        for name in ('weighted_rating','rating_average','popularity'):
            value=getattr(self,name)
            if value is not None and (type(value) not in {int,float} or not math.isfinite(value)):
                raise ValueError('Finite measured metric or unknown required')
        if (self.episode_count is not None or self.size_bytes is not None) and self.basis_option_id is None:
            raise ValueError('Option-based counts require their actual basis option')
        if any(getattr(self,name) not in {'pending','ready','unavailable'} for name in ('description_state','summary_state','cover_state')):
            raise ValueError('Explicit metadata state required')


def verify_metadata_in(connection):
    """Raw SQLite invariant check, including stored keys from older connections."""
    mismatch=connection.execute("""SELECT 1 FROM works w LEFT JOIN library_metadata m ON m.work_id=w.id
        WHERE m.work_id IS NULL OR m.title<>w.title_display OR m.author IS NOT w.author
        OR m.title_key<>library_search_key_v1(w.title_display)
        OR m.author_key<>library_search_key_v1(w.author)
        OR m.description_key<>library_search_key_v1(m.description_text) LIMIT 1""").fetchone()
    if mismatch:
        raise ValueError('Library metadata source or normalization differs')
    for table in ('library_tags','library_genres'):
        if connection.execute(f'SELECT 1 FROM {table} WHERE key<>library_search_key_v1(label) LIMIT 1').fetchone():
            raise ValueError('Library label normalization differs')


def rebuild_search_in(connection):
    """Rebuild only derivative FTS; metadata/source identity is unchanged."""
    verify_metadata_in(connection)
    connection.execute('DELETE FROM library_search_fts')
    connection.execute('UPDATE library_metadata SET search_epoch=search_epoch+1')


def invalidate_downloads_in(connection, reason):
    """No stop inference: audit and pins survive, active ownership is uncertain.

    Ticket digests originally bind the live session. A filtered snapshot keeps
    that historical digest but zeroes the session identifier and invalidates the
    execution, so it deliberately cannot pass the live ticket digest check.
    """
    tickets=connection.execute('UPDATE download_tickets SET session_token_hash=?,invalidated=1,invalidation_reason=?',
                               (DISABLED_TICKET_SESSION,reason)).rowcount
    attempts=connection.execute("""UPDATE download_attempts SET invalidated=1,
        state=CASE WHEN state IN ('running','cancelling') THEN 'uncertain' ELSE state END""").rowcount
    return dict(invalidated_download_tickets=tickets,invalidated_download_attempts=attempts,
                uncertain_download_attempts=connection.execute("SELECT count(*) FROM download_attempts WHERE state='uncertain'").fetchone()[0])
