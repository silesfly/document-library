"""Ephemeral provider discovery/identity/rights primitives, without persistence.

No network, credentials, model, database or publication occurs here. API search
hits are candidates. B3b must bind current provider/source/rights/job authority
and durable rate admission; none is inferred from these pure values.
"""
from dataclasses import asdict, dataclass
import hashlib
import re
import unicodedata
from urllib.parse import urlencode

from .catalog import title_match_key
from .library_contracts import canonical_json
from .safe_public_fetch import (FetchError, FetchRequest, OpenLibraryPolicy,
                                SEARCH_FIELDS, decode_metadata_json)


def _digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _text(value, maximum=512):
    if (not isinstance(value, str) or not value.strip() or len(value) > maximum
            or any(unicodedata.category(c).startswith('C') for c in value)):
        raise ValueError('bounded_bibliographic_text_required')
    return unicodedata.normalize('NFC', ' '.join(value.split()))


def _unicode_key(value):
    return unicodedata.normalize('NFKC', _text(value)).casefold()


def _language(value):
    value = _text(value, 35).lower()
    return {'eng': 'en', 'kor': 'ko', 'jpn': 'ja', 'chi': 'zh', 'zho': 'zh',
            'fra': 'fr', 'fre': 'fr', 'deu': 'de', 'ger': 'de'}.get(value, value)


def _isbn(value):
    value = re.sub(r'[\s-]', '', _text(value, 32)).upper()
    if re.fullmatch('[0-9]{9}[0-9X]', value):
        if sum((10-i) * (10 if c == 'X' else int(c)) for i, c in enumerate(value)) % 11:
            raise ValueError('invalid_isbn')
        value = '978' + value[:9]
        value += str((-sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(value))) % 10)
    elif not re.fullmatch('(?:978|979)[0-9]{10}', value) or value.startswith('9790'):
        raise ValueError('invalid_isbn')
    if sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(value)) % 10:
        raise ValueError('invalid_isbn')
    return value


@dataclass(frozen=True)
class WorkIdentity:
    """Server-resolved immutable snapshot; do not construct from request JSON."""
    work_id: str
    title: str
    authors: tuple[str, ...]
    isbns: tuple[str, ...] = ()
    language: str | None = None
    publisher: str | None = None
    original_title: str | None = None
    trusted_title_aliases: tuple[str, ...] = ()
    provider_editions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self):
        _text(self.work_id, 128)
        _text(self.title)
        for seq in (self.authors, self.isbns, self.trusted_title_aliases, self.provider_editions):
            if not isinstance(seq, tuple) or len(seq) > 8:
                raise ValueError('bounded_identity_tuple_required')
        for author in self.authors:
            _text(author, 256)
        for title in self.trusted_title_aliases:
            _text(title)
        for value in self.isbns:
            _isbn(value)
        for value in (self.language, self.publisher, self.original_title):
            if value is not None:
                _text(value)
        for provider, external_id in self.provider_editions:
            if provider not in {'openlibrary', 'googlebooks'}:
                raise ValueError('unknown_provider')
            _external_id(provider, external_id, edition=True)

    @property
    def digest(self):
        return _digest(asdict(self))


@dataclass(frozen=True)
class BibliographicQuery:
    title: str
    authors: tuple[str, ...]
    isbns: tuple[str, ...]
    original_title: str | None
    publisher: str | None
    language: str | None
    identity_digest: str

    @classmethod
    def from_work(cls, binding):
        if type(binding) is not WorkIdentity:
            raise TypeError('server_work_identity_required')
        return cls(_text(binding.title), tuple(_text(a, 256) for a in binding.authors),
                   tuple(_isbn(i) for i in binding.isbns), binding.original_title,
                   binding.publisher, binding.language, binding.digest)


@dataclass(frozen=True)
class LookupPlan:
    provider: str
    query_digest: str
    requests: tuple[FetchRequest, ...]
    maximum_exchanges: int = 6


@dataclass(frozen=True)
class Unconfigured:
    provider: str = 'googlebooks'
    reason: str = 'application_identification_not_configured'
    requests: tuple = ()


class OpenLibraryAdapter:
    def plan(self, query):
        if type(query) is not BibliographicQuery:
            raise TypeError('bibliographic_query_required')
        terms = {'isbn': query.isbns[0]} if query.isbns else {'title': query.title}
        if query.authors and not query.isbns:
            terms['author'] = query.authors[0]
        terms.update(limit='5', fields=SEARCH_FIELDS)
        request = FetchRequest('https://openlibrary.org/search.json?' + urlencode(terms))
        OpenLibraryPolicy().validate(request)
        return LookupPlan('openlibrary', query.identity_digest, (request,))

    def detail_plan(self, query, candidate):
        if candidate.provider != 'openlibrary' or candidate.query_digest != query.identity_digest:
            raise ValueError('candidate_binding')
        prefix = 'books' if candidate.kind == 'edition' else 'works'
        request = FetchRequest(f'https://openlibrary.org/{prefix}/{candidate.external_id}.json')
        OpenLibraryPolicy().validate(request)
        return LookupPlan('openlibrary', query.identity_digest, (request,))

    def cover_intent(self, observation):
        # An intent is not a storage grant. Caller must evaluate exact resource
        # permissions and current edition identity before creating any stage.
        if observation.provider != 'openlibrary' or observation.cover_id is None:
            return None
        return FetchRequest(f'https://covers.openlibrary.org/b/id/{observation.cover_id}-L.jpg?default=false', 'cover')


class GoogleBooksAdapter:
    def plan(self, query):
        if type(query) is not BibliographicQuery:
            raise TypeError('bibliographic_query_required')
        return Unconfigured()


def _external_id(provider, value, *, edition=False):
    pattern = r'OL[1-9][0-9]*M' if edition else r'OL[1-9][0-9]*[MW]'
    if provider == 'googlebooks':
        pattern = r'[A-Za-z0-9_-]{1,64}'
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError('provider_identifier')
    return value


@dataclass(frozen=True)
class ProviderObservation:
    provider: str
    external_id: str
    kind: str
    source_url: str
    title: str
    authors: tuple[str, ...]
    isbns: tuple[str, ...]
    languages: tuple[str, ...]
    publishers: tuple[str, ...]
    query_digest: str
    edition_ids: tuple[str, ...] = ()
    cover_id: int | None = None

    @property
    def digest(self):
        return _digest(asdict(self))


def _strings(value, maximum=8):
    if not isinstance(value, list) or len(value) > maximum:
        raise ValueError('provider_array')
    return tuple(_text(v) for v in value)


def parse_observation(provider, ephemeral_response, *, query_digest):
    """Discard every prose/image/accessInfo field; output remains ephemeral.

    Work-level search ISBNs can span editions. Only an edition observation may
    qualify exact edition matching; missing author names in OL detail is review,
    never an excuse to borrow the local query's author as remote evidence.
    """
    if not isinstance(query_digest, str) or not re.fullmatch('[0-9a-f]{64}', query_digest):
        raise ValueError('query_digest')
    data = decode_metadata_json(ephemeral_response)
    try:
        observations = []
        if provider == 'openlibrary':
            entries = data['docs'] if 'docs' in data else [data]
            if not isinstance(entries, list) or len(entries) > 5:
                raise ValueError('search_limit')
            for item in entries:
                if not isinstance(item, dict):
                    raise ValueError('provider_schema')
                key = item['key']
                if not isinstance(key, str) or not re.fullmatch(r'/(?:works/OL[1-9][0-9]*W|books/OL[1-9][0-9]*M)', key):
                    raise ValueError('provider_identifier')
                external_id = key.rsplit('/', 1)[-1]
                kind = 'edition' if key.startswith('/books/') else 'work'
                authors = _strings(item.get('author_name', []))
                isbns = tuple(_isbn(i) for i in _strings(item.get('isbn', item.get('isbn_13', [])), 20))
                langs = item.get('language', [])
                if 'languages' in item:
                    langs = [v['key'].removeprefix('/languages/') for v in item['languages']]
                languages = tuple(_language(v) for v in _strings(langs))
                publishers = _strings(item.get('publisher', item.get('publishers', [])))
                editions = tuple(_external_id(provider, v, edition=True) for v in _strings(item.get('edition_key', [])))
                cover = item.get('cover_i')
                if cover is None and 'covers' in item:
                    covers = item['covers']
                    if not isinstance(covers, list) or len(covers) > 8:
                        raise ValueError('provider_array')
                    cover = covers[0] if len(covers) == 1 else None
                if cover is not None and (type(cover) is not int or not 1 <= cover <= 10**12):
                    raise ValueError('cover_identifier')
                observations.append(ProviderObservation(provider, external_id, kind,
                    'https://openlibrary.org' + key, _text(item['title']), authors, isbns,
                    languages, publishers, query_digest, editions, cover))
        elif provider == 'googlebooks':
            entries = data.get('items', [])
            if not isinstance(entries, list) or len(entries) > 5:
                raise ValueError('search_limit')
            for item in entries:
                external_id = _external_id(provider, item['id'], edition=True)
                info = item['volumeInfo']
                ids = info.get('industryIdentifiers', [])
                if not isinstance(ids, list) or len(ids) > 8:
                    raise ValueError('provider_array')
                isbns = tuple(_isbn(i['identifier']) for i in ids if i['type'] in {'ISBN_10', 'ISBN_13'})
                observations.append(ProviderObservation(provider, external_id, 'edition',
                    'https://books.google.com/books?' + urlencode({'id': external_id}),
                    _text(info['title']), _strings(info.get('authors', [])), isbns,
                    (_language(info['language']),) if 'language' in info else (),
                    (_text(info['publisher']),) if 'publisher' in info else (), query_digest))
        else:
            raise ValueError('unknown_provider')
        return tuple(observations)
    except (KeyError, TypeError, AttributeError, ValueError):
        raise ValueError('provider_schema_or_identity_invalid') from None


@dataclass(frozen=True)
class MatchDecision:
    state: str
    reasons: tuple[str, ...]
    identity_digest: str
    observation_digest: str
    proof_digest: str


def match_observation(observation, work_identity):
    if type(observation) is not ProviderObservation or type(work_identity) is not WorkIdentity:
        raise TypeError('typed_identity_required')
    work = work_identity
    reasons, conflict = [], False
    if observation.query_digest != work.digest:
        reasons.append('stale_local_identity')
        conflict = True
    allowed_titles = (work.title,) + work.trusted_title_aliases
    if _unicode_key(observation.title) not in {_unicode_key(t) for t in allowed_titles}:
        reasons.append('title_conflict_or_unverified_translation')
        conflict = True
    candidate_key = title_match_key(work.title)
    # This lookup signal never grants exact identity, including nonempty CJK
    # strings which reduce to the same digits under catalog normalization.
    if not candidate_key or candidate_key.isdigit():
        reasons.append('catalog_key_not_identity_evidence')
    remote_authors = {_unicode_key(a) for a in observation.authors}
    if not work.authors or not remote_authors:
        reasons.append('author_missing')
    elif remote_authors != {_unicode_key(a) for a in work.authors}:
        reasons.append('author_conflict')
        conflict = True
    if not work.language or not observation.languages:
        reasons.append('language_missing')
    elif set(observation.languages) != {_language(work.language)}:
        reasons.append('language_or_edition_conflict')
        conflict = True
    if work.publisher and not observation.publishers:
        reasons.append('publisher_missing')
    elif work.publisher and {_unicode_key(p) for p in observation.publishers} != {_unicode_key(work.publisher)}:
        reasons.append('publisher_or_edition_conflict')
        conflict = True
    local_isbns, remote_isbns = {_isbn(i) for i in work.isbns}, set(observation.isbns)
    trusted = {i for p, i in work.provider_editions if p == observation.provider}
    if local_isbns and remote_isbns and not local_isbns.intersection(remote_isbns):
        reasons.append('isbn_conflict')
        conflict = True
    if trusted and observation.external_id not in trusted:
        reasons.append('edition_conflict')
        conflict = True
    strong = bool(local_isbns.intersection(remote_isbns) or observation.external_id in trusted)
    if not strong:
        reasons.append('strong_edition_identifier_missing')
    if observation.kind != 'edition' or len(remote_isbns) > 1:
        reasons.append('edition_detail_required')
    blocking = set(reasons) - {'catalog_key_not_identity_evidence'}
    state = 'reject' if conflict else ('review' if blocking else 'exact')
    proof = _digest(dict(state=state, reasons=reasons, identity=work.digest, observation=observation.digest))
    return MatchDecision(state, tuple(reasons), work.digest, observation.digest, proof)


@dataclass(frozen=True)
class ScopedPolicyReceipt:
    """Explicit server/current source policy input, not API/accessInfo evidence."""
    provider: str
    external_id: str
    source_url: str
    resource_kind: str
    resource_url: str
    identity_digest: str
    policy_digest: str
    valid_until_ms: int
    publisher_officiality: str
    display: bool | None
    cache: bool | None
    derivative: bool | None
    attribution: str | None
    cache_until_ms: int | None
    backup_allowed: bool | None
    all_copies_deletion_required: bool | None


@dataclass(frozen=True)
class LinkOnly:
    source_url: str
    external_id: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PermittedMetadata:
    provider: str
    external_id: str
    source_url: str
    resource_url: str
    resource_kind: str
    identity_digest: str
    observation_digest: str
    match_proof_digest: str
    policy_digest: str
    publisher_officiality: str
    display: bool
    cache: bool
    derivative: bool
    attribution: str
    cache_until_ms: int
    title: str
    authors: tuple[str, ...]


def evaluate_policy(observation, match, scoped_receipt, *, now_ms):
    link = lambda reason: LinkOnly(observation.source_url, observation.external_id, (reason,))
    if (match.state != 'exact' or match.observation_digest != observation.digest
            or match.identity_digest != observation.query_digest):
        return link('exact_current_edition_match_required')
    receipt = scoped_receipt
    if type(receipt) is not ScopedPolicyReceipt:
        return link('source_scoped_policy_unknown')
    if (receipt.provider != observation.provider or receipt.external_id != observation.external_id
            or receipt.source_url != observation.source_url or receipt.identity_digest != match.identity_digest
            or not re.fullmatch('[0-9a-f]{64}', receipt.policy_digest)
            or type(receipt.valid_until_ms) is not int or type(now_ms) is not int
            or receipt.valid_until_ms <= now_ms):
        return link('policy_scope_or_freshness')
    if receipt.resource_kind == 'metadata':
        resource = observation.source_url
    elif receipt.resource_kind == 'cover':
        intent = OpenLibraryAdapter().cover_intent(observation)
        resource = intent.url if intent is not None else None
    else:
        resource = None
    if not resource or receipt.resource_url != resource:
        return link('resource_scope')
    if (receipt.publisher_officiality != 'verified' or receipt.display is not True or receipt.cache is not True
            or type(receipt.derivative) is not bool or receipt.backup_allowed is not True
            or receipt.all_copies_deletion_required is not False):
        return link('independent_permissions_or_retention_unknown')
    if (not isinstance(receipt.attribution, str) or not receipt.attribution.strip() or len(receipt.attribution) > 1024
            or type(receipt.cache_until_ms) is not int or not now_ms < receipt.cache_until_ms <= receipt.valid_until_ms):
        return link('attribution_or_cache_deadline')
    return PermittedMetadata(observation.provider, observation.external_id, observation.source_url,
        resource, receipt.resource_kind, match.identity_digest, observation.digest, match.proof_digest,
        receipt.policy_digest, receipt.publisher_officiality, True, True, receipt.derivative,
        receipt.attribution, receipt.cache_until_ms, observation.title, observation.authors)


def inspect_permitted_cover(fd, declared_content_type, permission, *, now_ms):
    """Reuse A's real read-FD isolated decoder and unchanged effective 16M cap.

    This validates an already permitted stage; it does not register/install or
    publish it. Current policy/head/generation must be rechecked by B3b/A later.
    """
    from .ingest_formats import inspect_fd
    from .metadata_service import MetadataService
    names = {'image/jpeg': 'cover.jpg', 'image/png': 'cover.png', 'image/webp': 'cover.webp'}
    if (type(permission) is not PermittedMetadata or permission.resource_kind != 'cover'
            or not permission.cache or not permission.display
            or type(now_ms) is not int or now_ms >= permission.cache_until_ms
            or declared_content_type not in names):
        raise ValueError('permitted_cover_stage_required')
    return inspect_fd(fd, names[declared_content_type], MetadataService.COVER_LIMITS)
