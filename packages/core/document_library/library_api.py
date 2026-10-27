"""Explicit v1 library facade: public metadata, fresh authority and lazy streams."""
from datetime import datetime, timezone
import re
import uuid

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from starlette.responses import JSONResponse

from .auth import AuthError
from .cursors import CursorError, decode_cursor, encode_cursor
from .cover_service import CoverService
from .library_contracts import AssetSelection, LibraryQuery, RecommendationQuery, _ident
from .preview_service import PreviewNotReady
from .publication import PublicationError
from .request_quota import QuotaExceeded
from .storage import StorageError
from .streaming import AdmissionError, RangeError, parse_single_range, original_response
from .stream_lifecycle import LazyOwnedResponse, StreamSupervisor


class InputError(AuthError):
    def __init__(self):
        super().__init__('INVALID_INPUT',422)


# Public codes are explicit. Neither exception text nor arbitrary exception codes
# enter JSON. Details are constructed only from bounded known public fields.
PUBLIC_CODES = frozenset('''AUTHENTICATION_REQUIRED PERMISSION_DENIED CSRF_REJECTED
 ORIGIN_REJECTED INVALID_HOST DUPLICATE_HEADER JSON_REQUIRED BODY_TOO_LARGE
 INVALID_INPUT INVALID_CURSOR RESOURCE_NOT_FOUND ASSET_UNAVAILABLE ASSET_ACCESS_DENIED
 TICKET_ACCESS_DENIED TICKET_UNAVAILABLE TICKET_EXPIRED_OR_USED TICKET_BUSY
 TICKET_SNAPSHOT_CONFLICT TICKET_ARCHIVE_CONFLICT PREVIEW_NOT_READY
 PREVIEW_SELECTION_CHANGED SINGLE_ORIGINAL_REQUIRED ORIGINAL_SELECTION_REQUIRED
 INVALID_PREVIEW PREVIEW_KIND_MISMATCH INVALID_PREVIEW_PAGE INVALID_PREVIEW_CHAPTER
 INVALID_SELECTION ZIP_TOO_LARGE RESOURCE_STRUCTURE_LIMIT LIBRARY_SHUTTING_DOWN
 RANGE_NOT_SATISFIABLE INVALID_RANGE METHOD_NOT_ALLOWED STREAM_UNAVAILABLE
 RATE_LIMITED INTERNAL_ERROR'''.split())
MESSAGES = {401:'로그인이 필요합니다.',403:'요청 권한이 없습니다.',404:'접근 가능한 자료가 없습니다.',
    409:'선택한 자료를 현재 제공할 수 없습니다.',413:'요청한 자료가 제공 한도를 초과합니다.',
    416:'요청한 범위를 제공할 수 없습니다.',422:'요청 입력을 확인해 주세요.',
    429:'요청 횟수 한도를 초과했습니다.',400:'요청을 처리할 수 없습니다.',405:'지원하지 않는 요청 방식입니다.',
    415:'JSON 요청이 필요합니다.',503:'자료 전송을 현재 시작할 수 없습니다.',500:'요청을 완료할 수 없습니다.'}


def error_response(error, *, request_id=None):
    headers, details = {}, []
    if isinstance(error,AuthError):
        status = error.status if error.status in MESSAGES else 500
        code = str(error.code).upper()
        code = code if code in PUBLIC_CODES else ('AUTHENTICATION_REQUIRED' if status == 401 else 'PERMISSION_DENIED' if status == 403 else 'RESOURCE_NOT_FOUND' if status == 404 else 'ASSET_UNAVAILABLE' if status == 409 else 'INVALID_INPUT' if status in {400,422} else 'INTERNAL_ERROR')
        if isinstance(error,PreviewNotReady):
            state = error.state if error.state in {'unavailable','repair_running','repair_queued'} else 'unavailable'
            detail = dict(state=state)
            retry = error.retry_after_seconds
            if type(retry) is int and 1 <= retry <= 3600:
                detail['retry_after_seconds'] = retry
                headers['Retry-After'] = str(retry)
            details = [detail]
        if isinstance(error,QuotaExceeded):
            retry = error.retry_after_seconds
            if type(retry) is int and 1 <= retry <= 60:
                headers['Retry-After'] = str(retry)
        positions = getattr(error,'public_positions',None)
        if (isinstance(positions,tuple) and len(positions) <= 200
                and all(type(position) is int and position >= 0 for position in positions)):
            details = [dict(asset_positions=list(positions))]
    elif isinstance(error,RangeError):
        status,code = error.status, 'RANGE_NOT_SATISFIABLE' if error.status == 416 else 'INVALID_RANGE'
        if status == 416:
            headers['Content-Range'] = f'bytes */{error.size}'
    elif isinstance(error,CursorError):
        status,code = 422,'INVALID_CURSOR'
    elif isinstance(error,AdmissionError):
        status,code = 503,'STREAM_UNAVAILABLE'
    elif isinstance(error,(PublicationError,StorageError,OSError)):
        status,code = 409,'ASSET_UNAVAILABLE'
    else:
        status,code = 500,'INTERNAL_ERROR'
    headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer'})
    return JSONResponse(dict(error=dict(code=code,message=MESSAGES[status],
        request_id=request_id or str(uuid.uuid4()),details=details)),status_code=status,headers=headers)


def _params(request, *, scalar=(), repeated=()):
    values = request.query_params
    allowed = set(scalar)|set(repeated)
    if len(request.scope.get('query_string',b'')) > 65536 or any(name not in allowed for name in values):
        raise InputError()
    result = {}
    for name in scalar:
        entries = values.getlist(name)
        if len(entries)>1:
            raise InputError()
        if entries:
            result[name] = entries[0]
    for name in repeated:
        entries = values.getlist(name)
        if len(entries)>64:
            raise InputError()
        if entries:
            result[name] = tuple(entries)
    return result


def _number(value, *, low=0, high=10_000_000):
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{1,8}',value) or not low <= int(value) <= high:
        raise InputError()
    return int(value)


def _boolean(value):
    if value not in {'true','false'}:
        raise InputError()
    return value == 'true'


def _identifier(value):
    try:
        return _ident(value)
    except ValueError:
        raise InputError() from None


class DownloadChoice(BaseModel):
    model_config = ConfigDict(extra='forbid',strict=True)
    edition_id: str = Field(min_length=1,max_length=200)
    revision_id: str = Field(min_length=1,max_length=200)
    download_option_id: str = Field(min_length=1,max_length=200)


class DownloadSelections(BaseModel):
    model_config = ConfigDict(extra='forbid',strict=True)
    selection: list[DownloadChoice] = Field(min_length=1,max_length=64)
    delivery: str


class RatingValue(BaseModel):
    model_config = ConfigDict(extra='forbid',strict=True)
    rating_half_steps: int = Field(ge=1,le=10)


class FavoriteValue(BaseModel):
    model_config = ConfigDict(extra='forbid',strict=True)
    favorite: bool


class EmptyValue(BaseModel):
    model_config = ConfigDict(extra='forbid',strict=True)


class LibraryFacade:
    """All authorities must be explicitly constructed against the same engine."""
    def __init__(self, *, queries, policy, previews, downloads, members=None, recommendations=None, quota=None):
        self.queries,self.policy,self.previews,self.downloads = queries,policy,previews,downloads
        self.members,self.recommendations,self.quota = members,recommendations,quota
        self.auth,self.engine = policy.auth,policy.engine
        self.publisher = previews.publisher
        if (queries.auth is not self.auth or downloads.auth is not self.auth
                or previews.policy.auth is not self.auth or downloads.policy.auth is not self.auth
                or any(item.engine is not self.engine for item in (queries,previews,downloads,previews.policy,downloads.policy))
                or downloads.publisher is not self.publisher):
            raise ValueError('One library auth, engine and publisher required')
        if any(item is not None for item in (members,recommendations,quota,downloads.quota)):
            if (members is None or recommendations is None or quota is None
                    or queries.ranking is None or recommendations.queries is not queries
                    or members.auth is not self.auth or members.engine is not self.engine
                    or quota.auth is not self.auth or quota.engine is not self.engine
                    or members.quota is not quota or downloads.quota is not quota
                    or members.read_social_in != queries.read_social_in):
                raise ValueError('Explicit shared member, ranking, recommendation and quota authorities required')
        self.covers = CoverService(self.publisher,queries,admission=previews.admission)
        self.supervisor = StreamSupervisor(downloads=downloads,admissions=(previews.admission,downloads.admission))

    def recommend(self, token, query):
        self.quota.admit(token,action='recommend')
        with self.engine.connect() as conn, conn.begin():
            actor = self.auth.current(conn,token)
            preferences = self.members.preferences_in(conn,actor=actor)
            return self.recommendations.recommend_in(conn,actor=actor,query=query,
                preferences=preferences,as_of_ms=self.auth.now()*1000)

    def lazy(self, factory, request):
        return LazyOwnedResponse(factory,supervisor=self.supervisor,
            error_response=lambda error:error_response(error,request_id=getattr(request.state,'request_id',None)))

    def _options(self, conn, token, work, edition, revision, *, option_id=None, policy='current'):
        query = 'SELECT id FROM revision_options WHERE edition_revision_id=:revision'
        if option_id is not None:
            query += ' AND id=:option'
        rows = conn.execute(text(query+" ORDER BY CASE format WHEN 'txt' THEN 0 WHEN 'epub' THEN 1 WHEN 'pdf' THEN 2 ELSE 3 END,format,id LIMIT 65"),dict(revision=revision,option=option_id)).scalars().all()
        if len(rows)>64:
            raise AuthError('RESOURCE_STRUCTURE_LIMIT',413)
        resolved = []
        for ident in rows:
            try:
                selected = self.policy.resolve_selection_in(conn,token,(AssetSelection(revision,option_id=ident,work_id=work,edition_id=edition),),policy=policy)
            except AuthError as error:
                if option_id is not None or error.status not in {403,404,409}:
                    raise
                continue
            resolved.append(selected)
        return resolved

    def selection(self, token, work_id, values):
        work = _identifier(work_id)
        for key in ('edition_id','revision_id','download_option_id'):
            if key in values:
                _identifier(values[key])
        edition,revision,option = (values.get(key) for key in ('edition_id','revision_id','download_option_id'))
        if edition is None and revision is None and option is None:
            default = self.queries.detail(token,work,limit=1)['work']['default_option']
            edition,revision,option = default['edition_id'],default['revision_id'],default['option_id']
        with self.engine.connect() as conn, conn.begin():
            self.auth.current(conn,token)
            if revision is None:
                if edition is None:
                    raise InputError()
                revision = conn.execute(text('SELECT r.id FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id WHERE r.is_active=1 AND e.id=:edition AND e.work_id=:work'),dict(edition=edition,work=work)).scalar()
            else:
                bound = conn.execute(text('SELECT e.id FROM edition_revisions r JOIN work_editions e ON e.id=r.edition_id WHERE r.id=:revision AND e.work_id=:work'),dict(revision=revision,work=work)).scalar()
                if bound is None or (edition is not None and edition != bound):
                    raise AuthError('RESOURCE_NOT_FOUND',404)
                edition = bound
            if revision is None:
                raise AuthError('RESOURCE_NOT_FOUND',404)
            options = self._options(conn,token,work,edition,revision,option_id=option)
            if not options:
                raise AuthError('RESOURCE_NOT_FOUND',404)
            full = options[0]
            option = full.options[0].option_id
            positions = full.options[0].positions
            if 'asset_position' in values:
                position = _number(values['asset_position'])
                if position not in positions:
                    raise AuthError('ASSET_UNAVAILABLE',409)
            elif len(positions) == 1:
                position = positions[0]
            else:
                error = AuthError('ORIGINAL_SELECTION_REQUIRED',409)
                error.public_positions = positions
                raise error
        def resolve():
            # Full option check prevents a selected member hiding blocked peers.
            with self.engine.connect() as conn, conn.begin():
                self.policy.resolve_selection_in(conn,token,(AssetSelection(revision,option_id=option,work_id=work,edition_id=edition),))
                return self.policy.resolve_selection_in(conn,token,(AssetSelection(revision,positions=(position,),option_id=option,work_id=work,edition_id=edition),))
        return resolve

    def revisions(self, token, work_id, edition_id, *, cursor=None, limit=20):
        work,edition = _identifier(work_id),_identifier(edition_id)
        last = None
        if cursor is not None:
            data = decode_cursor(cursor,expected_kind='revisions')
            if (set(data) != {'work_id','edition_id','revision_no','id'} or data['work_id'] != work
                    or data['edition_id'] != edition or type(data['revision_no']) is not int or data['revision_no'] < 1):
                raise CursorError()
            last = (data['revision_no'],_identifier(data['id']))
        with self.engine.connect() as conn, conn.begin():
            actor = self.auth.current(conn,token)
            mode = 'admin_retained' if actor['role'] == 'admin' else 'current'
            rows = conn.execute(text('''SELECT r.id,r.revision_no,r.is_active,r.created_at FROM edition_revisions r
                JOIN work_editions e ON e.id=r.edition_id JOIN revision_seals s ON s.revision_id=r.id
                WHERE e.work_id=:work AND e.id=:edition AND (r.is_active=1 OR (:admin=1 AND s.retained=1))
                AND (:seek=0 OR r.revision_no<:number OR (r.revision_no=:number AND r.id<:id))
                ORDER BY r.revision_no DESC,r.id DESC'''),dict(work=work,edition=edition,admin=int(mode=='admin_retained'),seek=int(last is not None),number=last[0] if last else 0,id=last[1] if last else ''))
            items = []
            for row in rows.mappings():
                options = self._options(conn,token,work,edition,row['id'],policy=mode)
                if options:
                    items.append(dict(row,options=[item.options[0].public() for item in options]))
                    if len(items)>limit:
                        break
            if not items:
                # An empty continuation does not disclose hidden revision count.
                if cursor is not None:
                    return dict(work_id=work,edition_id=edition,revisions=[],next_cursor=None)
                raise AuthError('RESOURCE_NOT_FOUND',404)
            more = len(items)>limit
            items = items[:limit]
            tail = items[-1]
            next_cursor = encode_cursor('revisions',dict(work_id=work,edition_id=edition,revision_no=tail['revision_no'],id=tail['id'])) if more else None
            return dict(work_id=work,edition_id=edition,revisions=items,next_cursor=next_cursor)


def install_routes(app, library, credentials):
    def token(request):
        library.supervisor.admit()
        return credentials(request)[0]

    @app.get('/api/v1/items')
    def items(request: Request):
        fields = ('search','sort','direction','limit','cursor','unrated','favorite','has_summary','downloaded','has_gaps')
        values = _params(request,scalar=fields,repeated=('categories','states','formats','representations','genres'))
        for name in ('unrated','favorite','has_summary','downloaded','has_gaps'):
            if name in values:
                values[name] = _boolean(values[name])
        if 'limit' in values:
            values['limit'] = _number(values['limit'],low=1,high=100)
        try:
            query = LibraryQuery(**values)
        except (ValueError,TypeError):
            raise InputError() from None
        return library.queries.list(token(request),query).public()

    def detail(request, work_id):
        values = _params(request,scalar=('cursor','limit'))
        if 'limit' in values:
            values['limit'] = _number(values['limit'],low=1,high=50)
        return library.queries.detail(token(request),_identifier(work_id),**values)

    @app.get('/api/v1/items/{work_id}')
    def item(work_id: str, request: Request):
        return detail(request,work_id)

    @app.get('/api/v1/items/{work_id}/editions')
    def editions(work_id: str, request: Request):
        value = detail(request,work_id)
        default = value['work']['default_option']
        entries = []
        for edition in value['editions']:
            options = [dict(option,id=option['option_id'],kind='chapter_bundle' if option['delivery_kind']=='bundle' else 'single_file') for option in edition['options']]
            entries.append(dict(edition,download_options=options,default_download_option_id=options[0]['id'],is_default=edition['edition_id']==default['edition_id']))
        return dict(work_id=work_id,default_edition_id=default['edition_id'],default_download_option_id=default['option_id'],editions=entries,next_cursor=value['next_cursor'])

    @app.get('/api/v1/items/{work_id}/labels/{kind}')
    def labels(work_id: str, kind: str, request: Request):
        values = _params(request,scalar=('cursor','limit'))
        if 'limit' in values:
            values['limit'] = _number(values['limit'],low=1,high=64)
        return library.queries.labels(token(request),_identifier(work_id),kind,**values)

    @app.get('/api/v1/items/{work_id}/editions/{edition_id}/revisions')
    def revisions(work_id: str, edition_id: str, request: Request):
        values = _params(request,scalar=('cursor','limit'))
        if 'limit' in values:
            values['limit'] = _number(values['limit'],low=1,high=50)
        return library.revisions(token(request),work_id,edition_id,**values)

    def preview(request, work_id, *, kind='text', page=None):
        values = _params(request,scalar=('edition_id','revision_id','download_option_id','asset_position','cursor','chapter','thumbnail'))
        if (kind == 'chapters' and any(key in values for key in ('cursor','chapter','thumbnail'))
                or kind == 'image' and any(key in values for key in ('cursor','chapter'))
                or kind == 'text' and 'thumbnail' in values):
            raise InputError()
        current = token(request)
        def factory():
            resolve = library.selection(current,work_id,values)
            selected = resolve()
            range_value,if_range = request.headers.get('range'),request.headers.get('if-range')
            if kind == 'text' and selected.assets[0].file_format == 'pdf':
                if any(key in values for key in ('cursor','chapter','thumbnail')):
                    raise InputError()
                asset = selected.assets[0]
                parse_single_range(request.method,range_value,if_range,asset.byte_size,'"'+asset.object_sha+'"')
                return original_response(library.publisher,resolve,method=request.method,range_value=range_value,
                    if_range=if_range,inline=True,admission=library.previews.admission,on_created=library.supervisor.track)
            if range_value is not None or if_range is not None:
                raise InputError()
            kwargs = dict(kind=kind,method=request.method,on_created=library.supervisor.track)
            if 'cursor' in values:
                kwargs['cursor'] = values['cursor']
            if 'chapter' in values:
                kwargs['chapter'] = _number(values['chapter'])
            if 'thumbnail' in values:
                if kind != 'image':
                    raise InputError()
                kwargs['thumbnail'] = _boolean(values['thumbnail'])
            if page is not None:
                kwargs['page'] = _number(page,low=1,high=5000)
            return library.previews.response(resolve,**kwargs)
        return library.lazy(factory,request)

    @app.api_route('/api/v1/items/{work_id}/preview',methods=['GET','HEAD'])
    def text_preview(work_id: str, request: Request):
        return preview(request,work_id)

    @app.api_route('/api/v1/items/{work_id}/chapters',methods=['GET','HEAD'])
    def chapters(work_id: str, request: Request):
        return preview(request,work_id,kind='chapters')

    @app.api_route('/api/v1/items/{work_id}/pages/{page}',methods=['GET','HEAD'])
    def pages(work_id: str, page: str, request: Request):
        return preview(request,work_id,kind='image',page=page)

    @app.api_route('/api/v1/items/{work_id}/cover',methods=['GET','HEAD'])
    def cover(work_id: str, request: Request):
        values=_params(request,scalar=('version',))
        raw=values.get('version')
        if (not isinstance(raw,str) or not re.fullmatch(r'0|[1-9][0-9]{0,18}',raw)
                or int(raw)>9223372036854775807
                or request.headers.get('range') is not None or request.headers.get('if-range') is not None):
            raise InputError()
        work,current=_identifier(work_id),token(request)
        return library.lazy(lambda:library.covers.response(current,work,int(raw),method=request.method,
            on_created=library.supervisor.track),request)

    @app.api_route('/api/v1/items/{work_id}/download',methods=['GET','HEAD'])
    def download(work_id: str, request: Request):
        values = _params(request,scalar=('edition_id','revision_id','download_option_id','asset_position'))
        current = token(request)
        if 'revision_id' not in values or 'asset_position' not in values:
            raise InputError()
        def factory():
            work = _identifier(work_id)
            revision = _identifier(values['revision_id'])
            edition = _identifier(values['edition_id']) if 'edition_id' in values else None
            option = _identifier(values['download_option_id']) if 'download_option_id' in values else None
            selection = (AssetSelection(revision,positions=(_number(values['asset_position']),),option_id=option,work_id=work,edition_id=edition),)
            # This endpoint explicitly selects one original. Sibling members
            # remain independent; only tickets require an entire option.
            with library.engine.connect() as conn, conn.begin():
                selected = library.policy.resolve_selection_in(conn,current,selection)
            asset = selected.assets[0]
            range_value,if_range = request.headers.get('range'),request.headers.get('if-range')
            parse_single_range(request.method,range_value,if_range,asset.byte_size,'"'+asset.object_sha+'"')
            return library.downloads.original_download_response(current,selection,method=request.method,range_value=range_value,if_range=if_range)
        return library.lazy(factory,request)

    @app.post('/api/v1/items/{work_id}/download-selections',status_code=201)
    async def selections(work_id: str, body: DownloadSelections, request: Request):
        _params(request)
        if body.delivery != 'zip':
            raise InputError()
        work = _identifier(work_id)
        current = token(request)
        choice = tuple(AssetSelection(_identifier(item.revision_id),edition_id=_identifier(item.edition_id),option_id=_identifier(item.download_option_id),work_id=work) for item in body.selection)
        csrf = credentials(request)[1]
        try:
            snapshot = await library.supervisor.run_call(lambda:library.downloads.issue(current,csrf,work,choice))
        except AuthError as error:
            if error.status == 404:
                raise AuthError('ASSET_UNAVAILABLE',409) from None
            raise
        expiry = datetime.fromtimestamp(snapshot.expires_at_ms/1000,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        return dict(ticket_id=snapshot.ticket_id,expires_at=expiry,download_url='/api/v1/download-tickets/'+snapshot.ticket_id)

    @app.api_route('/api/v1/download-tickets/{ticket_id}',methods=['GET','HEAD'])
    def ticket(ticket_id: str, request: Request):
        _params(request)
        ident,current = _identifier(ticket_id),token(request)
        if request.headers.get('range') is not None or request.headers.get('if-range') is not None:
            raise InputError()
        return library.lazy(lambda:library.downloads.ticket_response(current,ident,method=request.method),request)

    if library.members is None:
        return

    @app.put('/api/v1/items/{work_id}/rating')
    async def rating(work_id: str, body: RatingValue, request: Request):
        _params(request)
        current,csrf = token(request),credentials(request)[1]
        work = _identifier(work_id)
        return await library.supervisor.run_call(lambda:library.members.set_rating(current,csrf,work,body.rating_half_steps))

    @app.delete('/api/v1/items/{work_id}/rating')
    async def delete_rating(work_id: str, body: EmptyValue, request: Request):
        _params(request)
        current,csrf = token(request),credentials(request)[1]
        work = _identifier(work_id)
        return await library.supervisor.run_call(lambda:library.members.delete_rating(current,csrf,work))

    @app.put('/api/v1/items/{work_id}/favorite')
    async def favorite(work_id: str, body: FavoriteValue, request: Request):
        _params(request)
        current,csrf = token(request),credentials(request)[1]
        work = _identifier(work_id)
        return await library.supervisor.run_call(lambda:library.members.set_favorite(current,csrf,work,body.favorite))

    @app.get('/api/v1/me/preferences')
    async def preferences(request: Request):
        _params(request)
        current = token(request)
        return await library.supervisor.run_call(lambda:library.members.get_preferences(current))

    @app.put('/api/v1/me/preferences')
    async def save_preferences(body: dict, request: Request):
        _params(request)
        current,csrf = token(request),credentials(request)[1]
        return await library.supervisor.run_call(lambda:library.members.set_preferences(current,csrf,body))

    @app.get('/api/v1/recommendations')
    async def recommendations(request: Request):
        values = _params(request,scalar=('mode','limit','exclude_rated','exclude_downloaded'),
            repeated=('categories','states','formats','representations','genres'))
        for name in ('exclude_rated','exclude_downloaded'):
            if name in values:
                values[name] = _boolean(values[name])
        if 'limit' in values:
            values['limit'] = _number(values['limit'],low=1,high=50)
        for name in ('categories','states','formats','representations','genres'):
            if name in values and '' in values[name]:
                if values[name] != ('',):
                    raise InputError()
                values[name] = ()
        try:
            query = RecommendationQuery(**values)
        except (ValueError,TypeError):
            raise InputError() from None
        current = token(request)
        return await library.supervisor.run_call(lambda:library.recommend(current,query))
