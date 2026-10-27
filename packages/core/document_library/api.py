"""Authentication API factory. No implicit database creation or server startup."""

from urllib.parse import urlsplit
from typing import Literal, Annotated
import re
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from .auth import AuthError, AuthService
from .permissions import PermissionService, PermissionConflict

BULK_PATHS = {"/admin/user-permissions/bulk-preview", "/admin/user-permissions/bulk",
              "/admin/users/bulk-status-preview", "/admin/users/bulk-status"}

SESSION_COOKIE = "__Host-dl-session"
CSRF_COOKIE = "__Host-dl-csrf"


def _versioned_path(path):
    return path == "/api/v1" or path.startswith("/api/v1/")


class AuthBoundary:
    """Bound JSON before parsing, enforce a fixed host and same-origin writes."""
    def __init__(self, app, *, origin, tus_enabled=False):
        self.app = app
        self.origin = origin
        self.host = urlsplit(origin).netloc
        self.tus_enabled=tus_enabled

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        versioned = _versioned_path(scope["path"])
        if versioned:
            scope.setdefault("state", {})["request_id"] = str(uuid.uuid4())
        async def rejected(code, status):
            if versioned:
                from .library_api import error_response
                return await error_response(AuthError(code,status),request_id=scope["state"]["request_id"])(scope,receive,secure_send)
            return await JSONResponse({"error":code},status_code=status)(scope,receive,secure_send)
        async def secure_send(message):
            if message['type']=='http.response.start':
                message['headers']=list(message.get('headers',[]))+[
                    (b'cache-control',b'no-store'),(b'x-content-type-options',b'nosniff'),
                    (b'referrer-policy',b'no-referrer')]
            await send(message)
        headers = {key.lower(): value for key, value in scope["headers"]}
        error = None
        protected={b'host',b'origin',b'cookie',b'x-csrf-token',b'content-type',b'content-length',b'transfer-encoding'}
        if versioned:
            protected |= {b'range', b'if-range'}
        keys=[key.lower() for key,_value in scope['headers']]
        if any(keys.count(key)>1 for key in protected):
            error=(400,'duplicate_header')
        if headers.get(b"host", b"").decode("latin-1") != self.host:
            error = (400, "invalid_host")
        mutation = scope["method"] not in {"GET", "HEAD", "OPTIONS"}
        if mutation and headers.get(b"origin", b"").decode("latin-1") != self.origin:
            error = (403, "origin_rejected")
        tus=self.tus_enabled and re.fullmatch(r'/tus/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',scope['path']) is not None
        if mutation and not tus and headers.get(b"content-type", b"").split(b";")[0].strip().lower() != b"application/json":
            error = (415, "json_required") if not error else error
        if error:
            return await rejected(error[1],error[0])
        if mutation and not tus:
            selection = versioned and re.fullmatch(r'/api/v1/items/[^/]{1,200}/download-selections',scope['path']) is not None
            # 64 selections, each with three 200-character public IDs.
            body_limit = 65536 if selection else 32768 if scope["path"] in BULK_PATHS else 8192
            body = bytearray()
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                body.extend(message.get("body", b""))
                if len(body) > body_limit:
                    return await rejected("body_too_large",413)
                if not message.get("more_body", False):
                    break
            original_receive = receive
            sent = False

            async def bounded_receive():
                nonlocal sent
                if not sent:
                    sent = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await original_receive()

            receive = bounded_receive

        await self.app(scope, receive, secure_send)


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Login(Body):
    username: str = Field(min_length=3, max_length=64)
    password: SecretStr = Field(max_length=256)


class Register(Login):
    invite: str = Field(min_length=32, max_length=128)


class ChangePassword(Body):
    current_password: SecretStr = Field(max_length=256)
    new_password: SecretStr = Field(min_length=12, max_length=256)


class ResetPassword(Body):
    token: str = Field(min_length=32, max_length=128)
    new_password: SecretStr = Field(min_length=12, max_length=256)


Identifier = Annotated[str, Field(min_length=1, max_length=64)]
Version = Annotated[int, Field(strict=True, ge=1)]


class Selection(Body):
    search: str = Field(default="", max_length=64)
    state: Literal["all", "active", "suspended", "upload", "no_upload"] = "all"


class BulkPreview(Body):
    permission: Literal["content.upload"] = "content.upload"
    action: Literal["grant", "revoke", "suspend", "activate"]
    reason: str = Field(min_length=1, max_length=500)
    user_ids: list[Identifier] | None = Field(default=None, min_length=1, max_length=200)
    expected_authz_versions: dict[Identifier, Version] | None = Field(default=None, max_length=200)
    selection: Selection | None = None


class BulkCommit(Body):
    admin_password: SecretStr = Field(min_length=1, max_length=256)
    permission: Literal["content.upload"] = "content.upload"
    preview_token: str = Field(min_length=32, max_length=128)
    action: Literal["grant", "revoke", "suspend", "activate"]
    reason: str = Field(min_length=1, max_length=500)
    confirm_target_count: int = Field(strict=True, ge=1, le=200)
    confirmed: bool = Field(strict=True)


class UploadBatch(Body):
    declared_total_bytes: int = Field(strict=True,ge=1,le=10*1024**3)
    request_key: str = Field(min_length=1,max_length=128)


class UploadFile(Body):
    filename: str = Field(min_length=1,max_length=255)
    declared_size: int = Field(strict=True,ge=1,le=2*1024**3)
    request_key: str = Field(min_length=1,max_length=128)


def create_app(service: AuthService, *, origin: str, uploads=None, tus=None, library=None, ingestion=None, metadata=None, ui_asset_root=None) -> FastAPI:
    url = urlsplit(origin)
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.path or url.query or url.fragment:
        raise ValueError("An exact HTTPS origin without path or credentials is required")
    if library is not None and (library.auth is not service or library.engine is not service.engine):
        raise ValueError("Shared library authorization service required")
    if ingestion is not None and (library is None or uploads is None
            or ingestion.auth is not service or ingestion.engine is not service.engine
            or ingestion.uploads is not uploads or ingestion.publisher is not library.publisher):
        raise ValueError('Shared explicit workflow services required')
    if metadata is not None and (library is None or metadata.engine is not service.engine):
        raise ValueError('Shared metadata authority service required')
    app = FastAPI(title="Document Library", docs_url=None, redoc_url=None, openapi_url=None,
                  lifespan=library.supervisor.lifespan if library is not None else None)
    app.add_middleware(AuthBoundary, origin=origin,tus_enabled=tus is not None)
    permissions = PermissionService(service)
    if tus is not None and (uploads is None or tus.uploads is not uploads):
        raise ValueError('Shared upload service required')

    @app.exception_handler(AuthError)
    async def auth_error(_request, exc):
        if _versioned_path(_request.url.path):
            from .library_api import error_response
            return error_response(exc,request_id=getattr(_request.state,"request_id",None))
        body = {"error": exc.code}
        if isinstance(exc, PermissionConflict):
            body["targets"] = exc.targets
        return JSONResponse(body, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request, _exc):
        if _versioned_path(_request.url.path):
            from .library_api import error_response, InputError
            return error_response(InputError(),request_id=getattr(_request.state,"request_id",None))
        # FastAPI's default error includes raw input; never echo credential fields.
        return JSONResponse({"error": "invalid_input"}, status_code=422)

    def credentials(request):
        return request.cookies.get(SESSION_COOKIE, ""), request.headers.get("x-csrf-token", "")

    def client_ip(request):
        # Deployment must configure trusted proxy IPs explicitly; no XFF parsing here.
        return request.client.host if request.client else "unknown"

    def cleared():
        response = JSONResponse({"ok": True})
        for name, private in ((SESSION_COOKIE, True), (CSRF_COOKIE, False)):
            response.delete_cookie(name, path="/", secure=True, httponly=private, samesite="strict")
        return response

    @app.post("/auth/login")
    def login(body: Login, request: Request):
        session = service.login(body.username, body.password.get_secret_value(), ip=client_ip(request))
        response = JSONResponse({"ok": True})
        for name, value, private in ((SESSION_COOKIE, session.token, True), (CSRF_COOKIE, session.csrf, False)):
            response.set_cookie(name, value, max_age=service.SESSION_SECONDS, path="/",
                                secure=True, httponly=private, samesite="strict")
        return response

    @app.get("/me")
    def me(request: Request):
        return service.me(request.cookies.get(SESSION_COOKIE, ""))

    @app.post("/auth/logout")
    def logout(request: Request):
        service.logout(*credentials(request))
        return cleared()

    @app.post("/admin/invites")
    def invite(request: Request):
        return {"token": service.issue_token(*credentials(request)), "expires_in": 86400}

    @app.post("/auth/register-invite", status_code=201)
    def register(body: Register, request: Request):
        service.limit_public_request(client_ip(request))
        ident = service.register(body.invite, body.username, body.password.get_secret_value())
        return {"id": ident}

    @app.post("/auth/change-password")
    def change_password(body: ChangePassword, request: Request):
        service.change_password(*credentials(request), body.current_password.get_secret_value(), body.new_password.get_secret_value())
        return cleared()

    @app.post("/admin/users/{user_id}/password-reset")
    def issue_reset(user_id: str, request: Request):
        return {"token": service.issue_token(*credentials(request), reset_user=user_id), "expires_in": 900}

    @app.post("/auth/reset-password")
    def reset_password(body: ResetPassword, request: Request):
        service.limit_public_request(client_ip(request))
        service.reset_password(body.token, body.new_password.get_secret_value())
        return cleared()

    @app.get("/admin/users")
    def users(request: Request, search: str = "", state: str = "all", limit: int = 50, offset: int = 0):
        return permissions.list_users(request.cookies.get(SESSION_COOKIE, ""), search=search, state=state, limit=limit, offset=offset)

    def check_action(request, action):
        status_path = "/users/" in request.url.path
        if (action in {"suspend", "activate"}) != status_path:
            raise AuthError("invalid_action", 400)

    @app.post("/admin/user-permissions/bulk-preview")
    @app.post("/admin/users/bulk-status-preview")
    def bulk_preview(body: BulkPreview, request: Request):
        check_action(request, body.action)
        return permissions.preview(*credentials(request), **body.model_dump(exclude={"permission"}))

    @app.post("/admin/user-permissions/bulk")
    @app.post("/admin/users/bulk-status")
    def bulk_commit(body: BulkCommit, request: Request):
        check_action(request, body.action)
        return permissions.commit(*credentials(request), **body.model_dump(exclude={"permission", "admin_password"}),
            admin_password=body.admin_password.get_secret_value(), idempotency_key=request.headers.get("idempotency-key", ""), client=client_ip(request)[:200])

    if uploads is not None:
        if uploads.engine is not service.engine:
            raise ValueError('Shared upload authorization engine required')

        @app.post('/uploads/batches',status_code=201)
        def upload_batch(body: UploadBatch, request: Request):
            return uploads.create_batch(*credentials(request),**body.model_dump())

        @app.post('/uploads/batches/{batch_id}/files',status_code=201)
        def upload_file(batch_id: str, body: UploadFile, request: Request):
            return uploads.reserve_file(*credentials(request),batch_id=batch_id,**body.model_dump())

    if tus is not None:
        @app.api_route('/tus/{upload_id}',methods=['POST','HEAD','PATCH','DELETE','GET','OPTIONS','PUT'])
        async def tus_request(upload_id: str, request: Request):
            return await tus.handle(request,upload_id,*credentials(request))

    if library is not None:
        from starlette.exceptions import HTTPException
        from fastapi.exception_handlers import http_exception_handler
        from .library_api import install_routes, error_response

        @app.exception_handler(HTTPException)
        async def library_http_error(request, error):
            if _versioned_path(request.url.path):
                code = "RESOURCE_NOT_FOUND" if error.status_code == 404 else "METHOD_NOT_ALLOWED" if error.status_code == 405 else "INVALID_INPUT"
                return error_response(AuthError(code,error.status_code),request_id=getattr(request.state,"request_id",None))
            return await http_exception_handler(request,error)

        @app.exception_handler(Exception)
        async def library_internal_error(request, error):
            if _versioned_path(request.url.path):
                return error_response(error,request_id=getattr(request.state,"request_id",None))
            from starlette.responses import PlainTextResponse
            return PlainTextResponse("Internal Server Error",status_code=500)

        from .cursors import CursorError

        @app.exception_handler(CursorError)
        async def library_cursor_error(request, error):
            if not _versioned_path(request.url.path):
                raise error
            return error_response(error,request_id=getattr(request.state,"request_id",None))

        from .activity_reads import ActivityReads
        from .library_api import _params, _number
        activity = ActivityReads(service,permissions=permissions)

        def read_page(request, default):
            values = _params(request,scalar=('cursor','limit'))
            if 'limit' in values:
                values['limit'] = _number(values['limit'],low=1,high=100)
            values.setdefault('limit',default)
            library.supervisor.admit()
            return values

        @app.get('/api/v1/upload-batches/{batch_id}')
        def upload_batch_status(batch_id: str, request: Request):
            values = read_page(request,20)
            return activity.batch(credentials(request)[0],batch_id,**values)

        @app.get('/api/v1/admin/users/{user_id}/activity')
        def user_activity(user_id: str, request: Request):
            _params(request)
            library.supervisor.admit()
            return activity.user(credentials(request)[0],user_id)

        @app.get('/api/v1/admin/permission-audit')
        def permission_audit(request: Request):
            values = read_page(request,50)
            return activity.permission_audit(credentials(request)[0],**values)

        install_routes(app,library,credentials)
        app.state.library = library
        if ingestion is not None:
            from .workflow_api import install_workflow_routes
            install_workflow_routes(app,ingestion,credentials,library.supervisor)
        if metadata is not None:
            from .metadata_admin_api import install_metadata_admin_routes
            install_metadata_admin_routes(app,metadata,credentials)

    if ui_asset_root is not None:
        from .web_ui import install_ui
        install_ui(app, asset_root=ui_asset_root)
    return app
