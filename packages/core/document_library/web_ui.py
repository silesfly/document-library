"""Explicit, immutable UI asset serving; never mount a source or data directory."""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat

from fastapi import Request
from starlette.responses import Response

from .storage import _open_absolute_directory

CSP = ("default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; "
       "img-src 'self' blob:; font-src 'self'; connect-src 'self'; worker-src 'self'; "
       "manifest-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
       "form-action 'self'")
_TYPES = {'html': 'text/html; charset=utf-8', 'js': 'text/javascript; charset=utf-8',
          'mjs': 'text/javascript; charset=utf-8', 'css': 'text/css; charset=utf-8',
          'svg': 'image/svg+xml', 'webmanifest': 'application/manifest+json',
          'json': 'application/json', 'bcmap': 'application/octet-stream',
          'pfb': 'application/octet-stream', 'ttf': 'font/ttf', 'wasm': 'application/wasm',
          'txt': 'text/plain; charset=utf-8', 'icc': 'application/octet-stream'}
_MAX_ASSET = 16 * 1024 * 1024


def _relative(value):
    if not isinstance(value, str):
        raise ValueError('Invalid static asset path')
    path = PurePosixPath(value)
    if (not value or path.is_absolute()
            or any(part in {'', '.', '..'} for part in value.split('/'))
            or '\\' in value or '%' in value or '\x00' in value):
        raise ValueError('Invalid static asset path')
    return path.parts


def _read_asset(root, relative):
    parts = _relative(relative)
    with ExitStack() as stack:
        fd = _open_absolute_directory(root)
        stack.callback(os.close, fd)
        for index, part in enumerate(parts):
            flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if index < len(parts) - 1:
                flags |= os.O_DIRECTORY
            fd = os.open(part, flags, dir_fd=fd)
            stack.callback(os.close, fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_ASSET:
            raise ValueError('Invalid static asset file')
        data = bytearray()
        while block := os.read(fd, min(65536, _MAX_ASSET + 1 - len(data))):
            data.extend(block)
            if len(data) > _MAX_ASSET:
                raise ValueError('Static asset too large')
        return bytes(data)


def install_ui(app, *, asset_root, manifest_path='asset-manifest.json'):
    """Install only an external build's frozen allowlist. No API/auth changes."""
    root = Path(asset_root)
    if not root.is_absolute() or '..' in root.parts:
        raise ValueError('An absolute dedicated asset root is required')
    raw = _read_asset(root, manifest_path)
    if len(raw) > 512 * 1024:
        raise ValueError('Static manifest too large')
    manifest = json.loads(raw)
    if manifest.get('schema') != 'novel-library-web-assets-v1':
        raise ValueError('Unsupported UI manifest')
    entries = manifest.get('assets')
    if not isinstance(entries, dict) or not 1 <= len(entries) <= 1024:
        raise ValueError('Invalid UI manifest entries')
    allowed = {}
    for name, record in entries.items():
        _relative(name)
        suffix = name.rsplit('.', 1)[-1]
        if suffix not in _TYPES or not isinstance(record, dict):
            raise ValueError('Unsupported static asset')
        data = _read_asset(root, name)
        actual = hashlib.sha256(data).hexdigest()
        if record.get('sha256') != actual or record.get('bytes') != len(data):
            raise ValueError('UI asset integrity mismatch')
        allowed[name] = (actual, _TYPES[suffix])
    if not {'index.html', 'offline.html', 'sw.js', 'manifest.webmanifest'} <= allowed.keys():
        raise ValueError('Missing UI entry assets')

    def reply(name, request):
        headers = {'Content-Security-Policy': CSP, 'X-Content-Type-Options': 'nosniff',
                   'Referrer-Policy': 'no-referrer', 'Cache-Control': 'no-store'}
        if name not in allowed:
            return Response('Not found', status_code=404, headers=headers)
        try:
            data = _read_asset(root, name)
            expected, media = allowed[name]
            if hashlib.sha256(data).hexdigest() != expected:
                raise ValueError('Changed static asset')
        except (OSError, ValueError):
            return Response('Asset unavailable', status_code=503, headers=headers)
        headers['Content-Length'] = str(len(data))
        if name == 'sw.js':
            headers['Service-Worker-Allowed'] = '/'
        return Response(b'' if request.method == 'HEAD' else data, media_type=media, headers=headers)

    @app.api_route('/', methods=['GET', 'HEAD'], include_in_schema=False)
    @app.api_route('/ui', methods=['GET', 'HEAD'], include_in_schema=False)
    def ui_entry(request: Request):
        return reply('index.html', request)

    @app.api_route('/offline.html', methods=['GET', 'HEAD'], include_in_schema=False)
    def ui_offline(request: Request):
        return reply('offline.html', request)

    @app.api_route('/sw.js', methods=['GET', 'HEAD'], include_in_schema=False)
    def ui_worker(request: Request):
        return reply('sw.js', request)

    @app.api_route('/manifest.webmanifest', methods=['GET', 'HEAD'], include_in_schema=False)
    def ui_manifest(request: Request):
        return reply('manifest.webmanifest', request)

    @app.api_route('/assets/{asset_path:path}', methods=['GET', 'HEAD'], include_in_schema=False)
    def ui_asset(asset_path: str, request: Request):
        # Entry HTML and service-worker are available only at their explicit routes.
        if asset_path in {'index.html', 'offline.html', 'sw.js', 'manifest.webmanifest'}:
            return Response('Not found', status_code=404)
        return reply(asset_path, request)

    app.state.ui_asset_manifest = {name: value[0] for name, value in allowed.items()}
