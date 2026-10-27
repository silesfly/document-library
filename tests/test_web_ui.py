"""Dedicated UI root boundaries over disposable public synthetic assets."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from document_library.web_ui import install_ui, CSP


class WebAssetBoundary(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='novel-library-ui-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'static'
        self.root.mkdir()
        self.files = {'index.html': b'<html>Public login shell</html>',
                      'offline.html': b'<html>Offline public shell</html>',
                      'sw.js': b'/* public static worker */',
                      'manifest.webmanifest': b'{"name":"Synthetic"}',
                      'app.js': b'export const publicShell = true;',
                      'vendor/fonts/font.ttf': b'synthetic public font'}
        for name, data in self.files.items():
            destination = self.root / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
        self.manifest = {'schema': 'novel-library-web-assets-v1', 'assets': {
            name: {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
            for name, data in self.files.items()}}
        self.save()

    def save(self):
        (self.root / 'asset-manifest.json').write_text(json.dumps(self.manifest))

    def client(self, **kwargs):
        app = FastAPI()
        install_ui(app, asset_root=self.root, **kwargs)
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def test_explicit_public_shell_and_asset_routes_get_head_security(self):
        client = self.client()
        for path, data in [('/', self.files['index.html']), ('/ui', self.files['index.html']),
                           ('/offline.html', self.files['offline.html']),
                           ('/assets/app.js', self.files['app.js'])]:
            with self.subTest(path=path):
                response = client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, data)
                self.assertEqual(response.headers['content-security-policy'], CSP)
                self.assertEqual(response.headers['cache-control'], 'no-store')
                self.assertEqual(response.headers['referrer-policy'], 'no-referrer')
                self.assertEqual(response.headers['x-content-type-options'], 'nosniff')
                head = client.head(path)
                self.assertEqual(head.content, b'')
                self.assertEqual(int(head.headers['content-length']), len(data))
        self.assertEqual(client.get('/sw.js').headers['service-worker-allowed'], '/')
        self.assertNotIn("'unsafe-eval'", CSP)
        self.assertNotIn('https:', CSP)

    def test_existing_exact_https_host_and_origin_boundary_remains_active(self):
        from types import SimpleNamespace
        from document_library.api import create_app
        service = SimpleNamespace(engine=object())
        origin = 'https://library.synthetic.invalid'
        with TestClient(create_app(service, origin=origin), base_url=origin) as api_only:
            self.assertEqual(api_only.get('/').status_code, 404)
        app = create_app(service, origin=origin, ui_asset_root=self.root)
        with TestClient(app, base_url='https://library.synthetic.invalid') as client:
            self.assertEqual(client.get('/').status_code, 200)
            rejected_host = client.get('/', headers={'Host': 'outside.invalid'})
            self.assertEqual(rejected_host.status_code, 400)
            rejected_origin = client.post('/auth/login', json={'username': 'synthetic'},
                                          headers={'Origin': 'https://outside.invalid'})
            self.assertEqual(rejected_origin.status_code, 403)
            self.assertEqual(client.head('/assets/app.js').status_code, 200)
        (self.root / 'app.js').write_bytes(b'changed before factory startup')
        with self.assertRaises(ValueError):
            create_app(service, origin=origin, ui_asset_root=self.root)

    def test_no_mount_source_fallback_manifest_or_private_routes(self):
        (self.root / 'private.sqlite3').write_bytes(b'private-not-admitted')
        client = self.client()
        for path in ['/assets/private.sqlite3', '/assets/asset-manifest.json', '/asset-manifest.json',
                     '/assets/index.html', '/assets/sw.js', '/assets/%2e%2e/private.sqlite3',
                     '/assets/vendor/%2e%2e/%2e%2e/private.sqlite3', '/api/v1/items',
                     '/storage/file.txt', '/ops/mcp/config.json', '/auth/login', '/missing']:
            with self.subTest(path=path):
                response = client.get(path)
                self.assertEqual(response.status_code, 404)
                self.assertNotIn(b'private-not-admitted', response.content)

    def test_runtime_tamper_and_leaf_parent_symlinks_fail_closed(self):
        client = self.client()
        (self.root / 'app.js').write_bytes(b'changed after startup')
        self.assertEqual(client.get('/assets/app.js').status_code, 503)
        (self.root / 'app.js').unlink()
        outside = Path(self.temp.name) / 'outside.js'
        outside.write_bytes(self.files['app.js'])
        (self.root / 'app.js').symlink_to(outside)
        self.assertEqual(client.get('/assets/app.js').status_code, 503)
        font = self.root / 'vendor/fonts/font.ttf'
        original = font.read_bytes()
        font.unlink()
        font.parent.rmdir()
        external = Path(self.temp.name) / 'external-fonts'
        external.mkdir()
        (external / 'font.ttf').write_bytes(original)
        font.parent.symlink_to(external, target_is_directory=True)
        self.assertEqual(client.get('/assets/vendor/fonts/font.ttf').status_code, 503)

    def test_startup_rejects_unsafe_manifest_and_changed_asset(self):
        for name in ['../escape.js', '/absolute.js', 'vendor/../escape.js', 'a\\b.js', 'a%2fb.js']:
            with self.subTest(name=name):
                original = self.manifest['assets']
                self.manifest['assets'] = {**original, name: {'sha256': '0' * 64, 'bytes': 0}}
                self.save()
                with self.assertRaises((ValueError, OSError)):
                    self.client()
                self.manifest['assets'] = original
        self.save()
        (self.root / 'app.js').write_bytes(b'changed')
        with self.assertRaises(ValueError):
            self.client()

    def test_root_and_manifest_symlinks_rejected_without_reading_target(self):
        alias = Path(self.temp.name) / 'alias'
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            install_ui(FastAPI(), asset_root=alias)
        manifest = self.root / 'asset-manifest.json'
        content = manifest.read_bytes()
        manifest.unlink()
        target = Path(self.temp.name) / 'manifest.json'
        target.write_bytes(content)
        manifest.symlink_to(target)
        with self.assertRaises(OSError):
            self.client()


    def test_close_failure_keeps_child_owned_and_never_retries_stale_descriptor(self):
        import errno
        import os
        from unittest.mock import patch
        from document_library import web_ui
        original_open, original_close = os.open, os.close
        for released_before_error in (False, True):
            with self.subTest(released_before_error=released_before_error):
                descriptors, closed, attempts = [], set(), {}
                root_fd = original_open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                descriptors.append(root_fd)
                failed = False
                def opened(*args, **kwargs):
                    descriptor = original_open(*args, **kwargs)
                    descriptors.append(descriptor)
                    return descriptor
                def closing(descriptor):
                    nonlocal failed
                    attempts[descriptor] = attempts.get(descriptor, 0) + 1
                    if descriptor == root_fd and not failed:
                        failed = True
                        if released_before_error:
                            original_close(descriptor)
                            closed.add(descriptor)
                        raise OSError(errno.EIO, 'Synthetic one-shot close failure')
                    original_close(descriptor)
                    closed.add(descriptor)
                try:
                    with patch.object(web_ui, '_open_absolute_directory', return_value=root_fd), \
                            patch.object(web_ui.os, 'open', opened), \
                            patch.object(web_ui.os, 'close', closing):
                        with self.assertRaisesRegex(OSError, 'Synthetic one-shot close failure'):
                            web_ui._read_asset(self.root, 'vendor/fonts/font.ttf')
                    self.assertTrue(failed)
                    self.assertGreater(len(descriptors), 1)
                    self.assertTrue(set(descriptors[1:]) <= closed,
                                    'A successfully opened child escaped cleanup')
                    self.assertTrue(all(count == 1 for count in attempts.values()),
                                    'Cleanup retried a possibly stale descriptor')
                    for descriptor in descriptors[1:]:
                        with self.assertRaises(OSError):
                            os.fstat(descriptor)
                finally:
                    for descriptor in descriptors:
                        if descriptor not in closed:
                            original_close(descriptor)


if __name__ == '__main__':
    unittest.main()
