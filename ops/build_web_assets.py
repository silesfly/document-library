"""Offline deterministic browser asset builder. Does not install or execute packages.

Supply the two pinned official npm archives and a NEW external output directory.
Generated vendor assets are runtime artifacts, not text-only source-export inputs.
Trusted operator-owned source/output ancestors must remain namespace-stable for
the entire build. The supported mount layout must not expose a protected source
or repository subtree through another path used by the output, including a
pre-existing, stable bind mount. Root identity checks cover ordinary paths and
case-spelling aliases under this condition, not general mount-aware confinement.
Malicious same-credential directory rename and arbitrary bind-mount layouts or
replacement are outside this builder's isolation boundary.
"""
from __future__ import annotations
import argparse
import base64
import io
import hashlib
import json
import os
import stat
from contextlib import ExitStack, contextmanager
from pathlib import Path, PurePosixPath
import tarfile

SOURCE_FILES = ('index.html', 'offline.html', 'styles.css', 'icon.svg', 'manifest.webmanifest',
                'app.js', 'api.js', 'config.js', 'dom.js', 'library.js', 'readers.js',
                'transfers.js', 'admin.js', 'auth.js', 'catalog.js', 'sw.js',
                'cards.js', 'filters.js', 'member.js', 'metadata.js')
VERSIONS = {'pdfjs-dist': '6.4.299', 'tus-js-client': '4.3.1'}


@contextmanager
def _directory_fd(path):
    path = Path(path)
    if path.anchor != '/' or '..' in path.parts:
        raise ValueError('Absolute canonical directory required')
    with ExitStack() as stack:
        descriptor = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        stack.callback(os.close, descriptor)
        for part in path.parts[1:]:
            descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                 dir_fd=descriptor)
            stack.callback(os.close, descriptor)
        yield descriptor


def _regular(path):
    path = Path(path)
    try:
        with _directory_fd(path.parent) as parent:
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                 dir_fd=parent)
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_size > 20 * 1024 * 1024:
                    raise ValueError('Regular bounded input file required')
                data = bytearray()
                while block := os.read(descriptor, min(65536, 20 * 1024 * 1024 + 1 - len(data))):
                    data.extend(block)
                    if len(data) > 20 * 1024 * 1024:
                        raise ValueError('Input file exceeds limit')
                return bytes(data)
            finally:
                os.close(descriptor)
    except OSError as error:
        raise ValueError('Regular non-symlink input required') from error


def _write_output(root_fd, name, data):
    parts = PurePosixPath(name).parts
    if (not parts or PurePosixPath(name).is_absolute() or '\\' in name
            or any(part in {'', '.', '..'} for part in name.split('/'))):
        raise ValueError('Invalid output asset name')
    with ExitStack() as stack:
        parent = os.dup(root_fd)
        stack.callback(os.close, parent)
        for part in parts[:-1]:
            try:
                os.mkdir(part, mode=0o700, dir_fd=parent)
            except FileExistsError:
                pass
            parent = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                             dir_fd=parent)
            stack.callback(os.close, parent)
        descriptor = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | os.O_NOFOLLOW | os.O_NONBLOCK, 0o644, dir_fd=parent)
        stack.callback(os.close, descriptor)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('Output must be a regular file')
        remaining = memoryview(data)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError('Incomplete output write')
            remaining = remaining[written:]


def _same_output(parent_fd, name, identity):
    info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino) != identity:
        raise ValueError('Output directory identity changed')


def _member(name):
    parts = PurePosixPath(name).parts
    if (not name.startswith('package/') or '\\' in name or '%' in name
            or any(x in {'', '.', '..'} for x in name.split('/'))
            or PurePosixPath(name).is_absolute()):
        raise ValueError('Unsafe archive member')
    return parts


def _archive(path, spec, name):
    raw = _regular(path)
    if len(raw) > 20 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != spec['sha256']:
        raise ValueError('Vendor archive SHA256 mismatch')
    if 'sha512-' + base64.b64encode(hashlib.sha512(raw).digest()).decode() != spec['integrity']:
        raise ValueError('Vendor archive SHA512 mismatch')
    selected = {}
    seen = set()
    total = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        members = archive.getmembers()
        if len(members) > 2048:
            raise ValueError('Archive member limit')
        for member in members:
            _member(member.name)
            if member.name in seen or not member.isfile() or member.size > 16 * 1024 * 1024:
                raise ValueError('Duplicate or nonregular archive member')
            seen.add(member.name)
            total += member.size
            if total > 192 * 1024 * 1024:
                raise ValueError('Uncompressed archive limit')
            relative = member.name.removeprefix('package/')
            take = (relative == 'package.json' or relative == 'LICENSE')
            if name == 'pdfjs-dist':
                take |= relative in {'build/pdf.mjs', 'build/pdf.worker.mjs'}
                take |= relative.startswith(('cmaps/', 'standard_fonts/', 'wasm/', 'iccs/'))
            else:
                take |= relative == 'dist/tus.min.js'
            if take:
                stream = archive.extractfile(member)
                data = stream.read(member.size + 1)
                if len(data) != member.size:
                    raise ValueError('Archive member size mismatch')
                selected[relative] = data
    metadata = json.loads(selected.pop('package.json'))
    if metadata['name'] != name or metadata['version'] != VERSIONS[name] or spec['version'] != VERSIONS[name]:
        raise ValueError('Pinned vendor version mismatch')
    return selected


def build(source, archives, output, *, brand='private', repository_root=None):
    source, archives, output = map(Path, (source, archives, output))
    if brand not in {'private', 'public'}:
        raise ValueError('Unknown product branding')
    if (output.anchor != '/' or '..' in output.parts or output.exists()
            or output.is_symlink() or any(p.is_symlink() for p in output.parents)):
        raise ValueError('A new absolute external output directory is required')
    try:
        with _directory_fd(source):
            pass
    except OSError as error:
        raise ValueError('Source must be an absolute physical directory') from error
    resolved_source = source.resolve(strict=True)
    project_root = Path(__file__).resolve().parent.parent
    if repository_root is None:
        if resolved_source != project_root / 'apps/web':
            raise ValueError('Custom source requires an explicit trusted repository_root')
        repository = project_root
    else:
        repository = Path(repository_root)
        try:
            with _directory_fd(repository):
                pass
        except OSError as error:
            raise ValueError('Trusted repository_root must be an absolute physical directory') from error
        if resolved_source != repository and repository not in resolved_source.parents:
            raise ValueError('Source must be inside the trusted repository_root')
    if output == resolved_source or resolved_source in output.parents or output in resolved_source.parents:
        raise ValueError('Runtime output must be outside the source tree')
    # Protect both the declared source repository and the builder's own project.
    for protected in (repository, project_root):
        if output == protected or protected in output.parents:
            raise ValueError('Runtime output must be outside the repository')
    lock = json.loads(_regular(source / 'dependencies.lock.json'))
    if lock.get('schema') != 'novel-library-browser-dependencies-v1' or set(lock.get('packages', {})) != set(VERSIONS):
        raise ValueError('Invalid dependency lock')
    assets = {name: _regular(source / name) for name in SOURCE_FILES}
    product = 'Novel Library' if brand == 'private' else 'Document Library'
    for name in ('index.html', 'offline.html', 'manifest.webmanifest', 'config.js'):
        assets[name] = assets[name].replace(b'Novel Library', product.encode())
    licenses = []
    for name, spec in lock['packages'].items():
        if spec.get('archive') != f'{name}-{VERSIONS[name]}.tgz':
            raise ValueError('Invalid archive name')
        selected = _archive(archives / spec['archive'], spec, name)
        prefix = 'vendor/pdfjs/' if name == 'pdfjs-dist' else 'vendor/tus/'
        for relative, data in selected.items():
            if name == 'tus-js-client' and relative == 'dist/tus.min.js':
                relative = 'tus.min.js'
            if relative.rsplit('/', 1)[-1].startswith('LICENSE'):
                relative += '.txt'
                licenses.append(prefix + relative)
            assets[prefix + relative] = data
    fingerprint = hashlib.sha256(b''.join(name.encode() + hashlib.sha256(data).digest() for name, data in sorted(assets.items()) if name != 'sw.js')).hexdigest()[:20]
    # Only public offline HTML and enumerated static assets; no query-dependent cache keys.
    shell = ['/offline.html'] + ['/assets/' + name for name in sorted(assets) if name not in {'index.html', 'offline.html', 'manifest.webmanifest', 'sw.js'} and not name.startswith('vendor/')]
    worker = assets['sw.js'].decode().replace('__STATIC_SHELL__', json.dumps(shell)).replace('__BUILD_CACHE__', 'novel-library-static-' + fingerprint)
    if '__STATIC_SHELL__' in worker or '__BUILD_CACHE__' in worker:
        raise ValueError('Unresolved service-worker placeholders')
    assets['sw.js'] = worker.encode()
    manifest = {'schema': 'novel-library-web-assets-v1', 'brand': brand,
                'assets': {name: {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)} for name, data in sorted(assets.items())},
                'dependencies': lock, 'licenses': sorted(licenses), 'static_shell': shell}
    # Pin the parent and newly created root before any write. Every descendant
    # is opened relative to an admitted descriptor, never a mutable path string.
    try:
        with _directory_fd(output.parent) as parent_fd:
            protected_identities = set()
            for protected in (source, repository, project_root):
                with _directory_fd(protected) as protected_fd:
                    info = os.fstat(protected_fd)
                    protected_identities.add((info.st_dev, info.st_ino))
            for ancestor in (output.parent, *output.parent.parents):
                with _directory_fd(ancestor) as ancestor_fd:
                    info = os.fstat(ancestor_fd)
                    if (info.st_dev, info.st_ino) in protected_identities:
                        raise ValueError('Runtime output must be physically outside the source and repositories')
            os.mkdir(output.name, mode=0o700, dir_fd=parent_fd)
            created = os.stat(output.name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISDIR(created.st_mode):
                raise ValueError('Output root replaced after creation')
            root_fd = os.open(output.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                              dir_fd=parent_fd)
            try:
                identity = (created.st_dev, created.st_ino)
                opened = os.fstat(root_fd)
                if (opened.st_dev, opened.st_ino) != identity:
                    raise ValueError('Output root changed before admission')
                for name, data in assets.items():
                    _same_output(parent_fd, output.name, identity)
                    _write_output(root_fd, name, data)
                _same_output(parent_fd, output.name, identity)
                encoded = (json.dumps(manifest, ensure_ascii=False, indent=2) + '\n').encode()
                _write_output(root_fd, 'asset-manifest.json', encoded)
                _same_output(parent_fd, output.name, identity)
            finally:
                os.close(root_fd)
    except OSError as error:
        raise ValueError('New output directory is unavailable or changed') from error
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--archives', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True,
                        help='New external directory under trusted operator-owned, namespace-stable ancestors')
    parser.add_argument('--repository-root', type=Path,
                        help='Trusted containing repository; required for a custom source directory')
    parser.add_argument('--brand', choices=('private', 'public'), default='private')
    arguments = parser.parse_args()
    result = build(arguments.source, arguments.archives, arguments.output,
                   brand=arguments.brand, repository_root=arguments.repository_root)
    print(json.dumps({'assets': len(result['assets']), 'brand': result['brand'], 'output': str(arguments.output)}))
