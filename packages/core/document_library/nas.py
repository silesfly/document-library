"""Immutable NAS artifacts under the coordinator's LOCAL backup lock.

This physical transfer boundary does not complete jobs, release local pins or
issue GC receipts. Only M4 may mutate the dedicated NAS tree. No SMB locking or
SQLite database execution on the share is used.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from sqlalchemy import text

from .auth import digest
from .backup import BackupError, SCHEMA_VERSION, EXCLUDED, canonical_json
from .publication import PublicationError, PublicationService, object_path
from .storage import StorageError, _identity, _open_absolute_directory


@dataclass(frozen=True)
class MacSMBVolume:
    mountpoint: Path
    source: str

    def verify(self, root, root_fd):
        if sys.platform != 'darwin':
            raise StorageError('Production NAS requires qualified macOS SMB')
        if (not self.mountpoint.is_absolute() or self.mountpoint not in root.parents
                or not self.source.startswith('//') or any(c.isspace() for c in self.source)):
            raise StorageError('NAS tree must be inside the approved SMB mount')
        fd = _open_absolute_directory(self.mountpoint)
        try:
            info = os.fstat(fd)
            if (not os.path.ismount(self.mountpoint) or info.st_dev != os.fstat(root_fd).st_dev
                    or info.st_dev == self.mountpoint.parent.stat().st_dev):
                raise StorageError('Expected SMB filesystem is absent or changed')
            mounts = subprocess.run(['/sbin/mount','-t','smbfs'],check=True,capture_output=True,text=True,timeout=10).stdout.splitlines()
            if not any(line.startswith(f'{self.source} on {self.mountpoint} (smbfs,') for line in mounts):
                raise StorageError('SMB mount source differs')
            if _identity(os.stat(self.mountpoint,follow_symlinks=False)) != _identity(info):
                raise StorageError('SMB mount changed during verification')
        finally:
            os.close(fd)


class NASBackupReader:
    """NAS verification without live database, publisher instance or source SSD."""
    def __init__(self, remote_root, locks, *, max_dependencies=1000,
                 max_snapshot_bytes=256*1024*1024, max_manifest_bytes=16*1024*1024):
        if type(max_dependencies) is not int or not 1 <= max_dependencies <= 10000:
            raise ValueError('Bounded dependency count required')
        if any(type(v) is not int or v < 4096 for v in (max_snapshot_bytes,max_manifest_bytes)):
            raise ValueError('Explicit positive metadata size limits required')
        self.remote, self.locks = remote_root, locks
        self.max_dependencies = max_dependencies
        self.max_snapshot_bytes, self.max_manifest_bytes = max_snapshot_bytes,max_manifest_bytes

    @staticmethod
    def paths(set_id):
        if str(uuid.UUID(set_id)) != set_id:
            raise BackupError('Canonical backup UUID required')
        base = f'sets/{set_id}'
        return dict(database=f'{base}/content.sqlite3',manifest=f'{base}/manifest.json',seal=f'{base}/SEALED.json')


    @staticmethod
    def _expected(value):
        object_path(value['sha256'])
        if type(value['byte_size']) is not int or value['byte_size'] < 0:
            raise BackupError('Nonnegative integer file size required')
        return value


    def _manifest(self, encoded, set_id):
        value = json.loads(encoded)
        if (canonical_json(value).encode() != encoded or value['set_id'] != set_id
                or value['format'] != 'NovelLibraryContentBackupV1' or value['schema'] != SCHEMA_VERSION
                or value['excluded'] != EXCLUDED
                or type(value['snapshot_at_ms']) is not int or value['snapshot_at_ms'] < 0
                or type(value['storage_sequence']) is not int or value['storage_sequence'] < 0
                or value['reauthentication_required'] is not True
                or value['execution_reconciliation_required'] is not True):
            raise BackupError('Unexpected content manifest policy or identity')
        self._expected(value['database'])
        if value['database']['byte_size'] > self.max_snapshot_bytes:
            raise BackupError('Snapshot exceeds configured metadata bound')
        shas = []
        for obj in value['objects']:
            self._expected(obj)
            if obj['local_relpath'] != object_path(obj['sha256']):
                raise BackupError('Unexpected original path')
            shas.append(obj['sha256'])
        if shas != sorted(set(shas)):
            raise BackupError('Manifest originals must be sorted and unique')
        archived = []
        for obj in value['archived_originals']:
            self._expected(obj)
            if obj['sha256'] in shas or not obj['receipts']:
                raise BackupError('Historical original requires a separate receipt')
            archived.append(obj['sha256'])
        if archived != sorted(set(archived)):
            raise BackupError('Historical originals must be sorted and unique')
        return value


    def _read(self, path, maximum):
        with self.remote.file(path) as fd:
            if os.fstat(fd).st_size > maximum:
                raise BackupError('NAS metadata exceeds configured bound')
            with os.fdopen(os.dup(fd),'rb') as stream:
                data = stream.read(maximum+1)
            if len(data) > maximum:
                raise BackupError('NAS metadata grew beyond configured bound')
        return data


    def _hash(self, path, expected):
        with self.remote.file(path) as fd:
            return PublicationService._hash_fd(fd,self._expected(expected))


    def _contents(self, manifest, visited, active):
        paths = self.paths(manifest['set_id'])
        self._hash(paths['database'],manifest['database'])
        for obj in manifest['objects']:
            self._hash(object_path(obj['sha256']),obj)
        for obj in manifest['archived_originals']:
            valid = False
            for receipt in obj['receipts']:
                try:
                    if receipt['backup_relpath'] != object_path(obj['sha256']):
                        raise BackupError('Historical receipt path differs')
                    prior,seal = self._verified_set(receipt['set_id'],visited,active)
                    valid = (seal['manifest_sha256'] == receipt['manifest_sha'] and any(
                        old['sha256']==obj['sha256'] and old['byte_size']==obj['byte_size'] for old in prior['objects']))
                except (OSError,StorageError,PublicationError,ValueError,TypeError,KeyError):
                    # Corrupt/unparseable prior metadata invalidates that
                    # receipt, not another independently verified copy.
                    valid = False
                if valid:
                    break
            if not valid:
                raise BackupError('Historical original has no physically verified sealed receipt')


    def _seal_value(self, encoded, manifest):
        value = json.loads(encoded)
        if (set(value) != {'format','set_id','manifest_sha256','snapshot_sha256','sealed_at_ms'}
                or canonical_json(value).encode() != encoded
                or value['format'] != 'NovelLibrarySealV1' or value['set_id'] != manifest['set_id']
                or value['manifest_sha256'] != hashlib.sha256(canonical_json(manifest).encode()).hexdigest()
                or value['snapshot_sha256'] != manifest['database']['sha256']
                or type(value['sealed_at_ms']) is not int or value['sealed_at_ms'] < manifest['snapshot_at_ms']):
            raise BackupError('NAS seal does not identify this snapshot')
        return value


    def _verified_set(self, set_id, visited, active):
        if set_id in active or len(active) >= 32:
            raise BackupError('Cyclic or excessive backup dependencies')
        if set_id in visited:
            if visited[set_id] is None:
                raise BackupError('Dependency already failed verification')
            return visited[set_id]
        if len(visited) >= self.max_dependencies:
            raise BackupError('Backup dependency policy exceeded')
        # Failed dependencies consume the same bounded budget as successful
        # ones; duplicate failed receipts must not restart expensive validation.
        visited[set_id] = None
        paths = self.paths(set_id)
        manifest = self._manifest(self._read(paths['manifest'],self.max_manifest_bytes),set_id)
        seal = self._seal_value(self._read(paths['seal'],4096),manifest)
        active.add(set_id)
        try:
            self._contents(manifest,visited,active)
        finally:
            active.remove(set_id)
        visited[set_id] = manifest,seal
        return manifest,seal


    def verify_set(self, set_id):
        """Physical sealed-set verification; no SQLite is opened on SMB."""
        with self.locks.backup_mutation():
            manifest,seal = self._verified_set(set_id,{},set())
            return dict(manifest=manifest,seal=seal)


class NASBackupStore(NASBackupReader):
    def __init__(self, snapshots, remote_root, *, max_dependencies=1000):
        super().__init__(remote_root,snapshots.locks,max_dependencies=max_dependencies,
                         max_snapshot_bytes=snapshots.max_snapshot_bytes,max_manifest_bytes=snapshots.max_manifest_bytes)
        self.snapshots, self.local = snapshots, snapshots.root
        self.jobs, self.engine = snapshots.jobs, snapshots.engine

    @contextmanager
    def _authority(self, lease, manifest):
        with self.jobs.ownership(lease) as conn:
            self.snapshots._binding(conn,lease)
            row = self.snapshots._row(conn,manifest['set_id'])
            if ((row['job_id'],row['generation'],row['owner_digest'],row['state'])
                    != (lease.job_id,lease.generation,digest(lease.owner),'BUILDING')
                    or row['manifest_json'] != canonical_json(manifest)):
                raise BackupError('NAS operation does not own this fixed capture')
            pins = conn.execute(text("SELECT object_sha FROM object_pins WHERE kind='backup' AND reference_id=:id ORDER BY object_sha"),dict(id=manifest['set_id'])).scalars().all()
            if pins != [obj['sha256'] for obj in manifest['objects']]:
                raise BackupError('Local capture protection changed')
            yield conn


    def _stage(self, set_id, source, expected):
        """Unique temporary copy; no final install or backup lock while reading SSD."""
        path = f'staging/{set_id}/{uuid.uuid4().hex}.part'
        size, hasher = 0, hashlib.sha256()
        with self.remote.file(path,create=True) as fd:
            with os.fdopen(os.dup(fd),'wb') as target:
                while chunk := source.read(1024*1024):
                    size += len(chunk)
                    if size > expected['byte_size']:
                        raise BackupError('Copy source grew beyond expected size')
                    hasher.update(chunk)
                    target.write(chunk)
                target.flush()
        if size != expected['byte_size'] or hasher.hexdigest() != expected['sha256']:
            raise BackupError('Copy source differs from fixed snapshot')
        # Close/reopen the NAS file; a source-side hash is not transfer evidence.
        self._hash(path,expected)
        return path


    def _publish(self, lease, manifest, staged, target, expected):
        with self.locks.backup_mutation():
            if target != self.paths(manifest['set_id'])['manifest']:
                self._hash(self.paths(manifest['set_id'])['manifest'],self._bytes_expected(canonical_json(manifest).encode()))
            self._hash(staged,expected)
            with self._authority(lease,manifest):
                try:
                    self._hash(target,expected)
                except FileNotFoundError:
                    self.remote.move(staged,target)
                else:
                    # Both files verified; only this call's own temporary file.
                    self.remote.unlink(staged)
                self.remote.sync_existing(target)
            self._hash(target,expected)


    @staticmethod
    def _bytes_expected(data):
        return dict(sha256=hashlib.sha256(data).hexdigest(),byte_size=len(data))


    def _put_bytes(self, lease, manifest, target, data):
        expected = self._bytes_expected(data)
        staged = self._stage(manifest['set_id'],BytesIO(data),expected)
        self._publish(lease,manifest,staged,target,expected)


    def _put_local(self, lease, manifest, path, target, expected, *, sha=None):
        with self.locks.backup_mutation():
            self._hash(self.paths(manifest['set_id'])['manifest'],self._bytes_expected(canonical_json(manifest).encode()))
            try:
                self._hash(target,expected)
            except FileNotFoundError:
                pass
            else:
                with self._authority(lease,manifest):
                    self.remote.sync_existing(target)
                return
        @contextmanager
        def open_source():
            if sha is None:
                with self.local.file(path) as fd:
                    yield fd
            else:
                def opener(stack):
                    return stack.enter_context(self.local.file(path))
                with self.locks.readers([sha],opener) as fd:
                    yield fd
        with open_source() as fd:
            before = self.snapshots.publisher._fingerprint(fd)
            with os.fdopen(os.dup(fd),'rb') as source:
                staged = self._stage(manifest['set_id'],source,expected)
            if self.snapshots.publisher._fingerprint(fd) != before:
                raise BackupError('Local source changed during transfer')
        # Local reader/storage locks are released before backup lock acquisition.
        self._publish(lease,manifest,staged,target,expected)


    def transfer(self, lease):
        manifest = self.snapshots.capture(lease)
        paths = self.paths(manifest['set_id'])
        # This immutable manifest is the durable BUILDING protection reference.
        # Install it before any original is installed or reused on the NAS.
        self._put_bytes(lease,manifest,paths['manifest'],canonical_json(manifest).encode())
        for obj in manifest['objects']:
            self._put_local(lease,manifest,obj['local_relpath'],object_path(obj['sha256']),obj,sha=obj['sha256'])
        self._put_local(lease,manifest,self.snapshots.paths(manifest['set_id'])['database'],paths['database'],manifest['database'])
        with self.locks.backup_mutation():
            self._hash(paths['manifest'],self._bytes_expected(canonical_json(manifest).encode()))
            self._contents(manifest,{manifest['set_id']: None}, {manifest['set_id']})
            try:
                seal = self._seal_value(self._read(paths['seal'],4096),manifest)
            except FileNotFoundError:
                with self._authority(lease,manifest):
                    seal = dict(format='NovelLibrarySealV1',set_id=manifest['set_id'],
                                manifest_sha256=hashlib.sha256(canonical_json(manifest).encode()).hexdigest(),
                                snapshot_sha256=manifest['database']['sha256'],sealed_at_ms=self.jobs._sample()[1])
                    data = canonical_json(seal).encode()
                    staged = self._stage(manifest['set_id'],BytesIO(data),self._bytes_expected(data))
                    self.remote.move(staged,paths['seal'])
            with self._authority(lease,manifest):
                self.remote.sync_existing(paths['seal'])
            result = self._verified_set(manifest['set_id'],{},set())
        return dict(manifest=result[0],seal=result[1])


