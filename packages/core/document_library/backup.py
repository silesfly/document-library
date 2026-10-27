"""Fixed local content snapshots; NAS sealing/restoration is a separate boundary.

The live DB is copied only to memory. Authentication material is removed before
VACUUM writes a compact new disk artifact. CAPTURING/BUILDING is never a backup
success receipt. Failure retains original-object pins until observed-use cleanup.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import uuid
from sqlalchemy import text
from .auth import digest
from .publication import PublicationError, object_path
from .storage import sync_file
from .library_projection import verify_projection_in
from .metadata_contracts import (COVER_REFERENCES_V1, HISTORICAL_COVERS_V1, verify_metadata_publication_in)
from .metadata_generated_contracts import verify_metadata_generated_in
from .library_contracts import (canonical_json, register_sqlite_functions, verify_metadata_in,
    invalidate_downloads_in, DISABLED_TICKET_SESSION)


class BackupError(PublicationError):
    pass


SCHEMA_VERSION = '0018_generated_review'
# Qualified against frozen migrations, never learned from a live database.
SCHEMA_SIGNATURE = '8a24a01feb87e884b16758795e3d8fa365761f83fbf49b9706f925fe6277cc00'
DISABLED_PASSWORD = '!backup-reauthentication-required'
DISABLED_RESOURCE = '!backup-external-observation-required'
EXCLUDED = ['authentication-material', 'unfinished-upload-staging', 'worker-staging',
            'unregistered-installations', 'regenerable-search-and-thumbnail-cache']
REQUIRED_SQL = """SELECT DISTINCT o.* FROM objects o WHERE o.sha256 IN (
    SELECT a.object_sha FROM revision_assets a JOIN edition_revisions r ON r.id=a.edition_revision_id
        LEFT JOIN revision_seals s ON s.revision_id=r.id WHERE r.is_active=1 OR s.retained=1
    UNION SELECT object_sha FROM jobs WHERE state IN ('queued','running','retry_wait')
        OR id IN (SELECT job_id FROM resource_slots WHERE job_id IS NOT NULL)
    UNION SELECT a.object_sha FROM revision_assets a JOIN jobs j ON j.input_revision_id=a.edition_revision_id
        WHERE j.state IN ('queued','running','retry_wait') OR j.id IN (SELECT job_id FROM resource_slots WHERE job_id IS NOT NULL)
    UNION SELECT object_sha FROM object_pins
    UNION SELECT object_sha FROM ("""+COVER_REFERENCES_V1+""")
) ORDER BY o.sha256"""


def schema_signature(db):
    entries = [tuple(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")]
    return hashlib.sha256(canonical_json(entries).encode()).hexdigest()


class SnapshotService:
    def __init__(self, publisher, *, max_snapshot_bytes=256*1024*1024, max_manifest_bytes=16*1024*1024):
        if any(type(v) is not int or v < 4096 for v in (max_snapshot_bytes,max_manifest_bytes)):
            raise ValueError('Explicit positive metadata size limits required')
        self.publisher = publisher
        self.root, self.locks = publisher.root, publisher.locks
        self.jobs, self.engine = publisher.jobs, publisher.engine
        self.max_snapshot_bytes, self.max_manifest_bytes = max_snapshot_bytes,max_manifest_bytes

    @staticmethod
    def paths(set_id):
        if str(uuid.UUID(set_id)) != set_id:
            raise BackupError('Canonical backup UUID required')
        base = f'backup/sets/{set_id}'
        return dict(database=f'{base}/content.sqlite3',manifest=f'{base}/manifest.json')

    def _row(self, conn, set_id):
        self.paths(set_id)
        return conn.execute(text('SELECT * FROM backup_sets WHERE id=:id'),dict(id=set_id)).mappings().one()

    def _binding(self, conn, lease):
        job = conn.execute(text('SELECT kind FROM jobs WHERE id=:id'),dict(id=lease.job_id)).scalar_one()
        if job not in {'backup_revision','backup_catalog'}:
            raise BackupError('Backup worker job required')

    @staticmethod
    def _schema(db):
        register_sqlite_functions(db)
        if (db.execute('SELECT version_num FROM alembic_version').fetchone()[0] != SCHEMA_VERSION
                or schema_signature(db) != SCHEMA_SIGNATURE):
            raise BackupError('Schema needs an explicit backup/redaction policy review')

    @staticmethod
    def _integrity(db, *, projections=True):
        register_sqlite_functions(db)
        try:
            verify_metadata_in(db)
            verify_metadata_publication_in(db)
            verify_metadata_generated_in(db)
            if projections:
                verify_projection_in(db)
        except ValueError as exc:
            raise BackupError(str(exc)) from exc
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)] or db.execute('PRAGMA foreign_key_check').fetchall():
            raise BackupError('Snapshot integrity or foreign keys differ')

    def _memory_snapshot(self):
        """Caller holds global storage lock, without a SQLite writer transaction."""
        path = Path(self.engine.url.database)
        try:
            relative = str(path.relative_to(self.root.path))
        except ValueError as exc:
            raise BackupError('Database must be inside the qualified storage root') from exc
        memory = sqlite3.connect(':memory:',autocommit=True)
        try:
            with self.root.file(relative), closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,autocommit=True)) as source:
                source.execute('PRAGMA query_only=ON')
                source.execute('BEGIN')
                # First read establishes snapshot; storage mutations/GC cannot
                # cross it while the global lock remains held.
                at = self.jobs._sample()[1]
                sequence = source.execute('SELECT value FROM storage_sequence WHERE id=1').fetchone()[0]
                size = source.execute('PRAGMA page_count').fetchone()[0]*source.execute('PRAGMA page_size').fetchone()[0]
                if size > self.max_snapshot_bytes:
                    raise BackupError('Snapshot exceeds configured memory policy')
                self._schema(source)
                source.backup(memory)
                source.rollback()
            self._integrity(memory)
            return memory,at,sequence
        except BaseException:
            memory.close()
            raise

    @staticmethod
    def _inventory(db, *, snapshot_at_ms=None):
        if snapshot_at_ms is not None and (type(snapshot_at_ms) is not int or snapshot_at_ms<0):
            raise BackupError('Fixed snapshot observation time required')
        if db.execute("SELECT 1 FROM metadata_candidate_control k JOIN metadata_retention_tombstones t ON t.candidate_id=k.candidate_id WHERE k.retained=1 LIMIT 1").fetchone():
            raise BackupError('RETENTION_RECONCILIATION_REQUIRED')
        if db.execute("SELECT 1 FROM metadata_candidates c JOIN metadata_rights r ON r.id=c.rights_id LEFT JOIN metadata_payloads p ON p.candidate_id=c.id WHERE (r.state<>'allowed' OR r.cache_allowed=0 OR (r.cache_until_ms IS NOT NULL AND (? IS NULL OR r.cache_until_ms<=?))) AND NOT EXISTS(SELECT 1 FROM metadata_retention_tombstones t WHERE t.candidate_id=c.id) AND (c.kind='cover' OR p.candidate_id IS NOT NULL) LIMIT 1",(snapshot_at_ms,snapshot_at_ms)).fetchone():
            raise BackupError('RETENTION_RECONCILIATION_REQUIRED')
        cursor = db.execute(REQUIRED_SQL)
        columns = [d[0] for d in cursor.description]
        required = [dict(zip(columns,row)) for row in cursor]
        objects = []
        for row in required:
            if row['state'] != 'LIVE' or row['local_relpath'] != object_path(row['sha256']):
                raise BackupError('Required original is not LIVE at its canonical path')
            objects.append({k:row[k] for k in ('sha256','byte_size','detected_mime','local_relpath')})
        required_shas = {o['sha256'] for o in objects}
        archived = []
        for sha,size,mime,path,state in db.execute('SELECT DISTINCT o.sha256,o.byte_size,o.detected_mime,o.local_relpath,o.state FROM objects o WHERE o.sha256 IN (SELECT object_sha FROM revision_assets UNION '+HISTORICAL_COVERS_V1+') ORDER BY o.sha256'):
            if sha in required_shas:
                continue
            # A locally available historical original may not have its first
            # backup receipt yet. Include it rather than deadlocking that first
            # backup on the receipt this backup is supposed to establish.
            if state == 'LIVE':
                if path != object_path(sha):
                    raise BackupError('Historical original has an unexpected path')
                objects.append(dict(sha256=sha,byte_size=size,detected_mime=mime,local_relpath=path))
                continue
            receipts = [dict(id=r[0],set_id=r[1],backup_relpath=r[2],manifest_sha=r[3]) for r in db.execute(
                'SELECT id,set_id,backup_relpath,manifest_sha FROM object_backup_receipts WHERE object_sha=? AND byte_size=? AND valid=1 ORDER BY id',(sha,size))]
            if not receipts:
                raise BackupError('Historical original has no retained backup receipt')
            archived.append(dict(sha256=sha,byte_size=size,receipts=receipts))
        return sorted(objects,key=lambda obj:obj['sha256']),archived

    @staticmethod
    def _optimizer_tables(db):
        return [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('sqlite_stat1','sqlite_stat2','sqlite_stat3','sqlite_stat4') ORDER BY name")]

    @staticmethod
    def _redact(db):
        register_sqlite_functions(db)
        db.execute('PRAGMA secure_delete=ON')
        db.execute('BEGIN')
        try:
            for table in ('auth_sessions','auth_tokens','auth_limits','permission_previews','resource_release_receipts'):
                db.execute(f'DELETE FROM {table}')
            # Existing cross-build STAT3/4 samples can retain credential index
            # bytes even after their source rows are removed. Discard all
            # optimizer statistics in memory; restore can rebuild them later.
            for table in SnapshotService._optimizer_tables(db):
                db.execute(f'DELETE FROM {table}')
            db.execute("UPDATE users SET password_hash=?,status='suspended',credential_version=credential_version+1,authz_version=authz_version+1",(DISABLED_PASSWORD,))
            # Occupancy survives; no resource is freed by taking a backup.
            db.execute('UPDATE resource_slots SET token_hash=? WHERE job_id IS NOT NULL',(DISABLED_RESOURCE,))
            invalidate_downloads_in(db,'content_backup_requires_reauthentication')
            db.execute('DELETE FROM library_search_fts')
            db.execute("UPDATE job_clock SET blocked=1,reason='backup_restore_requires_reconciliation'")
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise

    @staticmethod
    def _redaction_verified(db):
        if (db.execute('SELECT count(*) FROM download_tickets WHERE invalidated<>1 OR session_token_hash<>?',(DISABLED_TICKET_SESSION,)).fetchone()[0]
                or db.execute("SELECT count(*) FROM download_attempts WHERE invalidated<>1 OR state IN ('running','cancelling')").fetchone()[0]):
            raise BackupError('Download execution or session identifiers remain in snapshot')
        for table in SnapshotService._optimizer_tables(db):
            if db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:
                raise BackupError('Optimizer statistics remain in snapshot')
        for table in ('auth_sessions','auth_tokens','auth_limits','permission_previews','resource_release_receipts'):
            if db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:
                raise BackupError('Authentication material remains in snapshot')
        if (db.execute("SELECT count(*) FROM users WHERE password_hash<>? OR status<>'suspended'",(DISABLED_PASSWORD,)).fetchone()[0]
                or db.execute('SELECT count(*) FROM resource_slots WHERE job_id IS NOT NULL AND token_hash<>?',(DISABLED_RESOURCE,)).fetchone()[0]
                or db.execute('SELECT blocked FROM job_clock WHERE id=1').fetchone() != (1,)):
            raise BackupError('Snapshot needs reauthentication and execution reconciliation')

    def _write_database(self, db, relative):
        # Compact live redacted records only: no old free pages/WAL or raw
        # credential-bearing disk snapshot. SQLite supports existing empty files.
        with self.root.file(relative,create=True):
            db.execute('VACUUM INTO ?', (str(self.root.path/relative),))

    def _file_digest(self, relative, maximum):
        with self.root.file(relative) as fd:
            size = os.fstat(fd).st_size
            if size > maximum:
                raise BackupError('Backup metadata artifact exceeds configured bound')
            hasher = hashlib.sha256()
            while chunk := os.read(fd,1024*1024):
                hasher.update(chunk)
            return hasher.hexdigest(),size

    @staticmethod
    def set_id(lease):
        return str(uuid.uuid5(uuid.UUID(lease.job_id),f'backup:{lease.generation}'))

    def capture(self, lease):
        set_id = self.set_id(lease)
        memory = None
        try:
            with self.locks.mutation():
                with self.jobs.ownership(lease) as conn:
                    self._binding(conn,lease)
                    prior = conn.execute(text('SELECT * FROM backup_sets WHERE id=:id'),dict(id=set_id)).mappings().first()
                    if prior and prior['owner_digest'] != digest(lease.owner):
                        raise BackupError('Backup belongs to a different execution')
                    if not prior:
                        conn.execute(text("INSERT INTO backup_sets(id,job_id,generation,owner_digest,state,created_at) VALUES(:id,:job,:generation,:owner,'CAPTURING',:now)"),
                                     dict(id=set_id,job=lease.job_id,generation=lease.generation,owner=digest(lease.owner),now=self.jobs._sample()[1]))
                if not prior:
                    memory,at,sequence = self._memory_snapshot()
                    objects,archived = self._inventory(memory,snapshot_at_ms=at)
                    with self.jobs.ownership(lease) as conn:
                        for obj in objects:
                            conn.execute(text('INSERT INTO backup_members VALUES(:id,:sha)'),dict(id=set_id,sha=obj['sha256']))
                            conn.execute(text("INSERT INTO object_pins VALUES('backup',:id,:sha,:now)"),dict(id=set_id,sha=obj['sha256'],now=at))
                        conn.execute(text('UPDATE backup_sets SET snapshot_at=:at,storage_sequence=:sequence WHERE id=:id'),dict(id=set_id,at=at,sequence=sequence))
            if prior:
                # Only complete matching artifacts can resume; never overwrite
                # incomplete files or substitute a newer catalog snapshot.
                return self._finish(lease,set_id)
            self._redact(memory)
            self._redaction_verified(memory)
            self._integrity(memory)
            paths = self.paths(set_id)
            self._write_database(memory,paths['database'])
            sha,size = self._file_digest(paths['database'],self.max_snapshot_bytes)
            manifest = dict(format='NovelLibraryContentBackupV1',set_id=set_id,schema=SCHEMA_VERSION,
                            snapshot_at_ms=at,storage_sequence=sequence,database=dict(sha256=sha,byte_size=size),
                            objects=objects,archived_originals=archived,excluded=EXCLUDED,
                            reauthentication_required=True,execution_reconciliation_required=True)
            encoded = canonical_json(manifest).encode()
            if len(encoded) > self.max_manifest_bytes:
                raise BackupError('Manifest exceeds configured bound')
            manifest_sha = hashlib.sha256(encoded).hexdigest()
            with self.locks.mutation(), self.jobs.ownership(lease) as conn:
                conn.execute(text('UPDATE backup_sets SET snapshot_sha=:sha,snapshot_size=:size,manifest_sha=:manifest_sha,manifest_json=:manifest WHERE id=:id'),
                             dict(id=set_id,sha=sha,size=size,manifest_sha=manifest_sha,manifest=encoded.decode()))
            with self.root.file(paths['manifest'],create=True) as fd:
                with os.fdopen(os.dup(fd),'wb') as stream:
                    stream.write(encoded)
                    stream.flush()
            return self._finish(lease,set_id)
        finally:
            if memory is not None:
                memory.close()

    @staticmethod
    def validate_database(db, manifest, *, projections=True):
        """Shared snapshot closure validation, also usable without the live DB."""
        SnapshotService._schema(db)
        SnapshotService._integrity(db,projections=projections)
        SnapshotService._redaction_verified(db)
        if db.execute('SELECT count(*) FROM library_search_fts').fetchone()[0]:
            raise BackupError('Regenerable search cache remains in content snapshot')
        objects,archived = SnapshotService._inventory(db,snapshot_at_ms=manifest['snapshot_at_ms'])
        if (objects != manifest['objects'] or archived != manifest['archived_originals']
                or db.execute('SELECT value FROM storage_sequence WHERE id=1').fetchone()[0] != manifest['storage_sequence']):
            raise BackupError('Manifest does not cover the fixed snapshot')
        return objects,archived

    def _validate(self, set_id):
        with self.engine.connect() as conn:
            row = dict(self._row(conn,set_id))
        if row['state'] not in {'CAPTURING','BUILDING'} or row['manifest_json'] is None:
            raise BackupError('Fixed capture artifacts are incomplete or unavailable')
        manifest = json.loads(row['manifest_json'])
        paths,proof = self.paths(set_id),{}
        for key,sha,size in [('database',row['snapshot_sha'],row['snapshot_size']),
                             ('manifest',row['manifest_sha'],len(row['manifest_json'].encode()))]:
            with self.root.file(paths[key]) as fd:
                proof[paths[key]] = self.publisher._hash_fd(fd,dict(sha256=sha,byte_size=size))
        path = self.root.path/paths['database']
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,autocommit=True)) as db:
            objects,archived = self.validate_database(db,manifest)
        for obj in objects:
            def open_object(stack):
                return stack.enter_context(self.root.file(obj['local_relpath']))
            with self.locks.readers([obj['sha256']],open_object) as fd:
                proof[obj['local_relpath']] = self.publisher._hash_fd(fd,obj)
        return row,manifest,proof

    def _current(self, conn, row, manifest, proof):
        current = self._row(conn,row['id'])
        if dict(current) != row:
            raise BackupError('Backup changed during validation')
        expected = [o['sha256'] for o in manifest['objects']]
        members = conn.execute(text('SELECT object_sha FROM backup_members WHERE set_id=:id ORDER BY object_sha'),dict(id=row['id'])).scalars().all()
        pins = conn.execute(text("SELECT object_sha FROM object_pins WHERE kind='backup' AND reference_id=:id ORDER BY object_sha"),dict(id=row['id'])).scalars().all()
        if members != expected or pins != expected:
            raise BackupError('Durable backup protection differs from snapshot')
        for obj in manifest['objects']:
            state = conn.execute(text('SELECT state FROM objects WHERE sha256=:sha'),dict(sha=obj['sha256'])).scalar_one()
            if state != 'LIVE':
                raise BackupError('Required pinned original is no longer LIVE')
        for path in proof:
            self.publisher._unchanged(path,proof)

    def _finish(self, lease, set_id):
        row,manifest,proof = self._validate(set_id)
        # A previous file/directory flush may have failed after writing all
        # matching bytes. Revalidation alone does not reestablish durability.
        for path in self.paths(set_id).values():
            with self.root.file(path) as fd:
                sync_file(fd)
            self.root.sync_parent(path)
        with self.locks.mutation(), self.jobs.ownership(lease) as conn:
            if (row['job_id'],row['generation'],row['owner_digest']) != (lease.job_id,lease.generation,digest(lease.owner)):
                raise BackupError('Backup execution binding differs')
            self._current(conn,row,manifest,proof)
            conn.execute(text("UPDATE backup_sets SET state='BUILDING' WHERE id=:id"),dict(id=set_id))
        return manifest

    def verify_local(self, set_id):
        """Read-only verification, not a NAS receipt or a restore-set listing."""
        row,manifest,proof = self._validate(set_id)
        if row['state'] != 'BUILDING':
            raise BackupError('Capture is not ready for transfer')
        with self.locks.mutation(), self.engine.connect() as conn:
            self._current(conn,row,manifest,proof)
        return manifest
