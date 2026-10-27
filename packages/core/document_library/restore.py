"""Isolated local restoration. Verification never activates users or workers.

The caller supplies an existing, empty, qualified local destination and the one
local coordinator's NAS lock. Disaster recovery may provision a new control root
only after stopping the old coordinator. No source SSD or live DB is required.
"""
from contextlib import closing, contextmanager
import hashlib
import json
import os
import sqlite3
import time

from .backup import BackupError, SnapshotService, canonical_json
from .publication import PublicationService, object_path


from .library_contracts import register_sqlite_functions, invalidate_downloads_in, rebuild_search_in
from .library_projection import rebuild_projection_in
from .metadata_contracts import (rebuild_metadata_publication_in, reconcile_metadata_restore_in, verify_metadata_publication_in)


class RestoreService:
    SOURCE_DB = 'backup-source/content.sqlite3'
    DATABASE = 'database/library.sqlite3'
    MANIFEST = 'backup-source/manifest.json'
    SEAL = 'backup-source/SEALED.json'
    PLAN = 'restore/reconciliation.json'
    VERIFIED = 'restore/VERIFIED.json'

    def __init__(self, reader):
        self.reader = reader

    def _separate(self, target):
        target.validate()
        for root in (self.reader.remote,self.reader.locks.root):
            if root is self.reader.locks.root:
                root.validate()
            same_inode = root.fd is not None and (os.fstat(target.fd).st_dev,os.fstat(target.fd).st_ino) == (os.fstat(root.fd).st_dev,os.fstat(root.fd).st_ino)
            if (target.path == root.path or target.path in root.path.parents or root.path in target.path.parents
                    or same_inode):
                raise BackupError('Restore requires a separate isolated destination')

    @contextmanager
    def _execution(self, target):
        self._separate(target)
        info = os.fstat(target.fd)
        identity = hashlib.sha256(f'{info.st_dev}:{info.st_ino}'.encode()).hexdigest()
        with self.reader.locks._lock(f'jobs/locks/restores/{identity}.lock',blocking=False):
            yield

    @staticmethod
    def _write_json(target, path, value):
        data = canonical_json(value).encode()
        with target.file(path,create=True) as fd, os.fdopen(os.dup(fd),'wb') as stream:
            stream.write(data)
            stream.flush()
        return dict(sha256=hashlib.sha256(data).hexdigest(),byte_size=len(data))

    def _json(self, target, path):
        with target.file(path) as fd, os.fdopen(os.dup(fd),'rb') as stream:
            data = stream.read(self.reader.max_manifest_bytes+1)
        if len(data) > self.reader.max_manifest_bytes:
            raise BackupError('Restore metadata exceeds configured bound')
        value = json.loads(data)
        if canonical_json(value).encode() != data:
            raise BackupError('Restore metadata is not canonical')
        return value

    @staticmethod
    def _hash(target, path, expected):
        with target.file(path) as fd:
            return PublicationService._hash_fd(fd,expected)

    def _copy(self, source, source_path, target, target_path, expected):
        with source.file(source_path) as src:
            before = PublicationService._fingerprint(src)
            size = 0
            with target.file(target_path,create=True) as dst, os.fdopen(os.dup(dst),'wb') as stream:
                while chunk := os.read(src,1024*1024):
                    size += len(chunk)
                    if size > expected['byte_size']:
                        raise BackupError('Restore source exceeds fixed size')
                    stream.write(chunk)
                stream.flush()
            if PublicationService._fingerprint(src) != before:
                raise BackupError('Restore source changed during copy')
        self._hash(target,target_path,expected)

    @contextmanager
    def _database(self, target, path):
        # SQLite reads only this private local copy, never an SMB source.
        with target.file(path), closing(sqlite3.connect((target.path/path).as_uri()+'?mode=ro&immutable=1',uri=True,autocommit=True)) as db:
            register_sqlite_functions(db)
            db.execute('PRAGMA query_only=ON')
            yield db

    @staticmethod
    def _active_references(db):
        incomplete = db.execute('''SELECT r.id FROM edition_revisions r
            LEFT JOIN revision_seals s ON s.revision_id=r.id
            WHERE (r.is_active=1 OR s.retained=1) AND (
                s.revision_id IS NULL
                OR NOT EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=r.id)
                OR NOT EXISTS(SELECT 1 FROM revision_options o WHERE o.edition_revision_id=r.id)
                OR EXISTS(SELECT 1 FROM revision_assets a WHERE a.edition_revision_id=r.id
                    AND NOT EXISTS(SELECT 1 FROM option_assets oa WHERE oa.edition_revision_id=r.id AND oa.asset_position=a.asset_position))
                OR EXISTS(SELECT 1 FROM revision_options o WHERE o.edition_revision_id=r.id
                    AND NOT EXISTS(SELECT 1 FROM option_assets oa WHERE oa.option_id=o.id))
            ) LIMIT 1''').fetchone()
        if incomplete:
            raise BackupError('Active or retained revision has incomplete sealed references')
        if db.execute("SELECT 1 FROM metadata_candidate_control k JOIN metadata_candidates c ON c.id=k.candidate_id LEFT JOIN objects o ON o.sha256=c.object_sha WHERE k.retained=1 AND (o.sha256 IS NULL OR o.state<>'LIVE' OR o.local_relpath<>'objects/'||substr(o.sha256,1,2)||'/'||substr(o.sha256,3,2)||'/'||o.sha256) LIMIT 1").fetchone():
            raise BackupError('Retained cover object closure differs')

    def _snapshot(self, target, manifest):
        self._hash(target,self.SOURCE_DB,manifest['database'])
        with self._database(target,self.SOURCE_DB) as db:
            SnapshotService.validate_database(db,manifest,projections=False)
            self._active_references(db)
            for obj in manifest['objects'] + manifest['archived_originals']:
                row = db.execute('SELECT byte_size,local_relpath FROM objects WHERE sha256=?',(obj['sha256'],)).fetchone()
                if row != (obj['byte_size'],object_path(obj['sha256'])):
                    raise BackupError('Restored object identity differs from snapshot')

    @staticmethod
    def _reconcile(db, manifest):
        """Modify only the new isolated DB. Never infer external execution stop."""
        objects = manifest['objects'] + manifest['archived_originals']
        shas = canonical_json([obj['sha256'] for obj in objects])
        plan = dict(format='NovelLibraryRestoreReconciliationV1',activation_allowed=False,
                    reauthentication_required=True,execution_reconciliation_required=True,
                    original_snapshot_preserved=True)
        register_sqlite_functions(db)
        db.execute('BEGIN IMMEDIATE')
        try:
            plan['missing_unreferenced_objects'] = db.execute("UPDATE objects SET state='MISSING' WHERE sha256 NOT IN (SELECT value FROM json_each(?))",(shas,)).rowcount
            db.execute("UPDATE objects SET state='LIVE' WHERE sha256 IN (SELECT value FROM json_each(?))",(shas,))
            db.execute('UPDATE object_lifecycle SET unreferenced_since=NULL,quarantined_at=NULL,operation=NULL')
            plan['revoked_uploads'] = db.execute("UPDATE upload_sessions SET status='revoked' WHERE status IN ('receiving','staged')").rowcount
            db.execute("UPDATE upload_batches SET status='revoked' WHERE status='open'")
            plan['fenced_jobs'] = db.execute("""UPDATE jobs SET state='retry_wait',lease_generation=lease_generation+1,
                lease_owner=NULL,lease_until=NULL,error_code='restore_reconciliation_required'
                WHERE state IN ('queued','running','retry_wait')""").rowcount
            plan.update(invalidate_downloads_in(db,'restore_requires_execution_reconciliation'))
            if db.execute("SELECT 1 FROM metadata_candidate_control k JOIN metadata_candidates c ON c.id=k.candidate_id WHERE c.origin='PUBLISHER_OFFICIAL' LIMIT 1").fetchone():
                if 'set_id' not in manifest or 'database' not in manifest:
                    raise BackupError('Official metadata restore requires fixed manifest identity')
                plan.update(reconcile_metadata_restore_in(db,snapshot_set_id=manifest['set_id'],snapshot_sha256=manifest['database']['sha256'],observed_at_ms=time.time_ns()//1000000))
            else:
                plan['metadata_official_revalidated']=0
            rebuild_metadata_publication_in(db)
            rebuild_search_in(db)
            rebuild_projection_in(db)
            plan['invalidated_backup_receipts'] = db.execute('UPDATE object_backup_receipts SET valid=0 WHERE valid=1').rowcount
            for name,query in (
                ('held_external_resources','SELECT count(*) FROM resource_slots WHERE job_id IS NOT NULL'),
                ('unfinished_backup_sets',"SELECT count(*) FROM backup_sets WHERE state IN ('CAPTURING','BUILDING')"),
                ('prepared_installations',"SELECT count(*) FROM storage_intents WHERE state='prepared'"),
                ('pending_outbox',"SELECT count(*) FROM job_outbox WHERE state='pending'"),
                ('preserved_pins','SELECT count(*) FROM object_pins'),
            ):
                plan[name] = db.execute(query).fetchone()[0]
            SnapshotService._integrity(db)
            SnapshotService._redaction_verified(db)
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        return plan

    def _prepare_database(self, target, manifest):
        with self._database(target,self.SOURCE_DB) as source, target.file(self.DATABASE,create=True):
            with closing(sqlite3.connect(target.path/self.DATABASE,autocommit=True)) as db:
                db.execute('PRAGMA foreign_keys=ON')
                source.backup(db)
                plan = self._reconcile(db,manifest)
        # The SQLite handle is closed before explicit file/directory sync.
        target.sync_existing(self.DATABASE)
        return plan

    def _prepared(self, target, objects):
        with self._database(target,self.DATABASE) as db:
            SnapshotService._schema(db)
            SnapshotService._integrity(db)
            SnapshotService._redaction_verified(db)
            self._active_references(db)
            if (db.execute("SELECT count(*) FROM upload_sessions WHERE status IN ('receiving','staged')").fetchone()[0]
                    or db.execute("SELECT count(*) FROM upload_batches WHERE status='open'").fetchone()[0]
                    or db.execute("SELECT count(*) FROM jobs WHERE state='running' OR (state IN ('queued','retry_wait') AND error_code IS NOT 'restore_reconciliation_required')").fetchone()[0]
                    or db.execute('SELECT count(*) FROM object_backup_receipts WHERE valid=1').fetchone()[0]):
                raise BackupError('Restored execution or backup authority was not reconciled')
            actual = db.execute("SELECT sha256,byte_size,local_relpath FROM objects WHERE state='LIVE' ORDER BY sha256").fetchall()
            expected = sorted((o['sha256'],o['byte_size'],object_path(o['sha256'])) for o in objects)
            if actual != expected:
                raise BackupError('Restored LIVE inventory differs from copied originals')
        for obj in objects:
            self._hash(target,object_path(obj['sha256']),obj)

    def restore(self, set_id, target):
        """Fresh destination only; failures preserve partial evidence, never overwrite."""
        with self._execution(target):
            with os.scandir(target.fd) as entries:
                if next(entries,None) is not None:
                    raise BackupError('Restore destination must be empty')
            with self.reader.locks.backup_mutation():
                manifest,seal = self.reader._verified_set(set_id,{},set())
                self._write_json(target,self.MANIFEST,manifest)
                self._write_json(target,self.SEAL,seal)
                self._copy(self.reader.remote,self.reader.paths(set_id)['database'],target,self.SOURCE_DB,manifest['database'])
                for obj in manifest['objects'] + manifest['archived_originals']:
                    path = object_path(obj['sha256'])
                    self._copy(self.reader.remote,path,target,path,obj)
            self._snapshot(target,manifest)
            plan = self._prepare_database(target,manifest)
            objects = manifest['objects'] + manifest['archived_originals']
            self._prepared(target,objects)
            plan_identity = self._write_json(target,self.PLAN,plan)
            with target.file(self.DATABASE) as fd:
                size = os.fstat(fd).st_size
                if size > self.reader.max_snapshot_bytes:
                    raise BackupError('Prepared database exceeds configured bound')
                hasher = hashlib.sha256()
                while chunk := os.read(fd,1024*1024): hasher.update(chunk)
            result = dict(format='NovelLibraryVerifiedRestoreV1',set_id=set_id,status='VERIFIED_BLOCKED',
                          activation_allowed=False,reauthentication_required=True,execution_reconciliation_required=True,
                          source_manifest_sha256=seal['manifest_sha256'],snapshot_at_ms=manifest['snapshot_at_ms'],
                          storage_sequence=manifest['storage_sequence'],sealed_at_ms=seal['sealed_at_ms'],
                          copied_originals=len(objects),database=dict(sha256=hasher.hexdigest(),byte_size=size),
                          reconciliation=plan_identity)
            self._write_json(target,self.VERIFIED,result)
            return result

    def verify(self, target):
        """Offline content verification. This is not a service activation receipt."""
        with self._execution(target):
            result = self._json(target,self.VERIFIED)
            if (result['format'] != 'NovelLibraryVerifiedRestoreV1' or result['status'] != 'VERIFIED_BLOCKED'
                    or result['activation_allowed'] is not False or result['reauthentication_required'] is not True
                    or result['execution_reconciliation_required'] is not True):
                raise BackupError('Restore activation policy differs')
            manifest = self.reader._manifest(canonical_json(self._json(target,self.MANIFEST)).encode(),result['set_id'])
            seal = self.reader._seal_value(canonical_json(self._json(target,self.SEAL)).encode(),manifest)
            objects = manifest['objects'] + manifest['archived_originals']
            if (result['source_manifest_sha256'] != seal['manifest_sha256'] or result['copied_originals'] != len(objects)
                    or result['snapshot_at_ms'] != manifest['snapshot_at_ms'] or result['storage_sequence'] != manifest['storage_sequence']
                    or result['sealed_at_ms'] != seal['sealed_at_ms']):
                raise BackupError('Restore source binding differs')
            for key,limit in (('database',self.reader.max_snapshot_bytes),('reconciliation',self.reader.max_manifest_bytes)):
                if (type(result[key]['byte_size']) is not int or not 0 <= result[key]['byte_size'] <= limit):
                    raise BackupError('Restore artifact exceeds configured bound')
            self._hash(target,self.DATABASE,result['database'])
            self._hash(target,self.PLAN,result['reconciliation'])
            self._snapshot(target,manifest)
            self._prepared(target,objects)
            return result
