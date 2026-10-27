"""Synchronous backup execution, atomic local success and observed-stop cleanup.

Runtime adapters must use this coordinator for the whole execution. Its local
per-set lock serializes run/stop/retire across processes; it does not replace the
storage or backup mutation locks. No timeout proves that an external copy stopped.
"""
from contextlib import ExitStack
import json
import os
import re
import uuid

from sqlalchemy import text

from .auth import digest
from .backup import BackupError, canonical_json
from .publication import object_path


class BackupCoordinator:
    def __init__(self, store):
        self.store, self.snapshots = store, store.snapshots
        self.root, self.locks = store.local, store.locks
        self.jobs, self.engine = store.jobs, store.engine

    def _execution(self, set_id):
        self.snapshots.paths(set_id)
        # Outer execution serialization, never an object/storage mutation lock.
        return self.locks._lock(f'jobs/locks/backups/{set_id}.lock',blocking=False)

    @staticmethod
    def _bound(row, lease):
        if (row['job_id'],row['generation'],row['owner_digest']) != (lease.job_id,lease.generation,digest(lease.owner)):
            raise BackupError('Original backup execution differs')

    @staticmethod
    def _result(row):
        return dict(set_id=row['id'],manifest_sha256=row['manifest_sha'],snapshot_sha256=row['snapshot_sha'])

    def _no_resource(self, conn, row):
        if conn.execute(text('SELECT 1 FROM resource_slots WHERE job_id=:job AND generation=:generation'),
                        dict(job=row['job_id'],generation=row['generation'])).first():
            raise BackupError('External backup execution still holds a resource')

    def _fenced(self, conn, row):
        job = conn.execute(text('SELECT state,lease_generation FROM jobs WHERE id=:id'),dict(id=row['job_id'])).mappings().one()
        if not (job['lease_generation'] > row['generation'] or
                (job['lease_generation'] == row['generation'] and job['state'] in {'failed','cancelled'})):
            raise BackupError('Backup execution can still run or retry')
        self._no_resource(conn,row)

    def _stop(self, conn, row, disposition, confirmation, now):
        previous = conn.execute(text('SELECT * FROM backup_execution_stops WHERE set_id=:id'),dict(id=row['id'])).mappings().first()
        if previous:
            if (previous['owner_digest'],previous['disposition'],previous['confirmation']) != (row['owner_digest'],disposition,confirmation):
                raise BackupError('Backup stop receipt conflict')
            return False
        conn.execute(text('INSERT INTO backup_execution_stops VALUES(:id,:owner,:disposition,:confirmation,:now)'),
                     dict(id=row['id'],owner=row['owner_digest'],disposition=disposition,confirmation=confirmation,now=now))
        return True

    def run(self, lease):
        """Return only after closed NAS handles and atomic current-owner completion."""
        set_id = self.snapshots.set_id(lease)
        with self._execution(set_id):
            with self.engine.connect() as conn:
                prior = conn.execute(text('SELECT * FROM backup_sets WHERE id=:id'),dict(id=set_id)).mappings().first()
                if prior:
                    self._bound(prior,lease)
                    if prior['state'] == 'SEALED':
                        job = self.jobs.get(lease.job_id)
                        result = self._result(prior)
                        if (job['state'] != 'completed' or job['lease_generation'] != lease.generation
                                or json.loads(job['result_json']) != result):
                            raise BackupError('Completed backup job binding differs')
                        verified = self.store.verify_set(set_id)
                        if canonical_json(verified['manifest']) != prior['manifest_json']:
                            raise BackupError('Completed NAS identity differs')
                        return result
            verified = self.store.transfer(lease)
            # All transfer readers/writers and backup locks have returned/closed.
            row,manifest,proof = self.snapshots._validate(set_id)
            if (verified['manifest'] != manifest or verified['seal']['manifest_sha256'] != row['manifest_sha']
                    or verified['seal']['snapshot_sha256'] != row['snapshot_sha']):
                raise BackupError('Physical backup differs from fixed local capture')
            result = self._result(row)
            with self.locks.mutation(), self.jobs.completion(lease,result=result) as conn:
                self._bound(row,lease)
                self.snapshots._current(conn,row,manifest,proof)
                self._no_resource(conn,row)
                now = self.jobs._sample()[1]
                self._stop(conn,row,'completed','Synchronous NAS transfer and verification returned with handles closed',now)
                for obj in manifest['objects']:
                    conn.execute(text('INSERT INTO object_backup_receipts VALUES(:id,:sha,:size,:set_id,:path,:manifest,:now,1)'),
                                 dict(id=str(uuid.uuid5(uuid.UUID(set_id),obj['sha256'])),sha=obj['sha256'],size=obj['byte_size'],
                                      set_id=set_id,path=object_path(obj['sha256']),manifest=row['manifest_sha'],now=now))
                conn.execute(text("UPDATE backup_sets SET state='SEALED',sealed_at=:at WHERE id=:id"),
                             dict(id=set_id,at=verified['seal']['sealed_at_ms']))
                conn.execute(text("DELETE FROM object_pins WHERE kind='backup' AND reference_id=:id"),dict(id=set_id))
                # JobService's last owner/clock CAS commits or rolls back ALL effects.
            return result

    def confirm_stopped(self, lease, *, disposition, confirmation):
        """Trusted adapter AFTER observed original process/request termination.

        Cancellation requests, expired leases and lost connections do not qualify.
        Confirmation contains a sanitized observation label, never credentials.
        """
        if disposition not in {'completed','cancelled','not_started'} or not isinstance(confirmation,str) or not 1 <= len(confirmation.strip()) <= 500:
            raise ValueError('Observed terminal confirmation required')
        set_id = self.snapshots.set_id(lease)
        with self._execution(set_id), self.locks.mutation(), self.jobs._writer() as (conn,now):
            row = self.snapshots._row(conn,set_id)
            self._bound(row,lease)
            self._fenced(conn,row)
            if row['state'] == 'SEALED':
                raise BackupError('Successful backup cannot be retired')
            return self._stop(conn,row,disposition,confirmation.strip(),now)

    def _stopped(self, conn, set_id):
        row = self.snapshots._row(conn,set_id)
        self._fenced(conn,row)
        stop = conn.execute(text('SELECT owner_digest FROM backup_execution_stops WHERE set_id=:id'),dict(id=set_id)).scalar()
        if stop != row['owner_digest'] or row['state'] == 'SEALED':
            raise BackupError('Observed failed-execution stop is required')
        return row

    @staticmethod
    def _files(root, directory, allowed):
        """Shallow owned namespace only; reject unknowns before removing anything."""
        result = []
        with ExitStack() as stack:
            try:
                fd = stack.enter_context(root.directory(directory))
            except FileNotFoundError:
                root.validate()
                return []
            with os.scandir(fd) as entries:
                for entry in entries:
                    if len(result) >= 10000 or not allowed(entry.name):
                        raise BackupError('Unexpected or excessive backup cleanup entries')
                    path = f'{directory}/{entry.name}'
                    with root.file(path):
                        pass
                    result.append(path)
            # Retry may observe an earlier unlink whose directory flush failed.
            os.fsync(fd)
        return sorted(result)

    def retire(self, set_id):
        """Remove owned partial files after stop; keep shared originals/sealed sets.

        Empty private namespace directories and immutable DB history remain.
        An interrupted cleanup retries exact paths; pins survive until both roots
        have been checked, file unlinks flushed, and the local transaction commits.
        """
        with self._execution(set_id):
            with self.engine.connect() as conn:
                row = self._stopped(conn,set_id)
                if row['state'] == 'FAILED':
                    return False
            with self.locks.backup_mutation():
                self.store.remote.validate()
                staging = self._files(self.store.remote,f'staging/{set_id}',lambda name: bool(re.fullmatch(r'[0-9a-f]{32}\.part',name)))
                paths = self.store.paths(set_id)
                files = self._files(self.store.remote,f'sets/{set_id}',lambda name: name in {'content.sqlite3','manifest.json','SEALED.json'})
                # Even an untrusted/damaged seal is recovery evidence, never a
                # reason to erase a potentially complete backup after lost CAS.
                keep = paths['seal'] in files
                for path in staging + ([] if keep else sorted(files,key=lambda p:p.endswith('/manifest.json'))):
                    self.store.remote.unlink(path)
            with self.locks.mutation(), self.jobs._writer() as (conn,now):
                self._stopped(conn,set_id)
                files = self._files(self.root,f'backup/sets/{set_id}',lambda name: name in {'content.sqlite3','manifest.json'})
                for path in files:
                    self.root.unlink(path)
                conn.execute(text("UPDATE backup_sets SET state='FAILED',failure=:reason WHERE id=:id"),
                             dict(id=set_id,reason='observed_stop_sealed_preserved' if keep else 'observed_stop_partials_removed'))
                conn.execute(text("DELETE FROM object_pins WHERE kind='backup' AND reference_id=:id"),dict(id=set_id))
            return True
