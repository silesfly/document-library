"""Local retention and crash-recoverable GC. No scheduler or NAS authority.

Only the G0-D3 coordinator may create backup receipts after physical validation.
This module never creates one or interprets a queued backup job as proof.
"""
import uuid

from sqlalchemy import text

from .jobs import identifier
from .publication import PublicationError, object_path
from .storage import StorageBusy


class LifecycleService:
    DAY_MS = 86_400_000

    def __init__(self, publisher, *, unreferenced_grace_ms=7*DAY_MS, quarantine_grace_ms=7*DAY_MS):
        self.publisher = publisher
        self.root, self.locks = publisher.root, publisher.locks
        self.jobs, self.engine = publisher.jobs, publisher.engine
        for value in (unreferenced_grace_ms,quarantine_grace_ms):
            if type(value) is not int or value < 0:
                raise ValueError('Nonnegative explicit retention grace required')
        self.unreferenced_grace = unreferenced_grace_ms
        self.quarantine_grace = quarantine_grace_ms

    @staticmethod
    def trash_path(sha):
        object_path(sha)
        return f'trash/objects/{sha[:2]}/{sha}'

    def _row(self, conn, sha):
        row = conn.execute(text('''SELECT o.*,l.unreferenced_since,l.quarantined_at,l.operation
            FROM objects o LEFT JOIN object_lifecycle l ON l.object_sha=o.sha256 WHERE o.sha256=:sha'''),dict(sha=sha)).mappings().first()
        if not row or row['local_relpath'] != object_path(sha):
            raise PublicationError('Unknown object or unexpected canonical path')
        return row

    def _event(self, conn, sha, action, detail, now):
        conn.execute(text('INSERT INTO lifecycle_events VALUES(:id,:sha,:action,:detail,:now)'),
                     dict(id=str(uuid.uuid4()),sha=sha,action=action,detail=detail,now=now))

    def _reset_grace(self, conn, shas, now):
        for sha in shas:
            conn.execute(text('''INSERT INTO object_lifecycle(object_sha,unreferenced_since) VALUES(:sha,:now)
                ON CONFLICT(object_sha) DO UPDATE SET unreferenced_since=:now'''),dict(sha=sha,now=now))

    def release_revision(self, token, csrf, revision_id, *, reason):
        identifier(reason,200)
        with self.engine.connect() as conn:
            shas = conn.execute(text('SELECT object_sha FROM revision_assets WHERE edition_revision_id=:id'),dict(id=revision_id)).scalars().all()
        with self.locks.mutation(shas), self.jobs._writer() as (conn,now):
            actor = self.publisher.permissions.auth.current(conn,token,csrf=csrf,admin=True)
            row = conn.execute(text('''SELECT r.is_active,s.retained,r.edition_id FROM edition_revisions r
                JOIN revision_seals s ON s.revision_id=r.id WHERE r.id=:id'''),dict(id=revision_id)).first()
            if not row or row.is_active or not conn.execute(text('''SELECT 1 FROM edition_revisions r
                JOIN revision_seals s ON s.revision_id=r.id WHERE r.edition_id=:id AND r.is_active=1'''),dict(id=row.edition_id)).first():
                raise PublicationError('Only a sealed superseded revision may be released')
            if not row.retained:
                return False
            conn.execute(text('UPDATE revision_seals SET retained=0 WHERE revision_id=:id'),dict(id=revision_id))
            self._reset_grace(conn,shas,now)
            self.publisher.permissions.auth.audit(conn,actor['id'],'release_revision',details={'revision_id':revision_id,'reason':reason})
            return True

    def release_pin(self, kind, reference_id, *, confirmation):
        """Trusted adapter AFTER observed stream/copy/restore termination, never timeout."""
        if kind not in {'ticket','restore'}:
            raise ValueError('Ticket/restore required; backup pins belong to BackupCoordinator')
        identifier(reference_id,200)
        identifier(confirmation,200)
        with self.engine.connect() as conn:
            shas = conn.execute(text('SELECT object_sha FROM object_pins WHERE kind=:kind AND reference_id=:id'),dict(kind=kind,id=reference_id)).scalars().all()
        with self.locks.mutation(shas), self.jobs._writer() as (conn,now):
            current = conn.execute(text('SELECT object_sha FROM object_pins WHERE kind=:kind AND reference_id=:id'),dict(kind=kind,id=reference_id)).scalars().all()
            if set(shas) != set(current):
                raise PublicationError('Pin changed before release; retry with its current members')
            conn.execute(text('DELETE FROM object_pins WHERE kind=:kind AND reference_id=:id'),dict(kind=kind,id=reference_id))
            self._reset_grace(conn,shas,now)
            for sha in shas:
                self._event(conn,sha,'release_pin',f'{kind}:{reference_id}:{confirmation}',now)
            return len(shas)

    def _verified_paths(self, sha):
        def open_files(stack):
            with self.engine.connect() as conn:
                row = dict(self._row(conn,sha))
            opened = {}
            for path in (object_path(sha),self.trash_path(sha)):
                try:
                    opened[path] = stack.enter_context(self.root.file(path))
                except FileNotFoundError:
                    pass
            if len(opened) > 1:
                raise PublicationError('Ambiguous canonical and quarantine copies; manual reconciliation required')
            return row,opened
        # GC never waits on global/object locks; hashing holds only shared locks.
        with self.locks.readers([sha],open_files,blocking=False) as (row,opened):
            asset = dict(sha256=sha,byte_size=row['byte_size'])
            return {path:self.publisher._hash_fd(fd,asset) for path,fd in opened.items()}

    def _same_paths(self, sha, verified):
        for path in (object_path(sha),self.trash_path(sha)):
            try:
                with self.root.file(path) as fd:
                    if path not in verified or self.publisher._fingerprint(fd) != verified[path]:
                        raise PublicationError('File changed after validation')
            except FileNotFoundError:
                if path in verified:
                    raise PublicationError('File disappeared after validation')

    def _backup_verified(self, conn, row):
        return conn.execute(text('''SELECT id FROM object_backup_receipts
            WHERE object_sha=:sha AND byte_size=:size AND valid=1 LIMIT 1'''),dict(sha=row['sha256'],size=row['byte_size'])).scalar()

    def _eligible(self, conn, row, now):
        return (not self.publisher.reference_sources(conn,row['sha256'])
                and row['unreferenced_since'] is not None
                and now >= row['unreferenced_since'] + self.unreferenced_grace
                and bool(self._backup_verified(conn,row)))

    def _move(self, source, target):
        self.root.move(source,target)

    def _sync_parent(self, path):
        self.root.sync_parent(path)

    def _unlink(self, path):
        self.root.unlink(path)

    def collect(self, sha):
        """One bounded candidate. Busy and unmet policy are skips, not success."""
        object_path(sha)
        try:
            verified = self._verified_paths(sha)
            with self.locks.mutation([sha],blocking=False):
                self._same_paths(sha,verified)
                with self.jobs._writer() as (conn,now):
                    row = self._row(conn,sha)
                    if row['state'] in {'PURGED','MISSING'}:
                        if row['state'] == 'PURGED' and verified:
                            raise PublicationError('Bytes reappeared after purge; reconciliation required')
                        return row['state'].lower()
                    if row['operation'] == 'restore':
                        return 'restore_pending'
                    if not verified and row['operation'] != 'purge':
                        conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=:sha"),dict(sha=sha))
                        self._event(conn,sha,'missing','No canonical or quarantine file',now)
                        return 'missing'
                    if self.publisher.reference_sources(conn,sha):
                        if row['state'] == 'LIVE':
                            conn.execute(text('UPDATE object_lifecycle SET unreferenced_since=NULL WHERE object_sha=:sha'),dict(sha=sha))
                        return 'protected'
                    if row['unreferenced_since'] is None:
                        self._reset_grace(conn,[sha],now)
                        return 'grace'
                    if not self._eligible(conn,row,now):
                        return 'ineligible'
                    if row['state'] == 'LIVE':
                        if object_path(sha) not in verified:
                            raise PublicationError('LIVE object is outside its canonical location')
                        conn.execute(text("UPDATE objects SET state='QUARANTINED' WHERE sha256=:sha"),dict(sha=sha))
                        conn.execute(text("UPDATE object_lifecycle SET operation='quarantine',quarantined_at=NULL WHERE object_sha=:sha"),dict(sha=sha))
                    elif row['operation'] is None:
                        if self.trash_path(sha) not in verified:
                            raise PublicationError('Quarantine location differs from completed state')
                        if row['quarantined_at'] is None or now < row['quarantined_at'] + self.quarantine_grace:
                            return 'quarantined'
                        conn.execute(text("UPDATE object_lifecycle SET operation='purge' WHERE object_sha=:sha"),dict(sha=sha))
                return self._finish_collection(sha,verified)
        except StorageBusy:
            return 'busy'

    def _finish_collection(self, sha, verified):
        # Called while global/exclusive locks remain held. Intent already durable.
        with self.jobs._writer() as (conn,now):
            row = self._row(conn,sha)
            if row['state'] != 'QUARANTINED' or not self._eligible(conn,row,now):
                return 'ineligible'
            canonical, trash = object_path(sha),self.trash_path(sha)
            if row['operation'] == 'quarantine':
                if canonical in verified:
                    self._move(canonical,trash)
                elif trash in verified:
                    self._sync_parent(canonical)
                    self._sync_parent(trash)
                else:
                    raise PublicationError('Quarantine bytes are absent')
                confirmed = self.jobs._now_in(conn)
                conn.execute(text('UPDATE object_lifecycle SET operation=NULL,quarantined_at=:now WHERE object_sha=:sha'),dict(sha=sha,now=confirmed))
                self._event(conn,sha,'quarantine','Durable local move',confirmed)
                return 'quarantined'
            if row['operation'] == 'purge':
                if row['quarantined_at'] is None or now < row['quarantined_at'] + self.quarantine_grace:
                    return 'ineligible'
                if canonical in verified:
                    raise PublicationError('Cannot purge from canonical location')
                if trash in verified:
                    self._unlink(trash)
                else:
                    self._sync_parent(trash)
                conn.execute(text("UPDATE objects SET state='PURGED' WHERE sha256=:sha"),dict(sha=sha))
                conn.execute(text('UPDATE object_lifecycle SET operation=NULL WHERE object_sha=:sha'),dict(sha=sha))
                self._event(conn,sha,'purge','Durable local unlink; backup receipt retained',now)
                return 'purged'
            return 'quarantined'

    def restore_quarantine(self, token, csrf, sha):
        verified = self._verified_paths(sha)
        with self.locks.mutation([sha]):
            self._same_paths(sha,verified)
            with self.jobs._writer() as (conn,now):
                actor = self.publisher.permissions.auth.current(conn,token,csrf=csrf,admin=True)
                row = self._row(conn,sha)
                if row['state'] != 'QUARANTINED' or row['operation'] == 'purge' or not verified:
                    raise PublicationError('Only available quarantine bytes may be restored locally')
                conn.execute(text("UPDATE object_lifecycle SET operation='restore' WHERE object_sha=:sha"),dict(sha=sha))
                self._event(conn,sha,'restore_requested',actor['id'],now)
            canonical,trash = object_path(sha),self.trash_path(sha)
            if trash in verified:
                self._move(trash,canonical)
            else:
                self._sync_parent(canonical)
                try:
                    self._sync_parent(trash)
                except FileNotFoundError:
                    pass  # Quarantine intent may have stopped before creating trash.
            with self.jobs._writer() as (conn,now):
                self.publisher.permissions.auth.current(conn,token,csrf=csrf,admin=True)
                conn.execute(text("UPDATE objects SET state='LIVE' WHERE sha256=:sha"),dict(sha=sha))
                conn.execute(text('UPDATE object_lifecycle SET operation=NULL,unreferenced_since=NULL,quarantined_at=NULL WHERE object_sha=:sha'),dict(sha=sha))
                self._event(conn,sha,'restore','Verified bytes restored; revision not reactivated',now)
        return 'live'
