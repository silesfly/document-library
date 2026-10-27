"""Retire only physically stopped, publication-fenced installation generations.

A trusted execution adapter records observed stop. Cancellation/lease expiration
alone is insufficient. Worker staging and unknown untracked files are not swept.
"""
import uuid

from sqlalchemy import text

from .auth import digest
from .publication import PublicationError, object_path
from .storage import StorageBusy


class OrphanService:
    def __init__(self, publisher, *, quarantine_grace_ms=7*86_400_000):
        if type(quarantine_grace_ms) is not int or quarantine_grace_ms < 0:
            raise ValueError('Nonnegative quarantine grace required')
        self.publisher = publisher
        self.root, self.locks = publisher.root, publisher.locks
        self.jobs, self.engine = publisher.jobs, publisher.engine
        self.grace = quarantine_grace_ms

    def _intent(self, conn, intent_id):
        if str(uuid.UUID(intent_id)) != intent_id:
            raise PublicationError('Canonical intent UUID required')
        row = conn.execute(text('SELECT * FROM storage_intents WHERE id=:id'),dict(id=intent_id)).mappings().one()
        if row['target_relpath'] != object_path(row['object_sha']):
            raise PublicationError('Unexpected installation target')
        return row

    @staticmethod
    def paths(row):
        base = f'trash/installations/{row["id"]}'
        return dict(prepared=row['prepared_relpath'],target=row['target_relpath'],
                    prepared_archive=f'{base}/prepared.bin',target_archive=f'{base}/object.bin')

    def _fenced(self, conn, row):
        job = conn.execute(text('SELECT * FROM jobs WHERE id=:id'),dict(id=row['job_id'])).mappings().one()
        retired = (job['lease_generation'] > row['generation']
                   or (job['lease_generation'] == row['generation']
                       and (job['state'] in {'failed','cancelled'}
                            or (job['state'] == 'completed' and row['state'] == 'committed'))))
        if not retired:
            raise PublicationError('Installation generation can still publish or retry')
        if conn.execute(text('SELECT 1 FROM resource_slots WHERE job_id=:id AND generation=:generation'),
                        dict(id=row['job_id'],generation=row['generation'])).first():
            raise PublicationError('External execution still holds a resource')

    def confirm_stopped(self, lease, *, disposition, confirmation):
        """Trusted adapter only, AFTER observing process/request termination.

        Never called on a timeout or on cancellation request alone. Receipt must
        identify the original owner and generation, even if a successor runs.
        """
        if disposition not in {'completed','cancelled','not_started'} or not isinstance(confirmation,str) or not 1 <= len(confirmation.strip()) <= 500:
            raise ValueError('Observed terminal confirmation required')
        with self.locks.mutation(), self.jobs._writer() as (conn,now):
            rows = conn.execute(text('SELECT * FROM storage_intents WHERE job_id=:id AND generation=:generation'),
                                dict(id=lease.job_id,generation=lease.generation)).mappings().all()
            if not rows or any(not row['prepared_relpath'].startswith(self.publisher._prepared_prefix(lease)) for row in rows):
                raise PublicationError('Original installation owner/generation differs')
            for row in rows:
                self._fenced(conn,row)
            values = dict(job=lease.job_id,generation=lease.generation,owner=digest(lease.owner),
                          disposition=disposition,confirmation=confirmation.strip(),now=now)
            previous = conn.execute(text('SELECT * FROM installation_stops WHERE job_id=:job AND generation=:generation'),values).mappings().first()
            if previous:
                if (previous['owner_digest'],previous['disposition'],previous['confirmation']) != (values['owner'],disposition,values['confirmation']):
                    raise PublicationError('Stop receipt conflict')
                return False
            conn.execute(text('INSERT INTO installation_stops VALUES(:job,:generation,:owner,:disposition,:confirmation,:now)'),values)
            return True

    def _stopped(self, conn, row):
        self._fenced(conn,row)
        receipt = conn.execute(text('SELECT * FROM installation_stops WHERE job_id=:id AND generation=:generation'),
                               dict(id=row['job_id'],generation=row['generation'])).mappings().first()
        if not receipt:
            raise PublicationError('Observed termination receipt is absent')
        prefix = f'quarantine/prepared/jobs/{row["job_id"]}/generation-{row["generation"]}/{receipt["owner_digest"][:16]}/'
        if not row['prepared_relpath'].startswith(prefix):
            raise PublicationError('Prepared file belongs to a different execution')

    def _verified(self, intent_id, *, archives_only=False):
        with self.engine.connect() as conn:
            first = self._intent(conn,intent_id)
        def open_files(stack):
            with self.engine.connect() as conn:
                row = dict(self._intent(conn,intent_id))
                self._stopped(conn,row)
            paths = self.paths(row)
            opened = {}
            for key,path in paths.items():
                if archives_only and not key.endswith('_archive'):
                    continue
                try:
                    opened[path] = stack.enter_context(self.root.file(path))
                except FileNotFoundError:
                    pass
            return row,opened
        with self.locks.readers([first['object_sha']],open_files,blocking=False) as (row,opened):
            asset = dict(sha256=row['object_sha'],byte_size=row['byte_size'])
            verified = {path:self.publisher._hash_fd(fd,asset) for path,fd in opened.items()}
        return row,verified

    def _unchanged(self, paths, verified):
        for path in paths:
            try:
                self.publisher._unchanged(path,verified)
            except FileNotFoundError:
                if path in verified:
                    raise PublicationError('Installation file disappeared after validation')

    def _record(self, conn, intent_id):
        return conn.execute(text('SELECT * FROM installation_retirements WHERE intent_id=:id'),dict(id=intent_id)).mappings().first()

    def _event(self, conn, row, action, detail, now):
        conn.execute(text('INSERT INTO installation_retirement_events VALUES(:id,:intent,:action,:detail,:now)'),
                     dict(id=str(uuid.uuid4()),intent=row['id'],action=action,detail=detail,now=now))

    def _protected(self, conn, row):
        sha = row['object_sha']
        # Even non-LIVE registered objects belong to the object lifecycle, not
        # private installation cleanup. Protect unrelated recoverable intents.
        if conn.execute(text('SELECT 1 FROM objects WHERE sha256=:sha'),dict(sha=sha)).first():
            return True
        return any((kind,reference) != ('intent',row['id']) for kind,reference in self.publisher.reference_sources(conn,sha))

    def retire(self, intent_id):
        try:
            row,verified = self._verified(intent_id)
            paths = self.paths(row)
            with self.locks.mutation([row['object_sha']],blocking=False):
                self._unchanged(paths.values(),verified)
                with self.jobs._writer() as (conn,now):
                    row = self._intent(conn,intent_id)
                    self._stopped(conn,row)
                    previous = self._record(conn,intent_id)
                    if previous and previous['state'] != 'retiring':
                        self._validate_archives(previous,paths,verified)
                        return dict(previous)
                    if not previous:
                        conn.execute(text("INSERT INTO installation_retirements VALUES(:id,'retiring','pending','pending',:now,NULL,NULL)"),dict(id=intent_id,now=now))
                        if row['state'] != 'committed':
                            conn.execute(text("UPDATE storage_intents SET state='abandoned' WHERE id=:id"),dict(id=intent_id))
                        self._event(conn,row,'retire_started','Execution stopped and publication generation fenced',now)
                return self._finish_retirement(intent_id,verified)
        except StorageBusy:
            return dict(intent_id=intent_id,state='busy')

    def _finish_retirement(self, intent_id, verified):
        with self.jobs._writer() as (conn,_now):
            row = self._intent(conn,intent_id)
            self._stopped(conn,row)
            paths = self.paths(row)
            protected = self._protected(conn,row)
            actions = {}
            for kind in ('prepared','target'):
                source, archive = paths[kind],paths[kind+'_archive']
                shared = kind == 'target' and protected
                if archive in verified:
                    if source in verified and not shared:
                        raise PublicationError('Ambiguous original and retired installation copies')
                    self.root.sync_parent(source)
                    self.root.sync_parent(archive)
                    actions[kind] = 'moved'
                elif shared:
                    actions[kind] = 'protected'
                elif source in verified:
                    self.root.move(source,archive)
                    actions[kind] = 'moved'
                else:
                    actions[kind] = 'absent'
            now = self.jobs._now_in(conn)
            state = 'quarantined' if 'moved' in actions.values() else ('released' if protected else 'missing')
            conn.execute(text('''UPDATE installation_retirements SET state=:state,prepared_action=:prepared,
                target_action=:target,quarantined_at=:now WHERE intent_id=:id'''),dict(state=state,**actions,now=now,id=intent_id))
            self._event(conn,row,state,f'prepared={actions["prepared"]},target={actions["target"]}',now)
            return dict(self._record(conn,intent_id))

    @staticmethod
    def _validate_archives(record, paths, verified):
        expected = {paths[kind+'_archive'] for kind in ('prepared','target') if record[kind+'_action'] == 'moved'}
        actual = {path for key,path in paths.items() if key.endswith('_archive') and path in verified}
        if record['state'] == 'quarantined' and actual != expected:
            raise PublicationError('Retired installation artifact is missing or unexpected')
        if record['state'] in {'purged','released','missing'} and actual:
            raise PublicationError('Files appeared after terminal retirement')
        if record['state'] == 'purging' and not actual <= expected:
            raise PublicationError('Unexpected archive appeared during purge')

    def purge(self, intent_id):
        try:
            row,verified = self._verified(intent_id,archives_only=True)
            paths = self.paths(row)
            archives = [paths[kind+'_archive'] for kind in ('prepared','target')]
            with self.locks.mutation([row['object_sha']],blocking=False):
                self._unchanged(archives,verified)
                with self.jobs._writer() as (conn,now):
                    self._stopped(conn,row)
                    record = self._record(conn,intent_id)
                    if not record:
                        raise PublicationError('Installation has not been retired')
                    self._validate_archives(record,paths,verified)
                    if record['state'] in {'purged','released','missing'}:
                        return dict(record)
                    if record['state'] not in {'quarantined','purging'}:
                        raise PublicationError('Retirement must finish before purge')
                    if record['quarantined_at'] is None or now < record['quarantined_at'] + self.grace:
                        return dict(record)
                    conn.execute(text("UPDATE installation_retirements SET state='purging' WHERE intent_id=:id"),dict(id=intent_id))
                return self._finish_purge(intent_id,verified)
        except StorageBusy:
            return dict(intent_id=intent_id,state='busy')

    def _finish_purge(self, intent_id, verified):
        with self.jobs._writer() as (conn,now):
            row = self._intent(conn,intent_id)
            self._stopped(conn,row)
            record = self._record(conn,intent_id)
            if record['state'] != 'purging' or now < record['quarantined_at'] + self.grace:
                raise PublicationError('Purge authority changed')
            paths = self.paths(row)
            for kind in ('prepared','target'):
                archive = paths[kind+'_archive']
                if archive in verified:
                    self.root.unlink(archive)
            # Also flush after a previous unlink survived a DB/fsync failure.
            self.root.sync_parent(paths['prepared_archive'])
            now = self.jobs._now_in(conn)
            conn.execute(text("UPDATE installation_retirements SET state='purged',purged_at=:now WHERE intent_id=:id"),dict(id=intent_id,now=now))
            self._event(conn,row,'purged','Only generation-private retirement archives removed',now)
            return dict(self._record(conn,intent_id))
