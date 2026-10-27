"""Durable worker ownership. All times are integer milliseconds, not lease proofs."""
from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import platform
import re
import secrets
import subprocess
import time
import uuid

from sqlalchemy import text

from .auth import digest
from .database import write_transaction


class JobError(Exception):
    pass


class LostLease(JobError):
    pass


class ClockBlocked(JobError):
    pass


class ResourceBusy(JobError):
    pass


@dataclass(frozen=True)
class Lease:
    job_id: str
    generation: int
    owner: str

    @property
    def staging_key(self):
        # A relative namespace only. The storage authority creates/validates directories.
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("Invalid generation")
        return f"jobs/{uuid.UUID(self.job_id)}/generation-{self.generation}/{digest(self.owner)[:16]}"


@dataclass(frozen=True, repr=False)
class ResourceHold:
    resource: str
    lease: Lease
    token: str


class SystemClock:
    def __init__(self):
        system = platform.system()
        if system == "Linux":
            boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        elif system == "Darwin":
            boot = subprocess.check_output(['/usr/sbin/sysctl', '-n', 'kern.boottime'], text=True).strip()
        else:
            raise RuntimeError("Job clock needs a qualified stable boot identity")
        self.epoch = digest(platform.node() + ":" + boot)

    def sample(self):
        return self.epoch, time.time_ns() // 1000000, time.monotonic_ns() // 1000000


def json_object(value):
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    if len(encoded.encode('utf-8')) > 65536:
        raise ValueError("JSON object exceeds 64 KiB")
    return encoded


def identifier(value, maximum=128):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,' + str(maximum) + '}', value):
        raise ValueError("Invalid identifier")
    return value


class JobService:
    CLOCK_TOLERANCE_MS = 5000

    def __init__(self, engine, *, clock=None):
        self.engine = engine
        self.clock = clock if clock is not None else SystemClock()

    def _sample(self):
        epoch, wall, mono = self.clock.sample()
        if not isinstance(epoch, str) or not epoch or any(type(v) is not int or v < 0 for v in (wall, mono)):
            raise ClockBlocked("Invalid clock sample")
        return epoch, wall, mono

    def _block_clock(self, conn, reason):
        conn.execute(text("UPDATE job_clock SET blocked=1,fence=fence+1,reason=:reason WHERE id=1"), dict(reason=reason))

    def _observe(self, conn):
        previous = conn.execute(text("SELECT * FROM job_clock WHERE id=1")).mappings().first()
        if previous and previous['blocked']:
            return previous['wall_ms'], previous['reason']
        try:
            epoch, wall, mono = self._sample()
        except ClockBlocked:
            if not previous:
                raise
            reason = 'invalid_clock_sample'
            self._block_clock(conn, reason)
            return previous['wall_ms'], reason
        if not previous:
            conn.execute(text("INSERT INTO job_clock(id,epoch,wall_ms,mono_ms) VALUES(1,:epoch,:wall,:mono)"),
                         dict(epoch=epoch, wall=wall, mono=mono))
            return wall, None
        reason = None
        if epoch != previous['epoch']:
            reason = 'clock_epoch_changed'
        elif wall < previous['wall_ms'] or mono < previous['mono_ms']:
            reason = 'clock_reversed'
        elif abs((wall - previous['wall_ms']) - (mono - previous['mono_ms'])) > self.CLOCK_TOLERANCE_MS:
            reason = 'clock_jump'
        if reason:
            self._block_clock(conn, reason)
        else:
            conn.execute(text("UPDATE job_clock SET wall_ms=:wall,mono_ms=:mono WHERE id=1"), dict(wall=wall, mono=mono))
        return wall, reason

    @contextmanager
    def _writer(self):
        # Commit a newly observed clock fence even when no job operation may run.
        with write_transaction(self.engine) as conn:
            now, reason = self._observe(conn)
            if reason is None:
                savepoint = conn.begin_nested()
                try:
                    yield conn, now
                    _end, end_reason = self._observe(conn)
                    if end_reason:
                        raise ClockBlocked(end_reason)
                except ClockBlocked as exc:
                    savepoint.rollback()
                    reason = str(exc)
                    self._block_clock(conn, reason)
                except BaseException:
                    savepoint.rollback()
                    raise
                else:
                    savepoint.commit()
        if reason is not None:
            raise ClockBlocked(reason)

    def _now_in(self, conn):
        now, reason = self._observe(conn)
        if reason:
            # _writer rolls back its savepoint and commits only the clock fence.
            raise ClockBlocked(reason)
        return now

    def clock_status(self):
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM job_clock WHERE id=1")).mappings().first()
            return dict(row) if row else None

    def reconcile_clock(self, *, expected_fence, reason):
        """Local operator after inspecting the fault. Never frees resource slots."""
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise ValueError("Reconciliation reason required")
        with write_transaction(self.engine) as conn:
            epoch, wall, mono = self._sample()
            row = conn.execute(text("SELECT * FROM job_clock WHERE id=1")).mappings().one()
            if not row['blocked'] or row['fence'] != expected_fence:
                raise JobError("Clock reconciliation changed")
            changed = conn.execute(text("""UPDATE jobs SET
                state=CASE WHEN attempts>=max_attempts THEN 'failed' ELSE 'retry_wait' END,
                lease_generation=lease_generation+1,lease_owner=NULL,lease_until=NULL,
                next_run_at=:now,updated_at=:now,error_code='clock_reconciled' WHERE state='running'"""), dict(now=wall)).rowcount
            conn.execute(text("UPDATE job_clock SET epoch=:epoch,wall_ms=:wall,mono_ms=:mono,blocked=0,reason='' WHERE id=1"),
                         dict(epoch=epoch, wall=wall, mono=mono))
            conn.execute(text("INSERT INTO job_clock_reconciliations(old_fence,reason,epoch,wall_ms,invalidated_jobs) VALUES(:fence,:reason,:epoch,:wall,:count)"),
                         dict(fence=expected_fence, reason=reason.strip(), epoch=epoch, wall=wall, count=changed))
            return changed

    def enqueue(self, **request):
        with self._writer() as (conn, _now):
            return self.enqueue_in(conn, **request)

    def enqueue_in(self, conn, *, kind, idempotency_key, payload, priority=10, max_attempts=3,
                   available_at=None, work_id=None, input_revision_id=None, object_sha=None, upload_session_id=None):
        """Caller must own a BEGIN IMMEDIATE transaction; supports atomic followups."""
        identifier(kind, 64)
        identifier(idempotency_key, 200)
        if type(priority) is not int or not 0 <= priority <= 1000 or type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("Invalid job policy")
        if available_at is not None and (type(available_at) is not int or available_at < 0):
            raise ValueError("Invalid schedule")
        data = json_object(payload)
        request = dict(kind=kind, payload=payload, priority=priority, max_attempts=max_attempts, available_at=available_at,
                       work_id=work_id, input_revision_id=input_revision_id, object_sha=object_sha, upload_session_id=upload_session_id)
        fingerprint = digest(json.dumps(request, sort_keys=True, separators=(',', ':'), allow_nan=False))
        old = conn.execute(text("SELECT id,payload_digest FROM jobs WHERE idempotency_key=:key"), dict(key=idempotency_key)).mappings().first()
        if old:
            if old['payload_digest'] != fingerprint:
                raise JobError("Idempotency conflict")
            return old['id']
        now = self._sample()[1]
        ident = str(uuid.uuid4())
        conn.execute(text("""INSERT INTO jobs(id,kind,work_id,input_revision_id,object_sha,upload_session_id,
            payload_json,payload_digest,idempotency_key,priority,state,max_attempts,next_run_at,created_at,updated_at)
            VALUES(:id,:kind,:work,:revision,:sha,:upload,:payload,:digest,:key,:priority,'queued',:maximum,:available,:now,:now)"""),
            dict(id=ident, kind=kind, work=work_id, revision=input_revision_id, sha=object_sha, upload=upload_session_id,
                 payload=data, digest=fingerprint, key=idempotency_key, priority=priority, maximum=max_attempts,
                 available=now if available_at is None else available_at, now=now))
        return ident

    def claim(self, owner, *, lease_ms=30000, kind=None):
        identifier(owner)
        self._duration(lease_ms)
        if kind is not None:
            identifier(kind, 64)
        with self._writer() as (conn, now):
            conn.execute(text("""UPDATE jobs SET state='failed',lease_generation=lease_generation+1,
                lease_owner=NULL,lease_until=NULL,error_code='attempts_exhausted',updated_at=:now
                WHERE state='running' AND lease_until<=:now AND attempts>=max_attempts"""), dict(now=now))
            row = conn.execute(text("""SELECT id FROM jobs WHERE attempts<max_attempts AND
                ((state IN ('queued','retry_wait') AND next_run_at<=:now) OR (state='running' AND lease_until<=:now))
                AND (:kind IS NULL OR kind=:kind) ORDER BY priority,created_at,id LIMIT 1"""), dict(now=now, kind=kind)).first()
            if not row:
                return None
            generation = conn.execute(text("""UPDATE jobs SET state='running',attempts=attempts+1,
                lease_generation=lease_generation+1,lease_owner=:owner,lease_until=:until,updated_at=:now
                WHERE id=:id RETURNING lease_generation"""), dict(owner=owner, until=now+lease_ms, now=now, id=row.id)).scalar_one()
            return Lease(row.id, generation, owner)

    @staticmethod
    def _duration(value):
        if type(value) is not int or not 1000 <= value <= 300000:
            raise ValueError("Lease duration must be 1–300 seconds")

    def _cas(self, conn, lease, assignment, params, *, now=None):
        now = self._now_in(conn) if now is None else now
        values = dict(params, id=lease.job_id, generation=lease.generation, owner=lease.owner, now=now)
        changed = conn.execute(text("UPDATE jobs SET " + assignment + ",updated_at=:now WHERE id=:id "
            "AND state='running' AND lease_generation=:generation AND lease_owner=:owner AND lease_until>:now"), values).rowcount
        if changed != 1:
            raise LostLease("Worker no longer owns this job")
        return now

    def heartbeat(self, lease, *, lease_ms=30000):
        self._duration(lease_ms)
        with self._writer() as (conn, now):
            self._cas(conn, lease, "lease_until=MAX(lease_until,:until)", dict(until=now+lease_ms), now=now)

    def checkpoint(self, lease, value):
        encoded = json_object(value)
        with self._writer() as (conn, now):
            self._cas(conn, lease, "checkpoint_json=:checkpoint", dict(checkpoint=encoded), now=now)

    def fail(self, lease, error_code, *, retry_delay_ms=1000):
        identifier(error_code, 80)
        if type(retry_delay_ms) is not int or not 0 <= retry_delay_ms <= 86400000:
            raise ValueError("Invalid retry delay")
        with self._writer() as (conn, now):
            self._cas(conn, lease, "state=CASE WHEN attempts<max_attempts THEN 'retry_wait' ELSE 'failed' END, "
                "lease_owner=NULL,lease_until=NULL,next_run_at=:retry,error_code=:error",
                dict(retry=now+retry_delay_ms, error=error_code), now=now)

    def cancel(self, job_id):
        """Trusted control plane only. Cancellation does not confirm external stop."""
        with self._writer() as (conn, now):
            return conn.execute(text("""UPDATE jobs SET state='cancelled',lease_generation=lease_generation+1,
                lease_owner=NULL,lease_until=NULL,updated_at=:now WHERE id=:id
                AND state IN ('queued','running','retry_wait')"""), dict(id=job_id, now=now)).rowcount == 1

    def get(self, job_id):
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM jobs WHERE id=:id"), dict(id=job_id)).mappings().first()
            return dict(row) if row else None

    @contextmanager
    def ownership(self, lease):
        """Short owned writer for durable intents, without completing the job."""
        with self._writer() as (conn, now):
            self._cas(conn, lease, "lease_generation=lease_generation", {}, now=now)
            yield conn
            self._cas(conn, lease, "lease_generation=lease_generation", {})

    @contextmanager
    def completion(self, lease, *, result, followups=(), outbox=(), finalize=None):
        """Same writer; optional DB-only authority finalizer runs after all effects."""
        with self._writer() as (conn, now):
            self._cas(conn, lease, "lease_generation=lease_generation", {}, now=now)
            yield conn
            self._complete(conn, lease, result=result, followups=followups, outbox=outbox, finalize=finalize)

    def _complete(self, conn, lease, *, result, followups=(), outbox=(), finalize=None):
        """Final CAS after all effects. completion() owns rollback and clock fencing."""
        if len(followups) > 100 or len(outbox) > 100:
            raise ValueError("Too many completion effects")
        encoded = json_object(result)
        now = self._sample()[1]
        for request in followups:
            self.enqueue_in(conn, **request)
        for event in outbox:
            identifier(event['kind'], 64)
            identifier(event['dedupe_key'], 200)
            conn.execute(text("INSERT INTO job_outbox(id,job_id,kind,dedupe_key,payload_json,created_at) VALUES(:id,:job,:kind,:key,:payload,:now)"),
                         dict(id=str(uuid.uuid4()), job=lease.job_id, kind=event['kind'], key=event['dedupe_key'], payload=json_object(event['payload']), now=now))
        if finalize is not None:
            finalize(conn)
        self._cas(conn, lease, "state='completed',lease_owner=NULL,lease_until=NULL,result_json=:result,error_code=NULL", dict(result=encoded))

    def acquire_resource(self, lease, resource):
        token = secrets.token_urlsafe(32)
        with self._writer() as (conn, now):
            self._cas(conn, lease, "lease_generation=lease_generation", {}, now=now)
            count = conn.execute(text("""UPDATE resource_slots SET job_id=:id,generation=:generation,owner=:owner,
                token_hash=:token,acquired_at=:now WHERE name=:name AND job_id IS NULL"""),
                dict(id=lease.job_id, generation=lease.generation, owner=lease.owner, token=digest(token), now=now, name=resource)).rowcount
            if count != 1:
                raise ResourceBusy("Resource is held or not configured")
            return ResourceHold(resource, lease, token)

    def confirm_resource_stopped(self, hold, *, disposition, confirmation):
        """Trusted adapter only, AFTER observed terminal response/process exit.

        A timeout, lease expiry, cancellation request or lost connection is NOT
        confirmation. Keep the hold when uncertain. This core records evidence;
        G4 adapters must prove provider-specific stop semantics before calling it.
        """
        if disposition not in {'completed', 'cancelled', 'not_started'} or not isinstance(confirmation, str) or not 1 <= len(confirmation.strip()) <= 500:
            raise ValueError("Explicit terminal confirmation required")
        token_hash = digest(hold.token)
        # Safe to release confirmed occupancy while job clocks are blocked.
        with write_transaction(self.engine) as conn:
            previous = conn.execute(text("SELECT * FROM resource_release_receipts WHERE token_hash=:token"), dict(token=token_hash)).mappings().first()
            values = dict(token=token_hash, resource=hold.resource, job=hold.lease.job_id, generation=hold.lease.generation,
                          owner=hold.lease.owner, disposition=disposition, confirmation=confirmation.strip())
            if previous:
                if any(previous[k] != values[k] for k in ('resource', 'generation', 'owner', 'disposition', 'confirmation')) or previous['job_id'] != values['job']:
                    raise JobError("Release receipt conflict")
                return
            changed = conn.execute(text("""UPDATE resource_slots SET job_id=NULL,generation=NULL,owner=NULL,token_hash=NULL,acquired_at=NULL
                WHERE name=:resource AND job_id=:job AND generation=:generation AND owner=:owner AND token_hash=:token"""), values).rowcount
            if changed != 1:
                raise JobError("Resource hold no longer matches")
            conn.execute(text("INSERT INTO resource_release_receipts VALUES(:token,:resource,:job,:generation,:owner,:disposition,:confirmation,:now)"),
                         dict(values, now=self._sample()[1]))
