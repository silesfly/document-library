"""Disposable SQLite race/failure acceptance; no external AI operation is started."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest

from sqlalchemy import text

from document_library.database import database_engine, migrate, write_transaction
from document_library.jobs import JobService, JobError, LostLease, ClockBlocked, ResourceBusy, SystemClock

ROOT = Path(__file__).resolve().parents[1]


class FakeClock:
    def __init__(self):
        self.epoch, self.wall, self.mono = 'synthetic-boot-a', 1800000000000, 100000

    def sample(self):
        return self.epoch, self.wall, self.mono

    def advance(self, ms):
        self.wall += ms
        self.mono += ms


class JobAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='document-library-jobs-')
        self.path = Path(self.temp.name)
        self.engines = []
        self.addCleanup(self.cleanup)
        self.engine = self.new_engine()
        migrate(self.engine, ROOT / 'migrations')
        self.clock = FakeClock()
        self.jobs = JobService(self.engine, clock=self.clock)
        with write_transaction(self.engine) as conn:
            conn.execute(text('CREATE TABLE publication_probe(id TEXT PRIMARY KEY,value TEXT) STRICT'))

    def cleanup(self):
        for engine in self.engines:
            engine.dispose()
        self.temp.cleanup()
        self.assertFalse(self.path.exists(), 'Temporary project/DB/WAL/SHM must be absent')

    def new_engine(self):
        engine = database_engine(self.path / 'synthetic.sqlite3')
        self.engines.append(engine)
        return engine

    def enqueue(self, key='test', **kwargs):
        return self.jobs.enqueue(kind='test', idempotency_key=key, payload={'input':'synthetic'}, **kwargs)

    def scalar(self, sql):
        with self.engine.connect() as conn:
            return conn.execute(text(sql)).scalar_one()

    def publish(self, lease, **kwargs):
        with self.jobs.completion(lease, result={'published':True}, **kwargs) as conn:
            conn.execute(text('INSERT INTO publication_probe VALUES(:id,:value)'),dict(id=lease.job_id,value='synthetic'))

    def test_enqueue_idempotency_and_followup_rollback(self):
        ident = self.enqueue()
        self.assertEqual(self.enqueue(),ident)
        with self.assertRaises(JobError):
            self.enqueue(priority=2)
        with self.assertRaises(RuntimeError):
            with write_transaction(self.engine) as conn:
                self.jobs.enqueue_in(conn, kind='next', idempotency_key='rollback', payload={})
                raise RuntimeError('synthetic transaction failure')
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),1)
        with self.assertRaises(ValueError):
            self.jobs.enqueue(kind='test', idempotency_key='nan', payload={'bad':float('nan')})
        with self.assertRaises(ValueError):
            self.jobs.enqueue(kind='test', idempotency_key='large', payload={'bad':'x'*65537})

    def test_two_connections_claim_only_one_owner(self):
        ident=self.enqueue()
        second=JobService(self.new_engine(),clock=self.clock)
        barrier=threading.Barrier(2)
        def claim(args):
            service,owner=args
            barrier.wait()
            return service.claim(owner)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(claim,[(self.jobs,'worker-a'),(second,'worker-b')]))
        wins=[r for r in results if r is not None]
        self.assertEqual(len(wins),1)
        self.assertEqual(wins[0].job_id,ident)
        self.assertEqual(self.jobs.get(ident)['attempts'],1)

    def test_priority_kind_and_schedule(self):
        later=self.enqueue('later',priority=0,available_at=self.clock.wall+1000)
        low=self.enqueue('low',priority=30)
        high=self.enqueue('high',priority=1)
        other=self.jobs.enqueue(kind='backup',idempotency_key='backup',payload={},priority=0)
        self.assertEqual(self.jobs.claim('one',kind='test').job_id,high)
        self.assertEqual(self.jobs.claim('two',kind='test').job_id,low)
        self.assertEqual(self.jobs.claim('three').job_id,other)
        self.assertIsNone(self.jobs.claim('four'))
        self.clock.advance(1000)
        self.assertEqual(self.jobs.claim('four').job_id,later)

    def test_expired_generation_cannot_mutate_or_publish(self):
        ident=self.enqueue()
        old=self.jobs.claim('worker-a',lease_ms=1000)
        self.jobs.checkpoint(old,{'page':7})
        self.clock.advance(1000)
        second=JobService(self.new_engine(),clock=self.clock)
        current=second.claim('worker-b')
        self.assertGreater(current.generation,old.generation)
        self.assertNotEqual(current.staging_key,old.staging_key)
        for action in [lambda:self.jobs.heartbeat(old),lambda:self.jobs.checkpoint(old,{'page':999}),
                       lambda:self.jobs.fail(old,'late_failure'),lambda:self.publish(old),
                       lambda:self.jobs.acquire_resource(old,'local-qwen')]:
            with self.assertRaises(LostLease):
                action()
        for invalid in [replace(current,owner='wrong-owner'),replace(current,generation=current.generation+1)]:
            with self.assertRaises(LostLease):
                self.jobs.checkpoint(invalid,{'page':999})
        row=self.jobs.get(ident)
        self.assertEqual(row['checkpoint_json'],'{"page":7}')
        self.assertEqual(row['lease_owner'],'worker-b')
        self.publish(current)
        self.assertEqual(self.jobs.get(ident)['state'],'completed')
        with self.assertRaises(LostLease):
            self.jobs.heartbeat(current)

    def test_heartbeat_never_shortens_and_retry_exhaustion_is_terminal(self):
        ident=self.enqueue(max_attempts=2)
        first=self.jobs.claim('worker',lease_ms=20000)
        until=self.jobs.get(ident)['lease_until']
        self.clock.advance(1000)
        self.jobs.heartbeat(first,lease_ms=1000)
        self.assertEqual(self.jobs.get(ident)['lease_until'],until)
        self.jobs.fail(first,'synthetic_error',retry_delay_ms=1000)
        self.assertIsNone(self.jobs.claim('worker'))
        self.clock.advance(1000)
        second=self.jobs.claim('worker',lease_ms=1000)
        self.clock.advance(1000)
        self.assertIsNone(self.jobs.claim('worker'))
        self.assertEqual(self.jobs.get(ident)['state'],'failed')
        with self.assertRaises(LostLease):
            self.jobs.checkpoint(second,{})

    def test_publication_results_followups_and_outbox_are_one_commit(self):
        ident=self.enqueue()
        lease=self.jobs.claim('worker')
        effects=dict(followups=[dict(kind='backup',idempotency_key='backup-after',payload={'source':ident})],
                     outbox=[dict(kind='revision_published',dedupe_key='published-event',payload={'source':ident})])
        with self.assertRaises(RuntimeError):
            with self.jobs.completion(lease,result={},**effects) as conn:
                conn.execute(text("INSERT INTO publication_probe VALUES('rollback','test')"))
                raise RuntimeError('current input or rights validation failed')
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_probe'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),1)
        self.publish(lease,**effects)
        self.assertEqual(self.jobs.get(ident)['state'],'completed')
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),2)

    def test_followup_failure_rolls_back_already_written_result_and_job(self):
        ident=self.enqueue()
        self.enqueue('conflict')
        lease=self.jobs.claim('worker')
        with self.assertRaises(JobError):
            self.publish(lease,followups=[dict(kind='different',idempotency_key='conflict',payload={})])
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_probe'),0)
        self.assertEqual(self.jobs.get(lease.job_id)['state'],'running')
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),2)

    def test_lease_expiry_inside_publication_rolls_back_data(self):
        ident=self.enqueue()
        lease=self.jobs.claim('worker',lease_ms=1000)
        with self.assertRaises(LostLease):
            with self.jobs.completion(lease,result={}) as conn:
                conn.execute(text("INSERT INTO publication_probe VALUES('late','test')"))
                self.clock.advance(1000)
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_probe'),0)
        self.assertEqual(self.jobs.get(ident)['state'],'running')

    def test_clock_jump_during_publication_persists_fence_without_results(self):
        ident=self.enqueue()
        lease=self.jobs.claim('worker')
        with self.assertRaises(ClockBlocked):
            with self.jobs.completion(lease,result={},outbox=[dict(kind='event',dedupe_key='no-leak',payload={})]) as conn:
                conn.execute(text("INSERT INTO publication_probe VALUES('jump','test')"))
                self.clock.wall+=10000
        self.assertEqual(self.scalar('SELECT count(*) FROM publication_probe'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM job_outbox'),0)
        self.assertEqual(self.jobs.get(ident)['state'],'running')
        self.assertEqual(self.jobs.clock_status()['blocked'],1)
        self.clock.wall-=10000
        with self.assertRaises(ClockBlocked):
            JobService(self.new_engine(),clock=self.clock).claim('second')

    def test_invalid_clock_sample_persists_until_explicit_reconciliation(self):
        self.enqueue(max_attempts=10)
        lease=self.jobs.claim('worker')
        for field,value in [('wall',-1),('wall','invalid'),('mono',-1),('epoch','')]:
            with self.subTest(field=field,value=value):
                baseline=self.jobs.clock_status()
                original=getattr(self.clock,field)
                setattr(self.clock,field,value)
                with self.assertRaises(ClockBlocked):
                    self.jobs.heartbeat(lease)
                status=self.jobs.clock_status()
                self.assertEqual(status['blocked'],1)
                self.assertEqual(status['reason'],'invalid_clock_sample')
                self.assertEqual(status['fence'],baseline['fence']+1)
                for column in ['epoch','wall_ms','mono_ms']:
                    self.assertEqual(status[column],baseline[column])
                with self.assertRaises(ClockBlocked):
                    self.jobs.claim('second')
                self.assertEqual(self.jobs.clock_status(),status)
                setattr(self.clock,field,original)
                with self.assertRaises(ClockBlocked):
                    JobService(self.new_engine(),clock=self.clock).checkpoint(lease,{'unexpected':True})
                self.assertEqual(self.jobs.clock_status(),status)
                self.assertEqual(self.jobs.reconcile_clock(expected_fence=status['fence'],reason='synthetic clock repair'),1)
                with self.assertRaises(LostLease):
                    self.jobs.heartbeat(lease)
                lease=self.jobs.claim('recovered')
                self.jobs.checkpoint(lease,{'recovered':True})

    def test_invalid_initial_clock_cannot_create_work(self):
        original=self.clock.wall
        self.clock.wall=-1
        with self.assertRaises(ClockBlocked):
            self.enqueue()
        self.assertIsNone(self.jobs.clock_status())
        self.assertEqual(self.scalar('SELECT count(*) FROM jobs'),0)
        self.clock.wall=original
        self.enqueue()
        self.assertIsNotNone(self.jobs.claim('worker'))

    def test_clock_backwards_epoch_and_reconciliation_invalidate_workers(self):
        self.enqueue()
        lease=self.jobs.claim('worker')
        hold=self.jobs.acquire_resource(lease,'local-qwen')
        self.clock.wall-=1
        with self.assertRaises(ClockBlocked):
            self.jobs.claim('second')
        status=self.jobs.clock_status()
        self.assertEqual(status['reason'],'clock_reversed')
        with self.assertRaises(JobError):
            self.jobs.reconcile_clock(expected_fence=status['fence']+1,reason='synthetic operator check')
        self.assertEqual(self.jobs.reconcile_clock(expected_fence=status['fence'],reason='synthetic operator check'),1)
        current=self.jobs.claim('second')
        with self.assertRaises(LostLease):
            self.jobs.heartbeat(lease)
        with self.assertRaises(ResourceBusy):
            self.jobs.acquire_resource(current,'local-qwen')
        self.clock.epoch='synthetic-boot-b'
        with self.assertRaises(ClockBlocked):
            self.jobs.checkpoint(current,{})
        self.assertEqual(self.jobs.clock_status()['reason'],'clock_epoch_changed')
        self.jobs.confirm_resource_stopped(hold,disposition='completed',confirmation='synthetic terminal response')
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))

    def test_expiry_and_cancel_do_not_release_external_resource(self):
        self.enqueue()
        first=self.jobs.claim('worker-a',lease_ms=1000)
        hold=self.jobs.acquire_resource(first,'local-qwen')
        self.clock.advance(1000)
        second=self.jobs.claim('worker-b')
        with self.assertRaises(ResourceBusy):
            self.jobs.acquire_resource(second,'local-qwen')
        self.jobs.cancel(second.job_id)
        self.assertIsNotNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        with self.assertRaises(LostLease):
            self.jobs.heartbeat(second)
        with self.assertRaises(ValueError):
            self.jobs.confirm_resource_stopped(hold,disposition='cancel_requested',confirmation='not terminal')
        with self.assertRaises(JobError):
            self.jobs.confirm_resource_stopped(replace(hold,token='wrong'),disposition='completed',confirmation='test')
        self.jobs.confirm_resource_stopped(hold,disposition='cancelled',confirmation='synthetic process exit verified')
        self.enqueue('third')
        third=self.jobs.claim('worker-c')
        next_hold=self.jobs.acquire_resource(third,'local-qwen')
        self.jobs.confirm_resource_stopped(hold,disposition='cancelled',confirmation='synthetic process exit verified')
        self.assertEqual(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"),third.job_id)
        with self.assertRaises(JobError):
            self.jobs.confirm_resource_stopped(hold,disposition='completed',confirmation='different evidence')
        self.jobs.confirm_resource_stopped(next_hold,disposition='not_started',confirmation='synthetic no request dispatched')

    def test_concurrent_resource_acquire_has_one_holder(self):
        self.enqueue('first')
        self.enqueue('second')
        a=self.jobs.claim('one')
        b=self.jobs.claim('two')
        barrier=threading.Barrier(2)
        def acquire(lease):
            barrier.wait()
            try:
                return self.jobs.acquire_resource(lease,'local-ocr')
            except ResourceBusy:
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(acquire,[a,b]))
        holders=[h for h in results if h]
        self.assertEqual(len(holders),1)
        self.jobs.confirm_resource_stopped(holders[0],disposition='not_started',confirmation='synthetic no dispatch')

    def test_system_clock_has_stable_same_boot_identity(self):
        first=SystemClock().sample()
        second=SystemClock().sample()
        self.assertEqual(first[0],second[0])
        self.assertGreaterEqual(second[1],first[1])
        self.assertGreaterEqual(second[2],first[2])


if __name__=='__main__':
    unittest.main()
