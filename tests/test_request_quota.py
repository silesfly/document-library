"""Account-scoped fixed windows and committed admission, on disposable SQLite."""
from concurrent.futures import ThreadPoolExecutor
import threading
from unittest.mock import patch

from sqlalchemy import event, text

from document_library.auth import AuthError, AuthService, digest
from document_library.database import write_transaction
from document_library.request_quota import AccountQuota, QuotaExceeded
from library_helpers import LibraryFixture


class AccountQuotaAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.quota = AccountQuota(self.auth)

    def counter(self, action='recommend'):
        return self.scalar('SELECT coalesce(sum(attempts),0) FROM auth_limits WHERE bucket LIKE :prefix',
                           prefix='g3:'+action+':%')

    def reserve(self, action='social'):
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, self.token, csrf=self.csrf)
            self.quota.reserve_in(conn, actor=actor, action=action)

    def test_limits_are_action_specific_account_durable_and_denial_is_no_new_cost(self):
        for action, ceiling in (('social', 30), ('recommend', 30), ('download', 60)):
            for _ in range(ceiling):
                if action == 'social':
                    self.reserve()
                else:
                    self.quota.admit(self.token, action=action)
            with self.assertRaises(QuotaExceeded) as caught:
                if action == 'social':
                    self.reserve()
                else:
                    self.quota.admit(self.token, action=action)
            error = caught.exception
            self.assertEqual((error.code, error.status, error.retry_after_seconds), ('RATE_LIMITED', 429, 60))
            self.assertEqual(str(error), 'RATE_LIMITED')
            self.assertEqual(self.counter(action), ceiling)
        with self.engine.connect() as conn:
            buckets = conn.execute(text('SELECT bucket FROM auth_limits')).scalars().all()
        self.assertEqual(set(buckets), {'g3:'+action+':'+digest('u') for action in ('social', 'recommend', 'download')})
        for invalid in (True, 0, 61, 1.0):
            with self.assertRaises(ValueError):
                QuotaExceeded(invalid)

    def test_session_replacement_restart_and_other_account(self):
        for _ in range(30):
            self.quota.admit(self.token, action='recommend')
        self.auth.logout(self.token, self.csrf)
        replacement = self.session('synthetic-replacement')
        restarted = AccountQuota(AuthService(self.engine, clock=self.auth.clock))
        with self.assertRaises(QuotaExceeded):
            restarted.admit(replacement, action='recommend')
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('other','other','synthetic','test')"))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,'other',1,:now,:end)"),
                         dict(token=digest('synthetic-other'), csrf=digest(self.csrf), now=self.auth.now(), end=self.auth.now()+3600))
        restarted.admit('synthetic-other', action='recommend')
        self.assertEqual(self.counter(), 31)

    def test_boundaries_and_backwards_clock_are_conservative(self):
        for _ in range(30):
            self.quota.admit(self.token, action='recommend')
        self.h.clock.wall += 59000
        with self.assertRaises(QuotaExceeded) as caught:
            self.quota.admit(self.token, action='recommend')
        self.assertEqual(caught.exception.retry_after_seconds, 1)
        self.h.clock.wall -= 60000
        with self.assertRaises(QuotaExceeded) as caught:
            self.quota.admit(self.token, action='recommend')
        self.assertEqual(caught.exception.retry_after_seconds, 60)
        self.assertEqual(self.counter(), 30)
        self.h.clock.wall += 61000
        self.quota.admit(self.token, action='recommend')
        self.assertEqual(self.counter(), 1)
        # Rollback is denied even for a non-exhausted bucket.
        self.h.clock.wall -= 1000
        with self.assertRaises(QuotaExceeded):
            self.quota.admit(self.token, action='recommend')
        self.assertEqual(self.counter(), 1)

    def test_same_writer_reservation_no_auth_checkout_or_commit_and_rollback(self):
        with self.assertRaisesRegex(RuntimeError, 'synthetic later fault'):
            with write_transaction(self.engine) as conn:
                actor = self.auth.current(conn, self.token)
                with (patch.object(self.auth, 'current', side_effect=AssertionError('nested auth')),
                      patch.object(self.engine, 'connect', side_effect=AssertionError('new checkout')),
                      patch.object(conn, 'commit', side_effect=AssertionError('nested commit'))):
                    self.quota.reserve_in(conn, actor=actor, action='social')
                raise RuntimeError('synthetic later fault')
        self.assertEqual(self.counter('social'), 0)

    def test_committed_admission_survives_downstream_failure(self):
        for action in ('recommend', 'download'):
            with self.assertRaisesRegex(RuntimeError, 'synthetic downstream fault'):
                self.quota.admit(self.token, action=action, csrf=self.csrf)
                raise RuntimeError('synthetic downstream fault')
            self.assertEqual(self.counter(action), 1)
        with self.assertRaises(ValueError):
            self.quota.admit(self.token, action='social')
        self.assertEqual(self.counter('social'), 0)

    def test_fresh_same_writer_auth_optional_csrf_and_expired_session_reject_before_cost(self):
        for csrf in ('', False, 'wrong'):
            with self.assertRaises(AuthError) as caught:
                self.quota.admit(self.token, action='download', csrf=csrf)
            self.assertEqual(caught.exception.status, 403)
        with self.assertRaises(AuthError):
            self.quota.admit('anonymous', action='recommend')
        self.assertEqual(self.counter(), 0)
        trace = []
        def observe(conn, cursor, sql, parameters, context, many):
            trace.append((conn, sql))
        event.listen(self.engine, 'before_cursor_execute', observe)
        try:
            self.quota.admit(self.token, action='recommend')
        finally:
            event.remove(self.engine, 'before_cursor_execute', observe)
        sql = [statement for _, statement in trace]
        self.assertEqual(len({id(conn) for conn, _ in trace}), 1)
        self.assertLess(sql.index('BEGIN IMMEDIATE'), next(i for i, s in enumerate(sql) if 'FROM auth_sessions' in s))
        self.assertLess(next(i for i, s in enumerate(sql) if 'FROM auth_sessions' in s),
                        next(i for i, s in enumerate(sql) if 'FROM auth_limits' in s))
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE auth_sessions SET expires_at=created_at+1'))
        self.h.clock.wall += 2000
        with self.assertRaises(AuthError):
            self.quota.admit(self.token, action='recommend')
        self.assertEqual(self.counter(), 1)

    def test_parallel_last_two_slots_are_atomic_and_sessions_share_account_limit(self):
        second = self.session('synthetic-second')
        for _ in range(28):
            self.quota.admit(self.token, action='recommend')
        barrier = threading.Barrier(4)
        def admit(token):
            barrier.wait()
            try:
                self.quota.admit(token, action='recommend')
                return 'admitted'
            except QuotaExceeded:
                return 'denied'
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(admit, [self.token, second, self.token, second]))
        self.assertCountEqual(results, ['admitted', 'admitted', 'denied', 'denied'])
        self.assertEqual(self.counter(), 30)

    def test_login_namespace_is_unchanged(self):
        self.auth.limit_public_request('synthetic-loopback')
        with self.engine.connect() as conn:
            before = conn.execute(text("SELECT * FROM auth_limits WHERE bucket LIKE 'ip:%'")).one()
        self.quota.admit(self.token, action='recommend')
        self.reserve()
        with self.engine.connect() as conn:
            after = conn.execute(text("SELECT * FROM auth_limits WHERE bucket LIKE 'ip:%'")).one()
        self.assertEqual(before, after)
