"""Same-writer social boundaries, using a SELECT-only injected reader stub."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import threading
from unittest.mock import patch

from sqlalchemy import event, text

from document_library.auth import AuthError, digest
from document_library.database import write_transaction
from document_library.member_service import MemberService, MemberError
from document_library.request_quota import AccountQuota, QuotaExceeded
from library_helpers import LibraryFixture
from test_library_queries import seed_library


class MemberServiceAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        seed_library(self.engine, 2, ratings=False)
        self.work = 'w000000'
        self.quota = AccountQuota(self.auth)
        self.observed = []
        self.members = MemberService(self.auth, quota=self.quota, read_social_in=self.social_stub)

    def social_stub(self, conn, *, actor, work_id):
        self.assertTrue(conn.connection.driver_connection.in_transaction)
        self.observed.append((conn, actor['id'], work_id))
        own = conn.execute(text('SELECT rating_half_steps FROM ratings WHERE user_id=:user AND work_id=:work'),
                           dict(user=actor['id'], work=work_id)).scalar_one_or_none()
        votes, half_sum = conn.execute(text('SELECT votes,half_sum FROM library_rating_stats WHERE work_id=:work'),
                                       dict(work=work_id)).one()
        favorite = conn.execute(text('SELECT 1 FROM library_favorites WHERE user_id=:user AND work_id=:work'),
                                dict(user=actor['id'], work=work_id)).first() is not None
        average = half_sum / (2 * votes) if votes else None
        # The injected stub measures mutation visibility; ranking is B's contract.
        return dict(work_id=work_id, my_rating_half_steps=own, rating_average=average,
                    rating_votes=votes, weighted_rating=average, favorite=favorite)

    def set_rating(self, rating=9, work=None):
        return self.members.set_rating(self.token, self.csrf, work or self.work, rating)

    def bucket_count(self):
        return self.scalar("SELECT coalesce(sum(attempts),0) FROM auth_limits WHERE bucket LIKE 'g3:social:%'")

    def assert_error(self, status, invoke):
        with self.assertRaises(AuthError) as caught:
            invoke()
        self.assertEqual(caught.exception.status, status)

    def preferences(self):
        value = self.members.get_preferences(self.token)
        value.pop('updated_at')
        return value

    def test_same_writer_auth_readability_quota_mutation_callback_then_commit(self):
        trace = []
        def observe(conn, cursor, statement, parameters, context, many):
            trace.append((conn, statement))
        event.listen(self.engine, 'before_cursor_execute', observe)
        try:
            result = self.set_rating()
        finally:
            event.remove(self.engine, 'before_cursor_execute', observe)
        active = self.observed[-1][0]
        self.assertTrue(all(conn is active for conn, _ in trace))
        sql = [statement for _, statement in trace]
        order = [next(i for i, statement in enumerate(sql) if needle in statement)
                 for needle in ('BEGIN IMMEDIATE', 'FROM auth_sessions', 'WITH valid_options',
                                'FROM auth_limits', 'INSERT INTO ratings', 'SELECT rating_half_steps')]
        self.assertEqual(order, sorted(order))
        self.assertEqual(set(result), {'work_id', 'my_rating_half_steps', 'rating_average',
                                      'rating_votes', 'weighted_rating', 'favorite'})
        self.assertEqual((result['my_rating_half_steps'], result['rating_votes']), (9, 1))
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'), 9)
        self.assertEqual(self.bucket_count(), 1)

    def test_callback_failure_rolls_back_source_stats_and_new_quota(self):
        self.set_rating(4)
        before = self.scalar('SELECT half_sum FROM library_rating_stats WHERE work_id=:work', work=self.work)
        def failed(conn, *, actor, work_id):
            self.assertEqual(conn.execute(text('SELECT rating_half_steps FROM ratings WHERE work_id=:work'),
                                          dict(work=work_id)).scalar_one(), 10)
            raise RuntimeError('synthetic callback fault')
        self.members.read_social_in = failed
        with self.assertRaisesRegex(RuntimeError, 'synthetic callback fault'):
            self.set_rating(10)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'), 4)
        self.assertEqual(self.scalar('SELECT half_sum FROM library_rating_stats WHERE work_id=:work', work=self.work), before)
        self.assertEqual(self.bucket_count(), 1)
        def failed_favorite(conn, *, actor, work_id):
            self.assertEqual(conn.execute(text('SELECT count(*) FROM library_favorites')).scalar_one(), 1)
            raise RuntimeError('synthetic favorite callback fault')
        self.members.read_social_in = failed_favorite
        with self.assertRaisesRegex(RuntimeError, 'synthetic favorite callback fault'):
            self.members.set_favorite(self.token, self.csrf, self.work, True)
        self.assertEqual(self.scalar('SELECT count(*) FROM library_favorites'), 0)
        self.assertEqual(self.bucket_count(), 1)

    def test_failed_commit_never_returns_measured_result_or_admits_quota(self):
        def fail(conn):
            raise RuntimeError('synthetic commit fault')
        event.listen(self.engine, 'commit', fail)
        try:
            with self.assertRaisesRegex(RuntimeError, 'synthetic commit fault'):
                self.set_rating()
        finally:
            event.remove(self.engine, 'commit', fail)
        self.assertEqual(len(self.observed), 1)
        self.assertEqual(self.scalar('SELECT count(*) FROM ratings'), 0)
        self.assertEqual(self.bucket_count(), 0)

    def test_strict_input_idempotency_and_own_favorite(self):
        for invalid in (0, 11, True, False, 5.0, '5', None):
            self.assert_error(422, lambda: self.set_rating(invalid))
        for invalid in ('', None, 'x' * 201, 'bad\nwork'):
            self.assert_error(422, lambda: self.members.delete_rating(self.token, self.csrf, invalid))
        for invalid in (1, 'true', None):
            self.assert_error(422, lambda: self.members.set_favorite(self.token, self.csrf, self.work, invalid))
        self.assertEqual(self.bucket_count(), 0)
        self.assertEqual(self.set_rating(1)['rating_average'], 0.5)
        stamp = self.scalar('SELECT updated_at FROM ratings')
        self.h.clock.wall += 1000
        self.set_rating(1)
        self.assertEqual(self.scalar('SELECT updated_at FROM ratings'), stamp)
        self.assertEqual(self.set_rating(10)['rating_average'], 5)
        for _ in range(2):
            self.assertTrue(self.members.set_favorite(self.token, self.csrf, self.work, True)['favorite'])
        self.assertEqual(self.scalar('SELECT count(*) FROM library_favorites'), 1)
        self.assertFalse(self.members.set_favorite(self.token, self.csrf, self.work, False)['favorite'])
        for _ in range(2):
            result = self.members.delete_rating(self.token, self.csrf, self.work)
            self.assertEqual((result['my_rating_half_steps'], result['rating_votes']), (None, 0))
        self.assertEqual(self.scalar('SELECT half_sum FROM library_rating_stats WHERE work_id=:work', work=self.work), 0)

    def test_text_scan_same_work_and_adaptation_separate(self):
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_relations VALUES('w000001','w000000','adaptation_of')"))
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000000-scan-job'"))
        self.set_rating(3)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed' WHERE job_id='e000000-scan-job'"))
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000000-text-job'"))
        self.set_rating(8)
        self.set_rating(5, 'w000001')
        self.assertEqual(self.scalar('SELECT count(*) FROM ratings'), 2)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings WHERE work_id=:work', work=self.work), 8)
        self.assertEqual(self.scalar("SELECT half_sum FROM library_rating_stats WHERE work_id='w000001'"), 5)

    def test_fresh_source_recheck_and_delete_after_revocation_are_404(self):
        self.set_rating()
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000000-%'"))
        for invoke in (lambda: self.set_rating(),
                       lambda: self.members.delete_rating(self.token, self.csrf, self.work),
                       lambda: self.members.set_favorite(self.token, self.csrf, self.work, True),
                       lambda: self.set_rating(work='missing')):
            self.assert_error(404, invoke)
        self.assertEqual(self.bucket_count(), 1)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'), 9)
        self.assertEqual(len(self.observed), 1)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed' WHERE job_id LIKE 'e000000-%'"))
            conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256 IN (SELECT object_sha FROM revision_assets WHERE edition_revision_id LIKE 'e000000-%')"))
        self.assert_error(404, lambda: self.set_rating())
        self.assertEqual(self.bucket_count(), 1)

    def test_fresh_auth_csrf_and_upload_capability_boundaries(self):
        for csrf in (None, '', False, 'wrong'):
            self.assert_error(403, lambda: self.members.set_rating(self.token, csrf, self.work, 8))
        self.assert_error(401, lambda: self.members.set_rating('anonymous', self.csrf, self.work, 8))
        self.assertEqual(self.bucket_count(), 0)
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM user_permissions WHERE user_id='u'"))
        self.set_rating(7)
        self.h.clock.wall += 2000
        for change in ("UPDATE users SET status='suspended' WHERE id='u'",
                       "UPDATE users SET credential_version=credential_version+1 WHERE id='u'",
                       "UPDATE auth_sessions SET expires_at=created_at+1"):
            with write_transaction(self.engine) as conn:
                conn.execute(text(change))
            self.assert_error(401, lambda: self.set_rating())
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE users SET status='active',credential_version=1 WHERE id='u'"))
                conn.execute(text('UPDATE auth_sessions SET expires_at=:end'), dict(end=self.auth.now()+3600))
        self.auth.logout(self.token, self.csrf)
        self.assert_error(401, lambda: self.set_rating())
        self.assertEqual(self.bucket_count(), 1)

    def test_preferences_exact_replacement_defaults_canonicalization_and_own_isolation(self):
        absent = self.members.get_preferences(self.token)
        self.assertIsNone(absent['updated_at'])
        absent['filters']['genres'].append('mutated')
        self.assertEqual(self.preferences()['filters']['genres'], [])
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'), 0)
        value = self.preferences()
        value['filters'].update(categories=['novel_scan', 'novel_text', 'novel_scan'], genres=['fantasy'])
        value['view'] = 'list'
        value['recommendations']['exclude_downloaded'] = True
        saved = self.members.set_preferences(self.token, self.csrf, value)
        self.assertEqual(saved['filters']['categories'], ['novel_scan', 'novel_text'])
        self.assertTrue(saved['updated_at'].endswith('Z'))
        self.assertEqual(self.members.get_preferences(self.token), saved)
        value['filters']['genres'].append('later mutation')
        self.assertEqual(self.members.get_preferences(self.token)['filters']['genres'], ['fantasy'])
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('other','other','synthetic','test')"))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,'other',1,:now,:end)"),
                         dict(token=digest('synthetic-other'), csrf=digest(self.csrf), now=self.auth.now(), end=self.auth.now()+3600))
        self.assertIsNone(self.members.get_preferences('synthetic-other')['updated_at'])
        other = self.preferences()
        other['view'] = 'grid'
        self.members.set_preferences('synthetic-other', self.csrf, other)
        self.assertEqual(self.members.get_preferences(self.token), saved)
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'), 2)

    def test_preferences_reject_extra_missing_fields_bad_types_and_bound_filters(self):
        valid = self.preferences()
        invalid = [dict(valid, user_id='other'), dict(valid, updated_at='forged'), {}, None]
        for section, field, bad in [('filters', 'states', ['wrong']), ('filters', 'genres', 'fantasy'),
                                    ('filters', 'genres', ['x']*65), ('filters', 'formats', ['x'*129]),
                                    ('filters', 'genres', [True]), ('recommendations', 'exclude_rated', 1)]:
            value = deepcopy(valid)
            value[section][field] = bad
            invalid.append(value)
        invalid += [dict(valid, view=[]), dict(valid, view='gallery')]
        for value in invalid:
            self.assert_error(422, lambda: self.members.set_preferences(self.token, self.csrf, value))
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'), 0)
        self.assertEqual(self.bucket_count(), 0)

    def test_social_quota_shared_by_rating_favorite_and_preferences_and_rolls_back_prefs(self):
        valid = self.preferences()
        for _ in range(10):
            self.set_rating()
            self.members.set_favorite(self.token, self.csrf, self.work, True)
            self.members.set_preferences(self.token, self.csrf, valid)
        with self.assertRaises(QuotaExceeded):
            self.members.set_favorite(self.token, self.csrf, self.work, False)
        self.assertEqual(self.bucket_count(), 30)
        self.h.clock.wall += 60000
        def fail(*args, **kwargs):
            raise RuntimeError('synthetic prefs readback fault')
        before = self.scalar('SELECT preferences_json FROM member_preferences')
        with patch.object(self.members, 'preferences_in', side_effect=fail):
            with self.assertRaises(RuntimeError):
                self.members.set_preferences(self.token, self.csrf, dict(valid, view='list'))
        self.assertEqual(self.scalar('SELECT preferences_json FROM member_preferences'), before)
        self.assertEqual(self.bucket_count(), 30)

    def test_two_writers_last_commit_single_source_row_and_exact_stats(self):
        second = self.session('synthetic-second-session')
        gate = threading.Barrier(2)
        def write(token, value):
            gate.wait()
            self.members.set_rating(token, self.csrf, self.work, value)
        order = []
        def measured(conn, *, actor, work_id):
            value = self.social_stub(conn, actor=actor, work_id=work_id)
            order.append(value['my_rating_half_steps'])
            return value
        self.members.read_social_in = measured
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(write, self.token, 2), pool.submit(write, second, 10)]
            for future in futures:
                future.result()
        self.assertEqual(self.scalar('SELECT count(*) FROM ratings'), 1)
        rating = self.scalar('SELECT rating_half_steps FROM ratings')
        self.assertEqual(rating, order[-1])
        self.assertCountEqual(order, [2, 10])
        self.assertEqual(self.scalar('SELECT half_sum FROM library_rating_stats WHERE work_id=:work', work=self.work), rating)
        self.assertEqual(self.scalar('SELECT votes FROM library_rating_stats WHERE work_id=:work', work=self.work), 1)
        self.assertEqual(self.bucket_count(), 2)

    def test_two_members_keep_own_values_while_sharing_aggregates(self):
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('other','other','synthetic','test')"))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,'other',1,:now,:end)"),
                         dict(token=digest('synthetic-other-rating'), csrf=digest(self.csrf), now=self.auth.now(), end=self.auth.now()+3600))
        self.set_rating(2)
        other = self.members.set_rating('synthetic-other-rating', self.csrf, self.work, 10)
        mine = self.members.set_favorite(self.token, self.csrf, self.work, True)
        self.assertEqual((mine['my_rating_half_steps'], other['my_rating_half_steps']), (2, 10))
        self.assertEqual((mine['rating_average'], mine['rating_votes']), (3, 2))
        self.assertEqual((other['rating_average'], other['rating_votes']), (3, 2))
        self.assertTrue(mine['favorite'])
        self.assertFalse(other['favorite'])
        self.members.delete_rating('synthetic-other-rating', self.csrf, self.work)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'), 2)
        self.assertEqual(self.scalar('SELECT count(*) FROM library_favorites'), 1)

    def test_trigger_failure_rolls_back_source_integer_stats_and_quota(self):
        from sqlalchemy.exc import IntegrityError
        self.set_rating(4)
        with write_transaction(self.engine) as conn:
            conn.exec_driver_sql("CREATE TRIGGER synthetic_rating_failure AFTER UPDATE ON ratings BEGIN SELECT RAISE(ABORT,'synthetic trigger fault'); END")
        with self.assertRaises(IntegrityError):
            self.set_rating(10)
        self.assertEqual(self.scalar('SELECT rating_half_steps FROM ratings'), 4)
        self.assertEqual(self.scalar('SELECT votes FROM library_rating_stats WHERE work_id=:work', work=self.work), 1)
        self.assertEqual(self.scalar('SELECT half_sum FROM library_rating_stats WHERE work_id=:work', work=self.work), 4)
        self.assertEqual(self.bucket_count(), 1)
        self.assertEqual(len(self.observed), 1)

    def test_delete_put_race_retains_commit_order_without_negative_stats(self):
        self.set_rating(3)
        gate = threading.Barrier(2)
        order = []
        def measured(conn, *, actor, work_id):
            value = self.social_stub(conn, actor=actor, work_id=work_id)
            order.append(value['my_rating_half_steps'])
            return value
        self.members.read_social_in = measured
        def mutate(delete):
            gate.wait()
            if delete:
                self.members.delete_rating(self.token, self.csrf, self.work)
            else:
                self.set_rating(10)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(mutate, True), pool.submit(mutate, False)]
            for future in futures:
                future.result()
        self.assertCountEqual(order, [None, 10])
        with self.engine.connect() as conn:
            rating = conn.execute(text('SELECT rating_half_steps FROM ratings')).scalar_one_or_none()
        self.assertEqual(rating, order[-1])
        expected = (0, 0) if rating is None else (1, rating)
        with self.engine.connect() as conn:
            stats = conn.execute(text('SELECT votes,half_sum FROM library_rating_stats WHERE work_id=:work'), dict(work=self.work)).one()
        self.assertEqual(tuple(stats), expected)
        self.assertEqual(self.bucket_count(), 3)

    def test_preferences_in_uses_supplied_snapshot_without_auth_quota_or_checkout(self):
        with self.engine.connect() as conn:
            actor = self.auth.current(conn, self.token)
            with (patch.object(self.auth, 'current', side_effect=AssertionError('new auth')),
                  patch.object(self.engine, 'connect', side_effect=AssertionError('new checkout')),
                  patch.object(self.quota, 'reserve_in', side_effect=AssertionError('quota write')),
                  patch.object(conn, 'commit', side_effect=AssertionError('commit'))):
                value = self.members.preferences_in(conn, actor=actor)
        self.assertIsNone(value['updated_at'])
        self.assertEqual(self.scalar('SELECT count(*) FROM member_preferences'), 0)
        self.assertEqual(self.bucket_count(), 0)
