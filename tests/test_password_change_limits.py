"""Synthetic regression coverage for password-change attempt limits."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from sqlalchemy import text

from document_library.auth import AuthError, AuthService
from document_library.database import database_engine, migrate

ROOT = Path(__file__).resolve().parents[1]
PASSWORD = "synthetic-test-password-42"
REPLACEMENT = "synthetic-replacement-password-42"


class PasswordChangeLimits(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="document-library-password-limits-")
        self.addCleanup(self.temp.cleanup)
        self.engine = database_engine(Path(self.temp.name) / "synthetic.sqlite3")
        self.addCleanup(self.engine.dispose)
        migrate(self.engine, ROOT / "migrations")
        self.now = 1_800_000_000
        self.auth = AuthService(self.engine, clock=lambda: self.now)
        self.owner_id = self.auth.bootstrap("owner", PASSWORD)
        self.owner = self.auth.login("owner", PASSWORD, ip="synthetic-owner")
        self.member_id = self.auth.register(
            self.auth.issue_token(self.owner.token, self.owner.csrf), "member", PASSWORD
        )

    def session(self):
        return self.auth.login("member", PASSWORD, ip="synthetic-member")

    def budget_row(self):
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT attempts, window_start, next_allowed FROM auth_limits WHERE bucket=:bucket"),
                dict(bucket="change-password:" + __import__("document_library.auth", fromlist=["digest"]).digest(str(self.member_id))),
            ).mappings().first()

    def assert_error(self, code, action, status=None):
        with self.assertRaises(AuthError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)
        if status is not None:
            self.assertEqual(caught.exception.status, status)

    def test_wrong_password_consumes_committed_budget_before_eleventh_denial(self):
        session = self.session()
        for _ in range(10):
            self.assert_error(
                "invalid_credentials",
                lambda: self.auth.change_password(session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT),
            )
        row = self.budget_row()
        self.assertEqual(row["attempts"], 10)
        self.assertEqual(row["window_start"], self.now)
        with patch.object(self.auth, "_verify") as verify:
            self.assert_error(
                "try_later",
                lambda: self.auth.change_password(session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT),
                429,
            )
            verify.assert_not_called()

    def test_shared_budget_across_sessions_and_service_restart(self):
        first = self.session()
        second = self.session()
        for session in (first, second):
            for _ in range(5):
                self.assert_error(
                    "invalid_credentials",
                    lambda: self.auth.change_password(session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT),
                )
        self.assert_error(
            "try_later",
            lambda: self.auth.change_password(second.token, second.csrf, "wrong-synthetic-password", REPLACEMENT),
            429,
        )
        restarted = AuthService(self.engine, clock=lambda: self.now)
        with patch.object(restarted, "_verify") as verify:
            self.assert_error(
                "try_later",
                lambda: restarted.change_password(second.token, second.csrf, PASSWORD, REPLACEMENT),
                429,
            )
            verify.assert_not_called()

    def test_concurrent_reservations_never_exceed_ceiling(self):
        session = self.session()
        with patch.object(self.auth, "_verify", return_value=False) as verify:
            with ThreadPoolExecutor(max_workers=12) as pool:
                futures = [
                    pool.submit(self.auth.change_password, session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT)
                    for _ in range(12)
                ]
        outcomes = []
        for future in futures:
            try:
                future.result()
            except AuthError as error:
                outcomes.append((error.code, error.status))
            except AssertionError:
                outcomes.append(("verify", None))
        self.assertEqual(outcomes.count(("invalid_credentials", 401)), 10)
        self.assertEqual(outcomes.count(("try_later", 429)), 2)
        self.assertEqual(verify.call_count, 10)
        self.assertEqual(self.budget_row()["attempts"], 10)

    def test_other_account_login_remains_available_after_attack_limit(self):
        session = self.session()
        for _ in range(10):
            self.assert_error(
                "invalid_credentials",
                lambda: self.auth.change_password(session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT),
            )
        self.assert_error(
            "try_later",
            lambda: self.auth.change_password(session.token, session.csrf, "wrong-synthetic-password", REPLACEMENT),
            429,
        )
        self.auth.login("owner", PASSWORD, ip="synthetic-owner")

    def test_successful_change_retains_session_and_reset_grant_invalidation(self):
        session = self.session()
        grant = self.auth.issue_token(self.owner.token, self.owner.csrf, reset_user=self.member_id)
        self.assertIsNotNone(grant)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(
                text("SELECT count(*) FROM auth_tokens WHERE purpose='reset' AND target_id=:id AND consumed_at IS NULL"),
                dict(id=self.member_id),
            ).scalar_one(), 1)
        self.auth.change_password(session.token, session.csrf, PASSWORD, REPLACEMENT)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM auth_sessions WHERE user_id=:id"), dict(id=self.member_id)).scalar_one(), 0)
            self.assertEqual(conn.execute(
                text("SELECT count(*) FROM auth_tokens WHERE purpose='reset' AND target_id=:id AND consumed_at IS NULL"),
                dict(id=self.member_id),
            ).scalar_one(), 0)
            with self.assertRaises(AuthError):
                self.auth.current(conn, session.token)
        fresh = self.auth.login("member", REPLACEMENT, ip="127.0.0.1")
        self.assertIsNotNone(fresh)
        self.assertNotEqual(fresh.token, session.token)

    def test_wrong_csrf_does_not_spend_user_budget(self):
        session = self.session()
        with patch.object(self.auth, "_verify") as verify:
            self.assert_error(
                "csrf_rejected",
                lambda: self.auth.change_password(session.token, "wrong-synthetic-csrf", PASSWORD, REPLACEMENT),
                403,
            )
            self.assertEqual(verify.call_count, 0)
        self.assertIsNone(self.budget_row())
