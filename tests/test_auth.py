"""Disposable accounts, databases, tokens and HTTPS TestClient only."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import text

from document_library.api import create_app, CSRF_COOKIE, SESSION_COOKIE
from document_library.auth import AuthError, AuthService, digest
from document_library.database import database_engine, migrate, write_transaction

ROOT = Path(__file__).resolve().parents[1]
PASSWORD = "synthetic-test-password-42"
REPLACEMENT = "synthetic-replacement-password-42"
ORIGIN = "https://library.test"


class AuthenticationAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="document-library-auth-")
        self.addCleanup(self.temp.cleanup)
        self.engine = database_engine(Path(self.temp.name) / "synthetic.sqlite3")
        self.addCleanup(self.engine.dispose)
        migrate(self.engine, ROOT / "migrations")
        self.now = 1_800_000_000
        self.auth = AuthService(self.engine, clock=lambda: self.now)
        self.admin_id = self.auth.bootstrap("owner", PASSWORD)
        self.admin = self.auth.login("owner", PASSWORD, ip="synthetic-admin")

    def invite(self):
        return self.auth.issue_token(self.admin.token, self.admin.csrf)

    def member(self, name="reader"):
        ident = self.auth.register(self.invite(), name, PASSWORD)
        session = self.auth.login(name, PASSWORD, ip="synthetic-member")
        return ident, session

    def client(self):
        client = TestClient(create_app(self.auth, origin=ORIGIN), base_url=ORIGIN, headers={"Origin": ORIGIN})
        self.addCleanup(client.close)
        return client

    def assert_error(self, code, action, status=None):
        with self.assertRaises(AuthError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)
        if status is not None:
            self.assertEqual(caught.exception.status, status)

    def test_bootstrap_is_local_and_one_time(self):
        self.assert_error("already_initialized", lambda: self.auth.bootstrap("second", PASSWORD), 409)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM users")).scalar_one(), 1)
            stored = conn.execute(text("SELECT password_hash FROM users")).scalar_one()
            self.assertTrue(stored.startswith("$argon2id$"))
        self.assertEqual(self.client().post("/auth/bootstrap", json={}).status_code, 404)

    def test_invite_is_single_use_and_does_not_grant_upload(self):
        token = self.invite()
        ident = self.auth.register(token, "  Reader  ", PASSWORD)
        self.assert_error("invalid_or_expired_token", lambda: self.auth.register(token, "second", PASSWORD))
        session = self.auth.login("READER", PASSWORD, ip="example")
        user = self.auth.me(session.token)
        self.assertEqual((user["id"], user["role"], user["can_upload"]), (ident, "member", False))
        self.assert_error("permission_denied", lambda: self.auth.issue_token(session.token, session.csrf), 403)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM auth_tokens WHERE consumed_at IS NOT NULL")).scalar_one(), 1)
            self.assertEqual(conn.execute(text("SELECT count(*) FROM audit_events WHERE action='invite_redeemed'")).scalar_one(), 1)

    def test_invite_concurrent_redemption_has_one_winner(self):
        token = self.invite()
        barrier = threading.Barrier(2)
        def redeem(name):
            barrier.wait()
            try:
                return self.auth.register(token, name, PASSWORD)
            except AuthError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(redeem, ["first", "second"]))
        self.assertEqual(results.count("invalid_or_expired_token"), 1)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("SELECT count(*) FROM users WHERE role='member'")).scalar_one(), 1)

    def test_expired_invite_and_inactive_issuer_cannot_register(self):
        token = self.invite()
        self.now += 86400
        self.assert_error("invalid_or_expired_token", lambda: self.auth.register(token, "reader", PASSWORD))
        self.now -= 86400
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET status='suspended' WHERE id=:id"), dict(id=self.admin_id))
        self.assert_error("invalid_or_expired_token", lambda: self.auth.register(token, "reader", PASSWORD))

    def test_failed_registration_does_not_consume_invite(self):
        token = self.invite()
        self.assert_error("registration_rejected", lambda: self.auth.register(token, "owner", PASSWORD))
        ident = self.auth.register(token, "available", PASSWORD)
        self.assertTrue(ident)

    def test_session_expiry_logout_and_storage_do_not_retain_raw_tokens(self):
        self.auth.logout(self.admin.token, self.admin.csrf)
        self.assert_error("authentication_required", lambda: self.auth.me(self.admin.token))
        session = self.auth.login("owner", PASSWORD, ip="second-admin")
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT token_hash,csrf_hash FROM auth_sessions")).one()
            self.assertEqual(tuple(row), (digest(session.token), digest(session.csrf)))
        self.now += self.auth.SESSION_SECONDS
        self.assert_error("authentication_required", lambda: self.auth.me(session.token))

    def test_revocation_of_upload_keeps_read_session_but_suspension_does_not(self):
        ident, session = self.member()
        with write_transaction(self.engine) as conn:
            conn.execute(text("""INSERT INTO user_permissions VALUES(:id,'content.upload',:admin,'synthetic')"""),
                         dict(id=ident, admin=self.admin_id))
        self.assertTrue(self.auth.me(session.token)["can_upload"])
        with write_transaction(self.engine) as conn:
            conn.execute(text("DELETE FROM user_permissions WHERE user_id=:id"), dict(id=ident))
            conn.execute(text("UPDATE users SET authz_version=authz_version+1 WHERE id=:id"), dict(id=ident))
        self.assertFalse(self.auth.me(session.token)["can_upload"])
        with write_transaction(self.engine) as conn:
            self.assert_error("permission_denied", lambda: self.auth.current(conn, session.token, upload=True), 403)
            conn.execute(text("UPDATE users SET status='suspended' WHERE id=:id"), dict(id=ident))
        self.assert_error("authentication_required", lambda: self.auth.me(session.token))

    def test_password_change_invalidates_all_sessions_and_old_reset_link(self):
        ident, first = self.member()
        second = self.auth.login("reader", PASSWORD, ip="second-device")
        reset = self.auth.issue_token(self.admin.token, self.admin.csrf, reset_user=ident)
        self.auth.change_password(first.token, first.csrf, PASSWORD, REPLACEMENT)
        for token in (first.token, second.token):
            self.assert_error("authentication_required", lambda: self.auth.me(token))
        self.assert_error("invalid_or_expired_token", lambda: self.auth.reset_password(reset, PASSWORD))
        self.assert_error("invalid_credentials", lambda: self.auth.login("reader", PASSWORD, ip="old"))
        self.assertTrue(self.auth.login("reader", REPLACEMENT, ip="new"))

    def test_reset_latest_only_single_use_and_revokes_sessions(self):
        ident, session = self.member()
        old = self.auth.issue_token(self.admin.token, self.admin.csrf, reset_user=ident)
        current = self.auth.issue_token(self.admin.token, self.admin.csrf, reset_user=ident)
        self.assert_error("invalid_or_expired_token", lambda: self.auth.reset_password(old, REPLACEMENT))
        self.auth.reset_password(current, REPLACEMENT)
        self.assert_error("authentication_required", lambda: self.auth.me(session.token))
        self.assert_error("invalid_or_expired_token", lambda: self.auth.reset_password(current, PASSWORD))
        self.assertTrue(self.auth.login("reader", REPLACEMENT, ip="reset-login"))

    def test_login_password_verified_before_revocation_cannot_create_session(self):
        ident, _ = self.member()
        verified, proceed = threading.Event(), threading.Event()
        original = self.auth._verify
        def delayed(stored, supplied):
            result = original(stored, supplied)
            verified.set()
            if not proceed.wait(5):
                raise AssertionError("test synchronization failed")
            return result
        with patch.object(self.auth, "_verify", delayed), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.auth.login, "reader", PASSWORD, ip="race")
            try:
                self.assertTrue(verified.wait(5))
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE users SET credential_version=credential_version+1 WHERE id=:id"), dict(id=ident))
            finally:
                proceed.set()
            self.assert_error("invalid_credentials", future.result)

    def test_attempt_limits_persist_and_do_not_reveal_account_existence(self):
        for name in ("owner", "missing"):
            for _ in range(3):
                self.assert_error("invalid_credentials", lambda: self.auth.login(name, "synthetic-wrong", ip=name))
            self.assert_error("try_later", lambda: self.auth.login(name, PASSWORD, ip=name), 429)
        other = AuthService(self.engine, clock=lambda: self.now)
        self.assert_error("try_later", lambda: other.login("owner", PASSWORD, ip="other-process"))
        self.now += 601
        self.assertTrue(other.login("owner", PASSWORD, ip="other-process"))

    def test_writer_rollback_restores_changes_and_integrity(self):
        with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE users SET status='suspended'"))
                raise RuntimeError("synthetic failure")
        self.assertEqual(self.auth.me(self.admin.token)["role"], "admin")
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("PRAGMA integrity_check").scalar(), "ok")
            self.assertEqual(conn.exec_driver_sql("PRAGMA foreign_key_check").all(), [])

    def test_http_cookie_csrf_origin_and_logout(self):
        client = self.client()
        self.assertEqual(client.get("/me").status_code, 401)
        response = client.post("/auth/login", json={"username": "owner", "password": PASSWORD})
        self.assertEqual(response.status_code, 200)
        cookies = response.headers.get_list("set-cookie")
        self.assertTrue(all("Secure" in c and "SameSite=strict" in c and "Path=/" in c for c in cookies))
        self.assertIn("HttpOnly", next(c for c in cookies if c.startswith(SESSION_COOKIE)))
        self.assertEqual(client.get("/me").json()["role"], "admin")
        self.assertEqual(client.post("/admin/invites", json={}).status_code, 403)
        csrf = client.cookies[CSRF_COOKIE]
        headers = {"X-CSRF-Token": csrf}
        self.assertEqual(client.post("/admin/invites", json={}, headers={**headers, "Origin": "https://attacker.test"}).status_code, 403)
        self.assertEqual(client.post("/admin/invites", json={}, headers=headers).status_code, 200)
        self.assertEqual(client.post("/auth/logout", json={}, headers=headers).status_code, 200)
        self.assertEqual(client.get("/me").status_code, 401)

    def test_http_validation_does_not_echo_secrets_or_accept_oversized_body(self):
        client = self.client()
        secret = "synthetic-must-not-appear-in-response"
        bad = client.post("/auth/login", json={"username": "x", "password": secret})
        self.assertEqual(bad.status_code, 422)
        self.assertNotIn(secret, bad.text)
        self.assertEqual(client.post("/auth/login", content=b'x'*8193, headers={"Content-Type": "application/json"}).status_code, 413)
        self.assertEqual(client.post("/auth/login", data={"username": "owner", "password": PASSWORD}).status_code, 415)
        self.assertEqual(client.get("/me", headers={"Host": "attacker.test"}).status_code, 400)
        self.assertEqual(client.get("/me").headers["Cache-Control"], "no-store")

    def test_http_invite_and_password_reset_flow(self):
        client = self.client()
        client.post("/auth/login", json={"username": "owner", "password": PASSWORD})
        headers = {"X-CSRF-Token": client.cookies[CSRF_COOKIE]}
        invite = client.post("/admin/invites", json={}, headers=headers).json()["token"]
        new = client.post("/auth/register-invite", json={"invite": invite, "username": "reader", "password": PASSWORD})
        self.assertEqual(new.status_code, 201)
        reset = client.post(f"/admin/users/{new.json()['id']}/password-reset", json={}, headers=headers).json()["token"]
        response = client.post("/auth/reset-password", json={"token": reset, "new_password": REPLACEMENT})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(client.post("/auth/login", json={"username": "reader", "password": REPLACEMENT}).status_code, 200)
        self.assertFalse(client.get("/me").json()["can_upload"])
