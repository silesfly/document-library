"""Synthetic administrator changes and publication/revoke serialization."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import text

from document_library.api import create_app, CSRF_COOKIE, SESSION_COOKIE
from document_library.auth import AuthError, AuthService
from document_library.database import database_engine, migrate, write_transaction
from document_library.permissions import PermissionService

ROOT = Path(__file__).resolve().parents[1]


class PermissionAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="document-library-permissions-")
        self.addCleanup(self.temp.cleanup)
        self.engine = database_engine(Path(self.temp.name) / "synthetic.sqlite3")
        self.addCleanup(self.engine.dispose)
        migrate(self.engine, ROOT / "migrations")
        self.now = 1800000000
        self.auth = AuthService(self.engine, clock=lambda: self.now)
        self.admin_id = self.auth.bootstrap("owner", "synthetic-admin-password")
        self.admin = self.auth.login("owner", "synthetic-admin-password", ip="test-admin")
        self.permissions = PermissionService(self.auth)
        self.members = [self.add_user("reader-a"), self.add_user("reader-b")]

    def add_user(self, name, role="member"):
        ident = str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            # Non-login fixtures avoid 200 unnecessary expensive password hashes.
            conn.execute(text("INSERT INTO users(id,username,password_hash,role,created_at) VALUES(:id,:name,'synthetic-disabled',:role,'test')"),
                         dict(id=ident, name=name, role=role))
        return ident

    def preview(self, action="grant", ids=None, **kw):
        ids = self.members if ids is None else ids
        with self.engine.connect() as conn:
            versions = {r.id: r.authz_version for r in conn.execute(text("SELECT id,authz_version FROM users")) if r.id in ids}
        return self.permissions.preview(self.admin.token, self.admin.csrf, action=action, reason="검증용 변경",
                                       user_ids=ids, expected_authz_versions=versions, **kw)

    def commit(self, preview, key=None, **overrides):
        args = dict(preview_token=preview["preview_token"], action=preview["action"], reason=preview["reason"],
                    confirm_target_count=preview["eligible_count"], confirmed=True,
                    idempotency_key=key or str(uuid.uuid4()), client="test-client", admin_password="synthetic-admin-password")
        args.update(overrides)
        return self.permissions.commit(self.admin.token, self.admin.csrf, **args)

    def error(self, code, action):
        with self.assertRaises(AuthError) as cm:
            action()
        self.assertEqual(cm.exception.code, code)

    def sql(self, query):
        with self.engine.connect() as conn:
            return conn.execute(text(query)).all()

    def upload(self, owner=None):
        owner = owner or self.members[0]
        ident = str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            version = conn.execute(text("SELECT authz_version FROM users WHERE id=:id"), dict(id=owner)).scalar_one()
            conn.execute(text("INSERT INTO upload_batches VALUES(:id,:owner,10,'open',:now)"), dict(id=ident, owner=owner, now=self.now))
            conn.execute(text("INSERT INTO upload_sessions VALUES(:id,:id,:owner,NULL,:version,10,10,:expiry,'staged')"),
                         dict(id=ident, owner=owner, version=version, expiry=self.now+1000))
        return ident

    def test_grant_revoke_and_noop_do_not_break_read_session_or_valid_upload(self):
        self.commit(self.preview())
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO auth_sessions VALUES('synthetic-hash','csrf',:id,1,:now,:end)"),
                         dict(id=self.members[0], now=self.now, end=self.now+100))
        upload = self.upload()
        noop = self.commit(self.preview())
        self.assertEqual(noop["changed_count"], 0)
        with self.permissions.publication(upload):
            pass
        result = self.commit(self.preview("revoke"))
        self.assertEqual(result["changed_count"], 2)
        self.assertEqual(self.sql("SELECT count(*) FROM user_permissions"), [(0,)])
        self.assertEqual(self.sql("SELECT count(*) FROM auth_sessions"), [(2,)])
        self.assertEqual(self.sql("SELECT status FROM upload_sessions"), [("published",)])
        self.assertEqual(self.sql("SELECT count(*) FROM audit_events WHERE action='permission_batch'"), [(3,)])

    def test_protected_unknown_suspended_and_stale_targets_rollback_whole_batch(self):
        self.error("AUTHZ_CONFLICT", lambda: self.preview(ids=self.members+[self.admin_id]))
        self.error("AUTHZ_CONFLICT", lambda: self.permissions.preview(self.admin.token, self.admin.csrf,
            action="grant", reason="test", user_ids=["missing"], expected_authz_versions={"missing":1}))
        old = self.preview()
        self.commit(self.preview(ids=[self.members[0]]))
        self.error("AUTHZ_CONFLICT", lambda: self.commit(old))
        self.assertEqual(self.sql("SELECT count(*) FROM user_permissions"), [(1,)])
        self.commit(self.preview("suspend", ids=[self.members[1]]))
        self.error("AUTHZ_CONFLICT", lambda: self.preview(ids=[self.members[1]]))

    def test_audit_failure_rolls_back_change_consumption_and_receipt(self):
        preview = self.preview()
        original = self.auth.audit
        def fail(conn, actor, action, **kw):
            original(conn, actor, action, **kw)
            if action == "permission_batch":
                raise RuntimeError("synthetic audit failure")
        with patch.object(self.auth, "audit", side_effect=fail):
            with self.assertRaises(RuntimeError):
                self.commit(preview)
        self.assertEqual(self.sql("SELECT count(*) FROM user_permissions"), [(0,)])
        self.assertEqual(self.sql("SELECT count(*) FROM permission_change_batches"), [(0,)])
        self.assertEqual(self.sql("SELECT consumed_at FROM permission_previews"), [(None,)])
        self.commit(preview)

    def test_concurrent_replay_returns_one_receipt_and_different_input_conflicts(self):
        preview = self.preview()
        barrier = threading.Barrier(2)
        key = str(uuid.uuid4())
        def run(_):
            barrier.wait()
            return self.commit(preview, key)
        with ThreadPoolExecutor(max_workers=2) as pool:
            a,b = list(pool.map(run, range(2)))
        self.assertEqual(a,b)
        self.assertEqual(self.sql("SELECT count(*) FROM permission_change_batches"), [(1,)])
        self.assertEqual(self.sql("SELECT count(*) FROM audit_events WHERE action='permission_changed'"), [(2,)])
        self.error("idempotency_conflict", lambda: self.commit(preview, key, reason="different"))
        self.error("invalid_or_expired_preview", lambda: self.commit(preview))
        self.now += 1000
        self.assertEqual(self.commit(preview, key), a)

    def test_preview_actor_expiry_confirmation_and_count_binding(self):
        preview = self.preview()
        self.error("invalid_input", lambda: self.commit(preview, confirmed=False))
        self.error("preview_mismatch", lambda: self.commit(preview, confirm_target_count=1))
        self.error("preview_mismatch", lambda: self.commit(preview, action="revoke"))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE permission_previews SET actor_id=:other"),dict(other=self.members[0]))
        self.error("invalid_or_expired_preview", lambda: self.commit(preview))
        preview = self.preview()
        self.now += 300
        self.error("invalid_or_expired_preview", lambda: self.commit(preview))

    def test_reauthentication_wrong_password_rate_limit_and_hash_race(self):
        preview = self.preview()
        for _ in range(3):
            self.error("invalid_credentials", lambda: self.commit(preview, admin_password="synthetic-wrong-password"))
        self.error("try_later", lambda: self.commit(preview))
        self.now += 60
        original = self.auth._verify
        def changed_after_verify(stored, password):
            valid = original(stored, password)
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE users SET credential_version=credential_version+1 WHERE id=:id"),dict(id=self.admin_id))
            return valid
        with patch.object(self.auth, "_verify", side_effect=changed_after_verify):
            self.error("authentication_required", lambda: self.commit(preview))
        self.assertEqual(self.sql("SELECT count(*) FROM permission_change_batches"),[(0,)])
        self.assertEqual(self.sql("SELECT count(*) FROM user_permissions"),[(0,)])

    def test_http_requires_password_and_never_stores_it_in_audit_or_receipt(self):
        preview = self.preview()
        origin = "https://library.test"
        client = TestClient(create_app(self.auth, origin=origin),base_url=origin,headers={"Origin":origin,"X-CSRF-Token":self.admin.csrf,"Idempotency-Key":str(uuid.uuid4())})
        self.addCleanup(client.close)
        client.cookies.set(SESSION_COOKIE,self.admin.token)
        body=dict(preview_token=preview["preview_token"],action="grant",reason=preview["reason"],confirm_target_count=2,confirmed=True)
        path="/admin/user-permissions/bulk"
        self.assertEqual(client.post(path,json=body).status_code,422)
        body["admin_password"]="synthetic-wrong-password"
        wrong=client.post(path,json=body)
        self.assertEqual(wrong.status_code,401)
        self.assertNotIn(body["admin_password"],wrong.text)
        body["admin_password"]="synthetic-admin-password"
        response=client.post(path,json=body)
        self.assertEqual(response.status_code,200,response.text)
        for query in ["SELECT details_json FROM audit_events","SELECT result_json FROM permission_change_batches","SELECT payload_json FROM permission_previews"]:
            self.assertNotIn(body["admin_password"],str(self.sql(query)))
        self.assertNotIn(body["admin_password"],response.text)

    def test_search_snapshot_excludes_later_match_and_enforces_200_bound(self):
        preview = self.permissions.preview(self.admin.token, self.admin.csrf, action="grant", reason="search",
                                           selection={"search":"reader-","state":"all"})
        late = self.add_user("reader-late")
        result = self.commit(preview)
        self.assertNotIn(late, result["user_ids"])
        self.assertEqual(result["changed_count"], 2)
        self.error("selection_size", lambda: self.permissions.preview(self.admin.token, self.admin.csrf,
            action="grant", reason="test", user_ids=[late]*201, expected_authz_versions={late:1}))
        self.error("selection_size", lambda: self.preview(ids=[late,late]))

    def test_changed_upload_impact_requires_fresh_review_then_blocks_all(self):
        self.commit(self.preview())
        old = self.preview("revoke")
        self.upload()
        self.error("impact_changed", lambda: self.commit(old))
        fresh = self.preview("revoke")
        self.assertEqual(fresh["impact"], {"upload_sessions_to_block":1})
        self.assertEqual(self.commit(fresh)["upload_sessions_blocked"], 1)
        self.assertEqual(self.sql("SELECT status FROM upload_sessions"), [("revoked",)])
        self.assertEqual(self.sql("SELECT status FROM upload_batches"), [("revoked",)])

    def test_suspend_activate_clears_sessions_and_old_upload_stays_revoked(self):
        self.commit(self.preview())
        upload = self.upload()
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO auth_sessions VALUES('member-session','csrf',:id,1,:now,:expiry)"),
                         dict(id=self.members[0],now=self.now,expiry=self.now+100))
        self.commit(self.preview("suspend", ids=[self.members[0]]))
        self.assertEqual(self.sql("SELECT count(*) FROM auth_sessions"),[(1,)])
        self.commit(self.preview("activate", ids=[self.members[0]]))
        self.error("upload_permission_revoked", lambda: self.publish(upload))

    def publish(self, upload):
        with self.permissions.publication(upload) as conn:
            conn.execute(text("INSERT INTO publication_probe VALUES(:id)"),dict(id=upload))

    def test_revoke_before_publish_denies_and_publication_failure_rolls_back(self):
        self.commit(self.preview())
        upload = self.upload()
        with self.assertRaises(RuntimeError):
            with self.permissions.publication(upload):
                raise RuntimeError("synthetic downstream publication failure")
        self.assertEqual(self.sql("SELECT status FROM upload_sessions"),[("staged",)])
        self.commit(self.preview("revoke"))
        self.error("upload_permission_revoked", lambda: self.publish(upload))
        self.commit(self.preview())
        self.error("upload_permission_revoked", lambda: self.publish(upload))

    def test_publish_writer_before_revoke_preserves_published_data(self):
        self.commit(self.preview())
        upload = self.upload()
        with write_transaction(self.engine) as conn:
            conn.execute(text("CREATE TABLE publication_probe(id TEXT PRIMARY KEY) STRICT"))
        entered, release, revoke_started = threading.Event(), threading.Event(), threading.Event()
        def publishing():
            with self.permissions.publication(upload) as conn:
                conn.execute(text("INSERT INTO publication_probe VALUES(:id)"),dict(id=upload))
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("test synchronization failed")
        def revoking():
            if not entered.wait(5):
                raise RuntimeError("test synchronization failed")
            revoke_started.set()
            return self.commit(self.preview("revoke"))
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(publishing)
            second = pool.submit(revoking)
            self.assertTrue(revoke_started.wait(5))
            release.set()
            first.result()
            second.result()
        self.assertEqual(self.sql("SELECT id FROM publication_probe"),[(upload,)])
        self.assertEqual(self.sql("SELECT status FROM upload_sessions"),[("published",)])
        self.assertEqual(self.sql("SELECT count(*) FROM user_permissions"),[(0,)])

    def test_http_admin_boundary_and_200_member_payload(self):
        origin = "https://library.test"
        client = TestClient(create_app(self.auth, origin=origin),base_url=origin,headers={"Origin":origin})
        self.addCleanup(client.close)
        self.assertEqual(client.get("/admin/users").status_code,401)
        client.cookies.set(SESSION_COOKIE,self.admin.token)
        client.cookies.set(CSRF_COOKIE,self.admin.csrf)
        self.assertEqual(client.get("/admin/users").status_code,200)
        self.assertNotIn("password",client.get("/admin/users").text)
        ids=self.members+[self.add_user(f"bulk-{i:03d}") for i in range(198)]
        body=dict(action="grant",reason="maximum reviewed batch",user_ids=ids,expected_authz_versions={i:1 for i in ids})
        self.assertGreater(len(json.dumps(body)),8192)
        path="/admin/user-permissions/bulk-preview"
        self.assertEqual(client.post(path,json=body).status_code,403)
        client.headers["X-CSRF-Token"]=self.admin.csrf
        response=client.post(path,json=body)
        self.assertEqual(response.status_code,200,response.text)
        commit=dict(preview_token=response.json()["preview_token"],action="grant",reason=body["reason"],confirm_target_count=200,confirmed=True,admin_password="synthetic-admin-password")
        client.headers["Idempotency-Key"]=str(uuid.uuid4())
        result=client.post("/admin/user-permissions/bulk",json=commit)
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(result.json()["changed_count"],200)
        self.assertEqual(client.post("/admin/users/bulk-status",json=commit).status_code,400)
        self.assertEqual(client.post(path,content=' ' * 32769,headers={"Content-Type":"application/json"}).status_code,413)
        self.assertEqual(client.post("/auth/login",content=' '*8193,headers={"Content-Type":"application/json"}).status_code,413)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET role='member' WHERE id=:id"),dict(id=self.admin_id))
        self.assertEqual(client.get("/admin/users").status_code,403)
        self.assertEqual(client.post(path,json=body).status_code,403)


if __name__ == "__main__":
    unittest.main()
