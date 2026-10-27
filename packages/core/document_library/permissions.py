"""Admin review/commit protocol and the SQLite publication authority boundary."""
from contextlib import contextmanager
import json
import re
import secrets
import uuid

from sqlalchemy import bindparam, text

from .auth import AuthError, digest, stamp
from .database import write_transaction


class PermissionConflict(AuthError):
    def __init__(self, targets=()):
        super().__init__("AUTHZ_CONFLICT", 409)
        self.targets = list(targets)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


USER_SELECT = """SELECT u.id,u.username,u.role,u.status,u.authz_version,
    (u.role='admin' OR EXISTS(SELECT 1 FROM user_permissions p
     WHERE p.user_id=u.id AND p.permission='content.upload')) AS can_upload
    FROM users u"""


class PermissionService:
    PREVIEW_SECONDS = 300
    ACTIONS = {"grant", "revoke", "suspend", "activate"}

    def __init__(self, auth):
        self.auth = auth
        self.engine = auth.engine

    def _rows(self, conn, ids):
        stmt = text(USER_SELECT + " WHERE u.id IN :ids ORDER BY u.id").bindparams(bindparam("ids", expanding=True))
        return [dict(r) for r in conn.execute(stmt, {"ids": ids}).mappings()]

    def list_users(self, token, *, search="", state="all", limit=50, offset=0):
        if len(search) > 64 or state not in {"all", "active", "suspended", "upload", "no_upload"} or not 1 <= limit <= 200 or not 0 <= offset <= 1000000:
            raise AuthError("invalid_input", 400)
        with self.engine.connect() as conn:
            self.auth.current(conn, token, admin=True)
            rows = self._search(conn, search, state, limit, offset)
            return {"users": rows, "limit": limit, "offset": offset}

    def _search(self, conn, search, state, limit, offset=0):
        # Search is literal substring, not SQL LIKE with user wildcard semantics.
        sql = "SELECT * FROM (" + USER_SELECT + ") WHERE instr(username,:search)>0"
        if state in {"active", "suspended"}:
            sql += " AND status=:state"
        elif state in {"upload", "no_upload"}:
            sql += " AND can_upload=:upload"
        return [dict(r) for r in conn.execute(text(sql + " ORDER BY username,id LIMIT :limit OFFSET :offset"),
            dict(search=search.strip().lower(), state=state, upload=int(state == "upload"), limit=limit, offset=offset)).mappings()]

    def _validate_targets(self, rows, ids, expected, action):
        found = {r["id"]: r for r in rows}
        blocked = []
        for ident in ids:
            row = found.get(ident)
            why = None
            if not row:
                why = "unknown_user"
            elif row["role"] != "member":
                why = "protected_admin"
            elif row["authz_version"] != expected.get(ident):
                why = "authz_version_changed"
            elif row["status"] != ("suspended" if action == "activate" else "active"):
                why = "status_conflict"
            if why:
                blocked.append({"user_id": ident, "reason": why})
        if blocked:
            raise PermissionConflict(blocked)

    def _impact(self, conn, ids):
        stmt = text("""SELECT id FROM upload_sessions WHERE owner_id IN :ids
            AND status IN ('receiving','staged') AND expires_at>:now ORDER BY id""").bindparams(bindparam("ids", expanding=True))
        return list(conn.execute(stmt, dict(ids=ids, now=self.auth.now())).scalars())

    def preview(self, token, csrf, *, action, reason, user_ids=None, expected_authz_versions=None, selection=None):
        if action not in self.ACTIONS or not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise AuthError("invalid_input", 400)
        if (user_ids is None) == (selection is None):
            raise AuthError("invalid_selection", 400)
        raw = secrets.token_urlsafe(32)
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, token, csrf=csrf, admin=True)
            if selection is not None:
                if expected_authz_versions is not None or set(selection) != {"search", "state"}:
                    raise AuthError("invalid_selection", 400)
                search, state = selection["search"], selection["state"]
                if not isinstance(search, str) or len(search) > 64 or state not in {"all", "active", "suspended", "upload", "no_upload"}:
                    raise AuthError("invalid_selection", 400)
                rows = self._search(conn, search, state, 201)
                ids = sorted(r["id"] for r in rows)
                expected = {r["id"]: r["authz_version"] for r in rows}
            else:
                if not isinstance(user_ids, list) or not all(isinstance(i, str) and 1 <= len(i) <= 64 for i in user_ids):
                    raise AuthError("invalid_selection", 400)
                ids = sorted(user_ids)
                expected = expected_authz_versions
                if not isinstance(expected, dict) or set(expected) != set(ids) or any(type(v) is not int or v < 1 for v in expected.values()):
                    raise AuthError("invalid_selection", 400)
                rows = self._rows(conn, ids) if 1 <= len(ids) <= 200 else []
            if not 1 <= len(ids) <= 200 or len(set(ids)) != len(ids):
                raise AuthError("selection_size", 400)
            self._validate_targets(rows, ids, expected, action)
            affected = self._impact(conn, ids) if action in {"revoke", "suspend"} else []
            payload = dict(action=action, reason=reason.strip(), ids=ids, expected=expected, uploads=affected)
            now = self.auth.now()
            conn.execute(text("INSERT INTO permission_previews VALUES(:hash,:actor,:payload,:now,:expiry,NULL)"),
                dict(hash=digest(raw), actor=actor["id"], payload=canonical(payload), now=now, expiry=now + self.PREVIEW_SECONDS))
            return dict(preview_token=raw, expires_at=now + self.PREVIEW_SECONDS, eligible_count=len(ids), blocked=[],
                        targets=rows, action=action, reason=payload["reason"], impact={"upload_sessions_to_block": len(affected)})

    def commit(self, token, csrf, *, preview_token, action, reason, confirm_target_count, confirmed, idempotency_key, client, admin_password):
        if (action not in self.ACTIONS or not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500
                or confirmed is not True or type(confirm_target_count) is not int or not 1 <= confirm_target_count <= 200
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{8,128}", idempotency_key)
                or not 32 <= len(preview_token) <= 128 or len(client) > 200
                or not isinstance(admin_password, str) or not 1 <= len(admin_password) <= 256):
            raise AuthError("invalid_input", 400)
        payload_hash = digest(canonical(dict(preview=digest(preview_token), action=action,
                    reason=reason.strip(), count=confirm_target_count, confirmed=True)))
        # Expensive password verification is outside the publication writer lock.
        with self.engine.connect() as conn:
            verified_actor = self.auth.current(conn, token, csrf=csrf, admin=True)
            password_hash = conn.execute(text("SELECT password_hash FROM users WHERE id=:id"),
                                         dict(id=verified_actor["id"])).scalar_one()
        attempt_name = "permission-reauth:" + verified_actor["id"]
        self.auth._reserve_attempt(attempt_name, client)
        if not self.auth._verify(password_hash, admin_password):
            raise AuthError("invalid_credentials")
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, token, csrf=csrf, admin=True)
            if actor["credential_version"] != verified_actor["credential_version"]:
                raise AuthError("authentication_required")
            conn.execute(text("DELETE FROM auth_limits WHERE bucket=:bucket"), dict(bucket="user:" + digest(attempt_name)))
            previous = conn.execute(text("SELECT * FROM permission_change_batches WHERE idempotency_key=:key"),
                                    dict(key=idempotency_key)).mappings().first()
            if previous:
                if previous["actor_id"] != actor["id"] or previous["payload_digest"] != payload_hash:
                    raise AuthError("idempotency_conflict", 409)
                return json.loads(previous["result_json"])
            preview = conn.execute(text("""SELECT * FROM permission_previews WHERE token_hash=:hash
                AND actor_id=:actor AND consumed_at IS NULL AND expires_at>:now"""),
                dict(hash=digest(preview_token), actor=actor["id"], now=self.auth.now())).mappings().first()
            if not preview:
                raise AuthError("invalid_or_expired_preview", 409)
            payload = json.loads(preview["payload_json"])
            ids = payload["ids"]
            if payload["action"] != action or payload["reason"] != reason.strip() or len(ids) != confirm_target_count:
                raise AuthError("preview_mismatch", 409)
            rows = self._rows(conn, ids)
            self._validate_targets(rows, ids, payload["expected"], action)
            # A newly started upload changes the reviewed impact, even if permission did not change.
            affected = self._impact(conn, ids) if action in {"revoke", "suspend"} else []
            if affected != payload["uploads"]:
                raise AuthError("impact_changed", 409)
            batch = str(uuid.uuid4())
            changed_count = 0
            for row in rows:
                ident = row["id"]
                if action == "grant":
                    conn.execute(text("""INSERT INTO user_permissions VALUES(:id,'content.upload',:actor,:stamp)
                        ON CONFLICT(user_id,permission) DO NOTHING"""),
                        dict(id=ident, actor=actor["id"], stamp=stamp(self.auth.now())))
                elif action == "revoke":
                    conn.execute(text("DELETE FROM user_permissions WHERE user_id=:id AND permission='content.upload'"), dict(id=ident))
                status = "suspended" if action == "suspend" else "active"
                changed = action in {"suspend", "activate"} or bool(row["can_upload"]) != (action == "grant")
                changed_count += int(changed)
                conn.execute(text("UPDATE users SET authz_version=authz_version+:delta,status=:status WHERE id=:id"), dict(id=ident, status=status, delta=int(changed)))
                if action in {"suspend", "activate"}:
                    conn.execute(text("DELETE FROM auth_sessions WHERE user_id=:id"), dict(id=ident))
                if action in {"revoke", "suspend"}:
                    conn.execute(text("UPDATE upload_sessions SET status='revoked' WHERE owner_id=:id AND status IN ('receiving','staged')"), dict(id=ident))
                    conn.execute(text("UPDATE upload_batches SET status='revoked' WHERE uploader_id=:id AND status='open'"), dict(id=ident))
                new = dict(status=status, can_upload=action == "grant" or (bool(row["can_upload"]) and action != "revoke"), authz_version=row["authz_version"] + int(changed))
                self.auth.audit(conn, actor["id"], "permission_changed", target=ident,
                    details=dict(batch_id=batch, action=action, old=row, new=new, reason=payload["reason"]))
            result = dict(batch_id=batch, action=action, user_ids=ids, changed_count=changed_count, upload_sessions_blocked=len(affected))
            self.auth.audit(conn, actor["id"], "permission_batch", details=dict(**result, reason=payload["reason"], client=client, idempotency_key=idempotency_key))
            conn.execute(text("UPDATE permission_previews SET consumed_at=:now WHERE token_hash=:hash"), dict(now=self.auth.now(), hash=digest(preview_token)))
            conn.execute(text("INSERT INTO permission_change_batches VALUES(:id,:actor,:action,:key,:hash,'committed',:result,:now)"),
                dict(id=batch, actor=actor["id"], action=action, key=idempotency_key, hash=payload_hash, result=canonical(result), now=self.auth.now()))
            return result

    @contextmanager
    def publication(self, upload_id):
        """Worker-only boundary. Caller publishes ALL database changes in yielded txn.

        Acquire filesystem/storage locks before entering, per v2.1. No HTTP or
        filesystem work is performed here. G0-D/G1 must use this same connection.
        """
        with write_transaction(self.engine) as conn:
            with self.publication_in(conn, upload_id):
                yield conn

    def check_upload_in(self, conn, upload_id):
        """Recheck current upload authority in the caller's existing writer."""
        row = conn.execute(text("""SELECT s.id FROM upload_sessions s
                JOIN users u ON u.id=s.owner_id JOIN upload_batches b ON b.id=s.batch_id
                WHERE s.id=:id AND s.status='staged' AND b.status='open' AND s.expires_at>:now
                AND s.received_size=s.declared_size AND u.status='active'
                AND s.authz_version=u.authz_version
                AND (u.role='admin' OR EXISTS(SELECT 1 FROM user_permissions p
                    WHERE p.user_id=u.id AND p.permission='content.upload'))"""),
                dict(id=upload_id, now=self.auth.now())).first()
        if not row:
            raise AuthError("upload_permission_revoked", 403)

    @contextmanager
    def publication_in(self, conn, upload_id):
        """Compose with job completion; never starts or commits a second writer."""
        self.check_upload_in(conn, upload_id)
        yield conn
        self.finish_publication_in(conn, upload_id)

    def finish_publication_in(self, conn, upload_id):
        """Final DB-only authorization after all publication/followup effects."""
        self.check_upload_in(conn, upload_id)
        conn.execute(text("UPDATE upload_sessions SET status='published' WHERE id=:id"), dict(id=upload_id))
