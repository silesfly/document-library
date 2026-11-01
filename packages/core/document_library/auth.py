"""Server-side authority and credential lifecycle. Never log input credentials."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import secrets
import threading
import time
import uuid

from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, InvalidHashError
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

from .database import write_transaction


class AuthError(Exception):
    def __init__(self, code: str, status: int = 401):
        self.code = code
        self.status = status
        super().__init__(code)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stamp(now: int) -> str:
    return datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z")


def username(value: str) -> str:
    normalized = value.strip().lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]{2,63}", normalized):
        raise AuthError("invalid_input", 400)
    return normalized


@dataclass(repr=False)
class Session:
    token: str
    csrf: str
    expires_at: int


class AuthService:
    SESSION_SECONDS = 8 * 3600

    def __init__(self, engine: Engine, *, clock=time.time):
        self.engine = engine
        self.clock = clock
        self.hasher = PasswordHasher()  # RFC 9106 low-memory Argon2id, 64 MiB.
        self._hash_slots = threading.BoundedSemaphore(2)
        self._dummy = self.hasher.hash(secrets.token_urlsafe(32))

    def now(self):
        return int(self.clock())

    def _password_hash(self, password: str) -> str:
        if not 12 <= len(password) <= 256 or len(password.encode("utf-8")) > 1024:
            raise AuthError("invalid_input", 400)
        if not self._hash_slots.acquire(blocking=False):
            raise AuthError("try_later", 429)
        try:
            return self.hasher.hash(password)
        finally:
            self._hash_slots.release()

    def _verify(self, stored: str, password: str) -> bool:
        if len(password.encode("utf-8")) > 1024:
            return False
        if not self._hash_slots.acquire(blocking=False):
            raise AuthError("try_later", 429)
        try:
            try:
                return self.hasher.verify(stored, password)
            except (VerificationError, InvalidHashError):
                return False
        finally:
            self._hash_slots.release()

    def audit(self, conn, actor, action, *, target=None, details=None):
        conn.execute(text("""INSERT INTO audit_events
            (id,actor_id,target_id,action,details_json,created_at)
            VALUES(:id,:actor,:target,:action,:details,:stamp)"""),
            dict(id=str(uuid.uuid4()), actor=actor, target=target, action=action,
                 details=json.dumps(details or {}, separators=(",", ":")), stamp=stamp(self.now())))

    def bootstrap(self, name: str, password: str) -> str:
        """Local operator command only; deliberately not an HTTP endpoint."""
        name, hashed = username(name), self._password_hash(password)
        ident = str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            if conn.execute(text("SELECT 1 FROM users LIMIT 1")).first():
                raise AuthError("already_initialized", 409)
            conn.execute(text("""INSERT INTO users(id,username,password_hash,role,created_at)
                VALUES(:id,:name,:hashed,'admin',:stamp)"""),
                dict(id=ident, name=name, hashed=hashed, stamp=stamp(self.now())))
            self.audit(conn, ident, "bootstrap", target=ident)
        return ident

    def _reserve_attempt(self, name: str | None, ip: str):
        now = self.now()
        with write_transaction(self.engine) as conn:
            buckets = [("ip:", ip, 40)]
            if name is not None:
                buckets.append(("user:", name, 10))
            for prefix, value, ceiling in buckets:
                bucket = prefix + digest(value)
                row = conn.execute(text("SELECT * FROM auth_limits WHERE bucket=:bucket"),
                                   dict(bucket=bucket)).mappings().first()
                fresh = not row or now - row["window_start"] >= 600
                if not fresh and (row["attempts"] >= ceiling or now < row["next_allowed"]):
                    raise AuthError("try_later", 429)
                attempts = 1 if fresh else row["attempts"] + 1
                delay = min(60, 2 ** (attempts - 3)) if prefix == "user:" and attempts >= 3 else 0
                conn.execute(text("""INSERT INTO auth_limits VALUES(:bucket,:start,:attempts,:next)
                    ON CONFLICT(bucket) DO UPDATE SET window_start=excluded.window_start,
                    attempts=excluded.attempts,next_allowed=excluded.next_allowed"""),
                    dict(bucket=bucket, start=now if fresh else row["window_start"],
                         attempts=attempts, next=now + delay))

    def limit_public_request(self, ip: str):
        self._reserve_attempt(None, ip)

    def login(self, name: str, password: str, *, ip: str) -> Session:
        name = username(name)
        self._reserve_attempt(name, ip)
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT * FROM users WHERE username=:name"),
                               dict(name=name)).mappings().first()
        verified = self._verify(row["password_hash"] if row else self._dummy, password)
        if not row or not verified or row["status"] != "active":
            raise AuthError("invalid_credentials")
        session = Session(secrets.token_urlsafe(32), secrets.token_urlsafe(32), self.now() + self.SESSION_SECONDS)
        with write_transaction(self.engine) as conn:
            current = conn.execute(text("SELECT * FROM users WHERE id=:id"), dict(id=row["id"])).mappings().one()
            if (current["status"] != "active" or current["credential_version"] != row["credential_version"]
                    or current["password_hash"] != row["password_hash"]):
                raise AuthError("invalid_credentials")
            conn.execute(text("""INSERT INTO auth_sessions VALUES(:token,:csrf,:user,:version,:created,:expires)"""),
                dict(token=digest(session.token), csrf=digest(session.csrf), user=row["id"],
                     version=row["credential_version"], created=self.now(), expires=session.expires_at))
            conn.execute(text("DELETE FROM auth_limits WHERE bucket=:bucket"), dict(bucket="user:" + digest(name)))
        return session

    def current(self, conn, token: str, *, csrf: str | None = None, admin=False, upload=False) -> dict:
        row = conn.execute(text("""SELECT u.id,u.username,u.role,u.status,u.authz_version,
            u.credential_version,s.csrf_hash,
            EXISTS(SELECT 1 FROM user_permissions p WHERE p.user_id=u.id AND p.permission='content.upload') AS can_upload
            FROM auth_sessions s JOIN users u ON u.id=s.user_id
            WHERE s.token_hash=:token AND s.expires_at>:now AND s.credential_version=u.credential_version
            AND u.status='active'"""), dict(token=digest(token), now=self.now())).mappings().first()
        if not row:
            raise AuthError("authentication_required")
        if csrf is not None and not hmac.compare_digest(digest(csrf), row["csrf_hash"]):
            raise AuthError("csrf_rejected", 403)
        if (admin and row["role"] != "admin") or (upload and row["role"] != "admin" and not row["can_upload"]):
            raise AuthError("permission_denied", 403)
        return dict(id=row["id"], username=row["username"], role=row["role"],
                    authz_version=row["authz_version"], credential_version=row["credential_version"],
                    can_upload=bool(row["can_upload"] or row["role"] == "admin"))

    def me(self, token: str):
        with self.engine.connect() as conn:
            return self.current(conn, token)

    def logout(self, token: str, csrf: str):
        with write_transaction(self.engine) as conn:
            self.current(conn, token, csrf=csrf)
            conn.execute(text("DELETE FROM auth_sessions WHERE token_hash=:token"), dict(token=digest(token)))

    def issue_token(self, token: str, csrf: str, *, reset_user: str | None = None) -> str:
        raw = secrets.token_urlsafe(32)
        with write_transaction(self.engine) as conn:
            actor = self.current(conn, token, csrf=csrf, admin=True)
            if reset_user:
                target = conn.execute(text("SELECT role,status FROM users WHERE id=:id"),
                                      dict(id=reset_user)).mappings().first()
                if not target or target["role"] != "member" or target["status"] != "active":
                    raise AuthError("invalid_target", 400)
                # Only the newest reset link remains usable.
                conn.execute(text("DELETE FROM auth_tokens WHERE purpose='reset' AND target_id=:id AND consumed_at IS NULL"),
                             dict(id=reset_user))
            now = self.now()
            conn.execute(text("""INSERT INTO auth_tokens
                (token_hash,purpose,target_id,created_by,created_at,expires_at)
                VALUES(:hash,:purpose,:target,:actor,:now,:expires)"""),
                dict(hash=digest(raw), purpose="reset" if reset_user else "invite", target=reset_user,
                     actor=actor["id"], now=now, expires=now + (900 if reset_user else 86400)))
            self.audit(conn, actor["id"], "reset_issued" if reset_user else "invite_issued", target=reset_user)
        return raw

    def _consume(self, conn, raw: str, purpose: str):
        row = conn.execute(text("""SELECT t.* FROM auth_tokens t JOIN users u ON u.id=t.created_by
            WHERE token_hash=:hash AND purpose=:purpose AND consumed_at IS NULL
            AND expires_at>:now AND u.role='admin' AND u.status='active'"""),
            dict(hash=digest(raw), purpose=purpose, now=self.now())).mappings().first()
        if not row:
            raise AuthError("invalid_or_expired_token", 400)
        return row

    def register(self, invite: str, name: str, password: str) -> str:
        name, hashed = username(name), self._password_hash(password)
        ident = str(uuid.uuid4())
        try:
            with write_transaction(self.engine) as conn:
                grant = self._consume(conn, invite, "invite")
                conn.execute(text("""INSERT INTO users(id,username,password_hash,created_at)
                    VALUES(:id,:name,:hashed,:stamp)"""),
                    dict(id=ident, name=name, hashed=hashed, stamp=stamp(self.now())))
                self._mark_consumed(conn, grant["token_hash"], ident)
                self.audit(conn, grant["created_by"], "invite_redeemed", target=ident)
        except IntegrityError:
            raise AuthError("registration_rejected", 400) from None
        return ident

    def _mark_consumed(self, conn, token_hash: str, user_id: str):
        conn.execute(text("UPDATE auth_tokens SET consumed_at=:now,consumed_by=:user WHERE token_hash=:hash"),
                     dict(now=self.now(), user=user_id, hash=token_hash))

    def _replace_password(self, conn, user_id, hashed):
        conn.execute(text("""UPDATE users SET password_hash=:hash,credential_version=credential_version+1
            WHERE id=:id"""), dict(hash=hashed, id=user_id))
        conn.execute(text("DELETE FROM auth_sessions WHERE user_id=:id"), dict(id=user_id))
        conn.execute(text("DELETE FROM auth_tokens WHERE purpose='reset' AND target_id=:id AND consumed_at IS NULL"),
                     dict(id=user_id))

    def _reserve_password_change(self, user_id):
        now = self.now()
        bucket = "change-password:" + digest(str(user_id))
        with write_transaction(self.engine) as conn:
            row = conn.execute(
                text("SELECT * FROM auth_limits WHERE bucket=:bucket"),
                dict(bucket=bucket),
            ).mappings().first()
            if row is not None:
                elapsed = now - row["window_start"]
                fresh = elapsed >= 600
                if not fresh and (elapsed < 0 or row["attempts"] >= 10 or now < row["next_allowed"]):
                    raise AuthError("try_later", 429)
                start = now if fresh else row["window_start"]
                attempts = 1 if fresh else row["attempts"] + 1
            else:
                start = now
                attempts = 1
            conn.execute(
                text("""INSERT INTO auth_limits VALUES(:bucket,:start,:attempts,:next)
                    ON CONFLICT(bucket) DO UPDATE SET window_start=excluded.window_start,
                    attempts=excluded.attempts,next_allowed=excluded.next_allowed"""),
                dict(bucket=bucket, start=start, attempts=attempts, next=now),
            )

    def change_password(self, token: str, csrf: str, old: str, new: str):
        with self.engine.connect() as conn:
            actor = self.current(conn, token, csrf=csrf)
            old_hash = conn.execute(text("SELECT password_hash FROM users WHERE id=:id"), dict(id=actor["id"])).scalar_one()
        self._reserve_password_change(actor["id"])
        if not self._verify(old_hash, old):
            raise AuthError("invalid_credentials")
        hashed = self._password_hash(new)
        with write_transaction(self.engine) as conn:
            current = self.current(conn, token, csrf=csrf)
            if current["credential_version"] != actor["credential_version"]:
                raise AuthError("authentication_required")
            self._replace_password(conn, actor["id"], hashed)
            self.audit(conn, actor["id"], "password_changed", target=actor["id"])
    def reset_password(self, reset: str, new: str):
        hashed = self._password_hash(new)
        with write_transaction(self.engine) as conn:
            grant = self._consume(conn, reset, "reset")
            active = conn.execute(text("SELECT status FROM users WHERE id=:id"), dict(id=grant["target_id"])).scalar_one()
            if active != "active":
                raise AuthError("invalid_or_expired_token", 400)
            self._mark_consumed(conn, grant["token_hash"], grant["target_id"])
            self._replace_password(conn, grant["target_id"], hashed)
            self.audit(conn, grant["target_id"], "password_reset", target=grant["target_id"])
