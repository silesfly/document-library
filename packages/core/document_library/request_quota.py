"""Durable account quotas; callers retain their own fresh authority boundaries."""
from collections.abc import Mapping
from typing import Literal

from sqlalchemy import Connection, text

from .auth import AuthError, AuthService, digest
from .database import write_transaction

Action = Literal['social', 'recommend', 'download']
_LIMITS = {'social': 30, 'recommend': 30, 'download': 60}
_WINDOW_SECONDS = 60


class QuotaExceeded(AuthError):
    def __init__(self, retry_after_seconds: int):
        if type(retry_after_seconds) is not int or not 1 <= retry_after_seconds <= 60:
            raise ValueError('Bounded retry delay required')
        self.retry_after_seconds = retry_after_seconds
        super().__init__('RATE_LIMITED', 429)


class AccountQuota:
    def __init__(self, auth: AuthService):
        self.auth, self.engine = auth, auth.engine

    def reserve_in(self, conn: Connection, *, actor: Mapping[str, object], action: Action) -> None:
        """Caller owns the current authenticated writer, including its rollback."""
        if action not in _LIMITS:
            raise ValueError('Unknown quota action')
        now = self.auth.now()
        bucket = 'g3:' + action + ':' + digest(actor['id'])
        row = conn.execute(text('SELECT window_start,attempts FROM auth_limits WHERE bucket=:bucket'),
                           dict(bucket=bucket)).mappings().first()
        if row and now < row['window_start']:
            raise QuotaExceeded(_WINDOW_SECONDS)
        fresh = row is None or now - row['window_start'] >= _WINDOW_SECONDS
        if not fresh and row['attempts'] >= _LIMITS[action]:
            raise QuotaExceeded(max(1, min(60, row['window_start'] + _WINDOW_SECONDS - now)))
        start = now if fresh else row['window_start']
        attempts = 1 if fresh else row['attempts'] + 1
        conn.execute(text('''INSERT INTO auth_limits(bucket,window_start,attempts,next_allowed)
            VALUES(:bucket,:start,:attempts,:end)
            ON CONFLICT(bucket) DO UPDATE SET window_start=excluded.window_start,
            attempts=excluded.attempts,next_allowed=excluded.next_allowed'''),
            dict(bucket=bucket, start=start, attempts=attempts, end=start + _WINDOW_SECONDS))

    def admit(self, token: str, *, action: Literal['recommend', 'download'], csrf: str | None = None) -> None:
        """Commit a read/download cost, never an authority proof for later work."""
        if action not in {'recommend', 'download'}:
            raise ValueError('Separate social admission is forbidden')
        if csrf is not None and (not isinstance(csrf, str) or not csrf):
            raise AuthError('csrf_rejected', 403)
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, token, csrf=csrf)
            self.reserve_in(conn, actor=actor, action=action)
