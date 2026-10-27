"""Own social mutations, with fresh authority and quota on one SQLite writer."""
from collections.abc import Callable, Mapping
import json

from sqlalchemy import Connection, text

from .auth import AuthError, AuthService, stamp
from .database import write_transaction
from .library_contracts import LibraryQuery, _ident, canonical_json
from .library_projection import VALID_OPTIONS_V1
from .request_quota import AccountQuota

_FILTERS = ('categories', 'states', 'formats', 'representations', 'genres')


class MemberError(AuthError):
    pass


def _csrf(value):
    if not isinstance(value, str) or not value:
        raise MemberError('csrf_rejected', 403)


def _work(value):
    try:
        return _ident(value)
    except ValueError:
        raise MemberError('INVALID_INPUT', 422) from None


def _preferences(value):
    """Validate complete replacement; LibraryQuery owns filter normalization."""
    try:
        if not isinstance(value, Mapping) or set(value) != {'filters', 'view', 'recommendations'}:
            raise ValueError
        filters, recommendations = value['filters'], value['recommendations']
        if not isinstance(filters, Mapping) or set(filters) != set(_FILTERS):
            raise ValueError
        if (not isinstance(recommendations, Mapping)
                or set(recommendations) != {'exclude_rated', 'exclude_downloaded'}
                or any(type(v) is not bool for v in recommendations.values())):
            raise ValueError
        if not isinstance(value['view'], str) or value['view'] not in {'grid', 'list'}:
            raise ValueError
        if any(not isinstance(filters[name], (list, tuple)) for name in _FILTERS):
            raise ValueError
        query = LibraryQuery(**{name: tuple(filters[name]) for name in _FILTERS})
        return dict(filters={name: list(getattr(query, name)) for name in _FILTERS},
                    view=value['view'], recommendations=dict(recommendations))
    except (TypeError, ValueError):
        raise MemberError('INVALID_INPUT', 422) from None


class MemberService:
    def __init__(self, auth: AuthService, *, quota: AccountQuota, read_social_in: Callable[..., dict]):
        if quota.auth is not auth or quota.engine is not auth.engine or not callable(read_social_in):
            raise ValueError('One authority and quota with an explicit social reader required')
        self.auth, self.engine = auth, auth.engine
        self.quota, self.read_social_in = quota, read_social_in

    def _mutate(self, token, csrf, work_id, mutation):
        _csrf(csrf)
        work_id = _work(work_id)
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, token, csrf=csrf)
            if conn.execute(text('WITH ' + VALID_OPTIONS_V1 +
                    ' SELECT 1 FROM valid_options WHERE work_id=:work LIMIT 1'), dict(work=work_id)).first() is None:
                raise MemberError('NOT_FOUND', 404)
            self.quota.reserve_in(conn, actor=actor, action='social')
            mutation(conn, actor['id'], work_id)
            measured = self.read_social_in(conn, actor=actor, work_id=work_id)
        return measured

    def set_rating(self, token: str, csrf: str, work_id: str, rating_half_steps: int) -> dict:
        if type(rating_half_steps) is not int or not 1 <= rating_half_steps <= 10:
            raise MemberError('INVALID_INPUT', 422)
        def put(conn, user, work):
            conn.execute(text('''INSERT INTO ratings(user_id,work_id,rating_half_steps,updated_at)
                VALUES(:user,:work,:rating,:stamp)
                ON CONFLICT(user_id,work_id) DO UPDATE SET rating_half_steps=excluded.rating_half_steps,
                updated_at=excluded.updated_at WHERE ratings.rating_half_steps<>excluded.rating_half_steps'''),
                dict(user=user, work=work, rating=rating_half_steps, stamp=stamp(self.auth.now())))
        return self._mutate(token, csrf, work_id, put)

    def delete_rating(self, token: str, csrf: str, work_id: str) -> dict:
        def delete(conn, user, work):
            conn.execute(text('DELETE FROM ratings WHERE user_id=:user AND work_id=:work'),
                         dict(user=user, work=work))
        return self._mutate(token, csrf, work_id, delete)

    def set_favorite(self, token: str, csrf: str, work_id: str, favorite: bool) -> dict:
        if type(favorite) is not bool:
            raise MemberError('INVALID_INPUT', 422)
        def put(conn, user, work):
            if favorite:
                conn.execute(text('''INSERT INTO library_favorites(user_id,work_id,created_at)
                    VALUES(:user,:work,:stamp) ON CONFLICT(user_id,work_id) DO NOTHING'''),
                    dict(user=user, work=work, stamp=stamp(self.auth.now())))
            else:
                conn.execute(text('DELETE FROM library_favorites WHERE user_id=:user AND work_id=:work'),
                             dict(user=user, work=work))
        return self._mutate(token, csrf, work_id, put)

    def preferences_in(self, conn: Connection, *, actor: Mapping[str, object]) -> dict:
        """SELECT only on the caller snapshot; no-row reads create no state."""
        row = conn.execute(text('SELECT preferences_json,updated_at FROM member_preferences WHERE user_id=:user'),
                           dict(user=actor['id'])).mappings().first()
        if row is None:
            return dict(filters={name: [] for name in _FILTERS}, view='grid',
                        recommendations=dict(exclude_rated=True, exclude_downloaded=False), updated_at=None)
        return dict(json.loads(row['preferences_json']), updated_at=row['updated_at'])

    def get_preferences(self, token: str) -> dict:
        with self.engine.connect() as conn:
            actor = self.auth.current(conn, token)
            return self.preferences_in(conn, actor=actor)

    def set_preferences(self, token: str, csrf: str, preferences: Mapping[str, object]) -> dict:
        _csrf(csrf)
        payload = canonical_json(_preferences(preferences))
        with write_transaction(self.engine) as conn:
            actor = self.auth.current(conn, token, csrf=csrf)
            self.quota.reserve_in(conn, actor=actor, action='social')
            conn.execute(text('''INSERT INTO member_preferences(user_id,preferences_json,updated_at)
                VALUES(:user,:payload,:stamp) ON CONFLICT(user_id) DO UPDATE SET
                preferences_json=excluded.preferences_json,updated_at=excluded.updated_at
                WHERE member_preferences.preferences_json<>excluded.preferences_json'''),
                dict(user=actor['id'], payload=payload, stamp=stamp(self.auth.now())))
            saved = self.preferences_in(conn, actor=actor)
        return saved
