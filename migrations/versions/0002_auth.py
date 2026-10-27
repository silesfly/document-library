"""G0-B1: invitation and credential lifecycle, hashed sessions and audit."""

from alembic import op

revision = "0002_auth"
down_revision = "0001_catalog"
branch_labels = None
depends_on = None

STATEMENTS = (
    "ALTER TABLE users ADD COLUMN credential_version INTEGER NOT NULL DEFAULT 1 CHECK(credential_version>=1)",
    """CREATE TABLE user_permissions (
        user_id TEXT NOT NULL REFERENCES users(id),
        permission TEXT NOT NULL CHECK(permission='content.upload'),
        granted_by TEXT NOT NULL REFERENCES users(id), granted_at TEXT NOT NULL,
        PRIMARY KEY(user_id,permission)
    ) STRICT""",
    """CREATE TABLE auth_sessions (
        token_hash TEXT PRIMARY KEY NOT NULL, csrf_hash TEXT NOT NULL,
        user_id TEXT NOT NULL REFERENCES users(id), credential_version INTEGER NOT NULL,
        created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
        CHECK(expires_at>created_at)
    ) STRICT""",
    "CREATE INDEX ix_sessions_user ON auth_sessions(user_id)",
    """CREATE TABLE auth_tokens (
        token_hash TEXT PRIMARY KEY NOT NULL,
        purpose TEXT NOT NULL CHECK(purpose IN ('invite','reset')),
        target_id TEXT REFERENCES users(id), created_by TEXT NOT NULL REFERENCES users(id),
        created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL,
        consumed_at INTEGER, consumed_by TEXT REFERENCES users(id),
        CHECK(expires_at>created_at),
        CHECK((purpose='invite' AND target_id IS NULL) OR (purpose='reset' AND target_id IS NOT NULL)),
        CHECK((consumed_at IS NULL)=(consumed_by IS NULL))
    ) STRICT""",
    """CREATE TABLE auth_limits (
        bucket TEXT PRIMARY KEY NOT NULL, window_start INTEGER NOT NULL,
        attempts INTEGER NOT NULL CHECK(attempts>0), next_allowed INTEGER NOT NULL
    ) STRICT""",
    """CREATE TABLE audit_events (
        id TEXT PRIMARY KEY NOT NULL, actor_id TEXT NOT NULL REFERENCES users(id),
        target_id TEXT REFERENCES users(id), action TEXT NOT NULL,
        details_json TEXT NOT NULL CHECK(json_valid(details_json) AND json_type(details_json)='object'),
        created_at TEXT NOT NULL
    ) STRICT""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Destructive downgrade is not enabled")
