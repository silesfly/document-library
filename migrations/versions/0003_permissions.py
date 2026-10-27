"""G0-B2: fixed permission previews, atomic receipts and upload authority rows."""
from alembic import op

revision = "0003_permissions"
down_revision = "0002_auth"
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE permission_previews (
        token_hash TEXT PRIMARY KEY NOT NULL, actor_id TEXT NOT NULL REFERENCES users(id),
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
        created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL CHECK(expires_at>created_at),
        consumed_at INTEGER
    ) STRICT""",
    """CREATE TABLE permission_change_batches (
        id TEXT PRIMARY KEY NOT NULL, actor_id TEXT NOT NULL REFERENCES users(id),
        action TEXT NOT NULL CHECK(action IN ('grant','revoke','suspend','activate')),
        idempotency_key TEXT NOT NULL UNIQUE, payload_digest TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status='committed'),
        result_json TEXT NOT NULL CHECK(json_valid(result_json)), created_at INTEGER NOT NULL
    ) STRICT""",
    """CREATE TABLE upload_batches (
        id TEXT PRIMARY KEY NOT NULL, uploader_id TEXT NOT NULL REFERENCES users(id),
        declared_total_bytes INTEGER NOT NULL CHECK(declared_total_bytes>=0),
        status TEXT NOT NULL CHECK(status IN ('open','closed','revoked')),
        created_at INTEGER NOT NULL, UNIQUE(id,uploader_id)
    ) STRICT""",
    """CREATE TABLE upload_sessions (
        id TEXT PRIMARY KEY NOT NULL, batch_id TEXT NOT NULL, owner_id TEXT NOT NULL,
        tus_id TEXT UNIQUE, authz_version INTEGER NOT NULL CHECK(authz_version>=1),
        declared_size INTEGER NOT NULL CHECK(declared_size>=0),
        received_size INTEGER NOT NULL DEFAULT 0 CHECK(received_size>=0 AND received_size<=declared_size),
        expires_at INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('receiving','staged','published','revoked','expired')),
        FOREIGN KEY(batch_id,owner_id) REFERENCES upload_batches(id,uploader_id)
    ) STRICT""",
    "CREATE INDEX ix_upload_sessions_owner_status ON upload_sessions(owner_id,status)",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Destructive downgrade is not enabled")
