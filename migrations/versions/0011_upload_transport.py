"""Upload request identity and durable transport occupancy; no new credentials."""
from alembic import op

revision = '0011_upload_transport'
down_revision = '0010_sources'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE upload_batch_requests (
        batch_id TEXT PRIMARY KEY NOT NULL REFERENCES upload_batches(id),
        owner_id TEXT NOT NULL REFERENCES users(id),
        request_key TEXT NOT NULL, request_digest TEXT NOT NULL,
        authz_version INTEGER NOT NULL CHECK(authz_version>0),
        UNIQUE(owner_id,request_key)
    ) STRICT""")
    op.execute("""CREATE TABLE upload_reservations (
        upload_id TEXT PRIMARY KEY NOT NULL REFERENCES upload_sessions(id),
        batch_id TEXT NOT NULL REFERENCES upload_batches(id),
        request_key TEXT NOT NULL, request_digest TEXT NOT NULL,
        display_filename TEXT NOT NULL,
        UNIQUE(batch_id,request_key)
    ) STRICT""")
    op.execute("""CREATE TABLE upload_transport_ops (
        id TEXT PRIMARY KEY NOT NULL,
        upload_id TEXT NOT NULL REFERENCES upload_sessions(id),
        backend_id TEXT NOT NULL,
        method TEXT NOT NULL CHECK(method IN ('POST','HEAD','PATCH','DELETE')),
        state TEXT NOT NULL CHECK(state IN ('reserved','sent','uncertain','complete','stopped')),
        created_at INTEGER NOT NULL, stop_evidence TEXT
    ) STRICT""")
    op.execute("CREATE UNIQUE INDEX upload_one_operation ON upload_transport_ops(upload_id) WHERE state IN ('reserved','sent','uncertain')")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
