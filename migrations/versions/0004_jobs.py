"""G0-C: durable worker fences, completion outbox and non-expiring AI slots."""
from alembic import op

revision = "0004_jobs"
down_revision = "0003_permissions"
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE job_clock (
        id INTEGER PRIMARY KEY CHECK(id=1), epoch TEXT NOT NULL,
        wall_ms INTEGER NOT NULL, mono_ms INTEGER NOT NULL,
        blocked INTEGER NOT NULL DEFAULT 0 CHECK(blocked IN (0,1)),
        fence INTEGER NOT NULL DEFAULT 0 CHECK(fence>=0), reason TEXT NOT NULL DEFAULT ''
    ) STRICT""",
    """CREATE TABLE job_clock_reconciliations (
        id INTEGER PRIMARY KEY, old_fence INTEGER NOT NULL, reason TEXT NOT NULL,
        epoch TEXT NOT NULL, wall_ms INTEGER NOT NULL, invalidated_jobs INTEGER NOT NULL
    ) STRICT""",
    """CREATE TABLE jobs (
        id TEXT PRIMARY KEY NOT NULL, kind TEXT NOT NULL,
        work_id TEXT REFERENCES works(id), input_revision_id TEXT REFERENCES edition_revisions(id),
        object_sha TEXT REFERENCES objects(sha256), upload_session_id TEXT REFERENCES upload_sessions(id),
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json) AND json_type(payload_json)='object'),
        payload_digest TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
        priority INTEGER NOT NULL CHECK(priority>=0),
        state TEXT NOT NULL CHECK(state IN ('queued','running','retry_wait','completed','failed','cancelled')),
        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
        max_attempts INTEGER NOT NULL CHECK(max_attempts>=1), next_run_at INTEGER NOT NULL,
        lease_generation INTEGER NOT NULL DEFAULT 0 CHECK(lease_generation>=0),
        lease_owner TEXT, lease_until INTEGER,
        checkpoint_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(checkpoint_json) AND json_type(checkpoint_json)='object'),
        result_json TEXT CHECK(result_json IS NULL OR (json_valid(result_json) AND json_type(result_json)='object')),
        error_code TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        CHECK((state='running' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
           OR (state!='running' AND lease_owner IS NULL AND lease_until IS NULL))
    ) STRICT""",
    "CREATE INDEX ix_jobs_runnable ON jobs(state,priority,next_run_at,lease_until)",
    """CREATE TABLE job_outbox (
        id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL REFERENCES jobs(id),
        kind TEXT NOT NULL, dedupe_key TEXT NOT NULL UNIQUE,
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json) AND json_type(payload_json)='object'),
        state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','dispatched')),
        created_at INTEGER NOT NULL
    ) STRICT""",
    """CREATE TABLE resource_slots (
        name TEXT PRIMARY KEY NOT NULL, job_id TEXT REFERENCES jobs(id), generation INTEGER,
        owner TEXT, token_hash TEXT, acquired_at INTEGER,
        CHECK((job_id IS NULL AND generation IS NULL AND owner IS NULL AND token_hash IS NULL AND acquired_at IS NULL)
           OR (job_id IS NOT NULL AND generation IS NOT NULL AND owner IS NOT NULL AND token_hash IS NOT NULL AND acquired_at IS NOT NULL))
    ) STRICT""",
    "INSERT INTO resource_slots(name) VALUES('local-qwen'),('local-ocr')",
    """CREATE TABLE resource_release_receipts (
        token_hash TEXT PRIMARY KEY NOT NULL, resource TEXT NOT NULL REFERENCES resource_slots(name),
        job_id TEXT NOT NULL REFERENCES jobs(id), generation INTEGER NOT NULL, owner TEXT NOT NULL,
        disposition TEXT NOT NULL CHECK(disposition IN ('completed','cancelled','not_started')),
        confirmation TEXT NOT NULL, recorded_at INTEGER NOT NULL
    ) STRICT""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Destructive downgrade is not enabled")
