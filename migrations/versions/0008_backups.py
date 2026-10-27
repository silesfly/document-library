"""G0-D3: local fixed snapshot identity and durable required-object membership."""
from alembic import op
revision = '0008_backups'
down_revision = '0007_orphans'
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE backup_sets (
        id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL REFERENCES jobs(id),
        generation INTEGER NOT NULL CHECK(generation>0), owner_digest TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('CAPTURING','BUILDING','SEALED','FAILED')),
        created_at INTEGER NOT NULL, snapshot_at INTEGER, storage_sequence INTEGER,
        snapshot_sha TEXT, snapshot_size INTEGER, manifest_sha TEXT, manifest_json TEXT,
        sealed_at INTEGER, failure TEXT, UNIQUE(job_id,generation),
        CHECK(manifest_json IS NULL OR json_valid(manifest_json)),
        CHECK(state NOT IN ('BUILDING','SEALED') OR (snapshot_at IS NOT NULL AND
            storage_sequence IS NOT NULL AND snapshot_sha IS NOT NULL AND snapshot_size IS NOT NULL
            AND manifest_sha IS NOT NULL AND manifest_json IS NOT NULL)),
        CHECK(state<>'SEALED' OR sealed_at IS NOT NULL)
    ) STRICT""",
    """CREATE TABLE backup_members (
        set_id TEXT NOT NULL REFERENCES backup_sets(id),
        object_sha TEXT NOT NULL REFERENCES objects(sha256),
        PRIMARY KEY(set_id,object_sha)
    ) STRICT""",
    """CREATE TRIGGER backup_set_identity BEFORE UPDATE OF id,job_id,generation,owner_digest,created_at
        ON backup_sets BEGIN SELECT RAISE(ABORT,'backup execution identity is immutable'); END""",
    """CREATE TRIGGER backup_snapshot_point BEFORE UPDATE OF snapshot_at,storage_sequence ON backup_sets
        WHEN OLD.snapshot_at IS NOT NULL
        BEGIN SELECT RAISE(ABORT,'backup snapshot point is immutable'); END""",
    """CREATE TRIGGER backup_manifest_identity BEFORE UPDATE OF snapshot_sha,snapshot_size,manifest_sha,manifest_json
        ON backup_sets WHEN OLD.manifest_sha IS NOT NULL
        BEGIN SELECT RAISE(ABORT,'backup manifest identity is immutable'); END""",
    """CREATE TRIGGER backup_member_insert BEFORE INSERT ON backup_members
        WHEN NOT EXISTS(SELECT 1 FROM backup_sets WHERE id=NEW.set_id AND state='CAPTURING' AND snapshot_at IS NULL)
        BEGIN SELECT RAISE(ABORT,'backup membership is frozen'); END""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)
    for event in ('UPDATE','DELETE'):
        op.execute(f"CREATE TRIGGER backup_member_{event.lower()} BEFORE {event} ON backup_members "
                   "BEGIN SELECT RAISE(ABORT,'backup membership is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
