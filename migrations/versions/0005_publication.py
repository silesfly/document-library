"""G0-D2: approved plans, durable install intents and immutable revision seals."""
from alembic import op

revision = '0005_publication'
down_revision = '0004_jobs'
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE publication_plans (
        job_id TEXT NOT NULL REFERENCES jobs(id), generation INTEGER NOT NULL CHECK(generation>0),
        payload_json TEXT NOT NULL CHECK(json_valid(payload_json) AND json_type(payload_json)='object'),
        payload_digest TEXT NOT NULL, validation_receipt TEXT NOT NULL,
        rights_status TEXT NOT NULL CHECK(rights_status IN ('allowed','review','blocked')),
        created_at INTEGER NOT NULL, PRIMARY KEY(job_id,generation)
    ) STRICT""",
    """CREATE TABLE storage_intents (
        id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL, generation INTEGER NOT NULL,
        object_sha TEXT NOT NULL CHECK(length(object_sha)=64 AND object_sha NOT GLOB '*[^0-9a-f]*'),
        byte_size INTEGER NOT NULL CHECK(byte_size>=0), detected_mime TEXT NOT NULL,
        prepared_relpath TEXT NOT NULL, target_relpath TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('prepared','committed','abandoned')),
        created_at INTEGER NOT NULL, UNIQUE(job_id,generation,object_sha),
        FOREIGN KEY(job_id,generation) REFERENCES publication_plans(job_id,generation)
    ) STRICT""",
    """CREATE TABLE revision_seals (
        revision_id TEXT PRIMARY KEY NOT NULL REFERENCES edition_revisions(id),
        source_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id), source_generation INTEGER NOT NULL,
        sealed_at INTEGER NOT NULL, retained INTEGER NOT NULL DEFAULT 1 CHECK(retained IN (0,1)),
        FOREIGN KEY(source_job_id,source_generation) REFERENCES publication_plans(job_id,generation)
    ) STRICT""",
    """CREATE TABLE revision_asset_sources (
        edition_revision_id TEXT NOT NULL, asset_position INTEGER NOT NULL,
        source_job_id TEXT NOT NULL, source_generation INTEGER NOT NULL,
        PRIMARY KEY(edition_revision_id,asset_position,source_job_id,source_generation),
        FOREIGN KEY(edition_revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position),
        FOREIGN KEY(source_job_id,source_generation) REFERENCES publication_plans(job_id,generation)
    ) STRICT""",
    """CREATE TABLE object_sources (
        object_sha TEXT NOT NULL REFERENCES objects(sha256), job_id TEXT NOT NULL REFERENCES jobs(id),
        upload_session_id TEXT NOT NULL REFERENCES upload_sessions(id),
        validation_receipt TEXT NOT NULL, created_at INTEGER NOT NULL,
        PRIMARY KEY(object_sha,job_id)
    ) STRICT""",
    """CREATE TABLE object_pins (
        kind TEXT NOT NULL CHECK(kind IN ('ticket','backup','restore')),
        reference_id TEXT NOT NULL, object_sha TEXT NOT NULL REFERENCES objects(sha256),
        created_at INTEGER NOT NULL, PRIMARY KEY(kind,reference_id,object_sha)
    ) STRICT""",
    "CREATE TABLE storage_sequence(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL) STRICT",
    "INSERT INTO storage_sequence VALUES(1,0)",
    """CREATE TRIGGER object_identity_update BEFORE UPDATE OF sha256,byte_size,detected_mime,local_relpath,created_at ON objects
        BEGIN SELECT RAISE(ABORT,'object identity is immutable'); END""",
    """CREATE TRIGGER plan_identity_update BEFORE UPDATE OF job_id,generation,payload_json,payload_digest,validation_receipt,created_at ON publication_plans
        BEGIN SELECT RAISE(ABORT,'approved plan identity is immutable'); END""",
    """CREATE TRIGGER intent_identity_update BEFORE UPDATE OF id,job_id,generation,object_sha,byte_size,detected_mime,prepared_relpath,target_relpath,created_at ON storage_intents
        BEGIN SELECT RAISE(ABORT,'install intent identity is immutable'); END""",
    """CREATE TRIGGER sealed_revision_update BEFORE UPDATE OF id,edition_id,revision_no,created_at ON edition_revisions
        WHEN EXISTS(SELECT 1 FROM revision_seals WHERE revision_id=OLD.id)
        BEGIN SELECT RAISE(ABORT,'sealed revision is immutable'); END""",
    """CREATE TRIGGER sealed_revision_delete BEFORE DELETE ON edition_revisions
        WHEN EXISTS(SELECT 1 FROM revision_seals WHERE revision_id=OLD.id)
        BEGIN SELECT RAISE(ABORT,'sealed revision is immutable'); END""",
    """CREATE TRIGGER seal_delete BEFORE DELETE ON revision_seals
        BEGIN SELECT RAISE(ABORT,'revision seal cannot be removed'); END""",
    """CREATE TRIGGER seal_identity_update BEFORE UPDATE OF revision_id,source_job_id,source_generation,sealed_at ON revision_seals
        BEGIN SELECT RAISE(ABORT,'revision seal identity is immutable'); END""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)
    # Frozen table/column names, never user input. Prevent moving rows into OR out of a seal.
    for table in ('revision_assets', 'revision_options', 'option_assets', 'revision_asset_sources'):
        for event, aliases in (('INSERT', ('NEW',)), ('UPDATE', ('OLD','NEW')), ('DELETE', ('OLD',))):
            checks = ' OR '.join(f'EXISTS(SELECT 1 FROM revision_seals WHERE revision_id={alias}.edition_revision_id)' for alias in aliases)
            op.execute(f"CREATE TRIGGER sealed_{table}_{event.lower()} BEFORE {event} ON {table} WHEN {checks} "
                       "BEGIN SELECT RAISE(ABORT,'sealed revision content is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
