"""Explicit reviewed ingestion bindings and separate source-object provenance."""
from alembic import op

revision = '0012_ingestion'
down_revision = '0011_upload_transport'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE ingestion_inputs (
        job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id),
        edition_id TEXT NOT NULL REFERENCES work_editions(id),
        expected_revision_id TEXT REFERENCES edition_revisions(id),
        upload_id TEXT UNIQUE REFERENCES upload_sessions(id),
        observation_id TEXT UNIQUE REFERENCES source_observations(id),
        source_config_version INTEGER, source_event_epoch INTEGER,
        filename TEXT NOT NULL, byte_size INTEGER NOT NULL CHECK(byte_size>0),
        tus_id TEXT,
        rights_status TEXT NOT NULL CHECK(rights_status IN ('allowed','review','blocked')),
        rights_receipt TEXT NOT NULL, decision_actor TEXT NOT NULL REFERENCES users(id),
        request_digest TEXT NOT NULL,
        CHECK((upload_id IS NOT NULL AND observation_id IS NULL AND tus_id IS NOT NULL
                AND source_config_version IS NULL AND source_event_epoch IS NULL) OR
              (upload_id IS NULL AND observation_id IS NOT NULL AND tus_id IS NULL
                AND source_config_version IS NOT NULL AND source_config_version>0
                AND source_event_epoch IS NOT NULL AND source_event_epoch>=0))
    ) STRICT""")
    op.execute("""CREATE TABLE ingestion_runs (
        job_id TEXT NOT NULL REFERENCES ingestion_inputs(job_id),
        generation INTEGER NOT NULL CHECK(generation>0),
        sha256 TEXT NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
        byte_size INTEGER NOT NULL CHECK(byte_size>0),
        preview_json TEXT NOT NULL CHECK(json_valid(preview_json)),
        PRIMARY KEY(job_id,generation)
    ) STRICT""")
    op.execute("""CREATE TABLE source_object_sources (
        object_sha TEXT NOT NULL REFERENCES objects(sha256),
        job_id TEXT NOT NULL REFERENCES ingestion_inputs(job_id),
        observation_id TEXT NOT NULL REFERENCES source_content_receipts(observation_id),
        version_id TEXT NOT NULL REFERENCES source_content_versions(id),
        validation_receipt TEXT NOT NULL,
        PRIMARY KEY(object_sha,job_id)
    ) STRICT""")
    op.execute("""CREATE TRIGGER ingestion_input_identity BEFORE UPDATE OF
        job_id,edition_id,expected_revision_id,upload_id,observation_id,source_config_version,
        source_event_epoch,filename,byte_size,tus_id,request_digest ON ingestion_inputs
        BEGIN SELECT RAISE(ABORT,'ingestion binding is immutable'); END""")
    op.execute("""CREATE TRIGGER source_object_binding BEFORE INSERT ON source_object_sources
        WHEN NOT EXISTS(SELECT 1 FROM source_content_receipts r
            JOIN source_content_versions v ON v.id=r.version_id
            JOIN ingestion_inputs i ON i.observation_id=r.observation_id
            WHERE i.job_id=NEW.job_id AND r.observation_id=NEW.observation_id
                AND v.id=NEW.version_id AND v.sha256=NEW.object_sha)
        BEGIN SELECT RAISE(ABORT,'source object provenance mismatch'); END""")
    for table in ('ingestion_runs','source_object_sources'):
        for event in ('UPDATE','DELETE'):
            op.execute(f"CREATE TRIGGER {table}_{event.lower()} BEFORE {event} ON {table} "
                       "BEGIN SELECT RAISE(ABORT,'ingestion evidence is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
