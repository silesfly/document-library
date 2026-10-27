"""Durable discovery and independent reconciliation progress; no SDK secrets."""
from alembic import op

revision = '0010_sources'
down_revision = '0009_backup_coordination'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE ingestion_sources (
        id TEXT PRIMARY KEY NOT NULL,
        account_scope TEXT NOT NULL,
        chat_id INTEGER NOT NULL CHECK(chat_id<>0),
        config_version INTEGER NOT NULL DEFAULT 1 CHECK(config_version>0),
        enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
        extensions_json TEXT NOT NULL CHECK(json_valid(extensions_json)),
        max_bytes INTEGER NOT NULL CHECK(max_bytes BETWEEN 1 AND 2147483648),
        filename_filter TEXT NOT NULL,
        high_water INTEGER NOT NULL DEFAULT 0 CHECK(high_water>=0),
        event_cursor INTEGER NOT NULL DEFAULT 0 CHECK(event_cursor>=0),
        event_epoch INTEGER NOT NULL DEFAULT 0 CHECK(event_epoch>=0),
        recovery_state TEXT NOT NULL DEFAULT 'RECONCILING'
            CHECK(recovery_state IN ('RECONCILING','CURRENT')),
        UNIQUE(account_scope,chat_id)
    ) STRICT""")
    op.execute("""CREATE TABLE source_messages (
        source_id TEXT NOT NULL REFERENCES ingestion_sources(id),
        message_id INTEGER NOT NULL CHECK(message_id>0),
        PRIMARY KEY(source_id,message_id)
    ) STRICT""")
    op.execute("""CREATE TABLE source_scans (
        id TEXT PRIMARY KEY NOT NULL,
        source_id TEXT NOT NULL REFERENCES ingestion_sources(id),
        config_version INTEGER NOT NULL,
        request_key TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        mode TEXT NOT NULL CHECK(mode IN ('incremental','events','reconcile')),
        upper_bound INTEGER NOT NULL CHECK(upper_bound>=0),
        cursor INTEGER NOT NULL CHECK(cursor>=0 AND cursor<=upper_bound),
        event_epoch INTEGER NOT NULL,
        job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
        state TEXT NOT NULL CHECK(state IN ('active','complete','cancelled')),
        UNIQUE(source_id,request_key)
    ) STRICT""")
    op.execute("CREATE UNIQUE INDEX source_one_scan ON source_scans(source_id) WHERE state='active'")
    op.execute("""CREATE TABLE source_pages (
        scan_id TEXT NOT NULL REFERENCES source_scans(id),
        cursor_before INTEGER NOT NULL,
        cursor_after INTEGER NOT NULL,
        request_digest TEXT NOT NULL,
        receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),
        PRIMARY KEY(scan_id,cursor_before)
    ) STRICT""")
    op.execute("""CREATE TABLE source_observations (
        id TEXT PRIMARY KEY NOT NULL,
        scan_id TEXT NOT NULL REFERENCES source_scans(id),
        page_cursor INTEGER NOT NULL CHECK(page_cursor>=0),
        source_id TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        media_index INTEGER NOT NULL CHECK(media_index>=-1),
        state TEXT NOT NULL CHECK(state IN ('attached','no_media','deleted','inaccessible')),
        metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json)),
        admission TEXT NOT NULL CHECK(admission IN ('DISCOVERED','FILTERED','UNAVAILABLE')),
        download_job_id TEXT UNIQUE REFERENCES jobs(id),
        UNIQUE(scan_id,page_cursor,message_id,media_index),
        FOREIGN KEY(source_id,message_id) REFERENCES source_messages(source_id,message_id),
        CHECK((state='attached' AND media_index>=0) OR (state<>'attached' AND media_index=-1)),
        CHECK((admission='DISCOVERED' AND download_job_id IS NOT NULL) OR
              (admission<>'DISCOVERED' AND download_job_id IS NULL))
    ) STRICT""")
    op.execute("""CREATE TABLE source_content_versions (
        id TEXT PRIMARY KEY NOT NULL,
        source_id TEXT NOT NULL,
        message_id INTEGER NOT NULL,
        media_index INTEGER NOT NULL,
        sha256 TEXT NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
        byte_size INTEGER NOT NULL CHECK(byte_size>0),
        FOREIGN KEY(source_id,message_id) REFERENCES source_messages(source_id,message_id),
        UNIQUE(source_id,message_id,media_index,sha256)
    ) STRICT""")
    op.execute("""CREATE TABLE source_content_receipts (
        observation_id TEXT PRIMARY KEY NOT NULL REFERENCES source_observations(id),
        version_id TEXT NOT NULL REFERENCES source_content_versions(id)
    ) STRICT""")
    op.execute("""CREATE TRIGGER source_observation_binding BEFORE INSERT ON source_observations
        WHEN NOT EXISTS(SELECT 1 FROM source_scans WHERE id=NEW.scan_id AND source_id=NEW.source_id)
        BEGIN SELECT RAISE(ABORT,'observation source mismatch'); END""")
    op.execute("""CREATE TRIGGER source_content_binding BEFORE INSERT ON source_content_receipts
        WHEN NOT EXISTS(SELECT 1 FROM source_observations o JOIN source_content_versions v
            ON v.source_id=o.source_id AND v.message_id=o.message_id AND v.media_index=o.media_index
            WHERE o.id=NEW.observation_id AND v.id=NEW.version_id AND o.admission='DISCOVERED')
        BEGIN SELECT RAISE(ABORT,'content observation mismatch'); END""")
    for table in ('source_pages','source_observations','source_content_versions','source_content_receipts'):
        for event in ('UPDATE','DELETE'):
            op.execute(f"CREATE TRIGGER {table}_{event.lower()} BEFORE {event} ON {table} "
                       "BEGIN SELECT RAISE(ABORT,'source evidence is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
