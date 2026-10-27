"""Immutable derivative repair inputs/receipts; original revisions stay intact."""
from alembic import op

revision = '0014_preview_cache'
down_revision = '0013_library_read'
branch_labels = None
depends_on = None


def upgrade():
    # G2B measured query paths lack these leading columns. Keep both indexes
    # in this one schema head; they add metadata writer cost, not new authority.
    op.execute('CREATE INDEX ix_revision_options_revision ON revision_options(edition_revision_id,id)')
    op.execute('CREATE INDEX ix_ratings_work ON ratings(work_id,rating_half_steps)')
    op.execute("""CREATE TABLE preview_repair_inputs (
        job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id),
        revision_id TEXT NOT NULL, asset_position INTEGER NOT NULL CHECK(asset_position>=0),
        object_sha TEXT NOT NULL REFERENCES objects(sha256), byte_size INTEGER NOT NULL CHECK(byte_size>=0),
        file_format TEXT NOT NULL, asset_role TEXT NOT NULL,
        sources_json TEXT NOT NULL CHECK(json_valid(sources_json) AND json_type(sources_json)='array'),
        renderer INTEGER NOT NULL CHECK(renderer=2),
        limits_json TEXT NOT NULL CHECK(json_valid(limits_json) AND json_type(limits_json)='object'),
        profile_sha256 TEXT NOT NULL CHECK(length(profile_sha256)=64 AND profile_sha256 NOT GLOB '*[^0-9a-f]*'),
        damaged_key TEXT NOT NULL, expected_head TEXT,
        actor_id TEXT NOT NULL REFERENCES users(id), request_digest TEXT NOT NULL,
        FOREIGN KEY(revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    ) STRICT""")
    op.execute("""CREATE TABLE preview_cache_receipts (
        id TEXT PRIMARY KEY NOT NULL, job_id TEXT NOT NULL REFERENCES preview_repair_inputs(job_id),
        generation INTEGER NOT NULL CHECK(generation>0),
        revision_id TEXT NOT NULL, asset_position INTEGER NOT NULL CHECK(asset_position>=0),
        object_sha TEXT NOT NULL REFERENCES objects(sha256), byte_size INTEGER NOT NULL CHECK(byte_size>=0),
        renderer INTEGER NOT NULL CHECK(renderer=2),
        profile_sha256 TEXT NOT NULL CHECK(length(profile_sha256)=64 AND profile_sha256 NOT GLOB '*[^0-9a-f]*'),
        kind TEXT NOT NULL CHECK(kind IN ('text','images','unavailable')),
        reason TEXT, manifest_json TEXT NOT NULL CHECK(json_valid(manifest_json) AND length(manifest_json)<=16384),
        created_at INTEGER NOT NULL CHECK(created_at>=0), UNIQUE(job_id,generation),
        FOREIGN KEY(revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position),
        CHECK(json_type(manifest_json,'$.schema') IS 'integer' AND json_extract(manifest_json,'$.schema')=2
            AND json_type(manifest_json,'$.renderer') IS 'integer' AND json_extract(manifest_json,'$.renderer')=renderer
            AND json_type(manifest_json,'$.limits') IS 'object'
            AND json_type(manifest_json,'$.profile_sha256') IS 'text'
            AND json_extract(manifest_json,'$.profile_sha256')=profile_sha256),
        CHECK((kind='unavailable' AND reason IS NOT NULL AND json_type(manifest_json,'$.bundle_relpath') IS NULL)
              OR (kind<>'unavailable' AND reason IS NULL AND json_type(manifest_json,'$.bundle_relpath')='text'))
    ) STRICT""")
    op.execute("""CREATE TABLE preview_cache_heads (
        revision_id TEXT NOT NULL, asset_position INTEGER NOT NULL CHECK(asset_position>=0),
        renderer INTEGER NOT NULL CHECK(renderer=2),
        profile_sha256 TEXT NOT NULL CHECK(length(profile_sha256)=64 AND profile_sha256 NOT GLOB '*[^0-9a-f]*'),
        receipt_id TEXT NOT NULL UNIQUE REFERENCES preview_cache_receipts(id),
        PRIMARY KEY(revision_id,asset_position,renderer,profile_sha256),
        FOREIGN KEY(revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    ) STRICT""")
    op.execute("""CREATE TRIGGER preview_input_job BEFORE INSERT ON preview_repair_inputs
        WHEN NOT EXISTS(SELECT 1 FROM jobs j JOIN revision_assets a
            ON a.edition_revision_id=NEW.revision_id AND a.asset_position=NEW.asset_position
            JOIN objects o ON o.sha256=a.object_sha
            WHERE j.id=NEW.job_id AND j.kind='preview.repair' AND j.input_revision_id=NEW.revision_id
                AND j.object_sha=NEW.object_sha AND a.object_sha=NEW.object_sha AND o.byte_size=NEW.byte_size
                AND a.file_format=NEW.file_format AND a.asset_role=NEW.asset_role AND o.state='LIVE'
                AND json_extract(j.payload_json,'$.profile_sha256')=NEW.profile_sha256)
        BEGIN SELECT RAISE(ABORT,'preview repair input differs'); END""")
    op.execute("""CREATE TRIGGER preview_receipt_input BEFORE INSERT ON preview_cache_receipts
        WHEN NOT EXISTS(SELECT 1 FROM preview_repair_inputs i WHERE i.job_id=NEW.job_id
            AND i.revision_id=NEW.revision_id AND i.asset_position=NEW.asset_position
            AND i.object_sha=NEW.object_sha AND i.byte_size=NEW.byte_size AND i.renderer=NEW.renderer
            AND i.profile_sha256=NEW.profile_sha256
            AND json_extract(NEW.manifest_json,'$.limits')=i.limits_json)
        BEGIN SELECT RAISE(ABORT,'preview receipt input differs'); END""")
    for event in ('INSERT','UPDATE'):
        op.execute(f"""CREATE TRIGGER preview_head_{event.lower()} BEFORE {event} ON preview_cache_heads
            WHEN NOT EXISTS(SELECT 1 FROM preview_cache_receipts r WHERE r.id=NEW.receipt_id
                AND r.revision_id=NEW.revision_id AND r.asset_position=NEW.asset_position AND r.renderer=NEW.renderer
                AND r.profile_sha256=NEW.profile_sha256)
            BEGIN SELECT RAISE(ABORT,'preview head input differs'); END""")
    for table in ('preview_repair_inputs','preview_cache_receipts'):
        for event in ('UPDATE','DELETE'):
            op.execute(f"CREATE TRIGGER {table}_{event.lower()} BEFORE {event} ON {table} "
                       "BEGIN SELECT RAISE(ABORT,'preview evidence is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
