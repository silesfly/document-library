"""G2B versioned authoritative read projection and synchronous source refresh."""
from alembic import op

from document_library.library_projection import (AUTHORIZED_OPTIONS_V1,
    rebuild_projection_in, source_triggers_v1)

revision = '0015_library_read_projection'
down_revision = '0014_preview_cache'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE INDEX ix_library_allowed_plan_v1 ON publication_plans(job_id,generation)
        WHERE rights_status='allowed' AND library_plan_valid_v1(payload_json,payload_digest)=1""")
    op.execute('CREATE INDEX ix_library_asset_source_reverse ON revision_asset_sources(source_job_id,source_generation,edition_revision_id,asset_position)')
    op.execute('CREATE VIEW library_authorized_options_v1 AS '+AUTHORIZED_OPTIONS_V1)
    op.execute("""CREATE TABLE library_read_works (
        work_id TEXT PRIMARY KEY NOT NULL REFERENCES works(id) ON DELETE CASCADE,
        input_epoch INTEGER NOT NULL DEFAULT 0 CHECK(input_epoch>=0),
        computed_epoch INTEGER NOT NULL DEFAULT 0 CHECK(computed_epoch>=0),
        visible INTEGER NOT NULL DEFAULT 0 CHECK(visible IN (0,1)),
        basis_option_id TEXT, basis_revision_id TEXT, latest_accessible_seal INTEGER,
        created_key INTEGER, updated_key INTEGER, episode_count INTEGER, gaps_key INTEGER,
        size_bytes INTEGER, representation_mask INTEGER NOT NULL DEFAULT 0 CHECK(representation_mask BETWEEN 0 AND 15),
        has_scan INTEGER NOT NULL DEFAULT 0 CHECK(has_scan IN (0,1)),
        formats_ready INTEGER NOT NULL DEFAULT 0 CHECK(formats_ready IN (0,1)),
        CHECK((visible=1)=(basis_option_id IS NOT NULL)),
        CHECK((basis_option_id IS NULL)=(basis_revision_id IS NULL)),
        FOREIGN KEY(basis_option_id,basis_revision_id) REFERENCES revision_options(id,edition_revision_id)
    ) STRICT""")
    op.execute("""CREATE TABLE library_read_formats (
        work_id TEXT NOT NULL REFERENCES works(id) ON DELETE CASCADE, format TEXT NOT NULL,
        PRIMARY KEY(work_id,format)
    ) STRICT""")
    op.execute('CREATE INDEX ix_library_read_format ON library_read_formats(format,work_id)')
    op.execute("""CREATE TABLE library_rating_stats (
        work_id TEXT PRIMARY KEY NOT NULL REFERENCES works(id) ON DELETE CASCADE,
        votes INTEGER NOT NULL CHECK(votes>=0), half_sum INTEGER NOT NULL CHECK(half_sum>=0),
        CHECK((votes=0 AND half_sum=0) OR (votes>0 AND half_sum BETWEEN votes AND 10*votes))
    ) STRICT""")
    # One set-based rebuild precedes source triggers, inside Alembic's transaction.
    rebuild_projection_in(op.get_bind().connection.driver_connection)
    for statement in source_triggers_v1():
        op.execute(statement)


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
