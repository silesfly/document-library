"""G0-A: work identity, editions, revision options and work-level ratings."""

from alembic import op

revision = "0001_catalog"
down_revision = None
branch_labels = None
depends_on = None

# Frozen migration statements: do not import mutable application models here.
STATEMENTS = (
    """CREATE TABLE users (
        id TEXT PRIMARY KEY NOT NULL, username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member' CHECK(role IN ('admin','member')),
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','suspended')),
        authz_version INTEGER NOT NULL DEFAULT 1 CHECK(authz_version>=1),
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE works (
        id TEXT PRIMARY KEY NOT NULL,
        item_type TEXT NOT NULL CHECK(item_type IN ('novel','webtoon','comic','document','unknown')),
        title_display TEXT NOT NULL, title_match_key TEXT NOT NULL, author TEXT,
        publication_state TEXT NOT NULL DEFAULT 'unknown'
            CHECK(publication_state IN ('completed','ongoing','unknown')),
        state_evidence TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""",
    "CREATE INDEX ix_works_title_key ON works(title_match_key,id)",
    """CREATE TABLE work_editions (
        id TEXT PRIMARY KEY NOT NULL, work_id TEXT NOT NULL REFERENCES works(id),
        edition_key TEXT NOT NULL,
        representation TEXT NOT NULL CHECK(representation IN ('text','imagepack','document','single_image')),
        source_form TEXT NOT NULL DEFAULT 'unknown' CHECK(source_form IN ('born_digital','scan','unknown')),
        edition_label TEXT NOT NULL, language TEXT, source_series TEXT,
        created_at TEXT NOT NULL, UNIQUE(work_id,edition_key), UNIQUE(id,work_id)
    )""",
    "CREATE INDEX ix_editions_work_rep ON work_editions(work_id,representation)",
    """CREATE TABLE work_relations (
        work_id TEXT NOT NULL REFERENCES works(id),
        related_work_id TEXT NOT NULL REFERENCES works(id),
        type TEXT NOT NULL CHECK(type='adaptation_of'),
        PRIMARY KEY(work_id,related_work_id,type), CHECK(work_id<>related_work_id)
    )""",
    """CREATE TABLE classification_history (
        id TEXT PRIMARY KEY NOT NULL, work_id TEXT NOT NULL REFERENCES works(id),
        edition_id TEXT, actor_id TEXT NOT NULL REFERENCES users(id),
        evidence TEXT NOT NULL, before_json TEXT NOT NULL CHECK(json_valid(before_json)),
        after_json TEXT NOT NULL CHECK(json_valid(after_json)), created_at TEXT NOT NULL,
        FOREIGN KEY(edition_id,work_id) REFERENCES work_editions(id,work_id)
    )""",
    """CREATE TABLE objects (
        sha256 TEXT PRIMARY KEY NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
        byte_size INTEGER NOT NULL CHECK(byte_size>=0), detected_mime TEXT NOT NULL,
        local_relpath TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL DEFAULT 'LIVE' CHECK(state IN ('LIVE','QUARANTINED','PURGED','MISSING')),
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE edition_revisions (
        id TEXT PRIMARY KEY NOT NULL, edition_id TEXT NOT NULL REFERENCES work_editions(id),
        revision_no INTEGER NOT NULL CHECK(revision_no>=1),
        is_active INTEGER NOT NULL DEFAULT 0 CHECK(is_active IN (0,1)),
        created_at TEXT NOT NULL, UNIQUE(edition_id,revision_no)
    )""",
    """CREATE UNIQUE INDEX ix_one_active_revision_per_edition
        ON edition_revisions(edition_id) WHERE is_active=1""",
    """CREATE TABLE revision_assets (
        edition_revision_id TEXT NOT NULL REFERENCES edition_revisions(id),
        asset_position INTEGER NOT NULL CHECK(asset_position>=0),
        object_sha TEXT NOT NULL REFERENCES objects(sha256), file_format TEXT NOT NULL,
        asset_role TEXT NOT NULL DEFAULT 'content', original_filename TEXT,
        PRIMARY KEY(edition_revision_id,asset_position)
    )""",
    "CREATE INDEX ix_assets_by_object ON revision_assets(object_sha)",
    """CREATE TABLE revision_options (
        id TEXT PRIMARY KEY NOT NULL,
        edition_revision_id TEXT NOT NULL REFERENCES edition_revisions(id),
        format TEXT NOT NULL,
        delivery_kind TEXT NOT NULL CHECK(delivery_kind IN ('single_file','bundle')),
        coverage_json TEXT NOT NULL CHECK(json_valid(coverage_json) AND json_type(coverage_json)='object'),
        coverage_confidence TEXT NOT NULL CHECK(coverage_confidence IN ('verified','inferred','unknown')),
        UNIQUE(id,edition_revision_id)
    )""",
    """CREATE TABLE option_assets (
        option_id TEXT NOT NULL, edition_revision_id TEXT NOT NULL,
        asset_position INTEGER NOT NULL CHECK(asset_position>=0),
        ordinal INTEGER NOT NULL CHECK(ordinal>=0),
        PRIMARY KEY(option_id,ordinal), UNIQUE(option_id,asset_position),
        FOREIGN KEY(option_id,edition_revision_id) REFERENCES revision_options(id,edition_revision_id),
        FOREIGN KEY(edition_revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    )""",
    """CREATE TABLE ratings (
        user_id TEXT NOT NULL REFERENCES users(id), work_id TEXT NOT NULL REFERENCES works(id),
        rating_half_steps INTEGER NOT NULL CHECK(rating_half_steps BETWEEN 1 AND 10),
        updated_at TEXT NOT NULL, PRIMARY KEY(user_id,work_id)
    )""",
)


def upgrade():
    for statement in STATEMENTS:
        suffix = " STRICT" if statement.startswith("CREATE TABLE ") else ""
        op.execute(statement + suffix)


def downgrade():
    raise RuntimeError("Destructive downgrade is not enabled; restore a verified snapshot instead")
