"""G3 own preferences and bounded ranking lookup indexes."""
from alembic import op

revision = '0016_member_features'
down_revision = '0015_library_read_projection'
branch_labels = None
depends_on = None

STATEMENTS = (
    '''CREATE TABLE member_preferences (
        user_id TEXT PRIMARY KEY NOT NULL REFERENCES users(id),
        preferences_json TEXT NOT NULL CHECK(json_valid(preferences_json) AND json_type(preferences_json)='object'),
        updated_at TEXT NOT NULL
    ) STRICT''',
    'CREATE INDEX ix_ratings_work_user ON ratings(work_id,user_id)',
    "CREATE INDEX ix_download_completed_window ON download_events(ended_at_ms,work_id,user_id) WHERE status='completed'",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
