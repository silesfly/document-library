"""G0-D2b2: observed execution stop and private installation retirement."""
from alembic import op

revision = '0007_orphans'
down_revision = '0006_lifecycle'
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE installation_stops (
        job_id TEXT NOT NULL, generation INTEGER NOT NULL,
        owner_digest TEXT NOT NULL, disposition TEXT NOT NULL
            CHECK(disposition IN ('completed','cancelled','not_started')),
        confirmation TEXT NOT NULL, observed_at INTEGER NOT NULL,
        PRIMARY KEY(job_id,generation),
        FOREIGN KEY(job_id,generation) REFERENCES publication_plans(job_id,generation)
    ) STRICT""",
    """CREATE TABLE installation_retirements (
        intent_id TEXT PRIMARY KEY NOT NULL REFERENCES storage_intents(id),
        state TEXT NOT NULL CHECK(state IN ('retiring','quarantined','purging','purged','released','missing')),
        prepared_action TEXT NOT NULL CHECK(prepared_action IN ('pending','moved','absent')),
        target_action TEXT NOT NULL CHECK(target_action IN ('pending','moved','absent','protected')),
        started_at INTEGER NOT NULL, quarantined_at INTEGER, purged_at INTEGER
    ) STRICT""",
    """CREATE TABLE installation_retirement_events (
        id TEXT PRIMARY KEY NOT NULL, intent_id TEXT NOT NULL REFERENCES storage_intents(id),
        action TEXT NOT NULL, detail TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)
    for event in ('UPDATE','DELETE'):
        op.execute(f"CREATE TRIGGER installation_stop_{event.lower()} BEFORE {event} ON installation_stops "
                   "BEGIN SELECT RAISE(ABORT,'observed stop receipt is immutable'); END")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
