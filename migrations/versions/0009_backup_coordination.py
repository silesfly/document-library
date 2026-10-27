"""Observed backup execution termination and protected terminal transitions."""
from alembic import op

revision = '0009_backup_coordination'
down_revision = '0008_backups'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE backup_execution_stops (
        set_id TEXT PRIMARY KEY NOT NULL REFERENCES backup_sets(id),
        owner_digest TEXT NOT NULL,
        disposition TEXT NOT NULL CHECK(disposition IN ('completed','cancelled','not_started')),
        confirmation TEXT NOT NULL CHECK(length(confirmation) BETWEEN 1 AND 500),
        observed_at INTEGER NOT NULL
    ) STRICT""")
    op.execute("""CREATE TRIGGER backup_stop_binding BEFORE INSERT ON backup_execution_stops
        WHEN NOT EXISTS(SELECT 1 FROM backup_sets WHERE id=NEW.set_id AND owner_digest=NEW.owner_digest)
        BEGIN SELECT RAISE(ABORT,'backup stop owner differs'); END""")
    for event in ('UPDATE','DELETE'):
        op.execute(f"CREATE TRIGGER backup_stop_{event.lower()} BEFORE {event} ON backup_execution_stops "
                   "BEGIN SELECT RAISE(ABORT,'backup stop is immutable'); END")
        op.execute(f"""CREATE TRIGGER backup_pin_{event.lower()} BEFORE {event} ON object_pins
            WHEN OLD.kind='backup' AND NOT EXISTS(
                SELECT 1 FROM backup_sets b JOIN backup_execution_stops s ON s.set_id=b.id
                WHERE b.id=OLD.reference_id AND b.state IN ('SEALED','FAILED'))
            BEGIN SELECT RAISE(ABORT,'backup pin requires terminal coordinator record'); END""")
    op.execute("""CREATE TRIGGER backup_terminal_state BEFORE UPDATE OF state,sealed_at,failure ON backup_sets
        WHEN OLD.state IN ('SEALED','FAILED')
        BEGIN SELECT RAISE(ABORT,'backup terminal state is immutable'); END""")
    op.execute("""CREATE TRIGGER backup_terminal_stop BEFORE UPDATE OF state ON backup_sets
        WHEN NEW.state IN ('SEALED','FAILED') AND NOT EXISTS(
            SELECT 1 FROM backup_execution_stops WHERE set_id=NEW.id)
        BEGIN SELECT RAISE(ABORT,'backup terminal state requires observed stop'); END""")
    op.execute("""CREATE TRIGGER backup_sealed_receipts BEFORE UPDATE OF state ON backup_sets
        WHEN NEW.state='SEALED' AND (OLD.state<>'BUILDING' OR EXISTS(
            SELECT 1 FROM backup_members m JOIN objects o ON o.sha256=m.object_sha
            WHERE m.set_id=NEW.id AND NOT EXISTS(
                SELECT 1 FROM object_backup_receipts r WHERE r.set_id=NEW.id
                    AND r.object_sha=m.object_sha AND r.byte_size=o.byte_size
                    AND r.manifest_sha=NEW.manifest_sha AND r.valid=1)))
        BEGIN SELECT RAISE(ABORT,'sealed backup requires verified member receipts'); END""")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
