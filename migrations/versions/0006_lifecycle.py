"""G0-D2b: persistent local lifecycle and retention acquisition guards."""
from alembic import op

revision = '0006_lifecycle'
down_revision = '0005_publication'
branch_labels = None
depends_on = None

STATEMENTS = (
    """CREATE TABLE object_lifecycle (
        object_sha TEXT PRIMARY KEY NOT NULL REFERENCES objects(sha256),
        unreferenced_since INTEGER, quarantined_at INTEGER,
        operation TEXT CHECK(operation IN ('quarantine','restore','purge'))
    ) STRICT""",
    """CREATE TABLE object_backup_receipts (
        id TEXT PRIMARY KEY NOT NULL, object_sha TEXT NOT NULL REFERENCES objects(sha256),
        byte_size INTEGER NOT NULL CHECK(byte_size>=0), set_id TEXT NOT NULL,
        backup_relpath TEXT NOT NULL, manifest_sha TEXT NOT NULL
            CHECK(length(manifest_sha)=64 AND manifest_sha NOT GLOB '*[^0-9a-f]*'),
        verified_at INTEGER NOT NULL, valid INTEGER NOT NULL CHECK(valid IN (0,1))
    ) STRICT""",
    """CREATE TRIGGER backup_receipt_identity BEFORE UPDATE OF id,object_sha,byte_size,set_id,backup_relpath,manifest_sha,verified_at
        ON object_backup_receipts BEGIN SELECT RAISE(ABORT,'backup receipt identity is immutable'); END""",
    """CREATE TABLE lifecycle_events (
        id TEXT PRIMARY KEY NOT NULL, object_sha TEXT NOT NULL REFERENCES objects(sha256),
        action TEXT NOT NULL, detail TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT""",
    """CREATE TRIGGER pin_live_insert BEFORE INSERT ON object_pins
        WHEN NOT EXISTS(SELECT 1 FROM objects WHERE sha256=NEW.object_sha AND state='LIVE')
        BEGIN SELECT RAISE(ABORT,'new pin requires LIVE object'); END""",
    """CREATE TRIGGER active_live_update BEFORE UPDATE OF is_active ON edition_revisions
        WHEN NEW.is_active=1 AND EXISTS(SELECT 1 FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
            WHERE a.edition_revision_id=NEW.id AND o.state<>'LIVE')
        BEGIN SELECT RAISE(ABORT,'activation requires LIVE objects'); END""",
)


def upgrade():
    for statement in STATEMENTS:
        op.execute(statement)
    for event in ('INSERT','UPDATE OF retained'):
        op.execute(f"""CREATE TRIGGER retained_live_{event.split()[0].lower()} BEFORE {event} ON revision_seals
            WHEN NEW.retained=1 AND EXISTS(SELECT 1 FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
                WHERE a.edition_revision_id=NEW.revision_id AND o.state<>'LIVE')
            BEGIN SELECT RAISE(ABORT,'retention requires LIVE objects'); END""")
    # Every newly acquired/released reference invalidates a previously observed
    # unreferenced grace period, including jobs that finish between GC scans.
    for table, events, expressions in (
        ('object_pins', ('INSERT','DELETE','UPDATE'), ('SELECT NEW.object_sha','SELECT OLD.object_sha')),
        ('jobs', ('INSERT','DELETE','UPDATE OF state,object_sha,input_revision_id'),
         ('SELECT NEW.object_sha UNION SELECT object_sha FROM revision_assets WHERE edition_revision_id=NEW.input_revision_id',
          'SELECT OLD.object_sha UNION SELECT object_sha FROM revision_assets WHERE edition_revision_id=OLD.input_revision_id')),
        ('revision_assets', ('INSERT','DELETE','UPDATE'), ('SELECT NEW.object_sha','SELECT OLD.object_sha')),
        ('revision_seals', ('INSERT','UPDATE OF retained'),
         ('SELECT object_sha FROM revision_assets WHERE edition_revision_id=NEW.revision_id',None)),
        ('edition_revisions', ('UPDATE OF is_active',),
         ('SELECT object_sha FROM revision_assets WHERE edition_revision_id=NEW.id',None)),
    ):
        for event in events:
            verb = event.split()[0]
            values = [v for v in expressions if v and (verb != 'INSERT' or 'OLD.' not in v) and (verb != 'DELETE' or 'NEW.' not in v)]
            query = ' UNION '.join(values)
            op.execute(f"CREATE TRIGGER grace_{table}_{verb.lower()} AFTER {event} ON {table} "
                       f"BEGIN UPDATE object_lifecycle SET unreferenced_since=NULL WHERE object_sha IN ({query}); END")
    op.execute("""CREATE TRIGGER pin_live_update BEFORE UPDATE OF object_sha ON object_pins
        WHEN NOT EXISTS(SELECT 1 FROM objects WHERE sha256=NEW.object_sha AND state='LIVE')
        BEGIN SELECT RAISE(ABORT,'new pin requires LIVE object'); END""")
    for event in ('INSERT','UPDATE OF object_sha'):
        op.execute(f"""CREATE TRIGGER asset_live_{event.split()[0].lower()} BEFORE {event} ON revision_assets
            WHEN NOT EXISTS(SELECT 1 FROM objects WHERE sha256=NEW.object_sha AND state='LIVE')
            BEGIN SELECT RAISE(ABORT,'new asset requires LIVE object'); END""")
    for event in ('INSERT','UPDATE OF state,object_sha,input_revision_id'):
        op.execute(f"""CREATE TRIGGER job_live_{event.split()[0].lower()} BEFORE {event} ON jobs
            WHEN NEW.state IN ('queued','running','retry_wait') AND (
                (NEW.object_sha IS NOT NULL AND NOT EXISTS(SELECT 1 FROM objects WHERE sha256=NEW.object_sha AND state='LIVE'))
                OR EXISTS(SELECT 1 FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha
                    WHERE a.edition_revision_id=NEW.input_revision_id AND o.state<>'LIVE'))
            BEGIN SELECT RAISE(ABORT,'job input requires LIVE objects'); END""")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
