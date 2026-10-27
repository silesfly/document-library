"""Versioned metadata search and immutable, session-bound download snapshots."""
from alembic import op

revision = '0013_library_read'
down_revision = '0012_ingestion'
branch_labels = None
depends_on = None


def upgrade():
    # Functions are supplied by the bound connection, never imported from a
    # mutable model here. The v1 function names/semantics remain supported.
    op.execute("""CREATE TABLE library_metadata (
        search_id INTEGER PRIMARY KEY,
        work_id TEXT UNIQUE NOT NULL REFERENCES works(id) ON DELETE CASCADE,
        title TEXT NOT NULL, author TEXT,
        title_key TEXT GENERATED ALWAYS AS (library_search_key_v1(title)) STORED,
        author_key TEXT GENERATED ALWAYS AS (library_search_key_v1(author)) STORED,
        description_text TEXT,
        description_key TEXT GENERATED ALWAYS AS (library_search_key_v1(description_text)) STORED,
        description_state TEXT NOT NULL DEFAULT 'pending' CHECK(description_state IN ('pending','ready','unavailable')),
        description_origin TEXT,
        summary_state TEXT NOT NULL DEFAULT 'pending' CHECK(summary_state IN ('pending','ready','unavailable')),
        cover_state TEXT NOT NULL DEFAULT 'pending' CHECK(cover_state IN ('pending','ready','unavailable')),
        cover_revision_id TEXT, cover_asset_position INTEGER,
        classification_state TEXT NOT NULL DEFAULT 'unreviewed' CHECK(classification_state IN ('unreviewed','confirmed','review_required')),
        classification_evidence TEXT, updated_at TEXT,
        search_epoch INTEGER NOT NULL DEFAULT 0 CHECK(search_epoch>=0),
        CHECK(description_state<>'ready' OR (description_text IS NOT NULL AND length(trim(description_text))>0)),
        CHECK(classification_state<>'review_required' OR (classification_evidence IS NOT NULL AND length(trim(classification_evidence))>0)),
        CHECK((cover_revision_id IS NULL)=(cover_asset_position IS NULL)),
        CHECK(cover_state<>'ready' OR cover_revision_id IS NOT NULL),
        FOREIGN KEY(cover_revision_id,cover_asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    ) STRICT""")
    for table in ('library_genres','library_tags'):
        op.execute(f"""CREATE TABLE {table} (
            work_id TEXT NOT NULL REFERENCES works(id) ON DELETE CASCADE,
            label TEXT NOT NULL,
            key TEXT GENERATED ALWAYS AS (library_search_key_v1(label)) STORED,
            CHECK(length(key) BETWEEN 1 AND 128), PRIMARY KEY(work_id,label), UNIQUE(work_id,key)
        ) STRICT""")
        op.execute(f'CREATE INDEX ix_{table}_key ON {table}(key,work_id)')
    op.execute("""CREATE TABLE library_favorites (
        user_id TEXT NOT NULL REFERENCES users(id), work_id TEXT NOT NULL REFERENCES works(id),
        created_at TEXT NOT NULL, PRIMARY KEY(user_id,work_id)
    ) STRICT""")
    op.execute("""CREATE TABLE library_work_metrics (
        work_id TEXT PRIMARY KEY NOT NULL REFERENCES works(id) ON DELETE CASCADE,
        popularity REAL, popularity_source TEXT, popularity_computed_at TEXT,
        CHECK(popularity IS NULL OR popularity BETWEEN -1.0e308 AND 1.0e308),
        CHECK((popularity IS NULL)=(popularity_source IS NULL)),
        CHECK((popularity IS NULL)=(popularity_computed_at IS NULL))
    ) STRICT""")
    op.execute("CREATE VIRTUAL TABLE library_search_fts USING fts5(work_id UNINDEXED,title_key,author_key,description_key,tag_keys,genre_keys,tokenize='trigram case_sensitive 1')")
    for event in ('INSERT','UPDATE'):
        op.execute(f"""CREATE TRIGGER library_metadata_source_{event.lower()} BEFORE {event} ON library_metadata
            WHEN NOT EXISTS(SELECT 1 FROM works w WHERE w.id=NEW.work_id AND w.title_display=NEW.title AND w.author IS NEW.author)
            BEGIN SELECT RAISE(ABORT,'library metadata source differs'); END""")
        op.execute(f"""CREATE TRIGGER library_cover_source_{event.lower()} BEFORE {event} ON library_metadata
            WHEN NEW.cover_revision_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM edition_revisions r
                JOIN work_editions e ON e.id=r.edition_id WHERE r.id=NEW.cover_revision_id AND e.work_id=NEW.work_id)
            BEGIN SELECT RAISE(ABORT,'library cover belongs to another work'); END""")
    for event in ('INSERT','UPDATE OF title_display,author'):
        suffix='insert' if event=='INSERT' else 'update'
        op.execute(f"""CREATE TRIGGER library_work_{suffix} AFTER {event} ON works BEGIN
            INSERT INTO library_metadata(work_id,title,author) VALUES(NEW.id,NEW.title_display,NEW.author)
            ON CONFLICT(work_id) DO UPDATE SET title=excluded.title,author=excluded.author;
        END""")
    # Trigram text is a derivative. Every metadata/tag/genre writer follows this
    # same path, including direct SQL and initial backfill of existing works.
    for event in ('INSERT','UPDATE'):
        op.execute(f"""CREATE TRIGGER library_metadata_fts_{event.lower()} AFTER {event} ON library_metadata BEGIN
            DELETE FROM library_search_fts WHERE rowid=NEW.search_id;
            INSERT INTO library_search_fts(rowid,work_id,title_key,author_key,description_key,tag_keys,genre_keys)
            SELECT NEW.search_id,NEW.work_id,NEW.title_key,NEW.author_key,CASE WHEN NEW.description_state='ready' THEN NEW.description_key ELSE '' END,
                coalesce((SELECT group_concat(key,' ') FROM (SELECT key FROM library_tags WHERE work_id=NEW.work_id ORDER BY key)),''),
                coalesce((SELECT group_concat(key,' ') FROM (SELECT key FROM library_genres WHERE work_id=NEW.work_id ORDER BY key)),'');
        END""")
    op.execute("""CREATE TRIGGER library_metadata_fts_delete AFTER DELETE ON library_metadata BEGIN
        DELETE FROM library_search_fts WHERE rowid=OLD.search_id; END""")
    for table in ('library_genres','library_tags'):
        for event,alias in (('INSERT','NEW'),('DELETE','OLD'),('UPDATE','NEW')):
            extra='' if event!='UPDATE' else f'UPDATE library_metadata SET search_epoch=search_epoch+1 WHERE work_id=OLD.work_id AND OLD.work_id<>NEW.work_id;'
            op.execute(f"""CREATE TRIGGER {table}_fts_{event.lower()} AFTER {event} ON {table} BEGIN
                UPDATE library_metadata SET search_epoch=search_epoch+1 WHERE work_id={alias}.work_id;
                {extra} END""")
    op.execute("""CREATE TRIGGER library_metadata_identity BEFORE UPDATE OF search_id,work_id ON library_metadata
        BEGIN SELECT RAISE(ABORT,'library search identity is immutable'); END""")
    op.execute('INSERT INTO library_metadata(work_id,title,author) SELECT id,title_display,author FROM works')
    op.execute('CREATE INDEX ix_library_title_key ON library_metadata(title_key,work_id)')
    op.execute('CREATE INDEX ix_library_author_key ON library_metadata(author_key,work_id)')

    op.execute("""CREATE TABLE download_tickets (
        id TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL REFERENCES users(id),
        session_token_hash TEXT NOT NULL CHECK(length(session_token_hash)=64 AND session_token_hash NOT GLOB '*[^0-9a-f]*'),
        credential_version INTEGER NOT NULL CHECK(credential_version>0),
        work_id TEXT NOT NULL REFERENCES works(id),
        issued_at_ms INTEGER NOT NULL CHECK(issued_at_ms>=0),
        expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms=issued_at_ms+120000),
        archive_bytes INTEGER NOT NULL CHECK(archive_bytes BETWEEN 0 AND 2147483648),
        snapshot_digest TEXT CHECK(snapshot_digest IS NULL OR (length(snapshot_digest)=64 AND snapshot_digest NOT GLOB '*[^0-9a-f]*')),
        state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','issued','streaming','completed','failed','cancelled','expired')),
        used_at_ms INTEGER, invalidated INTEGER NOT NULL DEFAULT 0 CHECK(invalidated IN (0,1)),
        invalidation_reason TEXT,
        CHECK(state='draft' OR snapshot_digest IS NOT NULL),
        CHECK(state NOT IN ('streaming','completed','failed','cancelled') OR used_at_ms IS NOT NULL),
        CHECK(session_token_hash<>'0000000000000000000000000000000000000000000000000000000000000000' OR invalidated=1),
        UNIQUE(id,work_id)
    ) STRICT""")
    op.execute("""CREATE TRIGGER download_ticket_insert BEFORE INSERT ON download_tickets
        WHEN NEW.state<>'draft' OR NEW.used_at_ms IS NOT NULL OR NEW.snapshot_digest IS NOT NULL OR NEW.invalidated<>0 OR NEW.invalidation_reason IS NOT NULL
        BEGIN SELECT RAISE(ABORT,'ticket must begin as an unused draft'); END""")
    op.execute('CREATE INDEX ix_download_tickets_expiry ON download_tickets(state,expires_at_ms)')
    op.execute("""CREATE TABLE download_ticket_options (
        ticket_id TEXT NOT NULL REFERENCES download_tickets(id), selection_order INTEGER NOT NULL CHECK(selection_order>=0),
        edition_id TEXT NOT NULL REFERENCES work_editions(id), revision_id TEXT NOT NULL,
        option_id TEXT NOT NULL, format TEXT NOT NULL, delivery_kind TEXT NOT NULL CHECK(delivery_kind IN ('single_file','bundle')),
        coverage_json TEXT NOT NULL CHECK(json_valid(coverage_json) AND json_type(coverage_json)='object'),
        PRIMARY KEY(ticket_id,selection_order), UNIQUE(ticket_id,option_id), UNIQUE(ticket_id,selection_order,revision_id),
        FOREIGN KEY(option_id,revision_id) REFERENCES revision_options(id,edition_revision_id)
    ) STRICT""")
    op.execute("""CREATE TABLE download_ticket_assets (
        ticket_id TEXT NOT NULL, selection_order INTEGER NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0),
        revision_id TEXT NOT NULL, asset_position INTEGER NOT NULL, object_sha TEXT NOT NULL REFERENCES objects(sha256),
        byte_size INTEGER NOT NULL CHECK(byte_size>=0), detected_mime TEXT NOT NULL, file_format TEXT NOT NULL, asset_role TEXT NOT NULL,
        entry_name TEXT NOT NULL CHECK(library_archive_name_v1(entry_name)=1),
        PRIMARY KEY(ticket_id,selection_order,ordinal), UNIQUE(ticket_id,entry_name),
        FOREIGN KEY(ticket_id,selection_order,revision_id) REFERENCES download_ticket_options(ticket_id,selection_order,revision_id),
        FOREIGN KEY(revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    ) STRICT""")
    op.execute("""CREATE TABLE download_attempts (
        id TEXT PRIMARY KEY NOT NULL, ticket_id TEXT NOT NULL UNIQUE REFERENCES download_tickets(id),
        owner_instance TEXT NOT NULL, started_at_ms INTEGER NOT NULL CHECK(started_at_ms>=0),
        state TEXT NOT NULL DEFAULT 'running' CHECK(state IN ('running','cancelling','uncertain','completed','failed','cancelled')),
        ended_at_ms INTEGER, bytes_sent INTEGER NOT NULL DEFAULT 0 CHECK(bytes_sent>=0), stop_evidence TEXT,
        failure_code TEXT, invalidated INTEGER NOT NULL DEFAULT 0 CHECK(invalidated IN (0,1)),
        CHECK((state IN ('completed','failed','cancelled'))=(ended_at_ms IS NOT NULL)),
        CHECK((state IN ('completed','failed','cancelled'))=(stop_evidence IS NOT NULL)),
        CHECK(ended_at_ms IS NULL OR ended_at_ms>=started_at_ms),
        UNIQUE(id,ticket_id)
    ) STRICT""")
    op.execute("""CREATE TABLE download_events (
        id TEXT PRIMARY KEY NOT NULL, logical_download_id TEXT NOT NULL,
        user_id TEXT NOT NULL REFERENCES users(id), work_id TEXT NOT NULL REFERENCES works(id),
        ticket_id TEXT, attempt_id TEXT,
        started_at_ms INTEGER NOT NULL, ended_at_ms INTEGER NOT NULL CHECK(ended_at_ms>=started_at_ms),
        status TEXT NOT NULL CHECK(status IN ('completed','failed','cancelled')),
        bytes_sent INTEGER NOT NULL CHECK(bytes_sent>=0), stop_evidence TEXT NOT NULL, failure_code TEXT,
        CHECK((ticket_id IS NULL)=(attempt_id IS NULL)), UNIQUE(user_id,logical_download_id), UNIQUE(attempt_id),
        FOREIGN KEY(attempt_id,ticket_id) REFERENCES download_attempts(id,ticket_id)
    ) STRICT""")
    op.execute('CREATE INDEX ix_download_history ON download_events(user_id,work_id,status,ended_at_ms)')
    op.execute("""CREATE TABLE download_event_assets (
        event_id TEXT NOT NULL REFERENCES download_events(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0),
        edition_id TEXT NOT NULL REFERENCES work_editions(id), revision_id TEXT NOT NULL,
        asset_position INTEGER NOT NULL, object_sha TEXT NOT NULL REFERENCES objects(sha256),
        bytes_sent INTEGER NOT NULL CHECK(bytes_sent>=0), PRIMARY KEY(event_id,ordinal),
        FOREIGN KEY(revision_id,asset_position) REFERENCES revision_assets(edition_revision_id,asset_position)
    ) STRICT""")
    op.execute("""CREATE TRIGGER download_option_binding BEFORE INSERT ON download_ticket_options
        WHEN NOT EXISTS(SELECT 1 FROM download_tickets t JOIN work_editions e ON e.work_id=t.work_id
            JOIN edition_revisions r ON r.edition_id=e.id JOIN revision_seals s ON s.revision_id=r.id
            JOIN revision_options o ON o.edition_revision_id=r.id
            WHERE t.id=NEW.ticket_id AND e.id=NEW.edition_id AND r.id=NEW.revision_id AND o.id=NEW.option_id
                AND o.format=NEW.format AND o.delivery_kind=NEW.delivery_kind AND o.coverage_json=NEW.coverage_json)
        BEGIN SELECT RAISE(ABORT,'ticket option snapshot differs'); END""")
    op.execute("""CREATE TRIGGER download_asset_binding BEFORE INSERT ON download_ticket_assets
        WHEN NOT EXISTS(SELECT 1 FROM download_ticket_options t JOIN option_assets oa ON oa.option_id=t.option_id
            JOIN revision_assets a ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
            JOIN objects o ON o.sha256=a.object_sha
            WHERE t.ticket_id=NEW.ticket_id AND t.selection_order=NEW.selection_order AND oa.ordinal=NEW.ordinal
                AND a.edition_revision_id=NEW.revision_id AND a.asset_position=NEW.asset_position
                AND a.object_sha=NEW.object_sha AND o.byte_size=NEW.byte_size AND o.detected_mime=NEW.detected_mime
                AND a.file_format=NEW.file_format AND a.asset_role=NEW.asset_role)
        BEGIN SELECT RAISE(ABORT,'ticket asset snapshot differs'); END""")
    for table in ('download_ticket_options','download_ticket_assets'):
        for event,alias in (('INSERT','NEW'),('UPDATE','OLD'),('DELETE','OLD')):
            condition="1" if event=='UPDATE' else f"(SELECT state FROM download_tickets WHERE id={alias}.ticket_id)<>'draft'"
            op.execute(f"""CREATE TRIGGER {table}_frozen_{event.lower()} BEFORE {event} ON {table}
                WHEN {condition}
                BEGIN SELECT RAISE(ABORT,'issued ticket snapshot is immutable'); END""")
    op.execute("""CREATE TRIGGER download_ticket_identity BEFORE UPDATE OF id,user_id,credential_version,work_id,issued_at_ms,expires_at_ms,archive_bytes ON download_tickets
        BEGIN SELECT RAISE(ABORT,'ticket identity is immutable'); END""")
    op.execute("""CREATE TRIGGER download_ticket_session BEFORE UPDATE OF session_token_hash ON download_tickets
        WHEN NEW.session_token_hash<>OLD.session_token_hash AND NOT (NEW.invalidated=1 AND NEW.session_token_hash='0000000000000000000000000000000000000000000000000000000000000000')
        BEGIN SELECT RAISE(ABORT,'ticket session is immutable'); END""")
    op.execute("""CREATE TRIGGER download_ticket_digest BEFORE UPDATE OF snapshot_digest ON download_tickets
        WHEN OLD.state<>'draft' BEGIN SELECT RAISE(ABORT,'ticket snapshot digest is immutable'); END""")
    for table in ('download_tickets','download_attempts'):
        op.execute(f"""CREATE TRIGGER {table}_invalidation BEFORE UPDATE OF invalidated ON {table}
            WHEN OLD.invalidated=1 AND NEW.invalidated<>1 BEGIN SELECT RAISE(ABORT,'restored execution cannot reactivate'); END""")
        op.execute(f"CREATE TRIGGER {table}_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'download audit identity cannot be deleted'); END")
    op.execute("""CREATE TRIGGER download_ticket_issue BEFORE UPDATE OF state ON download_tickets
        WHEN OLD.state='draft' AND NEW.state='issued' AND (
            NEW.invalidated=1 OR NOT EXISTS(SELECT 1 FROM download_ticket_options WHERE ticket_id=NEW.id)
            OR NOT EXISTS(SELECT 1 FROM download_ticket_assets WHERE ticket_id=NEW.id)
            OR (SELECT min(selection_order) FROM download_ticket_options WHERE ticket_id=NEW.id)<>0
            OR (SELECT max(selection_order)+1 FROM download_ticket_options WHERE ticket_id=NEW.id)<>(SELECT count(*) FROM download_ticket_options WHERE ticket_id=NEW.id)
            OR EXISTS(SELECT 1 FROM download_ticket_options t WHERE t.ticket_id=NEW.id AND
                ((SELECT count(*) FROM download_ticket_assets a WHERE a.ticket_id=t.ticket_id AND a.selection_order=t.selection_order)<>
                 (SELECT count(*) FROM option_assets oa WHERE oa.option_id=t.option_id)))
            OR EXISTS(SELECT 1 FROM download_ticket_assets a LEFT JOIN object_pins p ON p.kind='ticket' AND p.reference_id=a.ticket_id AND p.object_sha=a.object_sha
                JOIN objects o ON o.sha256=a.object_sha WHERE a.ticket_id=NEW.id AND (p.object_sha IS NULL OR o.state<>'LIVE'))
            OR NEW.archive_bytes<(SELECT coalesce(sum(byte_size),0) FROM download_ticket_assets WHERE ticket_id=NEW.id))
        BEGIN SELECT RAISE(ABORT,'ticket snapshot is incomplete or unpinned'); END""")
    op.execute("""CREATE TRIGGER download_ticket_transition BEFORE UPDATE OF state ON download_tickets
        WHEN NEW.state<>OLD.state AND NOT (
            (OLD.state='draft' AND NEW.state='issued') OR
            (OLD.state='issued' AND NEW.state IN ('streaming','expired')) OR
            (OLD.state='streaming' AND NEW.state IN ('completed','failed','cancelled')))
        BEGIN SELECT RAISE(ABORT,'invalid ticket state transition'); END""")
    op.execute("""CREATE TRIGGER download_attempt_insert BEFORE INSERT ON download_attempts
        WHEN NEW.state<>'running' OR NEW.invalidated<>0 OR NOT EXISTS(SELECT 1 FROM download_tickets WHERE id=NEW.ticket_id AND state='issued' AND invalidated=0)
        BEGIN SELECT RAISE(ABORT,'ticket is not claimable'); END""")
    op.execute("""CREATE TRIGGER download_ticket_stream BEFORE UPDATE OF state ON download_tickets
        WHEN (NEW.state='streaming' AND NOT EXISTS(SELECT 1 FROM download_attempts a
            WHERE a.ticket_id=NEW.id AND a.state='running' AND a.invalidated=0)) OR
            (NEW.state IN ('completed','failed','cancelled') AND NOT EXISTS(SELECT 1 FROM download_attempts a
                WHERE a.ticket_id=NEW.id AND a.state=NEW.state AND a.ended_at_ms IS NOT NULL AND a.stop_evidence IS NOT NULL))
        BEGIN SELECT RAISE(ABORT,'ticket lacks matching physical attempt evidence'); END""")
    op.execute("""CREATE TRIGGER download_attempt_identity BEFORE UPDATE OF id,ticket_id,owner_instance,started_at_ms ON download_attempts
        BEGIN SELECT RAISE(ABORT,'attempt ownership is immutable'); END""")
    op.execute("""CREATE TRIGGER download_attempt_terminal BEFORE UPDATE ON download_attempts
        WHEN OLD.state IN ('completed','failed','cancelled') AND (
            NEW.state IS NOT OLD.state OR NEW.ended_at_ms IS NOT OLD.ended_at_ms OR NEW.bytes_sent IS NOT OLD.bytes_sent
            OR NEW.stop_evidence IS NOT OLD.stop_evidence OR NEW.failure_code IS NOT OLD.failure_code)
        BEGIN SELECT RAISE(ABORT,'terminal attempt evidence is immutable'); END""")
    op.execute("""CREATE TRIGGER download_attempt_transition BEFORE UPDATE OF state ON download_attempts
        WHEN NEW.state<>OLD.state AND NOT (
            (OLD.state IN ('running','cancelling') AND NEW.state IN ('cancelling','uncertain','completed','failed','cancelled')) OR
            (OLD.state='uncertain' AND NEW.state IN ('failed','cancelled')))
        BEGIN SELECT RAISE(ABORT,'invalid attempt state transition'); END""")
    for table in ('download_events','download_event_assets'):
        for event in ('UPDATE','DELETE'):
            op.execute(f"CREATE TRIGGER {table}_{event.lower()} BEFORE {event} ON {table} BEGIN SELECT RAISE(ABORT,'download audit is immutable'); END")
    op.execute("""CREATE TRIGGER download_event_binding BEFORE INSERT ON download_events
        WHEN NEW.ticket_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM download_tickets t
            JOIN download_attempts a ON a.ticket_id=t.id WHERE t.id=NEW.ticket_id AND a.id=NEW.attempt_id
                AND t.user_id=NEW.user_id AND t.work_id=NEW.work_id AND a.state=NEW.status
                AND a.started_at_ms=NEW.started_at_ms AND a.ended_at_ms=NEW.ended_at_ms AND a.stop_evidence=NEW.stop_evidence
                AND a.bytes_sent=NEW.bytes_sent AND a.failure_code IS NEW.failure_code)
        BEGIN SELECT RAISE(ABORT,'download event differs from physical attempt'); END""")
    op.execute("""CREATE TRIGGER download_event_asset_binding BEFORE INSERT ON download_event_assets
        WHEN NOT EXISTS(SELECT 1 FROM download_events ev JOIN work_editions e ON e.work_id=ev.work_id
            JOIN edition_revisions r ON r.edition_id=e.id JOIN revision_assets a ON a.edition_revision_id=r.id
            WHERE ev.id=NEW.event_id AND e.id=NEW.edition_id AND r.id=NEW.revision_id
                AND a.asset_position=NEW.asset_position AND a.object_sha=NEW.object_sha
                AND (ev.ticket_id IS NULL OR EXISTS(SELECT 1 FROM download_ticket_assets ta
                    JOIN download_ticket_options opt ON opt.ticket_id=ta.ticket_id AND opt.selection_order=ta.selection_order
                    WHERE ta.ticket_id=ev.ticket_id AND opt.edition_id=NEW.edition_id
                        AND ta.revision_id=NEW.revision_id AND ta.asset_position=NEW.asset_position AND ta.object_sha=NEW.object_sha)))
        BEGIN SELECT RAISE(ABORT,'download event asset differs'); END""")


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
