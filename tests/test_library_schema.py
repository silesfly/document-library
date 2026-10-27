"""Real SQLite invariants, Unicode mutations, fixed snapshots and restore safety."""
from contextlib import closing
import dataclasses
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.auth import digest
from document_library.backup import SnapshotService, BackupError
from document_library.database import database_engine, migrate, write_transaction
from document_library.library_contracts import (LibraryQuery, WorkReadDTO, archive_name_v1,
    register_sqlite_functions, search_key_v1, DISABLED_TICKET_SESSION)
from document_library.restore import RestoreService
from library_helpers import LibraryFixture

ROOT=Path(__file__).resolve().parents[1]


class LibrarySchemaAcceptance(LibraryFixture):
    def reject(self, sql, **params):
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:
                conn.execute(text(sql),params)

    def fts(self, phrase):
        with self.engine.connect() as conn:
            return conn.execute(text('SELECT work_id FROM library_search_fts WHERE library_search_fts MATCH :query'),dict(query='"'+search_key_v1(phrase).replace('"','""')+'"')).scalars().all()

    def test_direct_sql_title_author_description_and_labels_keep_unicode_keys_current(self):
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET title_display='Ｆｏｏ　Straße 가나 日本語',author='ÉMILIE' WHERE id='w'"))
            conn.execute(text("INSERT INTO library_genres VALUES('w','ＦＡＮＴＡＳＹ')"))
            conn.execute(text("INSERT INTO library_tags VALUES('w','고양이')"))
            conn.execute(text("UPDATE library_metadata SET description_text='ＡＢＣ紹介',description_state='pending' WHERE work_id='w'"))
        self.assertEqual(self.scalar('SELECT title_key FROM library_metadata'),'foo strasse 가나 日本語')
        self.assertEqual(self.fts('STRASSE'),['w'])
        self.assertEqual(self.fts('fantasy'),['w'])
        self.assertEqual(self.fts('고양이'),['w'])
        self.assertEqual(self.fts('abc紹介'),[])
        self.ready_description('w','ＡＢＣ紹介')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET author=NULL WHERE id='w'"))
            conn.execute(text("UPDATE library_genres SET label='역사소설' WHERE work_id='w'"))
            conn.execute(text("DELETE FROM library_tags WHERE work_id='w'"))
        self.assertEqual(self.scalar('SELECT author_key FROM library_metadata'),'')
        self.assertEqual(self.fts('abc紹介'),['w'])
        self.assertEqual(self.fts('fantasy'),[])
        self.assertEqual(self.fts('역사소설'),['w'])
        self.assertEqual(self.fts('고양이'),[])
        self.reject("INSERT INTO library_genres VALUES('w','역사소설 ')")
        self.reject("UPDATE library_metadata SET title='different' WHERE work_id='w'")
        self.reject("UPDATE library_metadata SET search_id=search_id+1 WHERE work_id='w'")
        self.reject("UPDATE library_metadata SET classification_state='review_required' WHERE work_id='w'")

    def test_raw_sql_connection_registered_udf_supports_insert_update_and_integrity(self):
        path=Path(self.engine.url.database)
        with closing(sqlite3.connect(path,autocommit=True)) as db:
            register_sqlite_functions(db)
            db.execute('PRAGMA foreign_keys=ON')
            db.execute("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('new','unknown','ＦＯＯ','foo','test','test')")
            db.execute("UPDATE works SET title_display='Straße',author=NULL WHERE id='new'")
            self.assertEqual(db.execute("SELECT title_key,author_key FROM library_metadata WHERE work_id='new'").fetchone(),('strasse',''))
            SnapshotService._integrity(db)
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_fts_uses_stable_rowid_lookup_and_survives_backup_vacuum(self):
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('delete','unknown','Delete me','delete','test','test')"))
            conn.execute(text("DELETE FROM works WHERE id='delete'"))
        path=Path(self.engine.url.database)
        with closing(sqlite3.connect(path,autocommit=True)) as db:
            register_sqlite_functions(db)
            before=db.execute('SELECT search_id,work_id FROM library_metadata ORDER BY search_id').fetchall()
            plan=db.execute('EXPLAIN QUERY PLAN DELETE FROM library_search_fts WHERE rowid=?',(before[0][0],)).fetchall()
            self.assertTrue(any('INDEX 0:=' in row[3] for row in plan),plan)
            self.assertEqual(db.execute('SELECT rowid,work_id FROM library_search_fts ORDER BY rowid').fetchall(),before)
            db.execute('VACUUM')
            self.assertEqual(db.execute('SELECT search_id,work_id FROM library_metadata ORDER BY search_id').fetchall(),before)
            self.assertEqual(db.execute('SELECT rowid,work_id FROM library_search_fts ORDER BY rowid').fetchall(),before)

    def test_query_and_dto_keep_full_filters_actual_unknown_and_both_semantics(self):
        query=LibraryQuery(search='ＦＯＯ',categories=('novel_scan','novel_text'),representations=('imagepack','text'),states=('ongoing',),formats=('txt','epub'),genres=('fantasy',),favorite=True,unrated=True,has_summary=False,downloaded=False,has_gaps=True,sort='episode')
        self.assertIn('ALL',LibraryQuery.__doc__)
        self.assertEqual(query.representations,('imagepack','text'))
        self.assertEqual(query.fingerprint(),dataclasses.replace(query,search='foo',limit=10,cursor='opaque').fingerprint())
        self.assertNotEqual(query.fingerprint(),dataclasses.replace(query,favorite=False).fingerprint())
        for sort in ('latest','updated','title','author','rating','votes','popularity','episode','size'):
            self.assertEqual(LibraryQuery(sort=sort).sort,sort)
        dto=WorkReadDTO('w','Book',None,'unknown')
        self.assertIsNone(dto.episode_count);self.assertIsNone(dto.popularity)
        with self.assertRaises(ValueError):WorkReadDTO('w','Book',None,'unknown',episode_count=3)
        for bad in ('/root/file','../x','x/./y','x\\y','x:stream','x/.hidden','x/COM1\x00','ｘ.txt'):
            self.assertFalse(archive_name_v1(bad),bad)
        self.assertTrue(archive_name_v1('판본/option/00001.txt'))

    def test_logout_does_not_delete_ticket_snapshot_or_physical_pin(self):
        revision,_,asset=self.first(); ticket=self.ticket(revision)
        self.auth.logout(self.token,self.csrf)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_tickets'),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_ticket_assets'),1)
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket' AND reference_id=:id AND object_sha=:sha",id=ticket.ticket_id,sha=asset['sha256']),1)
        self.assertEqual(self.scalar('SELECT count(*) FROM auth_sessions'),0)
        self.reject('DELETE FROM download_tickets WHERE id=:id',id=ticket.ticket_id)

    def test_issued_snapshot_cannot_be_moved_changed_or_claimed_twice(self):
        revision,_,_=self.first(); ticket=self.ticket(revision)
        self.reject("UPDATE download_ticket_assets SET byte_size=byte_size+1 WHERE ticket_id=:id",id=ticket.ticket_id)
        self.reject("DELETE FROM download_ticket_options WHERE ticket_id=:id",id=ticket.ticket_id)
        self.reject("UPDATE download_tickets SET session_token_hash=:session WHERE id=:id",session='f'*64,id=ticket.ticket_id)
        self.reject("UPDATE download_tickets SET snapshot_digest=:digest WHERE id=:id",digest='f'*64,id=ticket.ticket_id)
        self.reject("INSERT INTO download_tickets SELECT 'forged',user_id,session_token_hash,credential_version,work_id,issued_at_ms,expires_at_ms,archive_bytes,snapshot_digest,state,used_at_ms,invalidated,invalidation_reason FROM download_tickets WHERE id=:id",id=ticket.ticket_id)
        streaming=self.claim(ticket)
        self.reject("INSERT INTO download_attempts(id,ticket_id,owner_instance,started_at_ms) VALUES(:id,:ticket,'other',:now)",id=str(uuid.uuid4()),ticket=ticket.ticket_id,now=self.auth.now()*1000)
        self.reject("UPDATE download_attempts SET state='cancelled',ended_at_ms=:now WHERE id=:id",id=streaming.attempt_id,now=self.auth.now()*1000)
        self.assertEqual(self.scalar('SELECT state FROM download_attempts'),'running')
        self.assertEqual(self.scalar("SELECT count(*) FROM object_pins WHERE kind='ticket'"),1)

    def finish(self, ticket, *, status='completed', sent=5):
        now=self.auth.now()*1000
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE download_attempts SET state=:status,ended_at_ms=:now,bytes_sent=:sent,stop_evidence=:proof WHERE id=:id'),dict(status=status,now=now,sent=sent,proof='synthetic-physical-close-ack',id=ticket.attempt_id))
            conn.execute(text('UPDATE download_tickets SET state=:status WHERE id=:id'),dict(status=status,id=ticket.ticket_id))
            conn.execute(text("""INSERT INTO download_events(id,logical_download_id,user_id,work_id,ticket_id,attempt_id,started_at_ms,ended_at_ms,status,bytes_sent,stop_evidence)
                SELECT 'event','logical','u','w',t.id,a.id,a.started_at_ms,a.ended_at_ms,a.state,a.bytes_sent,a.stop_evidence FROM download_tickets t JOIN download_attempts a ON a.ticket_id=t.id WHERE t.id=:id"""),dict(id=ticket.ticket_id))
            conn.execute(text("""INSERT INTO download_event_assets SELECT 'event',0,o.edition_id,a.revision_id,a.asset_position,a.object_sha,:sent
                FROM download_ticket_assets a JOIN download_ticket_options o ON o.ticket_id=a.ticket_id AND o.selection_order=a.selection_order WHERE a.ticket_id=:id"""),dict(id=ticket.ticket_id,sent=sent))

    def test_actual_audit_is_append_only_and_matches_physical_attempt(self):
        revision,_,_=self.first();streaming=self.claim(self.ticket(revision));self.finish(streaming)
        self.assertEqual(self.scalar('SELECT bytes_sent FROM download_events'),5)
        self.reject("UPDATE download_events SET bytes_sent=0")
        self.reject("UPDATE download_attempts SET bytes_sent=6")
        self.reject("UPDATE download_attempts SET stop_evidence='changed'")
        self.reject("INSERT INTO download_events SELECT 'forged','other',user_id,work_id,ticket_id,attempt_id,started_at_ms,ended_at_ms,status,999,stop_evidence,failure_code FROM download_events")
        self.reject('DELETE FROM download_event_assets')
        self.reject("UPDATE download_attempts SET state='running',ended_at_ms=NULL,stop_evidence=NULL")
        self.reject("INSERT INTO download_events SELECT 'duplicate',logical_download_id,user_id,work_id,ticket_id,attempt_id,started_at_ms,ended_at_ms,status,bytes_sent,stop_evidence,failure_code FROM download_events")

    def test_ticket_event_requires_selected_asset_and_one_audit_per_attempt(self):
        revision,_,_=self.first()
        ticket=self.claim(self.ticket(revision));self.finish(ticket)
        self.reject("INSERT INTO download_events SELECT 'other','other-logical',user_id,work_id,ticket_id,attempt_id,started_at_ms,ended_at_ms,status,bytes_sent,stop_evidence,failure_code FROM download_events")
        latest,_,asset=self.publish(payload=b'not selected by the issued ticket',previous=revision)
        position=self.scalar('SELECT asset_position FROM revision_assets WHERE edition_revision_id=:id AND object_sha=:sha',id=latest,sha=asset['sha256'])
        self.reject("INSERT INTO download_event_assets VALUES('event',1,'e',:revision,:position,:sha,5)",revision=latest,position=position,sha=asset['sha256'])
        # A direct original transfer has no ticket selection. It retains exact
        # canonical work/edition/revision/object binding and physical evidence.
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO download_events VALUES('individual','individual-logical','u','w',NULL,NULL,1,2,'completed',5,'synthetic-direct-close',NULL)"))
            conn.execute(text("INSERT INTO download_event_assets VALUES('individual',0,'e',:revision,:position,:sha,5)"),dict(revision=latest,position=position,sha=asset['sha256']))
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events WHERE attempt_id=:id',id=ticket.attempt_id),1)
        self.assertEqual(self.scalar("SELECT object_sha FROM download_event_assets WHERE event_id='individual'"),asset['sha256'])

    def test_filtered_snapshot_preserves_audit_uncertain_pins_redacts_session_and_rebuilds_search(self):
        revision,_,asset=self.first()
        completed=self.claim(self.ticket(revision));self.finish(completed)
        active=self.claim(self.ticket(revision))
        issued=self.ticket(revision)
        service=SnapshotService(self.publisher)
        with self.publisher.locks.mutation():
            memory,_,_=service._memory_snapshot()
        with closing(memory):
            before=memory.execute("SELECT reference_id,object_sha FROM object_pins WHERE kind='ticket' ORDER BY reference_id").fetchall()
            service._redact(memory);service._integrity(memory);service._redaction_verified(memory)
            self.assertEqual(memory.execute('SELECT count(*) FROM library_search_fts').fetchone(),(0,))
            self.assertEqual(memory.execute('SELECT DISTINCT session_token_hash,invalidated FROM download_tickets').fetchall(),[(DISABLED_TICKET_SESSION,1)])
            self.assertEqual(memory.execute('SELECT state,ended_at_ms,stop_evidence,invalidated FROM download_attempts WHERE id=?',(active.attempt_id,)).fetchone(),('uncertain',None,None,1))
            self.assertEqual(memory.execute('SELECT bytes_sent,status,stop_evidence FROM download_events').fetchone(),(5,'completed','synthetic-physical-close-ack'))
            inventory,archived=service._inventory(memory)
            plan=RestoreService._reconcile(memory,dict(objects=inventory,archived_originals=archived))
            self.assertEqual(plan['uncertain_download_attempts'],1)
            self.assertEqual(memory.execute("SELECT reference_id,object_sha FROM object_pins WHERE kind='ticket' ORDER BY reference_id").fetchall(),before)
            self.assertEqual(memory.execute('SELECT rowid,work_id FROM library_search_fts').fetchall(),memory.execute('SELECT search_id,work_id FROM library_metadata').fetchall())
            self.assertEqual(memory.execute('SELECT count(*) FROM download_ticket_assets').fetchone(),(3,))
            with tempfile.TemporaryDirectory(prefix='g2a-filtered-') as temp:
                path=Path(temp)/'content.sqlite3'
                memory.execute('VACUUM INTO ?',(str(path),))
                self.assertNotIn(digest(self.token).encode(),path.read_bytes())
                self.assertNotIn(self.token.encode(),path.read_bytes())
        self.assertEqual(self.scalar('SELECT state FROM download_attempts WHERE id=:id',id=active.attempt_id),'running')
        self.assertEqual(self.scalar('SELECT invalidated FROM download_tickets WHERE id=:id',id=issued.ticket_id),0)


class ExistingMetadataMigration(unittest.TestCase):
    def test_backfill_existing_unicode_works_and_future_direct_insert(self):
        with tempfile.TemporaryDirectory(prefix='g2a-old-schema-') as temp:
            engine=database_engine(Path(temp)/'old.sqlite3')
            try:
                config=Config();config.set_main_option('script_location',str(ROOT/'migrations'))
                with engine.begin() as conn:
                    config.attributes['connection']=conn;command.upgrade(config,'0012_ingestion')
                    conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('old','novel','ＳＴＲＡＳＳＥ 日本語','legacy','test','test')"))
                migrate(engine,ROOT/'migrations')
                with engine.connect() as conn:
                    self.assertEqual(conn.execute(text("SELECT title_key,author_key FROM library_metadata WHERE work_id='old'")).one(),('strasse 日本語',''))
                    self.assertEqual(conn.execute(text("SELECT work_id FROM library_search_fts WHERE library_search_fts MATCH 'strasse'")).scalar_one(),'old')
                    self.assertEqual(conn.exec_driver_sql('PRAGMA integrity_check').scalar_one(),'ok')
            finally:engine.dispose()


class MemberFeaturesMigration(unittest.TestCase):
    def test_0015_upgrade_preserves_social_rows_and_adds_only_reviewed_schema(self):
        with tempfile.TemporaryDirectory(prefix='g3-member-migration-') as temp:
            engine = database_engine(Path(temp)/'old.sqlite3')
            try:
                config = Config()
                config.set_main_option('script_location', str(ROOT/'migrations'))
                with engine.begin() as conn:
                    config.attributes['connection'] = conn
                    command.upgrade(config, '0015_library_read_projection')
                    conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES('u','synthetic','disabled-synthetic','test')"))
                    conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('old','novel','Old','old','test','test')"))
                    conn.execute(text("INSERT INTO ratings VALUES('u','old',9,'test')"))
                    conn.execute(text("INSERT INTO library_favorites VALUES('u','old','test')"))
                    before = dict(conn.exec_driver_sql("SELECT name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").all())
                with engine.begin() as conn:
                    config.attributes['connection'] = conn
                    command.upgrade(config, '0016_member_features')
                with engine.connect() as conn:
                    after = dict(conn.exec_driver_sql("SELECT name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").all())
                    self.assertEqual(set(after)-set(before), {'member_preferences', 'ix_ratings_work_user', 'ix_download_completed_window'})
                    self.assertTrue(all(after[name] == sql for name, sql in before.items()))
                    self.assertEqual(conn.exec_driver_sql('SELECT * FROM ratings').one(), ('u','old',9,'test'))
                    self.assertEqual(conn.exec_driver_sql('SELECT * FROM library_favorites').one(), ('u','old','test'))
                    self.assertEqual(conn.exec_driver_sql("SELECT votes,half_sum FROM library_rating_stats WHERE work_id='old'").one(), (1,9))
                    self.assertEqual(conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalar_one(), '0016_member_features')
                    self.assertEqual(conn.exec_driver_sql('PRAGMA integrity_check').scalar_one(), 'ok')
                    self.assertEqual(conn.exec_driver_sql('PRAGMA foreign_key_check').all(), [])
                    self.assertEqual(conn.exec_driver_sql("SELECT strict FROM pragma_table_list WHERE name='member_preferences'").scalar_one(), 1)
                for payload in ('[]', 'broken'):
                    with self.assertRaises(IntegrityError):
                        with write_transaction(engine) as conn:
                            conn.execute(text("INSERT INTO member_preferences VALUES('u',:payload,'test')"), dict(payload=payload))
                with self.assertRaises(IntegrityError):
                    with write_transaction(engine) as conn:
                        conn.execute(text("INSERT INTO member_preferences VALUES('missing','{}','test')"))
            finally:
                engine.dispose()

    def test_fresh_schema_matches_frozen_backup_signature(self):
        from document_library.backup import SCHEMA_VERSION, SCHEMA_SIGNATURE, schema_signature
        from document_library.library_projection import verify_projection_in
        with tempfile.TemporaryDirectory(prefix='g3-fresh-schema-') as temp:
            path = Path(temp)/'fresh.sqlite3'
            engine = database_engine(path)
            try:
                migrate(engine, ROOT/'migrations')
                with closing(sqlite3.connect(path, autocommit=True)) as db:
                    register_sqlite_functions(db)
                    self.assertEqual(SCHEMA_VERSION, '0018_generated_review')
                    self.assertEqual(schema_signature(db), SCHEMA_SIGNATURE)
                    SnapshotService._schema(db)
                    SnapshotService._integrity(db)
                    verify_projection_in(db)
            finally:
                engine.dispose()
