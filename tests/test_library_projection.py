"""Independent synthetic source oracle and transactional projection boundaries."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError

from document_library.database import database_engine, migrate, write_transaction
from document_library.library_contracts import (LibraryQuery, plan_valid_v1, time_ms_v1,
    coverage_value_v1, register_sqlite_functions)
from document_library.library_projection import (verify_projection_in, rebuild_projection_in,
    WORK_COLUMNS_V1)
from document_library.library_queries import LibraryQueries, LibraryQueryError
from library_helpers import LibraryFixture
from test_library_queries import seed_library


class ProjectionAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.queries=LibraryQueries(self.auth)

    def parity(self, conn=None):
        if conn is None:
            with self.engine.connect() as opened:
                return self.parity(opened)
        db=conn.connection.driver_connection
        verify_projection_in(db)
        # Independent imperative option oracle: no SQL view/projection helpers.
        rows=conn.execute(text('''SELECT opt.*,r.edition_id,r.is_active,e.work_id,e.edition_key,
            e.representation,e.source_form,s.source_job_id,s.source_generation,s.sealed_at
            FROM revision_options opt JOIN edition_revisions r ON r.id=opt.edition_revision_id
            JOIN work_editions e ON e.id=r.edition_id LEFT JOIN revision_seals s ON s.revision_id=r.id''')).mappings().all()
        plans={(r['job_id'],r['generation']):dict(r) for r in conn.execute(text('SELECT * FROM publication_plans')).mappings()}
        def allowed(key):
            p=plans.get(key)
            return bool(p and p['rights_status']=='allowed' and hashlib.sha256(p['payload_json'].encode()).hexdigest()==p['payload_digest'])
        visible=[]
        for opt in rows:
            if not opt['is_active'] or not allowed((opt['source_job_id'],opt['source_generation'])):continue
            members=conn.execute(text('''SELECT oa.edition_revision_id,oa.asset_position,a.object_sha,a.file_format,
                o.state,o.local_relpath,o.byte_size FROM option_assets oa
                LEFT JOIN revision_assets a ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
                LEFT JOIN objects o ON o.sha256=a.object_sha WHERE oa.option_id=:id'''),dict(id=opt['id'])).mappings().all()
            good=bool(members)
            for member in members:
                sha=member['object_sha']
                sources=conn.execute(text('SELECT source_job_id,source_generation FROM revision_asset_sources WHERE edition_revision_id=:r AND asset_position=:p'),dict(r=member['edition_revision_id'],p=member['asset_position'])).all()
                good=good and (member['edition_revision_id']==opt['edition_revision_id'] and member['file_format']==opt['format']
                    and sha is not None and member['state']=='LIVE' and member['local_relpath']=='objects/'+sha[:2]+'/'+sha[2:4]+'/'+sha
                    and bool(sources) and all(allowed(tuple(source)) for source in sources))
            if good:visible.append(dict(opt,size=sum(m['byte_size'] for m in members)))
        actual=conn.execute(text('SELECT work_id,option_id FROM library_authorized_options_v1 ORDER BY work_id,option_id')).all()
        self.assertEqual(actual,sorted((o['work_id'],o['id']) for o in visible))
        for work in conn.execute(text('SELECT id FROM works')).scalars():
            opts=[o for o in visible if o['work_id']==work]
            opts.sort(key=lambda o:({'text':0,'imagepack':1,'document':2,'single_image':3}[o['representation']],o['edition_key'],o['edition_id'],{'txt':0,'epub':1,'pdf':2}.get(o['format'],3),o['format'],o['id']))
            row=conn.execute(text('SELECT * FROM library_read_works WHERE work_id=:w'),dict(w=work)).mappings().one()
            self.assertEqual(row['visible'],int(bool(opts)))
            self.assertEqual(row['basis_option_id'],opts[0]['id'] if opts else None)
            self.assertEqual(row['size_bytes'],opts[0]['size'] if opts else None)
            self.assertEqual(row['latest_accessible_seal'],max(o['sealed_at'] for o in opts) if opts else None)
            self.assertEqual(row['representation_mask'],sum({'text':1,'imagepack':2,'document':4,'single_image':8}[rep] for rep in {o['representation'] for o in opts}))
            self.assertEqual(row['has_scan'],int(any(o['source_form']=='scan' for o in opts)))
            self.assertEqual(conn.execute(text('SELECT format FROM library_read_formats WHERE work_id=:w ORDER BY format'),dict(w=work)).scalars().all(),sorted({o['format'] for o in opts}))

    def test_raw_digest_and_versioned_metric_contracts_and_unregistered_failure(self):
        raw=' {"x": "é"} '
        self.assertEqual(plan_valid_v1(raw,hashlib.sha256(raw.encode()).hexdigest()),1)
        for payload,digest in ((raw,'0'*64),(raw,1),(None,'0'*64),(b'{}','0'*64)):
            self.assertEqual(plan_valid_v1(payload,digest),0)
        self.assertEqual(time_ms_v1('1970-01-01T01:00:00+01:00'),0)
        self.assertIsNone(time_ms_v1('2026-10-23'))
        self.assertIsNone(coverage_value_v1('{"unit":"episode","intervals":[[2,1]],"gaps":[]}'))
        with closing(sqlite3.connect(':memory:')) as db:
            with self.assertRaises(sqlite3.OperationalError):db.execute("SELECT library_plan_valid_v1('{}','x')")
            register_sqlite_functions(db)
            self.assertEqual(db.execute("SELECT library_plan_valid_v1('{}','x')").fetchone(),(0,))

    def test_source_oracle_invalid_whole_options_and_visible_basis(self):
        seed_library(self.engine,3)
        for offset,invalid in enumerate(('digest','path','format','source','seal'),10):
            seed_library(self.engine,1,offset=offset,invalid=invalid)
        self.parity()
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='review' WHERE job_id='e000001-text-job'"))
            conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000002-scan-r')"))
            self.parity(conn)
        self.parity()
        self.assertEqual(self.queries.detail(self.token,'w000001')['work']['basis_option_id'],'e000001-scan-option')

    def test_old_new_edition_move_revision_and_metadata_times_commit_rollback(self):
        seed_library(self.engine,2)
        with self.engine.connect() as c:
            before=c.execute(text('SELECT * FROM library_read_works ORDER BY work_id')).all()
        with self.assertRaisesRegex(RuntimeError,'rollback'):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE work_editions SET work_id='w000001',edition_key='moved',representation='document',source_form='unknown' WHERE id='e000000-text'"))
                conn.execute(text("UPDATE edition_revisions SET is_active=0 WHERE id='e000001-scan-r'"))
                conn.execute(text("UPDATE works SET updated_at='2027-01-01T00:00:00Z' WHERE id='w000000'"))
                conn.execute(text("UPDATE library_metadata SET updated_at='2028-01-01T00:00:00Z' WHERE work_id='w000001'"))
                self.parity(conn)
                raise RuntimeError('rollback')
        with self.engine.connect() as c:self.assertEqual(c.execute(text('SELECT * FROM library_read_works ORDER BY work_id')).all(),before)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE work_editions SET work_id='w000001',edition_key='moved' WHERE id='e000000-text'"))
            self.parity(conn)
        self.parity()

    def test_inherited_source_revocation_changes_live_basis_and_global_population(self):
        revision,lease,_=self.first()
        newer,_,_=self.publish(previous=revision)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w',10,'test')"))
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:j"),dict(j=lease.job_id))
            self.parity(conn)
            self.assertEqual(conn.execute(text("SELECT visible FROM library_read_works WHERE work_id='w'")).scalar_one(),1)
        detail=self.queries.detail(self.token,'w')['work']
        self.assertEqual(detail['default_option']['revision_id'],newer)
        self.assertEqual(detail['default_option']['asset_positions'],[1])

    def test_defense_refresh_after_guard_removed_digest_source_and_member_corruption(self):
        seed_library(self.engine,1)
        mutations=(
            ('plan_identity_update',"UPDATE publication_plans SET payload_digest='0' WHERE job_id='e000000-text-job'"),
            ('sealed_revision_asset_sources_delete',"DELETE FROM revision_asset_sources WHERE edition_revision_id='e000000-text-r'"),
            ('sealed_option_assets_delete',"DELETE FROM option_assets WHERE option_id='e000000-text-option'"),
            ('object_identity_update',"UPDATE objects SET local_relpath='legacy' WHERE sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000000-text-r')"),
        )
        for guard,sql in mutations:
            with self.subTest(guard=guard),self.assertRaisesRegex(RuntimeError,'rollback'):
                with write_transaction(self.engine) as conn:
                    conn.exec_driver_sql('DROP TRIGGER '+guard)
                    conn.exec_driver_sql(sql)
                    self.parity(conn)
                    self.assertEqual(conn.execute(text("SELECT basis_option_id FROM library_read_works WHERE work_id='w000000'")).scalar_one(),'e000000-scan-option')
                    raise RuntimeError('rollback')
            self.parity()

    def test_shared_source_and_object_indexed_multiwork_fanout_and_last_option(self):
        seed_library(self.engine,3,invalid='seal')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE revision_assets SET object_sha=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000000-text-r') WHERE file_format='txt'"))
            conn.execute(text("INSERT OR IGNORE INTO revision_asset_sources SELECT edition_revision_id,asset_position,'e000000-text-job',1 FROM revision_assets WHERE file_format='txt'"))
            conn.execute(text("INSERT INTO revision_seals SELECT r.id,r.edition_id||'-job',1,100,1 FROM edition_revisions r WHERE r.edition_id LIKE 'e%'"))
            self.parity(conn)
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000000-text-job'"))
            self.parity(conn)
            self.assertEqual(conn.execute(text("SELECT count(*) FROM library_read_formats WHERE format='txt'")).scalar_one(),0)
            conn.execute(text("UPDATE publication_plans SET rights_status='allowed' WHERE job_id='e000000-text-job'"))
            conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000000-text-r')"))
            self.parity(conn)
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE '%scan-job'"))
            self.parity(conn)
            self.assertEqual(conn.execute(text('SELECT sum(visible) FROM library_read_works')).scalar_one(),0)
        self.assertEqual(self.queries.list(self.token).items,())

    def test_ratings_integer_delta_old_new_work_zero_and_suspended_retained(self):
        seed_library(self.engine,3,ratings=False)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w000000',10,'test')"))
            conn.execute(text("UPDATE ratings SET work_id='w000001',rating_half_steps=3"))
            self.parity(conn)
            conn.execute(text("UPDATE users SET status='suspended' WHERE id='u'"))
            self.parity(conn)
            self.assertEqual(conn.execute(text("SELECT votes,half_sum FROM library_rating_stats WHERE work_id='w000001'")).one(),(1,3))
            conn.execute(text('DELETE FROM ratings'))
            self.parity(conn)
            self.assertEqual(conn.execute(text('SELECT sum(votes),sum(half_sum) FROM library_rating_stats')).one(),(0,0))

    def test_missing_epoch_or_rating_rows_fail_entire_query_and_refresh_error_aborts_source(self):
        seed_library(self.engine,2)
        for sql in ("UPDATE library_read_works SET computed_epoch=computed_epoch+1 WHERE work_id='w000001'",
                    "DELETE FROM library_read_works WHERE work_id='w000001'",
                    "DELETE FROM library_rating_stats WHERE work_id='w000001'"):
            with self.subTest(sql=sql),write_transaction(self.engine) as conn:
                conn.exec_driver_sql(sql)
                with self.assertRaisesRegex(LibraryQueryError,'PROJECTION_NOT_READY'):
                    self.queries._rows_in(conn,dict(id='u'),LibraryQuery(search='000000'))
                rebuild_projection_in(conn.connection.driver_connection)
                self.parity(conn)
        with self.assertRaises(DatabaseError):
            with write_transaction(self.engine) as conn:
                conn.exec_driver_sql("CREATE TEMP TRIGGER injected_projection_failure BEFORE UPDATE OF input_epoch ON main.library_read_works BEGIN SELECT RAISE(ABORT,'synthetic refresh failure'); END")
                conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000000-text-job'"))
        self.assertEqual(self.scalar("SELECT rights_status FROM publication_plans WHERE job_id='e000000-text-job'"),'allowed')
        self.parity()

    def test_wal_snapshot_keeps_basis_rating_and_labels_before_concurrent_commit(self):
        seed_library(self.engine,2)
        with self.queries._read(self.token) as (reader,actor):
            old,_,_=self.queries._rows_in(reader,actor,LibraryQuery(sort='title'))
            with write_transaction(self.engine) as writer:
                writer.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000001-%'"))
                writer.execute(text("UPDATE ratings SET rating_half_steps=10 WHERE work_id='w000000'"))
                writer.execute(text("INSERT INTO library_tags VALUES('w000000','committed-new-label')"))
            same,_,_=self.queries._rows_in(reader,actor,LibraryQuery(sort='title'))
            self.assertEqual([(r['id'],r['basis_option_id'],r['weighted_rating']) for r in old],[(r['id'],r['basis_option_id'],r['weighted_rating']) for r in same])
            cards=self.queries._cards_in(reader,same)
            self.assertEqual(len(cards),2)
            self.assertNotIn('committed-new-label',cards[0].work.tags)
        fresh=self.queries.list(self.token,LibraryQuery(sort='title'))
        self.assertEqual(len(fresh.items),1)
        self.assertEqual(fresh.items[0].work.rating_average,5)
        self.assertIn('committed-new-label',fresh.items[0].work.tags)

    def test_replace_moves_edition_old_new_work_and_preserves_work_rating_source(self):
        seed_library(self.engine,2)
        with write_transaction(self.engine) as conn:
            self.assertEqual(conn.exec_driver_sql('PRAGMA recursive_triggers').scalar_one(),1)
            conn.exec_driver_sql("""INSERT OR REPLACE INTO work_editions
                SELECT id,'w000001','moved-document','document',source_form,edition_label,language,source_series,created_at
                FROM work_editions WHERE id='e000000-text'""")
            self.parity(conn)
            self.assertEqual(conn.exec_driver_sql("SELECT representation_mask FROM library_read_works WHERE work_id='w000000'").scalar_one(),2)
            self.assertEqual(conn.exec_driver_sql("SELECT representation_mask FROM library_read_works WHERE work_id='w000001'").scalar_one(),7)
            original=conn.exec_driver_sql("SELECT count(*),sum(rating_half_steps) FROM ratings WHERE work_id='w000001'").one()
            conn.exec_driver_sql("INSERT OR REPLACE INTO works SELECT * FROM works WHERE id='w000001'")
            self.parity(conn)
            self.assertEqual(conn.exec_driver_sql("SELECT votes,half_sum FROM library_rating_stats WHERE work_id='w000001'").one(),original)

    def test_immutable_replace_rejected_and_rights_state_conflict_update_retained(self):
        seed_library(self.engine,1)
        for table,condition in (
            ('publication_plans',"job_id='e000000-text-job'"),
            ('objects',"sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000000-text-r')"),
            ('revision_seals',"revision_id='e000000-text-r'"),
            ('revision_options',"id='e000000-text-option'"),
            ('revision_assets',"edition_revision_id='e000000-text-r'"),
            ('option_assets',"option_id='e000000-text-option'"),
            ('revision_asset_sources',"edition_revision_id='e000000-text-r'"),
        ):
            with self.subTest(table=table),self.assertRaises(DatabaseError):
                with write_transaction(self.engine) as conn:
                    conn.exec_driver_sql('INSERT OR REPLACE INTO '+table+' SELECT * FROM '+table+' WHERE '+condition)
            self.parity()
        with write_transaction(self.engine) as conn:
            conn.exec_driver_sql("""INSERT INTO publication_plans SELECT * FROM publication_plans WHERE job_id='e000000-text-job'
                ON CONFLICT(job_id,generation) DO UPDATE SET rights_status='blocked'""")
            self.parity(conn)
            conn.exec_driver_sql("""INSERT INTO objects SELECT * FROM objects WHERE sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000000-scan-r')
                ON CONFLICT(sha256) DO UPDATE SET state='MISSING'""")
            self.parity(conn)
            self.assertEqual(conn.exec_driver_sql('SELECT sum(visible) FROM library_read_works').scalar_one(),0)

    def test_disabled_recursive_triggers_reject_source_mutation_and_registration_repairs_setting(self):
        seed_library(self.engine,1)
        with self.engine.connect() as conn:
            db=conn.connection.driver_connection
            db.execute('PRAGMA recursive_triggers=OFF')
            with self.assertRaisesRegex(sqlite3.IntegrityError,'recursive library triggers required'):
                db.execute("INSERT OR REPLACE INTO ratings VALUES('u','w000000',10,'bad-mode')")
            self.assertEqual(db.execute("SELECT rating_half_steps FROM ratings WHERE work_id='w000000'").fetchone(),(1,))
            register_sqlite_functions(db)
            self.assertEqual(db.execute('PRAGMA recursive_triggers').fetchone(),(1,))
            self.parity(conn)

    def test_format_insert_update_old_new_work_invalidates_until_atomic_rebuild(self):
        seed_library(self.engine,2)
        for sql in ("INSERT INTO library_read_formats VALUES('w000000','forged')",
                    "UPDATE library_read_formats SET work_id='w000001',format='moved' WHERE work_id='w000000' AND format='txt'"):
            with self.subTest(sql=sql),write_transaction(self.engine) as conn:
                conn.exec_driver_sql(sql)
                flags=conn.exec_driver_sql("SELECT work_id,formats_ready FROM library_read_works WHERE work_id IN ('w000000','w000001') ORDER BY work_id").all()
                self.assertEqual(flags[0],('w000000',0))
                if sql.startswith('UPDATE'):self.assertEqual(flags[1],('w000001',0))
                with self.assertRaisesRegex(LibraryQueryError,'PROJECTION_NOT_READY'):
                    self.queries._rows_in(conn,dict(id='u'),LibraryQuery(search='000001'))
                rebuild_projection_in(conn.connection.driver_connection)
                self.parity(conn)

    def test_memory_backup_and_raw_readonly_connections_register_required_mode(self):
        from document_library.backup import SnapshotService
        self.first()
        service=SnapshotService(self.publisher)
        with self.publisher.locks.mutation():
            memory,_,_=service._memory_snapshot()
        try:
            self.assertEqual(memory.execute('PRAGMA recursive_triggers').fetchone(),(1,))
            verify_projection_in(memory)
        finally:memory.close()
        with closing(sqlite3.connect(Path(self.engine.url.database).as_uri()+'?mode=ro',uri=True,autocommit=True)) as db:
            self.assertEqual(db.execute('PRAGMA recursive_triggers').fetchone(),(0,))
            register_sqlite_functions(db)
            self.assertEqual(db.execute('PRAGMA recursive_triggers').fetchone(),(1,))

    def test_foreign_work_basis_row_does_not_disappear_before_current_default_check(self):
        seed_library(self.engine,2)
        with write_transaction(self.engine) as conn:
            conn.exec_driver_sql("""UPDATE library_read_works SET basis_option_id='e000001-text-option',basis_revision_id='e000001-text-r'
                WHERE work_id='w000000'""")
        with self.assertRaisesRegex(LibraryQueryError,'PROJECTION_NOT_READY'):
            self.queries.list(self.token,LibraryQuery(search='000000',sort='title'))

    def test_format_dirty_barrier_keeps_other_readers_on_committed_snapshot(self):
        seed_library(self.engine,2)
        entered=threading.Event();release=threading.Event();failures=[]
        def barrier():
            if not entered.is_set():
                entered.set()
                if not release.wait(10):raise RuntimeError('synthetic barrier not released')
            return 0
        def mutate():
            try:
                with write_transaction(self.engine) as writer:
                    db=writer.connection.driver_connection
                    db.create_function('projection_barrier',0,barrier)
                    writer.exec_driver_sql("""CREATE TEMP TRIGGER synthetic_format_barrier AFTER UPDATE OF formats_ready
                        ON main.library_read_works WHEN NEW.work_id='w000000' AND NEW.formats_ready=0
                        BEGIN SELECT projection_barrier(); END""")
                    try:
                        writer.exec_driver_sql("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000000-%'")
                    finally:
                        writer.exec_driver_sql('DROP TRIGGER IF EXISTS temp.synthetic_format_barrier')
                        db.create_function('projection_barrier',0,None)
            except BaseException as exc:failures.append(exc)
        # Preopen the additional handle; its first authority read still occurs
        # inside the dirty barrier. No DB operation reenters the writer UDF.
        with self.queries._read(self.token) as (old_reader,actor),self.engine.connect() as during:
            before,_,_=self.queries._rows_in(old_reader,actor,LibraryQuery(sort='title'))
            thread=threading.Thread(target=mutate)
            thread.start()
            try:
                self.assertTrue(entered.wait(10))
                self.assertEqual(old_reader.exec_driver_sql("SELECT formats_ready FROM library_read_works WHERE work_id='w000000'").scalar_one(),1)
                fresh_actor=self.auth.current(during,self.token)
                mid,_,_=self.queries._rows_in(during,fresh_actor,LibraryQuery(sort='title'))
                self.assertEqual([(r['id'],r['basis_option_id'],r['weighted_rating']) for r in mid],[(r['id'],r['basis_option_id'],r['weighted_rating']) for r in before])
                self.assertEqual(during.exec_driver_sql("SELECT formats_ready FROM library_read_works WHERE work_id='w000000'").scalar_one(),1)
            finally:
                release.set();thread.join(10)
            self.assertFalse(thread.is_alive())
            if failures:raise failures[0]
            after_old,_,_=self.queries._rows_in(old_reader,actor,LibraryQuery(sort='title'))
            self.assertEqual([r['id'] for r in after_old],[r['id'] for r in before])
        page=self.queries.list(self.token,LibraryQuery(sort='title'))
        self.assertEqual([card.work.work_id for card in page.items],['w000001'])
        self.assertEqual(page.items[0].work.weighted_rating,1)
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql('SELECT min(formats_ready),max(formats_ready) FROM library_read_works').one(),(1,1))
            self.assertFalse(conn.exec_driver_sql("SELECT 1 FROM sqlite_temp_master WHERE name='synthetic_format_barrier'").first())
            self.parity(conn)

    def test_failed_backfill_rolls_back_schema_and_version(self):
        with tempfile.TemporaryDirectory(prefix='projection-migration-') as folder:
            engine=database_engine(Path(folder)/'isolated.sqlite3')
            try:
                config=Config();config.set_main_option('script_location',str(Path(__file__).resolve().parents[1]/'migrations'))
                with engine.begin() as conn:
                    config.attributes['connection']=conn;command.upgrade(config,'0014_preview_cache')
                    conn.exec_driver_sql("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES('migration-w','novel','m','m','test','test')")
                def broken(_):raise ValueError('synthetic backfill failure')
                with engine.connect() as conn:conn.connection.driver_connection.create_function('library_episode_count_v1',1,broken,deterministic=True)
                with self.assertRaises((DatabaseError,sqlite3.DatabaseError)):migrate(engine,Path(__file__).resolve().parents[1]/'migrations')
                with engine.connect() as conn:
                    self.assertEqual(conn.exec_driver_sql('SELECT version_num FROM alembic_version').scalar_one(),'0014_preview_cache')
                    self.assertFalse(conn.exec_driver_sql("SELECT 1 FROM sqlite_master WHERE name='library_read_works'").first())
            finally:engine.dispose()
