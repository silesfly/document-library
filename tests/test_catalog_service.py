"""Synthetic catalog identity, explicit classification and bounded workflow reads."""
import json
import uuid
import unittest
from unittest.mock import patch
from sqlalchemy import text,event
from document_library.auth import AuthError
from document_library.database import write_transaction
from document_library.catalog_service import CatalogService
from document_library.cursors import CursorError
import test_ingestion as ingestion_fixtures


class CatalogAcceptance(unittest.TestCase):
    def setUp(self):
        self.h=ingestion_fixtures.IngestionAcceptance('runTest');self.h.setUp();self.addCleanup(self.h.doCleanups)
        self.service=CatalogService(self.h.ingest);self.engine=self.h.engine
        self.admin=self.h.admin;self.owner=self.h.uploader;self.member=self.h.member

    def work(self, ident=None, **kwargs):
        return self.service.put_work(self.admin.token,self.admin.csrf,ident or str(uuid.uuid4()),title_display='새 작품',**kwargs)

    def classify(self, work, key=None, **kwargs):
        return self.service.classify(self.admin.token,self.admin.csrf,work,key or str(uuid.uuid4()),evidence='Explicit synthetic classification',**kwargs)

    def test_explicit_ids_unknown_defaults_no_title_merge_and_fresh_admin(self):
        a=self.work();b=self.work()
        self.assertNotEqual(a['work_id'],b['work_id']);self.assertEqual(a['item_type'],'unknown');self.assertEqual(a['publication_state'],'unknown')
        rows=self.service.works(self.admin.token,q='새작품')['works']
        self.assertEqual(len(rows),2);self.assertTrue(all(x['title_match_hint'] for x in rows))
        self.assertEqual(self.h.scalar('SELECT count(*) FROM classification_history'),2)
        with self.assertRaises(AuthError):self.service.put_work(self.member.token,self.member.csrf,str(uuid.uuid4()),title_display='Denied')
        with self.assertRaises(AuthError):self.service.put_work(self.admin.token,'',str(uuid.uuid4()),title_display='Denied')
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE users SET role='member' WHERE id=:id"),dict(id=self.admin.id))
        with self.assertRaises(AuthError):self.service.works(self.admin.token)

    def test_original_create_replay_survives_later_classification_with_stable_initial_history(self):
        initial=self.work();ident=initial['work_id'];key=str(uuid.uuid4())
        self.assertEqual(self.work(ident),initial)
        first=self.classify(ident,key,item_type='novel',publication_state='ongoing',expected_item_type='unknown',expected_publication_state='unknown')
        self.assertEqual(self.work(ident)['item_type'],'novel')
        self.classify(ident,item_type='comic',publication_state='completed',expected_item_type='novel',expected_publication_state='ongoing')
        self.assertEqual(self.classify(ident,key,item_type='novel',publication_state='ongoing',expected_item_type='unknown',expected_publication_state='unknown'),first)
        self.assertEqual(self.work(ident)['item_type'],'comic')
        with self.assertRaises(AuthError):self.work(ident,item_type='novel')
        with self.assertRaises(AuthError):self.classify(ident,item_type='document',publication_state='unknown',expected_item_type='unknown',expected_publication_state='unknown')
        stable=str(uuid.uuid5(uuid.UUID(ident),'catalog.initial-classification.v1'))
        self.assertEqual(json.loads(self.h.scalar('SELECT before_json FROM classification_history WHERE id=:id',id=stable)),{'absent':True})
        # An earlier unrelated imported record cannot become the creation event.
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE classification_history SET created_at='1900-01-01T00:00:00Z' WHERE id!=:id"),dict(id=stable))
        self.assertEqual(self.work(ident)['publication_state'],'completed')
        legacy=str(uuid.uuid4())
        with write_transaction(self.engine) as conn:conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,created_at,updated_at) VALUES(:id,'unknown','새 작품','새작품','test','test')"),dict(id=legacy))
        with self.assertRaises(AuthError):self.work(legacy)

    def test_edition_retry_unique_key_and_existing_legacy_work_binding(self):
        edition=str(uuid.uuid4());data=dict(edition_key='text-ko',edition_label='Text',representation='text')
        put=lambda key=edition,work='w',**extra:self.service.put_edition(self.admin.token,self.admin.csrf,work,key,**dict(data,**extra))
        first=put();self.assertEqual(put(),first);self.assertEqual(first['source_form'],'unknown');self.assertIsNone(first['language'])
        with self.assertRaises(AuthError):put(str(uuid.uuid4()))
        with self.assertRaises(AuthError):put(representation='imagepack')
        other=self.work()['work_id']
        with self.assertRaises(AuthError):put(work=other)
        self.assertEqual(self.service.editions(self.admin.token,'w')['editions'][-1]['active_revision_id'],None)

    def test_catalog_page_cursor_is_bound_and_unpublished_editions_are_not_hidden(self):
        works=sorted(self.work()['work_id'] for _ in range(3))
        page=self.service.works(self.admin.token,q='새',limit=1)
        self.assertEqual(page['works'][0]['work_id'],works[0])
        nextpage=self.service.works(self.admin.token,q='새',limit=1,cursor=page['next_cursor'])
        self.assertEqual(nextpage['works'][0]['work_id'],works[1])
        with self.assertRaises(CursorError):self.service.works(self.admin.token,q='other',cursor=page['next_cursor'])
        with self.assertRaises(AuthError):self.service.editions(self.member.token,'w')

    def test_own_batch_and_admin_queue_include_real_unqueued_states_without_secrets(self):
        upload=self.h.staged();other=self.h.batch(self.admin)
        listing=self.service.batches(self.owner.token)
        self.assertEqual(len(listing['batches']),1);self.assertNotEqual(listing['batches'][0]['batch_id'],other)
        row=self.service.review_queue(self.admin.token)['uploads'][0]
        self.assertEqual(row['upload_id'],upload);self.assertEqual(row['ingestion_state'],'unqueued');self.assertIsNone(row['rights_status'])
        self.assertNotIn('tus_id',row);self.assertNotIn('job_id',row)
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE upload_reservations SET display_filename='../private/secret.txt' WHERE upload_id=:id"),dict(id=upload))
        self.assertIsNone(self.service.review_queue(self.admin.token)['uploads'][0]['display_filename'])
        with self.assertRaises(AuthError):self.service.review_queue(self.owner.token)
        def measured(limit):
            calls=[]
            def capture(conn,cursor,statement,parameters,context,executemany):
                if statement.lstrip().upper().startswith(('SELECT','WITH')):calls.append(statement)
            event.listen(self.engine,'before_cursor_execute',capture)
            try:self.service.review_queue(self.admin.token,limit=limit)
            finally:event.remove(self.engine,'before_cursor_execute',capture)
            return len(calls)
        self.assertEqual(measured(1),measured(100))

    def test_initial_history_namespace_collision_is_conflict_without_partial_work(self):
        future=str(uuid.uuid4())
        event_id=str(uuid.uuid5(uuid.UUID(future),'catalog.initial-classification.v1'))
        self.classify('w',event_id,item_type='comic',publication_state='unknown',expected_item_type='novel',expected_publication_state='unknown')
        with self.assertRaises(AuthError) as error:self.work(future)
        self.assertEqual(error.exception.status,409)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM works WHERE id=:id',id=future),0)

    def test_whole_selected_batch_rejects_mismatched_owner_without_dropping_legacy_rows(self):
        batch=self.h.batch()
        with self.engine.connect() as conn:
            db=conn.connection.driver_connection;db.autocommit=True
            try:
                db.execute('PRAGMA foreign_keys=OFF')
                for owner in (self.owner.id,self.admin.id):
                    db.execute("INSERT INTO upload_sessions VALUES(?,?,?,NULL,1,10,0,?,'receiving')",
                        (str(uuid.uuid4()),batch,owner,self.h.auth.now()+1000))
            finally:
                db.execute('PRAGMA foreign_keys=ON');db.autocommit=False
        with self.assertRaises(AuthError) as error:self.service.batches(self.owner.token,limit=1)
        self.assertEqual(error.exception.status,409)

    def test_creation_and_classification_history_failures_rollback_entire_writer(self):
        ident=str(uuid.uuid4())
        def fail(conn,cursor,statement,parameters,context,executemany):
            if statement.startswith('INSERT INTO classification_history'):raise RuntimeError('synthetic history failure')
        event.listen(self.engine,'before_cursor_execute',fail)
        try:
            with self.assertRaises(RuntimeError):self.work(ident)
        finally:event.remove(self.engine,'before_cursor_execute',fail)
        self.assertEqual(self.h.scalar('SELECT count(*) FROM works WHERE id=:id',id=ident),0)
        self.work(ident);event.listen(self.engine,'before_cursor_execute',fail)
        try:
            with self.assertRaises(RuntimeError):self.classify(ident,item_type='novel',publication_state='ongoing',expected_item_type='unknown',expected_publication_state='unknown')
        finally:event.remove(self.engine,'before_cursor_execute',fail)
        self.assertEqual(self.h.scalar('SELECT item_type FROM works WHERE id=:id',id=ident),'unknown')
