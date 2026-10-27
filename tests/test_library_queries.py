"""Synthetic SQLite metadata queries; no original object bytes are seeded."""
from dataclasses import replace
import hashlib
import json
import unittest

from sqlalchemy import event, text

from document_library.auth import digest
from document_library.catalog import title_match_key
from document_library.cursors import encode_cursor, decode_cursor
from document_library.database import write_transaction
from document_library.library_contracts import AssetSelection, LibraryQuery, canonical_json
from document_library.library_queries import LibraryQueries, LibraryQueryError
from document_library.publication import object_path
from library_helpers import LibraryFixture
import test_publication as publication_fixtures


def seed_library(engine, count, *, ratings=True, offset=0, invalid=None, edition_copies=1):
    """Trusted synthetic sealed metadata. Shared with the external100k harness.

    Insert content rows before seals, preserve all production constraints and
    real payload digests. This never claims an actual original was hashed/read.
    """
    with write_transaction(engine) as conn:
        for start in range(offset,offset+count,500):
            works=[];editions=[];revisions=[];jobs=[];plans=[];objects=[];assets=[];options=[];members=[];sources=[];seals=[];votes=[]
            for number in range(start,min(start+500,offset+count)):
                work=f'w{number:06}'
                stamp=f'2026-10-22T00:00:{number%60:02}.000Z'
                title=f'작품 Ｂｏｏｋ 日本語 {number:06}'
                works.append(dict(id=work,title=title,key=title_match_key(title),author=None if number%4==0 else 'Ａｕｔｈｏｒ '+str(number%3),stamp=stamp,state='ongoing' if number%2 else 'completed'))
                if ratings:votes.append(dict(work=work,rating=number%10+1))
                kinds=[('text','text','born_digital','txt'),('scan','imagepack','scan','cbz')]
                for kind,representation,source,format in [(key if copy==0 else f'{key}-{copy:03}',rep,src,fmt) for copy in range(edition_copies) for key,rep,src,fmt in kinds]:
                    edition=f'e{number:06}-{kind}';revision=edition+'-r';job=edition+'-job';option=edition+'-option'
                    sha=hashlib.sha256(edition.encode()).hexdigest();size=(100 if representation=='text' else 1000)+number%7
                    coverage=dict(unit='episode',intervals=[[1,number%5+1],[10,11]],gaps=[[number%5+2,9]],confidence='verified')
                    if representation=='imagepack':coverage=dict(unit='page',intervals=[[1,20]],gaps=[],confidence='verified')
                    elif number%5==0:coverage=dict(unit='unknown',intervals=[],gaps=[],confidence='unknown')
                    payload=canonical_json(dict(edition_id=edition,expected_revision_id=None,assets=[dict(sha256=sha,byte_size=size,file_format=format,asset_role='content',prepared_relpath=None)]))
                    editions.append(dict(id=edition,work=work,key=kind,rep=representation,source=source))
                    revisions.append(dict(id=revision,edition=edition));jobs.append(dict(id=job,work=work));plans.append(dict(job=job,payload=payload,digest='0'*64 if invalid=='digest' else digest(payload)))
                    objects.append(dict(sha=sha,size=size,mime='text/plain' if representation=='text' else 'application/zip',path='legacy/'+sha if invalid=='path' else object_path(sha)))
                    assets.append(dict(revision=revision,sha=sha,format='epub' if invalid=='format' else format));options.append(dict(id=option,revision=revision,format=format,coverage=canonical_json(coverage)))
                    # Publish after registration: otherwise registration's later
                    # timestamp would mask whether a hidden seal leaks updated.
                    members.append(dict(option=option,revision=revision))
                    if invalid!='source':sources.append(dict(revision=revision,job=job))
                    if invalid!='seal':seals.append(dict(revision=revision,job=job,at=1792627260000+number*1000+(500 if representation=='imagepack' else 0)))
            conn.execute(text("INSERT INTO works(id,item_type,title_display,title_match_key,author,publication_state,created_at,updated_at) VALUES(:id,'novel',:title,:key,:author,:state,:stamp,:stamp)"),works)
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,source_form,edition_label,created_at) VALUES(:id,:work,:key,:rep,:source,:key,'test')"),editions)
            conn.execute(text("INSERT INTO edition_revisions VALUES(:id,:edition,1,1,'test')"),revisions)
            conn.execute(text("INSERT INTO jobs(id,kind,work_id,payload_json,payload_digest,idempotency_key,priority,state,max_attempts,next_run_at,created_at,updated_at) VALUES(:id,'synthetic',:work,'{}',:id,:id,10,'completed',3,0,0,0)"),jobs)
            conn.execute(text("INSERT INTO publication_plans VALUES(:job,1,:payload,:digest,'synthetic-sealed-proof','allowed',0)"),plans)
            conn.execute(text("INSERT INTO objects(sha256,byte_size,detected_mime,local_relpath,created_at) VALUES(:sha,:size,:mime,:path,'test')"),objects)
            conn.execute(text("INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format) VALUES(:revision,0,:sha,:format)"),assets)
            conn.execute(text("INSERT INTO revision_options VALUES(:id,:revision,:format,'single_file',:coverage,'verified')"),options)
            conn.execute(text('INSERT INTO option_assets VALUES(:option,:revision,0,0)'),members)
            if sources:conn.execute(text('INSERT INTO revision_asset_sources VALUES(:revision,0,:job,1)'),sources)
            if seals:conn.execute(text('INSERT INTO revision_seals VALUES(:revision,:job,1,:at,1)'),seals)
            if votes:conn.execute(text("INSERT INTO ratings VALUES('u',:work,:rating,'test')"),votes)


class LibraryQueriesAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp();self.queries=LibraryQueries(self.auth)

    def seed(self,count=8,**kwargs):
        seed_library(self.engine,count,**kwargs)

    def ids(self,**kwargs):
        return [card.work.work_id for card in self.queries.list(self.token,LibraryQuery(**kwargs)).items]

    def test_basic_public_card_detail_and_fixed_query_counts(self):
        self.seed()
        statements=[]
        def count(conn,cursor,statement,parameters,context,many):
            if statement.lstrip().upper().startswith(('SELECT','WITH')):statements.append(statement)
        event.listen(self.engine,'before_cursor_execute',count)
        try:
            page=self.queries.list(self.token,LibraryQuery(sort='title',limit=3))
            self.assertEqual(len(statements),7);self.assertEqual(len(page.items),3);self.assertTrue(page.next_cursor)
            statements.clear();detail=self.queries.detail(self.token,'w000001',limit=1)
            self.assertEqual(len(statements),11);self.assertEqual(len(detail['editions']),1);self.assertTrue(detail['next_cursor'])
            next_page=self.queries.detail(self.token,'w000001',cursor=detail['next_cursor'],limit=1)
            self.assertNotEqual(detail['editions'][0]['edition_id'],next_page['editions'][0]['edition_id'])
        finally:event.remove(self.engine,'before_cursor_execute',count)
        encoded=json.dumps(page.public())+json.dumps(detail)
        for hidden in ('local_relpath','prepared_relpath','payload_digest','job_id','session_hash',self.token,digest(self.token)):
            self.assertNotIn(hidden,encoded)
        self.assertEqual(page.items[1].default_option.positions,(0,))

    def test_category_or_representation_and_and_hidden_edition_metrics(self):
        self.seed(3)
        self.assertEqual(len(self.ids(categories=('novel_text','novel_scan'))),3)
        self.assertEqual(len(self.ids(representations=('text','imagepack'))),3)
        before=self.queries.detail(self.token,'w000001')['work']
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000001-scan-job'"))
        self.assertNotIn('w000001',self.ids(representations=('text','imagepack')))
        self.assertIn('w000001',self.ids(categories=('novel_text',)))
        self.assertNotIn('w000001',self.ids(categories=('novel_scan',)))
        after=self.queries.detail(self.token,'w000001')['work']
        self.assertEqual(after['representations'],['text'])
        self.assertEqual((after['basis_option_id'],after['size_bytes']),(before['basis_option_id'],101))
        self.assertLess(after['updated_at'],before['updated_at'])
        self.assertEqual(after['episode_count'],4)  # [1,2]+[10,11], not max11 or minus gaps.

    def test_many_same_representation_editions_are_bounded_and_all_reachable(self):
        self.seed(1,edition_copies=61)
        self.seed(1,offset=1,edition_copies=61)  # Other work stays outside this page.
        observed={'members':0,'scoped_options':0,'scope_work_ids':set()}
        def member(value):
            observed['members']+=1;return value
        def scope(work_id,option_id):
            observed['scoped_options']+=1;observed['scope_work_ids'].add(work_id)
            return option_id
        def capture(conn,cursor,statement,parameters,context,many):
            if 'AS summary_size_bytes' in statement:
                observed['sql'],observed['parameters']=statement,parameters
                conn.connection.driver_connection.create_function('test_card_summary_member',1,member)
                conn.connection.driver_connection.create_function('test_card_summary_scope',2,scope)
                self.assertEqual(statement.count('SELECT v.* FROM json_each('),1)
                instrumented=statement.replace('SELECT v.* FROM json_each(',
                    'SELECT v.*,test_card_summary_scope(v.work_id,v.option_id) AS test_scope_member FROM json_each(',1)
                return instrumented.replace('sum(o.byte_size)','sum(test_card_summary_member(o.byte_size))'),parameters
            return statement,parameters
        event.listen(self.engine,'before_cursor_execute',capture,retval=True)
        try:page=self.queries.list(self.token,LibraryQuery(sort='title',limit=1))
        finally:event.remove(self.engine,'before_cursor_execute',capture)
        self.assertEqual(observed['members'],2)  # Two chosen options, not all122.
        self.assertEqual(observed['scoped_options'],122)  # Rank excludes the other work's122.
        self.assertEqual(observed['scope_work_ids'],{'w000000'})
        with self.engine.connect() as conn:
            source_options=conn.execute(text('SELECT count(*) FROM library_authorized_options_v1')).scalar_one()
            plan=[row[3] for row in conn.exec_driver_sql('EXPLAIN QUERY PLAN '+observed['sql'],observed['parameters'])]
            conn.connection.driver_connection.create_function('test_card_summary_member',1,None)
            conn.connection.driver_connection.create_function('test_card_summary_scope',2,None)
        self.assertEqual(source_options,244)
        self.assertTrue(any(detail.startswith('SEARCH e ') and 'work_id=?' in detail for detail in plan),plan)
        print('CARD_SUMMARY_SCOPE '+json.dumps(dict(cards=1,representation_summaries=2,source_options=source_options,
            scoped_options=observed['scoped_options'],scope_work_ids=sorted(observed['scope_work_ids']),
            member_evaluations=observed['members'],plan=plan)),flush=True)
        self.assertEqual([(v.edition_count,v.public()['has_more_editions']) for v in page.items[0].edition_summaries],[(61,True),(61,True)])
        self.assertEqual(page.items[0].representations,('imagepack','text'))
        self.assertEqual(page.items[0].default_option.option_id,'e000000-text-option')
        self.assertLess(len(json.dumps(page.public())),8000)
        found=[];cursor=None
        while True:
            detail=self.queries.detail(self.token,'w000000',limit=7,cursor=cursor)
            self.assertLessEqual(len(detail['editions']),7)
            found.extend(e['edition_id'] for e in detail['editions'])
            cursor=detail['next_cursor']
            if cursor is None:break
            self.assertLess(len(found),122)
        with self.engine.connect() as conn:
            expected=conn.execute(text("SELECT id FROM work_editions WHERE work_id='w000000' ORDER BY id")).scalars().all()
        self.assertEqual(found,expected);self.assertEqual(len(found),122)

    def test_label_pages_keep_full_search_filters_and_live_work_binding(self):
        self.seed(2)
        labels=[f'label-{n:03}'+(' '*3000 if n==63 else '') for n in range(70)]
        with write_transaction(self.engine) as conn:
            for table in ('library_tags','library_genres'):
                conn.execute(text('INSERT INTO '+table+"(work_id,label) VALUES('w000000',:label)"),[dict(label=label) for label in labels])
        card=self.queries.detail(self.token,'w000000')['work']
        for kind in ('tags','genres'):
            with self.subTest(kind=kind):
                self.assertEqual(card[kind],labels[:64])
                meta=card['label_pages'][kind]
                self.assertEqual((meta['total'],meta['has_more']),(70,True))
                self.assertEqual(decode_cursor(meta['next_cursor'],expected_kind='labels')['last'],dict(key='label-063'))
                following=self.queries.labels(self.token,'w000000',kind,cursor=meta['next_cursor'])
                self.assertEqual(following['items'],[f'label-{n:03}' for n in range(64,70)])
                self.assertEqual((following['total'],following['has_more'],following['next_cursor']),(70,False,None))
                with self.assertRaises(LibraryQueryError):self.queries.labels(self.token,'w000001',kind,cursor=meta['next_cursor'])
                with self.assertRaises(LibraryQueryError):self.queries.labels(self.token,'w000000','genres' if kind=='tags' else 'tags',cursor=meta['next_cursor'])
        self.assertEqual(self.ids(search='label-069'),['w000000'])
        self.assertEqual(self.ids(genres=('label-069',)),['w000000'])
        with self.assertRaises(LibraryQueryError):self.queries.labels(self.token,'w000000','tags; DROP TABLE works')
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000000-%'"))
        with self.assertRaises(LibraryQueryError):self.queries.labels(self.token,'w000000','tags',cursor=card['label_pages']['tags']['next_cursor'])

    def test_legacy_option_asset_overflow_is_explicit_instead_of_truncated(self):
        self.seed(1,invalid='seal')
        with write_transaction(self.engine) as conn:
            for n in range(64):
                params=dict(id=f'extra-{n:03}')
                conn.execute(text("INSERT INTO revision_options SELECT :id,edition_revision_id,format,delivery_kind,coverage_json,coverage_confidence FROM revision_options WHERE id='e000000-text-option'"),params)
                conn.execute(text("INSERT INTO option_assets SELECT :id,edition_revision_id,asset_position,ordinal FROM option_assets WHERE option_id='e000000-text-option'"),params)
            conn.execute(text("INSERT INTO revision_seals VALUES('e000000-text-r','e000000-text-job',1,1792627260000,1)"))
        with self.assertRaises(LibraryQueryError) as caught:self.queries.detail(self.token,'w000000')
        self.assertEqual((caught.exception.code,caught.exception.status),('RESOURCE_STRUCTURE_LIMIT',413))
        self.seed(1,offset=1,invalid='seal')
        with write_transaction(self.engine) as conn:
            for n in range(1,201):
                conn.execute(text("INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format) SELECT edition_revision_id,:position,object_sha,file_format FROM revision_assets WHERE edition_revision_id='e000001-text-r' AND asset_position=0"),dict(position=n))
                conn.execute(text("INSERT INTO option_assets VALUES('e000001-text-option','e000001-text-r',:position,:position)"),dict(position=n))
                conn.execute(text("INSERT INTO revision_asset_sources VALUES('e000001-text-r',:position,'e000001-text-job',1)"),dict(position=n))
            conn.execute(text("INSERT INTO revision_seals VALUES('e000001-text-r','e000001-text-job',1,1792627261000,1)"))
        with self.assertRaises(LibraryQueryError) as caught:self.queries.list(self.token,LibraryQuery(search='000001'))
        self.assertEqual((caught.exception.code,caught.exception.status),('RESOURCE_STRUCTURE_LIMIT',413))

    def test_all_live_source_and_seal_gates_have_no_partial_bundle(self):
        lease=self.h.make_job();first=self.h.asset(lease,b'first');second=self.h.asset(lease,b'second')
        opt=publication_fixtures.option(0);opt.update(delivery_kind='bundle',positions=[0,1])
        self.h.plan(lease,[first,second],[opt]);revision=self.publisher.publish(lease)
        self.assertEqual(self.ids(),['w'])
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=:sha"),dict(sha=second['sha256']))
        self.assertEqual(self.ids(),[])
        with self.assertRaises(LibraryQueryError):self.queries.detail(self.token,'w')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE objects SET state='LIVE' WHERE sha256=:sha"),dict(sha=second['sha256']))
            conn.execute(text('UPDATE edition_revisions SET is_active=0 WHERE id=:id'),dict(id=revision))
        self.admin();self.assertEqual(self.ids(),[])  # Normal admin list is current, not retained.

    def test_legacy_format_mismatch_and_inherited_revocation_match_policy(self):
        first,old,_=self.first();latest,new,asset=self.publish(previous=first)
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=old.job_id))
        detail=self.queries.detail(self.token,'w')
        self.assertEqual([o['assets'][0]['object_sha'] for e in detail['editions'] for o in e['options']],[asset['sha256']])
        self.assertEqual(len(detail['work']['edition_summaries']),1)
        summary=detail['work']['edition_summaries'][0]
        self.assertEqual((summary['revision_id'],summary['edition_count'],summary['size_bytes']),(latest,1,asset['byte_size']))
        with self.engine.connect() as conn:
            selection=AssetSelection(latest,positions=(1,))
            self.assertEqual(len(self.policy.resolve_selection_in(conn,self.token,(selection,)).assets),1)
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=new.job_id))
        self.assertEqual(self.ids(),[])

    def test_missing_source_wrong_digest_and_actual_format_fail_closed(self):
        self.seed(1)
        # A sealed trusted legacy plan may describe EPUB while the actual asset
        # is TXT: create it before seal through the existing publisher primitive.
        lease=self.h.make_job();asset=self.h.asset(lease,b'legacy TXT')
        self.h.plan(lease,[asset],[publication_fixtures.option(0,'epub')]);self.publisher.publish(lease)
        self.assertNotIn('w',self.ids())
        # The synthetic seed helper preserves triggers; an independently seeded
        # unsealed option/source is never eligible despite existing metadata.
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='review' WHERE job_id LIKE 'e000000-%'"))
        self.assertEqual(self.ids(),[])

    def test_preseal_legacy_proofs_and_all_nonlive_states_agree_with_resolver(self):
        for number,invalid in enumerate(('source','digest','path','format','seal')):
            self.seed(1,offset=number,invalid=invalid)
            with self.subTest(invalid=invalid):
                with self.engine.connect() as conn,self.assertRaises(Exception):
                    self.policy.resolve_selection_in(conn,self.token,(AssetSelection(f'e{number:06}-text-r',option_id=f'e{number:06}-text-option'),))
        self.assertEqual(self.ids(),[])
        self.seed(1,offset=5)
        for state in ('MISSING','QUARANTINED','PURGED'):
            with self.subTest(state=state):
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE objects SET state=:state WHERE sha256 IN (SELECT object_sha FROM revision_assets WHERE edition_revision_id LIKE 'e000005-%')"),dict(state=state))
                self.assertEqual(self.ids(),[])
                with self.engine.connect() as conn,self.assertRaises(Exception):
                    self.policy.resolve_selection_in(conn,self.token,(AssetSelection('e000005-text-r',option_id='e000005-text-option'),))

    def test_literal_unicode_search_ready_description_tags_genres_and_title_identity(self):
        self.seed(2)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET title_display=:title WHERE id='w000000'"),dict(title='ＦＯＯ Straße 고양이 % _ " OR *'))
            conn.execute(text("INSERT INTO library_tags VALUES('w000000','日本語タグ')"))
            conn.execute(text("INSERT INTO library_genres VALUES('w000000','ＦＡＮＴＡＳＹ')"))
            conn.execute(text("UPDATE library_metadata SET description_text='unique description',description_state='pending' WHERE work_id='w000000'"))
        for literal in ('ＦＯＯ','STRASSE','고양','%','_','" OR *','日本語タグ','fantasy'):
            with self.subTest(literal=literal):self.assertEqual(self.ids(search=literal),['w000000'])
        self.assertEqual(self.ids(search='unique description'),[])
        self.ready_description('w000000','unique description')
        self.assertEqual(self.ids(search='unique description'),['w000000'])
        self.assertEqual(title_match_key('日本語'),'')
        self.assertEqual(self.ids(search='日本語'),['w000001','w000000'])

    def test_every_sort_both_directions_walks_nulls_ties_without_duplicates(self):
        self.seed(12)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_work_metrics VALUES('w000001',7.5,'synthetic','test')"))
            conn.execute(text("INSERT INTO library_work_metrics VALUES('w000002',7.5,'synthetic','test')"))
        for sort in ('latest','updated','title','author','rating','votes','popularity','episode','size'):
            for direction in ('asc','desc'):
                with self.subTest(sort=sort,direction=direction):
                    query=LibraryQuery(sort=sort,direction=direction,limit=2)
                    def value(n):
                        return dict(latest=n,updated=n,title=f'작품 book 日本語 {n:06}',
                                    author=None if n%4==0 else 'author '+str(n%3),
                                    rating=((n%10+1)/2+5*(58/24))/6,votes=1,
                                    popularity=7.5 if n in (1,2) else None,
                                    episode=None if n%5==0 else n%5+3,size=100+n%7)[sort]
                    known=[n for n in range(12) if value(n) is not None]
                    expected=[f'w{n:06}' for n in sorted(known,key=value,reverse=direction=='desc')]
                    expected +=[f'w{n:06}' for n in range(12) if value(n) is None]
                    found=[]
                    while True:
                        page=self.queries.list(self.token,query);found.extend(c.work.work_id for c in page.items)
                        if page.next_cursor is None:break
                        query=replace(query,cursor=page.next_cursor)
                        self.assertLessEqual(len(found),12)
                    self.assertEqual(found,expected);self.assertEqual(len(set(found)),12)
        authors=self.queries.list(self.token,LibraryQuery(sort='author')).items
        self.assertTrue(all(c.work.author is None for c in authors[-3:]))
        size=self.queries.list(self.token,LibraryQuery(sort='size',direction='asc')).items
        self.assertEqual([c.work.size_bytes for c in size],sorted(c.work.size_bytes for c in size))

    def test_long_title_author_reference_walks_and_conditional_query_count(self):
        self.seed(4)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET title_display=:title,author=:author WHERE id=:work"),
                [dict(work=f'w{n:06}',title='a'*5000+str(n//2),author='b'*5000+str(n//2)) for n in range(4)])
        for sort in ('title','author'):
            for direction in ('asc','desc'):
                query=LibraryQuery(sort=sort,direction=direction,limit=1)
                found=[]
                while True:
                    statements=[]
                    def count(conn,cursor,statement,parameters,context,many):
                        if statement.lstrip().upper().startswith(('SELECT','WITH')):statements.append(statement)
                    event.listen(self.engine,'before_cursor_execute',count)
                    try:page=self.queries.list(self.token,query)
                    finally:event.remove(self.engine,'before_cursor_execute',count)
                    self.assertEqual(len(statements),8 if query.cursor else 7)
                    found.extend(c.work.work_id for c in page.items)
                    if page.next_cursor is None:break
                    payload=decode_cursor(page.next_cursor,expected_kind='library')
                    self.assertEqual(payload['last']['mode'],'ref');self.assertLess(len(page.next_cursor),4096)
                    query=replace(query,cursor=page.next_cursor)
                    self.assertLess(len(found),4)
                self.assertEqual(found,[f'w{n:06}' for n in ((0,1,2,3) if direction=='asc' else (2,3,0,1))])

    def test_reference_anchor_changes_rights_and_query_membership_are_stale(self):
        self.seed(3)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET title_display=:title WHERE id=:work"),[dict(work=f'w{n:06}',title='a'*5000+str(n)) for n in range(3)])
            conn.execute(text("INSERT INTO library_favorites SELECT 'u',id,'test' FROM works WHERE id LIKE 'w0%'"))
        query=LibraryQuery(sort='title',favorite=True,has_gaps=True,limit=1)
        first=self.queries.list(self.token,query);cursor=first.next_cursor
        self.assertEqual(first.items[0].work.work_id,'w000001')
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE works SET title_display=:title WHERE id='w000001'"),dict(title='changed '+('x'*5000)))
        with self.assertRaises(LibraryQueryError) as caught:self.queries.list(self.token,replace(query,cursor=cursor))
        self.assertEqual((caught.exception.code,caught.exception.status),('CURSOR_STALE',409))
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE works SET title_display=:title WHERE id='w000001'"),dict(title='a'*5000+'1'))
        with write_transaction(self.engine) as conn:conn.execute(text("DELETE FROM library_favorites WHERE user_id='u' AND work_id='w000001'"))
        with self.assertRaises(LibraryQueryError) as caught:self.queries.list(self.token,replace(query,cursor=cursor))
        self.assertEqual(caught.exception.code,'CURSOR_STALE')
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_favorites VALUES('u','w000001','test')"))
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000001-%'"))
        with self.assertRaises(LibraryQueryError) as caught:self.queries.list(self.token,replace(query,cursor=cursor))
        self.assertEqual(caught.exception.code,'CURSOR_STALE')

    def test_reference_payload_schema_rejects_mixed_or_wrong_modes(self):
        self.seed(1);query=LibraryQuery(sort='title')
        for last in (dict(mode='ref',anchor_work_id='w000000',typed_key_digest=True),
                     dict(mode='other',anchor_work_id='w000000',typed_key_digest='a'*64),
                     dict(mode='ref',anchor_work_id='w000000',typed_key_digest='a'*64,value='extra')):
            cursor=encode_cursor('library',dict(query=query.fingerprint(),sort='title',direction='asc',last=last))
            with self.assertRaises(LibraryQueryError) as caught:self.queries.list(self.token,replace(query,cursor=cursor))
            self.assertEqual((caught.exception.code,caught.exception.status),('INVALID_CURSOR',400))

    def test_current_user_filters_global_rating_population_and_no_fake_popularity(self):
        self.seed(3)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,status,created_at) VALUES('rater','suspended-rater','synthetic','suspended','test')"))
            conn.execute(text("INSERT INTO ratings VALUES('rater','w000000',10,'test')"))
            conn.execute(text("DELETE FROM ratings WHERE user_id='u' AND work_id='w000002'"))
            conn.execute(text("INSERT INTO library_favorites VALUES('u','w000001','test')"))
            conn.execute(text("INSERT INTO download_events VALUES('done','done','u','w000001',NULL,NULL,1,2,'completed',5,'actual-synthetic-close',NULL)"))
            conn.execute(text("INSERT INTO download_events VALUES('bad','bad','u','w000002',NULL,NULL,1,2,'failed',0,'actual-synthetic-close','failure')"))
        self.ready_description('w000001','official only')
        self.assertEqual(self.ids(unrated=True),['w000002'])
        self.assertEqual(self.ids(favorite=True),['w000001'])
        self.assertEqual(self.ids(downloaded=True),['w000001'])
        self.assertEqual(self.ids(has_summary=True),[])
        filtered=self.queries.list(self.token,LibraryQuery(search='000000')).items[0].work
        # Global rating rows are1,2,10 halfsteps ->13/(2*3), not the mean of work means.
        expected=(11/2+5*(13/6))/(2+5)
        self.assertAlmostEqual(filtered.weighted_rating,expected)
        self.assertEqual(filtered.rating_votes,2)
        all_cards=self.queries.list(self.token,LibraryQuery(sort='popularity')).items
        self.assertTrue(all(c.work.popularity is None for c in all_cards))
        self.assertIsNone(next(c.work.weighted_rating for c in all_cards if c.work.work_id=='w000002'))

    def test_cursor_query_type_and_kind_are_checked_and_revocation_is_live(self):
        self.seed(3);query=LibraryQuery(sort='title',limit=1)
        first=self.queries.list(self.token,query)
        with self.assertRaises(LibraryQueryError):self.queries.list(self.token,replace(query,cursor=first.next_cursor,favorite=True))
        with self.assertRaises(LibraryQueryError):self.queries.list(self.token,replace(query,cursor=encode_cursor('preview',{})))
        bad=encode_cursor('library',dict(query=LibraryQuery(sort='size').fingerprint(),sort='size',direction='desc',last=dict(null_rank=0,value=True,work_id='x')))
        with self.assertRaises(LibraryQueryError):self.queries.list(self.token,LibraryQuery(sort='size',cursor=bad))
        with write_transaction(self.engine) as conn:conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000001-%'"))
        rest=self.queries.list(self.token,replace(query,cursor=first.next_cursor,limit=100))
        self.assertEqual([c.work.work_id for c in rest.items],['w000002'])
        self.auth.logout(self.token,self.csrf)
        with self.assertRaises(Exception) as caught:self.queries.list(self.token,query)
        self.assertEqual(caught.exception.code,'authentication_required')

    def test_card_summaries_keep_each_representation_basis_range_size_and_seal(self):
        self.seed(2)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE work_editions SET edition_label='텍스트 Text 日本語 中文' WHERE id='e000001-text'"))
        page=self.queries.list(self.token,LibraryQuery(search='000001',formats=('cbz',),sort='title'))
        self.assertEqual(len(page.items),1)
        card=page.items[0];summaries=card.public()['edition_summaries']
        self.assertEqual([s['representation'] for s in summaries],['text','imagepack'])
        text_summary,image_summary=summaries
        self.assertEqual(text_summary['edition_label'],'텍스트 Text 日本語 中文')
        self.assertEqual((text_summary['edition_id'],text_summary['revision_id'],text_summary['basis_option_id']),
                         ('e000001-text','e000001-text-r','e000001-text-option'))
        self.assertEqual(text_summary['coverage']['intervals'],[[1,2],[10,11]])
        self.assertEqual(text_summary['coverage']['gaps'],[[3,9]])
        self.assertEqual((text_summary['format'],text_summary['coverage_confidence'],text_summary['source_form'],text_summary['size_bytes']),
                         ('txt','verified','born_digital',101))
        self.assertEqual((image_summary['coverage']['unit'],image_summary['coverage']['intervals'],image_summary['size_bytes']),
                         ('page',[[1,20]],1001))
        self.assertEqual(image_summary['source_form'],'scan')
        self.assertLess(text_summary['updated_at'],image_summary['updated_at'])
        self.assertTrue(all(s['edition_count']==1 and not s['has_more_editions'] for s in summaries))
        self.assertEqual(card.default_option.option_id,text_summary['basis_option_id'])
        self.assertEqual(card.work.size_bytes,101)
        # The format filter admits a work; it never substitutes a different
        # representation's coverage or changes the text-first default basis.
        unknown=self.queries.list(self.token,LibraryQuery(search='000000')).public()['items'][0]['edition_summaries'][0]
        self.assertEqual(unknown['coverage']['unit'],'unknown')
        self.assertEqual(unknown['coverage']['intervals'],[])

    def test_card_summary_format_choice_preserves_actual_option_coverage(self):
        lease=self.h.make_job()
        txt=self.h.asset(lease,b'text original','txt');epub=self.h.asset(lease,b'epub original source','epub')
        self.h.plan(lease,[txt,epub],[publication_fixtures.option(0,'txt',160),publication_fixtures.option(1,'epub',150)])
        self.publisher.publish(lease)
        summary=self.queries.list(self.token).public()['items'][0]['edition_summaries'][0]
        self.assertEqual((summary['format'],summary['coverage']['intervals'],summary['size_bytes']),('txt',[[1,160]],len(b'text original')))
        self.assertEqual(summary['edition_count'],1)  # Two formats are one edition.
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=:sha"),dict(sha=txt['sha256']))
        changed=self.queries.list(self.token).public()['items'][0]['edition_summaries'][0]
        self.assertEqual((changed['format'],changed['coverage']['intervals'],changed['size_bytes']),
                         ('epub',[[1,150]],len(b'epub original source')))
        self.assertNotEqual(changed['basis_option_id'],summary['basis_option_id'])

    def test_card_summary_count_and_representative_follow_current_allowed_editions(self):
        self.seed(1,offset=1,edition_copies=3)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE work_editions SET edition_key='a-first' WHERE id='e000001-text-002'"))
        first=self.queries.list(self.token).public()['items'][0]['edition_summaries'][0]
        self.assertEqual((first['edition_id'],first['edition_count'],first['has_more_editions']),('e000001-text-002',3,True))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000001-text-002-job'"))
        second=self.queries.list(self.token).public()['items'][0]['edition_summaries'][0]
        self.assertEqual((second['edition_id'],second['edition_count']),('e000001-text',2))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='review' WHERE job_id='e000001-text-job'"))
        third=self.queries.list(self.token).public()['items'][0]['edition_summaries'][0]
        self.assertEqual((third['edition_id'],third['edition_count'],third['has_more_editions']),('e000001-text-001',1,False))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id='e000001-text-001-job'"))
        only=self.queries.list(self.token).public()['items'][0]['edition_summaries']
        self.assertEqual([(s['representation'],s['edition_count']) for s in only],[('imagepack',3)])

    def test_card_summaries_cover_all_four_representation_values_without_card_duplicates(self):
        self.seed(1,offset=1,edition_copies=2)
        with write_transaction(self.engine) as conn:
            # Representation and file format are separate schema contracts.
            conn.execute(text("UPDATE work_editions SET representation='document',source_form='unknown',edition_label='문서 Document 中文' WHERE id='e000001-text-001'"))
            conn.execute(text("UPDATE work_editions SET representation='single_image' WHERE id='e000001-scan-001'"))
        page=self.queries.list(self.token)
        self.assertEqual(len(page.items),1)
        card=page.items[0];summaries=card.public()['edition_summaries']
        self.assertEqual([s['representation'] for s in summaries],['text','imagepack','document','single_image'])
        self.assertEqual(card.representations,('document','imagepack','single_image','text'))
        self.assertTrue(all(s['edition_count']==1 for s in summaries))
        document=summaries[2]
        self.assertEqual((document['edition_label'],document['source_form']),('문서 Document 中文','unknown'))

    def test_card_summary_bundle_size_counts_each_logical_member_and_never_partial_range(self):
        self.seed(1,offset=1,invalid='seal')
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO revision_assets(edition_revision_id,asset_position,object_sha,file_format,asset_role) SELECT edition_revision_id,1,object_sha,file_format,asset_role FROM revision_assets WHERE edition_revision_id='e000001-text-r' AND asset_position=0"))
            conn.execute(text("INSERT INTO revision_asset_sources SELECT edition_revision_id,1,source_job_id,source_generation FROM revision_asset_sources WHERE edition_revision_id='e000001-text-r' AND asset_position=0"))
            conn.execute(text("UPDATE revision_options SET delivery_kind='bundle' WHERE id='e000001-text-option'"))
            conn.execute(text("INSERT INTO option_assets VALUES('e000001-text-option','e000001-text-r',1,1)"))
            conn.execute(text("INSERT INTO revision_seals SELECT id,edition_id||'-job',1,1792627261000,1 FROM edition_revisions WHERE edition_id LIKE 'e000001-%'"))
        card=self.queries.list(self.token).items[0];summary=card.public()['edition_summaries'][0]
        self.assertEqual((summary['delivery_kind'],summary['size_bytes']),('bundle',202))
        self.assertEqual(summary['coverage']['intervals'],[[1,2],[10,11]])
        self.assertEqual(card.work.episode_count,4)  # Not doubled for two members.
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE objects SET state='MISSING' WHERE sha256=(SELECT object_sha FROM revision_assets WHERE edition_revision_id='e000001-text-r' AND asset_position=0)"))
        after=self.queries.list(self.token).public()['items'][0]
        self.assertEqual([s['representation'] for s in after['edition_summaries']],['imagepack'])

    def test_status_format_genre_gaps_unknown_and_explicit_classification(self):
        self.seed(4)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_genres VALUES('w000001','Fantasy')"))
            conn.execute(text("UPDATE library_metadata SET classification_state='review_required',classification_evidence='synthetic classification review' WHERE work_id='w000003'"))
        self.assertEqual(self.ids(states=('ongoing',),genres=('ＦＡＮＴＡＳＹ',)),['w000001'])
        self.assertEqual(self.ids(categories=('unknown',)),['w000003'])
        self.assertEqual(len(self.ids(formats=('txt','cbz'))),4)
        self.assertNotIn('w000000',self.ids(has_gaps=False))
        self.assertNotIn('w000000',self.ids(has_gaps=True))
        self.assertEqual(set(self.ids(has_gaps=True)),{'w000001','w000002','w000003'})


if __name__=='__main__':unittest.main()


class ConfiguredRankingAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        from document_library.library_contracts import RankingConfig
        self.queries=LibraryQueries(self.auth,ranking=RankingConfig())

    def test_social_callback_select_only_own_fields_full_visible_c_and_suspended_votes(self):
        from unittest.mock import patch
        seed_library(self.engine,3,ratings=False)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,status,created_at) VALUES('b','other','synthetic','suspended','test')"))
            conn.execute(text("INSERT INTO ratings VALUES('u','w000000',10,'test'),('b','w000000',10,'test'),('b','w000001',1,'test')"))
            conn.execute(text("INSERT INTO library_favorites VALUES('u','w000000','test')"))
            statements=[]
            def capture(connection,cursor,statement,parameters,context,many):statements.append(statement)
            event.listen(self.engine,'before_cursor_execute',capture)
            try:
                with patch.object(self.auth,'current',side_effect=AssertionError('auth forbidden')),patch.object(self.engine,'connect',side_effect=AssertionError('checkout forbidden')):
                    result=self.queries.read_social_in(conn,actor={'id':'u'},work_id='w000000')
                    other=self.queries.read_social_in(conn,actor={'id':'b'},work_id='w000000')
                self.assertTrue(conn.in_transaction())
            finally:event.remove(self.engine,'before_cursor_execute',capture)
            self.assertTrue(all(s.lstrip().upper().startswith(('SELECT','WITH')) for s in statements))
            self.assertEqual(set(result),{'work_id','my_rating_half_steps','rating_average','rating_votes','weighted_rating','favorite'})
            self.assertEqual((result['my_rating_half_steps'],result['rating_votes'],result['rating_average'],result['favorite']),(10,2,5.0,True))
            self.assertAlmostEqual(result['weighted_rating'],27.5/7)
            self.assertFalse(other['favorite'])
            conn.execute(text("UPDATE ratings SET rating_half_steps=6 WHERE user_id='u' AND work_id='w000000'"))
            a=self.queries.read_social_in(conn,actor={'id':'u'},work_id='w000000')
            b=self.queries.read_social_in(conn,actor={'id':'b'},work_id='w000000')
            self.assertEqual((a['my_rating_half_steps'],b['my_rating_half_steps']),(6,10))
            self.assertEqual(a['rating_votes'],b['rating_votes']);self.assertEqual(a['weighted_rating'],b['weighted_rating'])
        filtered=self.queries.list(self.token,LibraryQuery(unrated=False,genres=(),limit=1)).items[0].public()
        self.assertIsInstance(filtered['my_rating_half_steps'],int)
        empty=self.queries.detail(self.token,'w000002')['work']
        self.assertEqual((empty['rating_average'],empty['weighted_rating'],empty['my_rating_half_steps']),(None,None,None))

    def test_config_validation_immutability_m_filter_and_source_freshness(self):
        from dataclasses import FrozenInstanceError
        from document_library.library_contracts import RankingConfig
        for value in (True,False,0,-1,float('nan'),float('inf'),1_000_001,'5',None):
            with self.subTest(value=value),self.assertRaises(ValueError):RankingConfig(m=value)
        config=RankingConfig(m=10)
        with self.assertRaises(FrozenInstanceError):config.m=2
        seed_library(self.engine,2,ratings=False)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES('u','w000000',10,'test'),('u','w000001',1,'test')"))
            conn.execute(text("INSERT INTO library_genres(work_id,label) VALUES('w000000','Only')"))
        queries=LibraryQueries(self.auth,ranking=config)
        card=queries.list(self.token,LibraryQuery(genres=('Only',))).items[0].work
        self.assertAlmostEqual(card.weighted_rating,(5+10*2.75)/11)
        self.assertEqual(card.my_rating_half_steps,10)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000001-%'"))
        card=queries.list(self.token,LibraryQuery(genres=('Only',))).items[0].work
        self.assertAlmostEqual(card.weighted_rating,5)
        with self.engine.connect() as conn:
            with self.assertRaises(LibraryQueryError) as error:queries.read_social_in(conn,actor={'id':'u'},work_id='w000001')
            self.assertEqual(error.exception.status,404)

    def test_configured_popular_cursor_is_exhaustive_and_ignores_legacy_metrics(self):
        seed_library(self.engine,9)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_work_metrics VALUES('w000000',999,'stale','test')"))
        ids=[];cursor=None
        while True:
            page=self.queries.list(self.token,LibraryQuery(sort='popularity',limit=2,cursor=cursor))
            ids.extend(card.work.work_id for card in page.items)
            self.assertTrue(all(0<=card.work.popularity<=1 for card in page.items))
            cursor=page.next_cursor
            if cursor is None:break
        self.assertEqual(len(ids),9);self.assertEqual(len(set(ids)),9)
        self.assertNotEqual(ids[0],'w000000')

    def test_new_own_rating_field_preserves_existing_dto_positional_arguments(self):
        from document_library.library_contracts import WorkReadDTO
        dto=WorkReadDTO('w','title',None,'unknown','novel',None,None,(),(),False,
                        None,None,None,None,None,None,.25,True,False)
        self.assertEqual((dto.popularity,dto.favorite,dto.downloaded,dto.my_rating_half_steps),(.25,True,False,None))
