"""Independent numeric/privacy oracles on disposable source-valid SQLite data."""
from dataclasses import FrozenInstanceError
import json
import math
from unittest.mock import patch

from sqlalchemy import event, text

from document_library.database import write_transaction
from document_library.library_contracts import LibraryQuery, RankingConfig, RecommendationQuery
from document_library.library_queries import LibraryQueries, LibraryQueryError
from document_library.recommendations import RecommendationQueries
from library_helpers import LibraryFixture
from test_library_queries import seed_library

DAY=86400000
PREFERENCES=dict(filters=dict(categories=[],states=[],formats=[],representations=[],genres=[]),view='grid',
                 recommendations=dict(exclude_rated=True,exclude_downloaded=False),updated_at=None)


class RecommendationAcceptance(LibraryFixture):
    def setUp(self):
        super().setUp()
        self.queries=LibraryQueries(self.auth,ranking=RankingConfig())
        self.recommendations=RecommendationQueries(self.queries)
        self.as_of=40*DAY+1234

    def users(self,*names):
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,created_at) VALUES(:id,:id,'disabled-synthetic','test')"),[dict(id=name) for name in names])

    def ratings(self,user,values):
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO ratings VALUES(:user,:work,:rating,'test')"),
                         [dict(user=user,work=f'w{number:06}',rating=round(score*2)) for number,score in values.items()])

    def recommend(self,*,user='u',preferences=PREFERENCES,as_of=None,**kwargs):
        with self.engine.connect() as conn:
            return self.recommendations.recommend_in(conn,actor={'id':user},query=RecommendationQuery(**kwargs),
                    preferences=preferences,as_of_ms=self.as_of if as_of is None else as_of)

    def opposite_tastes(self):
        seed_library(self.engine,7,ratings=False)
        self.users('b','peer-a-private','peer-b-private')
        a=[5,4,1,2,3];b=[1,2,5,4,3]
        self.ratings('u',dict(enumerate(a)));self.ratings('b',dict(enumerate(b)))
        self.ratings('peer-a-private',dict(enumerate(a+[5,1])))
        self.ratings('peer-b-private',dict(enumerate(b+[1,5])))

    def test_opposite_taste_users_reverse_x_y_with_exact_pearson_oracle(self):
        self.opposite_tastes()
        a=self.recommend();b=self.recommend(user='b')
        self.assertEqual([x['work']['work_id'] for x in a['items']],['w000005','w000006'])
        self.assertEqual([x['work']['work_id'] for x in b['items']],['w000006','w000005'])
        for result in (a,b):
            self.assertIsNone(result['fallback_reason']);self.assertIsNone(result['next_cursor'])
            self.assertEqual([x['predicted_rating'] for x in result['items']],[5.0,1.0])
            for item in result['items']:
                self.assertAlmostEqual(item['confidence'],1/3)
                self.assertEqual(item['reason'],'similar_taste');self.assertEqual(item['work']['rating_average'],3)
            encoded=json.dumps(result)
            for private in ('peer-a-private','peer-b-private','user_id','password_hash','similarity','ratings_vector'):
                self.assertNotIn(private,encoded)

    def test_minimum_overlap_and_zero_variance_are_explicit_coldstarts(self):
        seed_library(self.engine,6,ratings=False);self.users('single','constant')
        self.ratings('u',dict(enumerate([5,4,1,2,3])))
        self.ratings('single',{0:5,5:5});self.ratings('constant',dict(enumerate([3]*6)))
        result=self.recommend()
        self.assertEqual(result['fallback_reason'],'insufficient_neighbors')
        self.assertTrue(all(x['predicted_rating'] is None and x['reason']=='popular' for x in result['items']))
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE ratings SET rating_half_steps=6 WHERE user_id='u'"))
        self.assertEqual(self.recommend()['fallback_reason'],'insufficient_neighbors')

    def test_overlap_shrink_and_candidate_confidence_do_not_use_global_average(self):
        seed_library(self.engine,6,ratings=False);self.users('peer')
        self.ratings('u',dict(enumerate([1,2,3,4,5])))
        self.ratings('peer',{0:1,1:2,5:5})
        low=self.recommend()['items'][0]
        self.assertAlmostEqual(low['confidence'],2/9)
        self.ratings('peer',{2:3,3:4,4:5})
        high=self.recommend()['items'][0]
        self.assertAlmostEqual(high['confidence'],1/3)
        self.assertGreater(high['confidence'],low['confidence'])
        self.assertTrue(0.5<=high['predicted_rating']<=5)

    def test_top50_peer_cap_has_bounded_deterministic_confidence(self):
        seed_library(self.engine,6,ratings=False)
        peers=[f'peer-{n:03}' for n in range(51)];self.users(*peers)
        self.ratings('u',dict(enumerate([1,2,3,4,5])))
        for peer in peers:self.ratings(peer,dict(enumerate([1,2,3,4,5,4])))
        item=self.recommend()['items'][0]
        self.assertAlmostEqual(item['confidence'],25/26)
        self.assertEqual(self.recommend()['items'],self.recommend()['items'])

    def test_coldstart_reasons_visibility_and_no_personalized_candidates(self):
        seed_library(self.engine,5,ratings=False);self.users('peer')
        self.ratings('u',{0:5,1:4,2:1,3:2})
        self.assertEqual(self.recommend()['fallback_reason'],'insufficient_ratings')
        self.ratings('u',{4:3});self.ratings('peer',dict(enumerate([5,4,1,2,3])))
        result=self.recommend();self.assertEqual(result['items'],[])
        self.assertEqual(result['fallback_reason'],'no_personalized_candidates')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id LIKE 'e000004-%'"))
        result=self.recommend(exclude_rated=False)
        self.assertEqual(result['fallback_reason'],'insufficient_ratings')
        self.assertNotIn('w000004',[x['work']['work_id'] for x in result['items']])

    def test_preferences_field_override_empty_clear_exclusions_and_genre_reason(self):
        seed_library(self.engine,3,ratings=False)
        self.ratings('u',{0:5})
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_genres(work_id,label) VALUES('w000001','Fantasy')"))
        preferences=json.loads(json.dumps(PREFERENCES));preferences['filters']['genres']=['Fantasy']
        before=json.dumps(preferences,sort_keys=True)
        saved=self.recommend(preferences=preferences)
        self.assertEqual([x['work']['work_id'] for x in saved['items']],['w000001'])
        self.assertEqual(saved['items'][0]['reason'],'preferred_genre')
        cleared=self.recommend(preferences=preferences,genres=[],exclude_rated=False)
        self.assertEqual(len(cleared['items']),3)
        forced=self.recommend(mode='unrated',exclude_rated=False,genres=[])
        self.assertNotIn('w000000',[x['work']['work_id'] for x in forced['items']])
        self.assertEqual(json.dumps(preferences,sort_keys=True),before)
        self.downloads([('u',2,self.as_of-1,'completed')])
        excluded=self.recommend(genres=[],exclude_downloaded=True)
        self.assertEqual([x['work']['work_id'] for x in excluded['items']],['w000001'])

    def downloads(self,events):
        with write_transaction(self.engine) as conn:
            start=conn.execute(text('SELECT count(*) FROM download_events')).scalar_one()
            values=[dict(id=f'event-{start+i}',user=user,work=f'w{work:06}',ended=ended,status=status)
                    for i,(user,work,ended,status) in enumerate(events)]
            conn.execute(text("""INSERT INTO download_events(id,logical_download_id,user_id,work_id,started_at_ms,ended_at_ms,status,bytes_sent,stop_evidence)
                VALUES(:id,:id,:user,:work,:ended,:ended,:status,1,'synthetic-closed')"""),values)

    def test_utc_windows_account_work_day_dedup_future_exclusion_audit_unchanged(self):
        seed_library(self.engine,3,ratings=False);self.users('other')
        lower7=self.as_of-7*DAY;lower30=self.as_of-30*DAY
        events=[('u',0,self.as_of-2,'completed')]*500
        events += [('other',0,self.as_of-2,'completed'),('u',0,lower7,'completed'),('u',0,lower30,'completed'),
                   ('u',0,lower30-1,'completed'),('u',0,self.as_of,'completed'),('u',0,self.as_of+1,'completed'),
                   ('u',0,self.as_of-1,'failed'),('u',0,self.as_of-1,'cancelled'),('u',1,self.as_of-1,'completed')]
        self.downloads(events)
        before=self.scalar('SELECT count(*) FROM download_events')
        result=self.recommend(mode='popular',exclude_rated=False)
        cards={x['work']['work_id']:x['work'] for x in result['items']}
        self.assertAlmostEqual(cards['w000000']['popularity'],0.3)
        self.assertAlmostEqual(cards['w000001']['popularity'],0.2*math.log(2)/math.log(4)+0.1*math.log(2)/math.log(5))
        self.assertEqual(cards['w000002']['popularity'],0)
        self.assertEqual(self.scalar('SELECT count(*) FROM download_events'),before)
        expired=self.recommend(mode='popular',exclude_rated=False,as_of=self.as_of+31*DAY)
        self.assertTrue(all(x['work']['popularity']==0 for x in expired['items']))
        # The two sides of UTC midnight are separate votes even within one hour.
        self.downloads([('u',2,39*DAY-1,'completed'),('u',2,39*DAY,'completed')])
        midnight=self.recommend(mode='popular',exclude_rated=False)
        score=next(x['work']['popularity'] for x in midnight['items'] if x['work']['work_id']=='w000002')
        self.assertAlmostEqual(score,0.2*math.log(3)/math.log(4)+0.1*math.log(3)/math.log(5))

    def test_popularity_each_component_and_full_visible_normalization(self):
        seed_library(self.engine,4,ratings=False);self.users('peer')
        self.ratings('u',{0:3,1:3,2:3,3:3});self.ratings('peer',{1:3})
        self.downloads([('u',2,self.as_of-1,'completed'),('u',3,self.as_of-15*DAY,'completed')])
        result=self.recommend(mode='popular',exclude_rated=False)
        scores={x['work']['work_id']:x['work']['popularity'] for x in result['items']}
        self.assertAlmostEqual(scores['w000000'],.65*3/5+.05*math.log(2)/math.log(3))
        self.assertAlmostEqual(scores['w000001'],.65*3/5+.05)
        self.assertAlmostEqual(scores['w000002'],scores['w000000']+.3)
        self.assertAlmostEqual(scores['w000003'],scores['w000000']+.1)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO library_genres(work_id,label) VALUES('w000000','Only')"))
            conn.execute(text("INSERT INTO library_work_metrics VALUES('w000000',999,'stale','test')"))
        filtered=self.recommend(mode='popular',exclude_rated=False,genres=['Only'])
        self.assertAlmostEqual(filtered['items'][0]['work']['popularity'],scores['w000000'])

    def test_recommendation_select_only_same_connection_and_fixed_query_count(self):
        self.opposite_tastes()
        with self.engine.connect() as conn:
            statements=[]
            def capture(connection,cursor,statement,parameters,context,many):statements.append(statement)
            event.listen(self.engine,'before_cursor_execute',capture)
            try:
                with patch.object(self.auth,'current',side_effect=AssertionError('auth forbidden')),patch.object(self.engine,'connect',side_effect=AssertionError('checkout forbidden')):
                    for limit in (1,50):
                        statements.clear()
                        result=self.recommendations.recommend_in(conn,actor={'id':'u'},query=RecommendationQuery(limit=limit),preferences=PREFERENCES,as_of_ms=self.as_of)
                        self.assertEqual(len(statements),8)
                        self.assertTrue(all(s.lstrip().upper().startswith(('WITH','SELECT')) for s in statements))
                        self.assertTrue(conn.in_transaction());self.assertTrue(result['items'])
            finally:event.remove(self.engine,'before_cursor_execute',capture)

    def test_strict_request_config_and_legacy_enablement(self):
        with self.assertRaises(ValueError):RecommendationQueries(LibraryQueries(self.auth))
        for kwargs in ({'limit':True},{'limit':0},{'limit':51},{'mode':'unknown'},{'mode':[]},{'mode':None},{'exclude_rated':1},{'genres':'Fantasy'}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):RecommendationQuery(**kwargs)
        query=RecommendationQuery(genres=['Fantasy'])
        self.assertEqual(query.genres,('Fantasy',))
        with self.assertRaises(FrozenInstanceError):query.limit=10
        for bad in (True,-1,1.2,253402300800000):
            with self.subTest(as_of=bad),self.assertRaises(LibraryQueryError):self.recommend(as_of=bad)

    def test_utc_epoch_boundary_uses_floor_and_window_has_no_epoch_clamp(self):
        seed_library(self.engine,2,ratings=False)
        self.downloads([('u',0,-1,'completed'),('u',0,0,'completed'),('u',1,1,'completed')])
        result=self.recommend(mode='popular',exclude_rated=False,as_of=DAY)
        scores={x['work']['work_id']:x['work']['popularity'] for x in result['items']}
        self.assertAlmostEqual(scores['w000000'],.3)
        self.assertAlmostEqual(scores['w000001'],.3*math.log(2)/math.log(3))

    def fallback_vote_tie(self):
        # Natural P=.18/W=1 tie: v1+d30=1 versus v3+d30=0. Own
        # anchors and the d30=15 normalizer stay outside the request category.
        seed_library(self.engine,5,ratings=False)
        for offset in (10,20,30):seed_library(self.engine,1,offset=offset,ratings=False)
        self.users('fallback-peer','fallback-rater-1','fallback-rater-2','fallback-rater-3')
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE works SET item_type='comic' WHERE id NOT IN ('w000010','w000020')"))
        anchors=dict(enumerate([.5,1,1,1,1.5]))
        self.ratings('u',anchors);self.ratings('fallback-peer',anchors)
        self.ratings('fallback-rater-1',{10:1,20:1})
        self.ratings('fallback-rater-2',{20:1});self.ratings('fallback-rater-3',{20:1})
        self.downloads([('u',30,self.as_of-(8+n)*DAY,'completed') for n in range(15)]
                       +[('u',10,self.as_of-10*DAY,'completed')])

    def test_peer_present_all_fallback_limit_uses_votes_after_exact_popularity_weight_tie(self):
        self.fallback_vote_tie()
        full=self.recommend(mode='popular',categories=('novel_text',))
        by_id={item['work']['work_id']:item['work'] for item in full['items']}
        a,b=by_id['w000010'],by_id['w000020']
        self.assertEqual((a['weighted_rating'],b['weighted_rating']),(1.0,1.0))
        self.assertEqual(a['popularity'],b['popularity']);self.assertAlmostEqual(a['popularity'],.18,places=15)
        self.assertEqual((a['rating_votes'],b['rating_votes']),(1,3))
        self.assertEqual(self.scalar("SELECT sum(r.rating_half_steps)/(2.0*count(*)) FROM ratings r JOIN library_read_works p ON p.work_id=r.work_id WHERE p.visible=1"),1.0)
        popular=self.recommend(mode='popular',categories=('novel_text',),limit=1)
        personal=self.recommend(categories=('novel_text',),limit=1)
        self.assertEqual([item['work']['work_id'] for item in popular['items']],['w000020'])
        self.assertEqual([item['work']['work_id'] for item in personal['items']],['w000020'])
        self.assertEqual(personal['fallback_reason'],'no_personalized_candidates')
        self.assertIsNone(personal['items'][0]['predicted_rating'])

    def test_mixed_prediction_limit_keeps_vote_tied_shared_fallback_tail(self):
        self.fallback_vote_tie()
        seed_library(self.engine,1,offset=5,ratings=False)
        self.ratings('fallback-peer',{5:1})
        full=self.recommend(categories=('novel_text',),limit=3)
        limited=self.recommend(categories=('novel_text',),limit=2)
        self.assertEqual([item['work']['work_id'] for item in full['items']],['w000005','w000020','w000010'])
        self.assertEqual([item['work']['work_id'] for item in limited['items']],['w000005','w000020'])
        predicted,b,a=full['items']
        self.assertEqual((predicted['reason'],predicted['predicted_rating']),('similar_taste',1.0))
        self.assertEqual(a['work']['popularity'],b['work']['popularity'])
        self.assertEqual(a['work']['weighted_rating'],b['work']['weighted_rating'])
        self.assertEqual((a['work']['rating_votes'],b['work']['rating_votes']),(1,3))
        self.assertIsNone(full['fallback_reason']);self.assertIsNone(limited['fallback_reason'])
        self.assertIsNone(a['predicted_rating']);self.assertIsNone(b['predicted_rating'])

    def test_predicted_equal_score_confidence_weight_keeps_id_tie_despite_different_votes(self):
        self.fallback_vote_tie()
        self.ratings('fallback-peer',{10:1,20:1})
        result=self.recommend(categories=('novel_text',),limit=2)
        self.assertEqual([item['work']['work_id'] for item in result['items']],['w000010','w000020'])
        a,b=result['items']
        self.assertEqual((a['predicted_rating'],b['predicted_rating']),(1.0,1.0))
        self.assertEqual(a['confidence'],b['confidence']);self.assertAlmostEqual(a['confidence'],1/3)
        self.assertEqual((a['work']['weighted_rating'],b['work']['weighted_rating']),(1.0,1.0))
        self.assertEqual((a['work']['rating_votes'],b['work']['rating_votes']),(2,4))
        self.assertEqual([item['reason'] for item in result['items']],['similar_taste','similar_taste'])
        self.assertIsNone(result['fallback_reason'])
