"""Finite same-snapshot rankings; no authority, persistence or cross-request cache."""
import json
import math

from sqlalchemy import text

from .library_contracts import LibraryQuery, RecommendationQuery
from .library_projection import READINESS_V1
from .library_queries import LibraryQueryError, _stamp_ms


class RecommendationQueries:
    def __init__(self, queries):
        if queries.ranking is None:
            raise ValueError('Recommendations require explicitly configured ranking')
        self.queries=queries

    @staticmethod
    def _effective(query, preferences):
        # The owning service validates/persists preferences. Never mutate them.
        filters=preferences['filters'];saved=preferences['recommendations']
        names=('categories','states','formats','representations','genres')
        values={name:tuple(filters[name]) if getattr(query,name) is None else getattr(query,name) for name in names}
        excluded={name:saved[name] if getattr(query,name) is None else getattr(query,name)
                  for name in ('exclude_rated','exclude_downloaded')}
        return LibraryQuery(**values,unrated=True if query.mode=='unrated' or excluded['exclude_rated'] else None,
                            downloaded=False if excluded['exclude_downloaded'] else None,limit=query.limit)

    def _peers_in(self, conn, actor):
        policy=self.queries.ranking
        params=dict(user=actor['id'],minimum=policy.MIN_OVERLAP,shrink=policy.OVERLAP_SHRINK,peers=policy.MAX_PEERS)
        # Own visible work -> work-first ratings index -> grouped overlap. Only
        # bounded selected peer IDs/weights enter Python, never all rating vectors.
        sql="""WITH own AS MATERIALIZED (
 SELECT r.work_id,r.rating_half_steps/2.0 AS x FROM ratings r
 JOIN library_read_works p ON p.work_id=r.work_id WHERE r.user_id=:user AND p.visible=1
), overlap AS (
 SELECT r.user_id,count(*) AS n,sum(o.x) AS sx,sum(r.rating_half_steps/2.0) AS sy,
 sum(o.x*o.x) AS sxx,sum((r.rating_half_steps/2.0)*(r.rating_half_steps/2.0)) AS syy,
 sum(o.x*r.rating_half_steps/2.0) AS sxy
 FROM own o JOIN ratings r ON r.work_id=o.work_id WHERE r.user_id<>:user
 GROUP BY r.user_id HAVING count(*)>=:minimum
), variances AS (
 SELECT *,n*sxx-sx*sx AS vx,n*syy-sy*sy AS vy FROM overlap
), similarities AS (
 SELECT *,max(-1.0,min(1.0,(n*sxy-sx*sy)/sqrt(vx*vy))) AS similarity
 FROM variances WHERE vx>0 AND vy>0
), weighted AS (
 SELECT user_id,n,similarity*n/(n+:shrink*1.0) AS q FROM similarities WHERE similarity>0
) SELECT user_id,n,q FROM weighted ORDER BY q DESC,n DESC,user_id LIMIT :peers"""
        return [dict(row) for row in conn.execute(text(sql),params).mappings()]

    def recommend_in(self, conn, *, actor, query, preferences, as_of_ms):
        """Caller owns current auth, quota and this single read snapshot."""
        if (not isinstance(query,RecommendationQuery) or type(as_of_ms) is not int
                or not 0<=as_of_ms<=253402300799999):
            raise LibraryQueryError('INVALID_INPUT',400)
        if conn.execute(text(READINESS_V1)).first():
            raise LibraryQueryError('PROJECTION_NOT_READY',503)
        effective=self._effective(query,preferences)
        params=self.queries._ranking_params(as_of_ms);params['now_ms']=self.queries.now_ms()
        where=self.queries._filters(effective,actor,params,projection=True)
        base=self.queries._base(where)
        peers=[];fallback=None;own=None
        if query.mode=='personalized':
            own=conn.execute(text("""SELECT count(*) AS n,avg(r.rating_half_steps/2.0) AS mean
                FROM ratings r JOIN library_read_works p ON p.work_id=r.work_id
                WHERE r.user_id=:user AND p.visible=1"""),dict(user=actor['id'])).mappings().one()
            if own['n']<self.queries.ranking.MIN_OWN_RATINGS:fallback='insufficient_ratings'
            else:
                peers=self._peers_in(conn,actor)
                if not peers:fallback='insufficient_neighbors'
        if peers:
            params.update(peers=json.dumps(peers),own_mean=own['mean'])
            base+=""", peers AS MATERIALIZED (
 SELECT json_extract(value,'$.user_id') AS user_id,json_extract(value,'$.q') AS q FROM json_each(:peers)
), peer_means AS MATERIALIZED (
 SELECT r.user_id,avg(r.rating_half_steps/2.0) AS mean FROM peers selected
 JOIN ratings r ON r.user_id=selected.user_id JOIN library_read_works p ON p.work_id=r.work_id
 WHERE p.visible=1 GROUP BY r.user_id
), predictions AS (
 SELECT r.work_id,max(0.5,min(5.0,:own_mean+sum(selected.q*(r.rating_half_steps/2.0-means.mean))/sum(selected.q))) AS predicted_rating,
 sum(selected.q)/(sum(selected.q)+1.0) AS confidence
 FROM peers selected JOIN peer_means means ON means.user_id=selected.user_id
 JOIN ratings r ON r.user_id=selected.user_id JOIN library_read_works p ON p.work_id=r.work_id
 WHERE p.visible=1 GROUP BY r.work_id
)"""
            selection='SELECT v.*,prediction.predicted_rating,prediction.confidence FROM values_for_work v LEFT JOIN predictions prediction ON prediction.work_id=v.id'
            order='predicted_rating IS NULL,predicted_rating DESC,confidence DESC,CASE WHEN predicted_rating IS NULL THEN popularity END DESC,weighted_rating DESC,CASE WHEN predicted_rating IS NULL THEN rating_votes END DESC,page.id'
        else:
            selection='SELECT v.*,NULL AS predicted_rating,NULL AS confidence FROM values_for_work v'
            order=('weighted_rating DESC,rating_votes DESC,page.id' if query.mode=='top_rated'
                   else 'popularity DESC,weighted_rating DESC,rating_votes DESC,page.id')
        params['limit']=query.limit
        # Outer page hydration uses these same rows/connection; no token list()
        # wrapper, second snapshot, per-work SQL or unbounded candidate copy.
        inner_order=order.replace('page.id','v.id')
        selection+=' ORDER BY '+inner_order+' LIMIT :limit'
        rows,_,_=self.queries._option_rows_in(conn,base,selection,params,order)
        if peers and (not rows or rows[0]['predicted_rating'] is None):fallback='no_personalized_candidates'
        cards=self.queries._cards_in(conn,rows)
        items=[]
        for row,card in zip(rows,cards,strict=True):
            prediction=row['predicted_rating'];confidence=row['confidence']
            if prediction is not None:
                if not math.isfinite(prediction) or not math.isfinite(confidence):
                    raise LibraryQueryError('PROJECTION_NOT_READY',503)
                reason='similar_taste'
            elif query.mode in {'top_rated','unrated'}:reason=query.mode
            else:reason='preferred_genre' if effective.genres else 'popular'
            items.append(dict(work=card.public(),reason=reason,predicted_rating=prediction,confidence=confidence))
        algorithm=('pearson-overlap-v1' if query.mode=='personalized' else 'shared-ranking-v1')+';'+self.queries.ranking.version
        return dict(mode=query.mode,algorithm=algorithm,as_of=_stamp_ms(as_of_ms),fallback_reason=fallback,
                    items=items,next_cursor=None)
