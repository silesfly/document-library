"""Current library metadata queries. No filesystem or streaming authority.

All visible/default/metric/detail choices share the same option predicate. A
metadata read snapshot never authorizes later original or derivative bytes;
those use fresh LibraryReadPolicy checks in the owning storage reader.
List reads use seven SELECTs, or eight for one long-key reference anchor. Detail
reads use eleven; paged labels use four. None grows with returned work count.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import math
import re
import time
import unicodedata

from sqlalchemy import text

from .auth import AuthError, digest
from .cursors import CursorError, encode_cursor, decode_cursor
from .cover_service import eligible_covers_in, cover_url, _now
from .library_contracts import LibraryQuery, RankingConfig, WorkReadDTO, OptionSnapshot, search_key_v1, canonical_json
from .library_contracts import time_ms_v1 as _time_ms, coverage_value_v1 as _coverage_value
from .library_projection import (VALID_OPTIONS_V1, DEFAULT_ORDER_V1, EDITION_ORDER_V1,
    OPTION_ORDER_V1, READINESS_V1)


class LibraryQueryError(AuthError):
    pass


def _stamp_ms(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value/1000,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')


def _identifier(value):
    if (not isinstance(value,str) or not 1<=len(value)<=200
            or any(unicodedata.category(char).startswith('C') for char in value)):
        raise LibraryQueryError('INVALID_INPUT',400)
    return value


VALID_OPTIONS = VALID_OPTIONS_V1
EDITION_ORDER = EDITION_ORDER_V1
OPTION_ORDER = OPTION_ORDER_V1
DEFAULT_ORDER = DEFAULT_ORDER_V1
CURRENT_DESCRIPTION = "EXISTS(SELECT 1 FROM metadata_heads mh JOIN metadata_eligible_candidates_v1 mc ON mc.id=mh.description_id JOIN metadata_rights mr ON mr.id=mc.rights_id WHERE mh.work_id=w.id AND (mr.cache_until_ms IS NULL OR mr.cache_until_ms>:now_ms))"
RATING_STATS = """global_rating AS MATERIALIZED (
 SELECT sum(rs.half_sum) AS half_sum,sum(rs.votes) AS votes
 FROM library_rating_stats rs JOIN library_read_works p ON p.work_id=rs.work_id
 WHERE p.visible=1
)"""


@dataclass(frozen=True)
class EditionSummary:
    """One current DEFAULT_ORDER basis per representation, never a union.

    edition_count counts distinct currently accessible editions of that type;
    one edition's alternative formats do not inflate it. More editions remain
    available through the work's existing paged detail endpoint.
    """
    edition_id: str
    representation: str
    source_form: str
    edition_label: str
    revision_id: str
    basis_option_id: str
    format: str
    delivery_kind: str
    coverage_json: str
    coverage_confidence: str
    size_bytes: int
    updated_at: str | None
    edition_count: int

    def public(self):
        return dict(edition_id=self.edition_id,representation=self.representation,
                    source_form=self.source_form,edition_label=self.edition_label,
                    revision_id=self.revision_id,basis_option_id=self.basis_option_id,
                    format=self.format,delivery_kind=self.delivery_kind,
                    coverage=json.loads(self.coverage_json),coverage_confidence=self.coverage_confidence,
                    size_bytes=self.size_bytes,updated_at=self.updated_at,edition_count=self.edition_count,
                    has_more_editions=self.edition_count>1)


@dataclass(frozen=True)
class WorkCard:
    work: WorkReadDTO
    representations: tuple
    default_option: OptionSnapshot
    has_gaps: bool | None
    label_pages: dict
    edition_summaries: tuple = ()

    def public(self):
        return dict(asdict(self.work),genres=list(self.work.genres),tags=list(self.work.tags),representations=list(self.representations),
                    default_option=self.default_option.public(),has_gaps=self.has_gaps,label_pages=self.label_pages,
                    edition_summaries=[summary.public() for summary in self.edition_summaries])


@dataclass(frozen=True)
class LibraryPage:
    items: tuple
    next_cursor: str | None

    def public(self):
        return dict(items=[item.public() for item in self.items],next_cursor=self.next_cursor)


class LibraryQueries:
    def __init__(self, auth, *, ranking: RankingConfig | None = None, now_ms=None):
        if ranking is not None and not isinstance(ranking,RankingConfig):
            raise ValueError('Explicit RankingConfig required')
        self.auth,self.engine,self.ranking=auth,auth.engine,ranking
        self.now_ms=now_ms if now_ms is not None else (lambda:int(time.time()*1000))

    @contextmanager
    def _read(self, token):
        # Engine/migration/backup/restore all register the same frozen UDFs.
        with self.engine.connect() as conn:
            actor=self.auth.current(conn,token)
            yield conn,actor

    @staticmethod
    def _search(query,params):
        value=search_key_v1(query.search)
        if not value:
            return ''
        if any(unicodedata.category(c).startswith('C') for c in value):
            raise LibraryQueryError('INVALID_INPUT',400)
        try:value.encode('utf-8')
        except UnicodeError:raise LibraryQueryError('INVALID_INPUT',400) from None
        params['needle']=value
        # Prefix ranges stay literal. The final instr gate also prevents FTS
        # query operators/punctuation from acquiring special meaning.
        last=ord(value[-1]);params['prefix_end']=value[:-1]+chr(last+1) if last<0x10ffff else None
        prefix=' UNION '.join(
            'SELECT work_id FROM '+table+' INDEXED BY '+index+' WHERE '+key+'>=:needle'+
            (' AND '+key+'<:prefix_end' if params['prefix_end'] is not None else '')
            for table,index,key in (
                ('library_metadata','ix_library_title_key','title_key'),
                ('library_metadata','ix_library_author_key','author_key'),
                ('library_tags','ix_library_tags_key','key'),('library_genres','ix_library_genres_key','key')))
        literal="""instr(m.title_key,:needle)>0 OR instr(m.author_key,:needle)>0
          OR (m.description_state='ready' AND """+CURRENT_DESCRIPTION+""" AND instr(m.description_key,:needle)>0)
          OR EXISTS(SELECT 1 FROM library_tags t WHERE t.work_id=w.id AND instr(t.key,:needle)>0)
          OR EXISTS(SELECT 1 FROM library_genres g WHERE g.work_id=w.id AND instr(g.key,:needle)>0)"""
        if len(value)<3:
            fallback=literal.replace('m.','candidate.').replace('w.id','candidate.work_id')
            candidates=prefix+' UNION SELECT candidate.work_id FROM library_metadata candidate WHERE '+fallback
        else:
            params['match']='"'+value.replace('"','""')+'"'
            candidates=prefix+' UNION SELECT work_id FROM library_metadata WHERE search_id IN (SELECT rowid FROM library_search_fts WHERE library_search_fts MATCH :match)'
        return ' AND w.id IN ('+candidates+') AND ('+literal+')'

    @staticmethod
    def _filters(query, actor, params, *, projection=False):
        params.update(user=actor['id'],states=json.dumps(query.states),formats=json.dumps(query.formats),
                      genres=json.dumps([search_key_v1(v) for v in query.genres]))
        # The later basis join enforces visibility. Keep independent metadata
        # filters ahead of option verification rather than probing every work.
        where='1'
        if query.categories:
            conditions=[]
            for category in query.categories:
                if category=='novel_text':conditions.append("(w.item_type='novel' AND EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.representation='text'))")
                elif category=='novel_scan':conditions.append("(w.item_type='novel' AND EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.source_form='scan'))")
                elif category=='unknown':conditions.append("(w.item_type='unknown' OR m.classification_state='review_required')")
                else:conditions.append("w.item_type='"+category+"'")  # LibraryQuery allowlist only.
            where+=' AND ('+' OR '.join(conditions)+')'
        for index,representation in enumerate(query.representations):
            params['representation'+str(index)]=representation
            where+=' AND EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.representation=:representation'+str(index)+')'
        if query.states:where+=' AND w.publication_state IN (SELECT value FROM json_each(:states))'
        if query.formats:where+=' AND EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.format IN (SELECT value FROM json_each(:formats)))'
        if query.genres:where+=' AND EXISTS(SELECT 1 FROM library_genres g WHERE g.work_id=w.id AND g.key IN (SELECT value FROM json_each(:genres)))'
        for name,exists in (
            ('unrated','EXISTS(SELECT 1 FROM ratings mine WHERE mine.user_id=:user AND mine.work_id=w.id)'),
            ('favorite','EXISTS(SELECT 1 FROM library_favorites f WHERE f.user_id=:user AND f.work_id=w.id)'),
            ('downloaded',"EXISTS(SELECT 1 FROM download_events h WHERE h.user_id=:user AND h.work_id=w.id AND h.status='completed')")):
            value=getattr(query,name)
            if value is not None:
                positive=not value if name=='unrated' else value
                where+=' AND '+('' if positive else 'NOT ')+exists
        if query.has_summary is not None:
            where+=" AND "+("" if query.has_summary else "NOT ")+"(m.summary_state='ready' AND "+CURRENT_DESCRIPTION+")"
        if projection:
            where=where.replace("EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.representation='text')","(p.representation_mask & 1)=1")
            where=where.replace("EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.source_form='scan')","p.has_scan=1")
            for index,representation in enumerate(query.representations):
                bit={'text':1,'imagepack':2,'document':4,'single_image':8}[representation]
                where=where.replace('EXISTS(SELECT 1 FROM valid_options v WHERE v.work_id=w.id AND v.representation=:representation'+str(index)+')',
                                    '(p.representation_mask & '+str(bit)+')='+str(bit))
            where=where.replace('FROM valid_options v WHERE v.work_id=w.id AND v.format IN',
                                'FROM library_read_formats v WHERE v.work_id=w.id AND v.format IN')
        return where+LibraryQueries._search(query,params)

    def _weighted_sql(self):
        # Equivalent convex mixture avoids multiplication of large m by C.
        if self.ranking is None:
            return '(rs.half_sum/2.0+5.0*global.half_sum/(2.0*global.votes))/(rs.votes+5.0)'
        return '(rs.votes/(rs.votes+:ranking_m))*(rs.half_sum/(2.0*rs.votes))+(:ranking_m/(rs.votes+:ranking_m))*(global.half_sum/(2.0*global.votes))'

    def _ranking_params(self, as_of_ms):
        return dict(ranking_m=self.ranking.m,as_of_ms=as_of_ms,
                    lower7=as_of_ms-7*86400000,lower30=as_of_ms-30*86400000)

    def _live_metrics(self):
        # Time-first scan, one UTC-day vote per account/work, never per option.
        # All normalization maxima precede request filters.
        weights=self.ranking.POPULAR_WEIGHTS
        return """, completed_days AS MATERIALIZED (
 SELECT user_id,work_id,cast(floor(ended_at_ms/86400000.0) AS INTEGER) AS utc_day,
 max(ended_at_ms>=:lower7) AS recent
 FROM download_events WHERE status='completed' AND ended_at_ms>=:lower30 AND ended_at_ms<:as_of_ms
 GROUP BY user_id,work_id,utc_day
), downloads AS (
 SELECT work_id,sum(recent) AS d7,count(*) AS d30 FROM completed_days GROUP BY work_id
), rank_inputs AS MATERIALIZED (
 SELECT p.work_id,rs.votes,
 CASE WHEN rs.votes>0 AND global.votes>0 THEN """+self._weighted_sql()+""" END AS weighted_rating,
 ln(1.0+coalesce(d.d7,0)) AS log7,ln(1.0+coalesce(d.d30,0)) AS log30,ln(1.0+rs.votes) AS logvotes
 FROM library_read_works p JOIN library_rating_stats rs ON rs.work_id=p.work_id
 CROSS JOIN global_rating global LEFT JOIN downloads d ON d.work_id=p.work_id WHERE p.visible=1
), maxima AS (
 SELECT max(log7) AS m7,max(log30) AS m30,max(logvotes) AS mv FROM rank_inputs
), live_metrics AS NOT MATERIALIZED (
 SELECT r.work_id,"""+str(weights[0])+"""*coalesce(r.weighted_rating,0)/5.0
 +"""+str(weights[1])+"""*coalesce(log7/nullif(m7,0),0)
 +"""+str(weights[2])+"""*coalesce(log30/nullif(m30,0),0)
 +"""+str(weights[3])+"""*coalesce(logvotes/nullif(mv,0),0) AS popularity
 FROM rank_inputs r CROSS JOIN maxima
)"""

    def _base(self, where):
        # One work row; C and live popularity use the entire visible cohort.
        metrics='library_work_metrics' if self.ranking is None else 'live_metrics'
        live='' if self.ranking is None else self._live_metrics()
        return "WITH "+RATING_STATS+live+""", values_for_work AS NOT MATERIALIZED (
  SELECT w.*,m.title_key,m.author_key,m.classification_state,CASE WHEN m.description_state='ready' AND """+CURRENT_DESCRIPTION+""" THEN 'ready' WHEN m.description_state='ready' THEN 'pending' ELSE m.description_state END AS description_state,CASE WHEN m.summary_state='ready' AND """+CURRENT_DESCRIPTION+""" THEN 'ready' WHEN m.summary_state='ready' THEN 'pending' ELSE m.summary_state END AS summary_state,m.cover_state,
 p.basis_option_id,p.basis_revision_id,p.latest_accessible_seal,p.created_key,p.updated_key,p.episode_count,p.gaps_key,p.size_bytes,
 rs.votes AS rating_votes,rs.half_sum/(2.0*nullif(rs.votes,0)) AS rating_average,
 CASE WHEN rs.votes>0 AND global.votes>0 THEN """+self._weighted_sql()+""" END AS weighted_rating,
 metric.popularity,
 (SELECT mine.rating_half_steps FROM ratings mine WHERE mine.user_id=:user AND mine.work_id=w.id) AS my_rating_half_steps,
 EXISTS(SELECT 1 FROM library_favorites f WHERE f.user_id=:user AND f.work_id=w.id) AS favorite,
 EXISTS(SELECT 1 FROM download_events h WHERE h.user_id=:user AND h.work_id=w.id AND h.status='completed') AS downloaded
 FROM library_read_works p JOIN works w ON w.id=p.work_id JOIN library_metadata m ON m.work_id=w.id
 JOIN library_rating_stats rs ON rs.work_id=w.id CROSS JOIN global_rating global
 LEFT JOIN """+metrics+""" metric ON metric.work_id=w.id
 WHERE p.visible=1 AND """+where+" ) "

    def read_social_in(self, conn, *, actor, work_id):
        """SELECT-only readback after the caller's fresh-authorized mutation."""
        work_id=_identifier(work_id)
        if conn.execute(text(READINESS_V1)).first():
            raise LibraryQueryError('PROJECTION_NOT_READY',503)
        params=dict(user=actor['id'],work=work_id)
        if self.ranking is not None:params['ranking_m']=self.ranking.m
        row=conn.execute(text('WITH '+RATING_STATS+""" SELECT p.work_id,
 (SELECT rating_half_steps FROM ratings WHERE user_id=:user AND work_id=p.work_id) AS my_rating_half_steps,
 rs.half_sum/(2.0*nullif(rs.votes,0)) AS rating_average,rs.votes AS rating_votes,
 CASE WHEN rs.votes>0 AND global.votes>0 THEN """+self._weighted_sql()+""" END AS weighted_rating,
 EXISTS(SELECT 1 FROM library_favorites f WHERE f.user_id=:user AND f.work_id=p.work_id) AS favorite
 FROM library_read_works p JOIN library_rating_stats rs ON rs.work_id=p.work_id CROSS JOIN global_rating global
 WHERE p.work_id=:work AND p.visible=1"""),params).mappings().first()
        if row is None:raise LibraryQueryError('RESOURCE_NOT_FOUND',404)
        result=dict(row);result['favorite']=bool(result['favorite'])
        return result

    @staticmethod
    def _sort(query):
        columns=dict(latest='created_key',updated='updated_key',title='title_key',author="nullif(author_key,'')",
                     rating='weighted_rating',votes='rating_votes',popularity='popularity',episode='episode_count',size='size_bytes')
        column=columns[query.sort]
        direction=query.direction or ('asc' if query.sort in {'title','author'} else 'desc')
        return column,direction

    @staticmethod
    def _cursor(raw, query, direction, *, conn, actor, now_ms=None):
        if raw is None:return None
        reference=None
        try:
            data=decode_cursor(raw,expected_kind='library')
            if (set(data)!={'query','sort','direction','last'} or data['query']!=query.fingerprint()
                    or data['sort']!=query.sort or data['direction']!=direction
                    or not isinstance(data['last'],dict)):raise CursorError()
            last=data['last']
            if set(last)=={'mode','anchor_work_id','typed_key_digest'}:
                if (last['mode']!='ref' or query.sort not in {'title','author'}
                        or not isinstance(last['typed_key_digest'],str)
                        or not re.fullmatch('[0-9a-f]{64}',last['typed_key_digest'])):raise CursorError()
                _identifier(last['anchor_work_id']);reference=last
            else:
                if set(last)!={'null_rank','value','work_id'}:raise CursorError()
                _identifier(last['work_id'])
                if type(last['null_rank']) is not int or last['null_rank'] not in {0,1}:raise CursorError()
                value=last['value']
                if last['null_rank']==1:
                    if value is not None:raise CursorError()
                elif query.sort in {'title','author'}:
                    if not isinstance(value,str) or len(value)>2048:raise CursorError()
                elif query.sort in {'rating','popularity'}:
                    if type(value) not in {float,int} or not math.isfinite(value):raise CursorError()
                elif type(value) is not int:raise CursorError()
                return last
        except (CursorError,LibraryQueryError):raise LibraryQueryError('INVALID_CURSOR',400) from None
        # One fresh anchor lookup shares this authenticated read transaction.
        # A reference grants no authority and must still match every filter.
        params=dict(now_ms=now_ms if now_ms is not None else int(time.time()*1000));where=LibraryQueries._filters(query,actor,params)
        params['anchor']=reference['anchor_work_id'];where+=' AND w.id=:anchor AND EXISTS(SELECT 1 FROM valid_options visible WHERE visible.work_id=w.id)'
        if query.has_gaps is not None:
            params['gaps']=int(query.has_gaps)
            where+=' AND (SELECT library_episode_gaps_v1(v.coverage_json) FROM valid_options v WHERE v.work_id=w.id ORDER BY '+DEFAULT_ORDER+' LIMIT 1)=:gaps'
        column='m.title_key' if query.sort=='title' else "nullif(m.author_key,'')"
        row=conn.execute(text('WITH '+VALID_OPTIONS+' SELECT '+column+' AS value FROM works w JOIN library_metadata m ON m.work_id=w.id WHERE '+where),params).mappings().first()
        if row is None or row['value'] is None or LibraryQueries._key_digest(row['value'])!=reference['typed_key_digest']:
            raise LibraryQueryError('CURSOR_STALE',409)
        return dict(null_rank=0,value=row['value'],work_id=reference['anchor_work_id'])

    @staticmethod
    def _key_digest(value):
        return digest(canonical_json(dict(type='text',value=value)))

    def _rows_in(self, conn, actor, query, *, work_id=None):
        if conn.execute(text(READINESS_V1)).first():
            raise LibraryQueryError('PROJECTION_NOT_READY',503)
        params=dict(now_ms=self.now_ms());where=self._filters(query,actor,params,projection=True)
        if self.ranking is not None:params.update(self._ranking_params(self.auth.now()*1000))
        if work_id is not None:params['work']=_identifier(work_id);where+=' AND w.id=:work'
        column,direction=self._sort(query);last=self._cursor(query.cursor,query,direction,conn=conn,actor=actor,now_ms=self.now_ms())
        sql=self._base(where)+', ordered AS NOT MATERIALIZED (SELECT v.*,('+column+') AS sort_value FROM values_for_work v) SELECT *,sort_value IS NULL AS null_rank FROM ordered WHERE 1'
        if query.has_gaps is not None:
            sql+=' AND gaps_key=:gaps';params['gaps']=int(query.has_gaps)
        if last is not None:
            params.update(last_id=last['work_id'],last_value=last['value'])
            if last['null_rank']:
                sql+=' AND sort_value IS NULL AND id>:last_id'
            else:
                compare='>' if direction=='asc' else '<'
                sql+=' AND (sort_value IS NULL OR sort_value '+compare+' :last_value OR (sort_value=:last_value AND id>:last_id))'
        sql+=' ORDER BY null_rank,sort_value '+direction+',id LIMIT :limit';params['limit']=query.limit+1
        # Resolve DTO option fields only for the returned page. Authoritative
        # default/visibility is checked for every selected card in _cards_in.
        split=sql.index(' SELECT *,sort_value IS NULL AS null_rank FROM ordered')
        return self._option_rows_in(conn,sql[:split],sql[split+1:],params,
                                    'null_rank,sort_value '+direction+',page.id')

    @staticmethod
    def _option_rows_in(conn, prefix, selection, params, order):
        sql=prefix+', page AS MATERIALIZED ('+selection+""")
            SELECT page.*,e.id AS edition_id,opt.edition_revision_id AS revision_id,
                opt.format,opt.delivery_kind,opt.coverage_json,opt.coverage_confidence
            FROM page LEFT JOIN revision_options opt ON opt.id=page.basis_option_id AND opt.edition_revision_id=page.basis_revision_id
            LEFT JOIN edition_revisions r ON r.id=opt.edition_revision_id
            LEFT JOIN work_editions e ON e.id=r.edition_id AND e.work_id=page.id
            ORDER BY """+order
        return conn.execute(text(sql),params).mappings().all(),sql,params

    def _cards_in(self, conn, rows):
        if not rows:return ()
        ids=json.dumps([r['id'] for r in rows]);options=json.dumps([r['basis_option_id'] for r in rows]);labels={r['id']:{'tags':[],'genres':[]} for r in rows}
        pages={r['id']:{kind:dict(total=0,has_more=False,next_cursor=None) for kind in ('tags','genres')} for r in rows}
        last_labels={}
        for row in conn.execute(text("""WITH label_rows AS (
            SELECT work_id,'tags' AS kind,label,key FROM library_tags WHERE work_id IN (SELECT value FROM json_each(:ids))
            UNION ALL SELECT work_id,'genres',label,key FROM library_genres WHERE work_id IN (SELECT value FROM json_each(:ids))
            ), ranked AS (SELECT *,row_number() OVER(PARTITION BY work_id,kind ORDER BY key,label) AS rank,
                count(*) OVER(PARTITION BY work_id,kind) AS total FROM label_rows)
            SELECT * FROM ranked WHERE rank<=65 ORDER BY work_id,kind,key,label"""),dict(ids=ids)).mappings():
            page=pages[row['work_id']][row['kind']];page['total']=row['total'];page['has_more']=row['total']>64
            if row['rank']<=64:
                labels[row['work_id']][row['kind']].append(row['label'])
                last_labels[row['work_id'],row['kind']]=row
        for (work,kind),last in last_labels.items():
            if pages[work][kind]['has_more']:pages[work][kind]['next_cursor']=self._label_token(work,kind,last)
        positions={r['basis_option_id']:[] for r in rows}
        for row in conn.execute(text('''WITH ranked AS (SELECT option_id,asset_position,ordinal,
            row_number() OVER(PARTITION BY option_id ORDER BY ordinal) AS rank FROM option_assets
            WHERE option_id IN (SELECT value FROM json_each(:options)))
            SELECT * FROM ranked WHERE rank<=201 ORDER BY option_id,ordinal'''),dict(options=options)).mappings():
            if row['rank']>200:raise LibraryQueryError('RESOURCE_STRUCTURE_LIMIT',413)
            positions[row['option_id']].append(row['asset_position'])
        summaries={r['id']:[] for r in rows}
        checked={}
        # Page work IDs drive authorization before either rank or size work.
        # Count editions, not formats; sum logical members without SHA dedup.
        for row in conn.execute(text('WITH '+VALID_OPTIONS+''', scoped AS MATERIALIZED (
            SELECT v.* FROM json_each(:ids) requested CROSS JOIN valid_options v
            WHERE v.work_id=requested.value
            ), counts AS (
            SELECT work_id,representation,count(DISTINCT edition_id) AS edition_count
            FROM scoped GROUP BY work_id,representation
            ), ranked AS (
            SELECT *,row_number() OVER(PARTITION BY work_id ORDER BY '''+DEFAULT_ORDER+''') AS basis_rank,
                row_number() OVER(PARTITION BY work_id,representation ORDER BY '''+DEFAULT_ORDER+''') AS representation_rank
            FROM scoped)
            SELECT ranked.*,counts.edition_count,
                (SELECT sum(o.byte_size) FROM option_assets oa JOIN revision_assets a
                    ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
                    JOIN objects o ON o.sha256=a.object_sha WHERE oa.option_id=ranked.option_id) AS summary_size_bytes
            FROM ranked JOIN counts USING(work_id,representation)
            WHERE representation_rank=1 ORDER BY work_id,'''+DEFAULT_ORDER),dict(ids=ids)).mappings():
            summaries[row['work_id']].append(EditionSummary(row['edition_id'],row['representation'],row['source_form'],
                row['edition_label'],row['revision_id'],row['option_id'],row['format'],row['delivery_kind'],
                row['coverage_json'],row['coverage_confidence'],row['summary_size_bytes'],_stamp_ms(row['sealed_at']),row['edition_count']))
            if row['basis_rank']==1:checked[row['work_id']]=row['option_id']
        if any(checked.get(row['id'])!=row['basis_option_id'] for row in rows):
            raise LibraryQueryError('PROJECTION_NOT_READY',503)
        covers=eligible_covers_in(conn,[r['id'] for r in rows],now_ms=_now(self.now_ms))
        cards=[]
        for row in rows:
            option=OptionSnapshot(0,row['id'],row['edition_id'],row['revision_id'],row['basis_option_id'],row['format'],row['delivery_kind'],canonical_json(json.loads(row['coverage_json'])),row['coverage_confidence'],tuple(positions[row['basis_option_id']]))
            dto=WorkReadDTO(row['id'],row['title_display'],row['author'],row['publication_state'],item_type=row['item_type'],created_at=_stamp_ms(row['created_key']),updated_at=_stamp_ms(row['updated_key']),genres=tuple(labels[row['id']]['genres']),tags=tuple(labels[row['id']]['tags']),classification_review_required=row['classification_state']=='review_required',basis_option_id=row['basis_option_id'],episode_count=row['episode_count'],size_bytes=row['size_bytes'],weighted_rating=row['weighted_rating'],rating_average=row['rating_average'],rating_votes=row['rating_votes'],my_rating_half_steps=row['my_rating_half_steps'],popularity=row['popularity'],favorite=bool(row['favorite']),downloaded=bool(row['downloaded']),description_state=row['description_state'],summary_state=row['summary_state'],cover_state='ready' if row['id'] in covers else 'pending',cover_url=cover_url(row['id'],covers[row['id']][0]['head_version']) if row['id'] in covers else None)
            cards.append(WorkCard(dto,tuple(sorted(summary.representation for summary in summaries[row['id']])),option,
                None if row['gaps_key'] is None else bool(row['gaps_key']),pages[row['id']],tuple(summaries[row['id']])))
        return tuple(cards)

    @staticmethod
    def _label_token(work_id,kind,last):
        return encode_cursor('labels',dict(work_id=work_id,kind=kind,last=dict(key=last['key'])))

    def labels(self, token, work_id, kind, *, cursor=None, limit=64):
        work_id=_identifier(work_id)
        if not isinstance(kind,str) or kind not in {'tags','genres'} or type(limit) is not int or not 1<=limit<=64:
            raise LibraryQueryError('INVALID_INPUT',400)
        with self._read(token) as (conn,actor):
            if not conn.execute(text('WITH '+VALID_OPTIONS+' SELECT 1 FROM valid_options WHERE work_id=:work LIMIT 1'),dict(work=work_id)).first():
                raise LibraryQueryError('RESOURCE_NOT_FOUND',404)
            params=dict(work=work_id,limit=limit+1);seek=''
            if cursor is not None:
                try:
                    data=decode_cursor(cursor,expected_kind='labels')
                    if (set(data)!={'work_id','kind','last'} or data['work_id']!=work_id or data['kind']!=kind
                            or not isinstance(data['last'],dict) or set(data['last'])!={'key'}):raise CursorError()
                    last=data['last']
                    if not isinstance(last['key'],str) or not 1<=len(last['key'])<=128 or search_key_v1(last['key'])!=last['key']:raise CursorError()
                    params['key']=last['key'];seek=' AND key>:key'
                except (CursorError,LibraryQueryError):raise LibraryQueryError('INVALID_CURSOR',400) from None
            # Identifiers come only from this fixed mapping; kind is never SQL.
            table={'tags':'library_tags','genres':'library_genres'}[kind]
            values=conn.execute(text('SELECT label,key FROM '+table+' WHERE work_id=:work'+seek+' ORDER BY key,label LIMIT :limit'),params).mappings().all()
            total=conn.execute(text('SELECT count(*) FROM '+table+' WHERE work_id=:work'),params).scalar_one()
            more=len(values)>limit;values=values[:limit]
            return dict(items=[r['label'] for r in values],total=total,has_more=more,
                        next_cursor=self._label_token(work_id,kind,values[-1]) if more else None)

    def list(self, token, query=LibraryQuery()):
        if not isinstance(query,LibraryQuery):raise LibraryQueryError('INVALID_INPUT',400)
        with self._read(token) as (conn,actor):
            rows,_,_=self._rows_in(conn,actor,query);more=len(rows)>query.limit;rows=rows[:query.limit]
            items=self._cards_in(conn,rows);cursor=None
            if more:
                last=rows[-1];_,direction=self._sort(query)
                payload=dict(query=query.fingerprint(),sort=query.sort,direction=direction,last=dict(null_rank=last['null_rank'],value=last['sort_value'],work_id=last['id']))
                try:cursor=encode_cursor('library',payload)
                except CursorError:
                    if query.sort not in {'title','author'} or not isinstance(last['sort_value'],str):raise
                    payload['last']=dict(mode='ref',anchor_work_id=last['id'],typed_key_digest=self._key_digest(last['sort_value']))
                    cursor=encode_cursor('library',payload)
            return LibraryPage(items,cursor)

    def detail(self, token, work_id, *, cursor=None, limit=10):
        if type(limit) is not int or not 1<=limit<=50:raise LibraryQueryError('INVALID_INPUT',400)
        work_id=_identifier(work_id)
        with self._read(token) as (conn,actor):
            rows,_,_=self._rows_in(conn,actor,LibraryQuery(limit=1),work_id=work_id)
            if not rows:raise LibraryQueryError('RESOURCE_NOT_FOUND',404)
            card=self._cards_in(conn,rows)[0]
            params=dict(work=work_id,limit=limit+1);seek=''
            if cursor is not None:
                try:
                    data=decode_cursor(cursor,expected_kind='editions')
                    if set(data)!={'work_id','last_id'} or data['work_id']!=work_id:raise CursorError()
                    params['last']=_identifier(data['last_id']);seek=' AND edition_id>:last'
                except (CursorError,LibraryQueryError):raise LibraryQueryError('INVALID_CURSOR',400) from None
            editions=conn.execute(text('WITH '+VALID_OPTIONS+' SELECT DISTINCT edition_id,edition_key,representation,source_form,edition_label,language,revision_id FROM valid_options WHERE work_id=:work'+seek+' ORDER BY edition_id LIMIT :limit'),params).mappings().all()
            more=len(editions)>limit;editions=editions[:limit];ids=json.dumps([e['edition_id'] for e in editions])
            options=conn.execute(text('WITH '+VALID_OPTIONS+''', ranked AS (
                SELECT *,row_number() OVER(PARTITION BY revision_id ORDER BY '''+OPTION_ORDER+''') AS rank,
                (SELECT count(*) FROM revision_assets a WHERE a.edition_revision_id=v.revision_id) AS revision_assets_count
                FROM valid_options v WHERE work_id=:work AND edition_id IN (SELECT value FROM json_each(:ids)))
                SELECT * FROM ranked WHERE rank<=65 ORDER BY '''+DEFAULT_ORDER),dict(work=work_id,ids=ids)).mappings().all()
            if any(o['rank']>64 or o['revision_assets_count']>200 for o in options):
                raise LibraryQueryError('RESOURCE_STRUCTURE_LIMIT',413)
            members=conn.execute(text("""WITH ranked AS (SELECT oa.option_id,oa.ordinal,a.edition_revision_id AS revision_id,a.asset_position,a.object_sha,a.file_format,a.asset_role,o.byte_size,o.detected_mime,
                row_number() OVER(PARTITION BY oa.option_id ORDER BY oa.ordinal) AS rank
                FROM option_assets oa JOIN revision_assets a ON a.edition_revision_id=oa.edition_revision_id AND a.asset_position=oa.asset_position
                JOIN objects o ON o.sha256=a.object_sha WHERE oa.option_id IN (SELECT value FROM json_each(:ids)))
                SELECT * FROM ranked WHERE rank<=201 ORDER BY option_id,ordinal"""),dict(ids=json.dumps([o['option_id'] for o in options]))).mappings().all()
            if any(m['rank']>200 for m in members):raise LibraryQueryError('RESOURCE_STRUCTURE_LIMIT',413)
            grouped={o['option_id']:[] for o in options}
            for row in members:grouped[row['option_id']].append({k:v for k,v in row.items() if k!='rank'})
            result=[]
            for edition in editions:
                values=[]
                for option in options:
                    if option['edition_id']==edition['edition_id']:
                        assets=grouped[option['option_id']]
                        values.append(dict(option_id=option['option_id'],revision_id=option['revision_id'],format=option['format'],delivery_kind=option['delivery_kind'],coverage=json.loads(option['coverage_json']),coverage_confidence=option['coverage_confidence'],asset_count=len(assets),assets=assets))
                result.append(dict(edition,active_revision_id=edition['revision_id'],options=values))
            next_cursor=encode_cursor('editions',dict(work_id=work_id,last_id=editions[-1]['edition_id'])) if more else None
            meta=conn.execute(text('SELECT description_text,description_origin FROM library_metadata WHERE work_id=:work'),dict(work=work_id)).mappings().first()
            description=dict(state=card.work.description_state,text=None,source=None)
            if card.work.description_state=='ready' and meta is not None:
                description=dict(state='ready',text=meta['description_text'],source=meta['description_origin'])
            return dict(work=card.public(),description=description,editions=result,next_cursor=next_cursor)
