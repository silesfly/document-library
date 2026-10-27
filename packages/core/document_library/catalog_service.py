"""Explicit administrator catalog writes and bounded current workflow reads."""
import json
import uuid
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from .auth import AuthError
from .catalog import title_match_key
from .cursors import CursorError, decode_cursor, encode_cursor
from .database import write_transaction
from .activity_reads import _id, _stamp, _page, _safe_text, STAGE, JOINS
from .uploads import upload_uuid, display_filename


def conflict():
    return AuthError('ASSET_UNAVAILABLE',409)


def value(raw, limit, *, optional=False):
    if raw is None and optional:
        return None
    clean = _safe_text(raw,limit)
    if clean is None or clean.strip()!=clean:
        raise AuthError('INVALID_INPUT',422)
    return clean


def choice(raw, options):
    if raw not in options:
        raise AuthError('INVALID_INPUT',422)
    return raw


class CatalogService:
    def __init__(self, ingestion):
        self.ingestion,self.auth,self.engine = ingestion,ingestion.auth,ingestion.engine

    def _now(self):
        return datetime.fromtimestamp(self.auth.now(),timezone.utc).isoformat().replace('+00:00','Z')

    @staticmethod
    def _seek(cursor, kind, binding):
        if cursor is None:
            return None
        data=decode_cursor(cursor,expected_kind=kind)
        if set(data)!={'binding','last'} or data['binding']!=binding:
            raise CursorError()
        try:
            return _id(data['last'])
        except AuthError:
            raise CursorError() from None

    @staticmethod
    def _finish(rows, limit, kind, binding, key):
        more=len(rows)>limit;rows=rows[:limit]
        return rows,encode_cursor(kind,dict(binding=binding,last=rows[-1][key])) if more else None

    def works(self, token, *, q='',cursor=None,limit=20):
        _page(limit,cursor)
        if not isinstance(q,str) or len(q)>200 or (q and _safe_text(q,200) is None):
            raise AuthError('INVALID_INPUT',422)
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            last=self._seek(cursor,'catalog.works',q)
            params=dict(q=q,hint=title_match_key(q),last=last,limit=limit+1)
            where="instr(lower(w.title_display),lower(:q))>0 OR (:hint!='' AND w.title_match_key=:hint)"
            if last is not None and not conn.execute(text('SELECT 1 FROM works w WHERE id=:last AND ('+where+')'),params).first():
                raise CursorError()
            rows=[dict(r) for r in conn.execute(text('''SELECT w.id AS work_id,w.title_display,w.author,w.item_type,w.publication_state,
                (w.title_match_key=:hint AND :hint!='') AS title_match_hint,
                (SELECT count(*) FROM work_editions e WHERE e.work_id=w.id) AS edition_count
                FROM works w WHERE ('''+where+''') AND (:last IS NULL OR w.id>:last) ORDER BY w.id LIMIT :limit'''),params).mappings()]
            for row in rows:row['title_match_hint']=bool(row['title_match_hint'])
            rows,next_cursor=self._finish(rows,limit,'catalog.works',q,'work_id')
            return dict(works=rows,next_cursor=next_cursor)

    def editions(self, token, work_id, *, cursor=None,limit=20):
        work_id=_id(work_id);_page(limit,cursor)
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            if not conn.execute(text('SELECT 1 FROM works WHERE id=:id'),dict(id=work_id)).first():
                raise AuthError('RESOURCE_NOT_FOUND',404)
            last=self._seek(cursor,'catalog.editions',work_id)
            if last is not None and not conn.execute(text('SELECT 1 FROM work_editions WHERE id=:id AND work_id=:work'),dict(id=last,work=work_id)).first():
                raise CursorError()
            rows=[dict(r) for r in conn.execute(text('''SELECT e.id AS edition_id,e.work_id,e.edition_key,e.edition_label,
                e.representation,e.source_form,e.language,e.source_series,r.id AS active_revision_id
                FROM work_editions e LEFT JOIN edition_revisions r ON r.edition_id=e.id AND r.is_active=1
                WHERE e.work_id=:work AND (:last IS NULL OR e.id>:last) ORDER BY e.id LIMIT :limit'''),dict(work=work_id,last=last,limit=limit+1)).mappings()]
            rows,next_cursor=self._finish(rows,limit,'catalog.editions',work_id,'edition_id')
            return dict(editions=rows,next_cursor=next_cursor)

    def put_work(self, token, csrf, work_id, *, title_display,author=None,item_type='unknown',publication_state='unknown'):
        work_id=upload_uuid(work_id)
        data=dict(title_display=value(title_display,500),author=value(author,200,optional=True),
            item_type=choice(item_type,{'novel','webtoon','comic','document','unknown'}),
            publication_state=choice(publication_state,{'completed','ongoing','unknown'}))
        with write_transaction(self.engine) as conn:
            self.ingestion._admin(conn,token,csrf)
            old=conn.execute(text('SELECT title_display,author,item_type,publication_state FROM works WHERE id=:id'),dict(id=work_id)).mappings().first()
            event_id=str(uuid.uuid5(uuid.UUID(work_id),'catalog.initial-classification.v1'))
            initial=conn.execute(text('SELECT * FROM classification_history WHERE id=:id'),dict(id=event_id)).mappings().first()
            if old is None and initial is not None:
                raise conflict()
            if old is not None:
                if (initial is None or initial['work_id']!=work_id or initial['edition_id'] is not None
                    or json.loads(initial['before_json'])!={'absent':True} or json.loads(initial['after_json'])!=data):raise conflict()
                data=dict(old)
            else:
                conn.execute(text('''INSERT INTO works(id,title_display,title_match_key,author,item_type,publication_state,created_at,updated_at)
                    VALUES(:id,:title_display,:key,:author,:item_type,:publication_state,:now,:now)'''),dict(data,id=work_id,key=title_match_key(data['title_display']),now=self._now()))
                actor=self.auth.current(conn,token,admin=True)
                conn.execute(text('''INSERT INTO classification_history(id,work_id,actor_id,evidence,before_json,after_json,created_at)
                    VALUES(:id,:work,:actor,'Explicit initial catalog classification',:before,:after,:now)'''),
                    dict(id=event_id,work=work_id,actor=actor['id'],before=json.dumps({'absent':True}),after=json.dumps(data),now=self._now()))

            return dict(work_id=work_id,**data)

    def put_edition(self, token, csrf, work_id, edition_id, *, edition_key,edition_label,representation,source_form='unknown',language=None,source_series=None):
        work_id=_id(work_id);edition_id=upload_uuid(edition_id)
        data=dict(work_id=work_id,edition_key=value(edition_key,128),edition_label=value(edition_label,200),
            representation=choice(representation,{'text','imagepack','document','single_image'}),
            source_form=choice(source_form,{'born_digital','scan','unknown'}),language=value(language,64,optional=True),source_series=value(source_series,200,optional=True))
        with write_transaction(self.engine) as conn:
            self.ingestion._admin(conn,token,csrf)
            if not conn.execute(text('SELECT 1 FROM works WHERE id=:id'),dict(id=work_id)).first():
                raise AuthError('RESOURCE_NOT_FOUND',404)
            old=conn.execute(text('SELECT work_id,edition_key,edition_label,representation,source_form,language,source_series FROM work_editions WHERE id=:id'),dict(id=edition_id)).mappings().first()
            if old is not None:
                if dict(old)!=data:raise conflict()
            else:
                try:
                    conn.execute(text('''INSERT INTO work_editions(id,work_id,edition_key,edition_label,representation,source_form,language,source_series,created_at)
                        VALUES(:id,:work_id,:edition_key,:edition_label,:representation,:source_form,:language,:source_series,:now)'''),dict(data,id=edition_id,now=self._now()))
                except IntegrityError:
                    raise conflict() from None
            return dict(edition_id=edition_id,**data)

    def classify(self, token, csrf, work_id, request_id, *, item_type,publication_state,expected_item_type,expected_publication_state,evidence):
        work_id=_id(work_id);request_id=upload_uuid(request_id);evidence=value(evidence,500)
        types={'novel','webtoon','comic','document','unknown'};states={'completed','ongoing','unknown'}
        before=dict(item_type=choice(expected_item_type,types),publication_state=choice(expected_publication_state,states))
        after=dict(item_type=choice(item_type,types),publication_state=choice(publication_state,states))
        with write_transaction(self.engine) as conn:
            actor=self.ingestion._admin(conn,token,csrf)
            previous=conn.execute(text('SELECT * FROM classification_history WHERE id=:id'),dict(id=request_id)).mappings().first()
            if previous:
                if (previous['work_id']!=work_id or previous['edition_id'] is not None or previous['actor_id']!=actor['id']
                    or previous['evidence']!=evidence or json.loads(previous['before_json'])!=before or json.loads(previous['after_json'])!=after):raise conflict()
            else:
                current=conn.execute(text('SELECT item_type,publication_state FROM works WHERE id=:id'),dict(id=work_id)).mappings().first()
                if current is None:raise AuthError('RESOURCE_NOT_FOUND',404)
                if dict(current)!=before:raise conflict()
                conn.execute(text('UPDATE works SET item_type=:item_type,publication_state=:publication_state,state_evidence=:evidence,updated_at=:now WHERE id=:id'),dict(after,id=work_id,evidence=evidence,now=self._now()))
                conn.execute(text('''INSERT INTO classification_history(id,work_id,actor_id,evidence,before_json,after_json,created_at)
                    VALUES(:id,:work,:actor,:evidence,:before,:after,:now)'''),dict(id=request_id,work=work_id,actor=actor['id'],evidence=evidence,before=json.dumps(before),after=json.dumps(after),now=self._now()))
            return dict(work_id=work_id,classification_id=request_id,**after)

    def batches(self, token, *, cursor=None,limit=20):
        _page(limit,cursor)
        with self.engine.connect() as conn:
            actor=self.auth.current(conn,token)
            last=self._seek(cursor,'workflow.batches',actor['id'])
            if last is not None and not conn.execute(text('SELECT 1 FROM upload_batches WHERE id=:id AND uploader_id=:owner'),dict(id=last,owner=actor['id'])).first():raise CursorError()
            rows=[dict(r) for r in conn.execute(text('''WITH page AS (SELECT * FROM upload_batches WHERE uploader_id=:owner
                AND (:last IS NULL OR id>:last) ORDER BY id LIMIT :limit)
                SELECT b.id AS batch_id,b.status,b.declared_total_bytes,b.created_at,count(s.id) AS file_count,
                coalesce(sum(s.declared_size),0) AS reserved_bytes,coalesce(sum(s.received_size),0) AS received_bytes,
                coalesce(sum(CASE WHEN s.owner_id!=b.uploader_id THEN 1 ELSE 0 END),0) AS wrong_owner
                FROM page b LEFT JOIN upload_sessions s ON s.batch_id=b.id GROUP BY b.id ORDER BY b.id'''),dict(owner=actor['id'],last=last,limit=limit+1)).mappings()]
            for row in rows:
                if row.pop('wrong_owner'):raise conflict()
                row['created_at']=_stamp(row['created_at'])
            rows,next_cursor=self._finish(rows,limit,'workflow.batches',actor['id'],'batch_id')
            return dict(batches=rows,next_cursor=next_cursor)

    def review_queue(self, token, *, cursor=None,limit=20):
        _page(limit,cursor)
        with self.engine.connect() as conn:
            self.auth.current(conn,token,admin=True)
            last=self._seek(cursor,'workflow.queue','pending')
            if last is not None and not conn.execute(text("SELECT 1 FROM upload_sessions WHERE id=:id AND status IN ('receiving','staged') AND expires_at>:now"),dict(id=last,now=self.auth.now())).first():raise CursorError()
            rows=[dict(r) for r in conn.execute(text('''SELECT s.id AS upload_id,s.batch_id,s.owner_id,s.declared_size,s.received_size,
                s.status AS transport_status,coalesce(res.display_filename,i.filename) AS display_filename,
                i.rights_status,i.edition_id,i.expected_revision_id,'''+STAGE+''' AS ingestion_state
                '''+JOINS+''' LEFT JOIN upload_reservations res ON res.upload_id=s.id
                WHERE s.status IN ('receiving','staged') AND s.expires_at>:now AND (:last IS NULL OR s.id>:last) ORDER BY s.id LIMIT :limit'''),dict(last=last,limit=limit+1,now=self.auth.now())).mappings()]
            for row in rows:
                try:row['display_filename']=display_filename(row['display_filename'])
                except AuthError:row['display_filename']=None
            rows,next_cursor=self._finish(rows,limit,'workflow.queue','pending','upload_id')
            return dict(uploads=rows,next_cursor=next_cursor)

    def upload_status(self, token, upload_id, *, admin=False):
        upload_id=upload_uuid(upload_id)
        with self.engine.connect() as conn:
            actor=self.auth.current(conn,token,admin=admin)
            row=conn.execute(text('SELECT s.id,s.owner_id,i.rights_status,'+STAGE+' AS ingestion_state'+JOINS+' WHERE s.id=:id'),dict(id=upload_id)).mappings().first()
            if row is None or (not admin and row['owner_id']!=actor['id']):raise AuthError('RESOURCE_NOT_FOUND',404)
            stage=row['ingestion_state']
            return dict(upload_id=row['id'],ingestion_state=stage,rights_status=row['rights_status'],
                worker_required=stage in {'queued','running','retry_wait','review_pending','validation_review'})
