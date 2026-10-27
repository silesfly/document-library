"""Composed synthetic fixture; does not inherit and rediscover acceptance tests."""
import json
import os
import unittest
import uuid
from sqlalchemy import text
from document_library.auth import digest
from document_library.database import write_transaction
from document_library.library_policy import LibraryReadPolicy
from document_library.library_contracts import AssetSelection
import test_publication as fixtures


class LibraryFixture(unittest.TestCase):
    def setUp(self):
        self.h=fixtures.PublicationAcceptance()
        self.addCleanup(self.h.doCleanups)
        self.h.setUp()
        os.chmod(self.h.path/'database/test.sqlite3',0o600)
        self.engine,self.auth,self.publisher,self.root=self.h.engine,self.h.auth,self.h.publisher,self.h.root
        self.token,self.csrf=self.h.token,'synthetic-csrf'
        self.policy=LibraryReadPolicy(self.auth,now_ms=lambda:self.h.clock.wall)

    def scalar(self, sql, **params):
        return self.h.scalar(sql,**params)

    def first(self):
        return self.h.first_revision()

    def publish(self, payload=b'new original', format='txt', previous=None):
        lease=self.h.make_job(previous)
        asset=self.h.asset(lease,payload,format)
        assets,options=[],[]
        if previous is not None:
            with self.engine.connect() as conn:
                rows=conn.execute(text("""SELECT a.object_sha AS sha256,o.byte_size,o.detected_mime,a.file_format,a.asset_role
                    FROM revision_assets a JOIN objects o ON o.sha256=a.object_sha WHERE a.edition_revision_id=:id ORDER BY a.asset_position"""),dict(id=previous)).mappings()
                assets=[dict(row,prepared_relpath=None) for row in rows]
                for old in conn.execute(text('SELECT * FROM revision_options WHERE edition_revision_id=:id ORDER BY id'),dict(id=previous)).mappings():
                    positions=conn.execute(text('SELECT asset_position FROM option_assets WHERE option_id=:id ORDER BY ordinal'),dict(id=old['id'])).scalars().all()
                    options.append(dict(format=old['format'],delivery_kind=old['delivery_kind'],positions=positions,coverage=json.loads(old['coverage_json'])))
        options.append(fixtures.option(len(assets),format));assets.append(asset)
        self.h.plan(lease,assets,options,previous=previous)
        return self.publisher.publish(lease),lease,asset

    def admin(self):
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET role='admin' WHERE id='u'"))

    def ready_description(self, work_id, body):
        """Publish a synthetic editorial ledger; test assertions stay independent."""
        from document_library.metadata_service import MetadataService
        actor='metadata-fixture-'+uuid.uuid4().hex
        token='synthetic-metadata-session-'+uuid.uuid4().hex
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,role,created_at) VALUES(:id,:id,'disabled-synthetic','admin','test')"),dict(id=actor))
            conn.execute(text("INSERT INTO auth_sessions VALUES(:token,:csrf,:user,1,:now,:end)"),dict(token=digest(token),csrf=digest(self.csrf),user=actor,now=self.auth.now(),end=self.auth.now()+3600))
        service=MetadataService(self.publisher)
        rights=str(uuid.uuid4())
        service.review_rights(token,self.csrf,rights_id=rights,work_id=work_id,kind='description',expected_version=0,request_id=str(uuid.uuid4()),
            decision=dict(state='allowed',officiality='not_applicable',match_state='not_applicable',display_allowed=True,cache_allowed=True,derivative_allowed=False,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={'license':'synthetic-owned'}))
        head=self.scalar('SELECT version FROM metadata_heads WHERE work_id=:id',id=work_id)
        return service.edit_description(token,self.csrf,work_id=work_id,text=body,rights_id=rights,expected_rights_version=1,expected_head_version=head,request_id=str(uuid.uuid4()))

    def session(self, token):
        with write_transaction(self.engine) as conn:
            conn.execute(text("""INSERT INTO auth_sessions(token_hash,csrf_hash,user_id,credential_version,created_at,expires_at)
                SELECT :token,:csrf,id,credential_version,:now,:end FROM users WHERE id='u'"""),
                dict(token=digest(token),csrf=digest(self.csrf),now=self.auth.now(),end=self.auth.now()+3600))
        return token

    def resolve(self, revision, *, token=None, policy='current', ticket_snapshot=None, positions=(0,), option_id=None):
        selection=AssetSelection(revision,positions=positions,option_id=option_id)
        with self.engine.connect() as conn:
            return self.policy.resolve_selection_in(conn,token or self.token,(selection,),policy=policy,ticket_snapshot=ticket_snapshot)

    def ticket(self, revision, token=None):
        token=token or self.token
        ident=str(uuid.uuid4())
        with self.engine.connect() as conn:
            options=conn.execute(text('SELECT id FROM revision_options WHERE edition_revision_id=:revision ORDER BY id'),dict(revision=revision)).scalars().all()
            selected=self.policy.resolve_selection_in(conn,token,tuple(AssetSelection(revision,option_id=option) for option in options))
        now=self.h.clock.wall
        # This fixture creates a trusted fixed snapshot; production issue service
        # additionally owns physical hashing and exact ZIP overhead in G2D.
        with self.publisher.locks.mutation([a.object_sha for a in selected.assets]), write_transaction(self.engine) as conn:
            self.policy.resolve_selection_in(conn,token,tuple(AssetSelection(revision,option_id=option) for option in options))
            actor=self.auth.current(conn,token)
            conn.execute(text("""INSERT INTO download_tickets(id,user_id,session_token_hash,credential_version,work_id,issued_at_ms,expires_at_ms,archive_bytes)
                VALUES(:id,:user,:session,:version,'w',:now,:end,:size)"""),dict(id=ident,user=actor['id'],session=digest(token),version=actor['credential_version'],now=now,end=now+120000,size=sum(a.byte_size for a in selected.assets)+4096))
            for option in selected.options:
                conn.execute(text('INSERT INTO download_ticket_options VALUES(:ticket,:ordering,:edition,:revision,:option,:format,:delivery,:coverage)'),
                    dict(ticket=ident,ordering=option.selection_index,edition=option.edition_id,revision=option.revision_id,option=option.option_id,format=option.format,delivery=option.delivery_kind,coverage=option.coverage_json))
            for asset in selected.assets:
                conn.execute(text('INSERT INTO download_ticket_assets VALUES(:ticket,:ordering,:ordinal,:revision,:position,:sha,:size,:mime,:format,:role,:name)'),
                    dict(ticket=ident,ordering=asset.selection_index,ordinal=asset.ordinal,revision=asset.revision_id,position=asset.asset_position,sha=asset.object_sha,size=asset.byte_size,mime=asset.detected_mime,format=asset.file_format,role=asset.asset_role,name=f'e/{asset.option_id}/{asset.ordinal}.{asset.file_format}'))
                conn.execute(text("INSERT OR IGNORE INTO object_pins VALUES('ticket',:id,:sha,:now)"),dict(id=ident,sha=asset.object_sha,now=now))
            conn.execute(text("UPDATE download_tickets SET snapshot_digest=:digest,state='issued' WHERE id=:id"),dict(digest=self.policy.ticket_digest_in(conn,ident),id=ident))
        with self.engine.connect() as conn:
            return self.policy.load_ticket_snapshot_in(conn,token,ident)

    def claim(self, snapshot):
        attempt=str(uuid.uuid4())
        with write_transaction(self.engine) as conn:
            self.policy.load_ticket_snapshot_in(conn,self.token,snapshot.ticket_id)
            conn.execute(text("INSERT INTO download_attempts(id,ticket_id,owner_instance,started_at_ms) VALUES(:id,:ticket,'synthetic-physical-owner',:now)"),dict(id=attempt,ticket=snapshot.ticket_id,now=self.auth.now()*1000))
            conn.execute(text("UPDATE download_tickets SET state='streaming',used_at_ms=:now WHERE id=:id"),dict(id=snapshot.ticket_id,now=self.auth.now()*1000))
        with self.engine.connect() as conn:
            return self.policy.load_ticket_snapshot_in(conn,self.token,snapshot.ticket_id,attempt_id=attempt)
