"""Synthetic upload actors and isolated databases shared by transport checks."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import uuid

from sqlalchemy import text

from document_library.auth import AuthService, digest
from document_library.database import database_engine, migrate, write_transaction
from document_library.uploads import UploadService

ROOT=Path(__file__).resolve().parents[1]


class UploadFixture:
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='document-library-upload-')
        self.path=Path(self.temp.name).resolve()
        self.engine=database_engine(self.path/'synthetic.sqlite3')
        self.addCleanup(self.cleanup)
        migrate(self.engine,ROOT/'migrations')
        self.now=1800000000
        self.auth=AuthService(self.engine,clock=lambda:self.now)
        self.uploads=UploadService(self.auth)
        self.admin=self.actor('admin',role='admin')
        self.uploader=self.actor('uploader',upload=True)
        self.member=self.actor('member')

    def cleanup(self):
        self.engine.dispose();self.temp.cleanup()
        self.assertFalse(self.path.exists())

    def actor(self, name, *, role='member', upload=False):
        actor=SimpleNamespace(id=str(uuid.uuid4()),token='synthetic-session-'+name,csrf='synthetic-csrf-'+name)
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO users(id,username,password_hash,role,created_at) VALUES(:id,:name,'synthetic-disabled',:role,'test')"),
                         dict(id=actor.id,name=name,role=role))
            conn.execute(text('INSERT INTO auth_sessions VALUES(:token,:csrf,:id,1,:now,:expiry)'),
                         dict(token=digest(actor.token),csrf=digest(actor.csrf),id=actor.id,now=self.now,expiry=self.now+10000))
            if upload:
                conn.execute(text("INSERT INTO user_permissions VALUES(:id,'content.upload',:admin,'test')"),dict(id=actor.id,admin=self.admin.id))
        return actor

    def batch(self, actor=None, *, total=100, key=None):
        actor=actor or self.uploader
        return self.uploads.create_batch(actor.token,actor.csrf,declared_total_bytes=total,
                                         request_key=key or str(uuid.uuid4()))['batch_id']

    def reserve(self, actor=None, *, total=100, length=3, filename='Book.txt', batch=None, key=None):
        actor=actor or self.uploader
        return self.uploads.reserve_file(actor.token,actor.csrf,batch_id=batch or self.batch(actor,total=total),
                                         filename=filename,declared_size=length,request_key=key or str(uuid.uuid4()))

    def scalar(self, sql, **params):
        with self.engine.connect() as conn:
            return conn.execute(text(sql),params).scalar_one()

    def revoke(self, actor=None, *, status='active', capability=False, version=True):
        actor=actor or self.uploader
        with write_transaction(self.engine) as conn:
            conn.execute(text('UPDATE users SET status=:status,authz_version=authz_version+:delta WHERE id=:id'),
                         dict(status=status,delta=int(version),id=actor.id))
            if not capability:
                conn.execute(text('DELETE FROM user_permissions WHERE user_id=:id'),dict(id=actor.id))
