"""Actual opt-in tusd process tests, never substituted by a fake backend.

Set TUSD_BINARY to the explicitly installed executable. Missing opt-in is a
visible skip. Invalid explicit paths fail. All accounts/data/ports are synthetic.
"""
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import time
import signal
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from document_library.api import create_app, SESSION_COOKIE
from document_library.auth import AuthError
from document_library.permissions import PermissionService
from document_library.tus_gateway import OwnedTusd, TusGateway
from upload_helpers import UploadFixture


@unittest.skipUnless(os.environ.get('TUSD_BINARY'),'TUSD_BINARY is not set: actual tusd integration NOT_RUN')
class TusdIntegration(UploadFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.controllers=[];self.clients=[]
        self.addCleanup(self.cleanup_transports)
        self.stage=self.path/'private-tus';self.stage.mkdir(mode=0o700)
        self.restart()
        self.batch_id=self.batch(total=3)
        self.upload=self.reserve(batch=self.batch_id)['upload_id']

    def cleanup_transports(self):
        for client in self.clients:client.close()
        for controller in reversed(self.controllers):
            controller.stop()
            self.assertIsNotNone(controller.process.returncode)

    @staticmethod
    def port():
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',0));return probe.getsockname()[1]

    def restart(self):
        binary=Path(os.environ['TUSD_BINARY'])
        self.assertTrue(binary.is_absolute() and binary.is_file() and os.access(binary,os.X_OK))
        self.controller=OwnedTusd(binary,self.stage,self.port())
        self.controllers.append(self.controller)
        self.gateway=TusGateway(self.uploads,self.controller.backend)
        self.client=TestClient(create_app(self.auth,origin='https://library.test',uploads=self.uploads,tus=self.gateway),
                               base_url='https://library.test')
        self.clients.append(self.client)

    def request(self, method, *, upload=None, actor=None, body=b'', offset=None, headers=None):
        actor=actor or self.uploader
        values={'Origin':'https://library.test','Tus-Resumable':'1.0.0','X-CSRF-Token':actor.csrf,
                'Cookie':SESSION_COOKIE+'='+actor.token}
        if method=='POST':values['Upload-Length']='3'
        if method=='PATCH':values.update({'Content-Type':'application/offset+octet-stream','Upload-Offset':str(offset)})
        values.update(headers or {})
        return self.client.request(method,'/tus/'+(upload or self.upload),headers=values,content=body)

    def create(self):
        response=self.request('POST');self.assertEqual(response.status_code,201,response.text)
        self.assertEqual(response.headers['location'],'/tus/'+self.upload)
        return self.scalar('SELECT tus_id FROM upload_sessions WHERE id=:id',id=self.upload)

    def test_actual_post_partial_head_remaining_old_offset_delete_and_private_stage(self):
        ident=self.create()
        metadata=json.loads((self.stage/(ident+'.info')).read_text())
        self.assertEqual(metadata['MetaData'],{'upload_id':self.upload})
        self.assertEqual(self.request('HEAD',actor=self.admin).status_code,403)
        self.assertEqual(self.request('HEAD',headers={'X-CSRF-Token':''}).status_code,403)
        self.assertEqual(self.request('PATCH',body=b'a',offset=0).status_code,204)
        self.assertEqual(self.request('HEAD').headers['upload-offset'],'1')
        self.assertEqual(self.request('PATCH',body=b'z',offset=0).status_code,409)
        self.assertEqual((self.stage/ident).read_bytes(),b'a')
        self.assertEqual(self.request('PATCH',body=b'bc',offset=1).status_code,204)
        self.assertEqual((self.stage/ident).read_bytes(),b'abc')
        with self.engine.connect() as conn:PermissionService(self.auth).check_upload_in(conn,self.upload)
        response=self.controller.backend.request('GET',ident,{'Tus-Resumable':'1.0.0'},b'')
        self.assertEqual(response.status,405)
        self.assertEqual(stat.S_IMODE(self.stage.stat().st_mode),0o700)
        for child in self.stage.iterdir():
            if child.is_file():self.assertEqual(stat.S_IMODE(child.stat().st_mode),0o600)
        self.assertEqual(self.request('DELETE').status_code,204)
        self.assertFalse((self.stage/ident).exists())

    def test_actual_revoke_between_chunks_and_after_full_receive(self):
        ident=self.create()
        self.assertEqual(self.request('PATCH',body=b'a',offset=0).status_code,204)
        self.revoke()
        self.assertEqual(self.request('PATCH',body=b'bc',offset=1).status_code,403)
        self.assertEqual(self.request('HEAD').status_code,403)
        self.assertEqual(self.request('DELETE').status_code,403)
        self.assertEqual((self.stage/ident).read_bytes(),b'a')
        other=self.reserve(self.admin)['upload_id']
        self.assertEqual(self.request('POST',actor=self.admin,upload=other).status_code,201)
        self.assertEqual(self.request('PATCH',actor=self.admin,upload=other,body=b'abc',offset=0).status_code,204)
        self.revoke(self.admin,status='suspended')
        with self.engine.connect() as conn,self.assertRaises(AuthError):
            PermissionService(self.auth).check_upload_in(conn,other)
        self.assertEqual(self.scalar('SELECT count(*) FROM edition_revisions'),0)

    def test_lost_patch_reply_actual_kill_takeover_keeps_new_generation_slot_and_resumes(self):
        ident=self.create();self.assertEqual(self.request('PATCH',body=b'a',offset=0).status_code,204)
        old=self.controller;old_gateway=self.gateway;real=old.backend.request
        def lose_reply(*args,**kwargs):
            real(*args,**kwargs)
            raise OSError('synthetic lost gateway receipt after actual HTTP exchange')
        with patch.object(old.backend,'request',side_effect=lose_reply):
            self.assertEqual(self.request('PATCH',body=b'b',offset=1).status_code,503)
        self.assertEqual((self.stage/ident).read_bytes(),b'ab')
        self.assertEqual(self.scalar('SELECT received_size FROM upload_sessions WHERE id=:id',id=self.upload),1)
        self.assertEqual(self.request('HEAD').status_code,423)
        with self.assertRaises(AuthError):old_gateway.confirm_stopped(old)
        # Simulate controller loss without writing a false stop receipt. The
        # actual child is killed/reaped; the new owner must prove same lock.
        old.process.kill();old.process.wait(timeout=10);old._close_ownership()
        self.restart()
        self.assertNotEqual(self.controller.backend.identity,old.backend.identity)
        other=self.reserve(self.admin)['upload_id']
        new_operation,_row=self.uploads.begin(self.admin.token,self.admin.csrf,other,method='POST',backend_id=self.controller.backend.identity)
        self.gateway.confirm_previous_stopped(self.controller)
        self.assertEqual(self.scalar('SELECT state FROM upload_transport_ops WHERE id=:id',id=new_operation),'reserved')
        self.assertEqual(self.scalar("SELECT count(*) FROM upload_transport_ops WHERE backend_id=:id AND state='uncertain'",id=old.backend.identity),0)
        self.uploads.interrupted(new_operation)
        self.assertEqual(self.request('HEAD').headers['upload-offset'],'2')
        self.assertEqual(self.request('PATCH',body=b'c',offset=2).status_code,204)
        self.assertEqual((self.stage/ident).read_bytes(),b'abc')

    def test_lost_creation_reply_requires_actual_stop_and_new_reservation_without_overwrite(self):
        real=self.controller.backend.request
        def lose_reply(*args,**kwargs):
            real(*args,**kwargs);raise OSError('synthetic creation reply loss')
        with patch.object(self.controller.backend,'request',side_effect=lose_reply):
            self.assertEqual(self.request('POST').status_code,503)
        before=set(self.stage.glob('*.info'));self.assertEqual(len(before),1)
        self.assertEqual(self.request('POST').status_code,423)
        old=self.controller;old_gateway=self.gateway
        old.stop();old_gateway.confirm_stopped(old)
        self.assertEqual(self.scalar('SELECT status FROM upload_sessions WHERE id=:id',id=self.upload),'expired')
        self.restart()
        self.upload=self.reserve(batch=self.batch_id)['upload_id']
        self.create()
        self.assertTrue(before<set(self.stage.glob('*.info')))
        self.assertEqual(len(list(self.stage.glob('*.info'))),2)

    def test_stage_lock_and_existing_port_never_adopt_another_listener(self):
        with self.assertRaises((BlockingIOError,ValueError)):
            OwnedTusd(os.environ['TUSD_BINARY'],self.stage,self.port())
        other=self.path/'other-stage';other.mkdir(mode=0o700)
        with self.assertRaises(OSError):
            OwnedTusd(os.environ['TUSD_BINARY'],other,self.controller.backend.port)
        self.assertIsNone(self.controller.process.poll())
        self.create()

    def test_actual_controller_parent_exit_leaves_child_lock_until_verified_child_exit(self):
        stage=self.path/'orphan-stage';stage.mkdir(mode=0o700)
        script='''import json,os,subprocess,sys
from document_library.tus_gateway import OwnedTusd
owned=OwnedTusd(sys.argv[1],sys.argv[2],int(sys.argv[3]))
fingerprint=subprocess.check_output(['/bin/ps','-p',str(owned.process.pid),'-o','lstart=','-o','comm='],text=True).strip()
record=json.dumps(dict(pid=owned.process.pid,fingerprint=fingerprint))
fd=os.open(os.path.join(sys.argv[2],'controller-child.json'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as stream:stream.write(record)
print(record,flush=True)
sys.stdin.read(1)
os._exit(91)
'''
        argv=[sys.executable,'-B','-c',script,os.environ['TUSD_BINARY'],str(stage),str(self.port())]
        parent=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        child=None
        def fingerprint(pid):
            result=subprocess.run(['/bin/ps','-p',str(pid),'-o','lstart=','-o','comm='],
                                  text=True,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5)
            return result.stdout.strip()
        try:
            stdout,stderr=parent.communicate(input='x',timeout=60)
            child=json.loads(stdout)
            self.assertEqual(parent.returncode,91,stderr)
            self.assertTrue(child['fingerprint'])
            self.assertEqual(fingerprint(child['pid']),child['fingerprint'])
            # The controller's kernel FD is gone. The living tusd child alone
            # must retain the inherited lifecycle lock and prevent takeover.
            with self.assertRaises(BlockingIOError):
                OwnedTusd(os.environ['TUSD_BINARY'],stage,self.port())
        finally:
            if parent.poll() is None:
                parent.kill();parent.wait(timeout=10)
            if child is None and (stage/'controller-child.json').exists():
                child=json.loads((stage/'controller-child.json').read_text())
            if child is not None and fingerprint(child['pid'])==child['fingerprint']:
                os.kill(child['pid'],signal.SIGTERM)
                for _ in range(500):
                    if fingerprint(child['pid'])!=child['fingerprint']:break
                    time.sleep(0.01)
                self.assertNotEqual(fingerprint(child['pid']),child['fingerprint'])
        # Same inode takeover supplies stop evidence for the dead generation.
        # This new direct child is terminated and wait()ed by normal cleanup.
        recovered=OwnedTusd(os.environ['TUSD_BINARY'],stage,self.port())
        self.controllers.append(recovered)
        self.assertTrue(recovered.stops)


if __name__=='__main__':
    unittest.main()
