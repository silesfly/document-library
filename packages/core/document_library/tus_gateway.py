"""Bounded authenticated tus adapter to one fixed loopback backend.

The backend receives server-made headers only. asyncio cancellation never proves
that a thread or tusd stopped. An owned controller's terminate+wait is the sole
reconciliation route for transport outcomes that remain unobserved.
"""
import asyncio
import base64
from dataclasses import dataclass
import fcntl
import http.client
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid

from fastapi.responses import Response

from .auth import AuthError
from .uploads import CHUNK_LIMIT, FILE_LIMIT, upload_uuid


def decimal(value):
    if not isinstance(value,str) or not re.fullmatch(r'0|[1-9][0-9]{0,10}',value):
        raise AuthError('invalid_tus_header',400)
    return int(value)


@dataclass(frozen=True)
class BackendResponse:
    status: int
    headers: tuple


class TusBackend:
    def __init__(self, port, *, timeout=30):
        if type(port) is not int or not 1 <= port <= 65535 or not 0 < timeout <= 60:
            raise ValueError('Explicit bounded loopback endpoint required')
        self.port,self.timeout=port,timeout
        self.identity=str(uuid.uuid4())
        self.origin=f'http://127.0.0.1:{port}'
        self.ownership_check=None
        self.request_gate=threading.Lock()
        self.admission_gate=threading.Lock()
        self.stop_requested=threading.Event()

    def location_id(self, location):
        if not isinstance(location,str) or len(location)>256:
            raise AuthError('invalid_backend_response',502)
        try:
            url=urlsplit(location)
        except ValueError:
            raise AuthError('invalid_backend_response',502) from None
        if (url.scheme,url.netloc) not in {('',''),('http',f'127.0.0.1:{self.port}')} or url.query or url.fragment:
            raise AuthError('invalid_backend_response',502)
        if not re.fullmatch(r'/files/[0-9a-f]{32}',url.path):
            raise AuthError('invalid_backend_response',502)
        return url.path.rsplit('/',1)[1]

    def request(self, method, tus_id, headers, body):
        # Own the lifecycle gate before checking identity or opening a socket.
        # stop() announces closure before waiting for this gate, so queued
        # requests cannot reach an endpoint after its generation was stopped.
        with self.request_gate:
            if self.stop_requested.is_set():
                raise AuthError('backend_stopping',503)
            if self.ownership_check is not None:
                self.ownership_check()
            if tus_id is not None and not re.fullmatch(r'[0-9a-f]{32}',tus_id):
                raise AuthError('invalid_backend_response',502)
            conn=http.client.HTTPConnection('127.0.0.1',self.port,timeout=self.timeout)
            try:
                conn.request(method,'/files/'+(tus_id or ''),body=body,headers=headers)
                response=conn.getresponse()
                if len(response.read(65537))>65536:
                    raise AuthError('invalid_backend_response',502)
                if self.ownership_check is not None:
                    self.ownership_check()
                return BackendResponse(response.status,tuple(response.getheaders()))
            finally:
                conn.close()


class OwnedTusd:
    """Explicit disposable/controller launch. Constructing a gateway starts none."""
    def __init__(self, binary, stage, port):
        stage=Path(stage)
        info=stage.lstat()
        if (not stage.is_absolute() or not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode)
                or stage.resolve()!=stage or stat.S_IMODE(info.st_mode)!=0o700 or info.st_uid!=os.getuid()):
            raise ValueError('Owned private staging directory required')
        self.stage=stage
        self.backend=TusBackend(port)
        self.process=None;self.stopped_evidence=None;self.stops={}
        self.dirfd=os.open(stage,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        self.lockfd=None
        try:
            self.lockfd=os.open('.tusd-lifecycle.lock',os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600,dir_fd=self.dirfd)
            lockinfo=os.fstat(self.lockfd)
            if (not stat.S_ISREG(lockinfo.st_mode) or lockinfo.st_nlink!=1
                    or stat.S_IMODE(lockinfo.st_mode)!=0o600 or lockinfo.st_uid!=os.getuid()):
                raise ValueError('Private lifecycle lock required')
            fcntl.flock(self.lockfd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            self.lock_identity=[lockinfo.st_dev,lockinfo.st_ino]
            previous=self._read_state()
            if previous:
                if previous.get('lock_identity')!=self.lock_identity:
                    raise ValueError('Lifecycle lock identity changed')
                self.stops=previous['stops']
                prior=upload_uuid(previous['backend_id'])
                if previous['status']!='stopped':
                    if previous['status']=='running' and previous.get('lock_inherited') is not True:
                        raise ValueError('Previous process ownership is unverified')
                    # A running tusd inherited this same flock. Acquiring it
                    # proves that generation no longer holds its process FD.
                    self.stops[prior]=str(uuid.uuid4())
            if len(self.stops)>256:
                raise ValueError('Lifecycle proof archive requires operator review')
            with socket.socket() as probe:
                probe.bind(('127.0.0.1',port))
            self._write_state('starting',False)
        except BaseException:
            self._close_ownership()
            raise
        self.argv=[str(binary),'-host','127.0.0.1','-port',str(port),'-base-path','/files/',
                   '-upload-dir',str(stage),'-dir-perms','700','-file-perms','600',
                   '-disable-download','-disable-concatenation','-disable-cors',
                   '-max-size',str(FILE_LIMIT),'-verbose=false','-show-startup-logs=false',
                   '-show-greeting=false','-network-timeout','30s','-request-completion-timeout','10s',
                   '-shutdown-timeout','5s']
        try:
            self.process=subprocess.Popen(self.argv,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                                          stderr=subprocess.DEVNULL,pass_fds=(self.lockfd,))
            ready=False
            for _ in range(300):
                if self.process.poll() is not None:
                    break
                if self._holds_lock() and self._owns_listener():
                    ready=True
                    break
                time.sleep(0.01)
            if not ready:
                raise ValueError('Owned tusd listener and inherited lock were not verified')
            self._write_state('running',True)
            self.backend.ownership_check=self._check_owned
        except BaseException:
            self.stop()
            raise

    def _read_state(self):
        try:
            fd=os.open('.tusd-owner.json',os.O_RDONLY|os.O_NOFOLLOW,dir_fd=self.dirfd)
        except FileNotFoundError:
            return None
        with os.fdopen(fd,'rb') as stream:
            info=os.fstat(stream.fileno())
            if stat.S_IMODE(info.st_mode)!=0o600 or info.st_uid!=os.getuid() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
                raise ValueError('Private lifecycle state required')
            raw=stream.read(65537)
        if len(raw)>65536:
            raise ValueError('Bounded lifecycle state required')
        state=json.loads(raw)
        if state.get('status') not in {'starting','running','stopped'} or not isinstance(state.get('stops'),dict):
            raise ValueError('Invalid lifecycle state')
        for ident,evidence in state['stops'].items():
            upload_uuid(ident);upload_uuid(evidence)
        return state

    def _write_state(self, status, inherited):
        state=dict(backend_id=self.backend.identity,status=status,lock_inherited=inherited,
                   lock_identity=self.lock_identity,stops=self.stops)
        name='.tusd-owner-'+str(uuid.uuid4())+'.tmp'
        fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=self.dirfd)
        try:
            with os.fdopen(fd,'wb') as stream:
                stream.write(json.dumps(state,sort_keys=True).encode());stream.flush();os.fsync(stream.fileno())
            os.replace(name,'.tusd-owner.json',src_dir_fd=self.dirfd,dst_dir_fd=self.dirfd)
            os.fsync(self.dirfd)
        finally:
            try:os.unlink(name,dir_fd=self.dirfd)
            except FileNotFoundError:pass

    def _holds_lock(self):
        if sys.platform.startswith('linux'):
            try:
                info=os.stat(f'/proc/{self.process.pid}/fd/{self.lockfd}')
                return [info.st_dev,info.st_ino]==self.lock_identity
            except FileNotFoundError:
                return False
        if sys.platform=='darwin':
            result=subprocess.run(['/usr/sbin/lsof','-nP','-a','-p',str(self.process.pid),'-d',str(self.lockfd),'-Fn'],
                                  stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,timeout=5)
            return 'n'+str(self.stage/'.tusd-lifecycle.lock') in result.stdout.splitlines()
        raise ValueError('Process ownership verification is unsupported on this host')

    def _owns_listener(self):
        if sys.platform.startswith('linux'):
            try:
                fds=Path(f'/proc/{self.process.pid}/fd')
                sockets={os.readlink(p) for p in fds.iterdir()}
                with open('/proc/net/tcp') as stream:
                    return any(parts[1]==f'0100007F:{self.backend.port:04X}' and parts[3]=='0A'
                               and 'socket:['+parts[9]+']' in sockets for parts in (line.split() for line in stream.readlines()[1:]))
            except FileNotFoundError:
                return False
        if sys.platform=='darwin':
            result=subprocess.run(['/usr/sbin/lsof','-nP','-a','-p',str(self.process.pid),
                                   '-iTCP@127.0.0.1:'+str(self.backend.port),'-sTCP:LISTEN','-Fn'],
                                  stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,timeout=5)
            return 'n127.0.0.1:'+str(self.backend.port) in result.stdout.splitlines()
        raise ValueError('Process ownership verification is unsupported on this host')

    def _check_owned(self):
        if self.process.poll() is not None or not self._holds_lock() or not self._owns_listener():
            raise AuthError('backend_ownership_lost',503)

    def _close_ownership(self):
        if self.lockfd is not None:
            os.close(self.lockfd);self.lockfd=None
        if self.dirfd is not None:
            os.close(self.dirfd);self.dirfd=None

    def stop(self):
        with self.backend.admission_gate:
            self.backend.stop_requested.set()
        with self.backend.request_gate:
            self._stop_owned()

    def _stop_owned(self):
        # No request can still be between ownership verification and connect,
        # or still own an HTTP exchange when process/flock evidence is retired.
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
        if self.process is not None:
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.stopped_evidence=str(uuid.uuid4())
        self.stops[self.backend.identity]=self.stopped_evidence
        if self.dirfd is not None:
            self._write_state('stopped',True)
            self._close_ownership()


class TusGateway:
    def __init__(self, uploads, backend):
        self.uploads,self.backend=uploads,backend

    def confirm_stopped(self, controller):
        if (not isinstance(controller,OwnedTusd) or controller.backend is not self.backend
                or not controller.stopped_evidence or controller.process.poll() is None):
            raise AuthError('backend_stop_unconfirmed',409)
        self.uploads._backend_stopped(self.backend.identity,controller.stopped_evidence)

    def confirm_previous_stopped(self, controller):
        if not isinstance(controller,OwnedTusd) or controller.backend is not self.backend:
            raise AuthError('backend_stop_unconfirmed',409)
        controller._check_owned()
        for identity,evidence in controller.stops.items():
            if identity!=self.backend.identity:
                self.uploads._backend_stopped(identity,evidence)

    @staticmethod
    def headers(raw):
        headers={}
        for key,value in raw:
            key=key.decode('latin-1').lower() if isinstance(key,bytes) else key.lower()
            value=value.decode('latin-1') if isinstance(value,bytes) else value
            if key in headers or len(key)>128 or len(value)>8192 or '\r' in value or '\n' in value:
                raise AuthError('invalid_tus_header',400)
            headers[key]=value
        forbidden={'authorization','forwarded','transfer-encoding','expect','x-http-method-override',
                   'x-method-override','x-http-method','upload-metadata','upload-concat','upload-defer-length'}
        if any(key in forbidden or key.startswith('x-forwarded-') or
               (key.startswith(('upload-','tus-')) and key not in {'upload-length','upload-offset','tus-resumable'}) for key in headers):
            raise AuthError('invalid_tus_header',400)
        if headers.get('tus-resumable')!='1.0.0':
            raise AuthError('tus_version_required',412)
        return headers

    async def handle(self, request, upload_id, token, csrf):
        upload_uuid(upload_id)
        method=request.method
        if method not in {'POST','HEAD','PATCH','DELETE'}:
            raise AuthError('method_rejected',405)
        if request.url.query:
            raise AuthError('invalid_tus_path',400)
        headers=self.headers(request.scope['headers'])
        row=self.uploads.authorize(token,csrf,upload_id)
        length=decimal(headers.get('content-length','0'))
        offset=None
        if method=='PATCH':
            if headers.get('content-type')!='application/offset+octet-stream' or 'upload-length' in headers:
                raise AuthError('invalid_tus_header',400)
            if not 1 <= length <= CHUNK_LIMIT:
                raise AuthError('chunk_size',413)
            offset=decimal(headers.get('upload-offset'))
        elif (length!=0 or 'upload-offset' in headers or headers.get('content-type')):
            raise AuthError('invalid_tus_header',400)
        if method=='POST':
            if decimal(headers.get('upload-length'))!=row['declared_size']:
                raise AuthError('upload_size_mismatch',400)
        elif 'upload-length' in headers:
            raise AuthError('invalid_tus_header',400)
        with self.uploads.ingress_guard():
            operation,row=self._begin(token,csrf,upload_id,method=method,offset=offset,length=length)
            try:
                body=bytearray()
                async for chunk in request.stream():
                    if len(body)+len(chunk)>length:
                        raise AuthError('chunk_size',413)
                    body.extend(chunk)
                if len(body)!=length:
                    raise AuthError('content_length_mismatch',400)
                return await self._execute(operation,row,token,csrf,method,bytes(body),offset)
            except BaseException:
                if operation is not None:
                    self.uploads.interrupted(operation)
                raise

    def _begin(self, token, csrf, upload_id, *, method, offset, length):
        # Stop announces under this short gate before waiting for physical
        # requests. Admission never waits on a thread's network exchange.
        with self.backend.admission_gate:
            if self.backend.stop_requested.is_set():
                raise AuthError('backend_stopping',503)
            return self.uploads.begin(token,csrf,upload_id,method=method,backend_id=self.backend.identity,
                                      offset=offset,length=length)

    async def transfer(self, token, csrf, upload_id, *, method, body=b'', offset=None):
        with self.uploads.ingress_guard():
            operation,row=self._begin(token,csrf,upload_id,method=method,offset=offset,length=len(body))
            return await self._execute(operation,row,token,csrf,method,body,offset)

    async def _execute(self, operation, row, token, csrf, method, body, offset):
        if operation is None:
            row=self.uploads.authorize(token,csrf,row['id'])
            return self._response(201,row['id'],row['received_size'],row['declared_size'])
        try:
            return await asyncio.to_thread(self._send,operation,row,token,csrf,method,body,offset)
        except (asyncio.CancelledError,AuthError):
            self.uploads.interrupted(operation)
            raise
        except Exception:
            self.uploads.interrupted(operation)
            raise AuthError('upload_transport_uncertain',503) from None

    def _send(self, operation, row, token, csrf, method, body, offset):
        self.uploads.sent(operation)
        headers={'Tus-Resumable':'1.0.0','Content-Length':str(len(body))}
        if method=='POST':
            headers['Upload-Length']=str(row['declared_size'])
            headers['Upload-Metadata']='upload_id '+base64.b64encode(row['id'].encode()).decode('ascii')
        elif method=='PATCH':
            headers.update({'Upload-Offset':str(offset),'Content-Type':'application/offset+octet-stream'})
        try:
            reply=self.backend.request(method,row['tus_id'],headers,body)
        except (OSError,http.client.HTTPException):
            raise AuthError('upload_transport_uncertain',503) from None
        response={}
        for key,value in reply.headers:
            key=key.lower()
            if key in {'tus-resumable','upload-offset','upload-length','location'}:
                if key in response:
                    raise AuthError('invalid_backend_response',502)
                response[key]=value
        expected={'POST':{201},'HEAD':{200,204},'PATCH':{204},'DELETE':{204}}
        if reply.status not in expected[method]:
            if reply.status in {400,404,409,413,423}:
                self.uploads.response(token,csrf,operation)
                raise AuthError('tus_request_rejected',reply.status)
            raise AuthError('upload_transport_uncertain',503)
        if response.get('tus-resumable')!='1.0.0':
            raise AuthError('invalid_backend_response',502)
        if 'upload-length' in response and decimal(response['upload-length'])!=row['declared_size']:
            raise AuthError('invalid_backend_response',502)
        tus_id=None;received=None
        if method=='POST':
            tus_id=self.backend.location_id(response.get('location'))
            if 'upload-offset' in response and decimal(response['upload-offset'])!=0:
                raise AuthError('invalid_backend_response',502)
            received=0
        elif method in {'HEAD','PATCH'}:
            received=decimal(response.get('upload-offset'))
            if not row['received_size']<=received<=row['declared_size']:
                raise AuthError('invalid_backend_response',502)
            if method=='HEAD' and decimal(response.get('upload-length'))!=row['declared_size']:
                raise AuthError('invalid_backend_response',502)
            if method=='PATCH' and received!=offset+len(body):
                raise AuthError('invalid_backend_response',502)
        self.uploads.response(token,csrf,operation,tus_id=tus_id,offset=received,deleted=method=='DELETE')
        return self._response(reply.status,row['id'],received,row['declared_size'])

    @staticmethod
    def _response(status, upload_id, offset, length):
        headers={'Tus-Resumable':'1.0.0','Cache-Control':'no-store'}
        if status==201:
            headers['Location']='/tus/'+upload_id
        if offset is not None:
            headers.update({'Upload-Offset':str(offset),'Upload-Length':str(length)})
        return Response(status_code=status,headers=headers)
