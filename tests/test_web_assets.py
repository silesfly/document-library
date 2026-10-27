"""Offline vendor/build and executable client boundary checks; no package install."""
import base64
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('web_asset_builder', ROOT / 'ops/build_web_assets.py')
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


class WebBuildAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import os
        configured = os.environ.get('NOVELLIBRARY_TEST_VENDOR_ARCHIVES')
        if not configured:
            raise unittest.SkipTest('Explicit pinned archive fixture not supplied')
        cls.archives = Path(configured)
        cls.temp = tempfile.TemporaryDirectory(prefix='novel-library-web-build-')
        cls.output = Path(cls.temp.name) / 'runtime'
        cls.manifest = BUILDER.build(ROOT / 'apps/web', cls.archives, cls.output)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'temp'):
            cls.temp.cleanup()

    def test_exact_pdf_core_worker_auxiliary_and_tus_bytes_and_licenses(self):
        self.assertEqual(self.manifest['dependencies']['packages']['pdfjs-dist']['version'], '6.4.299')
        for package, members in [('pdfjs-dist', {'build/pdf.mjs': 'vendor/pdfjs/build/pdf.mjs',
                                               'build/pdf.worker.mjs': 'vendor/pdfjs/build/pdf.worker.mjs',
                                               'LICENSE': 'vendor/pdfjs/LICENSE.txt'}),
                                 ('tus-js-client', {'dist/tus.min.js': 'vendor/tus/tus.min.js',
                                                    'LICENSE': 'vendor/tus/LICENSE.txt'})]:
            record = self.manifest['dependencies']['packages'][package]
            with tarfile.open(self.archives / record['archive']) as archive:
                for source, target in members.items():
                    self.assertEqual(archive.extractfile('package/' + source).read(), (self.output / target).read_bytes())
        for name, record in self.manifest['assets'].items():
            data = (self.output / name).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), record['sha256'])
            self.assertEqual(len(data), record['bytes'])
        self.assertTrue(any(name.endswith('.bcmap') for name in self.manifest['assets']))
        self.assertTrue(any(name.endswith('.wasm') for name in self.manifest['assets']))
        self.assertTrue(any(name.endswith('.ttf') for name in self.manifest['assets']))
        self.assertTrue(self.manifest['licenses'])
        for name in ('cards.js', 'filters.js', 'member.js'):
            self.assertIn(name, self.manifest['assets'])
            self.assertEqual((ROOT / 'apps/web' / name).read_bytes(), (self.output / name).read_bytes())
        self.assertFalse((ROOT / 'apps/web/vendor').exists())

    def test_output_never_reuses_directory_or_builds_inside_source(self):
        with self.assertRaises(ValueError):
            BUILDER.build(ROOT / 'apps/web', self.archives, self.output)
        with self.assertRaises(ValueError):
            BUILDER.build(ROOT / 'apps/web', self.archives, ROOT / 'apps/web/generated-not-created')
        self.assertFalse((ROOT / 'apps/web/generated-not-created').exists())

    def test_bad_archive_hash_and_symlink_input_fail_before_output_creation(self):
        base = Path(self.temp.name)
        with tempfile.TemporaryDirectory(dir=base) as folder:
            archives = Path(folder)
            for record in self.manifest['dependencies']['packages'].values():
                (archives / record['archive']).write_bytes(b'not the pinned package')
            with self.assertRaises(ValueError):
                BUILDER.build(ROOT / 'apps/web', archives, archives / 'bad-runtime')
            self.assertFalse((archives / 'bad-runtime').exists())
            (archives / 'alias').symlink_to(ROOT / 'apps/web', target_is_directory=True)
            with self.assertRaises(ValueError):
                BUILDER.build(archives / 'alias', self.archives, archives / 'alias-runtime')
            self.assertFalse((archives / 'alias-runtime').exists())

    def test_tar_traversal_link_duplicate_special_and_version_are_rejected(self):
        with tempfile.TemporaryDirectory(dir=self.temp.name) as folder:
            path = Path(folder) / 'synthetic.tgz'
            for bad in ['package/../escape.js', '/absolute.js', 'package/a\\b.js', 'package/a%2fb.js', 'link', 'duplicate', 'version']:
                with self.subTest(case=bad):
                    with tarfile.open(path, 'w:gz') as archive:
                        metadata = json.dumps({'name': 'pdfjs-dist', 'version': '0.0.0' if bad == 'version' else '6.4.299'}).encode()
                        info = tarfile.TarInfo('package/package.json'); info.size = len(metadata); archive.addfile(info, io.BytesIO(metadata))
                        if bad != 'version':
                            info = tarfile.TarInfo('package/link' if bad == 'link' else 'package/package.json' if bad == 'duplicate' else bad)
                            if bad == 'link':
                                info.type = tarfile.SYMTYPE; info.linkname = '/unread-target'
                            archive.addfile(info, io.BytesIO(b''))
                    raw = path.read_bytes()
                    spec = {'sha256': hashlib.sha256(raw).hexdigest(), 'integrity': 'sha512-' + base64.b64encode(hashlib.sha512(raw).digest()).decode(), 'version': '6.4.299'}
                    with self.assertRaises(ValueError):
                        BUILDER._archive(path, spec, 'pdfjs-dist')

    def test_public_brand_build_keeps_vendor_bytes_identical(self):
        output = Path(self.temp.name) / 'public-runtime'
        manifest = BUILDER.build(ROOT / 'apps/web', self.archives, output, brand='public')
        self.assertIn(b'Document Library', (output / 'index.html').read_bytes())
        self.assertNotIn(b'Novel Library', (output / 'config.js').read_bytes())
        for name, record in self.manifest['assets'].items():
            if name.startswith('vendor/'):
                self.assertEqual(record, manifest['assets'][name])

    def test_actual_service_worker_cache_routes_exclude_private_and_sensitive_query_urls(self):
        script = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const listeners = {}, stored = [], network = [], matches = [];
const cache = {addAll: async urls => stored.push(...urls), match: async request => {matches.push(typeof request === 'string' ? request : request.url); return 'STATIC';}};
const context = {URL, Promise, self: {location: {origin:'https://library.synthetic.invalid'}, clients:{claim:async()=>{}}, addEventListener:(name,fn)=>listeners[name]=fn}, caches:{open:async()=>cache,keys:async()=>[],delete:async()=>true}, fetch:async request=>{network.push(request.url);return 'NETWORK';}};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),context);
(async()=>{
 await new Promise((resolve,reject)=>listeners.install({waitUntil:p=>p.then(resolve,reject)}));
 assert(stored.includes('/offline.html'));
 assert(stored.every(x=>!x.includes('?')&&!x.startsWith('/api/')&&!x.startsWith('/auth/')));
 for(const path of ['/api/v1/items','/api/v1/items/w/preview','/api/v1/items/w/download','/api/v1/download-tickets/synthetic','/auth/login','/me','/assets/app.js?token=synthetic','/assets/app.js?invite=synthetic','/assets/app.js?ticket=synthetic']){
  let response;listeners.fetch({request:{method:'GET',url:'https://library.synthetic.invalid'+path,mode:'same-origin'},respondWith:p=>response=p});assert.equal(response,undefined,path);
 }
 let response;listeners.fetch({request:{method:'GET',url:'https://library.synthetic.invalid/assets/app.js',mode:'same-origin'},respondWith:p=>response=p});assert.equal(await response,'STATIC');
 const before=stored.length;response=null;listeners.fetch({request:{method:'GET',url:'https://library.synthetic.invalid/?reset=synthetic',mode:'navigate'},respondWith:p=>response=p});assert.equal(await response,'NETWORK');assert.equal(stored.length,before);
 assert(!stored.some(x=>x.includes('synthetic')));console.log('SERVICE_WORKER_PRIVATE_CACHE_EXCLUSION_PASS');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        done = subprocess.run([shutil.which('node'), '-e', script, str(self.output / 'sw.js')], text=True, capture_output=True, check=True)
        self.assertIn('SERVICE_WORKER_PRIVATE_CACHE_EXCLUSION_PASS', done.stdout)

    def test_real_api_module_same_origin_csrf_head_and_error_envelope(self):
        script = r'''
const assert = require('node:assert/strict');
(async()=>{
 global.location={origin:'https://library.synthetic.invalid'};
 global.document={cookie:'__Host-dl-csrf=synthetic-csrf'};
 let expired=0, call;
 global.window={dispatchEvent:()=>expired++};
 const api=await import(process.argv[1]);
 for(const path of ['https://outside.invalid/private','//outside.invalid/private','relative'])assert.throws(()=>api.sameOrigin(path));
 global.fetch=async (url,opts)=>{call={url,opts};return new Response('',{status:200});};
 await api.request('/tus/synthetic',{method:'HEAD',tus:true});
 assert.equal(call.opts.headers.get('X-CSRF-Token'),'synthetic-csrf');assert.equal(call.opts.cache,'no-store');assert.equal(call.opts.credentials,'same-origin');assert.equal(call.opts.redirect,'error');
 const controller=new AbortController();await api.request('/me',{raw:true,signal:controller.signal});assert.equal(call.opts.signal,controller.signal);
 global.fetch=async()=>new Response(JSON.stringify({error:{code:'AUTHENTICATION_REQUIRED',details:[]}}),{status:401,headers:{'Content-Type':'application/json'}});
 await assert.rejects(api.request('/api/v1/items'),error=>error.status===401&&error.code==='AUTHENTICATION_REQUIRED');assert.equal(expired,1);
 global.fetch=async()=>new Response(JSON.stringify({error:'permission_denied'}),{status:403,headers:{'Content-Type':'application/json'}});
 await assert.rejects(api.request('/admin/users'),error=>error.status===403&&error.code==='permission_denied');
 const query=api.apiPath('a/b','/preview',{cursor:'opaque+/=',categories:['x','y']});assert(query.startsWith('/api/v1/items/a%2Fb/preview?'));assert.equal(new URL(query,location.origin).searchParams.get('cursor'),'opaque+/=');
 console.log('API_BOUNDARY_PASS');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        done = subprocess.run([shutil.which('node'), '-e', script, (ROOT / 'apps/web/api.js').as_uri()], text=True, capture_output=True, check=True)
        self.assertIn('API_BOUNDARY_PASS', done.stdout)

    def test_upload_cancel_creation_ambiguity_and_abort_settlement(self):
        script = r'''
import assert from 'node:assert/strict';
class Element {
  constructor(tag='#text',text=''){this.tagName=tag;this.children=[];this.listeners={};this.attributes={};this.value='';this.disabled=false;this.hidden=false;this._text=text;this.classList={add(){},remove(){},toggle(){}};}
  append(...values){for(const value of values)this.children.push(value instanceof Element?value:new Element('#text',String(value)));}
  replaceChildren(...values){this.children=[];this._text='';this.append(...values);}
  setAttribute(key,value){this.attributes[key]=String(value);}
  addEventListener(name,fn){(this.listeners[name]||=[]).push(fn);}
  async emit(name,extra={}){for(const fn of this.listeners[name]||[])await fn({preventDefault(){},...extra});}
  get textContent(){return this._text+this.children.map(x=>x.textContent).join('');}
  set textContent(value){this._text=String(value);this.children=[];}
}
globalThis.Node=Element;
globalThis.location={origin:'https://library.synthetic.invalid'};
globalThis.document={cookie:'__Host-dl-csrf=synthetic-csrf',createElement:tag=>new Element(tag),createTextNode:value=>new Element('#text',value)};
globalThis.window={dispatchEvent(){}};
const {Lifetime}=await import(process.argv[1]+'/api.js');
const {uploadView,bindingForm}=await import(process.argv[1]+'/transfers.js');
const descendants=root=>[root,...root.children.flatMap(descendants)];
const button=(root,label)=>{const found=descendants(root).find(x=>x.tagName==='button'&&x.textContent===label);assert(found,label);return found;};
const defer=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
const flush=async()=>{for(let i=0;i<20;i++)await new Promise(r=>setImmediate(r));};
const uid='11111111-2222-4333-8444-555555555555',bid='66666666-7777-4888-8999-aaaaaaaaaaaa',reserved='/tus/'+uid;
const ctx=()=>({main:new Element('main'),me:{can_upload:true},life:new Lifetime(),errors:[],fail(error){this.errors.push(error);}});
async function selectFile(context){const input=descendants(context.main).find(x=>x.tagName==='input'&&x.attributes.type==='file');input.files=[new File(['abc'],'fixture.txt')];await input.emit('change');await flush();}
const created=()=>new Response(null,{status:201,headers:{Location:reserved,'Tus-Resumable':'1.0.0'}});
const headed=()=>new Response(null,{status:204,headers:{'Upload-Offset':'0','Upload-Length':'3','Tus-Resumable':'1.0.0'}});
const missing=()=>Response.json({error:'upload_not_created'},{status:409});
let cases=0;
// A real uncreated reservation rejects DELETE. Only POST+DELETE releases its slot.
{
 const context=ctx(),pending=defer(),methods=[];let exists=false,slots=1;
 globalThis.fetch=async(url,opts)=>{methods.push(opts.method+' '+url.pathname);if(url.pathname==='/uploads/batches')return Response.json({batch_id:bid});if(url.pathname.endsWith('/files'))return pending.promise;if(opts.method==='DELETE'){if(!exists)return missing();slots=0;return new Response(null,{status:204});}if(opts.method==='POST'){exists=true;return created();}throw Error('Unexpected request');};
 await uploadView(context);await selectFile(context);await button(context.main,'취소').emit('click');assert(context.main.textContent.includes('서버 정리 확인 중'));assert(!context.main.textContent.includes('서버 전송 취소 확인'));
 pending.resolve(Response.json({upload_id:uid,url:reserved}));await flush();assert.deepEqual(methods.filter(x=>x.endsWith(reserved)),['DELETE '+reserved,'POST '+reserved,'DELETE '+reserved]);assert.equal(slots,0);assert(context.main.textContent.includes('서버 전송 취소 확인'));assert.equal(context.errors.length,0);context.life.dispose();cases++;
}
// A cancel during POST waits for the creation outcome, then deletes that reservation.
{
 const context=ctx(),pending=defer(),methods=[];
 globalThis.fetch=async(url,opts)=>{methods.push(opts.method+' '+url.pathname);if(url.pathname==='/uploads/batches')return Response.json({batch_id:bid});if(url.pathname.endsWith('/files'))return Response.json({upload_id:uid,url:reserved});if(opts.method==='POST')return pending.promise;if(opts.method==='DELETE')return new Response(null,{status:204});throw Error('Unexpected request');};
 await uploadView(context);await selectFile(context);await button(context.main,'취소').emit('click');await flush();assert(!methods.includes('DELETE '+reserved));pending.resolve(created());await flush();assert.equal(methods.filter(x=>x==='DELETE '+reserved).length,1);assert(context.main.textContent.includes('서버 전송 취소 확인'));context.life.dispose();cases++;
}
// A lost creation response resumes by HEAD on exactly the same reservation.
{
 const context=ctx(),methods=[];let starts=0;
 globalThis.fetch=async(url,opts)=>{methods.push(opts.method+' '+url.pathname);if(url.pathname==='/uploads/batches')return Response.json({batch_id:bid});if(url.pathname.endsWith('/files'))return Response.json({upload_id:uid,url:reserved});if(opts.method==='POST')throw new TypeError('Synthetic lost creation reply');if(opts.method==='HEAD')return headed();throw Error('Unexpected request');};
 globalThis.tus={Upload:class{constructor(file,options){this.options=options;}start(){starts++;this.options.onSuccess();}async abort(){}}};
 await uploadView(context);await selectFile(context);assert.equal(starts,0);await button(context.main,'재개').emit('click');await flush();assert.equal(starts,1);assert.equal(methods.filter(x=>x==='POST '+reserved).length,1);assert.equal(methods.filter(x=>x.endsWith('/files')).length,1);assert(methods.filter(x=>x==='HEAD '+reserved).length>=1);context.life.dispose();cases++;
}
// No cleanup or new stream starts before an existing abort settles, even if an old success arrives.
{
 const context=ctx(),stopped=defer(),methods=[];let starts=0,oldOptions;
 globalThis.fetch=async(url,opts)=>{methods.push(opts.method+' '+url.pathname);if(url.pathname==='/uploads/batches')return Response.json({batch_id:bid});if(url.pathname.endsWith('/files'))return Response.json({upload_id:uid,url:reserved});if(opts.method==='POST')return created();if(opts.method==='HEAD')return headed();if(opts.method==='DELETE')return new Response(null,{status:204});throw Error('Unexpected request');};
 globalThis.tus={Upload:class{constructor(file,options){oldOptions=options;}start(){starts++;}abort(){return stopped.promise;}}};
 await uploadView(context);await selectFile(context);assert.equal(starts,1);await button(context.main,'일시정지').emit('click');await button(context.main,'재개').emit('click');await button(context.main,'취소').emit('click');oldOptions.onSuccess();await flush();assert.equal(starts,1);assert(!methods.includes('DELETE '+reserved));assert(!context.main.textContent.includes('서버 전송 취소 확인'));stopped.resolve();await flush();assert.equal(methods.filter(x=>x==='DELETE '+reserved).length,1);assert.equal(starts,1);assert(context.main.textContent.includes('서버 전송 취소 확인'));oldOptions.onProgress(3,3);assert(context.main.textContent.includes('서버 전송 취소 확인'));context.life.dispose();cases++;
}
// An uncertain/busy deletion stays pending rather than showing successful cleanup.
{
 const context=ctx();
 globalThis.fetch=async(url,opts)=>{if(url.pathname==='/uploads/batches')return Response.json({batch_id:bid});if(url.pathname.endsWith('/files'))return Response.json({upload_id:uid,url:reserved});if(opts.method==='POST')return created();if(opts.method==='HEAD')return headed();if(opts.method==='DELETE')return Response.json({error:'upload_busy'},{status:423});throw Error('Unexpected request');};
 globalThis.tus={Upload:class{constructor(file,options){this.options=options;}start(){}async abort(){}}};await uploadView(context);await selectFile(context);await button(context.main,'취소').emit('click');await flush();assert(context.main.textContent.includes('취소 확인 보류'));assert(!context.main.textContent.includes('서버 전송 취소 확인'));context.life.dispose();cases++;
}
// Even a retained old DOM button cannot bind after a different work is selected.
{
 const context=ctx(),late=defer();let binds=0;
 globalThis.fetch=async(url,opts)=>{if(url.pathname.endsWith('/works'))return Response.json({works:[{work_id:'a',title_display:'A'},{work_id:'b',title_display:'B'}],next_cursor:null});if(url.pathname.endsWith('/a/editions')&&url.searchParams.has('cursor'))return late.promise;if(url.pathname.endsWith('/a/editions'))return Response.json({editions:[{work_id:'a',edition_id:'ea',edition_label:'EA',representation:'text',active_revision_id:null}],next_cursor:'a-next'});if(url.pathname.endsWith('/b/editions'))return Response.json({editions:[{work_id:'b',edition_id:'eb',edition_label:'EB',representation:'text',active_revision_id:null}],next_cursor:null});if(opts?.method==='POST'){binds++;return Response.json({});}throw Error('Unexpected request');};
 const root=bindingForm(context,uid,true);await button(root,'작품 검색').emit('click');await button(root,'A').emit('click');await flush();const oldButton=button(root,'EA · text · 초판');const nextRequest=button(root,'다음 판본').emit('click');await flush();await button(root,'B').emit('click');await flush();late.resolve(Response.json({editions:[{work_id:'a',edition_id:'ea2',edition_label:'EA2',representation:'text',active_revision_id:null}],next_cursor:'old-next'}));await nextRequest;await flush();await oldButton.emit('click');assert.equal(binds,0);assert(root.textContent.includes('선택 작품: B'));assert(root.textContent.includes('EB · text · 초판'));assert(!root.textContent.includes('EA2'));assert(button(root,'다음 판본').hidden);context.life.dispose();cases++;
}
console.log('OWNERSHIP_SETTLEMENT_CASES_PASS='+cases);
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True, check=True)
        self.assertIn('OWNERSHIP_SETTLEMENT_CASES_PASS=6', done.stdout)

    def test_builder_fifo_and_dirfd_root_and_descendant_replacement(self):
        import multiprocessing
        import os
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='novel-library-output-race-') as folder:
            outer = Path(folder)
            fifo = outer / 'blocked.tgz'
            os.mkfifo(fifo)
            receive, send = multiprocessing.Pipe(duplex=False)
            def read_fifo():
                try:
                    BUILDER._regular(fifo)
                except (OSError, ValueError):
                    send.send('rejected')
                finally:
                    send.close()
            child = multiprocessing.get_context('fork').Process(target=read_fifo)
            child.start()
            send.close()
            try:
                self.assertTrue(receive.poll(1), 'builder regular read blocked on FIFO')
                self.assertEqual(receive.recv(), 'rejected')
            finally:
                if child.is_alive():
                    child.terminate()
                child.join(5)
                self.assertFalse(child.is_alive())
                receive.close()
            for boundary in ('root', 'descendant'):
                with self.subTest(boundary=boundary):
                    output = outer / ('output-' + boundary)
                    escaped = outer / ('unadmitted-' + boundary)
                    escaped.mkdir()
                    mkdir = os.mkdir
                    exchanged = False
                    def replace_created(name, *args, dir_fd=None, **kwargs):
                        nonlocal exchanged
                        result = mkdir(name, *args, dir_fd=dir_fd, **kwargs)
                        match = name == output.name if boundary == 'root' else name == 'cmaps'
                        if dir_fd is not None and match and not exchanged:
                            exchanged = True
                            os.rename(name, name + '-displaced', src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
                            os.symlink(str(escaped), name, dir_fd=dir_fd, target_is_directory=True)
                        return result
                    with patch.object(BUILDER, '_archive', return_value={'cmaps/public.bcmap': b'public fixture'}), patch.object(BUILDER.os, 'mkdir', replace_created):
                        with self.assertRaises(ValueError):
                            BUILDER.build(ROOT / 'apps/web', self.archives, output)
                    self.assertTrue(exchanged)
                    self.assertEqual(list(escaped.iterdir()), [])


    def test_double_slash_outputs_are_rejected_before_mkdir(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='novel-library-anchor-') as folder:
            repository = Path(folder) / 'repository'
            source = repository / 'apps/web'
            shutil.copytree(ROOT / 'apps/web', source)
            before = {path.relative_to(repository): path.read_bytes()
                      for path in repository.rglob('*') if path.is_file()}
            for destination in (repository / 'runtime', source / 'runtime'):
                with self.subTest(destination=destination):
                    alias = Path('/' + str(destination))
                    self.assertEqual(alias.anchor, '//')
                    with patch.object(BUILDER.os, 'mkdir', wraps=BUILDER.os.mkdir) as mkdir:
                        with self.assertRaisesRegex(ValueError, 'new absolute external output'):
                            BUILDER.build(source, self.archives, alias,
                                          repository_root=repository)
                    mkdir.assert_not_called()
                    self.assertFalse(destination.exists())
            after = {path.relative_to(repository): path.read_bytes()
                     for path in repository.rglob('*') if path.is_file()}
            self.assertEqual(after, before)
            with patch.object(BUILDER.os, 'open', wraps=BUILDER.os.open) as opened:
                with self.assertRaises(ValueError):
                    with BUILDER._directory_fd(Path('/' + str(source))):
                        self.fail('Double-slash directory was admitted')
            opened.assert_not_called()

    def test_custom_source_requires_explicit_repository_and_preserves_all_boundaries(self):
        with tempfile.TemporaryDirectory(prefix='novel-library-repository-') as folder:
            outer = Path(folder)
            repository = outer / 'repository'
            source = repository / 'ui-source'
            shutil.copytree(ROOT / 'apps/web', source)
            output = outer / 'runtime'
            with self.assertRaisesRegex(ValueError, 'repository_root'):
                BUILDER.build(source, self.archives, output)
            self.assertFalse(output.exists())
            for destination in (repository / 'generated', source / 'generated',
                                ROOT / 'generated-not-created'):
                with self.subTest(destination=destination):
                    with self.assertRaises(ValueError):
                        BUILDER.build(source, self.archives, destination,
                                      repository_root=repository)
                    self.assertFalse(destination.exists())
            wrong_repository = outer / 'unrelated'
            wrong_repository.mkdir()
            with self.assertRaises(ValueError):
                BUILDER.build(source, self.archives, output,
                              repository_root=wrong_repository)
            self.assertFalse(output.exists())
            repository_alias = outer / 'repository-alias'
            repository_alias.symlink_to(repository, target_is_directory=True)
            with self.assertRaises(ValueError):
                BUILDER.build(source, self.archives, output,
                              repository_root=repository_alias)
            self.assertFalse(output.exists())
            source_alias = repository / 'source-alias'
            source_alias.symlink_to(source, target_is_directory=True)
            with self.assertRaises(ValueError):
                BUILDER.build(source_alias, self.archives, output,
                              repository_root=repository)
            self.assertFalse(output.exists())
            manifest = BUILDER.build(source, self.archives, output,
                                     repository_root=repository)
            self.assertEqual((output / 'index.html').read_bytes(),
                             (source / 'index.html').read_bytes())
            self.assertEqual(manifest['brand'], 'private')


class WebBuildPhysicalAliasBoundary(unittest.TestCase):
    def test_physical_root_and_ancestor_aliases_reject_before_mkdir_with_external_control(self):
        # Same-inode spelling model only: no actual case-insensitive filesystem,
        # symlink, rename or mount is involved in this regression.
        from contextlib import contextmanager
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='novel-library-physical-alias-') as folder:
            outer = Path(folder)
            repository = outer / 'DeclaredRepository'
            source = repository / 'WebSource'
            shutil.copytree(ROOT / 'apps/web', source)
            project = outer / 'CoLocatedProject'
            (project / 'ops').mkdir(parents=True)
            external = outer / 'External'
            external.mkdir()
            directory = BUILDER._directory_fd
            for label, physical, forbidden in [('source', source, True),
                                                ('repository', repository, True),
                                                ('project', project, True),
                                                ('external', external, False)]:
                (physical / 'nested').mkdir()
                for nested in (False, True):
                    with self.subTest(boundary=label, nested=nested):
                        alias = outer / (label.upper() + '-ALIAS')
                        requested_parent = alias / 'nested' if nested else alias
                        actual_parent = physical / 'nested' if nested else physical
                        output = requested_parent / 'runtime'
                        admissions = []
                        @contextmanager
                        def physical_alias(path):
                            path = Path(path)
                            selected = physical / path.relative_to(alias) if path == alias or alias in path.parents else path
                            with directory(selected) as descriptor:
                                if path == requested_parent:
                                    admissions.append(BUILDER.os.fstat(descriptor).st_ino)
                                yield descriptor
                        with patch.object(BUILDER, '__file__', str(project / 'ops/build_web_assets.py')), \
                                patch.object(BUILDER, '_directory_fd', physical_alias), \
                                patch.object(BUILDER, '_archive', return_value={'LICENSE': b'Public synthetic fixture'}), \
                                patch.object(BUILDER.os, 'mkdir', wraps=BUILDER.os.mkdir) as mkdir:
                            if forbidden:
                                with self.assertRaisesRegex(ValueError, 'physically outside'):
                                    BUILDER.build(source, outer, output, repository_root=repository)
                                mkdir.assert_not_called()
                            else:
                                result = BUILDER.build(source, outer, output, repository_root=repository)
                                self.assertEqual(result['schema'], 'novel-library-web-assets-v1')
                        self.assertTrue(admissions, 'Physical alias was never admitted')
                        self.assertTrue(all(inode == actual_parent.stat().st_ino for inode in admissions))
                        self.assertEqual((actual_parent / output.name).exists(), not forbidden)


if __name__ == '__main__':
    unittest.main()
