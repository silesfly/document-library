"""Executable member/query contracts without a second DOM simulator."""
from pathlib import Path
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class MemberClientContracts(unittest.TestCase):
    def test_password_rejection_keeps_forms_and_session_until_a_real_expiry(self):
        script = r'''
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const existing=readFileSync(new URL('../../tests/test_web_assets.py',process.argv[1]+'/'),'utf8');
const start=existing.indexOf('class Element {'),end=existing.indexOf('const {Lifetime}',start);assert(start>=0&&end>start);
await import('data:text/javascript,'+encodeURIComponent(existing.slice(start,end)));
// Supply only the dialog methods missing from the shared fixture.
document.body=document.createElement('body');document.activeElement=null;
Node.prototype.showModal=function(){this.open=true;};Node.prototype.close=function(){this.open=false;};
Node.prototype.remove=function(){document.body.children=document.body.children.filter(node=>node!==this);};
const {request,Lifetime,errorText}=await import(process.argv[1]+'/api.js');
const {passwordChange}=await import(process.argv[1]+'/auth.js');
const {adminView}=await import(process.argv[1]+'/admin.js');
const descendants=root=>[root,...root.children.flatMap(descendants)];
const find=(root,predicate)=>{const value=descendants(root).find(predicate);assert(value);return value;};
const findButton=(root,label)=>find(root,node=>node.tagName==='button'&&node.textContent===label);
let expired=0,current;
window.dispatchEvent=event=>{assert.equal(event.type,'session-expired');expired++;current?.life.dispose();current?.main.replaceChildren();document.body.replaceChildren();};
function context(){return current={main:document.createElement('main'),params:new URLSearchParams(),me:{role:'admin'},life:new Lifetime(),errors:[],routes:[],clears:0,fail(error){this.errors.push(error);},clearSession(){this.clears++;},go(...args){this.routes.push(args);}};}
const rejected=()=>Response.json({error:'invalid_credentials'},{status:401});
{
 const ctx=context(),form=passwordChange(ctx);ctx.main.append(form);const inputs=descendants(form).filter(node=>node.tagName==='input'),submit=findButton(form,'변경 후 다시 로그인');let attempts=0;
 globalThis.fetch=async url=>{assert.equal(url.pathname,'/auth/change-password');return ++attempts===1?rejected():new Response(null,{status:204});};
 inputs[0].value='synthetic-rejected';inputs[1].value='synthetic-new-value';await form.emit('submit');
 assert.equal(expired,0);assert(!ctx.life.disposed);assert.equal(ctx.main.children[0],form);assert.equal(ctx.clears,0);assert.deepEqual(ctx.routes,[]);assert(!submit.disabled);assert.equal(ctx.errors.length,1);assert(!errorText(ctx.errors[0]).includes('만료'));assert(errorText(ctx.errors[0]).includes('비밀번호'));assert(inputs.every(input=>input.value===''));
 inputs[0].value='synthetic-confirmed';inputs[1].value='synthetic-new-value';await form.emit('submit');assert.equal(attempts,2);assert.equal(ctx.clears,1);assert.deepEqual(ctx.routes,[['login']]);assert.equal(expired,0);ctx.life.dispose();
}
{
 const ctx=context(),commits=[];globalThis.fetch=async(url,options)=>{
  if(url.pathname==='/admin/users')return Response.json({users:[{id:'member',username:'synthetic-member',role:'member',can_upload:false,status:'active',authz_version:3}]});
  if(url.pathname==='/admin/user-permissions/bulk-preview')return Response.json({eligible_count:1,impact:{upload_sessions_to_block:0},expires_at:2000000000,targets:[{username:'synthetic-member',authz_version:3}],preview_token:'synthetic-preview',action:'grant',reason:'synthetic reason'});
  assert.equal(url.pathname,'/admin/user-permissions/bulk');commits.push(options);return commits.length===1?rejected():new Response(null,{status:204});
 };
 await adminView(ctx);const selected=find(ctx.main,node=>node.attributes['aria-label']==='synthetic-member 선택');selected.checked=true;await selected.emit('change');find(ctx.main,node=>node.attributes['aria-label']==='권한 변경 사유').value='synthetic reason';await findButton(ctx.main,'업로드 허용').emit('click');
 const dialog=find(document.body,node=>node.tagName==='dialog'),confirm=find(dialog,node=>node.attributes.type==='checkbox'),password=find(dialog,node=>node.attributes.type==='password'),submit=findButton(dialog,'변경 확정');confirm.checked=true;password.value='synthetic-rejected';await submit.emit('click');
 assert.equal(expired,0);assert(!ctx.life.disposed);assert(dialog.open);assert(confirm.checked&&!confirm.disabled&&!submit.disabled);assert.equal(password.value,'');assert(ctx.main.textContent.includes('현재 페이지 1명 선택'));assert(!dialog.textContent.includes('만료되었습니다'));assert(dialog.textContent.includes('비밀번호'));assert.deepEqual(ctx.routes,[]);
 password.value='synthetic-confirmed';await submit.emit('click');assert.equal(commits.length,2);assert.equal(commits[0].headers.get('Idempotency-Key'),commits[1].headers.get('Idempotency-Key'));assert(!dialog.open);assert.equal(ctx.routes[0][0],'admin');assert.equal(expired,0);ctx.life.dispose();
}
// Only real authentication-required responses retain the session-cleanup event.
for(const error of ['authentication_required',{code:'AUTHENTICATION_REQUIRED'}]){
 const ctx=context();globalThis.fetch=async()=>Response.json({error},{status:401});const before=expired;
 await assert.rejects(request('/me'),failure=>{assert.equal(failure.status,401);assert(errorText(failure).includes('만료'));return true;});assert.equal(expired,before+1);assert(ctx.life.disposed);
}
console.log('PASSWORD_REJECTION_SESSION_PASS');
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn('PASSWORD_REJECTION_SESSION_PASS', done.stdout)

    def test_actual_modules_payload_singleflight_uncertainty_and_disposal(self):
        script = r'''
import assert from 'node:assert/strict';
globalThis.location={origin:'https://member.synthetic.invalid'};
globalThis.document={cookie:'__Host-dl-csrf=synthetic-csrf'};
let expired=0;globalThis.window={dispatchEvent(){expired++;}};
const {request,Lifetime,errorText}=await import(process.argv[1]+'/api.js');
const {memberActions,preferencesPayload,recommendationReason,RECOMMENDATION_MODES}=await import(process.argv[1]+'/member.js');
const {FILTER_NAMES,filterValues,appendFilters,backendParams,viewName}=await import(process.argv[1]+'/filters.js');
const {ratingText}=await import(process.argv[1]+'/cards.js');
const filters={categories:['novel_text'],states:[],formats:['txt'],representations:['text','imagepack'],genres:['환상']};
const explicit=appendFilters(new URLSearchParams({mode:'popular',view:'list'}),filters,true);
assert.deepEqual(explicit.getAll('states'),['']);assert.deepEqual(explicit.getAll('representations'),['text','imagepack']);
assert(!backendParams(explicit).has('view'));assert.equal(viewName(explicit,'grid'),'list');
const inherited=filterValues(new URLSearchParams(),filters);assert.deepEqual(inherited,filters);
assert.deepEqual(filterValues(new URLSearchParams('categories='),filters).categories,[]);
const cleared=appendFilters(new URLSearchParams('categories=novel_scan'),Object.fromEntries(FILTER_NAMES.map(name=>[name,[]])),true);
for(const name of FILTER_NAMES)assert.deepEqual(cleared.getAll(name),['']);
assert.equal(appendFilters(new URLSearchParams(),filters).has('states'),false);
const pref=preferencesPayload(filters,'list',{exclude_rated:false,exclude_downloaded:true,updated_at:'ignored'});
assert.deepEqual(Object.keys(pref).sort(),['filters','recommendations','view']);assert.deepEqual(Object.keys(pref.recommendations).sort(),['exclude_downloaded','exclude_rated']);
pref.filters.formats.push('pdf');assert.deepEqual(filters.formats,['txt']);
assert.equal(ratingText({rating_votes:2,rating_average:3.25,weighted_rating:4.99}),'공유 평균 ★ 3.25 · 2명 평가');assert.equal(ratingText({rating_votes:0,rating_average:null}),'아직 평가가 없어요');
assert.equal(RECOMMENDATION_MODES.length,4);assert(recommendationReason({reason:'similar_taste'}).includes('취향'));
const life=new Lifetime(),commits=[],failures=[],pending=[];
const run=memberActions(life,{pending:(...args)=>pending.push(args),committed:value=>commits.push(value),failed:error=>failures.push(error)});
let calls=[];const respond=value=>Response.json(value);let resolve;
globalThis.fetch=async(url,opts)=>{calls.push({url,opts});return new Promise(r=>{resolve=r;});};
const put=run(()=>request('/api/v1/items/work/rating',{method:'PUT',body:{rating_half_steps:7},signal:life.signal}));
await run(()=>request('/api/v1/items/work/favorite',{method:'PUT',body:{favorite:true},signal:life.signal}));
assert.equal(calls.length,1);assert.equal(commits.length,0);assert.equal(JSON.parse(calls[0].opts.body).rating_half_steps,7);assert.equal(calls[0].opts.headers.get('X-CSRF-Token'),'synthetic-csrf');
resolve(respond({my_rating_half_steps:7,rating_average:3.5,favorite:false}));await put;assert.equal(commits[0].my_rating_half_steps,7);assert.deepEqual(pending.at(-1),[false,false]);
globalThis.fetch=async(url,opts)=>{calls.push({url,opts});return respond({my_rating_half_steps:null});};
await run(()=>request('/api/v1/items/work/rating',{method:'DELETE',body:{},signal:life.signal}));assert.deepEqual(JSON.parse(calls.at(-1).opts.body),{});
await run(()=>request('/api/v1/items/work/favorite',{method:'PUT',body:{favorite:false},signal:life.signal}));assert.equal(JSON.parse(calls.at(-1).opts.body).favorite,false);
globalThis.fetch=async(url,opts)=>{calls.push({url,opts});return Response.json({error:'RATE_LIMITED'},{status:429,headers:{'Retry-After':'12'}});};
await run(()=>request('/api/v1/items/work/rating',{method:'PUT',body:{rating_half_steps:10},signal:life.signal}));
assert.equal(failures.length,1);assert(errorText(failures[0]).includes('12초'));assert(errorText(failures[0]).includes('직접'));
const count=calls.length;await new Promise(r=>setTimeout(r,20));await run(()=>request('/api/v1/items/work/favorite',{method:'PUT',body:{favorite:true}}));assert.equal(calls.length,count);assert.deepEqual(pending.at(-1),[false,true]);
assert(!errorText({status:429,retryAfter:'999999999<script>'}).includes('<script>'));
globalThis.fetch=async(url,opts)=>{calls.push({url,opts});return respond({my_rating_half_steps:10});};
await run(()=>request('/api/v1/items/work',{signal:life.signal}),{refresh:true});assert.equal(calls.at(-1).opts.method,'GET');assert.deepEqual(pending.at(-1),[false,false]);
globalThis.fetch=async(url,opts)=>{calls.push({url,opts});return new Promise(r=>{resolve=r;});};
const late=run(()=>request('/api/v1/items/work/favorite',{method:'PUT',body:{favorite:true},signal:life.signal}));const committed=commits.length,changed=pending.length;life.dispose();resolve(respond({favorite:true}));await late;assert.equal(commits.length,committed);assert.equal(pending.length,changed);const disposedCalls=calls.length;await run(()=>request('/me'),{refresh:true});assert.equal(calls.length,disposedCalls);
const expiredLife=new Lifetime();let privateValue='private';const expiryRun=memberActions(expiredLife,{pending(){},committed(){privateValue='late';},failed(){privateValue='error';}});globalThis.window.dispatchEvent=()=>{expired++;privateValue='';expiredLife.dispose();};
globalThis.fetch=async()=>Response.json({error:'AUTHENTICATION_REQUIRED'},{status:401});await expiryRun(()=>request('/api/v1/me/preferences',{method:'PUT',body:pref,signal:expiredLife.signal}));assert.equal(expired,1);assert.equal(privateValue,'');
console.log('MEMBER_CLIENT_CONTRACTS_PASS');
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True, check=True)
        self.assertIn('MEMBER_CLIENT_CONTRACTS_PASS', done.stdout)

    def test_exact_genre_selection_add_remove_and_disabled_controls(self):
        script = r'''
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
// Reuse the existing upload DOM fixture; no separate DOM implementation.
const existing=readFileSync(new URL('../../tests/test_web_assets.py',process.argv[1]+'/'),'utf8');
const start=existing.indexOf('class Element {'),end=existing.indexOf('const {Lifetime}',start);assert(start>=0&&end>start);
await import('data:text/javascript,'+encodeURIComponent(existing.slice(start,end)));
const {filterControls}=await import(process.argv[1]+'/filters.js');
const {preferencesPayload}=await import(process.argv[1]+'/member.js');
const original=['액션, 모험',' 앞뒤 공백 ','line\nbreak','carriage\rreturn','tab\tgenre','nul\0genre','💚'.repeat(128)];
const saved={filters:{categories:[],states:[],formats:[],representations:[],genres:original},view:'list',recommendations:{exclude_rated:true,exclude_downloaded:false}};
const selection=filterControls(saved.filters),payload=()=>preferencesPayload(selection.read(),saved.view,saved.recommendations);
assert.deepEqual(payload(),saved);assert.equal(JSON.stringify(payload()),JSON.stringify(saved));
const input=selection.controls.find(x=>x.attributes.name==='new_genre'),add=selection.controls.find(x=>x.tagName==='button'&&x.textContent==='장르 추가');assert(input&&add);
const storedChecks=selection.controls.filter(x=>x.attributes.name==='genres');assert.equal(storedChecks.length,original.length);
// Stored values remain exact even if the DOM value property changes its spelling.
storedChecks[0].value='DOM-normalized-value';assert.deepEqual(payload(),saved);
input.value='아직 추가하지 않은 장르';assert.deepEqual(payload(),saved);
let prevented=false;await input.emit('keydown',{key:'Enter',preventDefault(){prevented=true;}});assert(prevented);assert.deepEqual(payload(),saved);
input.value=' 새 장르, 하나 ';await add.emit('click');assert.deepEqual(selection.read().genres,[...original,' 새 장르, 하나 ']);assert.equal(input.value,'');
const added=selection.controls.at(-1);assert.equal(added.attributes.name,'genres');assert(added.checked);
storedChecks[0].checked=false;assert.deepEqual(selection.read().genres,[...original.slice(1),' 새 장르, 하나 ']);
input.value=original[0];const size=selection.controls.length;await add.emit('click');assert.equal(selection.controls.length,size);assert.deepEqual(selection.read().genres,[...original,' 새 장르, 하나 ']);
// All dynamically added controls take part in the preference write fence.
for(const control of selection.controls)control.disabled=true;
input.value='busy genre';await add.emit('click');assert.equal(selection.controls.length,size);assert(!selection.read().genres.includes('busy genre'));assert(added.disabled);
for(const control of selection.controls)control.disabled=false;
input.value=' ';await add.emit('click');assert(selection.read().genres.includes(' '));
input.value='💛'.repeat(128);await add.emit('click');assert(selection.read().genres.includes('💛'.repeat(128)));
input.value='x'.repeat(129);const before=selection.read().genres;await add.emit('click');assert.deepEqual(selection.read().genres,before);
input.value='';await add.emit('click');assert.deepEqual(selection.read().genres,before);
const bounded=filterControls({genres:Array.from({length:64},(_,i)=>'장르 '+i)}),boundedInput=bounded.controls.find(x=>x.attributes.name==='new_genre'),boundedAdd=bounded.controls.find(x=>x.tagName==='button');
boundedInput.value='장르 65';await boundedAdd.emit('click');assert.equal(bounded.read().genres.length,64);assert(!bounded.read().genres.includes('장르 65'));
bounded.controls.find(x=>x.attributes.name==='genres').checked=false;await boundedAdd.emit('click');assert.equal(bounded.read().genres.length,64);assert(bounded.read().genres.includes('장르 65'));assert(!bounded.read().genres.includes('장르 0'));
console.log('EXACT_GENRE_CONTROLS_PASS');
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True, check=True)
        self.assertIn('EXACT_GENRE_CONTROLS_PASS', done.stdout)


    def test_recommendation_request_keeps_displayed_snapshot_after_preferences_change(self):
        script = r'''
import assert from 'node:assert/strict';
globalThis.location={origin:'https://recommendation.synthetic.invalid'};
globalThis.document={cookie:'__Host-dl-csrf=synthetic-csrf'};globalThis.window={dispatchEvent(){}};
const {request}=await import(process.argv[1]+'/api.js');
const {recommendationParams}=await import(process.argv[1]+'/member.js');
const {FILTER_NAMES,filterValues}=await import(process.argv[1]+'/filters.js');
const snapshotA={filters:{categories:['novel_text'],states:[],formats:['txt'],representations:['text'],genres:['장르 A, 하나']},view:'grid',recommendations:{exclude_rated:true,exclude_downloaded:false}};
const snapshotB={filters:{categories:['comic'],states:['ongoing'],formats:['cbz'],representations:['imagepack'],genres:['장르 B']},view:'list',recommendations:{exclude_rated:false,exclude_downloaded:true}};
let stored=snapshotA;const calls=[];
globalThis.fetch=async(url,opts)=>{
 calls.push({url,opts});
 if(url.pathname==='/api/v1/me/preferences'){const response=Response.json(stored);stored=snapshotB;return response;}
 assert.equal(url.pathname,'/api/v1/recommendations');
 // Model the already verified server contract: omitted values inherit its current snapshot.
 const effective={filters:Object.fromEntries(FILTER_NAMES.map(name=>[name,url.searchParams.has(name)?url.searchParams.getAll(name).filter(Boolean):stored.filters[name]])),recommendations:Object.fromEntries(['exclude_rated','exclude_downloaded'].map(name=>[name,url.searchParams.has(name)?url.searchParams.get(name)==='true':stored.recommendations[name]]))};
 return Response.json({items:[],effective});
};
const displayed=await request('/api/v1/me/preferences');assert.deepEqual(displayed,snapshotA);assert.equal(stored,snapshotB);
const query=recommendationParams(new URLSearchParams('view=grid'),displayed.filters,displayed.recommendations,'20');
const result=await request('/api/v1/recommendations?'+query);
assert.deepEqual(result.effective,{filters:snapshotA.filters,recommendations:snapshotA.recommendations});
assert.notDeepEqual(result.effective,{filters:snapshotB.filters,recommendations:snapshotB.recommendations});
assert.equal(calls.length,2);assert(calls.every(call=>call.opts.method==='GET'));assert.equal(calls.filter(call=>call.url.pathname==='/api/v1/recommendations').length,1);
assert.equal(query.get('limit'),'20');assert(!query.has('view'));assert.deepEqual(query.getAll('states'),['']);assert.deepEqual(query.getAll('genres'),['장르 A, 하나']);
const params=new URLSearchParams('mode=unrated&limit=50&view=list&categories=comic&genres=&exclude_rated=false&exclude_downloaded=true');
const shown=filterValues(params,displayed.filters),overridden=recommendationParams(params,shown,{exclude_rated:false,exclude_downloaded:true},'50');
assert.deepEqual(overridden.getAll('categories'),['comic']);assert.deepEqual(overridden.getAll('genres'),['']);assert.deepEqual(overridden.getAll('formats'),['txt']);assert.deepEqual(overridden.getAll('representations'),['text']);
assert.equal(overridden.get('mode'),'unrated');assert.equal(overridden.get('limit'),'50');assert.equal(overridden.get('exclude_rated'),'false');assert.equal(overridden.get('exclude_downloaded'),'true');assert(!overridden.has('view'));assert.equal(params.get('view'),'list');
const empty=Object.fromEntries(FILTER_NAMES.map(name=>[name,[]])),cleared=recommendationParams(new URLSearchParams('mode=popular&view=list&genres=old&genres=older&limit=999'),empty,{exclude_rated:false,exclude_downloaded:false},'20');
for(const name of FILTER_NAMES)assert.deepEqual(cleared.getAll(name),['']);assert.equal(cleared.get('mode'),'popular');assert.equal(cleared.get('limit'),'20');assert.equal(cleared.get('exclude_rated'),'false');assert.equal(cleared.get('exclude_downloaded'),'false');assert(!cleared.has('view'));assert.equal(calls.length,2);
console.log('RECOMMENDATION_SNAPSHOT_CRITERIA_PASS');
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True, check=True)
        self.assertIn('RECOMMENDATION_SNAPSHOT_CRITERIA_PASS', done.stdout)



if __name__ == '__main__':
    unittest.main()
