"""Exercise metadata payloads and request lifecycle through the actual browser modules."""
from pathlib import Path
import shutil
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class MetadataClientContracts(unittest.TestCase):
    def test_explicit_rights_immutable_editor_and_replay_contracts(self):
        script = r'''
import assert from 'node:assert/strict';
globalThis.location={origin:'https://metadata.synthetic.invalid'};
globalThis.document={cookie:'__Host-dl-csrf=synthetic-csrf'};
globalThis.window={dispatchEvent(){}};
const {Lifetime}=await import(process.argv[1]+'/api.js');
const {metadataRightsDefaults,metadataRightsDecision,metadataEditorSnapshot,metadataDescriptionAction,metadataMutation,metadataReads,
 metadataGeneratedRanges,metadataGeneratedReviewPayload,metadataPanels}=await import(process.argv[1]+'/metadata.js');

const item={rights_id:'rights-a',work_id:'actual/work 💚',kind:'description',version:7,state:'review',officiality:'unknown',match_state:'review',
 display_allowed:0,cache_allowed:0,derivative_allowed:1,cache_until_ms:1700000000123,
 retention_policy:{backup_allowed:false,all_copies_deletion_required:true,policy_id:'keep-contract'},evidence:{license:'existing-proof'}};
const original=structuredClone(item),defaults=metadataRightsDefaults(item);
assert.equal(defaults.display_allowed,false);assert.equal(defaults.cache_allowed,false);assert.equal(defaults.derivative_allowed,true);
defaults.retention_policy.policy_id='changed-copy';assert.deepEqual(item,original);
const values={state:'review',officiality:'unknown',match_state:'review',display_allowed:false,cache_allowed:false,derivative_allowed:true,
 backup_allowed:false,all_copies_deletion_required:true,expiry_mode:'until',cache_until_ms:1700000000123,no_expiry_confirmed:false,evidence_reference:''};
const unchanged=metadataRightsDecision(item,values);
assert.deepEqual(unchanged,{...metadataRightsDefaults(item)});
const blocked=metadataRightsDecision(item,{...values,state:'blocked'});
assert.equal(blocked.derivative_allowed,true);assert.equal(blocked.retention_policy.policy_id,'keep-contract');assert.deepEqual(blocked.evidence,{license:'existing-proof'});
assert.throws(()=>metadataRightsDecision(item,{...values,state:'allowed'}),/근거 참조/);
assert.throws(()=>metadataRightsDecision(item,{...values,state:'allowed',cache_allowed:true,evidence_reference:'receipt'}),/사본 삭제/);
const allowedValues={...values,state:'allowed',display_allowed:true,cache_allowed:true,backup_allowed:true,all_copies_deletion_required:false,evidence_reference:'review-proof-1'};
const allowed=metadataRightsDecision(item,allowedValues);
assert.equal(allowed.officiality,'unknown');assert.equal(allowed.match_state,'review');assert.equal(allowed.derivative_allowed,true);
assert.equal(allowed.cache_until_ms,1700000000123);assert.deepEqual(allowed.evidence,{license:'existing-proof',reference:'review-proof-1'});
assert.throws(()=>metadataRightsDecision(item,{...allowedValues,expiry_mode:'none'}),/만료 없는/);
assert.equal(metadataRightsDecision(item,{...allowedValues,expiry_mode:'none',no_expiry_confirmed:true}).cache_until_ms,null);
assert.throws(()=>metadataRightsDecision(item,{...allowedValues,cache_until_ms:NaN}),/만료 시각/);
assert.throws(()=>metadataRightsDecision(item,{...allowedValues,evidence_reference:'x'.repeat(201)}),/200자/);
assert.throws(()=>metadataRightsDecision(item,{...allowedValues,evidence_reference:'bad\nreference'}),/제어 문자/);
assert.deepEqual(item,original);

const workId='actual/work '+ 'x'.repeat(150)+' 💚';
const right={rights_id:'allowed-a',kind:'description',state:'allowed',version:9,editorial_eligible:true,display_allowed:1,cache_allowed:1,
 cache_until_ms:2001,retention_policy:{backup_allowed:true,all_copies_deletion_required:false}};
const state={work_id:workId,now_ms:2000,head:{version:12},rights:[right,
 {...right,rights_id:'second-explicit-choice',version:10,cache_until_ms:null},
 {...right,rights_id:'blocked-display',display_allowed:0},
 {...right,rights_id:'expired',cache_until_ms:2000},
 {...right,rights_id:'unsupported-retention',retention_policy:{backup_allowed:false,all_copies_deletion_required:false}},
 {...right,rights_id:'not-editorial',editorial_eligible:false},
 {...right,rights_id:'cover-only',kind:'cover'}]};
const snapshot=metadataEditorSnapshot(workId,state);
assert.deepEqual(snapshot.rights.map(r=>r.rights_id),['allowed-a','second-explicit-choice']);
assert(Object.isFrozen(snapshot));assert(Object.isFrozen(snapshot.rights[0]));
state.head.version=99;state.rights[0].version=100;state.work_id='other-work';
assert.throws(()=>metadataDescriptionAction(snapshot,'','draft'),/선택/);
assert.throws(()=>metadataDescriptionAction(snapshot,'expired','draft'),/선택/);
assert.throws(()=>metadataDescriptionAction(snapshot,'allowed-a','  '),/본문/);
const action=metadataDescriptionAction(snapshot,'allowed-a','  exact drafted text\n');
assert.equal(action.path,`/api/v1/admin/metadata/works/${encodeURIComponent(workId)}/description`);
assert.deepEqual(action.body,{text:'  exact drafted text\n',rights_id:'allowed-a',expected_rights_version:9,expected_head_version:12});
assert.equal(metadataDescriptionAction(snapshot,'second-explicit-choice','another').body.expected_rights_version,10);
assert.throws(()=>metadataEditorSnapshot('wrong-id',state),/다시 조회/);

const life=new Lifetime(),events=[],commits=[],failures=[],calls=[];
const run=metadataMutation(life,{pending:s=>events.push(s),committed:v=>commits.push(v),failed:(e,u)=>failures.push({e,u})});
let resolve;
globalThis.fetch=async(url,options)=>{calls.push({url,options});return new Promise(r=>{resolve=r;});};
const candidate={expected_control_version:2,expected_head_version:12,review_status:'approved',select:true};
const first=run('/api/v1/admin/metadata/candidates/a/review',candidate);
await run('/api/v1/admin/metadata/candidates/a/review',{...candidate,review_status:'rejected'});
assert.equal(calls.length,1);assert.equal(commits.length,0);assert.equal(events.at(-1).busy,true);
candidate.expected_head_version=90;
resolve(Response.json({error:'SERVER_UNAVAILABLE'},{status:503}));await first;
assert.equal(failures.at(-1).u,true);assert.equal(events.at(-1).uncertain,true);
const frozenBody=calls[0].options.body;
assert.equal(JSON.parse(frozenBody).expected_head_version,12);
assert.equal(calls[0].options.headers.get('X-CSRF-Token'),'synthetic-csrf');
await run('/api/v1/admin/metadata/candidates/a/review',candidate);
assert.equal(calls.length,1);assert.equal(failures.at(-1).e.code,'METADATA_INPUT');
globalThis.fetch=async(url,options)=>{calls.push({url,options});return Response.json({version:13});};
await run('/api/v1/admin/metadata/candidates/a/review',{...candidate,expected_head_version:12});
assert.equal(calls.length,2);assert.equal(calls[1].options.body,frozenBody);assert.equal(commits.length,1);assert.equal(events.at(-1).done,true);
await run('/api/v1/admin/metadata/candidates/a/review',{...candidate,expected_head_version:12});assert.equal(calls.length,2);

// Lost responses keep a description's text, work/head/rights binding and request ID unchanged.
const editLife=new Lifetime(),editCommits=[],editFailures=[];
const editRun=metadataMutation(editLife,{pending(){},committed:v=>editCommits.push(v),failed:(e,u)=>editFailures.push(u)});
globalThis.fetch=async(url,options)=>{calls.push({url,options});throw new TypeError('synthetic response lost');};
await editRun(action.path,action.body);const lost=calls.at(-1).options.body;
assert.equal(editFailures.at(-1),true);assert.equal(action.body.text,'  exact drafted text\n');
globalThis.fetch=async(url,options)=>{calls.push({url,options});return Response.json({version:13});};
await editRun(action.path,action.body);assert.equal(calls.at(-1).options.body,lost);assert.equal(editCommits.length,1);

// A definite validation rejection permits an edited draft with a new request identity.
const rejected=metadataMutation(new Lifetime(),{pending(){},committed(){},failed:(e,u)=>assert.equal(u,false)});
globalThis.fetch=async(url,options)=>{calls.push({url,options});return Response.json({error:'INPUT_INVALID'},{status:422});};
await rejected(action.path,action.body);const rejectedId=JSON.parse(calls.at(-1).options.body).request_id;
await rejected(action.path,{...action.body,text:'corrected draft'});
assert.notEqual(JSON.parse(calls.at(-1).options.body).request_id,rejectedId);

const readLife=new Lifetime(),reads=metadataReads(readLife),older=reads.next(),latest=reads.next();
assert.equal(reads.current(older),false);assert.equal(reads.current(latest),true);reads.invalidate();assert.equal(reads.current(latest),false);
const finalRead=reads.next();readLife.dispose();assert.equal(reads.current(finalRead),false);
let lateCommits=0,latePending=0;
const disposedLife=new Lifetime(),late=metadataMutation(disposedLife,{pending(){latePending++;},committed(){lateCommits++;},failed(){assert.fail('disposed response');}});
globalThis.fetch=async()=>new Promise(r=>{resolve=r;});const pending=late(action.path,action.body);disposedLife.dispose();resolve(Response.json({version:14}));await pending;
assert.equal(lateCommits,0);assert.equal(latePending,1);

// Generated-review units and evidence positions are server-issued and codepoint based.
assert.deepEqual(metadataGeneratedRanges([{start:'1',end:'2'}],Array.from('A💚B').length,2),[{start:1,end:2}]);
assert.throws(()=>metadataGeneratedRanges([{start:'19',end:'21'}],40,20),/선택 본문과 결말/);
assert.throws(()=>metadataGeneratedRanges([{start:'0',end:'2049'}],3000,2500),/2,048/);
const unitId='summary-v1:'+'b'.repeat(64),summaryHash='a'.repeat(64),contextHash='c'.repeat(64);
const generatedDetail={candidate_id:'generated-1',summary:'A💚B',summary_sha256:summaryHash,item_type:'novel',
 current:{revalidatable:true},review_units:[{unit_id:unitId,summary_start:0,summary_end:3,summary_sha256:summaryHash}],
 qa_codes:['RISK_FLAG','SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'],excluded_tail_range:{start:20,end:40},full_text_codepoints:40};
const generatedEvidence={candidate_id:'generated-1',review_context_digest:contextHash,expected_control_version:4,expected_head_version:32,expected_rights_version:7,
 ranges:[{start:0,end:5,text:'ABCDE',sha256:'d'.repeat(64),tail:false},{start:20,end:25,text:'FGHIJ',sha256:'e'.repeat(64),tail:true}]};
const generatedDraft={summaryAttested:true,summaryRangeIndexes:[0],spoilerRangeIndexes:[1],grounding:'grounded',spoiler:'acceptable',
 flagResolutions:{RISK_FLAG:{resolution:'accept_with_evidence',rangeIndexes:[0]}}};
const generatedBody=metadataGeneratedReviewPayload(generatedDetail,generatedEvidence,generatedDraft,'approved',true);
assert.deepEqual(generatedBody.review_units,[{unit_id:unitId,range_indexes:[0]}]);
assert.deepEqual(generatedBody.ranges,[{start:0,end:5},{start:20,end:25}]);
assert.equal(generatedBody.expected_head_version,32);assert.equal(generatedBody.expected_rights_version,7);
assert.deepEqual(generatedBody.flag_resolutions,[{code:'RISK_FLAG',resolution:'accept_with_evidence',range_indexes:[0]}]);
assert.equal('request_id' in generatedBody,false);assert.equal('unit_id' in generatedBody,false);
assert.throws(()=>metadataGeneratedReviewPayload(generatedDetail,generatedEvidence,{...generatedDraft,summaryRangeIndexes:[1]},'approved'),/전체 요약 근거/);
assert.throws(()=>metadataGeneratedReviewPayload(generatedDetail,generatedEvidence,{...generatedDraft,spoilerRangeIndexes:[]},'approved'),/원문 근거/);
assert.throws(()=>metadataGeneratedReviewPayload(generatedDetail,generatedEvidence,{...generatedDraft,flagResolutions:{}},'approved'),/품질 검토 코드/);
assert.throws(()=>metadataGeneratedReviewPayload({...generatedDetail,review_units:[{...generatedDetail.review_units[0],summary_end:2}]},generatedEvidence,generatedDraft,'approved'),/서버가 발급한 전체 요약/);

// Exercise the actual panel routing and the API wire contract with a tiny DOM harness.
class TestNode {
  constructor(tag,text=''){this.tagName=tag;this.text=text;this.children=[];this.attributes={};this.listeners={};this.className='';this.value='';this.checked=false;this.disabled=false;this.hidden=false;}
  append(...nodes){for(const node of nodes)node.parentElement=this;this.children.push(...nodes);}
  replaceChildren(...nodes){for(const node of nodes)node.parentElement=this;this.children=[...nodes];}
  addEventListener(name,fn){(this.listeners[name]??=[]).push(fn);}
  setAttribute(name,value){this.attributes[name]=String(value);if(name==='selected')this.selected=true;}
  getAttribute(name){return this.attributes[name]??null;}
  get textContent(){return this.tagName==='#text'?this.text:this.children.map(x=>x.textContent).join('');}
  async click(){if(this.disabled)return;for(const fn of this.listeners.click||[])await fn({preventDefault(){}});}
  async dispatch(name){for(const fn of this.listeners[name]||[])await fn({preventDefault(){}});}
}
globalThis.Node=TestNode;
globalThis.document={cookie:'__Host-dl-csrf=synthetic-csrf',createElement:tag=>new TestNode(tag),createTextNode:text=>new TestNode('#text',String(text))};
const byLabel=(root,label)=>{const found=[];const walk=node=>{if(node.getAttribute?.('aria-label')===label)found.push(node);for(const child of node.children||[])walk(child);};walk(root);return found[0];};
const byText=(root,text,tag=null)=>{const found=[];const walk=node=>{if((!tag||node.tagName===tag)&&node.textContent===text)found.push(node);for(const child of node.children||[])walk(child);};walk(root);return found[0];};
const byTag=(root,tag)=>{const found=[];const walk=node=>{if(node.tagName===tag)found.push(node);for(const child of node.children||[])walk(child);};walk(root);return found;};
const generatedSummary='<img src=x onerror=alert(1)> A💚B';
const uiDetail={candidate_id:'generated-1',work_id:'work-1',summary:generatedSummary,summary_sha256:summaryHash,item_type:'novel',
 uncertainties:['모델이 확신하지 못한 부분'],spoiler_risk:'medium',qa_status:'review_required',qa_codes:['RISK_FLAG','SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'],
 source_revision_id:'revision-1',option_snapshot:{format:'text',delivery_kind:'bundle',coverage_confidence:'verified',members:[{asset_position:0,file_format:'txt',byte_size:300,state:'LIVE'}]},
 selected_ranges:[{start:0,end:20}],excluded_tail_range:{start:20,end:40},full_text_codepoints:40,
 generation:{job_id:'job-1',generation:2,head_version:31,rights_id:'rights-1',rights_version:6,request_manifest:[{},{}]},
 control:{version:4},current:{revalidatable:true,source_current:true,option_current:true,classification_current:true,head_version:32,rights_version:7},
 review_units:[{unit_id:unitId,summary_start:0,summary_end:Array.from(generatedSummary).length,summary_sha256:summaryHash}]};
const uiEvidence={candidate_id:'generated-1',review_context_digest:contextHash,expected_control_version:4,expected_head_version:32,expected_rights_version:7,
 ranges:[{start:0,end:5,text:'ABCDE',sha256:'d'.repeat(64),tail:false},{start:20,end:25,text:'FGHIJ',sha256:'e'.repeat(64),tail:true}]};
const uiCalls=[];let currentHead=32,currentContextHash=contextHash,reviewAttempts=0,evidenceFailures=0,deferDetail=false,resolveDetail=null;
globalThis.fetch=async(url,options={})=>{
  const path=String(url);uiCalls.push({path,options});
  if(path.includes('/review-queue?'))return Response.json({candidates:[
    {candidate_id:'generated-1',work_id:'work-1',kind:'description',origin:'QWEN_TEXT',title:'생성 후보',text_preview:'일부 미리보기',control_version:4,head_version:32},
    {candidate_id:'editorial-1',work_id:'work-1',kind:'description',origin:'EDITORIAL',title:'편집 후보',text_preview:'편집 본문',control_version:3,head_version:32}
  ],rights:[]});
  if(path.endsWith('/generated-review')&&options.method==='POST'){
    reviewAttempts++;
    if(reviewAttempts===1)return Response.json({error:'METADATA_CONFLICT'},{status:409});
    return Response.json({decision:'approved',selected:true});
  }
  if(path.endsWith('/generated-review')){
    if(deferDetail){deferDetail=false;return new Promise(resolve=>{resolveDetail=resolve;});}
    return Response.json({...uiDetail,current:{...uiDetail.current,head_version:currentHead}});
  }
  if(path.endsWith('/generated-evidence')){
    evidenceFailures++;
    if(evidenceFailures===1)return Response.json({error:'AUTHENTICATION_REQUIRED'},{status:401});
    if(evidenceFailures===2)return Response.json({error:'PERMISSION_DENIED'},{status:403});
    return Response.json({...uiEvidence,review_context_digest:currentContextHash,expected_head_version:currentHead});
  }
  throw new Error('unexpected synthetic route '+path);
};
const panelLife=new Lifetime(),panels=metadataPanels({life:panelLife});
await byText(panels,'메타데이터 검토 대기 조회','button').click();
let generatedRow=byText(panels,'생성 후보','h3').parentElement;
const buttonsIn=root=>{const found=[];const walk=node=>{if(node.tagName==='button')found.push(node);for(const child of node.children||[])walk(child);};walk(root);return found;};
assert.equal(buttonsIn(generatedRow).some(x=>['승인','승인 · 선택','거절'].includes(x.textContent)),false,'generated rows must not expose generic review');
const editorialRow=byText(panels,'편집 후보','h3').parentElement;
assert(buttonsIn(editorialRow).some(x=>x.textContent==='승인'));
assert.equal(generatedRow.textContent.includes('일부 미리보기'),false,'generated review must start from full detail');
deferDetail=true;
const staleDetailRead=byText(generatedRow,'전체 요약·원문 근거 검토','button').click();
await Promise.resolve();assert.equal(typeof resolveDetail,'function');
await byText(panels,'메타데이터 검토 대기 조회','button').click();
generatedRow=byText(panels,'생성 후보','h3').parentElement;
resolveDetail(Response.json(uiDetail));await staleDetailRead;
assert.equal(generatedRow.textContent.includes('파일 형식'),false,'late detail reads from a replaced queue cannot write into the current row');
await byText(generatedRow,'전체 요약·원문 근거 검토','button').click();
assert(generatedRow.textContent.includes(generatedSummary));assert(generatedRow.textContent.includes('모델이 확신하지 못한 부분'));
for(const hidden of ['revision-1','job-1','rights-1','SHA-256','codepoint','provenance',summaryHash,unitId,'QWEN_TEXT'])
  assert.equal(generatedRow.textContent.includes(hidden),false,'generated review hides protocol detail: '+hidden);
assert(generatedRow.textContent.includes('기타 점검 항목 (RISK_FLAG)'),'unknown QA codes have a readable label');
assert.equal(byText(generatedRow,'','img'),undefined);
const start1=byLabel(generatedRow,'원문 범위 1 시작 위치'),end1=byLabel(generatedRow,'원문 범위 1 끝 위치');
start1.value='0';await start1.dispatch('input');end1.value='5';await end1.dispatch('input');
await byText(generatedRow,'원문 범위 추가','button').click();
const start2=byLabel(generatedRow,'원문 범위 2 시작 위치'),end2=byLabel(generatedRow,'원문 범위 2 끝 위치');
start2.value='20';await start2.dispatch('input');end2.value='25';await end2.dispatch('input');
await byText(generatedRow,'원문 발췌 다시 읽기','button').click();
assert(generatedRow.textContent.includes('로그인이 만료되었습니다.'),'401 source failures explain expired authentication');
assert.equal(byLabel(generatedRow,'원문 범위 1 시작 위치').value,'0','401 preserves the submitted range draft');
await byText(generatedRow,'원문 발췌 다시 읽기','button').click();
assert(generatedRow.textContent.includes('현재 권한으로 실행할 수 없습니다.'),'403 source failures remain explicit');
assert.equal(byLabel(generatedRow,'원문 범위 1 시작 위치').value,'0','403 preserves the submitted range draft');
await byText(generatedRow,'원문 발췌 다시 읽기','button').click();
const evidenceRequest=uiCalls.find(call=>call.path.endsWith('/generated-evidence'));
assert.deepEqual(JSON.parse(evidenceRequest.options.body),{ranges:[{start:0,end:5},{start:20,end:25}]});
assert(generatedRow.textContent.includes('ABCDE'));assert(generatedRow.textContent.includes('FGHIJ'));
assert(generatedRow.textContent.includes('결말 확인 범위'));assert(generatedRow.textContent.includes('현재 원문 상태를 확인했습니다'));
assert.equal(byLabel(generatedRow,'원문 범위 2을 전체 요약 근거로 연결'),undefined,'tail text cannot be selected as whole-summary evidence');
assert(byLabel(generatedRow,'원문 범위 2을 결말 스포일러 검토 근거로 확인'),'novel tail remains available for spoiler review');
const generatedTextNodes=byTag(generatedRow,'pre');
assert.equal(generatedTextNodes.length,3,'summary and both source excerpts render as preformatted text');
for(const node of generatedTextNodes){
  assert.equal(node.className,'reader-paper','generated text keeps the shared wrapping style');
  assert.equal(node.getAttribute('style'),'min-height:0','generated text removes the reader-only 600px minimum height locally');
}
byLabel(generatedRow,'원문 범위 1을 전체 요약 근거로 연결').checked=true;
await byLabel(generatedRow,'원문 범위 1을 전체 요약 근거로 연결').dispatch('change');
byLabel(generatedRow,'원문 범위 2을 결말 스포일러 검토 근거로 확인').checked=true;
await byLabel(generatedRow,'원문 범위 2을 결말 스포일러 검토 근거로 확인').dispatch('change');
const attestation=byLabel(generatedRow,'전체 요약의 모든 내용을 원문 근거와 대조했습니다');attestation.checked=true;await attestation.dispatch('change');
const grounding=byLabel(generatedRow,'전체 요약 근거 판단');grounding.value='grounded';await grounding.dispatch('change');
const spoiler=byLabel(generatedRow,'스포일러 판단');spoiler.value='acceptable';await spoiler.dispatch('change');
const flagResolution=byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG) 해결');
const flagEvidence=byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG)에 원문 범위 1 연결');
flagEvidence.checked=true;await flagEvidence.dispatch('change');
flagResolution.value='accept_with_evidence';await flagResolution.dispatch('change');
await byText(generatedRow,'승인 · 선택','button').click();
assert.equal(reviewAttempts,1);assert(generatedRow.textContent.includes('작성한 판정은 보존했습니다'));
currentHead=33;currentContextHash='f'.repeat(64);
await byText(generatedRow,'전체 요약·원문 근거 검토','button').click();
assert.equal(byLabel(generatedRow,'원문 범위 1 시작 위치').value,'0','409 refresh preserves range draft');
await byText(generatedRow,'원문 발췌 다시 읽기','button').click();
assert(generatedRow.textContent.includes('현재 원문 상태를 확인했습니다'));
assert.equal(generatedRow.textContent.includes('자료나 권한이 변경되었습니다'),false,'fresh evidence clears the stale conflict error');
assert.equal(byLabel(generatedRow,'전체 요약의 모든 내용을 원문 근거와 대조했습니다').checked,false,'fresh evidence requires renewed whole-summary attestation');
byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG) 해결').value='accept_with_evidence';await byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG) 해결').dispatch('change');
byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG)에 원문 범위 1 연결').checked=true;await byLabel(generatedRow,'기타 점검 항목 (RISK_FLAG)에 원문 범위 1 연결').dispatch('change');
byLabel(generatedRow,'원문 범위 1을 전체 요약 근거로 연결').checked=true;await byLabel(generatedRow,'원문 범위 1을 전체 요약 근거로 연결').dispatch('change');
byLabel(generatedRow,'원문 범위 2을 결말 스포일러 검토 근거로 확인').checked=true;await byLabel(generatedRow,'원문 범위 2을 결말 스포일러 검토 근거로 확인').dispatch('change');
byLabel(generatedRow,'전체 요약의 모든 내용을 원문 근거와 대조했습니다').checked=true;await byLabel(generatedRow,'전체 요약의 모든 내용을 원문 근거와 대조했습니다').dispatch('change');
await byText(generatedRow,'승인 · 선택','button').click();
assert.equal(reviewAttempts,2);
const reviewCall=uiCalls.filter(call=>call.path.endsWith('/generated-review')&&call.options.method==='POST').at(-1);
const reviewBody=JSON.parse(reviewCall.options.body);
assert.equal(reviewBody.expected_head_version,33);assert.equal(reviewBody.review_context_digest,currentContextHash);
assert.deepEqual(Object.keys(reviewBody).sort(),['decision','expected_control_version','expected_head_version','expected_rights_version','flag_resolutions','grounding_disposition','ranges','request_id','review_context_digest','review_units','select','spoiler_disposition'].sort(),'review API field names remain unchanged');
assert.deepEqual(reviewBody.review_units,[{unit_id:unitId,range_indexes:[0]}]);
assert.deepEqual(reviewBody.ranges,[{start:0,end:5},{start:20,end:25}]);
assert.equal(reviewBody.grounding_disposition,'grounded','409 refresh preserves the grounded decision');
assert.equal(reviewBody.spoiler_disposition,'acceptable');assert.equal(reviewBody.select,true);
assert.deepEqual(reviewBody.flag_resolutions,[{code:'RISK_FLAG',resolution:'accept_with_evidence',range_indexes:[0]}]);
assert.equal(Object.keys(reviewBody).some(key=>['claim_id','claims','summary_sha256','unit_id'].includes(key)),false);
assert.equal(byLabel(generatedRow,'review_context_digest'),undefined,'internal digest is never a user input');
panelLife.dispose();
console.log('METADATA_CLIENT_CONTRACTS_PASS');
'''
        done = subprocess.run([shutil.which('node'), '--input-type=module', '-e', script,
                               (ROOT / 'apps/web').as_uri()], text=True, capture_output=True, check=False)
        self.assertEqual(done.returncode, 0, msg=done.stdout + done.stderr)
        self.assertIn('METADATA_CLIENT_CONTRACTS_PASS', done.stdout)


if __name__ == '__main__':
    unittest.main()
