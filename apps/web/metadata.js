import {request,errorText} from './api.js';
import {el,button,field,notice,options} from './dom.js';

const flag=value=>value===true||value===1;
const clone=value=>JSON.parse(JSON.stringify(value));
function freeze(value) {
  if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}
  return value;
}
function invalid(message) {return Object.assign(new Error(message),{code:'METADATA_INPUT'});}
const message=error=>error.code==='METADATA_INPUT'?error.message:errorText(error);

// A failed transport may have committed. Keep its exact identity until a replay resolves it.
export function metadataMutation(life,{pending,committed,failed}) {
  let busy=false,uncertain=false,done=false,attempt=null;
  const state=()=>({busy,uncertain,done,body:attempt?.body});
  return async(path,body)=>{
    if(busy||done||life.disposed)return;
    const identity=JSON.stringify({path,body});
    if(uncertain&&attempt.identity!==identity){failed(invalid('저장 결과를 먼저 확인해야 합니다. 같은 요청을 다시 시도해 주세요.'),true);return;}
    if(!attempt||attempt.identity!==identity)attempt=freeze({identity,path,body:{...clone(body),request_id:crypto.randomUUID()}});
    busy=true;pending(state());
    try{
      const value=await request(attempt.path,{method:'POST',signal:life.signal,body:attempt.body});
      if(life.disposed)return;
      uncertain=false;done=true;committed(value);
    }catch(error){
      if(life.disposed)return;
      uncertain=!Number.isInteger(error.status)||error.status>=500;
      failed(error,uncertain);
    }finally{busy=false;if(!life.disposed)pending(state());}
  };
}

// Only the most recently requested read may replace a rendered snapshot.
export function metadataReads(life) {
  let version=0;
  return {next:()=>++version,current:token=>!life.disposed&&token===version,invalidate:()=>++version};
}

const GENERATED_REVIEW_META_CODES=new Set(['SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED']);
const GENERATED_FLAG_RESOLUTIONS=new Set(['accept_with_evidence','reject','fix_required']);
const GENERATED_SHA=/^[0-9a-f]{64}$/;
const GENERATED_CODE_LABELS=new Map([
  ['HIGH_SPOILER_RISK','자동 점검에서 스포일러 위험이 높다고 표시됨'],
  ['MEDIUM_SPOILER_RISK','자동 점검에서 스포일러 가능성이 있다고 표시됨'],
  ['FORBIDDEN_PHRASE','제한 문구가 요약이나 불확실성에 포함됨'],
  ['TAIL_ONLY_ENTITY','결말에만 나온 인물·사건이 요약이나 불확실성에 포함됨'],
  ['UNCERTAINTIES','생성 과정에서 불확실하다고 남긴 내용이 있음'],
  ['FORBIDDEN_PHRASES_UNAVAILABLE','제한 문구 목록을 사용한 자동 점검을 수행하지 못함'],
  ['TAIL_ENTITIES_UNAVAILABLE','결말 전용 인물·사건 목록을 사용한 자동 점검을 수행하지 못함'],
  ['SEMANTIC_GROUNDING_NOT_VERIFIED','요약의 의미상 원문 근거를 자동으로 확인하지 않음'],
  ['SPOILER_SEMANTICS_NOT_VERIFIED','스포일러의 의미 적합성을 자동으로 확인하지 않음']
]);
const GENERATED_FORMAT_LABELS=new Map([
  ['txt','텍스트'],['text','텍스트'],['md','마크다운'],['markdown','마크다운'],
  ['csv','표 데이터'],['html','웹 문서'],['htm','웹 문서'],['fb2','전자책'],
  ['epub','전자책'],['docx','문서 파일'],['odt','문서 파일']
]);
const generatedCodeLabel=code=>GENERATED_CODE_LABELS.get(code)||'기타 점검 항목 ('+code+')';
const generatedFormatLabel=format=>GENERATED_FORMAT_LABELS.get(format)||'문서 파일';
const generatedRangeLabel=(start,end)=>Number.isSafeInteger(start)&&Number.isSafeInteger(end)&&end>start
  ?((start+1).toLocaleString()+'–'+end.toLocaleString()+'번째 글자'):'확인할 수 없음';
const generatedRangesLabel=ranges=>Array.isArray(ranges)&&ranges.length
  ?ranges.map(range=>generatedRangeLabel(range?.start,range?.end)).join(', '):'확인할 수 없음';
const generatedTypeLabel=type=>type==='novel'?'소설':'문서';
const generatedQAStatusLabel=status=>({review_required:'자동 점검 추가 확인 필요',passed:'자동 점검 완료',failed:'자동 점검에서 확인 필요'})[status]||'자동 점검 상태 확인 필요';
const generatedSpoilerRiskLabel=risk=>({high:'높음',medium:'보통',low:'낮음',unknown:'확인 불가'})[risk]||'확인 불가';
const generatedDeliveryLabel=kind=>kind==='bundle'?'묶음 제공':kind==='single'?'단일 파일':'확인 불가';

export function metadataGeneratedRanges(values,fullTextCodepoints,tailStart) {
  if(!Array.isArray(values)||values.length<1||values.length>16||!Number.isSafeInteger(fullTextCodepoints)||fullTextCodepoints<1)
    throw invalid('원문 범위를 1~16개 입력해 주세요.');
  return values.map(value=>{
    if(!value||String(value.start??'').trim()===''||String(value.end??'').trim()==='')throw invalid('원문 범위의 시작과 끝을 모두 입력해 주세요.');
    const start=Number(value.start),end=Number(value.end);
    if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<0||end<=start||end>fullTextCodepoints||end-start>2048)
      throw invalid('원문 글자 범위는 1~2,048자이며, 전체 원문 안에 있어야 합니다.');
    if(Number.isSafeInteger(tailStart)&&start<tailStart&&end>tailStart)
      throw invalid('선택 본문과 결말 확인 범위는 서로 겹치지 않게 나누어 주세요.');
    return {start,end};
  });
}

function generatedIndexes(value,ranges,{required=false,selectedOnly=false}={}) {
  if(!Array.isArray(value)||value.length>16||(!value.length&&required))throw invalid('검토할 원문 근거 범위를 선택해 주세요.');
  const indexes=[...value];
  if(indexes.some(index=>!Number.isSafeInteger(index)||index<0||index>=ranges.length)||new Set(indexes).size!==indexes.length)
    throw invalid('원문 근거 선택이 현재 표시된 범위와 다릅니다.');
  if(selectedOnly&&indexes.some(index=>ranges[index].tail))throw invalid('전체 요약 근거에는 선택 본문 안의 범위만 연결할 수 있습니다.');
  return indexes;
}

export function metadataGeneratedReviewPayload(detail,evidence,draft,decision,select=false) {
  if(!detail||!evidence||!draft||!['approved','rejected'].includes(decision)||typeof select!=='boolean'||(select&&decision!=='approved'))
    throw invalid('생성 후보의 현재 검토 정보를 다시 불러와 주세요.');
  if(detail.current?.revalidatable!==true||typeof detail.candidate_id!=='string'||typeof detail.summary!=='string'
      ||!GENERATED_SHA.test(detail.summary_sha256||'')||!['novel','document'].includes(detail.item_type))
    throw invalid('현재 원문·권리 상태를 다시 확인해야 검토할 수 있습니다.');
  const summaryCodepoints=Array.from(detail.summary).length;
  const units=detail.review_units;
  if(!Array.isArray(units)||units.length!==1||!units[0]||typeof units[0].unit_id!=='string'
      ||!/^summary-v1:[0-9a-f]{64}$/.test(units[0].unit_id)||units[0].summary_start!==0
      ||units[0].summary_end!==summaryCodepoints||units[0].summary_sha256!==detail.summary_sha256)
    throw invalid('서버가 발급한 전체 요약 검토 항목을 확인할 수 없습니다.');
  if(evidence.candidate_id!==detail.candidate_id||!GENERATED_SHA.test(evidence.review_context_digest||'')
      ||!Number.isSafeInteger(evidence.expected_control_version)||!Number.isSafeInteger(evidence.expected_head_version)
      ||!Number.isSafeInteger(evidence.expected_rights_version)||!Array.isArray(evidence.ranges)
      ||evidence.ranges.length<1||evidence.ranges.length>16)
    throw invalid('현재 원문 근거와 버전 결합이 올바르지 않습니다. 다시 불러와 주세요.');
  const tailStart=detail.excluded_tail_range?.start;
  const ranges=evidence.ranges.map(item=>{
    if(!item||!Number.isSafeInteger(item.start)||!Number.isSafeInteger(item.end)||item.start<0
        ||item.end>detail.full_text_codepoints||item.end<=item.start||item.end-item.start>2048
        ||typeof item.text!=='string'||!GENERATED_SHA.test(item.sha256||'')||typeof item.tail!=='boolean'
        ||(Number.isSafeInteger(tailStart)&&(item.tail!==(item.end>tailStart)||(item.start<tailStart&&item.end>tailStart))))
      throw invalid('서버 원문 근거 응답을 확인할 수 없습니다. 다시 불러와 주세요.');
    return {start:item.start,end:item.end,tail:item.tail};
  });
  const sourceRanges=ranges.map(({start,end})=>({start,end}));
  const grounding=draft.grounding;
  const spoiler=draft.spoiler;
  if(!['grounded','insufficient'].includes(grounding)||!['acceptable','reject','not_applicable'].includes(spoiler))
    throw invalid('요약 근거와 스포일러 판정을 각각 선택해 주세요.');
  const summaryIndexes=generatedIndexes(draft.summaryRangeIndexes||[],ranges,{required:decision==='approved',selectedOnly:true});
  const flagCodes=Array.isArray(detail.qa_codes)?detail.qa_codes.filter(code=>!GENERATED_REVIEW_META_CODES.has(code)):null;
  if(!flagCodes||flagCodes.some(code=>typeof code!=='string')||new Set(flagCodes).size!==flagCodes.length)
    throw invalid('서버 품질 검토 코드를 확인할 수 없습니다.');
  const resolutions=draft.flagResolutions||{};
  if(Object.keys(resolutions).some(code=>!flagCodes.includes(code)))throw invalid('현재 후보에 없는 품질 검토 항목이 포함되었습니다.');
  const flagResolutions=flagCodes.map(code=>{
    const item=resolutions[code];
    if(!item||!GENERATED_FLAG_RESOLUTIONS.has(item.resolution))throw invalid('표시된 품질 검토 코드마다 판정을 선택해 주세요.');
    const rangeIndexes=generatedIndexes(item.rangeIndexes||[],ranges,{required:item.resolution==='accept_with_evidence'});
    return {code,resolution:item.resolution,range_indexes:rangeIndexes};
  });
  let reviewUnits=[];
  if(draft.summaryAttested===true&&summaryIndexes.length)
    reviewUnits=[{unit_id:units[0].unit_id,range_indexes:summaryIndexes}];
  if(decision==='approved'){
    if(draft.summaryAttested!==true||grounding!=='grounded'||reviewUnits.length!==1
        ||flagResolutions.some(item=>item.resolution!=='accept_with_evidence'))
      throw invalid('승인하려면 전체 요약을 직접 확인하고 근거·모든 품질 항목을 해결해야 합니다.');
    if(detail.item_type==='novel'){
      const spoilerIndexes=generatedIndexes(draft.spoilerRangeIndexes||[],ranges,{required:true});
      if(spoiler!=='acceptable'||!spoilerIndexes.some(index=>ranges[index].tail&&ranges[index].start>=tailStart))
        throw invalid('소설은 선택 본문 근거와 분리된 결말 범위 및 명시적 스포일러 판정이 필요합니다.');
    }else if(!['acceptable','not_applicable'].includes(spoiler))throw invalid('문서 스포일러 판정을 확인해 주세요.');
  }else if(select){
    throw invalid('거절한 후보는 선택할 수 없습니다.');
  }
  return {
    expected_control_version:evidence.expected_control_version,
    expected_head_version:evidence.expected_head_version,
    expected_rights_version:evidence.expected_rights_version,
    review_context_digest:evidence.review_context_digest,
    ranges:sourceRanges,
    review_units:reviewUnits,
    grounding_disposition:grounding,
    spoiler_disposition:spoiler,
    flag_resolutions:flagResolutions,
    decision,
    select
  };
}

export function metadataRightsDefaults(item) {
  return {
    state:item.state,officiality:item.officiality,match_state:item.match_state,
    display_allowed:flag(item.display_allowed),cache_allowed:flag(item.cache_allowed),
    derivative_allowed:flag(item.derivative_allowed),cache_until_ms:item.cache_until_ms,
    retention_policy:clone(item.retention_policy||{}),evidence:clone(item.evidence||{})
  };
}

export function metadataRightsDecision(item,values) {
  const decision=metadataRightsDefaults(item);
  for(const [key,allowed] of Object.entries({state:['allowed','review','blocked'],officiality:['verified','unknown','not_applicable'],match_state:['exact','review','not_applicable']})){
    if(!allowed.includes(values[key]))throw invalid('권리 판정·공식성·작품 일치 여부를 선택해 주세요.');
    decision[key]=values[key];
  }
  for(const key of ['display_allowed','cache_allowed','derivative_allowed'])decision[key]=values[key]===true;
  const policy=decision.retention_policy;
  policy.backup_allowed=values.backup_allowed===true;
  policy.all_copies_deletion_required=values.all_copies_deletion_required===true;
  if(decision.cache_allowed&&(!policy.backup_allowed||policy.all_copies_deletion_required))throw invalid('백업 불가 또는 모든 사본 삭제 의무가 있으면 캐시를 허용할 수 없습니다.');
  if(values.expiry_mode==='none'){
    if(decision.state==='allowed'&&decision.cache_allowed&&values.no_expiry_confirmed!==true)throw invalid('만료 없는 캐시 허용이 근거에 명시되었는지 확인해 주세요.');
    decision.cache_until_ms=null;
  }else if(values.expiry_mode==='until'&&Number.isSafeInteger(values.cache_until_ms)&&values.cache_until_ms>=0){
    decision.cache_until_ms=values.cache_until_ms;
  }else throw invalid('캐시 만료 시각을 지정하거나 만료 없음을 명시해 주세요.');
  const reference=String(values.evidence_reference||'').trim();
  if(reference&&(Array.from(reference).length>200||/[\u0000-\u001f\u007f]/.test(reference)))throw invalid('근거 참조는 제어 문자 없이 200자 이내로 입력해 주세요.');
  if(decision.state==='allowed'&&!reference)throw invalid('허용 판정을 저장하려면 확인한 근거 참조를 직접 입력해 주세요.');
  if(reference)decision.evidence.reference=reference;
  return decision;
}

export function metadataEditorSnapshot(workId,state) {
  if(!workId.trim()||Array.from(workId).length>200||state.work_id!==workId||!Number.isSafeInteger(state.head?.version))throw invalid('작품 상태를 다시 조회해 주세요.');
  const now=state.now_ms;
  const rights=state.rights.filter(r=>r.editorial_eligible===true&&r.kind==='description'&&r.state==='allowed'&&flag(r.display_allowed)&&flag(r.cache_allowed)&&
    (r.cache_until_ms===null||(Number.isSafeInteger(now)&&Number.isSafeInteger(r.cache_until_ms)&&r.cache_until_ms>now))&&
    r.retention_policy?.backup_allowed===true&&r.retention_policy?.all_copies_deletion_required===false);
  return freeze({work_id:workId,head_version:state.head.version,rights:clone(rights)});
}

export function metadataDescriptionAction(snapshot,rightsId,text) {
  const rights=snapshot.rights.find(r=>r.rights_id===rightsId);
  if(!rights)throw invalid('설명 게시에 사용할 권리 근거를 선택해 주세요.');
  if(!text.trim()||Array.from(text).length>16384)throw invalid('새 설명 본문을 1~16,384자로 입력해 주세요.');
  return {path:`/api/v1/admin/metadata/works/${encodeURIComponent(snapshot.work_id)}/description`,body:{text,rights_id:rights.rights_id,expected_rights_version:rights.version,expected_head_version:snapshot.head_version}};
}

function localDate(value) {
  if(!Number.isSafeInteger(value))return '';
  const date=new Date(value);if(!Number.isFinite(date.getTime()))return '';
  return new Date(value-date.getTimezoneOffset()*60000).toISOString().slice(0,-1);
}

export function metadataPanels(ctx) {
  const {life}=ctx;
  const meta=el('section',{class:'panel'},el('h2',{},'메타데이터 검토'));
  const queue=el('div'),queueStatus=el('div'),queueReads=metadataReads(life),locks=new Set();
  let queueLoading=false,queueRequest=0,queueGeneration=0;
  const queueLoad=button('메타데이터 검토 대기 조회',loadQueue);
  const queueLocked=()=>locks.size>0;
  function queuePending(owner,state){
    if(state.busy||state.uncertain){locks.add(owner);queueReads.invalidate();}else locks.delete(owner);
    queueLoad.disabled=queueLoading||queueLocked();
  }
  async function loadQueue(){
    if(queueLocked()||life.disposed)return;
    const generation=++queueGeneration,token=queueReads.next(),read=++queueRequest;queueLoading=true;queueLoad.disabled=true;
    try{
      const result=await request('/api/v1/admin/metadata/review-queue?limit=50',{signal:life.signal});
      if(!queueReads.current(token)||queueLocked())return;
      queue.replaceChildren();queueStatus.replaceChildren();
      if(!result.candidates.length&&!result.rights.length)queue.append(notice('검토할 메타데이터가 없습니다.'));
      if(result.candidates.length)queue.append(el('h3',{},`후보 검토 대기 ${result.candidates.length}건`));
      for(const item of result.candidates)queue.append(candidateRow(freeze(clone(item)),generation));
      if(result.rights.length)queue.append(el('h3',{},`권리 검토 대기 ${result.rights.length}건`));
      for(const item of result.rights)queue.append(rightsRow(freeze(clone(item))));
    }catch(error){if(queueReads.current(token))queueStatus.replaceChildren(notice(message(error),'error'));}
    finally{if(!life.disposed&&read===queueRequest){queueLoading=false;queueLoad.disabled=queueLocked();}}
  }
  function generatedReviewPanel(item,generation,box,status){
    const panel=el('section',{class:'generated-review'}),body=el('div'),detailStatus=el('div');
    const detailButton=button('전체 요약·원문 근거 검토',loadDetail),replayButton=button('같은 검토 요청 결과 확인',replay);
    replayButton.hidden=true;
    panel.append(detailButton,replayButton,detailStatus,body);
    const detailReads=metadataReads(life),evidenceReads=metadataReads(life);
    const reviewPath=`/api/v1/admin/metadata/candidates/${encodeURIComponent(item.candidate_id)}/generated-review`;
    let detail=null,evidence=null,rangeDrafts=null,contextStale=false,mutationState={busy:false,uncertain:false,done:false},pendingAction=null;
    let rangeFields=[],detailControls=[],evidenceStatus=null,evidenceView=null,rangeList=null,rangeAdd=null,evidenceLoad=null;
    let formDraft={summaryAttested:false,summaryRangeIndexes:[],spoilerRangeIndexes:[],grounding:'',spoiler:'',flagResolutions:{}};
    const rowCurrent=()=>!life.disposed&&generation===queueGeneration;
    const clearEvidenceLinks=()=>{
      formDraft.summaryAttested=false;formDraft.summaryRangeIndexes=[];formDraft.spoilerRangeIndexes=[];
      for(const value of Object.values(formDraft.flagResolutions))value.rangeIndexes=[];
    };
    function showError(target,error,tail=''){
      if(life.disposed||generation!==queueGeneration)return;
      target.replaceChildren(notice(message(error)+tail,'error'));
    }
    function updateControls(){
      const locked=mutationState.busy||mutationState.uncertain||mutationState.done;
      detailButton.disabled=locked||!rowCurrent();
      replayButton.hidden=!mutationState.uncertain||!pendingAction;
      replayButton.disabled=mutationState.busy||mutationState.done||!pendingAction;
      for(const control of detailControls)control.disabled=locked||!rowCurrent()||contextStale;
      for(const control of rangeFields)control.disabled=locked||!rowCurrent()||contextStale;
      if(evidenceLoad)evidenceLoad.disabled=locked||!rowCurrent()||contextStale;
      if(rangeAdd)rangeAdd.disabled=locked||!rowCurrent()||contextStale||(rangeDrafts||[]).length>=16;
    }
    function invalidateEvidence(){
      evidenceReads.invalidate();evidence=null;detailControls=[];clearEvidenceLinks();
      if(evidenceStatus)evidenceStatus.replaceChildren(notice('범위가 바뀌었습니다. 새 원문 근거를 다시 불러와 주세요.'));
      renderEvidence();updateControls();
    }
    function renderRanges(){
      if(!rangeList||!detail)return;
      rangeFields=[];
      rangeList.replaceChildren(...(rangeDrafts||[]).map((value,index)=>{
        const start=el('input',{type:'number',min:0,max:detail.full_text_codepoints,value:value.start,'aria-label':'원문 범위 '+(index+1)+' 시작 위치'});
        const end=el('input',{type:'number',min:1,max:detail.full_text_codepoints,value:value.end,'aria-label':'원문 범위 '+(index+1)+' 끝 위치'});
        const remove=button('범위 삭제',()=>{
          if(!rowCurrent())return;
          rangeDrafts.splice(index,1);invalidateEvidence();renderRanges();
        });
        const changed=()=>{
          if(!rowCurrent())return;
          rangeDrafts[index]={start:start.value,end:end.value};invalidateEvidence();
        };
        start.addEventListener('input',changed);end.addEventListener('input',changed);
        rangeFields.push(start,end,remove);
        return el('div',{class:'row'},field('확인할 원문 범위 '+(index+1)+' · 시작 위치 포함',start),field('끝 위치 · 미포함',end),remove);
      }));
      if(rangeAdd)rangeAdd.disabled=(rangeDrafts||[]).length>=16||contextStale||mutationState.busy||mutationState.uncertain||mutationState.done;
    }
    function renderEvidence(){
      if(!evidenceView)return;
      if(!evidence){evidenceView.replaceChildren();return;}
      detailControls=[];
      const rangeCards=evidence.ranges.map((item,index)=>{
        const links=[];
        if(!item.tail){
          const summaryCheck=el('input',{type:'checkbox',checked:formDraft.summaryRangeIndexes.includes(index),
            'aria-label':'원문 범위 '+(index+1)+'을 전체 요약 근거로 연결'});
          detailControls.push(summaryCheck);
          summaryCheck.addEventListener('change',()=>{
            const next=new Set(formDraft.summaryRangeIndexes);
            if(summaryCheck.checked)next.add(index);else next.delete(index);
            formDraft.summaryRangeIndexes=[...next].sort((a,b)=>a-b);
          });
          links.push(el('label',{},summaryCheck,' 전체 요약 근거'));
        }
        if(detail.item_type==='novel'&&item.tail){
          const spoilerCheck=el('input',{type:'checkbox',checked:formDraft.spoilerRangeIndexes.includes(index),
            'aria-label':'원문 범위 '+(index+1)+'을 결말 스포일러 검토 근거로 확인'});
          detailControls.push(spoilerCheck);
          spoilerCheck.addEventListener('change',()=>{
            const next=new Set(formDraft.spoilerRangeIndexes);
            if(spoilerCheck.checked)next.add(index);else next.delete(index);
            formDraft.spoilerRangeIndexes=[...next].sort((a,b)=>a-b);
          });
          links.push(el('label',{},spoilerCheck,' 결말 스포일러 판단 근거로 확인'));
        }
        return el('article',{class:'edition'},el('h4',{},'원문 '+generatedRangeLabel(item.start,item.end)+' · '+(item.tail?'결말 확인 범위':'선택 본문')),
          el('pre',{class:'reader-paper',style:'min-height:0'},item.text),...links);
      });
      evidenceView.replaceChildren(el('p',{class:'small'},'표시된 글자 위치의 원문 발췌입니다. 범위와 내용을 직접 확인하세요.'),...rangeCards);
      renderReviewForm();
    }
    function renderReviewForm(){
      if(!evidenceView||!evidence)return;
      const summaryAttestation=el('input',{type:'checkbox',checked:formDraft.summaryAttested,'aria-label':'전체 요약의 모든 내용을 원문 근거와 대조했습니다'});
      summaryAttestation.addEventListener('change',()=>{formDraft.summaryAttested=summaryAttestation.checked;});
      const grounding=el('select',{'aria-label':'전체 요약 근거 판단'},options([['','요약 근거 판단 선택'],['grounded','전체 요약이 원문에 근거함'],['insufficient','근거가 부족함']],formDraft.grounding));
      grounding.addEventListener('change',()=>{formDraft.grounding=grounding.value;});
      const spoilerChoices=[['','스포일러 판단 선택'],['acceptable','스포일러 검토 수용'],['reject','스포일러 우려 있음']];
      if(detail.item_type==='document')spoilerChoices.push(['not_applicable','문서 · 해당 없음']);
      const spoiler=el('select',{'aria-label':'스포일러 판단'},options(spoilerChoices,formDraft.spoiler));
      spoiler.addEventListener('change',()=>{formDraft.spoiler=spoiler.value;});
      const controls=[field('사람이 전체 요약을 직접 검토',el('div',{class:'row'},summaryAttestation,
        el('span',{},'전체 요약 '+Array.from(detail.summary).length.toLocaleString()+'자를 원문과 대조해 확인했습니다.'))),
        field('요약의 원문 근거 판정',grounding),field('스포일러 판정',spoiler)];
      const flagCodes=(detail.qa_codes||[]).filter(code=>!GENERATED_REVIEW_META_CODES.has(code));
      for(const code of flagCodes){
        const saved=formDraft.flagResolutions[code]||{resolution:'',rangeIndexes:[]};
        const resolution=el('select',{'aria-label':generatedCodeLabel(code)+' 해결'},options([
          ['','이 항목의 판정 선택'],['accept_with_evidence','근거를 확인하고 수용'],['reject','결과를 거절'],['fix_required','수정 필요']
        ],saved.resolution));
        resolution.addEventListener('change',()=>{
          const current=formDraft.flagResolutions[code]||{resolution:'',rangeIndexes:[]};
          formDraft.flagResolutions[code]={...current,resolution:resolution.value};
        });
        detailControls.push(resolution);
        const evidenceChecks=evidence.ranges.map((item,index)=>{
          const input=el('input',{type:'checkbox',checked:saved.rangeIndexes.includes(index),
            'aria-label':generatedCodeLabel(code)+'에 원문 범위 '+(index+1)+' 연결'});
          detailControls.push(input);
          input.addEventListener('change',()=>{
            const current=formDraft.flagResolutions[code]||{resolution:resolution.value,rangeIndexes:[]};
            const next=new Set(current.rangeIndexes);
            if(input.checked)next.add(index);else next.delete(index);
            formDraft.flagResolutions[code]={...current,rangeIndexes:[...next].sort((a,b)=>a-b)};
          });
          return el('label',{},input,' 범위 '+(index+1)+(item.tail?' · 결말':''));
        });
        controls.push(el('fieldset',{},el('legend',{},generatedCodeLabel(code)),field('사람의 해결 판정',resolution),...evidenceChecks));
      }
      const reject=button('검토 거절',()=>submitReview('rejected',false));
      const approve=button('승인',()=>submitReview('approved',false),'primary');
      const approveSelect=button('승인 · 선택',()=>submitReview('approved',true),'primary');
      detailControls.push(summaryAttestation,grounding,spoiler,reject,approve,approveSelect);
      evidenceView.append(el('section',{class:'generated-review-decision'},el('h3',{},'사람의 검토 결정'),
        el('p',{},'자동 점검은 참고용입니다. 승인 전에 전체 요약과 원문 범위, 각 점검 항목을 직접 확인하세요. 소설은 결말 구간도 확인해야 합니다.'),
        ...controls,reject,approve,approveSelect));
      updateControls();
    }
    function renderDetail(){
      if(!detail)return;
      detailControls=[];rangeFields=[];
      const option=detail.option_snapshot||{},members=Array.isArray(option.members)?option.members:[];
      const memberText=members.map((member,index)=>el('li',{},'원문 파일 '+(Number.isSafeInteger(member.asset_position)?member.asset_position+1:index+1)+' · '+generatedFormatLabel(member.file_format)));
      const requestCount=Array.isArray(detail.generation?.request_manifest)?detail.generation.request_manifest.length:0;
      const current=detail.current||{};
      const source=el('section',{},el('h3',{},'원문과 생성 기록'),
        el('p',{},'자료 유형 '+generatedTypeLabel(detail.item_type)+' · 파일 형식 '+generatedFormatLabel(option.format)+' · '+generatedDeliveryLabel(option.delivery_kind)),
        el('p',{},'전체 원문 '+Number(detail.full_text_codepoints||0).toLocaleString()+'자 · 선택 본문 '+generatedRangesLabel(detail.selected_ranges)+' · 결말 확인 범위 '+(detail.excluded_tail_range?generatedRangeLabel(detail.excluded_tail_range.start,detail.excluded_tail_range.end):'없음')),
        el('ul',{},...memberText),
        el('p',{},'생성 요청 '+requestCount+'회'),
        el('p',{},current.revalidatable?'현재 원문·권리·작품 유형을 다시 확인할 수 있습니다.':'현재 원문·권리·작품 유형을 확인할 수 없습니다.'),
        el('p',{},'원문 '+(current.source_current?'현재 내용 확인됨':'변경 여부 확인 필요')+' · 파일 구성 '+(current.option_current?'확인됨':'다시 확인 필요')+' · 작품 유형 '+(current.classification_current?'확인됨':'다시 확인 필요')));
      const qa=el('section',{},el('h3',{},'자동 점검 참고'),el('p',{},'점검 상태 '+generatedQAStatusLabel(detail.qa_status)+' · 자동 표시된 스포일러 위험 '+generatedSpoilerRiskLabel(detail.spoiler_risk)),
        el('ul',{},...(detail.qa_codes||[]).map(code=>el('li',{},generatedCodeLabel(code)))),
        el('h4',{},'생성 중 불확실하다고 남긴 내용'),
        detail.uncertainties?.length?el('ul',{},...detail.uncertainties.map(value=>el('li',{},value))):el('p',{},'불확실성 항목 없음'));
      const summary=el('section',{},el('h3',{},'생성된 전체 요약'),el('p',{},'요약 전체를 읽고 원문과 대조해 주세요. 자동 점검은 의미가 원문과 맞는지 판단하지 않습니다.'),el('pre',{class:'reader-paper',style:'min-height:0'},detail.summary));
      rangeDrafts=rangeDrafts||[{start:'',end:''}];
      rangeList=el('div');rangeAdd=button('원문 범위 추가',()=>{
        if(!rowCurrent()||rangeDrafts.length>=16)return;
        rangeDrafts.push({start:'',end:''});invalidateEvidence();renderRanges();
      });
      renderRanges();
      evidenceLoad=button('원문 발췌 다시 읽기',loadEvidence);
      evidenceStatus=el('div');evidenceView=el('div');
      const rangeSection=el('section',{},el('h3',{},'원문 근거 범위'),
        el('p',{},'글자 위치는 0부터 셉니다. 시작 위치는 포함하고 끝 위치는 포함하지 않습니다. 각 범위는 최대 2,048자이며 16개까지 확인할 수 있습니다.'),
        rangeList,rangeAdd,evidenceLoad,evidenceStatus,evidenceView);
      const detailError=current.revalidatable?null:notice('현재 원문·권리·작품 유형을 다시 확인할 수 없어 이 후보를 승인할 수 없습니다.','error');
      body.replaceChildren(source,qa,summary,rangeSection,...(detailError?[detailError]:[]));
      updateControls();
    }
    async function loadDetail(){
      if(!rowCurrent()||mutationState.busy||mutationState.uncertain||mutationState.done)return;
      const token=detailReads.next();detailButton.disabled=true;detailStatus.replaceChildren(notice('전체 요약과 현재 원문 상태를 읽는 중…'));
      try{
        const value=await request(reviewPath,{signal:life.signal});
        if(!detailReads.current(token)||!rowCurrent())return;
        detail=value;evidence=null;contextStale=false;evidenceReads.invalidate();clearEvidenceLinks();
        if(!rangeDrafts)rangeDrafts=[{start:'',end:''}];
        detailStatus.replaceChildren();renderDetail();
      }catch(error){if(detailReads.current(token)&&rowCurrent())showError(detailStatus,error);}
      finally{if(!life.disposed&&detailReads.current(token)&&rowCurrent())updateControls();}
    }
    async function loadEvidence(){
      if(!detail||!rowCurrent()||contextStale||mutationState.busy||mutationState.uncertain||mutationState.done||detail.current?.revalidatable!==true)return;
      let ranges;
      try{ranges=metadataGeneratedRanges(rangeDrafts,detail.full_text_codepoints,detail.excluded_tail_range?.start);}
      catch(error){showError(evidenceStatus,error);return;}
      const token=evidenceReads.next();evidence=null;clearEvidenceLinks();evidenceView.replaceChildren();
      evidenceStatus.replaceChildren(notice('현재 원문 범위와 권리 상태를 다시 확인하는 중…'));
      try{
        const value=await request(`/api/v1/admin/metadata/candidates/${encodeURIComponent(item.candidate_id)}/generated-evidence`,
          {method:'POST',signal:life.signal,body:{ranges}});
        if(!evidenceReads.current(token)||!rowCurrent())return;
        const validRanges=Array.isArray(value.ranges)&&value.ranges.length===ranges.length&&value.ranges.every((excerpt,index)=>
          excerpt&&excerpt.start===ranges[index].start&&excerpt.end===ranges[index].end&&typeof excerpt.text==='string'
          &&GENERATED_SHA.test(excerpt.sha256||'')&&typeof excerpt.tail==='boolean'
          &&(!Number.isSafeInteger(detail.excluded_tail_range?.start)
            ||(excerpt.tail===(excerpt.end>detail.excluded_tail_range.start)
              &&!(excerpt.start<detail.excluded_tail_range.start&&excerpt.end>detail.excluded_tail_range.start))));
        if(value.candidate_id!==detail.candidate_id||!GENERATED_SHA.test(value.review_context_digest||'')
            ||!Number.isSafeInteger(value.expected_control_version)||!Number.isSafeInteger(value.expected_head_version)
            ||!Number.isSafeInteger(value.expected_rights_version)||!validRanges)
          throw invalid('원문 근거 응답이 현재 후보와 일치하지 않습니다.');
        evidence=value;contextStale=false;evidenceStatus.replaceChildren(notice(
          '현재 원문 상태를 확인했습니다. 표시된 글자 범위와 발췌문을 직접 확인하세요.'));
        status.replaceChildren();
        renderEvidence();updateControls();
      }catch(error){if(evidenceReads.current(token)&&rowCurrent())showError(evidenceStatus,error);}
      finally{if(!life.disposed&&evidenceReads.current(token)&&rowCurrent())updateControls();}
    }
    async function submitReview(decision,select){
      if(!detail||!evidence||!rowCurrent()||contextStale||mutationState.busy||mutationState.uncertain||mutationState.done)return;
      try{
        const body=metadataGeneratedReviewPayload(detail,evidence,formDraft,decision,select);
        pendingAction={path:reviewPath,body};
        await runReview(pendingAction.path,pendingAction.body);
      }catch(error){showError(status,error);}
    }
    const runReview=metadataMutation(life,{
      pending:state=>{mutationState=state;queuePending(box,state);updateControls();},
      committed:()=>{contextStale=true;status.replaceChildren(notice('생성 후보 검토를 저장했습니다. 대기 목록을 다시 조회하세요.'));updateControls();},
      failed:(error,uncertain)=>{
        if(error.status===409){contextStale=true;detailStatus.replaceChildren(notice('원문·권리 또는 검토 상태가 바뀌었습니다. 작성한 판정은 보존했습니다. 최신 상태를 다시 읽고 원문 근거를 새로 확인하세요.','error'));}
        showError(status,error,uncertain?' 저장 결과가 불확실합니다. 모든 입력을 고정했고 같은 검토 요청만 다시 시도할 수 있습니다.':'');
        updateControls();
      }
    });
    function replay(){
      if(!mutationState.uncertain||!pendingAction||mutationState.busy||mutationState.done)return;
      runReview(pendingAction.path,pendingAction.body);
    }
    return panel;
  }
  function candidateRow(item,generation){
    const status=el('div'),box=el('article',{class:'edition'},el('h3',{},item.title||'제목 확인 불가'));
    if(item.origin==='QWEN_TEXT'){
      box.append(el('p',{class:'small'},'자동 생성 설명 검토'));
      box.append(generatedReviewPanel(item,generation,box,status),status);return box;
    }
    box.append(el('p',{class:'small'},`작품 ${item.work_id} · ${item.kind==='cover'?'표지':'설명'} · 출처 ${item.origin}`));
    if(item.origin==='QWEN_OCR'){
      box.append(notice('OCR 생성 후보는 이 화면에서 검토·승인할 수 없습니다.'));
      return box;
    }
    if(item.text_preview)box.append(el('p',{},item.text_preview));
    const controls=[];
    const run=metadataMutation(life,{
      pending:state=>{queuePending(box,state);for(const {control,review_status,select} of controls)control.disabled=state.busy||state.done||(state.uncertain&&(state.body.review_status!==review_status||state.body.select!==select));},
      committed:()=>status.replaceChildren(notice('후보 검토를 저장했습니다. 최신 목록은 다시 조회해 주세요.')),
      failed:(error,uncertain)=>status.replaceChildren(notice(message(error)+(uncertain?' 저장 결과가 불확실합니다. 같은 동작을 다시 누르면 동일 요청을 확인합니다.':''),'error'))
    });
    for(const [label,review_status,select] of [['승인','approved',false],['승인 · 선택','approved',true],['거절','rejected',false]]){
      const control=button(label,()=>run(`/api/v1/admin/metadata/candidates/${encodeURIComponent(item.candidate_id)}/review`,{expected_control_version:item.control_version,expected_head_version:item.head_version,review_status,select}));
      controls.push({control,review_status,select});box.append(control);
    }
    box.append(status);return box;
  }
  function rightsRow(item){
    const current=metadataRightsDefaults(item),controls=[],status=el('div');
    const select=(label,values,value)=>{const input=el('select',{'aria-label':label},options(values,value));controls.push(input);return input;};
    const check=(label,checked)=>{const input=el('input',{type:'checkbox',checked,'aria-label':label});controls.push(input);return input;};
    const state=select('권리 판정',[['review','검토 대기'],['allowed','허용'],['blocked','차단']],current.state);
    const officiality=select('공식성',[['unknown','확인 중'],['verified','공식 출처 확인'],['not_applicable','해당 없음']],current.officiality);
    const match=select('작품 일치 여부',[['review','검토 대기'],['exact','정확히 일치'],['not_applicable','해당 없음']],current.match_state);
    const display=check('표시 허용',current.display_allowed),cache=check('캐시 허용',current.cache_allowed),derivative=check('파생물 허용',current.derivative_allowed);
    const backup=check('백업 허용',current.retention_policy.backup_allowed===true),allCopies=check('모든 사본 삭제 의무',current.retention_policy.all_copies_deletion_required===true);
    const expiry=select('캐시 만료',[['until','만료 시각 지정'],['none','만료 없음 · 근거에 명시된 경우']],current.cache_until_ms===null?'none':'until');
    const until=el('input',{type:'datetime-local',step:'0.001',value:localDate(current.cache_until_ms),'aria-label':'캐시 만료 시각'});
    const originalUntil=until.value;
    const noExpiry=check('만료 없는 캐시 근거 확인',false),evidence=el('input',{type:'text',maxlength:200,'aria-label':'새 권리 근거 참조'});controls.push(until,evidence);
    const form=el('form'),save=el('button',{type:'submit',class:'primary'},'권리 검토 저장');
    const box=el('article',{class:'edition'},el('h3',{},item.title||'제목 확인 불가'),el('p',{class:'small'},`작품 ${item.work_id} · ${item.kind==='cover'?'표지':'설명'} · 근거 ${item.rights_id}`));
    const run=metadataMutation(life,{
      pending:state=>{queuePending(box,state);for(const input of controls)input.disabled=state.busy||state.uncertain||state.done;save.disabled=state.busy||state.done;save.textContent=state.uncertain?'같은 권리 요청 다시 시도':'권리 검토 저장';},
      committed:()=>status.replaceChildren(notice('권리 판정을 저장했습니다. 최신 목록은 다시 조회해 주세요.')),
      failed:(error,uncertain)=>status.replaceChildren(notice(message(error)+(uncertain?' 입력을 유지했습니다. 같은 권리 요청을 다시 시도해 주세요.':''),'error'))
    });
    form.append(field('권리 판정',state),field('공식성',officiality),field('작품 일치 여부',match),
      el('div',{class:'row'},el('label',{},display,' 표시 허용'),el('label',{},cache,' 캐시 허용'),el('label',{},derivative,' 파생물 허용')),
      field('캐시 만료',expiry),field('캐시 만료 시각 · 현재 기기 시간대',until),el('label',{},noExpiry,' 만료 없는 캐시가 근거에 명시되어 있음을 확인했습니다.'),
      el('div',{class:'row'},el('label',{},backup,' 백업 허용'),el('label',{},allCopies,' 모든 사본 삭제 의무')),
      el('p',{class:'small'},'백업 불가 또는 모든 사본 삭제 의무가 있으면 캐시를 허용할 수 없습니다.'),
      el('p',{class:'small'},'기존 근거: '+JSON.stringify(current.evidence).slice(0,400)),field('확인한 새 근거 참조 · 200자 이내',evidence),save,status);
    form.addEventListener('submit',event=>{
      event.preventDefault();
      try{
        const decision=metadataRightsDecision(item,{state:state.value,officiality:officiality.value,match_state:match.value,
          display_allowed:display.checked,cache_allowed:cache.checked,derivative_allowed:derivative.checked,
          backup_allowed:backup.checked,all_copies_deletion_required:allCopies.checked,
          expiry_mode:expiry.value,cache_until_ms:until.value===originalUntil?current.cache_until_ms:Date.parse(until.value),no_expiry_confirmed:noExpiry.checked,evidence_reference:evidence.value});
        run(`/api/v1/admin/metadata/rights/${encodeURIComponent(item.rights_id)}/review`,{work_id:item.work_id,kind:item.kind,expected_version:item.version,decision});
      }catch(error){status.replaceChildren(notice(message(error),'error'));}
    });
    box.append(form);return box;
  }
  meta.append(queueLoad,queueStatus,queue);

  const editWork=el('input',{type:'text',maxlength:200,'aria-label':'설명 편집 작품 ID'}),editor=el('div'),editStatus=el('div');
  const editReads=metadataReads(life),drafts=new Map();let editLoading=false,editLocked=false,editRequest=0,updateEditor=()=>{};
  const editLoad=button('설명 상태 조회',loadEditor);
  editWork.addEventListener('input',()=>{editReads.invalidate();updateEditor();});
  async function loadEditor(){
    if(editLocked||life.disposed)return;
    const id=editWork.value;
    if(!id.trim()||Array.from(id).length>200){editStatus.replaceChildren(notice('작품 ID를 1~200자로 입력하세요.','error'));return;}
    const token=editReads.next(),read=++editRequest;editLoading=true;editLoad.disabled=true;updateEditor();
    try{
      const result=await request(`/api/v1/admin/metadata/works/${encodeURIComponent(id)}`,{signal:life.signal});
      if(!editReads.current(token)||editWork.value!==id||editLocked)return;
      renderEditor(metadataEditorSnapshot(id,result));editStatus.replaceChildren();
    }catch(error){if(editReads.current(token))editStatus.replaceChildren(notice(message(error)+' 작성 중인 본문을 유지했습니다.','error'));}
    finally{if(!life.disposed&&read===editRequest){editLoading=false;editLoad.disabled=editLocked;updateEditor();}}
  }
  function renderEditor(snapshot){
    const draft=drafts.get(snapshot.work_id)||{text:'',rights_id:''};
    const text=el('textarea',{'aria-label':'새 설명 본문',rows:4,maxlength:16384,value:draft.text});
    const rights=el('select',{'aria-label':'설명 게시 권리 근거'},options([['','권리 근거 선택'],...snapshot.rights.map(r=>[r.rights_id,r.rights_id])],draft.rights_id));
    const status=el('div'),save=el('button',{type:'submit',class:'primary'},'설명 게시'),form=el('form');let mutationState={busy:false,uncertain:false,done:false};
    const saveDraft=()=>drafts.set(snapshot.work_id,{text:text.value,rights_id:rights.value});text.addEventListener('input',saveDraft);rights.addEventListener('change',saveDraft);
    updateEditor=()=>{const locked=mutationState.busy||mutationState.uncertain||mutationState.done;text.disabled=locked;rights.disabled=locked;save.disabled=mutationState.busy||mutationState.done||editLoading||editWork.value!==snapshot.work_id||!snapshot.rights.length;};
    const run=metadataMutation(life,{
      pending:state=>{mutationState=state;editLocked=state.busy||state.uncertain;editWork.disabled=editLocked;editLoad.disabled=editLoading||editLocked;if(editLocked)editReads.invalidate();save.textContent=state.uncertain?'같은 설명 요청 다시 시도':'설명 게시';updateEditor();},
      committed:()=>{drafts.delete(snapshot.work_id);status.replaceChildren(notice('설명을 저장했습니다. 최신 상태는 다시 조회해 주세요.'));},
      failed:(error,uncertain)=>status.replaceChildren(notice(message(error)+' 작성 중인 본문을 유지했습니다.'+(uncertain?' 같은 설명 요청을 다시 시도해 주세요.':''),'error'))
    });
    form.append(el('p',{class:'small'},`편집 대상 작품 ${snapshot.work_id}`),field('설명 게시 권리 근거 · 직접 선택',rights),field('새 설명 본문',text),save,status);
    if(!snapshot.rights.length)form.append(notice('현재 설명을 게시할 수 있는 권리 근거가 없습니다. 표시·캐시 허용, 만료와 보존 의무를 먼저 검토하세요.'));
    form.addEventListener('submit',event=>{event.preventDefault();if(editLoading||editWork.value!==snapshot.work_id)return;try{saveDraft();const action=metadataDescriptionAction(snapshot,rights.value,text.value);run(action.path,action.body);}catch(error){status.replaceChildren(notice(message(error),'error'));}});
    editor.replaceChildren(form);updateEditor();
  }
  const edit=el('section',{class:'panel'},el('h2',{},'설명 편집'),field('설명 편집 작품 ID',editWork),editLoad,editStatus,editor);
  return el('div',{},meta,edit);
}
