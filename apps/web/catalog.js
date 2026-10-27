import {request} from './api.js';
import {el,button,field,notice,options} from './dom.js';
const TYPES=[['unknown','미분류 · 검토 필요'],['novel','소설'],['webtoon','웹툰'],['comic','만화'],['document','문서']];
const STATES=[['unknown','확인 중'],['ongoing','연재 중'],['completed','완결']];
export function catalogPanel(ctx){
  const root=el('section',{class:'panel'},el('h2',{},'작품 · 판본 관리'),notice('제목 일치는 검토 힌트입니다. 원작 소설과 각색 작품을 자동으로 합치지 않습니다.'));
  const q=el('input',{type:'search',maxlength:200,'aria-label':'관리 카탈로그 검색'}),list=el('div'),editor=el('div');let cursor=null;
  const next=button('다음 작품',()=>lookup(false));next.hidden=true;
  async function lookup(reset=true){if(reset)cursor=null;try{const result=await request('/api/v1/admin/catalog/works?'+new URLSearchParams({q:q.value,limit:20,...(cursor?{cursor}:{})}),{signal:ctx.life.signal});if(ctx.life.disposed)return;list.replaceChildren();for(const work of result.works)list.append(button(`${work.title_display} · ${work.author||'작가 확인 중'} · ${work.item_type}${work.title_match_hint?' · 제목 일치 힌트':''}`,()=>choose(work)));cursor=result.next_cursor;next.hidden=!cursor;}catch(e){ctx.fail(e);}}
  function choose(work){editor.replaceChildren(el('h3',{},work.title_display),notice(`현재 분류 ${work.item_type} · 상태 ${work.publication_state} · ${work.edition_count}개 판본`),editionCreate(ctx,work),classification(ctx,work));}
  root.append(el('div',{class:'toolbar'},q,button('미게시 포함 작품 검색',()=>lookup())),list,next,editor);
  const create=el('details',{},el('summary',{},'별도 작품 만들기'));const title=el('input',{required:true,maxlength:500}),author=el('input',{maxlength:200}),type=el('select',{},options(TYPES,'unknown')),state=el('select',{},options(STATES,'unknown'));
  create.append(idempotentForm(ctx,'새 작품',()=>({title_display:title.value,author:author.value||null,item_type:type.value,publication_state:state.value}),id=>`/api/v1/admin/catalog/works/${id}`,[field('제목',title),field('작가',author),field('자료 분류',type),field('연재 상태',state)],value=>{q.value=value.title_display;lookup();}));root.append(create);return root;
}
function idempotentForm(ctx,label,bodyFor,pathFor,controls,done){
  const form=el('form'),status=notice('저장할 내용을 확인해 주세요.');let frozen=null,identifier=null,pending=false;
  const submit=el('button',{type:'submit',class:'primary'},label+' 저장'),fresh=button('새 요청으로 작성',()=>{frozen=null;identifier=null;for(const control of form.querySelectorAll('input,select,textarea'))control.disabled=false;status.textContent='새 항목을 작성합니다. 같은 제목의 별도 작품 여부를 확인해 주세요.';submit.textContent=label+' 저장';});
  form.append(...controls,submit,' ',fresh,status);
  form.addEventListener('submit',async e=>{e.preventDefault();if(pending)return;pending=true;submit.disabled=true;if(!frozen){frozen=bodyFor();identifier=crypto.randomUUID();for(const control of form.querySelectorAll('input,select,textarea'))control.disabled=true;}
    try{const result=await request(pathFor(identifier),{method:'PUT',body:frozen,signal:ctx.life.signal});if(ctx.life.disposed)return;status.textContent='저장되었습니다. 목록을 새로 조회해 확인해 주세요.';done?.(result);submit.textContent='동일 요청 결과 다시 확인';}catch(error){ctx.fail(error);status.textContent='실패했습니다. 동일 요청으로 다시 확인하거나 새 요청을 작성하세요.';submit.textContent='동일 요청 재시도';}finally{pending=false;submit.disabled=false;}});return form;
}
function editionCreate(ctx,work){
  const details=el('details',{},el('summary',{},'이 작품의 판본 만들기')),key=el('input',{required:true,maxlength:128}),label=el('input',{required:true,maxlength:200}),representation=el('select',{},options([['text','텍스트'],['imagepack','이미지 묶음'],['document','문서'],['single_image','단일 이미지']],'text')),source=el('select',{},options([['unknown','출처 형식 검토'],['born_digital','디지털 원본'],['scan','스캔']],'unknown')),language=el('input',{maxlength:64}),series=el('input',{maxlength:200});
  details.append(idempotentForm(ctx,'판본',()=>({edition_key:key.value,edition_label:label.value,representation:representation.value,source_form:source.value,language:language.value||null,source_series:series.value||null}),id=>`/api/v1/admin/catalog/works/${encodeURIComponent(work.work_id)}/editions/${id}`,[field('판본 키',key),field('판본 이름',label),field('표현형',representation),field('원본 형식',source),field('언어',language),field('출처 시리즈',series)]));return details;
}
function classification(ctx,work){
  const details=el('details',{},el('summary',{},'분류 · 연재 상태 판정')),type=el('select',{},options(TYPES,work.item_type)),state=el('select',{},options(STATES,work.publication_state)),evidence=el('input',{required:true,maxlength:500});
  details.append(idempotentForm(ctx,'판정',()=>({item_type:type.value,publication_state:state.value,expected_item_type:work.item_type,expected_publication_state:work.publication_state,evidence:evidence.value}),id=>`/api/v1/admin/catalog/works/${encodeURIComponent(work.work_id)}/classification/${id}`,[field('자료 분류',type),field('연재 상태',state),field('판정 근거',evidence)]));return details;
}
