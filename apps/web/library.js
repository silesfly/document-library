import {request, apiPath} from './api.js';
import {el, button, field, notice, heading, bytes, labels, coverage, options} from './dom.js';
import {downloadPanel} from './transfers.js';
import {workCard,workCover} from './cards.js';
import {filterControls,filterValues,appendFilters,viewControl,viewName,backendParams} from './filters.js';
import {socialPanel} from './member.js';
const SORTS = [['latest','최신 등록'],['updated','최근 업데이트'],['title','제목'],['author','작가'],['rating','가중평점'],['votes','평가 수'],['popularity','인기'],['episode','회차 많은 순'],['size','용량']];
export async function favoritesView(ctx) {return libraryView({...ctx,favoriteOnly:true});}
export async function libraryView(ctx) {
  const {main, params, life, go,favoriteOnly=false} = ctx,route=favoriteOnly?'favorites':'library';
  main.append(heading(favoriteOnly?'내 즐겨찾기':'내 서재',favoriteOnly?'내 계정에 저장한 작품을 모아봅니다.':'소설, 스캔, 웹툰과 만화. 모든 판본을 한곳에서.'));
  const saved=await request('/api/v1/me/preferences',{signal:life.signal});if(life.disposed)return;
  const query=backendParams(params),inherit=!favoriteOnly&&query.size===0;
  const values=filterValues(params,inherit?saved.filters:{});
  if(inherit)appendFilters(query,values);
  if(!query.has('sort'))query.set('sort','latest');if(!query.has('direction'))query.set('direction',['title','author'].includes(query.get('sort'))?'asc':'desc');
  if(favoriteOnly)query.set('favorite','true');
  const form = el('form', {class: 'toolbar'});
  const search = el('input', {class: 'search', type: 'search', maxlength: 200, value: params.get('search') || '', placeholder: '제목, 작가 검색', 'aria-label':'제목, 작가 검색'});
  const sort = el('select', {'aria-label':'정렬'}, options(SORTS, query.get('sort')));
  const direction = el('select', {'aria-label':'정렬 방향'}, options([['desc','내림차순'],['asc','오름차순']], query.get('direction')));
  const view=viewControl(viewName(params,saved.view));
  form.append(search,sort,direction,view,el('button',{class:'primary',type:'submit'},'검색'));
  const filters=el('details',{},el('summary',{},'분류 · 상세 필터')),common=filterControls(values),fields=[];filters.append(common.node);
  for (const [name,value,text] of [['unrated','true','미평가'],['favorite','true','즐겨찾기'],['has_summary','false','요약 대기'],['downloaded','true','다운로드 완료 기록'],['has_gaps','true','누락 회차']]) {
    if(favoriteOnly&&name==='favorite')continue;
    const input=el('input',{type:'checkbox',name,value,checked:params.get(name)===value});fields.push(input);filters.append(el('label',{class:'small'},input,' '+text+' '));
  }
  filters.append(button('필터 적용',()=>form.requestSubmit()),button('필터 모두 지우기',()=>go(route,{sort:'latest',direction:'desc',view:view.value})),button('저장된 기본 필터',()=>go('library')));
  form.addEventListener('submit',event=>{event.preventDefault();const next=appendFilters(new URLSearchParams({sort:sort.value,direction:direction.value,view:view.value}),common.read());if(search.value.trim())next.set('search',search.value.trim());for(const input of fields)if(input.checked)next.set(input.name,input.value);if(favoriteOnly)next.set('favorite','true');go(route,next);});
  main.append(form,filters);
  if(inherit)main.append(notice('저장된 기본 필터를 적용했습니다. 필터 모두 지우기로 전체 조건을 볼 수 있습니다.'));
  const status=notice('서재를 불러오고 있습니다.'),grid=el('div',{class:'books'+(view.value==='list'?' books-list':'')});main.append(status,grid);
  query.set('limit','20');const result=await request('/api/v1/items?'+query,{signal:life.signal});if(life.disposed)return;
  status.replaceWith(notice(result.items.length?`이번 페이지 ${result.items.length}개 작품`:'조건에 맞는 작품이 없습니다.'));
  for(const work of result.items)grid.append(workCard(work,go));
  if(result.next_cursor)main.append(button('다음 작품',()=>{const next=new URLSearchParams(query);next.delete('limit');next.set('cursor',result.next_cursor);next.set('view',view.value);go(route,next);}));
  main.append(notice('평점과 즐겨찾기는 작품 상세에서 변경할 수 있습니다. 소개·요약·표지는 현재 제공 가능한 내용을 표시합니다.'));
}
export async function detailView(ctx) {
  const {main,params,life,go}=ctx;const id=params.get('work');if(!id)throw new Error('Missing work');
  const loading=notice('작품과 판본을 불러오고 있습니다.');main.append(loading);
  const data=await request(apiPath(id,'',{cursor:params.get('cursor'),limit:10}),{signal:life.signal});if(life.disposed)return;loading.remove();
  const work=data.work;main.append(heading(work.title,work.author||'작가 확인 중'),el('div',{class:'tags'},el('span',{class:'tag'},labels[work.publication_state]||'상태 확인 중'),el('span',{class:'tag'},work.classification_review_required?'분류 검토 필요':work.item_type)));
  main.append(workCover(work,{detail:true}));
  main.append(socialPanel(ctx,work));
  const desc=data.description||{};const descSource={EDITORIAL:'편집 입력',PUBLISHER_OFFICIAL:'출판사 공식',QWEN_TEXT:'AI 텍스트',QWEN_OCR:'AI OCR',EMBEDDED:'내장 메타데이터',FIRST_PAGE:'첫 페이지'}[desc.source]||desc.source||null;
  const descBody=desc.state==='ready'&&desc.text?el('div',{},el('p',{},desc.text),el('p',{class:'small'},'출처: '+(descSource||'알 수 없음'))):el('p',{class:'small'},'본문은 준비 완료 시 표시합니다.');
  main.append(el('section',{class:'panel'},el('h2',{},'작품 정보'),el('p',{},`소개 ${work.description_state||'대기'} · 요약 ${work.summary_state||'대기'} · 표지 ${work.cover_state||'대기'}`),descBody));
  for(const kind of ['genres','tags']) {
    const panel=el('div',{class:'tags'});main.append(el('h2',{},kind==='genres'?'장르':'태그'),panel);
    const first=work[kind]||[];panel.append(...first.map(label=>el('span',{class:'tag'},label)));
    const meta=work.label_pages?.[kind];
    let cursor=meta?.next_cursor;
    // Always provide the authoritative paged route, including when the first card page is truncated.
    const load=button(cursor?'다음 라벨':'라벨 목록 새로 조회',async()=>{
      load.disabled=true;try{const page=await request(apiPath(id,`/labels/${kind}`,{cursor,limit:64}),{signal:life.signal});if(life.disposed)return;panel.replaceChildren(...page.items.map(label=>el('span',{class:'tag'},typeof label==='string'?label:label.label||label.name||'')));cursor=page.next_cursor;load.hidden=!cursor;load.textContent='다음 라벨';}catch(e){ctx.fail(e);}finally{load.disabled=false;}
    });main.append(load);
  }
  main.append(el('h2',{},'판본 · 파일 선택'));
  let state=ctx.selections.get(id);if(!state){state={choices:new Map(),explicit:false,defaultId:null};ctx.selections.set(id,state);}
  if(!state.explicit&&state.defaultId!==work.default_option?.option_id){const value=work.default_option;state.choices.clear();state.defaultId=value?.option_id;if(value)state.choices.set(value.option_id,{...value,assets:value.asset_positions.map(asset_position=>({asset_position,file_format:value.format,byte_size:null}))});}
  const downloads=downloadPanel(ctx,id,state);const chosenCount=el('p',{class:'small'});const refresh=()=>{chosenCount.textContent=`${state.choices.size}개 옵션 선택 · 여러 원본 또는 옵션은 ZIP으로 제공합니다.`;downloads.refresh();};
  main.append(chosenCount);
  for(const edition of data.editions) {
    const panel=el('section',{class:'edition'},el('h3',{},`${edition.edition_label||edition.edition_key} · ${labels[edition.representation]||edition.representation}${edition.source_form==='scan'?' · 스캔':''}`),el('p',{class:'metadata'},`현재 판본 · ${edition.language||'언어 확인 중'}`));
    for(const option of edition.options) {
      const choice={...option,edition_id:edition.edition_id,representation:edition.representation,work_id:id};
      if(state.choices.has(option.option_id)&&state.choices.get(option.option_id).revision_id===choice.revision_id)state.choices.set(option.option_id,choice);
      if(!state.explicit&&option.option_id===state.defaultId)state.choices.set(option.option_id,choice);
      const check=el('input',{type:'checkbox',checked:state.choices.has(option.option_id),'aria-label':option.format+' 선택'});
      check.addEventListener('change',()=>{state.explicit=true;if(check.checked)state.choices.set(option.option_id,choice);else state.choices.delete(option.option_id);refresh();});
      panel.append(el('label',{},check,el('span',{},el('strong',{},option.format.toUpperCase()+(option.option_id===state.defaultId?' · 기본 선택':'')),el('br'),coverage(option.coverage,option.coverage_confidence),el('br'),`${option.delivery_kind==='bundle'?'원본 묶음':'단일 원본'} · ${option.asset_count}개 · ${bytes(option.assets.reduce((a,b)=>a+b.byte_size,0))}`)));
      const select=el('select',{'aria-label':'미리볼 원본'},option.assets.map(asset=>el('option',{value:asset.asset_position},`원본 ${asset.asset_position} · ${asset.file_format.toUpperCase()} · ${bytes(asset.byte_size)}`)));
      const read=button('읽기',()=>{const asset=option.assets.find(x=>String(x.asset_position)===select.value);go(['txt','epub'].includes(asset.file_format)?'reader':'image',{work:id,edition_id:edition.edition_id,revision_id:option.revision_id,download_option_id:option.option_id,asset_position:asset.asset_position,format:asset.file_format});});
      panel.append(el('div',{class:'row'},select,read));
    }
    main.append(panel);
  }
  if(data.next_cursor)main.append(button('다음 판본',()=>go('detail',{work:id,cursor:data.next_cursor})));
  main.append(downloads.node,notice('선택은 이 세션의 메모리에만 유지됩니다. 자료를 열 때 현재 이용 권한과 판본 상태를 다시 확인합니다.'));refresh();
}
