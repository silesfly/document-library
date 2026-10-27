import {request,apiPath,errorText} from './api.js';
import {el,button,field,notice,heading,options} from './dom.js';
import {ratingText,workCard} from './cards.js';
import {FILTER_NAMES,filterValues,appendFilters,filterControls,viewControl,viewName,backendParams} from './filters.js';

// One in-flight action, committed readback only, and explicit refresh after uncertainty.
export function memberActions(life, {pending,committed,failed}) {
  let busy=false,uncertain=false;
  return async (action, {refresh=false}={})=>{
    if(busy||life.disposed||(uncertain&&!refresh))return;
    busy=true;pending(true,uncertain);
    try{const value=await action();if(life.disposed)return;uncertain=false;committed(value);}
    catch(error){if(life.disposed)return;uncertain=true;failed(error);}
    finally{busy=false;if(!life.disposed)pending(false,uncertain);}
  };
}
export function socialPanel(ctx, initial) {
  const {life}=ctx,id=initial.work_id;let saved=initial;
  const shared=el('p'),own=el('p'),status=el('div');
  const rating=el('select',{'aria-label':'내 별점'},options([['','별점 선택'],...Array.from({length:10},(_,i)=>[String(i+1),`${(i+1)/2}점`])],''));
  const save=button('별점 저장',()=>{const value=Number(rating.value);if(!Number.isInteger(value)||value<1||value>10){status.replaceChildren(notice('0.5점부터 5점까지 선택해 주세요.','error'));return;}run(()=>request(apiPath(id,'/rating'),{method:'PUT',body:{rating_half_steps:value},signal:life.signal}));},'primary');
  const remove=button('내 별점 삭제',()=>run(()=>request(apiPath(id,'/rating'),{method:'DELETE',body:{},signal:life.signal})));
  const favorite=button('즐겨찾기 추가',()=>run(()=>request(apiPath(id,'/favorite'),{method:'PUT',body:{favorite:!saved.favorite},signal:life.signal})));
  const refresh=button('평점·즐겨찾기 새로 조회',()=>run(async()=>(await request(apiPath(id,'',{limit:1}),{signal:life.signal})).work,{refresh:true}));
  function display(){shared.textContent=ratingText(saved);own.textContent=saved.my_rating_half_steps==null?'내 별점 없음':`내 별점 ★ ${saved.my_rating_half_steps/2}`;rating.value=saved.my_rating_half_steps==null?'':String(saved.my_rating_half_steps);favorite.textContent=saved.favorite?'즐겨찾기 해제':'즐겨찾기 추가';favorite.setAttribute('aria-pressed',String(Boolean(saved.favorite)));}
  const run=memberActions(life,{
    pending:(busy,uncertain)=>{for(const control of [rating,save,remove,favorite])control.disabled=busy||uncertain;refresh.disabled=busy;remove.disabled=busy||uncertain||saved.my_rating_half_steps==null;},
    committed:value=>{saved=value;display();status.replaceChildren(notice('서버에 저장된 상태를 확인했습니다.'));},
    failed:error=>status.replaceChildren(notice(errorText(error)+' 저장 결과가 불확실할 수 있습니다. 평점·즐겨찾기를 새로 조회한 뒤 변경해 주세요.','error'))
  });
  display();remove.disabled=saved.my_rating_half_steps==null;
  return el('section',{class:'panel social-panel','aria-label':'평점과 즐겨찾기'},el('h2',{},'평점과 즐겨찾기'),shared,own,el('p',{class:'small'},'같은 작품의 텍스트·스캔 판본은 평가를 공유합니다. 각색 작품은 별도로 평가합니다.'),el('div',{class:'row'},field('내 별점 · 0.5점 단위',rating),save,remove,favorite,refresh),status);
}
export const RECOMMENDATION_MODES=[['personalized','내 취향 추천'],['top_rated','높은 평점'],['popular','인기 작품'],['unrated','내 미평가 작품']];
export function recommendationReason(item) {
  const labels={similar_taste:'비슷한 평가 취향을 바탕으로 추천',top_rated:'공유 가중평점 순위',popular:'인기 지표를 바탕으로 추천',preferred_genre:'선택한 장르 안의 인기 작품',unrated:'내가 아직 평가하지 않은 작품'};
  return labels[item.reason]||'현재 추천 조건에 맞는 작품';
}
const FALLBACKS={insufficient_ratings:'내가 평가한 이용 가능 작품이 5개 미만이어서 인기순으로 안내합니다. 5개 이상 평가하면 취향 추천을 계산합니다.',insufficient_neighbors:'평가가 충분히 겹치는 취향 정보가 없어 인기순으로 안내합니다.',no_personalized_candidates:'현재 조건에서 취향을 예측할 작품이 없어 인기순으로 안내합니다.'};
function exclusions(values) {
  const rated=el('input',{type:'checkbox',checked:values.exclude_rated}),downloaded=el('input',{type:'checkbox',checked:values.exclude_downloaded});
  return {node:el('div',{class:'filters'},el('label',{},rated,'내가 평가한 작품 제외'),el('label',{},downloaded,'다운로드 완료 기록이 있는 작품 제외')),controls:[rated,downloaded],read:()=>({exclude_rated:rated.checked,exclude_downloaded:downloaded.checked})};
}
export function recommendationParams(params,values,excluded,limit) {
  const query=appendFilters(backendParams(params),values,true);
  for(const name of ['exclude_rated','exclude_downloaded'])query.set(name,String(excluded[name]));
  query.set('limit',limit);return query;
}
export async function recommendationView(ctx) {
  const {main,params,life,go}=ctx;
  main.append(heading('추천 작품','공유 평가와 내 취향을 바탕으로 작품을 찾아보세요.'));
  const saved=await request('/api/v1/me/preferences',{signal:life.signal});if(life.disposed)return;
  const mode=el('select',{'aria-label':'추천 방식'},options(RECOMMENDATION_MODES,params.get('mode')||'personalized'));
  const limit=el('select',{'aria-label':'추천 작품 수'},options([['20','20개 작품'],['50','50개 작품']],params.get('limit')==='50'?'50':'20'));
  const view=viewControl(viewName(params,saved.view)),filters=filterControls(filterValues(params,saved.filters));
  const exclude=exclusions(Object.fromEntries(['exclude_rated','exclude_downloaded'].map(name=>[name,params.has(name)?params.get(name)==='true':saved.recommendations[name]])));
  const form=el('form',{class:'panel'},el('div',{class:'toolbar'},field('추천 방식',mode),field('추천 작품 수',limit),field('보기 방식',view)),filters.node,exclude.node,el('button',{class:'primary',type:'submit'},'추천 조건 적용'),button('저장된 추천 기본값',()=>go('recommendations')));
  form.addEventListener('submit',event=>{event.preventDefault();const query=recommendationParams(new URLSearchParams({mode:mode.value}),filters.read(),exclude.read(),limit.value);query.set('view',view.value);go('recommendations',query);});
  main.append(form,notice('미평가 방식은 평가한 작품을 항상 제외합니다. 다운로드 완료 기록은 독서 완료를 뜻하지 않습니다.'));
  const query=recommendationParams(params,filters.read(),exclude.read(),limit.value);
  const result=await request('/api/v1/recommendations?'+query,{signal:life.signal});if(life.disposed)return;
  if(result.fallback_reason)main.append(notice(FALLBACKS[result.fallback_reason]||'현재 조건에서는 기본 순위를 안내합니다.'));
  main.append(notice(result.items.length?`이번 추천 ${result.items.length}개 · 선택한 ${limit.value}개 한도`:'조건에 맞는 추천 작품이 없습니다.'));
  const grid=el('div',{class:'books'+(view.value==='list'?' books-list':'')});for(const item of result.items)grid.append(workCard(item.work,go,recommendationReason(item)));main.append(grid);
}
export function preferencesPayload(filters,view,recommendations) {return {filters:Object.fromEntries(FILTER_NAMES.map(name=>[name,[...filters[name]]])),view,recommendations:{exclude_rated:recommendations.exclude_rated,exclude_downloaded:recommendations.exclude_downloaded}};}
export async function preferencesView(ctx) {
  const {main,life}=ctx;main.append(heading('회원 선호','서재·추천의 기본 조건을 계정에 저장합니다.'));
  const saved=await request('/api/v1/me/preferences',{signal:life.signal});if(life.disposed)return;
  const form=el('form',{class:'panel'}),body=el('div'),status=el('div');let filters,view,exclude;
  const save=el('button',{class:'primary',type:'submit'},'선호 저장');
  const refresh=button('저장된 선호 새로 조회',()=>run(()=>request('/api/v1/me/preferences',{signal:life.signal}),{refresh:true}));
  function display(value){filters=filterControls(value.filters);view=viewControl(value.view);exclude=exclusions(value.recommendations);body.replaceChildren(filters.node,field('기본 보기 방식',view),exclude.node);}
  const run=memberActions(life,{
    pending:(busy,uncertain)=>{for(const control of [...filters.controls,view,...exclude.controls,save])control.disabled=busy||uncertain;refresh.disabled=busy;},
    committed:value=>{display(value);status.replaceChildren(notice('회원 선호를 서버에서 확인했습니다.'));},
    failed:error=>status.replaceChildren(notice(errorText(error)+' 입력을 유지했습니다. 저장된 선호를 새로 조회한 뒤 변경해 주세요.','error'))
  });
  display(saved);form.append(body,el('div',{class:'row'},save,refresh),status);
  form.addEventListener('submit',event=>{event.preventDefault();const payload=preferencesPayload(filters.read(),view.value,exclude.read());run(()=>request('/api/v1/me/preferences',{method:'PUT',body:payload,signal:life.signal}));});
  main.append(form,notice('저장 버튼을 눌렀을 때만 변경됩니다. 새로 조회하면 저장하지 않은 입력이 서버의 값으로 바뀝니다. 다운로드 기록 제외는 독서 완료 여부와 다릅니다.'));
}
