import {el,field,options,button,notice} from './dom.js';

export const FILTER_NAMES=['categories','states','formats','representations','genres'];
const GROUPS=[
  ['categories','자료 분류 · 여러 선택은 OR',[['novel_text','소설 텍스트'],['novel_scan','스캔'],['webtoon','웹툰'],['comic','만화'],['document','문서'],['unknown','미분류']]],
  ['representations','표현형 · 여러 선택은 모두 보유',[['text','텍스트'],['imagepack','이미지'],['document','문서'],['single_image','단일 이미지']]],
  ['states','연재 상태',[['completed','완결'],['ongoing','연재 중'],['unknown','확인 중']]],
  ['formats','파일 형식',['txt','epub','pdf','cbz','zip','png','jpg'].map(x=>[x,x.toUpperCase()])]
];
export function filterValues(params, inherited = {}) {
  return Object.fromEntries(FILTER_NAMES.map(name=>[name,params.has(name)?params.getAll(name).filter(Boolean):inherited[name]||[]]));
}
export function appendFilters(query, values, explicitEmpty = false) {
  for(const name of FILTER_NAMES){query.delete(name);const items=[...new Set(values[name])];if(items.length)for(const value of items)query.append(name,value);else if(explicitEmpty)query.set(name,'');}
  return query;
}
export function filterControls(values = {}) {
  const node=el('div'),controls=[];
  for(const [name,title,entries]of GROUPS){
    const set=el('fieldset',{class:'filters'},el('legend',{},title));
    // Preserve server-supported values absent from the short common-format list.
    const extra=(values[name]||[]).filter(value=>!entries.some(([key])=>key===value));
    for(const [value,text]of [...entries,...extra.map(value=>[value,value])]){const input=el('input',{type:'checkbox',name,value,checked:(values[name]||[]).includes(value)});controls.push(input);set.append(el('label',{},input,text));}
    node.append(set);
  }
  const genres=[],selected=el('div',{class:'filters'}),status=el('div');
  const group=el('fieldset',{class:'filters'},el('legend',{},'장르 · 여러 선택은 OR'),selected);
  function addGenre(value){
    const control=el('input',{type:'checkbox',name:'genres',value,checked:true});
    // The closure retains exact server strings even when a DOM value is normalized.
    genres.push({value,control});controls.push(control);selected.append(el('label',{},control,value));
  }
  for(const value of values.genres||[])addGenre(value);
  const genre=el('input',{type:'text',name:'new_genre',placeholder:'장르 한 개를 입력','aria-label':'추가할 장르'});
  const add=button('장르 추가',()=>{
    if(genre.disabled||add.disabled)return;
    const value=genre.value,existing=genres.find(entry=>entry.value===value);
    if(value===''||[...value].length>128){status.replaceChildren(notice('장르는 1자부터 128자까지 한 개씩 입력해 주세요.','error'));return;}
    if(!existing?.control.checked&&genres.filter(entry=>entry.control.checked).length>=64){status.replaceChildren(notice('장르는 최대 64개까지 선택할 수 있습니다.','error'));return;}
    if(existing)existing.control.checked=true;else addGenre(value);
    genre.value='';status.replaceChildren();
  });
  genre.addEventListener('keydown',event=>{if(event.key==='Enter')event.preventDefault();});
  controls.push(genre,add);group.append(field('장르 한 개 추가 · 쉼표도 이름의 일부로 저장됩니다',genre),add,el('p',{class:'small'},'선택을 해제하면 해당 장르를 제외합니다.'),status);node.append(group);
  return {node,controls,read:()=>Object.fromEntries(FILTER_NAMES.map(name=>[name,name==='genres'?genres.filter(entry=>entry.control.checked).map(entry=>entry.value):controls.filter(x=>x.name===name&&x.checked).map(x=>x.value)]))};
}
export function viewControl(value) {
  return el('select',{'aria-label':'보기 방식'},options([['grid','격자 보기'],['list','목록 보기']],value==='list'?'list':'grid'));
}
export function viewName(params, fallback) {return ['grid','list'].includes(params.get('view'))?params.get('view'):fallback==='list'?'list':'grid';}
export function backendParams(params) {const query=new URLSearchParams(params);query.delete('view');return query;}
