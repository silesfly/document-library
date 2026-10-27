import {el,bytes,labels,coverage} from './dom.js';

export function ratingText(work) {
  return work.rating_votes > 0 ? `공유 평균 ★ ${Number.isFinite(work.rating_average) ? work.rating_average.toFixed(2) : '확인 중'} · ${work.rating_votes}명 평가` : work.rating_votes === 0 ? '아직 평가가 없어요' : '평가 상태 확인 중';
}
export function workCover(work, {detail = false} = {}) {
  const placeholder=el('span',{class:'cover-placeholder'},'표지 준비 중');
  const node=el('div',{class:'book-cover'+(detail?' detail-cover':'')},placeholder);
  const encoded=encodeURIComponent(work.work_id).replace(/[!'()*]/g,c=>'%'+c.charCodeAt(0).toString(16).toUpperCase());
  const prefix=`/api/v1/items/${encoded}/cover?version=`;
  const url=work.cover_url;
  if(work.cover_state!=='ready'||typeof url!=='string'||!url.startsWith(prefix))return node;
  const version=url.slice(prefix.length);
  if(!/^(0|[1-9][0-9]{0,18})$/.test(version)||BigInt(version)>9223372036854775807n)return node;
  let failed=false;
  const image=el('img',{class:'cover-image cover-loading',alt:'',loading:detail?'eager':'lazy',decoding:'async',referrerpolicy:'no-referrer',
    onload:()=>{if(!failed){placeholder.hidden=true;image.className='cover-image';}},
    onerror:()=>{failed=true;image.remove();placeholder.hidden=false;},src:url});
  node.append(image);return node;
}
export function workCard(work, go, reason = '') {
  const card=el('button',{type:'button',class:'book',onclick:()=>go('detail',{work:work.work_id}),'aria-label':work.title+' 상세 보기'},workCover(work),el('h3',{},work.title),el('div',{class:'byline'},work.author||'작가 확인 중'),el('div',{class:'tags'},(work.representations||[]).map(x=>el('span',{class:'tag'},labels[x]||x)),el('span',{class:'tag'},labels[work.publication_state]||work.publication_state)),el('div',{class:'metadata'},ratingText(work)),el('div',{class:'metadata'},work.my_rating_half_steps == null ? '내 별점 없음' : `내 별점 ★ ${work.my_rating_half_steps/2}`,work.favorite ? ' · 즐겨찾기' : ''));
  if(reason)card.append(el('p',{class:'recommendation-reason'},reason));
  const summaries=el('div',{class:'book-range'});
  for(const summary of work.edition_summaries||[])summaries.append(el('span',{},`${summary.source_form==='scan'?'스캔':labels[summary.representation]||summary.representation} · ${summary.format.toUpperCase()} · ${summary.edition_count}판본`),el('span',{},coverage(summary.coverage,summary.coverage_confidence)),el('span',{},bytes(summary.size_bytes)),el('span',{},`${summary.edition_label||'판본 확인 중'} · 갱신 ${summary.updated_at?new Date(summary.updated_at).toLocaleDateString():'확인 중'}`));
  card.append(summaries);return card;
}
