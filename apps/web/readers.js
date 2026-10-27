import {request, apiPath, selectionParams, sameOrigin, Lifetime} from './api.js';
import {el, button, field, notice, heading} from './dom.js';
function source(ctx) {
  const p=ctx.params;
  const result={edition_id:p.get('edition_id'),revision_id:p.get('revision_id'),option_id:p.get('download_option_id'),asset_position:p.get('asset_position')};
  if(!p.get('work')||!result.edition_id||!result.revision_id||!result.option_id||!/^\d+$/.test(result.asset_position||''))throw new Error('Missing explicit source');
  return result;
}
export async function textReader(ctx) {
  const {main,params,life}=ctx;const choice=source(ctx), id=params.get('work'), selected=selectionParams(choice);
  main.append(heading('텍스트 읽기','선택한 판본과 원본의 검증된 청크를 읽습니다.'));
  const controls=el('div',{class:'toolbar'}), chapters=el('select',{'aria-label':'목차'}), status=notice('텍스트 미리보기를 불러오고 있습니다.');
  const paper=el('article',{class:'reader-paper','aria-label':'텍스트 본문'});const navigation=el('div',{class:'toolbar'});
  main.append(controls,status,paper,navigation);
  let chunkLife=new Lifetime(), cursor=null, next=null, generation=0, history=[], chapter=null,retryLoad=null;
  life.add(()=>chunkLife.dispose());
  const previous=button('이전 청크',()=>{if(history.length)load(history.at(-1),'backward');});
  const more=button('다음 청크',()=>{if(next)load(next,'forward');});
  const retry=button('같은 청크 다시 시도',()=>{if(retryLoad)load(...retryLoad);});retry.hidden=true;
  navigation.append(previous,more,retry);
  async function load(target=null,movement='replace',jump=null){
    if(life.disposed)return;
    const current=++generation;chunkLife.dispose();const requestLife=chunkLife=new Lifetime();previous.disabled=more.disabled=retry.disabled=true;retry.hidden=true;retryLoad=null;status.textContent='본문을 불러오고 있습니다.';
    try {
      const response=await request(apiPath(id,'/preview',{...selected,...(target?{cursor:target}:jump!==null?{chapter:jump}:chapter!==null?{chapter}: {})}),{signal:requestLife.signal,raw:true});
      if(!response.headers.get('content-type')?.toLowerCase().startsWith('text/plain'))throw new Error('Invalid text response');
      const text=await response.text();if(current!==generation||life.disposed||requestLife.signal.aborted)return;
      if(text.length>512*1024)throw new Error('Text window too large');
      if(movement==='forward'){history.push(cursor);if(history.length>100)history.shift();}else if(movement==='backward')history.pop();else if(jump!==null){history=[];chapter=jump;}
      cursor=target;next=response.headers.get('x-preview-next-cursor');
      // Three server chunks at most; no whole-original download or HTML parsing.
      if(movement!=='forward')paper.replaceChildren();const p=el('p');p.textContent=text;paper.append(p);while(paper.childNodes.length>3)paper.firstChild.remove();
      status.textContent=`청크 ${response.headers.get('x-preview-chunk')||'확인됨'} · 현재 화면 최대 3청크${next?'':' · 마지막 청크'}`;
      previous.disabled=!history.length;more.disabled=!next;
    }catch(error){
      if(current!==generation||life.disposed||requestLife.signal.aborted||error.name==='AbortError')return;
      previous.disabled=!history.length;more.disabled=!next;retryLoad=[target,movement,jump];retry.hidden=false;retry.disabled=false;
      status.textContent='본문을 불러오지 못했습니다. 같은 청크를 다시 시도해 주세요.';ctx.fail(error);
    }
  }
  controls.append(button('작게',()=>{paper.classList.remove('font-large');paper.classList.add('font-small');}),button('크게',()=>{paper.classList.remove('font-small');paper.classList.add('font-large');}));
  const toc=await request(apiPath(id,'/chapters',selected),{signal:life.signal});if(life.disposed)return;
  if(toc.chapters.length){let start=0;const refresh=()=>{chapters.replaceChildren(el('option',{value:''},'목차 선택'),...toc.chapters.slice(start,start+100).map((item,index)=>el('option',{value:start+index},`${item.number??start+index+1} ${item.unit||''}`)));};refresh();chapters.addEventListener('change',()=>{if(chapters.value!=='')load(null,'replace',Number(chapters.value));});controls.append(field('목차',chapters),button('이전 목차',()=>{start=Math.max(0,start-100);refresh();}),button('다음 목차',()=>{if(start+100<toc.chapters.length){start+=100;refresh();}}));}
  else controls.append(notice(toc.review_reason?'목차 검토 대기: '+toc.review_reason:'확인된 목차가 없습니다. 청크 순서로 읽을 수 있습니다.'));
  await load();
}
function pageHeaders(response,requested){
  const raw=response.headers.get('x-preview-page-count'), page=response.headers.get('x-preview-page');
  if(!/^\d+$/.test(raw||'')||!/^\d+$/.test(page||''))throw new Error('Missing validated page metadata');
  const count=Number(raw);if(!Number.isSafeInteger(count)||count<1||count>5000||Number(page)!==requested||requested>count)throw new Error('Invalid page metadata');
  if(!response.headers.get('content-type')?.startsWith('image/png'))throw new Error('Invalid image response');
  return count;
}
export async function imageReader(ctx){
  if(ctx.params.get('format')==='pdf')return pdfReader(ctx);
  const {main,params,life}=ctx;const choice=source(ctx),id=params.get('work'),selected=selectionParams(choice);
  main.append(heading('이미지 읽기','검증된 페이지 수를 기준으로 이동합니다.'));
  const controls=el('div',{class:'toolbar'}),status=notice('페이지를 확인하고 있습니다.'),images=el('div',{class:'reader-images vertical'}),thumbs=el('div',{class:'row','aria-label':'근처 페이지 미리보기'});
  let current=1,count=null,pageLife=new Lifetime(),epoch=0,rotation=0;life.add(()=>pageLife.dispose());
  const number=el('input',{type:'number',min:1,value:1,'aria-label':'페이지 번호'});
  const prev=button('이전',()=>load(current-1)),next=button('다음',()=>load(current+1));
  controls.append(prev,number,button('이동',()=>load(Number(number.value))),next,button('세로 / 가로',()=>images.classList.toggle('vertical')),button('회전',()=>{rotation=(rotation+90)%360;images.firstChild?.setAttribute('class',`rotate-${rotation}`);}));main.append(controls,status,images,thumbs);
  async function load(page){
    if(!Number.isInteger(page)||page<1||(count!==null&&page>count))return;
    const thisEpoch=++epoch;pageLife.dispose();pageLife=new Lifetime();prev.disabled=next.disabled=true;images.replaceChildren();thumbs.replaceChildren();status.textContent='권한과 페이지를 확인하고 있습니다.';
    try {
      const path=apiPath(id,`/pages/${page}`,selected);
      const head=await request(path,{method:'HEAD',signal:pageLife.signal});const headCount=pageHeaders(head,page);
      const response=await request(path,{raw:true,signal:pageLife.signal});const verified=pageHeaders(response,page);if(verified!==headCount)throw new Error('Changed page count');
      const blob=await response.blob();if(thisEpoch!==epoch||life.disposed)return;if(blob.size>32*1024*1024)throw new Error('Image too large');
      count=verified;current=page;number.value=page;number.max=count;
      images.append(el('img',{src:pageLife.objectURL(blob),alt:`선택 원본 ${current} / ${count} 페이지`,class:`rotate-${rotation}`}));
      status.textContent=`${current} / ${count} 페이지`;prev.disabled=current<=1;next.disabled=current>=count;
      // At most five thumbnails, each tied to this source's validated metadata.
      for(let n=Math.max(1,current-2);n<=Math.min(count,current+2);n++){
        const target=n, thumb=button(`${n}쪽`,()=>load(target));thumbs.append(thumb);
        request(apiPath(id,`/pages/${n}`,{...selected,thumbnail:true}),{raw:true,signal:pageLife.signal}).then(async r=>{pageHeaders(r,target);const b=await r.blob();if(thisEpoch!==epoch||life.disposed)return;if(b.size<=2*1024*1024)thumb.prepend(el('img',{src:pageLife.objectURL(b),alt:'',class:'thumbnail'}));}).catch(e=>{if(e.name!=='AbortError')thumb.append(' · 미리보기 불가');});
      }
    }catch(error){if(error.name!=='AbortError')ctx.fail(error);}
  }
  await load(1);
}
async function pdfReader(ctx){
  const {main,params,life}=ctx;const choice=source(ctx),id=params.get('work');
  main.append(heading('PDF 읽기','선택한 PDF 자료를 페이지별로 읽습니다.'));
  const status=notice('PDF를 준비하고 있습니다.'),controls=el('div',{class:'toolbar'}),images=el('div',{class:'reader-images vertical'});main.append(controls,status,images);
  const path=apiPath(id,'/preview',selectionParams(choice));
  const head=await request(path,{method:'HEAD',signal:life.signal});if(!head.headers.get('content-type')?.startsWith('application/pdf'))throw new Error('Not a PDF');
  const pdf=await import('/assets/vendor/pdfjs/build/pdf.mjs');if(life.disposed)return;
  if(pdf.version!=='6.4.299')throw new Error('PDF version mismatch');
  pdf.GlobalWorkerOptions.workerSrc='/assets/vendor/pdfjs/build/pdf.worker.mjs';
  const loading=pdf.getDocument({url:sameOrigin(path).href,withCredentials:true,disableAutoFetch:true,disableStream:true,rangeChunkSize:65536,isEvalSupported:false,useSystemFonts:false,cMapUrl:'/assets/vendor/pdfjs/cmaps/',cMapPacked:true,standardFontDataUrl:'/assets/vendor/pdfjs/standard_fonts/',wasmUrl:'/assets/vendor/pdfjs/wasm/',iccUrl:'/assets/vendor/pdfjs/iccs/'});
  let document=null,render=null,page=null,epoch=0,current=1,rotation=0;
  life.add(()=>{epoch++;render?.cancel();page?.cleanup();loading.destroy().catch(()=>{});images.replaceChildren();});
  document=await loading.promise;if(life.disposed)return;
  const number=el('input',{type:'number',min:1,max:document.numPages,value:1,'aria-label':'PDF 페이지'});
  const prev=button('이전',()=>paint(current-1)),next=button('다음',()=>paint(current+1));
  controls.append(prev,number,button('이동',()=>paint(Number(number.value))),next,button('회전',()=>{rotation=(rotation+90)%360;paint(current);}),button('세로 / 가로',()=>images.classList.toggle('vertical')));
  async function paint(n){
    if(!Number.isInteger(n)||n<1||n>document.numPages)return;
    const thisEpoch=++epoch;render?.cancel();page?.cleanup();images.replaceChildren();status.textContent='페이지를 렌더링하고 있습니다.';
    try{
      const loaded=await document.getPage(n);if(thisEpoch!==epoch||life.disposed){loaded.cleanup();return;}page=loaded;
      const unit=page.getViewport({scale:1,rotation});const scale=Math.min(1.5,Math.max(0.1,(main.clientWidth-40)/unit.width),2048/unit.width,2048/unit.height);
      const viewport=page.getViewport({scale,rotation}),canvas=el('canvas',{'aria-label':`PDF ${n} / ${document.numPages} 페이지`,role:'img'});canvas.width=Math.ceil(viewport.width);canvas.height=Math.ceil(viewport.height);images.append(canvas);
      render=page.render({canvasContext:canvas.getContext('2d'),viewport});await render.promise;if(thisEpoch!==epoch||life.disposed)return;
      current=n;number.value=n;prev.disabled=n===1;next.disabled=n===document.numPages;status.textContent=`${n} / ${document.numPages} 페이지 · 화면 한 페이지 유지`;
    }catch(error){if(error.name!=='RenderingCancelledException'&&!life.disposed)ctx.fail(error);}
  }
  await paint(1);
}
