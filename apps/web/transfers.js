import {request,apiPath,selectionParams,sameOrigin,csrfToken,errorText} from './api.js';
import {el,button,notice,bytes,field,heading} from './dom.js';
const SHARE_LIMIT=20*1024*1024;
async function boundedBlob(response,limit,signal){
  const reader=response.body.getReader();const blocks=[];let size=0;
  try{while(true){if(signal.aborted)throw new DOMException('Aborted','AbortError');const {done,value}=await reader.read();if(done)break;size+=value.byteLength;if(size>limit)throw new Error('Share limit');blocks.push(value);}return new Blob(blocks,{type:response.headers.get('content-type')||'application/octet-stream'});}finally{await reader.cancel().catch(()=>{});}
}
export function downloadPanel(ctx,id,state){
  const node=el('section',{class:'panel'},el('h2',{},'선택 자료 받기')),status=notice('원본 하나는 직접 다운로드, 여러 원본은 일회용 ZIP으로 제공합니다.');
  let prepared=null,preparedSignature=null,busy=false;
  ctx.life.add(()=>{prepared=null;preparedSignature=null;});
  const choices=()=>[...state.choices.values()];const signature=()=>choices().map(x=>x.option_id+':'+x.revision_id).sort().join('|');
  async function target(){
    const selected=choices();if(!selected.length)throw new Error('No selection');
    if(selected.length===1&&selected[0].assets.length===1){const choice=selected[0];return{url:apiPath(id,'/download',selectionParams({...choice,asset_position:choice.assets[0].asset_position})),name:`자료.${choice.format}`,single:true};}
    const result=await request(apiPath(id,'/download-selections'),{method:'POST',signal:ctx.life.signal,body:{delivery:'zip',selection:selected.map(x=>({edition_id:x.edition_id,revision_id:x.revision_id,download_option_id:x.option_id}))}});
    sameOrigin(result.download_url);if(!/^\/api\/v1\/download-tickets\/[^/?#]+$/.test(result.download_url))throw new Error('Unsafe ticket');
    return{url:result.download_url,name:'선택자료.zip',single:false};
  }
  function launch(url,name){const anchor=el('a',{href:url,download:name,rel:'noreferrer'});node.append(anchor);anchor.click();anchor.remove();}
  const download=button('다운로드',async()=>{if(busy)return;busy=true;refresh();try{const item=await target();await request(item.url,{method:'HEAD',signal:ctx.life.signal});if(ctx.life.disposed)return;launch(item.url,item.name);status.textContent='다운로드를 요청했습니다. 브라우저에서 진행 상태를 확인해 주세요. 중간 실패 시 새로 요청해야 합니다.';}catch(error){ctx.fail(error);}finally{busy=false;refresh();}},'primary');
  const prepare=button('공유할 파일 준비',async()=>{
    if(busy)return;busy=true;refresh();prepared=null;
    try{
      if(!navigator.share||!navigator.canShare){status.textContent='이 기기는 파일 공유를 지원하지 않습니다. 다운로드를 사용해 주세요.';return;}
      const sig=signature(),item=await target(),head=await request(item.url,{method:'HEAD',signal:ctx.life.signal});
      const raw=head.headers.get('content-length');if(!/^\d+$/.test(raw||'')||Number(raw)>SHARE_LIMIT){status.textContent='크기를 확인할 수 없거나 20MB를 넘는 파일입니다. 다운로드를 사용해 주세요.';return;}
      const response=await request(item.url,{raw:true,signal:ctx.life.signal}),blob=await boundedBlob(response,SHARE_LIMIT,ctx.life.signal);if(ctx.life.disposed||sig!==signature())return;
      if(blob.size!==Number(raw))throw new Error('Changed share length');
      const file=new File([blob],item.name,{type:blob.type});if(!navigator.canShare({files:[file]})){status.textContent='이 파일 형식의 OS 공유를 지원하지 않습니다. 다운로드를 사용해 주세요.';return;}
      prepared=file;preparedSignature=sig;status.textContent='파일이 준비되었습니다. 아래 OS 공유 버튼을 눌러 앱을 선택하세요.';
    }catch(error){ctx.fail(error);}finally{busy=false;refresh();}
  });
  // navigator.share is called in a fresh user gesture, after the bounded file is ready.
  const share=button('OS 공유',()=>{if(!prepared||preparedSignature!==signature())return;const file=prepared;navigator.share({files:[file]}).then(()=>{status.textContent='OS 공유 요청이 완료되었습니다.';},e=>{status.textContent=e.name==='AbortError'?'공유를 취소했습니다.':'OS 공유에 실패했습니다. 다운로드를 사용해 주세요.';});});
  function refresh(){const selected=choices();download.disabled=prepare.disabled=busy||!selected.length;share.disabled=busy||!prepared||preparedSignature!==signature();if(preparedSignature!==signature())prepared=null;download.textContent=selected.length===1&&selected[0].assets.length===1?'선택 원본 다운로드':`선택 ${selected.length}개 옵션 ZIP 다운로드`;}
  node.append(el('div',{class:'actions'},download,prepare,share),status);refresh();return{node,refresh};
}
// One controller owns reservation, creation, streaming and final cancellation.
// Pausing changes intent; it never authorizes a second concurrent run.
class ReservedUpload {
  constructor(ctx, batchId, file, update) {
    Object.assign(this, {ctx, batchId, file, size:file.size, name:file.name, update,
      key:crypto.randomUUID(), intent:'run', state:'queued', reservation:null,
      reservationAttempted:false, creation:'not_started', upload:null,
      offset:0, generation:0, active:false, queued:false});
  }
  announce(state, message) { this.state=state; this.update(message,this.offset); }
  wanted(generation) {
    return !this.ctx.life.disposed && this.generation===generation;
  }
  pause() {
    if (['cancelled','staged'].includes(this.state) || this.intent==='cancel') return;
    this.intent='pause';
    this.announce('paused','일시정지 요청 · 진행 중인 전송을 확인하고 있습니다.');
    this.upload?.abort().catch(error=>this.ctx.fail(error));
  }
  cancel() {
    if (this.state==='cancelled') return;
    this.intent='cancel';
    if (!this.active && !this.reservationAttempted) {
      this.file=null;
      this.announce('cancelled','대기 파일 취소');
      return;
    }
    this.announce('cancel_pending','취소 요청 · 서버 정리 확인 중');
    this.upload?.abort().catch(error=>this.ctx.fail(error));
  }
  async reserve() {
    if (!this.reservation) {
      this.reservationAttempted=true;
      const value=await request(`/uploads/batches/${encodeURIComponent(this.batchId)}/files`,{
        method:'POST',body:{filename:this.name,declared_size:this.size,request_key:this.key},
        signal:this.ctx.life.signal
      });
      if (!/^\/tus\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value.url)
          || value.url!=='/tus/'+value.upload_id || sameOrigin(value.url).pathname!==value.url) {
        throw new Error('Invalid reservation URL');
      }
      this.reservation=value;
    }
    return this.reservation.url;
  }
  async head() {
    const response=await request(this.reservation.url,{
      method:'HEAD',raw:true,tus:true,signal:this.ctx.life.signal,
      headers:{'Tus-Resumable':'1.0.0'}
    });
    const offset=response.headers.get('Upload-Offset');
    const length=response.headers.get('Upload-Length');
    const version=response.headers.get('Tus-Resumable');
    if (![200,204].includes(response.status) || !/^\d+$/.test(offset||'')
        || Number(offset)>this.size || !Number.isSafeInteger(Number(offset))
        || (length!==null && length!==String(this.size))
        || (version!==null && version!=='1.0.0')) throw new Error('Invalid upload offset');
    this.offset=Number(offset);
    this.creation='created';
    return response;
  }
  async create() {
    // An uncertain POST is reconciled on the same reserved resource first.
    // The gateway's duplicate POST is idempotent; no second reservation is made.
    if (this.creation==='created') return;
    if (this.creation==='uncertain') {
      try { await this.head(); return; }
      catch (error) {
        if (error.status!==409 || error.code!=='upload_not_created') throw error;
      }
    }
    this.creation='uncertain';
    const response=await request(this.reservation.url,{
      method:'POST',raw:true,tus:true,signal:this.ctx.life.signal,
      headers:{'Tus-Resumable':'1.0.0','Upload-Length':String(this.size)}
    });
    if (response.status!==201 || response.headers.get('Tus-Resumable')!=='1.0.0'
        || response.headers.get('Location')!==this.reservation.url) {
      throw new Error('Invalid upload creation response');
    }
    this.creation='created';
  }
  async remove() {
    const deleted=async()=>{
      const response=await request(this.reservation.url,{
        method:'DELETE',raw:true,tus:true,signal:this.ctx.life.signal,
        headers:{'Tus-Resumable':'1.0.0'}
      });
      if (response.status!==204) throw new Error('Upload deletion not confirmed');
    };
    try { await deleted(); }
    catch (error) {
      if (error.status!==409 || error.code!=='upload_not_created') throw error;
      // The server cannot delete an uncreated tus reservation. Create exactly
      // this owned reservation, then terminate it; never report 409 as success.
      await this.create();
      await deleted();
    }
    this.file=null;
    this.announce('cancelled','서버 전송 취소 확인');
  }
  stream(generation) {
    if (!globalThis.tus?.Upload) throw new Error('Tus client unavailable');
    return new Promise((resolve,reject)=>{
      let finished=null,settled=false,abortPending=false,abortPromise=null;
      const finish=()=>{
        if(settled || !finished || abortPending)return;
        settled=true;
        if(finished.error)reject(finished.error);else resolve(finished.outcome);
      };
      const upload=new globalThis.tus.Upload(this.file,{
        uploadUrl:sameOrigin(this.reservation.url).href,chunkSize:8*1024*1024,
        retryDelays:null,storeFingerprintForResuming:false,
        removeFingerprintOnSuccess:true,withCredentials:true,
        onBeforeRequest:req=>{
          const url=new URL(req.getURL(),location.origin);
          if (!this.wanted(generation) || this.intent!=='run' || abortPending
              || url.origin!==location.origin || url.pathname!==this.reservation.url
              || url.search || url.hash || !['HEAD','PATCH','DELETE'].includes(req.getMethod())) {
            throw new Error('Unowned upload request');
          }
          req.setHeader('X-CSRF-Token',csrfToken());
        },
        onProgress:(sent,total)=>{
          if (!this.wanted(generation) || this.intent!=='run' || abortPending) return;
          this.offset=sent;
          this.announce('uploading',`${Math.floor(sent/total*100)}% 전송 중`);
        },
        onError:error=>{finished=this.intent==='run'?{error}:{outcome:'aborted'};finish();},
        onSuccess:()=>{finished={outcome:'completed'};finish();}
      });
      this.upload=upload;
      const abort=upload.abort.bind(upload);
      upload.abort=()=>{
        if(abortPromise)return abortPromise;
        abortPending=true;
        abortPromise=(async()=>{
          try { await abort(false); }
          catch(error) { finished={error}; }
          finally {
            abortPending=false;
            finished=finished||{outcome:'aborted'};
            finish();
          }
        })();
        return abortPromise;
      };
      if (this.wanted(generation) && this.intent==='run') upload.start();
      else resolve('aborted');
    });
  }
  async run() {
    if (this.active || this.state==='cancelled' || this.state==='staged') return;
    this.active=true;
    const generation=++this.generation;
    try {
      this.announce(this.intent==='cancel'?'cancel_pending':'reserving',
        this.intent==='cancel'?'취소 요청 · 서버 정리 확인 중':'파일 예약 중');
      await this.reserve();
      if (!this.wanted(generation)) return;
      if (this.intent==='cancel') { await this.remove(); return; }
      if (this.intent==='pause') { this.announce('paused','일시정지 · 재개 가능'); return; }
      this.announce('creating','전송을 준비하고 있습니다.');
      await this.create();
      if (!this.wanted(generation)) return;
      if (this.intent==='cancel') { await this.remove(); return; }
      if (this.intent==='pause') { this.announce('paused','일시정지 · 재개 가능'); return; }
      await this.head();
      if (!this.wanted(generation)) return;
      if (this.intent==='cancel') { await this.remove(); return; }
      if (this.intent==='pause') { this.announce('paused','일시정지 · 재개 가능'); return; }
      this.announce('uploading','서버가 확인한 위치에서 전송 중');
      const outcome=await this.stream(generation);
      this.upload=null;
      if (!this.wanted(generation)) return;
      if (this.intent==='cancel') { await this.remove(); return; }
      if (outcome==='completed' && this.intent==='run') {
        this.offset=this.size;
        this.announce('staged','100% 전송 완료 · 검증·권리 확인·게시는 미완료');
      } else this.announce('paused','일시정지 · 재개 시 전송 위치를 다시 확인합니다.');
    } catch (error) {
      if (!this.ctx.life.disposed) {
        const status=error.originalResponse?.getStatus?.()||error.status;
        if (this.intent==='cancel') {
          this.announce('cancel_pending','취소 확인 보류 · 취소를 다시 눌러 서버 상태를 확인해 주세요.');
        } else if (this.intent==='pause') {
          this.announce('paused','일시정지 · 재개 시 서버 상태를 다시 확인합니다.');
        } else this.announce('failed',status===403?'권한이 회수되어 전송이 중단되었습니다.':errorText(error));
        this.ctx.fail(error);
      }
    } finally {
      this.upload=null;
      this.active=false;
    }
  }
  dispose() {
    this.intent='pause';
    this.generation++;
    this.upload?.abort().catch(()=>{});
    this.file=null;
  }
}
export async function uploadView(ctx) {
  const {main,life,me}=ctx;
  main.append(heading('자료 업로드','전송 → 형식·권리 검증 → 게시 상태를 따로 확인합니다.'));
  if (!me.can_upload) {
    main.append(notice('현재 업로드 권한이 없습니다. 자료 열람과 다운로드는 계속 이용할 수 있습니다.'));
    return;
  }
  const rows=el('tbody');
  const table=el('div',{class:'table-wrap'},el('table',{},
    el('thead',{},el('tr',{},['파일명','크기','전송','상태','작업'].map(x=>el('th',{},x)))),rows));
  const picker=el('input',{type:'file',multiple:true,'aria-label':'업로드 파일 선택',accept:'.txt,.epub,.pdf,.cbz,.zip,.png,.jpg,.jpeg'});
  const folder=el('input',{type:'file',multiple:true,webkitdirectory:'','aria-label':'업로드 폴더 선택'});
  const drop=el('div',{class:'dropzone'},el('h2',{},'파일을 놓거나 선택해 주세요'),field('파일 선택',picker),field('폴더 선택 · 지원 브라우저',folder));
  const status=notice('한 번에 최대 2개 파일을 전송합니다. 파일과 재개 정보는 이 창을 닫으면 지워집니다.');
  main.append(drop,status,table);
  const entries=[];
  let running=0,queue=[],batch=null,closed=false,adding=false;
  const batchKey=crypto.randomUUID();
  function enqueue(item) {
    if (closed || item.active || item.queued || ['cancelled','staged'].includes(item.state)) return;
    item.queued=true;
    queue.push(item);
    schedule();
  }
  function schedule() {
    if (closed) return;
    while (running<2 && queue.length) {
      const item=queue.shift();
      item.queued=false;
      if (item.intent==='pause' || item.state==='cancelled') continue;
      running++;
      item.run().finally(()=>{
        running--;
        // A resume received during abort settlement waits for this run to finish.
        if (item.intent==='run' && item.state==='paused') enqueue(item);
        schedule();
      });
    }
  }
  async function add(files) {
    if (batch || adding) { status.textContent='이 전송 묶음을 마친 후 새 화면에서 다음 묶음을 시작해 주세요.'; return; }
    const selected=[...files],total=selected.reduce((n,file)=>n+file.size,0);
    if (!selected.length) return;
    if (selected.length>200) { status.textContent='한 묶음에 최대 200개 파일을 선택해 주세요.'; return; }
    if (selected.some(file=>file.size===0||file.size>2*1024**3) || total>10*1024**3) {
      status.textContent='파일은 0바이트 초과·2GiB 이하, 묶음은 10GiB 이하여야 합니다.'; return;
    }
    adding=true;
    picker.disabled=folder.disabled=true;
    try {
      const result=await request('/uploads/batches',{
        method:'POST',body:{declared_total_bytes:total,request_key:batchKey},signal:life.signal
      });
      if (life.disposed) return;
      batch=result.batch_id;
      status.textContent='업로드 묶음이 준비되었습니다. 전송 후 검증·권리 상태를 확인해 주세요.';
      for (const file of selected) {
        const progress=el('progress',{max:file.size,value:0,class:'progress','aria-label':file.name+' 전송률'});
        const message=el('span',{},'대기 중'),actions=el('div',{class:'actions'});
        const item=new ReservedUpload(ctx,batch,file,(text,offset)=>{message.textContent=text;progress.value=offset;});
        actions.append(
          button('일시정지',()=>item.pause()),
          button('재개',()=>{
            if (item.intent==='cancel' || !['paused','failed'].includes(item.state)) return;
            item.intent='run';
            enqueue(item);
          }),
          button('취소',()=>{item.cancel();if(item.intent==='cancel')enqueue(item);})
        );
        rows.append(el('tr',{},el('td',{},file.name),el('td',{},bytes(file.size)),
          el('td',{},progress),el('td',{},message),el('td',{},actions)));
        entries.push(item);
        enqueue(item);
      }
    } catch (error) { ctx.fail(error);picker.disabled=folder.disabled=false; }
    finally { adding=false; }
  }
  picker.addEventListener('change',()=>add(picker.files));
  folder.addEventListener('change',()=>add(folder.files));
  drop.addEventListener('dragover',event=>event.preventDefault());
  drop.addEventListener('drop',event=>{event.preventDefault();add(event.dataTransfer.files);});
  main.append(button('모두 일시정지',()=>{for(const item of entries)item.pause();}));
  life.add(()=>{closed=true;queue=[];for(const item of entries)item.dispose();});
  const batches=el('section',{class:'panel'},el('h2',{},'업로드 처리 상태'));
  main.append(batches);
  let cursor=null;
  const more=button('내 묶음 조회',async()=>{
    more.disabled=true;
    try {
      const result=await request('/api/v1/upload-batches?'+new URLSearchParams({limit:20,...(cursor?{cursor}:{})}),{signal:life.signal});
      if (life.disposed) return;
      batches.replaceChildren(el('h2',{},'업로드 처리 상태'));
      for(const value of result.batches)batches.append(button(
        `${value.created_at?new Date(value.created_at).toLocaleString():'등록일 확인 중'} · ${value.status} · ${value.file_count}개 · ${bytes(value.received_bytes)} / ${bytes(value.reserved_bytes)}`,
        ()=>batchStatus(ctx,batches,value.batch_id).catch(ctx.fail)
      ));
      cursor=result.next_cursor;
      more.textContent=cursor?'다음 묶음':'처음부터 새로 조회';
    } catch(error) { ctx.fail(error); }
    finally { more.disabled=false; }
  });
  main.append(more,button('현재 묶음 상태 새로 조회',()=>{
    if(batch)batchStatus(ctx,batches,batch).catch(ctx.fail);
    else status.textContent='먼저 파일을 선택해 주세요.';
  }));
}
export async function batchStatus(ctx,panel,batchId,cursor=null){
  const result=await request(`/api/v1/upload-batches/${encodeURIComponent(batchId)}?`+new URLSearchParams({limit:20,...(cursor?{cursor}:{})}),{signal:ctx.life.signal});if(ctx.life.disposed)return;
  panel.replaceChildren(el('h2',{},'묶음 처리 상태'),notice(`${result.status} · 게시와 전송은 별도 상태입니다.`));
  for(const file of result.files){const box=el('div',{class:'edition'},el('strong',{},file.display_filename||'파일명 확인 불가'),el('p',{},`${bytes(file.received_size)} / ${bytes(file.declared_size)} · 전송 ${file.transport_status} · 처리 ${file.ingestion_state} · 권리 ${file.rights_state||'확인 전'} · 게시 ${file.publication_state}`));
    if(file.published)box.append(button('게시된 작품 보기',()=>ctx.go('detail',{work:file.published.work_id})));
    else if(file.transport_status==='staged'&&file.ingestion_state==='unqueued')box.append(bindingForm(ctx,file.upload_id,false));
    panel.append(box);
  }
  if(result.next_cursor)panel.append(button('다음 파일',()=>batchStatus(ctx,panel,batchId,result.next_cursor).catch(ctx.fail)));
}
export function bindingForm(ctx,uploadId,admin) {
  const box=el('div',{class:'panel'});
  const search=el('input',{type:'search',maxlength:200,'aria-label':'연결할 작품 검색'});
  const results=el('div'),editions=el('div');
  const selected=notice('연결할 작품을 선택해 주세요.');
  const status=notice('연결할 작품과 판본을 명시적으로 선택해 주세요.');
  let worksCursor=null,editionsCursor=null,workId=null,workQuery='';
  let searchGeneration=0,selectionGeneration=0,editionGeneration=0;
  let searchRequest=null,editionRequest=null;
  ctx.life.add(()=>{searchRequest?.abort();editionRequest?.abort();});
  const moreWorks=button('다음 작품',()=>findWorks(false));
  const moreEditions=button('다음 판본',()=>findEditions(false));
  moreWorks.hidden=moreEditions.hidden=true;
  function clearSelection() {
    selectionGeneration++;
    editionGeneration++;
    editionRequest?.abort();
    workId=null;
    editionsCursor=null;
    editions.replaceChildren();
    moreEditions.hidden=true;
    selected.textContent='연결할 작품을 선택해 주세요.';
  }
  function chooseWork(work) {
    clearSelection();
    workId=work.work_id;
    selected.textContent='선택 작품: '+(work.title_display||work.title);
    findEditions();
  }
  async function findWorks(reset=true) {
    const generation=++searchGeneration;
    searchRequest?.abort();
    searchRequest=new AbortController();
    if(reset) { worksCursor=null;workQuery=search.value; }
    clearSelection();
    results.replaceChildren();
    moreWorks.hidden=true;
    const query=new URLSearchParams({[admin?'q':'search']:workQuery,limit:20,...(worksCursor?{cursor:worksCursor}:{})});
    try {
      const page=await request((admin?'/api/v1/admin/catalog/works':'/api/v1/items')+'?'+query,{
        signal:AbortSignal.any([ctx.life.signal,searchRequest.signal])
      });
      if(ctx.life.disposed || generation!==searchGeneration) return;
      results.replaceChildren();
      for(const work of page.works||page.items||[])results.append(button(work.title_display||work.title,()=>{chooseWork(work);}));
      worksCursor=page.next_cursor;
      moreWorks.hidden=!worksCursor;
      if(!(page.works||page.items||[]).length)results.append(notice('작품이 없습니다. 미게시 작품은 관리자 카탈로그에서 만들거나 선택하세요.'));
    } catch(error) {
      if(generation===searchGeneration && error.name!=='AbortError')ctx.fail(error);
    }
  }
  async function findEditions(reset=true) {
    if(!workId) return;
    const generation=++editionGeneration,selection=selectionGeneration,requestedWork=workId;
    editionRequest?.abort();
    editionRequest=new AbortController();
    if(reset)editionsCursor=null;
    editions.replaceChildren();
    moreEditions.hidden=true;
    const cursor=editionsCursor;
    const current=()=>!ctx.life.disposed && generation===editionGeneration
      && selection===selectionGeneration && requestedWork===workId;
    try {
      const query=new URLSearchParams({limit:20,...(cursor?{cursor}:{})});
      const path=admin?`/api/v1/admin/catalog/works/${encodeURIComponent(requestedWork)}/editions?${query}`
        :apiPath(requestedWork,'/editions',Object.fromEntries(query));
      const data=await request(path,{signal:AbortSignal.any([ctx.life.signal,editionRequest.signal])});
      if(!current())return;
      if((data.work_id!==undefined && data.work_id!==requestedWork)
          || data.editions.some(edition=>edition.work_id!==undefined && edition.work_id!==requestedWork)) {
        throw new Error('Edition response work mismatch');
      }
      for(const edition of data.editions) {
        const revision=admin?edition.active_revision_id:edition.revision_id;
        editions.append(button(`${edition.edition_label||edition.edition_key} · ${edition.representation} · ${revision?'현재 판본':'초판'}`,async()=>{
          if(!current())return;
          try {
            const result=await request(`/api/v1/${admin?'admin/':''}uploads/${encodeURIComponent(uploadId)}/${admin?'bind':'enqueue'}`,{
              method:'POST',signal:ctx.life.signal,
              body:{edition_id:edition.edition_id,expected_revision_id:revision??null}
            });
            if(!current())return;
            status.textContent=`접수 확인 · ${result.ingestion_state} · 권리 ${result.rights_status} · ${result.worker_required?'자료 처리 대기':'현재 상태 새로 조회 필요'}`;
          }catch(error){if(current())ctx.fail(error);}
        }));
      }
      editionsCursor=data.next_cursor;
      moreEditions.hidden=!editionsCursor;
      if(!data.editions.length)editions.append(notice('연결할 판본이 없습니다. 관리자 판본 생성 후 다시 조회하세요.'));
    }catch(error){if(current()&&error.name!=='AbortError')ctx.fail(error);}
  }
  box.append(field('연결할 작품',search),button('작품 검색',()=>findWorks()),results,
    moreWorks,selected,editions,moreEditions,status);
  return box;
}
