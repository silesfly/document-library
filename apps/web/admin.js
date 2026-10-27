import {request,errorText} from './api.js';
import {el,button,field,notice,heading,options,modal,bytes} from './dom.js';
import {bindingForm} from './transfers.js';
import {passwordChange} from './auth.js';
import {catalogPanel} from './catalog.js';
import {metadataPanels} from './metadata.js';
export async function adminView(ctx){
  const {main,me,life,params,go}=ctx;main.append(heading('회원 · 권한 관리','변경 대상과 업로드 영향을 확인한 뒤 실행합니다.'));
  if(me.role!=='admin'){main.append(notice('관리자 권한이 필요합니다.','error'));return;}
  const search=el('input',{type:'search',maxlength:64,value:params.get('search')||'','aria-label':'회원 검색'}),state=el('select',{'aria-label':'회원 상태'},options([['all','전체'],['active','이용 중'],['suspended','정지'],['upload','업로드 허용'],['no_upload','업로드 권한 없음']],params.get('state')||'all'));
  const form=el('form',{class:'toolbar'},search,state,el('button',{type:'submit',class:'primary'},'회원 검색'));form.addEventListener('submit',e=>{e.preventDefault();go('admin',{search:search.value,state:state.value});});main.append(form);
  const result=await request('/admin/users?'+new URLSearchParams({search:search.value,state:state.value,limit:50,offset:params.get('offset')||0}),{signal:life.signal});if(life.disposed)return;
  const selected=new Map(),summary=el('p',{class:'small'},'현재 페이지 0명 선택'),body=el('tbody'),selectAll=el('input',{type:'checkbox','aria-label':'현재 페이지 일반 회원 모두 선택'}),checks=[];
  main.append(summary,el('div',{class:'table-wrap'},el('table',{},el('thead',{},el('tr',{},el('th',{},selectAll),['회원','역할','업로드 권한','상태','조회'].map(x=>el('th',{},x)))),body)));
  const update=()=>{summary.textContent=`현재 페이지 ${selected.size}명 선택 · 관리자는 변경 대상에서 제외됩니다.`;};
  for(const user of result.users){const check=el('input',{type:'checkbox',disabled:user.role==='admin','aria-label':user.username+' 선택'});const onChange=()=>{if(check.checked)selected.set(user.id,user);else selected.delete(user.id);update();};check.addEventListener('change',onChange);checks.push({check,user,onChange});const details=el('td');
    details.append(button('실제 활동',async()=>{try{const value=await request(`/api/v1/admin/users/${encodeURIComponent(user.id)}/activity`,{signal:life.signal});const content=el('div');for(const [key,val] of Object.entries(value))if(typeof val!=='object')content.append(el('p',{},key+': '+String(val)));await modal('회원 활동 집계',content,life);}catch(e){ctx.fail(e);}}));
    if(user.role!=='admin')details.append(button('재설정 토큰',()=>issueToken(`/admin/users/${encodeURIComponent(user.id)}/password-reset`,'비밀번호 재설정 토큰',ctx)));
    body.append(el('tr',{},el('td',{},check),el('td',{},user.username),el('td',{},user.role==='admin'?'관리자':'회원'),el('td',{},user.role==='admin'||user.can_upload?'허용':'없음'),el('td',{},user.status),details));
  }
  selectAll.addEventListener('change',()=>{for(const x of checks)if(x.user.role!=='admin'){x.check.checked=selectAll.checked;x.onChange();}});
  const reason=el('textarea',{maxlength:500,required:true,rows:2,'aria-label':'권한 변경 사유'});main.append(field('변경 사유',reason));const actions=el('div',{class:'actions'});
  for(const [action,label]of [['grant','업로드 허용'],['revoke','업로드 회수'],['suspend','회원 정지'],['activate','회원 활성화']])actions.append(button(label,async()=>{
    if(!selected.size||!reason.value.trim()){ctx.fail({status:400});return;}
    const path=action==='suspend'||action==='activate'?'/admin/users/bulk-status':'/admin/user-permissions/bulk';
    const values=[...selected.values()],body={action,reason:reason.value,user_ids:values.map(x=>x.id),expected_authz_versions:Object.fromEntries(values.map(x=>[x.id,x.authz_version]))};
    try{const preview=await request(path+'-preview',{method:'POST',body,signal:life.signal});if(life.disposed)return;await permissionConfirm(ctx,path,preview);}catch(e){ctx.fail(e);}
  }));main.append(actions);
  if(result.users.length===50)main.append(button('다음 회원',()=>go('admin',{search:search.value,state:state.value,offset:Number(params.get('offset')||0)+50})));
  main.append(button('회원 초대 토큰 만들기',()=>issueToken('/admin/invites','일회용 초대 토큰',ctx)),passwordChange(ctx));
  const audit=el('section',{class:'panel'},el('h2',{},'최근 권한 변경')),auditMore=button('감사 기록 조회',()=>loadAudit());main.append(audit,auditMore);let auditCursor=null;
  async function loadAudit(){try{const result=await request('/api/v1/admin/permission-audit?'+new URLSearchParams({limit:20,...(auditCursor?{cursor:auditCursor}:{})}),{signal:life.signal});if(life.disposed)return;audit.replaceChildren(el('h2',{},'권한 변경 기록'));for(const item of result.items||result.events||[])audit.append(el('p',{class:'small'},Object.entries(item).filter(([,v])=>typeof v!=='object').map(([k,v])=>k+': '+v).join(' · ')));auditCursor=result.next_cursor;auditMore.textContent=auditCursor?'다음 감사 기록':'처음부터 새로 조회';}catch(e){ctx.fail(e);}}
  main.append(catalogPanel(ctx));
  const reviews=el('section',{class:'panel'},el('h2',{},'업로드 권리 · 판본 검토'));main.append(reviews);let reviewCursor=null;
  main.append(button('검토 대기 조회',async()=>{try{const result=await request('/api/v1/admin/uploads/review-queue?'+new URLSearchParams({limit:20,...(reviewCursor?{cursor:reviewCursor}:{})}),{signal:life.signal});if(life.disposed)return;reviews.replaceChildren(el('h2',{},'업로드 검토'));if(!result.uploads.length)reviews.append(notice('검토할 업로드가 없습니다.'));for(const item of result.uploads){const box=el('article',{class:'edition'},el('h3',{},item.display_filename||'파일명 확인 불가'),el('p',{class:'small'},`소유자 ${item.owner_id} · ${bytes(item.received_size)} / ${bytes(item.declared_size)} · 전송 ${item.transport_status} · 처리 ${item.ingestion_state} · 권리 ${item.rights_status||'검토 전'}`));if(!item.edition_id&&item.transport_status==='staged')box.append(bindingForm(ctx,item.upload_id,true));
      if(item.edition_id){const rights=el('select',{'aria-label':'권리 판정'},options([['review','검토 대기'],['allowed','허용'],['blocked','차단']],item.rights_status||'review')),receipt=el('input',{type:'text',pattern:'[A-Za-z0-9_.:-]{1,200}',required:true,maxlength:200,'aria-label':'권리 판정 근거 ID'});box.append(field('권리 판정',rights),field('근거 ID · 영문·숫자·_ . : -',receipt),button('권리 검토 저장',async()=>{try{if(!/^[A-Za-z0-9_.:-]{1,200}$/.test(receipt.value))throw {status:422};const value=await request(`/api/v1/admin/uploads/${encodeURIComponent(item.upload_id)}/rights-review`,{method:'POST',signal:life.signal,body:{rights_status:rights.value,rights_receipt:receipt.value}});box.append(notice(`실제 처리 ${value.ingestion_state} · 권리 ${value.rights_status} · ${value.worker_required?'자료 처리 대기':'새로 조회 필요'}`));}catch(e){ctx.fail(e);}}));}reviews.append(box);}reviewCursor=result.next_cursor;}catch(e){ctx.fail(e);}}));
  main.append(metadataPanels(ctx));
}
async function permissionConfirm(ctx,path,preview){
  const content=el('div',{},notice(`고정 대상 ${preview.eligible_count}명 · 중단 영향 업로드 ${preview.impact.upload_sessions_to_block}개 · 만료 ${new Date(preview.expires_at*1000).toLocaleString()}`),el('p',{},`변경: ${preview.action} · 사유: ${preview.reason}`),el('ul',{},preview.targets.map(x=>el('li',{},`${x.username} · 버전 ${x.authz_version}`))));
  const confirm=el('input',{type:'checkbox'}),password=el('input',{type:'password',autocomplete:'current-password',required:true,maxlength:256}),status=notice('대상·영향을 확인하고 관리자 비밀번호를 다시 입력해 주세요.');content.append(el('label',{},confirm,' 고정 대상과 영향을 확인했습니다.'),field('관리자 비밀번호 재확인',password),status);
  const key=crypto.randomUUID();let pending=false;const submit=button('변경 확정',async()=>{if(pending||!confirm.checked||!password.value)return;pending=true;submit.disabled=true;try{await request(path,{method:'POST',signal:ctx.life.signal,headers:{'Idempotency-Key':key},body:{admin_password:password.value,permission:'content.upload',preview_token:preview.preview_token,action:preview.action,reason:preview.reason,confirm_target_count:preview.eligible_count,confirmed:true}});close();ctx.go('admin',{refresh:crypto.randomUUID()});}catch(error){status.textContent=errorText(error);if(error.status===409){status.append(' 미리보기가 무효입니다. 닫은 뒤 회원을 다시 조회하세요.');confirm.disabled=true;submit.disabled=true;}}finally{password.value='';pending=false;if(!confirm.disabled)submit.disabled=false;}},'primary');content.append(submit);const {close}=await modal('권한 변경 최종 확인',content,ctx.life);ctx.life.add(()=>{password.value='';});
}
async function issueToken(path,title,ctx){try{const value=await request(path,{method:'POST',body:{},signal:ctx.life.signal});const input=el('input',{type:'password',value:value.token,readonly:true,'aria-label':title,autocomplete:'off'});const content=el('div',{},notice(`한 번만 전달하고 보관하지 마세요. 유효 기간 ${value.expires_in}초`),input,button('토큰 표시',()=>{input.type=input.type==='password'?'text':'password';}));await modal(title,content,ctx.life);ctx.life.add(()=>{input.value='';});}catch(e){ctx.fail(e);}}
