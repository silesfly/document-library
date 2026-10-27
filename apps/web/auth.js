import {request} from './api.js';
import {el,button,field,notice,heading,options} from './dom.js';
export async function loginView(ctx){
  const wrap=el('div',{class:'login-wrap'});ctx.main.append(wrap);wrap.append(heading('내 서재에 로그인','초대받은 회원만 이용할 수 있습니다.'));
  const mode=el('select',{'aria-label':'인증 작업'},options([['login','로그인'],['register','초대받아 가입'],['reset','비밀번호 재설정']],ctx.params.get('mode')||'login'));
  const box=el('div',{class:'panel'}),message=notice('아이디와 비밀번호를 입력해 주세요.');wrap.append(mode,box,message);
  function render(){
    box.replaceChildren();const form=el('form');const username=el('input',{name:'username',autocomplete:mode.value==='login'?'username':'off',minlength:3,maxlength:64,required:true});const password=el('input',{name:'password',type:'password',autocomplete:mode.value==='login'?'current-password':'new-password',minlength:mode.value==='login'?1:12,maxlength:256,required:true});const token=el('input',{type:'password',autocomplete:'off',minlength:32,maxlength:128,required:true});
    if(mode.value!=='reset')form.append(field('아이디',username));
    if(mode.value!=='login')form.append(field(mode.value==='register'?'초대 토큰':'재설정 토큰',token));
    form.append(field(mode.value==='login'?'비밀번호':'새 비밀번호 · 12자 이상',password));
    const submit=el('button',{class:'primary full',type:'submit'},mode.value==='login'?'로그인':mode.value==='register'?'가입':'비밀번호 재설정');form.append(submit);
    form.addEventListener('submit',async event=>{event.preventDefault();submit.disabled=true;const currentMode=mode.value;try{if(currentMode==='login'){await request('/auth/login',{method:'POST',signal:ctx.life.signal,body:{username:username.value,password:password.value}});password.value='';await ctx.refreshSession();ctx.go('library');}else if(currentMode==='register'){await request('/auth/register-invite',{method:'POST',signal:ctx.life.signal,body:{username:username.value,password:password.value,invite:token.value}});message.textContent='가입되었습니다. 로그인해 주세요.';mode.value='login';render();}else{await request('/auth/reset-password',{method:'POST',signal:ctx.life.signal,body:{token:token.value,new_password:password.value}});message.textContent='비밀번호가 변경되었습니다. 다시 로그인해 주세요.';mode.value='login';render();}}catch(error){ctx.fail(error);}finally{password.value='';token.value='';submit.disabled=false;}});
    ctx.life.add(()=>{password.value='';token.value='';});box.append(form);
  }
  mode.addEventListener('change',render);render();wrap.append(notice('초대 또는 비밀번호 재설정 토큰은 관리자에게 받으세요. 브라우저 저장소에 보관하지 않습니다.'));
}
export function passwordChange(ctx){
  const form=el('form',{class:'panel'},el('h2',{},'비밀번호 변경'));const old=el('input',{type:'password',autocomplete:'current-password',required:true,maxlength:256}),next=el('input',{type:'password',autocomplete:'new-password',required:true,minlength:12,maxlength:256});const submit=el('button',{type:'submit',class:'primary'},'변경 후 다시 로그인');form.append(field('현재 비밀번호',old),field('새 비밀번호',next),submit);form.addEventListener('submit',async e=>{e.preventDefault();submit.disabled=true;try{await request('/auth/change-password',{method:'POST',signal:ctx.life.signal,body:{current_password:old.value,new_password:next.value}});ctx.clearSession();ctx.go('login');}catch(error){ctx.fail(error);}finally{old.value=next.value='';submit.disabled=false;}});ctx.life.add(()=>{old.value=next.value='';});return form;
}
