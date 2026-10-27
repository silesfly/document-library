import {request,Lifetime,errorText} from './api.js';
import {el,button,notice} from './dom.js';
import {BRAND} from './config.js';
import {libraryView,detailView,favoritesView} from './library.js';
import {recommendationView,preferencesView} from './member.js';
import {textReader,imageReader} from './readers.js';
import {uploadView} from './transfers.js';
import {adminView} from './admin.js';
import {loginView} from './auth.js';
const main=document.querySelector('#main'),account=document.querySelector('#account'),navigation=document.querySelector('#navigation');
const views={library:libraryView,favorites:favoritesView,recommendations:recommendationView,preferences:preferencesView,detail:detailView,reader:textReader,image:imageReader,upload:uploadView,admin:adminView,login:loginView};
let me=null,life=new Lifetime(),generation=0;const selections=new Map(),scrolls=new Map();let previousHash=location.hash;
document.title=BRAND;document.querySelector('#brand').textContent=BRAND;
function go(name,query={}){const params=query instanceof URLSearchParams?query:new URLSearchParams(query);location.hash=name+(params.size?'?'+params:'');}
function clearSession(){me=null;selections.clear();scrolls.clear();life.dispose();main.replaceChildren();account.replaceChildren();navigation.replaceChildren();}
async function refreshSession(){me=await request('/me');}
function navigationRender(page){
  navigation.replaceChildren();account.replaceChildren();
  for(const [id,label]of [['library','▦ 내 서재'],['favorites','♡ 내 즐겨찾기'],['recommendations','☆ 추천 작품'],['preferences','⚙ 회원 선호'],['upload','↑ 자료 업로드'],...(me?.role==='admin'?[['admin','♙ 회원 · 권한 관리']]:[])])navigation.append(el('a',{href:'#'+id,...(page===id?{'aria-current':'page'}:{})},label));
  if(me)account.append(el('span',{class:'account-name'},me.username+' '),button('로그아웃',async()=>{try{await request('/auth/logout',{method:'POST',body:{}});}catch(error){if(error.status!==401){main.prepend(notice(errorText(error),'error'));return;}}clearSession();go('login');if(location.hash==='#login')render();}));
}
async function render(){
  const thisGeneration=++generation;scrolls.set(previousHash,window.scrollY);previousHash=location.hash;life.dispose();life=new Lifetime();const current=life;main.replaceChildren();
  const hash=location.hash.slice(1),separator=hash.indexOf('?'),name=(separator<0?hash:hash.slice(0,separator))||'library',params=new URLSearchParams(separator<0?'':hash.slice(separator+1));
  let page=Object.hasOwn(views,name)?name:'library';if(!me&&page!=='login')page='login';navigationRender(page);
  const fail=error=>{if(current.disposed||error.name==='AbortError')return;const message=notice(errorText(error),'error');message.append(' ',button('현재 화면 다시 조회',render));main.prepend(message);};
  const ctx={main,params,life:current,go,me,selections,fail,refreshSession,clearSession};
  try{await views[page](ctx);if(thisGeneration!==generation||current.disposed)return;const saved=scrolls.get(location.hash);if(saved!==undefined)window.scrollTo(0,saved);else{window.scrollTo(0,0);main.querySelector('h1')?.focus();}}
  catch(error){fail(error);}
}
window.addEventListener('hashchange',render);
window.addEventListener('session-expired',()=>{if(!me)return;clearSession();go('login');render();});
window.addEventListener('pagehide',clearSession);
window.addEventListener('pageshow',async event=>{if(!event.persisted)return;try{await refreshSession();}catch{clearSession();}await render();});
document.addEventListener('keydown',event=>{if((event.ctrlKey||event.metaKey)&&event.key==='k'){event.preventDefault();go('library');setTimeout(()=>main.querySelector('input[type=search]')?.focus(),0);}});
try{await refreshSession();}catch(error){if(error.status!==401)main.replaceChildren(notice(errorText(error),'error'));}
await render();
if('serviceWorker' in navigator&&isSecureContext)navigator.serviceWorker.register('/sw.js',{scope:'/'}).catch(()=>{ /* Installation is optional; online errors remain visible in each action. */ });
