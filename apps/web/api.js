// All credentials remain in the browser's cookie jar; no persistent client state.
export class ApiError extends Error {
  constructor(status, code, details = {}, retryAfter = null) { super(code); Object.assign(this, {status, code, details, retryAfter}); }
}
export function sameOrigin(path) {
  const url = new URL(path, location.origin);
  if (url.origin !== location.origin || !path.startsWith('/') || path.startsWith('//')) throw new Error('Unsafe URL');
  return url;
}
function csrf() {
  const cookie = document.cookie.split('; ').find(x => x.startsWith('__Host-dl-csrf='));
  return cookie ? decodeURIComponent(cookie.slice('__Host-dl-csrf='.length)) : '';
}
export async function request(path, {method = 'GET', body, signal, headers = {}, raw = false, tus = false} = {}) {
  const url = sameOrigin(path);
  const h = new Headers(headers);
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method) || tus) h.set('X-CSRF-Token', csrf());
  if (body !== undefined && !(body instanceof Blob) && !(body instanceof ArrayBuffer)) {
    body = JSON.stringify(body); h.set('Content-Type', 'application/json');
  }
  const response = await fetch(url, {method, body, signal, headers: h, credentials: 'same-origin', cache: 'no-store', redirect: 'error'});
  if (!response.ok) {
    let value = {}; try { value = await response.json(); } catch { /* Non-JSON transport failures are not successful. */ }
    const error = value.error;
    const code = typeof error === 'string' ? error : error?.code || `HTTP_${response.status}`;
    const details = typeof error === 'object' ? error.details || {} : value;
    const failure = new ApiError(response.status, code, details, response.headers.get('Retry-After'));
    if (response.status === 401 && code.toUpperCase() === 'AUTHENTICATION_REQUIRED') window.dispatchEvent(new Event('session-expired'));
    throw failure;
  }
  if (raw || method === 'HEAD') return response;
  if (response.status === 204) return null;
  return response.json();
}
export function apiPath(id, suffix = '', params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    for (const item of Array.isArray(value) ? value : [value]) query.append(key, item);
  }
  return `/api/v1/items/${encodeURIComponent(id)}${suffix}${query.size ? '?' + query : ''}`;
}
export const selectionParams = choice => ({edition_id: choice.edition_id, revision_id: choice.revision_id, download_option_id: choice.option_id, asset_position: choice.asset_position});
export class Lifetime {
  constructor() { this.controller = new AbortController(); this.cleanups = []; this.disposed = false; }
  get signal() { return this.controller.signal; }
  add(fn) { if (this.disposed) fn(); else this.cleanups.push(fn); return fn; }
  dispose() { if (this.disposed) return; this.disposed = true; this.controller.abort(); for (const fn of this.cleanups.splice(0).reverse()) fn(); }
  objectURL(blob) { const url = URL.createObjectURL(blob); this.add(() => URL.revokeObjectURL(url)); return url; }
}
export function errorText(error) {
  if (error.name === 'AbortError') return '요청을 중지했습니다.';
  const code = (error.code || '').toUpperCase();
  if (code === 'INVALID_CREDENTIALS') return '계정 정보 또는 비밀번호가 올바르지 않습니다. 다시 입력해 주세요.';
  if (error.status === 401 && code === 'AUTHENTICATION_REQUIRED') return '로그인이 만료되었습니다. 다시 로그인해 주세요.';
  if (error.status === 429) {
    const value = /^\d{1,4}$/.test(String(error.retryAfter || '')) ? Number(error.retryAfter) : 0;
    return `요청이 많습니다. ${value >= 1 && value <= 3600 ? value + '초 뒤 ' : ''}직접 다시 조회하거나 시도해 주세요.`;
  }
  if (error.status === 403) return '현재 권한으로 실행할 수 없습니다. 권한이 변경되었을 수 있습니다.';
  if (code.includes('CURSOR')) return '목록 커서가 만료되었거나 조건이 바뀌었습니다. 처음부터 다시 조회해 주세요.';
  if (code.includes('REPAIR') || code.includes('PENDING') || code === 'PREVIEW_NOT_READY' || (Array.isArray(error.details) && error.details.some(x=>['repair_queued','repair_running'].includes(x.state)))) return '자료 복구 또는 미리보기 준비 중입니다. 잠시 후 다시 조회해 주세요.';
  if (error.status === 409) return '자료나 권한이 변경되었습니다. 다시 조회하고 선택을 확인해 주세요.';
  if (error.status === 404) return '자료를 찾을 수 없거나 현재 접근할 수 없습니다.';
  if (error.status === 413) return '자료 또는 선택 크기가 허용 범위를 초과했습니다.';
  if (error.status === 416) return '요청한 범위를 사용할 수 없습니다. 자료를 다시 선택해 주세요.';
  if (error.status === 422 || error.status === 400) return '입력 또는 선택 조건을 확인해 주세요.';
  if (error.status === 503) return '서비스가 준비되지 않았습니다. 다시 시도해 주세요.';
  return '요청에 실패했습니다. 네트워크 연결을 확인하고 다시 시도해 주세요.';
}
export function csrfToken() { return csrf(); }
