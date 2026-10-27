// Untrusted metadata is always a text node, never HTML.
export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (key === 'checked' || key === 'disabled' || key === 'hidden' || key === 'multiple') node[key] = value;
    else if (key === 'value') node.value = value;
    else if (value !== undefined && value !== null) node.setAttribute(key, value);
  }
  for (const child of children.flat(Infinity)) if (child !== null && child !== undefined) node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}
export const button = (label, action, cls = 'secondary') => el('button', {type: 'button', class: cls, onclick: action}, label);
export function field(label, control) { return el('label', {class: 'field'}, el('span', {}, label), control); }
export const notice = (text, kind = '') => el('p', {class: `notice ${kind}`, role: kind === 'error' ? 'alert' : 'status'}, text);
export function heading(title, intro) { return el('header', {class: 'pageheading'}, el('div', {}, el('div', {class: 'eyebrow'}, 'YOUR PRIVATE LIBRARY'), el('h1', {tabindex: '-1'}, title), intro ? el('p', {class: 'intro'}, intro) : null)); }
export function bytes(size) {
  if (!Number.isFinite(size)) return '크기 확인 중';
  if (size < 1024) return `${size} B`;
  const unit = size < 1024 ** 2 ? 'KB' : size < 1024 ** 3 ? 'MB' : 'GB';
  return `${(size / (1024 ** ({KB: 1, MB: 2, GB: 3}[unit]))).toFixed(1)} ${unit}`;
}
export const labels = {text: '텍스트', imagepack: '이미지', document: '문서', single_image: '단일 이미지', scan: '스캔', completed: '완결', ongoing: '연재 중', unknown: '확인 중', allowed: '허용', review: '검토 대기', blocked: '차단'};
export function coverage(value, confidence) {
  const confidenceText={verified:'확인됨',inferred:'추정',unknown:'확인 중'}[confidence||value?.confidence]||'확인 중';
  if (!value || value.unit === 'unknown') return '보유 범위 확인 중 · '+confidenceText;
  const ranges = (value.intervals || []).map(x => Array.isArray(x) ? (x[0] === x[1] ? x[0] : `${x[0]}–${x[1]}`) : `${x.start ?? '?'}–${x.end ?? '?'}`).join(', ');
  const gaps = (value.gaps || []).map(x => Array.isArray(x) ? x.join('–') : String(x)).join(', ');
  return `${ranges || '범위 미확인'} ${{episode:'화',volume:'권',page:'페이지'}[value.unit]||''}${gaps ? ` · 누락 ${gaps}` : ''} · ${confidenceText}`;
}
export function options(values, current) { return values.map(([value, text]) => el('option', {value, ...(value === current ? {selected: ''} : {})}, text)); }
export async function modal(title, content, lifetime) {
  const dialog = el('dialog', {class: 'dialog', 'aria-label': title});
  const previous = document.activeElement;
  const close = () => { dialog.close(); dialog.remove(); if (previous?.isConnected) previous.focus(); };
  dialog.append(el('h2', {}, title), content, button('닫기', close));
  document.body.append(dialog); dialog.showModal();
  dialog.addEventListener('cancel', event => { event.preventDefault(); close(); });
  lifetime?.add(close);
  return {dialog, close};
}
