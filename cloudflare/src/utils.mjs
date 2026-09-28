export const json = (data, status = 200, headers = {}) => new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options':'nosniff', ...headers } });
export const err = (message, status = 400) => json({ error: message }, status);
export const uuid = () => crypto.randomUUID();
export const randomToken = (n = 32) => { const b = new Uint8Array(n); crypto.getRandomValues(b); return base64url(b); };
export const base64url = b => btoa(String.fromCharCode(...b)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
export const sha256 = async v => base64url(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(v))));
export const timingEqual = (a, b) => {
  const aa = new TextEncoder().encode(String(a || '')), bb = new TextEncoder().encode(String(b || ''));
  let diff = aa.length ^ bb.length;
  for (let i = 0; i < Math.max(aa.length, bb.length); i++) diff |= (aa[i] || 0) ^ (bb[i] || 0);
  return diff === 0;
};
export async function hmacVerify(secret, body, header) {
  if (!header?.startsWith('sha256=') || !secret) return false;
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, body));
  const expected = [...sig].map(x => x.toString(16).padStart(2, '0')).join('');
  return timingEqual(expected, header.slice(7).toLowerCase());
}
export function isAllowedRedirect(uri, allowedCsv) {
  if (!uri || !allowedCsv) return false;
  try {
    const u = new URL(uri);
    return u.protocol === 'https:' && !u.hash && allowedCsv.split(',').map(x => x.trim()).filter(Boolean).includes(uri);
  } catch { return false; }
}
export function validJobRequest(a) {
  if (!a || typeof a !== 'object' || Array.isArray(a)) throw new Error('Arguments must be a JSON object');
  const mode = String(a.mode || 'query_meta');
  if (!['discover_accounts','discover_fields','query_meta','analyze_data','export_report','inspect_creatives','start_historical_audit'].includes(mode)) throw new Error('Unsupported request mode');
  const p = a.params || {};
  if (typeof p !== 'object' || !p || Array.isArray(p) || JSON.stringify(p).length > 13000) throw new Error('Invalid parameters');
  return { mode, params: p };
}
export const safeText = s => String(s || '').replace(/[<>&"']/g, x => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[x]));
