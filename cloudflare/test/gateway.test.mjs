import test from 'node:test';
import assert from 'node:assert/strict';
import app from '../src/index.mjs';
const env={PUBLIC_BASE_URL:'https://example.workers.dev'};
test('health and OAuth discovery work without heavy code or Meta calls',async()=>{
 const health=await app.fetch(new Request('https://example.workers.dev/health'),env);
 assert.equal(health.status,200);
 const meta=await app.fetch(new Request('https://example.workers.dev/.well-known/oauth-authorization-server'),env);
 const d=await meta.json();assert.ok(d.code_challenge_methods_supported.includes('S256'));
 assert.equal(d.registration_endpoint,'https://example.workers.dev/register');
 const res=await app.fetch(new Request('https://example.workers.dev/.well-known/oauth-protected-resource/mcp'),env);
 assert.equal((await res.json()).resource,'https://example.workers.dev/mcp');
});
test('unauthorized MCP does not expose data or tools',async()=>{
 const r=await app.fetch(new Request('https://example.workers.dev/mcp',{method:'POST',body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/list'})}),{...env,DB:{prepare:()=>{throw Error('Must not read DB without a bearer')}}});
 assert.equal(r.status,401);assert.ok(r.headers.get('www-authenticate').includes('oauth-protected-resource'));
});
